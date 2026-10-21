"""HTTP/HTTPS federation transport backend (V1, T-095 / T-101 / T-102).

The transport talks **only** to the remote FedNode's own HTTP listener
(``POST {endpoint}/fed/v1/inbox``, ``GET {endpoint}/fed/v1/results/{id}``,
``POST {endpoint}/fed/v1/ack``, ``GET {endpoint}/fed/v1/discover``). It
never reaches a remote relay's scheduler directly — that is the job of the
remote FedNode, which re-submits the task into its own local relay.

What this version does:

* ``connect`` stores an ``httpx.Client`` session; raises on unreachable.
* ``sync_outbox`` POSTs each pending outbox file to ``/fed/v1/inbox``.
  When a :class:`federation.crypto.Crypto` is configured, the plaintext
  envelope is sealed into a ciphertext envelope first (T-101). The
  receiver returns an inline ACK (``accepted``/``rejected``/``failed``);
  on ``accepted`` the outbox file is moved to ``archive/`` and a
  ``POST /fed/v1/ack`` is sent back so the receiver can free state
  (T-102). On ``rejected`` the file is archived (terminal). On ``failed``
  or no-ACK the file stays and is retried per :class:`RetryPolicy`
  (exponential backoff + full jitter).
* ``sync_inbox`` polls ``/fed/v1/results/{id}`` for pending ids and
  writes any 200-response payload as an inbox file atomically.
* ``discover_capabilities`` GETs ``/fed/v1/discover`` and returns the
  parsed JSON list.

Atomic file I/O (``.tmp_<id>`` + ``os.replace``) is used for inbox
writes and outbox archival via :mod:`federation.lifecycle` helpers.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import httpx

from federation.ack import (
    ACK_STATUS_ACCEPTED,
    ACK_STATUS_REJECTED,
    SenderAck,
)
from federation.crypto import Crypto
from federation.envelope import seal as envelope_seal
from federation.index import MetadataIndex
from federation.lifecycle import (
    LifecycleRegistry,
    MessageState,
    RetryPolicy,
    atomic_write_json,
)
from federation.transport import Transport

log = logging.getLogger("federation.http")

# Endpoints (P2P API, served by both FedNodes identically).
INBOX_PATH = "/fed/v1/inbox"
RESULTS_PATH = "/fed/v1/results"  # /{message_id} appended
DISCOVER_PATH = "/fed/v1/discover"
ACK_PATH = "/fed/v1/ack"

# Default per-request timeout. The listener is on a trusted peer; a few
# seconds is plenty and keeps a hung peer from blocking the sync cycle.
DEFAULT_TIMEOUT = 10.0


class HttpTransport(Transport):
    """HTTP transport — talks to the remote FedNode's P2P listener."""

    def __init__(self, cfg: dict[str, Any]) -> None:
        self.cfg = cfg
        self._client: httpx.Client | None = None
        self._url: str = ""
        self._token: str = ""
        # Pending remote task ids the caller wants results polled for.
        # Set via ``pending_ids`` (or passed to sync_inbox) — minimal T-095
        # contract; the full poll loop is T-096.
        self.pending_ids: set[str] = set()
        # Optional injected httpx transport (MockTransport in tests).
        self._mock_transport: httpx.BaseTransport | None = None
        # E2EE (T-101). When set, outbound envelopes are sealed with this
        # Crypto before POSTing. The daemon wires one per peer.
        self.crypto: Crypto | None = None
        # Lifecycle (T-102). Optional registry + retry policy.
        self.registry: LifecycleRegistry | None = None
        self.retry_policy: RetryPolicy = RetryPolicy()
        # T-103 metadata index. When wired (by the daemon), the transport
        # mirrors state transitions into it on every ACK outcome. Optional
        # — the transport works without it (pre-T-103 behaviour).
        self.index: "MetadataIndex | None" = None
        # SenderACK ledger is created lazily per sync_outbox call (needs
        # the outbox/ + archive/ dirs). Kept on the instance for status.
        self.sender_ack: SenderAck | None = None

    # ------------------------------------------------------------------ #
    # Connection
    # ------------------------------------------------------------------ #
    def connect(self, url: str, token: str) -> None:
        """Open a session to the remote FedNode listener.

        Raises ``httpx.HTTPError`` (or a subclass) if the peer is
        unreachable. ``url`` is the peer endpoint base (e.g.
        ``https://fed-b:8791``); trailing slashes are normalised.
        """
        if not url:
            raise ValueError("connect: url must be non-empty")
        if not token:
            raise ValueError("connect: token must be non-empty")
        self._url = url.rstrip("/")
        self._token = token
        headers = {"Authorization": f"Bearer {token}"}
        self._client = httpx.Client(
            base_url=self._url,
            headers=headers,
            timeout=DEFAULT_TIMEOUT,
            transport=self._mock_transport,
        )
        # Probe the peer with a cheap GET. The listener's discover endpoint
        # is the lightest reachable route; an unreachable peer raises here.
        # We swallow 401/403 (still "reachable") but bubble network errors.
        try:
            self._client.get(DISCOVER_PATH).raise_for_status()
        except httpx.HTTPStatusError as e:
            # 4xx means the peer is up but auth/config is wrong — still a
            # connect failure for our purposes, but distinct from a network
            # error. Close the half-open client and re-raise with context.
            self._client.close()
            self._client = None
            raise ConnectionError(
                f"peer {self._url} reachable but rejected probe: {e.response.status_code}"
            ) from e
        except httpx.HTTPError:
            # Network-level failure: close the half-open client and re-raise.
            self._client.close()
            self._client = None
            raise

    def _require_client(self) -> httpx.Client:
        if self._client is None:
            raise RuntimeError("HttpTransport not connected — call connect() first")
        return self._client

    # ------------------------------------------------------------------ #
    # sync_outbox
    # ------------------------------------------------------------------ #
    def sync_outbox(self, outbox_dir: Path) -> list[str]:
        """Send each pending outbox file to the remote via POST /fed/v1/inbox.

        Returns the list of ``message_id``s the remote **accepted** (HTTP
        202 + ``status=accepted`` inline ACK). On a non-202 response, or
        when the inline ACK is ``failed``/missing, the file is left in
        place and retried per :class:`RetryPolicy` (backoff + jitter); a
        message past ``max_attempts`` is moved to ``FAILED`` and left for
        the daemon to archive. ``rejected`` ACKs archive the file
        immediately (terminal). A network error stops the cycle and
        re-raises so the daemon can back off the whole peer.

        When ``self.crypto`` is set, each plaintext envelope is sealed
        into a ciphertext envelope before POSTing (T-101).
        """
        client = self._require_client()
        outbox_dir = Path(outbox_dir)
        if not outbox_dir.is_dir():
            return []
        archive_dir = outbox_dir.parent / "archive"
        archive_dir.mkdir(parents=True, exist_ok=True)
        sender_ack = SenderAck(
            outbox_dir=outbox_dir,
            archive_dir=archive_dir,
            registry=self.registry,
        )
        self.sender_ack = sender_ack

        # Collect outbox files, recursing into hash-partition subdirs (T-103).
        outbox_files = sorted(
            f for f in outbox_dir.rglob("*.json") if f.is_file()
        )
        accepted: list[str] = []
        for f in outbox_files:
            if f.name.startswith("."):
                continue
            try:
                payload = json.loads(f.read_text())
            except (json.JSONDecodeError, OSError) as e:
                log.warning("skipping unreadable outbox file %s: %s", f, e)
                continue
            message_id = payload.get("message_id")
            if not message_id:
                log.warning("outbox file %s has no message_id; skipping", f)
                continue

            # E2EE: seal the plaintext envelope into a ciphertext envelope.
            wire_payload: Any = payload
            if self.crypto is not None:
                try:
                    wire_payload = envelope_seal(self.crypto, payload, message_id=message_id)
                except Exception as e:  # noqa: BLE001 — crypto must not crash the cycle
                    log.error("encrypt failed for %s: %s", message_id, e)
                    self._record_failure(message_id, "encrypt_failed")
                    continue

            # Send + handle the inline ACK.
            try:
                resp = client.post(INBOX_PATH, json=wire_payload)
            except httpx.HTTPError as e:
                log.warning("POST /fed/v1/inbox failed for %s: %s", message_id, e)
                self._record_failure(message_id, "network_error")
                raise
            if resp.status_code == 202:
                try:
                    ack_body = resp.json()
                except json.JSONDecodeError:
                    ack_body = None
                status = sender_ack.handle_inbox_response(
                    message_id, ack_body, outbox_file=f
                )
                self._mirror_index_state(message_id, status)
                if status == ACK_STATUS_ACCEPTED:
                    accepted.append(message_id)
                    self._post_ack_back(message_id, status)
                elif status == ACK_STATUS_REJECTED:
                    self._post_ack_back(message_id, status)
                else:
                    # failed / unknown — retry path.
                    self._record_failure(message_id, f"ack={status}")
            elif resp.status_code == 409:
                # Receiver rejected as replay. Treat as a rejected ACK:
                # archive the file (terminal) so we don't loop forever.
                log.warning(
                    "peer rejected %s as replay — archiving", message_id
                )
                sender_ack.handle_inbox_response(
                    message_id,
                    {"status": ACK_STATUS_REJECTED, "message_id": message_id},
                    outbox_file=f,
                )
                self._mirror_index_state(message_id, ACK_STATUS_REJECTED)
                self._post_ack_back(message_id, ACK_STATUS_REJECTED)
            elif resp.status_code == 413:
                # T-109: the peer rejected the payload as too large. This is
                # terminal — the same payload cannot succeed on a retry, so
                # the message must not be re-queued. Mirror the 409 path:
                # archive + index mirror REJECTED + ack-back.
                log.warning(
                    "peer rejected %s as too large — terminal", message_id
                )
                sender_ack.handle_inbox_response(
                    message_id,
                    {"status": ACK_STATUS_REJECTED, "message_id": message_id},
                    outbox_file=f,
                )
                self._mirror_index_state(message_id, ACK_STATUS_REJECTED)
                self._post_ack_back(message_id, ACK_STATUS_REJECTED)
            else:
                log.warning(
                    "peer rejected %s: %s %s",
                    message_id,
                    resp.status_code,
                    resp.text[:200],
                )
                self._record_failure(message_id, f"http={resp.status_code}")
        return accepted

    def _mirror_index_state(self, message_id: str, ack_status: str) -> None:
        """T-103: mirror the sender's view of the state into the index.

        Maps the ACK outcome to a lifecycle state and updates the index row
        (if a transport has one wired). The mapping mirrors
        :meth:`SenderAck._advance_lifecycle` so the index stays in lock-step
        with the in-memory registry. Best-effort — a failure here is logged
        but never raised.
        """
        if self.index is None:
            return
        if ack_status == ACK_STATUS_ACCEPTED:
            target = MessageState.ARCHIVED
        elif ack_status == ACK_STATUS_REJECTED:
            target = MessageState.REJECTED
        else:
            return  # failed/unknown handled by _record_failure retry path
        try:
            self.index.update_state(message_id, target)
        except Exception as exc:  # noqa: BLE001
            log.debug("index mirror %s → %s failed: %s", message_id, target, exc)

    def _record_failure(self, message_id: str, reason: str) -> None:
        """Record a send failure; advance retry counter via the registry.

        T-103: the retry increment + FAILED mirror are also written to the
        index (best-effort), independent of the in-memory registry so the
        index stays consistent even when the transport has no registry
        wired (e.g. a transport used standalone).
        """
        # T-103: mirror the retry increment into the index.
        new_count = -1
        if self.index is not None:
            try:
                new_count = self.index.increment_retry(message_id)
            except Exception as exc:  # noqa: BLE001
                log.debug("index increment_retry %s failed: %s", message_id, exc)

        # In-memory registry path (advance the Lifecycle).
        if self.registry is not None:
            lc = self.registry.get(message_id)
            if lc is not None and lc.state in (
                MessageState.QUEUED, MessageState.SENT, MessageState.PENDING_REMOTE_RESPONSE
            ):
                delay = lc.record_retry(self.retry_policy)
                if delay < 0:
                    log.warning("message %s exhausted retries: %s", message_id, reason)
                else:
                    log.info(
                        "message %s will retry in %.1fs (reason=%s)",
                        message_id, delay, reason,
                    )

        # T-103: mirror FAILED into the index when retries are exhausted,
        # independent of the registry. We detect exhaustion by either the
        # in-memory record_retry returning -1 OR the index retry_count
        # reaching the policy's max_attempts.
        exhausted = False
        if self.registry is not None:
            lc = self.registry.get(message_id)
            if lc is not None and lc.state is MessageState.FAILED:
                exhausted = True
        if not exhausted and new_count >= 0 and new_count + 1 >= self.retry_policy.max_attempts:
            exhausted = True
        if exhausted and self.index is not None:
            try:
                self.index.update_state(message_id, MessageState.FAILED)
            except Exception as exc:  # noqa: BLE001
                log.debug("index FAILED mirror %s: %s", message_id, exc)

    def _post_ack_back(self, message_id: str, status: str) -> None:
        """POST /fed/v1/ack to the receiver so it can free state.

        Best-effort: a failure here is logged but does not raise — the
        sender's outbox file is already archived; the receiver's ledger
        entry will TTL-prune.
        """
        client = self._client
        if client is None:
            return
        body = self.sender_ack.build_ack_request(message_id, status) if self.sender_ack else {
            "message_id": message_id,
            "status": status,
        }
        try:
            resp = client.post(ACK_PATH, json=body)
            if resp.status_code != 200:
                log.debug(
                    "ack-back for %s returned %s (non-fatal)", message_id, resp.status_code
                )
        except httpx.HTTPError as e:
            log.debug("ack-back for %s failed (non-fatal): %s", message_id, e)

    # ------------------------------------------------------------------ #
    # sync_inbox (minimal — full poll loop is T-096)
    # ------------------------------------------------------------------ #
    def sync_inbox(self, inbox_dir: Path) -> list[str]:
        """Poll pending remote task results and write any as inbox files.

        Minimal T-095 contract: iterate ``self.pending_ids``, GET
        ``/fed/v1/results/{id}``, and on 200 write the body as an inbox
        file atomically. 404 → skip (not ready yet). Returns the ids newly
        written.
        """
        client = self._require_client()
        inbox_dir = Path(inbox_dir)
        inbox_dir.mkdir(parents=True, exist_ok=True)
        new_ids: list[str] = []
        for mid in sorted(self.pending_ids):
            try:
                resp = client.get(f"{RESULTS_PATH}/{mid}")
            except httpx.HTTPError as e:
                log.warning("GET /fed/v1/results/%s failed: %s", mid, e)
                raise
            if resp.status_code == 404:
                continue
            if resp.status_code != 200:
                log.warning(
                    "result poll for %s returned %s", mid, resp.status_code
                )
                continue
            try:
                body = resp.json()
            except json.JSONDecodeError as e:
                log.warning("result body for %s not JSON: %s", mid, e)
                continue
            # Use the message_id from the body if present, else the polled id.
            written_id = body.get("message_id", mid)
            _atomic_write_json(inbox_dir / f"{written_id}.json", body)
            new_ids.append(written_id)
        return new_ids

    # ------------------------------------------------------------------ #
    # discover
    # ------------------------------------------------------------------ #
    def discover_capabilities(self) -> list[dict[str, Any]]:
        """GET /fed/v1/discover → remote peer's capability offer list."""
        client = self._require_client()
        resp = client.get(DISCOVER_PATH)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list):
            raise ValueError(
                f"discover: expected list, got {type(data).__name__}"
            )
        return data

    # ------------------------------------------------------------------ #
    # teardown
    # ------------------------------------------------------------------ #
    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None


# ---------------------------------------------------------------------- #
# helpers
# ---------------------------------------------------------------------- #
def _atomic_write_json(path: Path, obj: dict[str, Any]) -> None:
    """Write JSON to ``path`` atomically: tmp file + ``os.replace``.

    Thin wrapper over :func:`federation.lifecycle.atomic_write_json` so
    the transport uses the single source of truth.
    """
    atomic_write_json(path, obj)