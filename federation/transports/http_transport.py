"""HTTP/HTTPS federation transport backend (V1, T-095).

The transport talks **only** to the remote FedNode's own HTTP listener
(``POST {endpoint}/fed/v1/inbox``, ``GET {endpoint}/fed/v1/results/{id}``,
``GET {endpoint}/fed/v1/discover``). It never reaches a remote relay's
scheduler directly — that is the job of the remote FedNode, which re-submits
the task into its own local relay.

For T-095 this is a minimal but real implementation:

* ``connect`` stores an ``httpx.Client`` session; raises on unreachable.
* ``sync_outbox`` POSTs each pending outbox file to ``/fed/v1/inbox``;
  returns the list of ``message_id``s that the remote accepted (202).
  ACK-driven outbox cleanup lands in T-102 — T-095 only needs success ids.
* ``sync_inbox`` is intentionally minimal (the plan defers the full poll
  loop to the T-096 daemon): it polls ``/fed/v1/results/{id}`` for a set
  of pending remote task ids supplied via ``pending_ids`` on the instance
  and writes any 200-response payload as an inbox file; 404s are skipped.
* ``discover_capabilities`` GETs ``/fed/v1/discover`` and returns the
  parsed JSON list.

Plaintext JSON bodies for now (E2EE arrives in T-101); the listener accepts
both, so tests pass either way.

Atomic file I/O (``.tmp_<id>`` + ``os.rename``) is used when writing inbox
files, matching the design in the plan.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Any

import httpx

from federation.transport import Transport

log = logging.getLogger("federation.http")

# Endpoints (P2P API, served by both FedNodes identically).
INBOX_PATH = "/fed/v1/inbox"
RESULTS_PATH = "/fed/v1/results"  # /{message_id} appended
DISCOVER_PATH = "/fed/v1/discover"

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

        Returns the list of ``message_id``s the remote accepted (HTTP 202).
        On a non-202 response the file is left in place (T-102 retry policy
        handles backoff); a network error stops the cycle and re-raises.
        """
        client = self._require_client()
        outbox_dir = Path(outbox_dir)
        if not outbox_dir.is_dir():
            return []
        sent: list[str] = []
        for f in sorted(outbox_dir.iterdir()):
            if not f.is_file() or f.name.startswith("."):
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
            try:
                resp = client.post(INBOX_PATH, json=payload)
            except httpx.HTTPError as e:
                log.warning("POST /fed/v1/inbox failed for %s: %s", message_id, e)
                raise
            if resp.status_code == 202:
                sent.append(message_id)
            else:
                log.warning(
                    "peer rejected %s: %s %s",
                    message_id,
                    resp.status_code,
                    resp.text[:200],
                )
        return sent

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
    """Write JSON to ``path`` atomically: tmp file + ``os.rename``."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(
        prefix=f".tmp_{path.stem}_", dir=str(path.parent), suffix=".json"
    )
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(obj, f, indent=2)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise