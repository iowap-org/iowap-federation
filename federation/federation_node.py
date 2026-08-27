"""Federation Daemon (T-096 / T-101 / T-102).

The daemon that runs the claim/forward/complete loop + sync cycle. It is the
bridge between a local AI-Relay and one or more remote FedNodes.

Architecture (threads, mirrors ``nodes.common.node_daemon``)::

    Thread 1: Heartbeat  — heartbeats ``federation`` + the approved forward
              capabilities to the local relay (``RelayClient``).
    Thread 2: Claim loop — claims stages for the approved forward caps; on
              claim, writes a forward envelope to the target peer's outbox.
    Thread 3: Sync cycle — ``sync_outbox()`` → ``sync_inbox()`` → cleanup
              per peer. T-102: the transport now drives the ACK/retry
              lifecycle; the daemon owns the :class:`LifecycleRegistry`
              and the per-peer :class:`Crypto` (E2EE, T-101).
    Thread 4: Inbox processing — reads each peer's inbox files: a ``forward``
              message is re-submitted as a normal task into the local relay
              (``submit_simple_task``); a ``reply`` message completes the
              originator's own stage (correlated via ``remote_task_id``).

The pure logic (``_build_forward_envelope`` / ``_claim_and_forward`` /
``_process_inbox_file`` / ``_process_inbox``) is kept free of threads and
network so unit tests exercise it directly with a fake ``RelayClient``.

Entry point: console script ``federation-daemon`` (``[project.scripts]``).
Config from ``~/.relay/node.yaml`` (``federation`` section, T-095).

T-101 E2EE: when a ``Crypto`` factory is supplied (``crypto_factory=``),
the daemon builds one :class:`federation.crypto.Crypto` per peer and
attaches it to the peer's transport (so outbound envelopes are sealed)
and uses it to decrypt inbound inbox files (the listener writes
plaintext to disk, but a daemon may also receive files placed by other
means). The default is plaintext (no crypto) — backwards compatible.

T-102 lifecycle: every forward message is tracked in a
:class:`federation.lifecycle.LifecycleRegistry`; the state machine
(``CREATED → ENCRYPTED → QUEUED → SENT → ACKED → COMPLETED → ARCHIVED``,
terminal ``FAILED/EXPIRED/REJECTED``) is advanced as the daemon/transport
progress the message. Replay dedupe uses
:class:`federation.lifecycle.ReplayGuard` (TTL-pruned). Async transports
move a sent message to ``PENDING_REMOTE_RESPONSE`` so a worker does not
block on the remote.

In-memory reply correlation (``_forwarded_stages``) — SQLite persistence
lands in T-103.

Fair-Use enforcement (T-099) is optional: the daemon accepts a
``FairUseRegistry``. When the registry is ``enforce=False`` (the default)
no claim is gated. When enforced, ``_claim_and_forward`` reserves a
parallel+daily slot on a successful claim and ``_process_inbox_file``
releases the parallel slot when the reply completes the stage.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Protocol

from federation.config import NodeConfig, Peer, load_node_config
from federation.crypto import Crypto, CryptoError
from federation.envelope import open as envelope_open
from federation.fairuse import FairUseRegistry
from federation.lifecycle import (
    Lifecycle,
    LifecycleRegistry,
    MessageState,
    ReplayGuard,
    RetryPolicy,
    atomic_write_json,
    emit_event,
)
from federation.paths import (
    ensure_federation_dirs,
    federation_root,
    remote_dirs,
)

log = logging.getLogger("federation.daemon")

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

HEARTBEAT_INTERVAL = 30.0      # seconds, like the node daemon
CLAIM_INTERVAL = 2.0           # poll the scheduler for claimable stages
SYNC_INTERVAL = 5.0            # outbox→inbox sweep per peer
INBOX_INTERVAL = 1.0           # drain the inboxes
_SHUTDOWN_GRACE = 5.0          # join timeout per thread

# The synthetic capability this FedNode advertises to its local relay.
FEDERATION_CAP = "federation"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _utcnow_str() -> str:
    return _utcnow().isoformat()


# ---------------------------------------------------------------------------
# Atomic JSON write (mirrors the listener/transport helpers)
# ---------------------------------------------------------------------------

def _atomic_write_json(path: Path, obj: dict[str, Any]) -> None:
    """Write ``obj`` as JSON to ``path`` atomically: tmp file + ``os.replace``.

    Thin wrapper over :func:`federation.lifecycle.atomic_write_json` so
    the daemon uses the single source of truth for atomic I/O.
    """
    atomic_write_json(path, obj)


def _read_json(path: Path) -> dict[str, Any] | None:
    """Read a JSON file, returning ``None`` if unreadable/invalid."""
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError) as e:
        log.warning("could not read %s: %s", path, e)
        return None


# ---------------------------------------------------------------------------
# RelayClient protocol (for typing + fake clients in tests)
# ---------------------------------------------------------------------------

class RelayClientLike(Protocol):
    """The subset of ``RelayClient`` the daemon uses.

    The real ``nodes.common.relay_client.RelayClient`` satisfies this; tests
    pass a fake with the same method signatures.
    """

    meta: dict[str, Any]
    base_url: str

    def heartbeat(
        self, caps: list[dict[str, Any]], in_flight: dict[str, int]
    ) -> dict[str, Any]: ...

    def claim(self, capability: str) -> dict[str, Any] | None: ...

    def complete(
        self, task_id: str, stage_id: str, result: dict[str, Any]
    ) -> dict[str, Any]: ...

    def submit_simple_task(
        self,
        capability: str,
        payload: dict[str, Any],
        *,
        name: str = ...,
        priority: int = ...,
        owner_node_id: str | None = ...,
    ) -> dict[str, Any]: ...

    def get_task(self, task_id: str) -> dict[str, Any]: ...

    def maybe_refresh_token(self) -> None: ...

    def _current_backoff(self) -> float: ...


# A transport the sync cycle can drive. Mirrors ``federation.transport.Transport``
# but kept as a structural protocol so tests can pass a minimal fake.
class TransportLike(Protocol):
    def sync_outbox(self, outbox_dir: Path) -> list[str]: ...
    def sync_inbox(self, inbox_dir: Path) -> list[str]: ...
    def close(self) -> None: ...


# Factory: build a connected transport for a peer. ``main`` supplies a real
# factory (HttpTransport.connect); tests inject a fake factory.
TransportFactory = Callable[[Peer], TransportLike]

# Factory: build a :class:`federation.crypto.Crypto` for a peer (T-101 E2EE).
# Returns ``None`` when no crypto is configured for the peer (plaintext mode).
CryptoFactory = Callable[[Peer], "Crypto | None"]


# ---------------------------------------------------------------------------
# FederationDaemon
# ---------------------------------------------------------------------------

class FederationDaemon:
    """The claim/forward/complete + sync cycle daemon.

    The pure helpers (``_build_forward_envelope``, ``_claim_and_forward``,
    ``_process_inbox_file``, ``_process_inbox``, ``_sync_one_peer``) do not
    touch threads or the network and are the unit-test surface. ``run()``
    starts the four threads described in the plan.
    """

    def __init__(
        self,
        client: RelayClientLike,
        cfg: dict[str, Any],
        *,
        fed_config: NodeConfig | None = None,
        forward_caps: list[str] | None = None,
        transport_factory: TransportFactory | None = None,
        crypto_factory: CryptoFactory | None = None,
        base_dir: Path | None = None,
        intervals: dict[str, float] | None = None,
        fairuse_registry: FairUseRegistry | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        self.client = client
        self.cfg = cfg
        self.fed_config = fed_config or load_node_config()
        # Capabilities whose stages this FedNode forwards to a remote peer.
        # In production these are the admin-approved remote capabilities from
        # node.yaml; tests pass an explicit list.
        self.forward_caps = list(forward_caps) if forward_caps else []
        self.transport_factory = transport_factory
        self.crypto_factory = crypto_factory
        self.base_dir = Path(base_dir) if base_dir else federation_root()

        # Fair-Use enforcement (T-099). Optional: when ``None`` (or when a
        # registry is present but ``enforce=False``) the daemon never blocks
        # a claim. When enforced, ``_claim_and_forward`` asks the policy
        # before calling ``client.claim`` and releases the parallel slot
        # when the reply completes (or the stage is abandoned).
        self.fairuse = fairuse_registry or FairUseRegistry(enforce=False)

        # T-102 lifecycle: per-message state tracker + TTL-pruned replay
        # guard. The registry is shared with the transport so the ACK
        # handler can advance state.
        self.lifecycle = LifecycleRegistry()
        self.replay_guard = ReplayGuard()
        self.retry_policy = retry_policy or RetryPolicy()

        iv = intervals or {}
        self.heartbeat_interval = iv.get("heartbeat", HEARTBEAT_INTERVAL)
        self.claim_interval = iv.get("claim", CLAIM_INTERVAL)
        self.sync_interval = iv.get("sync", SYNC_INTERVAL)
        self.inbox_interval = iv.get("inbox", INBOX_INTERVAL)

        # Thread coordination.
        self._stop_event = threading.Event()
        self._threads: list[threading.Thread] = []

        # In-flight forward stages per capability (heartbeat in_flight dict).
        self._in_flight: dict[str, int] = {}
        self._lock = threading.Lock()

        # Reply correlation: stage_id → task_id (the stage WE claimed and
        # forwarded; we complete it when the reply arrives). In-memory for
        # T-096; T-103 moves this to SQLite.
        self._forwarded_stages: dict[str, str] = {}

        # Connected transports per peer id (lazy-built on first sync cycle).
        self._transports: dict[str, TransportLike] = {}
        # Per-peer Crypto (T-101), built lazily alongside the transport.
        self._cryptos: dict[str, Crypto | None] = {}

        # Observability counters (status file / smoke test).
        self.tasks_forwarded = 0
        self.tasks_submitted = 0
        self.stages_completed = 0
        self.last_heartbeat_status = "unknown"
        self._started_at = _utcnow()

    # ------------------------------------------------------------------ #
    # Pure helpers — the unit-test surface
    # ------------------------------------------------------------------ #

    def _peer_for_stage(self, stage: dict[str, Any]) -> Peer | None:
        """Resolve which peer a claimed stage should be forwarded to.

        Resolution order:
        1. explicit ``stage["payload"]["remote"]`` (peer id),
        2. exactly one configured peer → that peer,
        3. ``None`` (unroutable — log + skip).
        """
        peers = self.fed_config.peers
        if not peers:
            return None
        payload = stage.get("payload") or {}
        remote = payload.get("remote")
        if remote:
            for p in peers:
                if p.id == remote:
                    return p
            log.warning("stage references unknown peer %r", remote)
            return None
        if len(peers) == 1:
            return peers[0]
        return None

    def _build_forward_envelope(self, stage: dict[str, Any], peer: Peer) -> dict[str, Any]:
        """Build a plaintext forward envelope for a claimed stage.

        ``remote_task_id`` carries OUR ``stage_id`` so the remote's reply can
        correlate back to this stage (see ``_process_inbox_file``).
        """
        stage_id = str(stage.get("stage_id") or "")
        task_id = str(stage.get("task_id") or "")
        capability = str(stage.get("capability") or "")
        payload = stage.get("payload") or {}
        # If the payload already carries the remote task id (a re-forward),
        # keep it; otherwise it is the originating stage's id.
        remote_task_id = str(payload.get("remote_task_id") or stage_id)
        return {
            "message_id": str(uuid.uuid4()),
            "type": "task",
            "direction": "forward",
            "capability": capability,
            "remote_task_id": remote_task_id,
            "created_at": _utcnow_str(),
            "payload": payload,
            # Provenance for the remote's inbox processor.
            "_origin": {
                "peer": self.fed_config.node_name,
                "stage_id": stage_id,
                "task_id": task_id,
            },
        }

    def _write_forward_file(self, peer: Peer, stage: dict[str, Any]) -> str | None:
        """Write a forward envelope to the peer's outbox. Returns message_id.

        Records the stage_id→task_id correlation for the reply path and
        registers a :class:`Lifecycle` for the new message (state
        ``CREATED`` → ``QUEUED``). Returns ``None`` if the stage is
        unroutable.
        """
        peer_obj = peer
        inbox, outbox = ensure_federation_dirs(peer_obj.id)
        envelope = self._build_forward_envelope(stage, peer_obj)
        message_id = envelope["message_id"]
        stage_id = envelope["_origin"]["stage_id"]
        task_id = envelope["_origin"]["task_id"]
        dest = outbox / f"{message_id}.json"
        atomic_write_json(dest, envelope)
        # Register + advance the lifecycle tracker.
        lc = Lifecycle(message_id=message_id, state=MessageState.CREATED)
        try:
            self.lifecycle.register(lc)
            emit_event(
                "MESSAGE_CREATED",
                message_id=message_id,
                peer=peer_obj.id,
                capability=envelope["capability"],
                stage=stage_id,
            )
            lc.queued()
        except ValueError:
            # Already registered (rare re-entrance) — fetch + re-queue.
            lc = self.lifecycle.get(message_id)
            if lc is not None:
                try:
                    lc.queued()
                except Exception:  # noqa: BLE001
                    pass
        # Record correlation so the reply can complete our stage.
        if stage_id:
            with self._lock:
                self._forwarded_stages[stage_id] = task_id
                self._in_flight[FEDERATION_CAP] = (
                    self._in_flight.get(FEDERATION_CAP, 0) + 1
                )
        log.info(
            "MESSAGE_CREATED message_id=%s peer=%s capability=%s stage=%s",
            message_id, peer_obj.id, envelope["capability"], stage_id,
        )
        return message_id

    def _claim_and_forward(self, capability: str) -> None:
        """Claim one stage for ``capability`` and forward it to a peer's outbox.

        Pure-ish (only touches the relay client + filesystem); safe to call
        from a test without threads. On a 204 (``claim`` returns ``None``) it
        is a no-op. When a fair-use policy is enforced for ``capability`` and
        the parallel/daily cap is exhausted, the claim is skipped entirely
        (no ``client.claim`` call) so the stage stays available for a peer.
        """
        # Fair-use gate (T-099): skip claiming while the cap is exhausted.
        policy = self.fairuse.for_cap(capability)
        if policy is not None and not policy.can_claim():
            log.info(
                "FAIRUSE_SKIP capability=%s — parallel/daily cap exhausted",
                capability,
            )
            return
        try:
            stage = self.client.claim(capability)
        except Exception as exc:  # noqa: BLE001 — never crash the claim loop
            log.error("claim %s failed: %s", capability, exc)
            return
        if stage is None:
            return
        # Reserve the fair-use slot only after a successful claim. If the
        # reservation fails (e.g. raced between can_claim and claim), release
        # the stage by leaving it claimed-but-unforwarded and do not consume
        # a slot. This is conservative — a re-run will re-claim later.
        if policy is not None and not policy.claim():
            log.warning(
                "FAIRUSE_RACE capability=%s — stage claimed but slot denied; "
                "leaving stage claimed (will be retried by scheduler)",
                capability,
            )
            return
        peer = self._peer_for_stage(stage)
        if peer is None:
            log.warning(
                "claimed stage %s for %s but no routable peer — leaving claimed",
                stage.get("stage_id"), capability,
            )
            # We consumed a fair-use slot for nothing — release it.
            if policy is not None:
                policy.release()
            return
        message_id = self._write_forward_file(peer, stage)
        if message_id is not None:
            self.tasks_forwarded += 1
            log.info(
                "forwarded stage %s → peer %s (message_id=%s)",
                stage.get("stage_id"), peer.id, message_id,
            )
        else:
            # Forward failed (unroutable mid-write) — release the slot.
            if policy is not None:
                policy.release()

    def _decrypt_inbox_file(self, peer: Peer, raw: dict[str, Any]) -> dict[str, Any] | None:
        """Decrypt an inbox file body if it is a ciphertext envelope.

        The listener writes plaintext envelopes to disk (after decrypting
        inbound POSTs), so in the normal path this is a no-op. However, a
        daemon may also pick up files placed by other means (e.g. an
        email transport drops a ciphertext envelope). When a Crypto is
        configured for the peer and the file looks like a ciphertext
        envelope (``ciphertext`` + ``nonce``), decrypt it here.

        Returns the plaintext envelope dict, or ``None`` on decrypt
        failure (the caller skips the file). When the body is already
        plaintext, it is returned unchanged.
        """
        if not isinstance(raw, dict):
            return None
        if "ciphertext" not in raw or "nonce" not in raw:
            return raw  # plaintext path
        crypto = self._cryptos.get(peer.id)
        if crypto is None:
            # File looks encrypted but we have no key for this peer — skip.
            log.warning(
                "inbox file is ciphertext but no crypto for peer %s — skipping",
                peer.id,
            )
            return None
        try:
            inner, header = envelope_open(crypto, raw)
        except CryptoError as e:
            log.error("inbox decrypt failed for peer %s: %s", peer.id, e)
            return None
        emit_event("MESSAGE_DECRYPTED", message_id=header["message_id"])
        return inner

    def _process_inbox_file(self, peer: Peer, path: Path) -> str | None:
        """Process a single inbox file. Returns the action taken or ``None``.

        Actions: ``"submitted"`` (forward → local task), ``"completed"``
        (reply → complete our stage), ``"duplicate"`` (replay),
        ``"skipped"``. Removes the file once processed.

        T-102: replay dedupe uses the TTL-pruned :class:`ReplayGuard`.
        Encrypted inbox files are decrypted via :meth:`_decrypt_inbox_file`
        when a Crypto is configured for the peer.
        """
        raw = _read_json(path)
        if raw is None:
            return "skipped"
        envelope = self._decrypt_inbox_file(peer, raw)
        if envelope is None:
            try:
                path.unlink()
            except OSError:
                pass
            return "skipped"
        message_id = str(envelope.get("message_id") or "")
        if not message_id:
            log.warning("inbox file %s has no message_id — skipping", path.name)
            return "skipped"
        # Replay dedupe (T-102: TTL-pruned ReplayGuard).
        if not self.replay_guard.check_and_add(message_id):
            log.info("duplicate message_id=%s — ignoring", message_id)
            try:
                path.unlink()
            except OSError:
                pass
            return "duplicate"

        direction = str(envelope.get("direction") or "")
        etype = str(envelope.get("type") or "")
        capability = str(envelope.get("capability") or "")
        payload = envelope.get("payload") or {}

        if direction == "forward" or etype == "task":
            # Remote sent us a task → re-submit as a normal task into our
            # own relay. The remote's stage_id rides in ``remote_task_id``
            # so a later result can correlate back.
            try:
                submit_payload = dict(payload)
                # Carry the origin correlation through the local relay so
                # the eventual result can be routed back to the remote.
                if envelope.get("remote_task_id"):
                    submit_payload.setdefault(
                        "remote_task_id", envelope["remote_task_id"]
                    )
                submit_payload.setdefault("remote", self.fed_config.node_name)
                self.client.submit_simple_task(
                    capability,
                    submit_payload,
                    name=f"federated:{message_id}",
                    owner_node_id=str(self.client.meta.get("node_id") or "")
                    or None,
                )
                self.tasks_submitted += 1
                log.info(
                    "MESSAGE_DECRYPTED→SUBMITTED message_id=%s capability=%s",
                    message_id, capability,
                )
                action = "submitted"
            except Exception as exc:  # noqa: BLE001
                log.error(
                    "submit_simple_task failed for message_id=%s: %s",
                    message_id, exc,
                )
                action = "skipped"
        elif direction == "reply" or etype == "result":
            # A forwarded task's result came back → complete our stage.
            remote_task_id = str(envelope.get("remote_task_id") or "")
            if not remote_task_id:
                log.warning(
                    "reply message_id=%s has no remote_task_id — cannot complete",
                    message_id,
                )
                action = "skipped"
            else:
                with self._lock:
                    task_id = self._forwarded_stages.pop(remote_task_id, "")
                    cur = self._in_flight.get(FEDERATION_CAP, 0)
                    if cur > 0:
                        self._in_flight[FEDERATION_CAP] = cur - 1
                if not task_id:
                    log.warning(
                        "reply message_id=%s remote_task_id=%s not in correlation map",
                        message_id, remote_task_id,
                    )
                    action = "skipped"
                else:
                    try:
                        self.client.complete(task_id, remote_task_id, dict(payload))
                        self.stages_completed += 1
                        log.info(
                            "MESSAGE_COMPLETED message_id=%s stage=%s task=%s",
                            message_id, remote_task_id, task_id,
                        )
                        # T-102: advance the forward message's lifecycle
                        # to COMPLETED (the reply correlates via
                        # remote_task_id == our stage_id).
                        lc = self.lifecycle.get(remote_task_id)
                        if lc is not None:
                            try:
                                lc.completed()
                            except Exception:  # noqa: BLE001
                                pass
                        # Fair-use release (T-099): the forwarded task is
                        # done — free its parallel slot.
                        rel_policy = self.fairuse.for_cap(capability)
                        if rel_policy is not None:
                            rel_policy.release()
                        action = "completed"
                    except Exception as exc:  # noqa: BLE001
                        # Put it back so a later retry can complete.
                        with self._lock:
                            self._forwarded_stages[remote_task_id] = task_id
                            self._in_flight[FEDERATION_CAP] = (
                                self._in_flight.get(FEDERATION_CAP, 0) + 1
                            )
                        log.error(
                            "complete failed for stage=%s: %s",
                            remote_task_id, exc,
                        )
                        action = "skipped"
        else:
            log.warning(
                "inbox message_id=%s unknown direction/type — skipping",
                message_id,
            )
            action = "skipped"

        # T-096: remove once processed. T-102 lifecycle keeps the *sender's*
        # outbox file until ACK; the receiver's inbox file is consumed here.
        try:
            path.unlink()
        except OSError as exc:
            log.warning("could not remove processed inbox file %s: %s", path, exc)
        return action

    def _process_inbox(self, peer: Peer) -> int:
        """Drain one peer's inbox. Returns the number of files processed."""
        inbox, _ = remote_dirs(peer.id)
        if not inbox.is_dir():
            return 0
        count = 0
        for f in sorted(inbox.iterdir()):
            if not f.is_file() or f.name.startswith("."):
                continue
            self._process_inbox_file(peer, f)
            count += 1
        return count

    def _get_transport(self, peer: Peer) -> TransportLike | None:
        """Lazily build + connect a transport for a peer (cached).

        Also builds (and caches) the per-peer :class:`Crypto` via
        ``crypto_factory`` (T-101) and attaches it + the lifecycle
        registry + retry policy to the transport (T-102) so the
        transport's ACK/retry handler can advance state.
        """
        if peer.id in self._transports:
            return self._transports[peer.id]
        if self.transport_factory is None:
            return None
        try:
            t = self.transport_factory(peer)
        except Exception as exc:  # noqa: BLE001
            log.warning("transport for peer %s failed to connect: %s", peer.id, exc)
            return None
        # T-101: build + attach the per-peer Crypto (if a factory is set).
        if peer.id not in self._cryptos:
            if self.crypto_factory is not None:
                try:
                    self._cryptos[peer.id] = self.crypto_factory(peer)
                except Exception as exc:  # noqa: BLE001
                    log.warning("crypto for peer %s failed: %s", peer.id, exc)
                    self._cryptos[peer.id] = None
            else:
                self._cryptos[peer.id] = None
        # Wire T-102 lifecycle into the transport (if it supports it).
        crypto = self._cryptos.get(peer.id)
        for attr, value in (
            ("crypto", crypto),
            ("registry", self.lifecycle),
            ("retry_policy", self.retry_policy),
        ):
            if hasattr(t, attr):
                setattr(t, attr, value)
        self._transports[peer.id] = t
        return t

    def _sync_one_peer(self, peer: Peer) -> dict[str, list[str]]:
        """Run one sync cycle for a peer: outbox → inbox.

        Returns ``{"sent": [...], "received": [...]}``. T-102: the
        transport drives the ACK/retry lifecycle internally; here we
        advance any tracked messages from ``QUEUED`` → ``SENT`` for the
        ids the transport reports as accepted, and move async ones to
        ``PENDING_REMOTE_RESPONSE``.
        """
        inbox, outbox = remote_dirs(peer.id)
        outbox.mkdir(parents=True, exist_ok=True)
        inbox.mkdir(parents=True, exist_ok=True)
        transport = self._get_transport(peer)
        if transport is None:
            return {"sent": [], "received": []}
        sent = transport.sync_outbox(outbox)
        received = transport.sync_inbox(inbox)
        # Advance lifecycle for accepted sends: the transport's ACK handler
        # already moved them through ACKED→COMPLETED→ARCHIVED, so we only
        # need to catch messages the transport could not track (no
        # registry wiring on a fake transport).
        for mid in sent:
            lc = self.lifecycle.get(mid)
            if lc is not None and lc.state is MessageState.QUEUED:
                try:
                    lc.sent()
                except Exception:  # noqa: BLE001
                    pass
        return {"sent": list(sent), "received": list(received)}

    # ------------------------------------------------------------------ #
    # Status
    # ------------------------------------------------------------------ #

    def _write_status(self, error: str | None = None) -> None:
        status_path = self.base_dir / "federation_status.json"
        backoff = 0.0
        try:
            backoff = float(self.client._current_backoff())
        except Exception:  # noqa: BLE001
            backoff = 0.0
        status = {
            "pid": os.getpid(),
            "node_id": self.client.meta.get("node_id"),
            "daemon": "federation-daemon",
            "started_at": self._started_at.isoformat(),
            "last_heartbeat": _utcnow_str(),
            "heartbeat_status": self.last_heartbeat_status,
            "forward_caps": list(self.forward_caps),
            "peers": [p.id for p in self.fed_config.peers],
            "in_flight": dict(self._in_flight),
            "tasks_forwarded": self.tasks_forwarded,
            "tasks_submitted": self.tasks_submitted,
            "stages_completed": self.stages_completed,
            "pending_replies": len(self._forwarded_stages),
            "lifecycle": {
                "tracked": len(self.lifecycle),
                "queued": self.lifecycle.count_in_state(MessageState.QUEUED),
                "sent": self.lifecycle.count_in_state(MessageState.SENT),
                "pending_remote_response": self.lifecycle.count_in_state(
                    MessageState.PENDING_REMOTE_RESPONSE
                ),
                "acked": self.lifecycle.count_in_state(MessageState.ACKED),
                "completed": self.lifecycle.count_in_state(MessageState.COMPLETED),
                "archived": self.lifecycle.count_in_state(MessageState.ARCHIVED),
                "failed": self.lifecycle.count_in_state(MessageState.FAILED),
                "rejected": self.lifecycle.count_in_state(MessageState.REJECTED),
                "expired": self.lifecycle.count_in_state(MessageState.EXPIRED),
            },
            "replay_cache_size": len(self.replay_guard),
            "error": error,
            "auth_backoff_seconds": backoff,
        }
        try:
            _atomic_write_json(status_path, status)
        except OSError as exc:
            log.warning("could not write status file: %s", exc)

    # ------------------------------------------------------------------ #
    # Threads
    # ------------------------------------------------------------------ #

    def _heartbeat_loop(self) -> None:
        """Thread 1: heartbeat ``federation`` + approved forward caps."""
        caps = [{"name": FEDERATION_CAP, "claimable": True}] + [
            {"name": c, "claimable": True} for c in self.forward_caps
        ]
        while not self._stop_event.is_set():
            error: str | None = None
            try:
                self.client.maybe_refresh_token()
                with self._lock:
                    inflight = dict(self._in_flight)
                hb = self.client.heartbeat(caps, inflight)
                self.last_heartbeat_status = str(hb.get("status", "ok"))
                log.info("heartbeat %s", self.last_heartbeat_status)
            except Exception as exc:  # noqa: BLE001 — daemon must survive
                error = str(exc)
                log.error("heartbeat error: %s", exc)
            self._write_status(error=error)
            self._sleep(self.heartbeat_interval + backoff_of(self.client))

    def _claim_loop(self) -> None:
        """Thread 2: claim stages for approved caps → forward to outbox."""
        while not self._stop_event.is_set():
            for cap in list(self.forward_caps):
                if self._stop_event.is_set():
                    break
                self._claim_and_forward(cap)
            self._sleep(self.claim_interval)

    def _sync_loop(self) -> None:
        """Thread 3: per-peer sync cycle (outbox → inbox)."""
        while not self._stop_event.is_set():
            for peer in self.fed_config.peers:
                if self._stop_event.is_set():
                    break
                try:
                    self._sync_one_peer(peer)
                except Exception as exc:  # noqa: BLE001
                    log.error("sync cycle for %s failed: %s", peer.id, exc)
            self._sleep(self.sync_interval)

    def _inbox_loop(self) -> None:
        """Thread 4: drain each peer's inbox (forward→submit, reply→complete)."""
        while not self._stop_event.is_set():
            for peer in self.fed_config.peers:
                if self._stop_event.is_set():
                    break
                try:
                    self._process_inbox(peer)
                except Exception as exc:  # noqa: BLE001
                    log.error("inbox processing for %s failed: %s", peer.id, exc)
            self._sleep(self.inbox_interval)

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    def _sleep(self, seconds: float) -> None:
        """Sleep that stays responsive to ``_stop_event``."""
        deadline = time.monotonic() + max(0.0, seconds)
        while not self._stop_event.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            time.sleep(min(0.2, remaining))

    def _install_signal_handlers(self) -> None:
        # signal.signal() only works in the main thread of the main
        # interpreter. When the daemon is driven from a worker thread (e.g.
        # the smoke test) we skip installation — shutdown is still possible
        # via ``stop()``.
        if threading.current_thread() is not threading.main_thread():
            return
        signal.signal(signal.SIGTERM, self._on_term)
        signal.signal(signal.SIGINT, self._on_term)

    def _on_term(self, *_: Any) -> None:
        log.info("received shutdown signal, stopping federation-daemon …")
        self._stop_event.set()

    def run(self) -> None:
        """Start the four threads and block until shutdown."""
        self._install_signal_handlers()
        log.info(
            "federation-daemon starting (node=%s base_url=%s peers=%s forward_caps=%s)",
            self.client.meta.get("node_id"),
            getattr(self.client, "base_url", ""),
            [p.id for p in self.fed_config.peers],
            self.forward_caps,
        )
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._write_status()

        for name, target in (
            ("heartbeat", self._heartbeat_loop),
            ("claim", self._claim_loop),
            ("sync", self._sync_loop),
            ("inbox", self._inbox_loop),
        ):
            t = threading.Thread(target=target, daemon=True, name=name)
            t.start()
            self._threads.append(t)

        try:
            while not self._stop_event.is_set():
                time.sleep(0.5)
        finally:
            self._stop_event.set()
            for t in self._threads:
                if t.is_alive():
                    t.join(timeout=_SHUTDOWN_GRACE)
            # Close any connected transports.
            for tp in self._transports.values():
                try:
                    tp.close()
                except Exception:  # noqa: BLE001
                    pass
            self._write_status()
            log.info("federation-daemon stopped")

    def stop(self) -> None:
        """Request a graceful shutdown from outside the process."""
        self._stop_event.set()


def backoff_of(client: RelayClientLike) -> float:
    """Best-effort read of the relay client's auth backoff (0 if unknown)."""
    try:
        return float(client._current_backoff())
    except Exception:  # noqa: BLE001
        return 0.0


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="federation-daemon",
        description="IOWAP Federation Node daemon — bridges capabilities between relays.",
    )
    parser.add_argument(
        "--log-level",
        default=None,
        help="Log level (DEBUG/INFO/WARNING/ERROR). Default: env RELAY_LOG_LEVEL or INFO.",
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Path to node.yaml (default: ~/.relay/node.yaml).",
    )
    return parser


def _setup_logging(level: str | None) -> None:
    lvl = level or os.environ.get("RELAY_LOG_LEVEL", "INFO")
    numeric = getattr(logging, lvl.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def _default_forward_caps(fed_config: NodeConfig) -> list[str]:
    """Derive the forward capability list from the node config.

    The ``federation.forward_caps`` list in node.yaml is the explicit source;
    falling back to the capability names of the local ``offer`` (the caps
    this node bridges to peers). Tests pass an explicit list directly.
    """
    # ``load_node_config_from_dict`` does not currently parse ``forward_caps``;
    # read it from the offer capability names as a sensible default.
    caps: list[str] = []
    for entry in fed_config.offer:
        name = entry.get("name") or entry.get("capability")
        if name and name not in caps:
            caps.append(str(name))
    return caps


def _build_transport_factory(cfg: dict[str, Any]) -> TransportFactory:
    """Build a real transport factory (HttpTransport) from relay cfg."""
    from federation.transports.http_transport import HttpTransport

    def factory(peer: Peer) -> TransportLike:
        t = HttpTransport(cfg)
        t.connect(peer.endpoint, peer.token)
        return t  # type: ignore[return-value]

    return factory


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    _setup_logging(args.log_level)
    try:
        from nodes.common.node_utils import load_meta
        from nodes.common.relay_client import RelayClient, _effective_config

        meta = load_meta()
        cfg = _effective_config()
        client: RelayClientLike = RelayClient(meta, cfg)  # type: ignore[assignment]
    except Exception as exc:  # noqa: BLE001
        print(f"federation-daemon: failed to initialise relay client: {exc}",
              file=sys.stderr)
        return 1

    fed_config = load_node_config(args.config)
    if not fed_config.peers:
        print("federation-daemon: no peers configured in node.yaml (federation.peers)",
              file=sys.stderr)
        return 1

    forward_caps = _default_forward_caps(fed_config)
    daemon = FederationDaemon(
        client,
        cfg,
        fed_config=fed_config,
        forward_caps=forward_caps,
        transport_factory=_build_transport_factory(cfg),
    )
    daemon.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())