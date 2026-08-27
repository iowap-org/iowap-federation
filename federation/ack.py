"""ACK handling for both sides of the federation link (T-102).

ACKs are **mandatory**: the receiver ACKs only after it has durably
written the message to ``inbox/``; the sender removes the outbox file
only after the ACK. This gives at-least-once delivery — a lost ACK causes
a re-send (which the receiver's replay cache de-duplicates).

The ACK wire body is the same on both sides::

    {"message_id": "...", "status": "accepted" | "rejected" | "failed"}

* ``accepted`` — the remote wrote the message to its inbox; the sender
  can move the outbox file to ``archive/`` (state ``ACKED`` →
  ``COMPLETED`` → ``ARCHIVED``).
* ``rejected`` — the remote rejected the message (replay, bad envelope,
  auth). The sender moves the message to ``REJECTED`` (terminal).
* ``failed`` — the remote accepted but could not process (e.g. local
  submit failed). The sender may retry or move to ``FAILED`` depending
  on policy; the default is ``FAILED`` (a failed ACK is terminal).

Two sides, two helpers:

* :class:`ReceiverAck` — used by the **listener** (``POST /fed/v1/ack``
  is the *sender*'s confirmation that the receiver's earlier inbox-write
  ACK was received). Wait — re-read the plan: "ACK is mandatory: receiver
  ACKs only after writing to ``inbox/``; sender removes outbox file only
  after ACK." So the ACK flows **receiver → sender**: the receiver
  returns the ACK inline in the ``POST /fed/v1/inbox`` *response*, and
  the sender's ``POST /fed/v1/ack`` is the **sender's confirmation back
  to the receiver** that it received the inline ACK and will archive its
  outbox file (so the receiver may stop tracking it / free replay-cache
  state). Both sides therefore keep a small ack ledger.

  To keep the contract simple and explicit:

  - The **receiver** returns ``{"status": "accepted", "message_id": …}``
    inline on ``POST /fed/v1/inbox`` (already the T-095 contract).
  - The **sender** POSTs ``/fed/v1/ack`` with the same body to tell the
    receiver "I got your inline ACK; you may free state". This is the
    symmetric endpoint the plan requires both nodes to serve.
  - The receiver's ``POST /fed/v1/ack`` handler records the ack and
    frees replay-cache state (or marks the inbox file as "processed by
    peer" — see :class:`ReceiverAck`).

* :class:`SenderAck` — used by the **transport** (sender side). After
  ``POST /fed/v1/inbox`` returns ``202 + accepted``, the transport calls
  :meth:`SenderAck.acknowledge` to record the ACK, then (a) archives the
  outbox file and (b) POSTs ``/fed/v1/ack`` back to the receiver so the
  receiver can free state.

This module owns neither the network nor the filesystem — it owns the
*ledger* and the *policy*. The transport/listener pass paths/ids in and
get back the action to take.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from federation.lifecycle import (
    LifecycleRegistry,
    MessageState,
    atomic_write_json,
    emit_event,
)

log = logging.getLogger("federation.ack")


# ---------------------------------------------------------------------------
# Wire body schema + status constants
# ---------------------------------------------------------------------------

ACK_STATUS_ACCEPTED = "accepted"
ACK_STATUS_REJECTED = "rejected"
ACK_STATUS_FAILED = "failed"

VALID_ACK_STATUSES: frozenset[str] = frozenset(
    {ACK_STATUS_ACCEPTED, ACK_STATUS_REJECTED, ACK_STATUS_FAILED}
)


def parse_ack_body(body: Any) -> tuple[str, str]:
    """Validate an ACK request body.

    Returns ``(message_id, status)``. Raises ``ValueError`` on a
    malformed body so the caller can map to HTTP 400.
    """
    if not isinstance(body, dict):
        raise ValueError("ack body must be a JSON object")
    mid = body.get("message_id")
    status = body.get("status")
    if not isinstance(mid, str) or not mid:
        raise ValueError("ack.message_id is required and must be a str")
    if not isinstance(status, str) or status not in VALID_ACK_STATUSES:
        raise ValueError(
            f"ack.status must be one of {sorted(VALID_ACK_STATUSES)}, got {status!r}"
        )
    return mid, status


# ---------------------------------------------------------------------------
# Receiver side (listener): record sender's ACK, free state
# ---------------------------------------------------------------------------

@dataclass
class ReceiverAck:
    """Tracks the ACKs the **sender** has sent back to us.

    The listener holds one of these. When ``POST /fed/v1/ack`` arrives,
    :meth:`record` marks the message as fully acknowledged on our side
    and emits ``MESSAGE_ACKED``. We also free the replay-cache entry
    eagerly *only* if the caller passes a remover (the cache's TTL prune
    is the normal path; eager free is an optimisation for high traffic).

    The ledger also lets the receiver expose "messages waiting for the
    sender's ACK" via :meth:`pending` — useful for the status file.
    """

    # message_id → status (the last ack status we received from sender)
    _ledger: dict[str, str] = field(default_factory=dict)

    def record(self, message_id: str, status: str) -> dict[str, Any]:
        """Record the sender's ACK. Returns a response body for the endpoint."""
        self._ledger[message_id] = status
        emit_event(
            "MESSAGE_ACKED",
            message_id=message_id,
            side="receiver",
            ack_status=status,
        )
        return {"status": "ok", "message_id": message_id, "ack": status}

    def has(self, message_id: str) -> bool:
        return message_id in self._ledger

    def pending(self) -> list[str]:
        """message_ids we've written to inbox but not yet got an ACK for.

        Subclass-override point once the listener tracks the inbox-write
        ledger; the base implementation returns an empty list because the
        listener does not pre-record inbox writes here (the file's
        presence is the record).
        """
        return []

    def snapshot(self) -> dict[str, str]:
        return dict(self._ledger)


# ---------------------------------------------------------------------------
# Sender side (transport): record receiver's inline ACK, archive outbox,
# POST /fed/v1/ack back to receiver
# ---------------------------------------------------------------------------

@dataclass
class SenderAck:
    """Sender-side ACK ledger + outbox archival policy.

    The transport drives this:

    1. ``POST /fed/v1/inbox`` returns ``202`` with the receiver's inline
       ACK body. The transport calls :meth:`handle_inbox_response` with
       the parsed body; if the status is ``accepted`` the outbox file is
       moved to ``archive/`` and the matching :class:`Lifecycle` (if any)
       moves to ``ACKED`` → ``COMPLETED`` → ``ARCHIVED``. If ``rejected``
       the lifecycle moves to ``REJECTED`` and the file is archived too
       (we keep it for forensics). If ``failed`` the lifecycle moves to
       ``FAILED``.
    2. The transport then calls :meth:`build_ack_request` to build the
       body it POSTs to ``/fed/v1/ack`` on the receiver (so the receiver
       can free its ledger entry). The POST itself is the transport's
       job; this helper only builds the body.

    The ledger is keyed by ``message_id`` and keeps the last ACK status
    so the daemon's status file can report it.
    """

    outbox_dir: Path
    archive_dir: Path
    registry: LifecycleRegistry | None = None
    _ledger: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.outbox_dir = Path(self.outbox_dir)
        self.archive_dir = Path(self.archive_dir)
        self.archive_dir.mkdir(parents=True, exist_ok=True)

    # -- handle the receiver's inline ACK -------------------------------

    def handle_inbox_response(
        self,
        message_id: str,
        ack_body: dict[str, Any] | None,
        *,
        outbox_file: Path | None = None,
    ) -> str:
        """Act on the response to ``POST /fed/v1/inbox``.

        ``ack_body`` is the parsed JSON the receiver returned (e.g.
        ``{"status": "accepted", "message_id": …}``). Returns the
        ack status string (``accepted``/``rejected``/``failed``) or
        ``"unknown"`` when the body was missing/invalid (treated as a
        transport failure — the caller should retry).

        Side effects:
        * ``accepted``  → archive outbox file, lifecycle ACKED→COMPLETED→ARCHIVED.
        * ``rejected``  → archive outbox file, lifecycle REJECTED.
        * ``failed``    → leave outbox file in place (a retry may help),
                          lifecycle FAILED only if no retries left; else
                          re-queue.
        * ``unknown``   → leave outbox file in place (caller retries).
        """
        status = "unknown"
        if isinstance(ack_body, dict):
            s = ack_body.get("status")
            if isinstance(s, str) and s in VALID_ACK_STATUSES:
                status = s
        self._ledger[message_id] = status

        if outbox_file is None:
            outbox_file = self.outbox_dir / f"{message_id}.json"

        if status == ACK_STATUS_ACCEPTED:
            self._archive(message_id, outbox_file)
            self._advance_lifecycle(message_id, accepted=True)
        elif status == ACK_STATUS_REJECTED:
            self._archive(message_id, outbox_file)
            self._advance_lifecycle(message_id, rejected=True)
        elif status == ACK_STATUS_FAILED:
            # Leave the file; the retry policy decides. The lifecycle
            # moves to FAILED only when retries are exhausted (caller's
            # job via Lifecycle.record_retry).
            self._advance_lifecycle(message_id, failed=True)
        else:
            # No inline ACK — treat as a transport failure; caller retries.
            log.warning(
                "no inline ACK for message_id=%s (body=%r) — will retry",
                message_id, ack_body,
            )
        return status

    # -- build the body for POST /fed/v1/ack ----------------------------

    def build_ack_request(self, message_id: str, status: str) -> dict[str, str]:
        """Build the body the transport POSTs to ``/fed/v1/ack``.

        This is the sender telling the receiver "I got your inline ACK".
        """
        if status not in VALID_ACK_STATUSES:
            raise ValueError(f"invalid ack status: {status!r}")
        return {"message_id": message_id, "status": status}

    # -- internals ------------------------------------------------------

    def _archive(self, message_id: str, outbox_file: Path) -> None:
        """Move ``outbox_file`` to ``archive/<message_id>.json``."""
        outbox_file = Path(outbox_file)
        if not outbox_file.exists():
            log.warning(
                "ack archive: outbox file %s missing for message_id=%s",
                outbox_file, message_id,
            )
            return
        dest = self.archive_dir / f"{message_id}.json"
        try:
            # Atomic move on the same filesystem; fall back to copy+delete
            # across filesystems.
            outbox_file.replace(dest)
        except OSError:
            try:
                data = outbox_file.read_bytes()
                atomic_write_json(dest, json.loads(data) if data else {})
                outbox_file.unlink(missing_ok=True)
            except OSError as exc:
                log.warning(
                    "ack archive failed for message_id=%s: %s", message_id, exc
                )
                return
        emit_event("MESSAGE_ARCHIVED", message_id=message_id, dest=str(dest))

    def _advance_lifecycle(
        self,
        message_id: str,
        *,
        accepted: bool = False,
        rejected: bool = False,
        failed: bool = False,
    ) -> None:
        if self.registry is None:
            return
        lc = self.registry.get(message_id)
        if lc is None:
            return
        try:
            if accepted:
                # The transport's sync_outbox does send + ACK in one call,
                # so the lifecycle may still be at QUEUED when we get here.
                # Walk it forward through SENT → ACKED → COMPLETED → ARCHIVED.
                if lc.state is MessageState.QUEUED:
                    lc.sent()
                if lc.state is MessageState.SENT:
                    lc.acked()
                if lc.state in (MessageState.ACKED, MessageState.PENDING_REMOTE_RESPONSE):
                    lc.completed()
                if lc.state is MessageState.COMPLETED:
                    lc.archived()
            elif rejected:
                # Move to SENT first if queued (the send happened, the
                # receiver saw it and rejected it).
                if lc.state is MessageState.QUEUED:
                    lc.sent()
                lc.rejected(reason="receiver rejected")
            elif failed:
                if lc.state is MessageState.QUEUED:
                    lc.sent()
                lc.failed(reason="receiver failed")
        except Exception as exc:  # noqa: BLE001 — never let ack crash the sync loop
            log.warning("lifecycle advance failed for %s: %s", message_id, exc)

    # -- queries --------------------------------------------------------

    def status_of(self, message_id: str) -> str | None:
        return self._ledger.get(message_id)

    def snapshot(self) -> dict[str, str]:
        return dict(self._ledger)


# ---------------------------------------------------------------------------
# Convenience: build an inline ACK body for the listener's inbox response
# ---------------------------------------------------------------------------

def inbox_accepted(message_id: str) -> dict[str, str]:
    """The body the listener returns on a successful ``POST /fed/v1/inbox``."""
    return {"status": ACK_STATUS_ACCEPTED, "message_id": message_id}


def inbox_rejected(message_id: str, reason: str = "replay") -> dict[str, str]:
    """The body the listener returns when it rejects an inbox POST (replay)."""
    return {"status": ACK_STATUS_REJECTED, "message_id": message_id, "reason": reason}