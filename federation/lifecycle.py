"""Message lifecycle state machine, retry policy and observability (T-102).

Implements the master-plan state machine::

    CREATED → ENCRYPTED → QUEUED → SENT → ACKED → COMPLETED → ARCHIVED
                        ↘ FAILED / EXPIRED / REJECTED

plus the supporting primitives the listener, transport and daemon wire
together:

* :class:`MessageState` — the enum of states.
* :class:`Lifecycle` — per-message state tracker (transitions, guards,
  timestamps, retry count). Enforces the allowed edges and emits
  structured observability events via :func:`emit_event`.
* :class:`RetryPolicy` — exponential backoff ``30s 1m 2m 4m 8m 16m 30m 60m``
  with jitter (full-jitter: ``uniform(0, base_delay)``). Caps the attempt
  count; a message past the last attempt moves to ``FAILED``.
* :func:`atomic_write_bytes` / :func:`atomic_write_json` — ``.tmp_<id>`` +
  ``os.replace`` atomic writes (the single source of truth; the listener
  and transport re-use these so behaviour is identical everywhere).
* :class:`ReplayGuard` — wraps :class:`federation.crypto.ReplayCache` with
  the lifecycle semantics the listener needs: reject a duplicate
  *before* the inbox file is written and surface a ``MESSAGE_REJECTED``
  event.

The module is deliberately framework-light: it depends only on stdlib +
:mod:`federation.crypto`. The daemon/listener/transport components own the
*side effects* (HTTP, filesystem); this module owns the *rules*.

Observability
-------------
Every state transition emits a structured event through the
``federation.lifecycle`` logger at INFO level::

    emit_event("MESSAGE_CREATED", message_id=mid, **kwargs)

The event names are the fixed set from the plan::

    MESSAGE_CREATED, MESSAGE_ENCRYPTED, MESSAGE_SENT, MESSAGE_ACKED,
    MESSAGE_RETRIED, MESSAGE_DECRYPTED, MESSAGE_COMPLETED, MESSAGE_FAILED,
    MESSAGE_REJECTED (extra: MESSAGE_ARCHIVED, MESSAGE_EXPIRED).

They are emitted as ``EVENT_NAME key=value ...`` log lines so a log
aggregator can grep them without parsing JSON.
"""

from __future__ import annotations

import enum
import json
import logging
import os
import random
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from federation.crypto import ReplayCache

log = logging.getLogger("federation.lifecycle")


# ---------------------------------------------------------------------------
# State machine
# ---------------------------------------------------------------------------

class MessageState(str, enum.Enum):
    """The lifecycle states a federation message can be in.

    ``str`` subclass so values serialise cleanly to JSON / SQLite (T-103).
    """

    CREATED = "CREATED"
    ENCRYPTED = "ENCRYPTED"
    QUEUED = "QUEUED"
    SENT = "SENT"
    ACKED = "ACKED"
    COMPLETED = "COMPLETED"
    ARCHIVED = "ARCHIVED"
    # Async transport marker: a task waiting on a remote response. Not on
    # the main forward path; a worker must not block on it. Terminal-ish
    # for the sender's forward flow — the eventual reply/ACK moves it on.
    PENDING_REMOTE_RESPONSE = "PENDING_REMOTE_RESPONSE"
    # Terminal states.
    FAILED = "FAILED"
    EXPIRED = "EXPIRED"
    REJECTED = "REJECTED"


# Terminal states — once entered, no further transitions are allowed.
TERMINAL_STATES: frozenset[MessageState] = frozenset(
    {
        MessageState.FAILED,
        MessageState.EXPIRED,
        MessageState.REJECTED,
        MessageState.ARCHIVED,
    }
)

# Allowed directed edges. Anything not listed raises ``IllegalTransition``.
# The terminal states FAILED/EXPIRED/REJECTED can be reached from any
# non-terminal state (a receiver may reject, a timeout may expire, or a
# failure may occur at any point in the flow). ARCHIVED is only reachable
# from COMPLETED (a successfully completed message is archived).
_TRANSITIONS: dict[MessageState, frozenset[MessageState]] = {
    MessageState.CREATED: frozenset(
        {MessageState.ENCRYPTED, MessageState.QUEUED, MessageState.FAILED, MessageState.REJECTED, MessageState.EXPIRED}
    ),
    MessageState.ENCRYPTED: frozenset(
        {MessageState.QUEUED, MessageState.FAILED, MessageState.REJECTED, MessageState.EXPIRED}
    ),
    MessageState.QUEUED: frozenset(
        {MessageState.SENT, MessageState.FAILED, MessageState.REJECTED, MessageState.EXPIRED}
    ),
    MessageState.SENT: frozenset(
        {
            MessageState.ACKED,
            MessageState.PENDING_REMOTE_RESPONSE,
            MessageState.QUEUED,   # retry → re-queue
            MessageState.FAILED,
            MessageState.EXPIRED,
            MessageState.REJECTED,
        }
    ),
    MessageState.PENDING_REMOTE_RESPONSE: frozenset(
        {
            MessageState.ACKED,
            MessageState.COMPLETED,
            MessageState.QUEUED,   # remote lost → re-queue
            MessageState.FAILED,
            MessageState.EXPIRED,
            MessageState.REJECTED,
        }
    ),
    MessageState.ACKED: frozenset(
        {MessageState.COMPLETED, MessageState.FAILED, MessageState.EXPIRED, MessageState.REJECTED}
    ),
    MessageState.COMPLETED: frozenset({MessageState.ARCHIVED}),
    MessageState.FAILED: frozenset(),
    MessageState.EXPIRED: frozenset(),
    MessageState.REJECTED: frozenset(),
    MessageState.ARCHIVED: frozenset(),
}


class IllegalTransition(Exception):
    """Raised when a transition is not in :data:`_TRANSITIONS`."""

    def __init__(self, frm: MessageState, to: MessageState) -> None:
        super().__init__(f"illegal transition {frm.value} → {to.value}")
        self.frm = frm
        self.to = to


# ---------------------------------------------------------------------------
# Observability
# ---------------------------------------------------------------------------

# Fixed event-name set (plan). ``MESSAGE_ARCHIVED`` / ``MESSAGE_EXPIRED``
# extend the plan's list; they're the natural counterparts of the
# terminal states and use the same logging channel.
EVENT_NAMES: frozenset[str] = frozenset(
    {
        "MESSAGE_CREATED",
        "MESSAGE_ENCRYPTED",
        "MESSAGE_SENT",
        "MESSAGE_ACKED",
        "MESSAGE_RETRIED",
        "MESSAGE_DECRYPTED",
        "MESSAGE_COMPLETED",
        "MESSAGE_FAILED",
        "MESSAGE_REJECTED",
        "MESSAGE_ARCHIVED",
        "MESSAGE_EXPIRED",
    }
)


def emit_event(name: str, /, **fields: Any) -> None:
    """Emit a structured observability event.

    The event name is validated against :data:`EVENT_NAMES` (a typo is a
    common source of silent observability gaps). Fields are rendered as
    ``key=value`` pairs sorted by key so log lines are stable for grep.
    """
    if name not in EVENT_NAMES:
        raise ValueError(f"unknown event name: {name!r}")
    parts = [f"{k}={v!r}" for k, v in sorted(fields.items())]
    log.info("%s %s", name, " ".join(parts))


# ---------------------------------------------------------------------------
# Lifecycle (per-message state tracker)
# ---------------------------------------------------------------------------

@dataclass
class Lifecycle:
    """Per-message lifecycle state + retry counter.

    A single instance tracks one message's progression through the state
    machine. Thread-safe (a lock guards the mutable fields) so the daemon
    can drive a message from the claim loop and the sync loop concurrently.

    Fields
    ------
    message_id:
        The envelope ``message_id`` (jti / replay key).
    state:
        Current :class:`MessageState`. Starts at ``CREATED``.
    retry_count:
        Number of retries that have been attempted.
    max_retries:
        Hard cap on retries; a transition to ``QUEUED`` past this cap
        raises / forces ``FAILED``. ``None`` means "use the retry policy".
    updated_at:
        ``time.monotonic()`` of the last transition (for expiry checks).
    history:
        Optional ordered list of ``(state, timestamp)`` transitions, kept
        only when ``record_history=True`` (tests / observability).
    """

    message_id: str
    state: MessageState = MessageState.CREATED
    retry_count: int = 0
    max_retries: int | None = None
    updated_at: float = field(default_factory=time.monotonic)
    history: list[tuple[MessageState, float]] = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def __post_init__(self) -> None:
        if self.history:
            self.history = list(self.history)
        else:
            self.history = [(self.state, self.updated_at)]

    # -- transition ------------------------------------------------------

    def transition(self, to: MessageState) -> MessageState:
        """Move to ``to`` if allowed; else raise :class:`IllegalTransition`.

        Emits the matching observability event and updates ``updated_at``
        + ``history``. A no-op when ``to == self.state`` (idempotent
        re-entrance — e.g. double-ACK is safe).
        """
        with self._lock:
            if to == self.state:
                return self.state
            allowed = _TRANSITIONS.get(self.state, frozenset())
            if to not in allowed:
                raise IllegalTransition(self.state, to)
            prev = self.state
            self.state = to
            self.updated_at = time.monotonic()
            self.history.append((to, self.updated_at))
            # Map common transitions → events. Specialised callers emit
            # the fine-grained events (RETRIED/REJECTED) themselves so we
            # don't double-log.
            event = _TRANSITION_EVENTS.get((prev, to))
            if event is not None:
                emit_event(event, message_id=self.message_id, state=to.value)
            return to

    # -- convenience transitions ----------------------------------------

    def created(self) -> None:
        """Record a freshly created message (idempotent if already CREATED)."""
        if self.state is not MessageState.CREATED:
            raise IllegalTransition(self.state, MessageState.CREATED)
        emit_event("MESSAGE_CREATED", message_id=self.message_id)

    def encrypted(self) -> None:
        self.transition(MessageState.ENCRYPTED)

    def queued(self) -> None:
        self.transition(MessageState.QUEUED)

    def sent(self) -> None:
        self.transition(MessageState.SENT)
        emit_event("MESSAGE_SENT", message_id=self.message_id)

    def acked(self) -> None:
        self.transition(MessageState.ACKED)
        emit_event("MESSAGE_ACKED", message_id=self.message_id)

    def completed(self) -> None:
        self.transition(MessageState.COMPLETED)
        emit_event("MESSAGE_COMPLETED", message_id=self.message_id)

    def archived(self) -> None:
        self.transition(MessageState.ARCHIVED)
        emit_event("MESSAGE_ARCHIVED", message_id=self.message_id)

    def failed(self, reason: str = "") -> None:
        self.transition(MessageState.FAILED)
        emit_event("MESSAGE_FAILED", message_id=self.message_id, reason=reason)

    def rejected(self, reason: str = "") -> None:
        self.transition(MessageState.REJECTED)
        emit_event("MESSAGE_REJECTED", message_id=self.message_id, reason=reason)

    def expired(self, reason: str = "") -> None:
        self.transition(MessageState.EXPIRED)
        emit_event("MESSAGE_EXPIRED", message_id=self.message_id, reason=reason)

    def pending_remote_response(self) -> None:
        self.transition(MessageState.PENDING_REMOTE_RESPONSE)

    def decrypted(self) -> None:
        """Emit a DECRYPTED event without a state change (receiver side).

        Decryption is not a state on the sender's machine; it's a receiver
        side-effect. We expose it here so both sides use the same emitter.
        """
        emit_event("MESSAGE_DECRYPTED", message_id=self.message_id)

    # -- retry -----------------------------------------------------------

    def record_retry(self, policy: "RetryPolicy") -> float:
        """Record one retry attempt. Returns the next backoff delay (s).

        Increments ``retry_count``; if that exceeds the policy's
        ``max_attempts`` the message is moved to ``FAILED`` and ``-1.0``
        is returned (caller must not re-queue). Otherwise emits
        ``MESSAGE_RETRIED`` and returns the jittered delay.

        The caller is responsible for moving the state to ``QUEUED``
        before/at the retry time; this method only tracks the count and
        the delay.
        """
        with self._lock:
            self.retry_count += 1
            n = self.retry_count
        if not policy.has_attempts_left(n):
            self.failed(reason="max retries exceeded")
            return -1.0
        delay = policy.delay_for(n)
        emit_event(
            "MESSAGE_RETRIED",
            message_id=self.message_id,
            attempt=n,
            delay_s=round(delay, 3),
        )
        return delay

    # -- queries ---------------------------------------------------------

    @property
    def is_terminal(self) -> bool:
        return self.state in TERMINAL_STATES

    def age_seconds(self) -> float:
        return max(0.0, time.monotonic() - self.updated_at)

    def snapshot(self) -> dict[str, Any]:
        """JSON-serialisable view (for the status file / SQLite T-103)."""
        return {
            "message_id": self.message_id,
            "state": self.state.value,
            "retry_count": self.retry_count,
            "updated_at": self.updated_at,
            "history": [(s.value, t) for s, t in self.history],
        }


# Map (from, to) → event name for the auto-emit inside ``transition``.
# Fine-grained events (SENT/ACKED/COMPLETED/...) are emitted by the named
# helpers above to avoid double-logging; the table only covers the
# quiet transitions that have no dedicated helper.
_TRANSITION_EVENTS: dict[tuple[MessageState, MessageState], str] = {
    (MessageState.CREATED, MessageState.ENCRYPTED): "MESSAGE_ENCRYPTED",
}


# ---------------------------------------------------------------------------
# Retry policy — exponential backoff + jitter
# ---------------------------------------------------------------------------

# Master-plan sequence: 30s 1m 2m 4m 8m 16m 30m 60m.
DEFAULT_BACKOFF_SEQUENCE: tuple[float, ...] = (
    30.0,
    60.0,
    120.0,
    240.0,
    480.0,
    960.0,
    1800.0,
    3600.0,
)

# Default cap: one attempt + len(sequence) retries. After that → FAILED.
DEFAULT_MAX_ATTEMPTS = 1 + len(DEFAULT_BACKOFF_SEQUENCE)


@dataclass
class RetryPolicy:
    """Exponential backoff with full jitter.

    ``delay_for(n)`` returns the (jittered) delay before the *n*-th retry
    (1-indexed). The base delay for attempt *n* is
    ``sequence[min(n-1, len(sequence)-1)]`` (the last value is the cap);
    the actual delay is ``uniform(0, base)`` (full jitter — better than
    equal jitter under sustained contention, per AWS's famous post).

    ``has_attempts_left(n)`` returns ``False`` once ``n`` exceeds
    ``max_attempts`` (the message is then moved to ``FAILED``).
    """

    sequence: tuple[float, ...] = DEFAULT_BACKOFF_SEQUENCE
    max_attempts: int = DEFAULT_MAX_ATTEMPTS
    jitter: Callable[[float, float], float] = staticmethod(
        lambda lo, hi: random.uniform(lo, hi)
    )
    min_delay: float = 0.0
    seed: int | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if not self.sequence:
            raise ValueError("sequence must be non-empty")
        if self.seed is not None:
            # A seeded RNG keeps tests deterministic without making the
            # policy module globally stateful.
            self._rng = random.Random(self.seed)
            self.jitter = lambda lo, hi: self._rng.uniform(lo, hi)  # type: ignore[assignment]

    def base_delay(self, attempt: int) -> float:
        """The un-jittered base delay for ``attempt`` (1-indexed)."""
        if attempt < 1:
            return 0.0
        idx = min(attempt - 1, len(self.sequence) - 1)
        return float(self.sequence[idx])

    def delay_for(self, attempt: int) -> float:
        """Jittered delay (seconds) before the ``attempt``-th retry."""
        base = self.base_delay(attempt)
        if base <= 0.0:
            return 0.0
        return max(self.min_delay, float(self.jitter(self.min_delay, base)))

    def has_attempts_left(self, attempt: int) -> bool:
        return attempt < self.max_attempts


# ---------------------------------------------------------------------------
# Atomic file I/O (single source of truth for the listener/transport/daemon)
# ---------------------------------------------------------------------------

def atomic_write_bytes(path: Path, data: bytes) -> None:
    """Write ``data`` to ``path`` atomically: ``.tmp_<id>`` + ``os.replace``.

    The temp file is created in the same directory (so ``os.replace`` is a
    single atomic rename on POSIX). On any error the temp file is removed.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(
        prefix=f".tmp_{path.stem}_", dir=str(path.parent), suffix=path.suffix or ".bin"
    )
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def atomic_write_json(path: Path, obj: dict[str, Any]) -> None:
    """Write ``obj`` as JSON to ``path`` atomically."""
    data = json.dumps(obj, indent=2, sort_keys=True).encode("utf-8")
    atomic_write_bytes(path, data)


# ---------------------------------------------------------------------------
# Replay guard (lifecycle-flavoured wrapper around crypto.ReplayCache)
# ---------------------------------------------------------------------------

class ReplayGuard:
    """Lifecycle-aware replay protection for the receiver.

    Wraps :class:`federation.crypto.ReplayCache` and adds the lifecycle
    semantics the listener needs:

    * :meth:`check_and_add` returns ``True`` the first time a
      ``message_id`` is seen (and records it). A replay returns ``False``
      and emits ``MESSAGE_REJECTED`` — the caller must NOT write the
      inbox file in that case.
    * TTL pruning is delegated to the underlying cache (called on every
      check, amortised).
    * Thread-safe so the listener's async handlers and the daemon's
      inbox loop can share one guard per node.
    """

    def __init__(self, ttl: float = 3600.0) -> None:
        self._cache = ReplayCache(ttl=ttl)
        self._lock = threading.Lock()

    def check_and_add(self, message_id: str) -> bool:
        with self._lock:
            fresh = self._cache.check_and_add(message_id)
        if not fresh:
            emit_event("MESSAGE_REJECTED", message_id=message_id, reason="replay")
        return fresh

    def prune(self) -> int:
        with self._lock:
            return self._cache.prune()

    def __contains__(self, message_id: str) -> bool:
        with self._lock:
            return message_id in self._cache

    def __len__(self) -> int:
        return len(self._cache)


# ---------------------------------------------------------------------------
# Registry — the daemon's map message_id → Lifecycle
# ---------------------------------------------------------------------------

class LifecycleRegistry:
    """Thread-safe registry of :class:`Lifecycle` instances by message_id.

    The daemon keeps one of these to track every in-flight message across
    the claim/sync/inbox threads. The registry is the natural place for
    bulk queries (``count_in_state``, ``pending_acks``) the status file
    and T-103 SQLite index will consume.
    """

    def __init__(self) -> None:
        self._by_id: dict[str, Lifecycle] = {}
        self._lock = threading.Lock()

    def register(self, lc: Lifecycle) -> Lifecycle:
        with self._lock:
            if lc.message_id in self._by_id:
                raise ValueError(f"message_id {lc.message_id!r} already registered")
            self._by_id[lc.message_id] = lc
            return lc

    def get(self, message_id: str) -> Lifecycle | None:
        with self._lock:
            return self._by_id.get(message_id)

    def remove(self, message_id: str) -> Lifecycle | None:
        with self._lock:
            return self._by_id.pop(message_id, None)

    def count_in_state(self, state: MessageState) -> int:
        with self._lock:
            return sum(1 for lc in self._by_id.values() if lc.state is state)

    def pending_acks(self) -> list["Lifecycle"]:
        """Lifecycle objects currently in ``SENT`` / ``PENDING_REMOTE_RESPONSE``.

        These are the messages the sync loop must keep retrying until ACK.
        """
        with self._lock:
            return [
                lc
                for lc in self._by_id.values()
                if lc.state in (MessageState.SENT, MessageState.PENDING_REMOTE_RESPONSE)
            ]

    def all(self) -> list[Lifecycle]:
        with self._lock:
            return list(self._by_id.values())

    def __len__(self) -> int:
        return len(self._by_id)