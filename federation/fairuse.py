"""Fair-Use Policy enforcement (T-099).

Enforces per-subscription ``max_parallel`` / ``max_daily`` limits in the
Federation Daemon before a stage is claimed and forwarded to a remote peer.

Contracts (from the master plan):

* ``FairUse(capability, max_parallel=2, max_daily=50)``.
* ``can_claim() -> bool`` — respects current in-flight count + today's count.
* ``claim()`` / ``release()`` — track parallel in-flight slots.
* ``check_daily() -> bool`` — daily cap against a daily counter.
* Reject claim when over limit; log a structured ``FAIRUSE_REJECTED`` event.

Persistence: the daily counter is persisted in a small JSON file
``<state_dir>/<capability>/daily/<YYYY-MM-DD>.json`` so a daemon restart does
not reset the daily budget mid-day. T-103 will replace/augment this with the
SQLite metadata index; until then a per-day file is sufficient and cheap.
The in-flight counter is purely in-memory (a crashed in-flight task is
re-claimed on restart, so no persistence needed).

Thread-safety: the daemon calls ``claim``/``release`` from multiple threads
(claim loop vs. inbox/reply loop). All mutating methods hold an internal
``threading.Lock``.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger("federation.fairuse")

# Default limits (plan contracts).
DEFAULT_MAX_PARALLEL = 2
DEFAULT_MAX_DAILY = 50


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _today_key(now: datetime | None = None) -> str:
    """UTC date key used for the daily counter file (``YYYY-MM-DD``).

    UTC is used so the daily budget is independent of the host's timezone —
    a forwarded task is a remote concern, the boundary should be stable.
    """
    return (now or _utcnow()).strftime("%Y-%m-%d")


def _atomic_write_json(path: Path, obj: dict[str, Any]) -> None:
    """Write ``obj`` as JSON to ``path`` atomically: tmp file + ``os.replace``."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(
        prefix=f".tmp_{path.stem}_", dir=str(path.parent), suffix=".json"
    )
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(obj, f)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return None


class FairUse:
    """Enforces ``max_parallel`` and ``max_daily`` for one capability.

    One ``FairUse`` instance per forwarded capability. The daemon keeps a
    ``dict[str, FairUse]`` keyed by capability name and consults it before
    calling ``client.claim(capability)``.

    Parameters
    ----------
    capability:
        The capability name this policy guards (e.g. ``"image.gen@fed-b"``).
    max_parallel:
        Maximum number of concurrently in-flight (claimed, not yet
        completed) forwards. ``0`` means unbounded.
    max_daily:
        Maximum number of forwards per UTC day. ``0`` means unbounded.
    state_dir:
        Root directory for persisted daily counters. Defaults to
        ``~/.relay/federation/fairuse``. Each capability gets its own
        ``<state_dir>/<safe_capability>/daily/<YYYY-MM-DD>.json`` file.
    clock:
        Optional callable returning a ``datetime`` (UTC). Injected in tests
        to advance the day without ``time.sleep``. Defaults to ``_utcnow``.
    """

    def __init__(
        self,
        capability: str,
        *,
        max_parallel: int = DEFAULT_MAX_PARALLEL,
        max_daily: int = DEFAULT_MAX_DAILY,
        state_dir: Path | str | None = None,
        clock: "Any" = None,
    ) -> None:
        if not capability:
            raise ValueError("capability must be non-empty")
        if max_parallel < 0:
            raise ValueError("max_parallel must be >= 0")
        if max_daily < 0:
            raise ValueError("max_daily must be >= 0")
        self.capability = capability
        self.max_parallel = max_parallel
        self.max_daily = max_daily

        if state_dir is None:
            state_dir = Path.home() / ".relay" / "federation" / "fairuse"
        self.state_dir = Path(state_dir)
        # Filesystem-safe capability path component. Keep it readable but
        # replace path separators / characters that confuse directories.
        safe = "".join(c if c.isalnum() or c in "._-@" else "_" for c in capability)
        safe = safe or "_"
        self._cap_dir = self.state_dir / safe / "daily"

        self._clock = clock or _utcnow
        self._lock = threading.Lock()
        self._in_flight = 0
        self._daily_count = 0
        self._daily_key = _today_key(self._clock())
        # Load today's persisted counter (if any) so a restart continues
        # the day's budget rather than resetting it.
        self._daily_count = self._load_daily(self._daily_key)

    # ------------------------------------------------------------------ #
    # Persistence helpers
    # ------------------------------------------------------------------ #

    @property
    def _daily_path(self) -> Path:
        return self._cap_dir / f"{self._daily_key}.json"

    def _load_daily(self, key: str) -> int:
        path = self._cap_dir / f"{key}.json"
        data = _read_json(path)
        if not data:
            return 0
        try:
            return int(data.get("count", 0))
        except (TypeError, ValueError):
            return 0

    def _persist_daily(self, key: str, count: int) -> None:
        path = self._cap_dir / f"{key}.json"
        _atomic_write_json(
            path,
            {
                "capability": self.capability,
                "date": key,
                "count": count,
                "updated_at": self._clock().isoformat(),
            },
        )

    def _rollover_if_needed(self) -> None:
        """Roll the in-memory daily counter + key when the UTC day changes.

        Called under ``self._lock``. On rollover the previous day's file is
        left in place (it is the historical record; T-103 may archive it).
        """
        key = _today_key(self._clock())
        if key != self._daily_key:
            self._daily_key = key
            self._daily_count = self._load_daily(key)

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def check_daily(self) -> bool:
        """Return ``True`` if the daily cap is not yet exhausted.

        ``max_daily == 0`` means unbounded.
        """
        with self._lock:
            self._rollover_if_needed()
            if self.max_daily == 0:
                return True
            return self._daily_count < self.max_daily

    def can_claim(self) -> bool:
        """Return ``True`` iff a new claim fits both the parallel and daily caps.

        Does **not** mutate state — call ``claim()`` to actually reserve a
        slot. Used by the daemon to decide whether to call ``client.claim()``.
        """
        with self._lock:
            self._rollover_if_needed()
            if self.max_daily != 0 and self._daily_count >= self.max_daily:
                return False
            if self.max_parallel != 0 and self._in_flight >= self.max_parallel:
                return False
            return True

    def claim(self) -> bool:
        """Reserve an in-flight + daily slot.

        Returns ``True`` on success, ``False`` if either cap is exhausted. On
        rejection a structured ``FAIRUSE_REJECTED`` event is logged with the
        reason (``parallel`` or ``daily``) and the current counters.

        Idempotency: this is *not* idempotent — each successful ``claim()``
        consumes one daily + one parallel slot. Pair with exactly one
        ``release()`` when the forward completes (or the stage is abandoned).
        """
        with self._lock:
            self._rollover_if_needed()
            reason: str | None = None
            if self.max_daily != 0 and self._daily_count >= self.max_daily:
                reason = "daily"
            elif self.max_parallel != 0 and self._in_flight >= self.max_parallel:
                reason = "parallel"
            if reason is not None:
                log.info(
                    "FAIRUSE_REJECTED capability=%s reason=%s in_flight=%d "
                    "daily_count=%d max_parallel=%d max_daily=%d",
                    self.capability, reason, self._in_flight, self._daily_count,
                    self.max_parallel, self.max_daily,
                )
                return False
            self._in_flight += 1
            self._daily_count += 1
            self._persist_daily(self._daily_key, self._daily_count)
            log.debug(
                "FAIRUSE_CLAIM capability=%s in_flight=%d daily_count=%d",
                self.capability, self._in_flight, self._daily_count,
            )
            return True

    def release(self) -> int:
        """Release one in-flight slot (a forward completed or was abandoned).

        Decrements the in-flight counter, never below 0. Returns the new
        in-flight count. Does **not** refund the daily counter — a forwarded
        task still counts toward the day's budget even if it failed fast.

        Safe to call when no slot is held (clamped at 0).
        """
        with self._lock:
            if self._in_flight > 0:
                self._in_flight -= 1
            log.debug(
                "FAIRUSE_RELEASE capability=%s in_flight=%d",
                self.capability, self._in_flight,
            )
            return self._in_flight

    # ------------------------------------------------------------------ #
    # Introspection (test-friendly, no lock needed for reads that tolerate
    # a small race; tests use them after joining the relevant work)
    # ------------------------------------------------------------------ #

    @property
    def in_flight(self) -> int:
        return self._in_flight

    @property
    def daily_count(self) -> int:
        with self._lock:
            self._rollover_if_needed()
            return self._daily_count

    @property
    def daily_key(self) -> str:
        return self._daily_key


class FairUseRegistry:
    """A small registry of ``FairUse`` policies keyed by capability.

    The daemon holds one of these and asks ``registry.for_cap(cap)`` before
    forwarding. Capabilities without an explicit policy get a ``FairUse``
    built from default limits (or a configured default) on first use — but
    only if ``enforce=True``. With ``enforce=False`` (the default for the
    daemon until the admin configures limits) the registry returns ``None``
    so the daemon skips fair-use gating entirely.

    This keeps T-099 self-contained: the policy exists and is tested, and
    the daemon wires it in only when configured.
    """

    def __init__(
        self,
        policies: dict[str, FairUse] | None = None,
        *,
        state_dir: Path | str | None = None,
        default_max_parallel: int = DEFAULT_MAX_PARALLEL,
        default_max_daily: int = DEFAULT_MAX_DAILY,
        enforce: bool = False,
        clock: "Any" = None,
    ) -> None:
        self._policies: dict[str, FairUse] = dict(policies) if policies else {}
        self.state_dir = state_dir
        self.default_max_parallel = default_max_parallel
        self.default_max_daily = default_max_daily
        self.enforce = enforce
        self._clock = clock
        self._lock = threading.Lock()

    def for_cap(self, capability: str) -> FairUse | None:
        """Return the policy for ``capability``, or ``None`` if not enforced.

        When ``enforce`` is False, returns ``None`` so callers can skip
        gating. When enforce is True and no explicit policy exists, a
        default policy is created lazily.
        """
        if not self.enforce:
            return None
        with self._lock:
            pol = self._policies.get(capability)
            if pol is None:
                pol = FairUse(
                    capability,
                    max_parallel=self.default_max_parallel,
                    max_daily=self.default_max_daily,
                    state_dir=self.state_dir,
                    clock=self._clock,
                )
                self._policies[capability] = pol
            return pol

    def register(self, policy: FairUse) -> None:
        """Register/replace an explicit policy for its capability."""
        with self._lock:
            self._policies[policy.capability] = policy

    def known(self) -> list[str]:
        with self._lock:
            return list(self._policies)