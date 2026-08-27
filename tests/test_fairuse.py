"""Tests for Fair-Use Policy enforcement (T-099).

Covers the plan's acceptance criteria:
* parallel cap blocks the 3rd concurrent claim,
* daily cap blocks after N claims,
* ``release()`` frees a parallel slot,
* structured ``FAIRUSE_REJECTED`` events are emitted on rejection,
* daily counter persists across restart (per-day JSON file),
* daily counter rolls over at the UTC day boundary,
* the ``FairUseRegistry`` gates only when ``enforce=True``.
"""

from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from federation.fairuse import (
    DEFAULT_MAX_DAILY,
    DEFAULT_MAX_PARALLEL,
    FairUse,
    FairUseRegistry,
)


# --------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------- #

class FixedClock:
    """A controllable UTC clock for daily-rollover tests."""

    def __init__(self, start: datetime) -> None:
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def advance(self, **kwargs: int) -> None:
        self.now = self.now + timedelta(**kwargs)


def utc(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


# --------------------------------------------------------------------- #
# Construction / validation
# --------------------------------------------------------------------- #

class TestConstruction:
    def test_defaults_match_plan(self, tmp_path: Path) -> None:
        fu = FairUse("image.gen@fed-b", state_dir=tmp_path)
        assert fu.capability == "image.gen@fed-b"
        assert fu.max_parallel == DEFAULT_MAX_PARALLEL == 2
        assert fu.max_daily == DEFAULT_MAX_DAILY == 50

    def test_zero_means_unbounded(self, tmp_path: Path) -> None:
        fu = FairUse("cap", max_parallel=0, max_daily=0, state_dir=tmp_path)
        assert fu.can_claim() is True
        for _ in range(100):
            assert fu.claim() is True
        # Release back to zero.
        for _ in range(100):
            fu.release()
        assert fu.in_flight == 0

    def test_rejects_empty_capability(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError):
            FairUse("", state_dir=tmp_path)

    @pytest.mark.parametrize("kw", ["max_parallel", "max_daily"])
    def test_rejects_negative(self, tmp_path: Path, kw: str) -> None:
        with pytest.raises(ValueError):
            FairUse("cap", state_dir=tmp_path, **{kw: -1})


# --------------------------------------------------------------------- #
# Parallel cap
# --------------------------------------------------------------------- #

class TestParallelCap:
    def test_blocks_third_concurrent(self, tmp_path: Path) -> None:
        fu = FairUse("cap", max_parallel=2, max_daily=50, state_dir=tmp_path)
        assert fu.can_claim() is True
        assert fu.claim() is True
        assert fu.claim() is True
        # 3rd concurrent must be blocked.
        assert fu.can_claim() is False
        assert fu.claim() is False
        assert fu.in_flight == 2

    def test_release_frees_a_slot(self, tmp_path: Path) -> None:
        fu = FairUse("cap", max_parallel=2, max_daily=50, state_dir=tmp_path)
        assert fu.claim() is True
        assert fu.claim() is True
        assert fu.claim() is False   # at cap
        assert fu.release() == 1
        # Now a slot is free again.
        assert fu.can_claim() is True
        assert fu.claim() is True
        assert fu.in_flight == 2

    def test_release_clamps_at_zero(self, tmp_path: Path) -> None:
        fu = FairUse("cap", max_parallel=2, max_daily=50, state_dir=tmp_path)
        # Releasing without a claim must not go negative.
        assert fu.release() == 0
        assert fu.in_flight == 0

    def test_release_does_not_refund_daily(self, tmp_path: Path) -> None:
        fu = FairUse("cap", max_parallel=2, max_daily=5, state_dir=tmp_path)
        fu.claim()
        fu.claim()
        fu.release()
        fu.release()
        # Parallel slots are free, but daily still counts both claims.
        assert fu.daily_count == 2
        assert fu.can_claim() is True


# --------------------------------------------------------------------- #
# Daily cap
# --------------------------------------------------------------------- #

class TestDailyCap:
    def test_blocks_after_n(self, tmp_path: Path) -> None:
        fu = FairUse("cap", max_parallel=10, max_daily=3, state_dir=tmp_path)
        for _ in range(3):
            assert fu.claim() is True
        # 4th blocked by daily cap (parallel still has room).
        assert fu.check_daily() is False
        assert fu.can_claim() is False
        assert fu.claim() is False
        # Releasing parallel slots does not unblock the daily cap.
        fu.release()
        fu.release()
        assert fu.can_claim() is False

    def test_check_daily_without_claiming(self, tmp_path: Path) -> None:
        fu = FairUse("cap", max_parallel=10, max_daily=2, state_dir=tmp_path)
        assert fu.check_daily() is True
        fu.claim()
        fu.claim()
        assert fu.check_daily() is False
        # check_daily must not mutate state.
        assert fu.daily_count == 2

    def test_daily_rollover_resets_counter(self, tmp_path: Path) -> None:
        clock = FixedClock(utc(2026, 8, 27, 23, 59))
        fu = FairUse(
            "cap", max_parallel=10, max_daily=2, state_dir=tmp_path, clock=clock
        )
        assert fu.claim() is True
        assert fu.claim() is True
        assert fu.claim() is False   # daily exhausted on day 1
        assert fu.daily_key == "2026-08-27"
        # Cross midnight UTC.
        clock.advance(minutes=1)    # → 2026-08-28 00:00
        assert fu.daily_key == "2026-08-27"   # not rolled yet (lazy)
        # check_daily triggers rollover.
        assert fu.check_daily() is True
        assert fu.daily_key == "2026-08-28"
        assert fu.daily_count == 0
        assert fu.claim() is True

    def test_daily_counter_persists_across_restart(self, tmp_path: Path) -> None:
        fu1 = FairUse("cap", max_parallel=10, max_daily=5, state_dir=tmp_path)
        fu1.claim()
        fu1.claim()
        fu1.claim()
        # New instance on the same state_dir + day → loads persisted count.
        fu2 = FairUse("cap", max_parallel=10, max_daily=5, state_dir=tmp_path)
        assert fu2.daily_count == 3
        # Two more claims fit, the 6th is blocked.
        assert fu2.claim() is True
        assert fu2.claim() is True
        assert fu2.claim() is False
        # The persisted file exists and is valid JSON.
        day_file = tmp_path / "cap" / "daily" / f"{fu2.daily_key}.json"
        assert day_file.is_file()
        data = json.loads(day_file.read_text())
        assert data["count"] == 5
        assert data["capability"] == "cap"


# --------------------------------------------------------------------- #
# FAIRUSE_REJECTED structured event
# --------------------------------------------------------------------- #

class TestRejectedLogging:
    def test_parallel_rejection_logs_event(self, tmp_path: Path, caplog) -> None:
        fu = FairUse("cap", max_parallel=1, max_daily=50, state_dir=tmp_path)
        fu.claim()
        with caplog.at_level(logging.INFO, logger="federation.fairuse"):
            assert fu.claim() is False
        rejected = [r for r in caplog.records if "FAIRUSE_REJECTED" in r.getMessage()]
        assert len(rejected) == 1
        msg = rejected[0].getMessage()
        assert "reason=parallel" in msg
        assert "capability=cap" in msg
        assert "in_flight=1" in msg

    def test_daily_rejection_logs_event(self, tmp_path: Path, caplog) -> None:
        fu = FairUse("cap", max_parallel=50, max_daily=1, state_dir=tmp_path)
        fu.claim()
        with caplog.at_level(logging.INFO, logger="federation.fairuse"):
            assert fu.claim() is False
        rejected = [r for r in caplog.records if "FAIRUSE_REJECTED" in r.getMessage()]
        assert len(rejected) == 1
        assert "reason=daily" in rejected[0].getMessage()


# --------------------------------------------------------------------- #
# Thread safety
# --------------------------------------------------------------------- #

class TestThreadSafety:
    def test_concurrent_claims_respect_parallel_cap(self, tmp_path: Path) -> None:
        fu = FairUse("cap", max_parallel=10, max_daily=1000, state_dir=tmp_path)
        successes = []
        barrier = threading.Barrier(20)

        def worker() -> None:
            barrier.wait()
            for _ in range(50):
                if fu.claim():
                    successes.append(1)
                    fu.release()

        threads = [threading.Thread(target=worker) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        # All threads released after each claim, so all 1000 should succeed.
        assert len(successes) == 1000
        assert fu.in_flight == 0
        # Daily counter should equal the number of successful claims.
        assert fu.daily_count == 1000

    def test_concurrent_claims_never_exceed_parallel_when_no_release(
        self, tmp_path: Path
    ) -> None:
        fu = FairUse("cap", max_parallel=4, max_daily=1000, state_dir=tmp_path)
        won = []
        barrier = threading.Barrier(20)

        def worker() -> None:
            barrier.wait()
            if fu.claim():
                won.append(1)

        threads = [threading.Thread(target=worker) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(won) == 4
        assert fu.in_flight == 4


# --------------------------------------------------------------------- #
# FairUseRegistry
# --------------------------------------------------------------------- #

class TestRegistry:
    def test_returns_none_when_not_enforced(self, tmp_path: Path) -> None:
        reg = FairUseRegistry(state_dir=tmp_path, enforce=False)
        assert reg.for_cap("cap") is None

    def test_creates_default_policy_when_enforced(self, tmp_path: Path) -> None:
        reg = FairUseRegistry(
            state_dir=tmp_path, enforce=True,
            default_max_parallel=3, default_max_daily=7,
        )
        pol = reg.for_cap("cap")
        assert pol is not None
        assert pol.max_parallel == 3
        assert pol.max_daily == 7
        # Cached on second call.
        assert reg.for_cap("cap") is pol

    def test_register_explicit_policy(self, tmp_path: Path) -> None:
        reg = FairUseRegistry(state_dir=tmp_path, enforce=True)
        pol = FairUse("cap", max_parallel=1, max_daily=1, state_dir=tmp_path)
        reg.register(pol)
        assert reg.for_cap("cap") is pol
        assert reg.known() == ["cap"]

    def test_enforce_false_ignores_registered(self, tmp_path: Path) -> None:
        pol = FairUse("cap", max_parallel=1, max_daily=1, state_dir=tmp_path)
        reg = FairUseRegistry(state_dir=tmp_path, enforce=False, policies={"cap": pol})
        # Even with a registered policy, enforce=False short-circuits to None.
        assert reg.for_cap("cap") is None