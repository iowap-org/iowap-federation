"""Tests for the message lifecycle state machine + ACK (T-102).

Covers:

* Full state progression ``CREATED → ENCRYPTED → QUEUED → SENT → ACKED →
  COMPLETED → ARCHIVED``.
* Terminal states ``FAILED`` / ``EXPIRED`` / ``REJECTED`` — no further
  transitions allowed.
* :class:`IllegalTransition` on a disallowed edge.
* ACK removes the outbox file (via :class:`SenderAck`); ``accepted``
  archives, ``rejected`` archives (terminal), ``failed`` leaves for
  retry.
* Duplicate ``message_id`` rejected by :class:`ReplayGuard`.
* Retry backoff sequence ``30s 1m 2m 4m 8m 16m 30m 60m`` with jitter in
  range ``[0, base]``; max-attempts cap moves the message to ``FAILED``.
* Atomic file I/O (``.tmp_<id>`` + ``os.replace``) leaves no temp files.
* Observability events emitted on transitions.
* :class:`ReceiverAck` records the sender's ACK and snapshots.
* ``pending_remote_response`` for async transports.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from federation.ack import (
    ACK_STATUS_ACCEPTED,
    ACK_STATUS_FAILED,
    ACK_STATUS_REJECTED,
    ReceiverAck,
    SenderAck,
    inbox_accepted,
    inbox_rejected,
    parse_ack_body,
)
from federation.lifecycle import (
    DEFAULT_BACKOFF_SEQUENCE,
    DEFAULT_MAX_ATTEMPTS,
    IllegalTransition,
    Lifecycle,
    LifecycleRegistry,
    MessageState,
    ReplayGuard,
    RetryPolicy,
    TERMINAL_STATES,
    atomic_write_bytes,
    atomic_write_json,
    emit_event,
)


# --------------------------------------------------------------------- #
# State machine — full progression
# --------------------------------------------------------------------- #
def test_full_state_progression_happy_path():
    lc = Lifecycle(message_id="m1")
    assert lc.state is MessageState.CREATED
    lc.encrypted()
    assert lc.state is MessageState.ENCRYPTED
    lc.queued()
    assert lc.state is MessageState.QUEUED
    lc.sent()
    assert lc.state is MessageState.SENT
    lc.acked()
    assert lc.state is MessageState.ACKED
    lc.completed()
    assert lc.state is MessageState.COMPLETED
    lc.archived()
    assert lc.state is MessageState.ARCHIVED
    assert lc.is_terminal
    # History records every transition in order.
    states = [s for s, _ in lc.history]
    assert states == [
        MessageState.CREATED,
        MessageState.ENCRYPTED,
        MessageState.QUEUED,
        MessageState.SENT,
        MessageState.ACKED,
        MessageState.COMPLETED,
        MessageState.ARCHIVED,
    ]


def test_terminal_states_admit_no_transitions():
    for terminal in (MessageState.FAILED, MessageState.EXPIRED, MessageState.REJECTED, MessageState.ARCHIVED):
        lc = Lifecycle(message_id="t", state=terminal)
        assert lc.is_terminal
        for target in (MessageState.QUEUED, MessageState.SENT, MessageState.ACKED):
            with pytest.raises(IllegalTransition):
                lc.transition(target)


def test_illegal_transition_raises():
    lc = Lifecycle(message_id="x")
    # CREATED → ACKED is not a direct edge.
    with pytest.raises(IllegalTransition):
        lc.acked()


def test_failed_terminal_via_queued():
    lc = Lifecycle(message_id="f")
    lc.queued()
    lc.failed(reason="boom")
    assert lc.state is MessageState.FAILED
    assert lc.is_terminal


def test_expired_terminal_via_queued():
    lc = Lifecycle(message_id="e")
    lc.queued()
    lc.expired(reason="timeout")
    assert lc.state is MessageState.EXPIRED


def test_rejected_terminal():
    lc = Lifecycle(message_id="r")
    lc.rejected(reason="replay")
    assert lc.state is MessageState.REJECTED


def test_idempotent_same_state_transition_is_noop():
    lc = Lifecycle(message_id="i")
    lc.queued()
    lc.queued()  # no-op, no raise
    assert lc.state is MessageState.QUEUED


def test_retry_loop_queued_to_sent_to_queued():
    """A failed send can re-queue (SENT → QUEUED) for retry."""
    lc = Lifecycle(message_id="retry-1")
    lc.queued()
    lc.sent()
    lc.transition(MessageState.QUEUED)  # re-queue after a send failure
    assert lc.state is MessageState.QUEUED
    lc.sent()
    assert lc.state is MessageState.SENT


# --------------------------------------------------------------------- #
# pending_remote_response (async transport)
# --------------------------------------------------------------------- #
def test_pending_remote_response_from_sent():
    """A sent message waiting on a remote response moves to PENDING_REMOTE_RESPONSE."""
    lc = Lifecycle(message_id="async-1")
    lc.queued()
    lc.sent()
    lc.pending_remote_response()
    assert lc.state is MessageState.PENDING_REMOTE_RESPONSE
    # Can later be ACKED / COMPLETED once the remote replies.
    lc.acked()
    lc.completed()
    assert lc.state is MessageState.COMPLETED


def test_pending_remote_response_can_requeue_on_loss():
    lc = Lifecycle(message_id="async-2")
    lc.queued()
    lc.sent()
    lc.pending_remote_response()
    lc.transition(MessageState.QUEUED)  # remote lost → re-queue
    assert lc.state is MessageState.QUEUED


# --------------------------------------------------------------------- #
# RetryPolicy — exponential backoff + jitter
# --------------------------------------------------------------------- #
def test_retry_policy_base_delay_sequence_matches_plan():
    p = RetryPolicy(seed=42)
    # 1-indexed attempts; the plan's sequence is 30s,1m,2m,4m,8m,16m,30m,60m
    bases = [p.base_delay(n) for n in range(1, len(DEFAULT_BACKOFF_SEQUENCE) + 1)]
    assert bases == list(DEFAULT_BACKOFF_SEQUENCE)


def test_retry_policy_caps_at_last_sequence_value():
    p = RetryPolicy()
    cap = p.base_delay(1000)
    assert cap == DEFAULT_BACKOFF_SEQUENCE[-1]  # 3600.0


def test_retry_policy_jitter_within_zero_to_base():
    p = RetryPolicy(seed=123)
    for attempt in range(1, len(DEFAULT_BACKOFF_SEQUENCE) + 1):
        base = p.base_delay(attempt)
        for _ in range(20):  # sample many draws
            d = p.delay_for(attempt)
            assert 0.0 <= d <= base


def test_retry_policy_has_attempts_left_until_cap():
    p = RetryPolicy()
    # max_attempts = 1 + len(sequence). Attempt N is allowed while N < max.
    assert p.has_attempts_left(1)
    assert p.has_attempts_left(DEFAULT_MAX_ATTEMPTS - 1)
    assert not p.has_attempts_left(DEFAULT_MAX_ATTEMPTS)
    assert not p.has_attempts_left(DEFAULT_MAX_ATTEMPTS + 5)


def test_record_retry_returns_delay_then_failed_when_exhausted():
    p = RetryPolicy(seed=7, max_attempts=3)
    lc = Lifecycle(message_id="retry-ex")
    lc.queued()
    lc.sent()
    d1 = lc.record_retry(p)
    assert d1 >= 0.0
    assert lc.state is MessageState.SENT  # not failed yet
    d2 = lc.record_retry(p)
    assert d2 >= 0.0
    # Third retry attempt (== max_attempts) → exhausted → FAILED, -1.0
    d3 = lc.record_retry(p)
    assert d3 == -1.0
    assert lc.state is MessageState.FAILED
    assert lc.is_terminal


def test_record_retry_increments_counter():
    p = RetryPolicy(seed=1)
    lc = Lifecycle(message_id="cnt")
    lc.queued()
    lc.sent()
    assert lc.retry_count == 0
    lc.record_retry(p)
    assert lc.retry_count == 1
    lc.record_retry(p)
    assert lc.retry_count == 2


def test_retry_policy_rejects_zero_max_attempts():
    with pytest.raises(ValueError):
        RetryPolicy(max_attempts=0)


def test_retry_policy_rejects_empty_sequence():
    with pytest.raises(ValueError):
        RetryPolicy(sequence=())


# --------------------------------------------------------------------- #
# ReplayGuard
# --------------------------------------------------------------------- #
def test_replay_guard_first_seen_returns_true():
    g = ReplayGuard(ttl=60.0)
    assert g.check_and_add("a") is True
    assert "a" in g


def test_replay_guard_second_seen_returns_false():
    g = ReplayGuard(ttl=60.0)
    g.check_and_add("a")
    assert g.check_and_add("a") is False


def test_replay_guard_ttl_prune_drops_stale_entries():
    g = ReplayGuard(ttl=0.05)  # 50 ms
    g.check_and_add("old")
    time.sleep(0.06)
    # After TTL, the id is no longer in the cache → re-adding is fresh.
    assert "old" not in g
    assert g.check_and_add("old") is True


def test_replay_guard_prune_returns_count():
    g = ReplayGuard(ttl=0.05)
    g.check_and_add("a")
    g.check_and_add("b")
    time.sleep(0.06)
    n = g.prune()
    assert n == 2
    assert len(g) == 0


def test_replay_guard_distinct_ids_are_independent():
    g = ReplayGuard(ttl=60.0)
    assert g.check_and_add("a") is True
    assert g.check_and_add("b") is True
    assert g.check_and_add("a") is False
    assert g.check_and_add("b") is False


# --------------------------------------------------------------------- #
# Atomic file I/O
# --------------------------------------------------------------------- #
def test_atomic_write_bytes_writes_file_and_no_temps(tmp_path):
    dest = tmp_path / "outbox" / "m1.json"
    atomic_write_bytes(dest, b'{"hello": "world"}')
    assert dest.read_bytes() == b'{"hello": "world"}'
    leftover = [p.name for p in (tmp_path / "outbox").iterdir() if p.name.startswith(".tmp_")]
    assert leftover == []


def test_atomic_write_json_writes_sorted_json(tmp_path):
    dest = tmp_path / "f.json"
    atomic_write_json(dest, {"b": 2, "a": 1})
    text = dest.read_text()
    # sort_keys=True → "a" before "b"
    assert text.index('"a"') < text.index('"b"')


def test_atomic_write_bytes_creates_parent_dirs(tmp_path):
    dest = tmp_path / "deep" / "deeper" / "m.json"
    atomic_write_bytes(dest, b"x")
    assert dest.exists()


# --------------------------------------------------------------------- #
# ACK — SenderAck (outbox archival on ACK)
# --------------------------------------------------------------------- #
def test_sender_ack_accepted_archives_outbox_file(tmp_path):
    outbox = tmp_path / "outbox"
    archive = tmp_path / "archive"
    outbox.mkdir()
    (outbox / "m-acc.json").write_text(json.dumps({"message_id": "m-acc"}))
    sa = SenderAck(outbox_dir=outbox, archive_dir=archive)
    status = sa.handle_inbox_response(
        "m-acc", {"status": "accepted", "message_id": "m-acc"},
        outbox_file=outbox / "m-acc.json",
    )
    assert status == "accepted"
    assert not (outbox / "m-acc.json").exists()
    assert (archive / "m-acc.json").exists()


def test_sender_ack_rejected_archives_outbox_file(tmp_path):
    outbox = tmp_path / "outbox"
    archive = tmp_path / "archive"
    outbox.mkdir()
    (outbox / "m-rej.json").write_text(json.dumps({"message_id": "m-rej"}))
    sa = SenderAck(outbox_dir=outbox, archive_dir=archive)
    status = sa.handle_inbox_response(
        "m-rej", {"status": "rejected", "message_id": "m-rej"},
        outbox_file=outbox / "m-rej.json",
    )
    assert status == "rejected"
    assert (archive / "m-rej.json").exists()
    assert not (outbox / "m-rej.json").exists()


def test_sender_ack_failed_leaves_outbox_file_for_retry(tmp_path):
    outbox = tmp_path / "outbox"
    archive = tmp_path / "archive"
    outbox.mkdir()
    f = outbox / "m-fail.json"
    f.write_text(json.dumps({"message_id": "m-fail"}))
    sa = SenderAck(outbox_dir=outbox, archive_dir=archive)
    status = sa.handle_inbox_response(
        "m-fail", {"status": "failed", "message_id": "m-fail"},
        outbox_file=f,
    )
    assert status == "failed"
    # File remains for retry.
    assert f.exists()
    assert not (archive / "m-fail.json").exists()


def test_sender_ack_unknown_status_leaves_file(tmp_path):
    outbox = tmp_path / "outbox"
    archive = tmp_path / "archive"
    outbox.mkdir()
    f = outbox / "m-unk.json"
    f.write_text(json.dumps({"message_id": "m-unk"}))
    sa = SenderAck(outbox_dir=outbox, archive_dir=archive)
    status = sa.handle_inbox_response("m-unk", None, outbox_file=f)
    assert status == "unknown"
    assert f.exists()


def test_sender_ack_advances_lifecycle_to_archived(tmp_path):
    outbox = tmp_path / "outbox"
    archive = tmp_path / "archive"
    outbox.mkdir()
    (outbox / "m-lc.json").write_text(json.dumps({"message_id": "m-lc"}))
    reg = LifecycleRegistry()
    lc = Lifecycle(message_id="m-lc", state=MessageState.SENT)
    reg.register(lc)
    sa = SenderAck(outbox_dir=outbox, archive_dir=archive, registry=reg)
    sa.handle_inbox_response(
        "m-lc", {"status": "accepted", "message_id": "m-lc"},
        outbox_file=outbox / "m-lc.json",
    )
    assert lc.state is MessageState.ARCHIVED
    assert lc.is_terminal


def test_sender_ack_rejected_advances_lifecycle_to_rejected(tmp_path):
    outbox = tmp_path / "outbox"
    archive = tmp_path / "archive"
    outbox.mkdir()
    (outbox / "m-rej2.json").write_text(json.dumps({"message_id": "m-rej2"}))
    reg = LifecycleRegistry()
    lc = Lifecycle(message_id="m-rej2", state=MessageState.SENT)
    reg.register(lc)
    sa = SenderAck(outbox_dir=outbox, archive_dir=archive, registry=reg)
    sa.handle_inbox_response(
        "m-rej2", {"status": "rejected", "message_id": "m-rej2"},
        outbox_file=outbox / "m-rej2.json",
    )
    assert lc.state is MessageState.REJECTED


def test_sender_ack_build_ack_request_validates_status():
    outbox = Path("/tmp/x")
    archive = Path("/tmp/y")
    sa = SenderAck(outbox_dir=outbox, archive_dir=archive)
    body = sa.build_ack_request("mid", "accepted")
    assert body == {"message_id": "mid", "status": "accepted"}
    with pytest.raises(ValueError):
        sa.build_ack_request("mid", "bogus")


def test_sender_ack_missing_outbox_file_logs_but_does_not_raise(tmp_path):
    outbox = tmp_path / "outbox"
    archive = tmp_path / "archive"
    outbox.mkdir()
    sa = SenderAck(outbox_dir=outbox, archive_dir=archive)
    # File does not exist — handle_inbox_response should not raise.
    sa.handle_inbox_response(
        "ghost", {"status": "accepted", "message_id": "ghost"},
        outbox_file=outbox / "ghost.json",
    )
    # Nothing archived.
    assert not (archive / "ghost.json").exists()


# --------------------------------------------------------------------- #
# ACK — ReceiverAck
# --------------------------------------------------------------------- #
def test_receiver_ack_records_status():
    ra = ReceiverAck()
    resp = ra.record("m1", "accepted")
    assert resp == {"status": "ok", "message_id": "m1", "ack": "accepted"}
    assert ra.has("m1")
    assert ra.snapshot() == {"m1": "accepted"}


def test_receiver_ack_overwrites_on_resend():
    ra = ReceiverAck()
    ra.record("m1", "accepted")
    ra.record("m1", "failed")
    assert ra.snapshot()["m1"] == "failed"


def test_parse_ack_body_valid():
    mid, status = parse_ack_body({"message_id": "m", "status": "accepted"})
    assert (mid, status) == ("m", "accepted")


def test_parse_ack_body_invalid_status():
    with pytest.raises(ValueError):
        parse_ack_body({"message_id": "m", "status": "bogus"})


def test_parse_ack_body_missing_message_id():
    with pytest.raises(ValueError):
        parse_ack_body({"status": "accepted"})


def test_parse_ack_body_non_object():
    with pytest.raises(ValueError):
        parse_ack_body(["not", "an", "object"])


def test_inbox_accepted_and_rejected_helpers():
    assert inbox_accepted("m") == {"status": "accepted", "message_id": "m"}
    rej = inbox_rejected("m", reason="replay")
    assert rej == {"status": "rejected", "message_id": "m", "reason": "replay"}


# --------------------------------------------------------------------- #
# LifecycleRegistry
# --------------------------------------------------------------------- #
def test_registry_register_and_get():
    reg = LifecycleRegistry()
    lc = Lifecycle(message_id="r1")
    reg.register(lc)
    assert reg.get("r1") is lc
    assert reg.get("missing") is None


def test_registry_duplicate_register_raises():
    reg = LifecycleRegistry()
    reg.register(Lifecycle(message_id="dup"))
    with pytest.raises(ValueError):
        reg.register(Lifecycle(message_id="dup"))


def test_registry_count_in_state():
    reg = LifecycleRegistry()
    a = Lifecycle(message_id="a"); a.queued()
    b = Lifecycle(message_id="b"); b.queued(); b.sent()
    c = Lifecycle(message_id="c"); c.queued()
    for lc in (a, b, c):
        reg.register(lc)
    assert reg.count_in_state(MessageState.QUEUED) == 2
    assert reg.count_in_state(MessageState.SENT) == 1


def test_registry_pending_acks_lists_sent_and_pending_remote():
    reg = LifecycleRegistry()
    a = Lifecycle(message_id="a"); a.queued(); a.sent()
    b = Lifecycle(message_id="b"); b.queued(); b.sent(); b.pending_remote_response()
    c = Lifecycle(message_id="c"); c.queued()
    for lc in (a, b, c):
        reg.register(lc)
    pending = {lc.message_id for lc in reg.pending_acks()}
    assert pending == {"a", "b"}


def test_registry_remove():
    reg = LifecycleRegistry()
    lc = Lifecycle(message_id="rm")
    reg.register(lc)
    assert reg.remove("rm") is lc
    assert reg.get("rm") is None
    assert reg.remove("rm") is None


# --------------------------------------------------------------------- #
# Observability events
# --------------------------------------------------------------------- #
def test_emit_event_valid_name(caplog):
    with caplog.at_level("INFO", logger="federation.lifecycle"):
        emit_event("MESSAGE_CREATED", message_id="ev-1")
    assert any("MESSAGE_CREATED" in r.message for r in caplog.records)


def test_emit_event_rejects_unknown_name():
    with pytest.raises(ValueError):
        emit_event("MESSAGE_BOGUS", message_id="x")


def test_emit_event_fields_sorted_and_quoted(caplog):
    with caplog.at_level("INFO", logger="federation.lifecycle"):
        emit_event("MESSAGE_FAILED", message_id="z", reason="boom", code=42)
    rec = [r for r in caplog.records if "MESSAGE_FAILED" in r.message]
    assert rec
    # Fields are sorted by key: code, message_id, reason
    msg = rec[-1].message
    assert msg.index("code=") < msg.index("message_id=")
    assert msg.index("message_id=") < msg.index("reason=")


# --------------------------------------------------------------------- #
# End-to-end: full lifecycle + ACK removes outbox file
# --------------------------------------------------------------------- #
def test_full_lifecycle_ack_removes_outbox_file(tmp_path):
    """The plan's headline contract: 'sender removes outbox file only after ACK'."""
    outbox = tmp_path / "outbox"
    archive = tmp_path / "archive"
    outbox.mkdir()
    # Simulate a created + queued + sent message on disk.
    message_id = "e2e-1"
    (outbox / f"{message_id}.json").write_text(
        json.dumps({"message_id": message_id, "type": "task", "payload": {}})
    )
    reg = LifecycleRegistry()
    lc = Lifecycle(message_id=message_id)
    reg.register(lc)
    lc.queued()

    sa = SenderAck(outbox_dir=outbox, archive_dir=archive, registry=reg)
    # The receiver returns an accepted inline ACK.
    sa.handle_inbox_response(
        message_id,
        {"status": "accepted", "message_id": message_id},
        outbox_file=outbox / f"{message_id}.json",
    )
    # The outbox file is gone (archived) and the lifecycle is terminal.
    assert not (outbox / f"{message_id}.json").exists()
    assert (archive / f"{message_id}.json").exists()
    assert lc.state is MessageState.ARCHIVED
    assert lc.is_terminal


def test_duplicate_message_id_rejected_by_replay_guard():
    """Replay guard rejects a duplicate before it can be processed twice."""
    g = ReplayGuard(ttl=60.0)
    assert g.check_and_add("dup") is True
    assert g.check_and_add("dup") is False
    assert g.check_and_add("other") is True


# --------------------------------------------------------------------- #
# Retry backoff sequence (explicit plan values)
# --------------------------------------------------------------------- #
def test_retry_backoff_sequence_values():
    """The plan specifies: 30s 1m 2m 4m 8m 16m 30m 60m."""
    expected = [30, 60, 120, 240, 480, 960, 1800, 3600]
    p = RetryPolicy()
    for i, want in enumerate(expected, start=1):
        assert p.base_delay(i) == want


def test_retry_jitter_bounded_by_base():
    """Full jitter: delay ∈ [0, base]."""
    p = RetryPolicy(seed=99)
    for attempt in range(1, 9):
        base = p.base_delay(attempt)
        for _ in range(50):
            d = p.delay_for(attempt)
            assert 0.0 <= d <= base, f"attempt={attempt} d={d} base={base}"


def test_max_attempts_default_is_one_plus_sequence_length():
    assert DEFAULT_MAX_ATTEMPTS == 1 + len(DEFAULT_BACKOFF_SEQUENCE)