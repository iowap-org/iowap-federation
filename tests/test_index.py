"""Tests for the SQLite metadata index + hash partitioning (T-103).

Covers (per the master-plan acceptance):

* insert / update_state / get_state / count_in_flight / count_today /
  increment_retry / mark_processed / prune_ttl.
* Hash partition: file path = ``<base>/<SHA256(message_id)[:2]>/<message_id>.json``.
* Terminal-state archival cleans the payload files.
* TTL prune works on processed + terminal rows.
* Atomic writes preserved (the index helpers use ``os.replace``).
* Idempotent insert (INSERT OR IGNORE on PK).
* Reply correlation via ``get_by_remote_task_id``.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from federation.index import (
    IN_FLIGHT_STATES,
    MetadataIndex,
    ensure_partition_dir,
    partition_dir,
    partition_path,
    partition_subdir,
)
from federation.lifecycle import MessageState


# --------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------- #

@pytest.fixture
def idx(tmp_path: Path) -> MetadataIndex:
    db = tmp_path / "state.db"
    return MetadataIndex(db)


@pytest.fixture
def fs(tmp_path: Path) -> Path:
    """A scratch directory tree mirroring a remote's outbox."""
    outbox = tmp_path / "outbox"
    outbox.mkdir()
    return outbox


# --------------------------------------------------------------------- #
# Hash partitioning
# --------------------------------------------------------------------- #

def test_partition_subdir_is_two_hex_chars():
    sub = partition_subdir("abc123")
    assert len(sub) == 2
    # all hex chars
    assert all(c in "0123456789abcdef" for c in sub)


def test_partition_subdir_deterministic():
    assert partition_subdir("m-1") == partition_subdir("m-1")


def test_partition_subdir_different_messages_differ_usually():
    # not a strict collision test, just sanity that ids spread
    subs = {partition_subdir(f"m-{i}") for i in range(50)}
    assert len(subs) > 1


def test_partition_path_shape():
    p = partition_path("/tmp/outbox", "m-1")
    assert p.name == "m-1.json"
    assert p.parent.name == partition_subdir("m-1")
    assert p.parent.parent.name == "outbox"


def test_ensure_partition_dir_creates_dir(fs: Path):
    d = ensure_partition_dir(fs, "m-1")
    assert d.is_dir()
    assert d.name == partition_subdir("m-1")


def test_partition_dir_does_not_create(fs: Path):
    d = partition_dir(fs, "m-1")
    assert not d.exists()


def test_partition_depth_override():
    # depth=4 → 4-char subdir
    sub = partition_subdir("m-1", depth=4)
    assert len(sub) == 4
    p = partition_path("/tmp/outbox", "m-1", depth=4)
    assert p.parent.name == sub


# --------------------------------------------------------------------- #
# insert / get / get_state
# --------------------------------------------------------------------- #

def test_insert_and_get(idx: MetadataIndex):
    assert idx.insert("m-1", state=MessageState.CREATED, type="task",
                      remote="fed-b", capability="image.gen",
                      remote_task_id="st-1", schema_hash="h1", size=42)
    row = idx.get("m-1")
    assert row is not None
    assert row["message_id"] == "m-1"
    assert row["state"] == "CREATED"
    assert row["type"] == "task"
    assert row["remote"] == "fed-b"
    assert row["capability"] == "image.gen"
    assert row["remote_task_id"] == "st-1"
    assert row["schema_hash"] == "h1"
    assert row["size"] == 42
    assert row["retry_count"] == 0
    assert row["processed"] is False
    assert row["created_at"]
    assert row["updated_at"] == row["created_at"]


def test_insert_accepts_string_state(idx: MetadataIndex):
    assert idx.insert("m-x", state="QUEUED")
    assert idx.get_state("m-x") == "QUEUED"


def test_insert_idempotent_on_pk(idx: MetadataIndex):
    assert idx.insert("m-1", state=MessageState.CREATED)
    # second insert is a no-op (returns False, does not overwrite)
    assert idx.insert("m-1", state=MessageState.SENT) is False
    assert idx.get_state("m-1") == "CREATED"


def test_insert_empty_message_id_raises(idx: MetadataIndex):
    with pytest.raises(ValueError):
        idx.insert("")


def test_get_state_unknown_returns_none(idx: MetadataIndex):
    assert idx.get_state("nope") is None


def test_get_unknown_returns_none(idx: MetadataIndex):
    assert idx.get("nope") is None


def test_insert_with_explicit_created_at(idx: MetadataIndex):
    ts = "2026-01-01T00:00:00+00:00"
    idx.insert("m-ts", created_at=ts)
    row = idx.get("m-ts")
    assert row["created_at"] == ts
    assert row["updated_at"] == ts


# --------------------------------------------------------------------- #
# update_state
# --------------------------------------------------------------------- #

def test_update_state_sets_and_refreshes_updated_at(idx: MetadataIndex):
    idx.insert("m-1", state=MessageState.CREATED)
    before = idx.get("m-1")["updated_at"]
    # ensure the clock ticks so updated_at differs
    time.sleep(0.01)
    assert idx.update_state("m-1", MessageState.SENT)
    row = idx.get("m-1")
    assert row["state"] == "SENT"
    assert row["updated_at"] >= before


def test_update_state_unknown_returns_false(idx: MetadataIndex):
    assert idx.update_state("ghost", MessageState.SENT) is False


# --------------------------------------------------------------------- #
# increment_retry
# --------------------------------------------------------------------- #

def test_increment_retry_counts(idx: MetadataIndex):
    idx.insert("m-1", state=MessageState.SENT)
    assert idx.increment_retry("m-1") == 1
    assert idx.increment_retry("m-1") == 2
    assert idx.get("m-1")["retry_count"] == 2


def test_increment_retry_unknown_returns_neg1(idx: MetadataIndex):
    assert idx.increment_retry("ghost") == -1


def test_increment_retry_refreshes_updated_at(idx: MetadataIndex):
    idx.insert("m-1")
    before = idx.get("m-1")["updated_at"]
    time.sleep(0.01)
    idx.increment_retry("m-1")
    assert idx.get("m-1")["updated_at"] > before


# --------------------------------------------------------------------- #
# mark_processed / is_processed
# --------------------------------------------------------------------- #

def test_mark_processed_sets_flag(idx: MetadataIndex):
    idx.insert("m-1")
    assert idx.is_processed("m-1") is False
    assert idx.mark_processed("m-1")
    assert idx.is_processed("m-1") is True


def test_mark_processed_idempotent_returns_false(idx: MetadataIndex):
    idx.insert("m-1")
    assert idx.mark_processed("m-1")
    assert idx.mark_processed("m-1") is False  # already processed


def test_mark_processed_unknown_returns_false(idx: MetadataIndex):
    assert idx.mark_processed("ghost") is False


def test_is_processed_unknown_returns_false(idx: MetadataIndex):
    assert idx.is_processed("ghost") is False


# --------------------------------------------------------------------- #
# count_in_flight
# --------------------------------------------------------------------- #

def test_count_in_flight_counts_non_terminal_non_completed(idx: MetadataIndex):
    idx.insert("m-1", state=MessageState.CREATED, remote="fed-b", capability="image.gen")
    idx.insert("m-2", state=MessageState.QUEUED, remote="fed-b", capability="image.gen")
    idx.insert("m-3", state=MessageState.SENT, remote="fed-b", capability="image.gen")
    idx.insert("m-4", state=MessageState.PENDING_REMOTE_RESPONSE, remote="fed-b", capability="image.gen")
    idx.insert("m-5", state=MessageState.ACKED, remote="fed-b", capability="image.gen")
    idx.insert("m-6", state=MessageState.COMPLETED, remote="fed-b", capability="image.gen")
    idx.insert("m-7", state=MessageState.ARCHIVED, remote="fed-b", capability="image.gen")
    idx.insert("m-8", state=MessageState.FAILED, remote="fed-b", capability="image.gen")
    # in-flight = CREATED, QUEUED, SENT, PENDING_REMOTE_RESPONSE, ACKED = 5
    assert idx.count_in_flight() == 5


def test_count_in_flight_filtered_by_remote(idx: MetadataIndex):
    idx.insert("m-1", state=MessageState.QUEUED, remote="fed-b")
    idx.insert("m-2", state=MessageState.QUEUED, remote="fed-c")
    assert idx.count_in_flight(remote="fed-b") == 1
    assert idx.count_in_flight(remote="fed-c") == 1
    assert idx.count_in_flight() == 2


def test_count_in_flight_filtered_by_capability(idx: MetadataIndex):
    idx.insert("m-1", state=MessageState.QUEUED, remote="fed-b", capability="image.gen")
    idx.insert("m-2", state=MessageState.QUEUED, remote="fed-b", capability="text.gen")
    assert idx.count_in_flight(capability="image.gen") == 1
    assert idx.count_in_flight(capability="text.gen") == 1


def test_in_flight_states_constant_excludes_terminal_and_completed():
    assert MessageState.COMPLETED.value not in IN_FLIGHT_STATES
    assert MessageState.ARCHIVED.value not in IN_FLIGHT_STATES
    assert MessageState.FAILED.value not in IN_FLIGHT_STATES
    assert MessageState.EXPIRED.value not in IN_FLIGHT_STATES
    assert MessageState.REJECTED.value not in IN_FLIGHT_STATES
    assert MessageState.QUEUED.value in IN_FLIGHT_STATES


# --------------------------------------------------------------------- #
# count_today
# --------------------------------------------------------------------- #

def test_count_today_counts_messages_created_today(idx: MetadataIndex):
    idx.insert("m-1", state=MessageState.QUEUED, remote="fed-b", capability="image.gen")
    idx.insert("m-2", state=MessageState.QUEUED, remote="fed-b", capability="image.gen")
    assert idx.count_today() == 2


def test_count_today_excludes_older_messages(idx: MetadataIndex):
    # insert a message with an old created_at
    old = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
    idx.insert("m-old", created_at=old)
    idx.insert("m-now")
    assert idx.count_today() == 1


def test_count_today_filtered(idx: MetadataIndex):
    idx.insert("m-1", remote="fed-b", capability="image.gen")
    idx.insert("m-2", remote="fed-c", capability="image.gen")
    idx.insert("m-3", remote="fed-b", capability="text.gen")
    assert idx.count_today(remote="fed-b") == 2
    assert idx.count_today(capability="image.gen") == 2
    assert idx.count_today(remote="fed-b", capability="text.gen") == 1


# --------------------------------------------------------------------- #
# get_by_remote_task_id (reply correlation)
# --------------------------------------------------------------------- #

def test_get_by_remote_task_id_finds_message(idx: MetadataIndex):
    idx.insert("m-1", remote_task_id="st-42", remote="fed-b")
    row = idx.get_by_remote_task_id("st-42")
    assert row is not None
    assert row["message_id"] == "m-1"


def test_get_by_remote_task_id_unknown_returns_none(idx: MetadataIndex):
    assert idx.get_by_remote_task_id("nope") is None


def test_get_by_remote_task_id_picks_most_recent(idx: MetadataIndex):
    idx.insert("m-1", remote_task_id="st-1", created_at="2026-01-01T00:00:00+00:00")
    idx.insert("m-2", remote_task_id="st-1", created_at="2026-01-02T00:00:00+00:00")
    row = idx.get_by_remote_task_id("st-1")
    assert row["message_id"] == "m-2"


# --------------------------------------------------------------------- #
# count_in_state (status file helper)
# --------------------------------------------------------------------- #

def test_count_in_state(idx: MetadataIndex):
    idx.insert("m-1", state=MessageState.QUEUED)
    idx.insert("m-2", state=MessageState.QUEUED)
    idx.insert("m-3", state=MessageState.SENT)
    assert idx.count_in_state(MessageState.QUEUED) == 2
    assert idx.count_in_state(MessageState.SENT) == 1
    assert idx.count_in_state(MessageState.ARCHIVED) == 0


# --------------------------------------------------------------------- #
# delete
# --------------------------------------------------------------------- #

def test_delete_removes_row(idx: MetadataIndex):
    idx.insert("m-1")
    assert idx.delete("m-1")
    assert idx.get("m-1") is None
    assert idx.delete("m-1") is False  # already gone


# --------------------------------------------------------------------- #
# prune_ttl
# --------------------------------------------------------------------- #

def test_prune_ttl_removes_old_processed_rows(idx: MetadataIndex):
    old = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    idx.insert("m-old-processed", created_at=old)
    idx.insert("m-old-processed-2", created_at=old)
    idx.insert("m-fresh-processed")
    # mark all processed
    idx.mark_processed("m-old-processed")
    idx.mark_processed("m-old-processed-2")
    idx.mark_processed("m-fresh-processed")
    # TTL of 1 hour → the two old rows go, the fresh one stays
    removed = idx.prune_ttl(3600)
    assert removed == 2
    assert idx.get("m-old-processed") is None
    assert idx.get("m-old-processed-2") is None
    assert idx.get("m-fresh-processed") is not None


def test_prune_ttl_does_not_remove_unprocessed_old_rows(idx: MetadataIndex):
    old = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    idx.insert("m-old-unprocessed", created_at=old)
    # processed=0 so prune should leave it
    removed = idx.prune_ttl(3600)
    assert removed == 0
    assert idx.get("m-old-unprocessed") is not None


def test_prune_ttl_removes_old_terminal_rows(idx: MetadataIndex):
    old = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    idx.insert("m-old-failed", state=MessageState.FAILED, created_at=old)
    idx.insert("m-old-archived", state=MessageState.ARCHIVED, created_at=old)
    idx.insert("m-old-completed", state=MessageState.COMPLETED, created_at=old)
    # COMPLETED is NOT terminal — it stays until archived/pruned.
    removed = idx.prune_ttl(3600)
    assert removed == 2  # FAILED + ARCHIVED
    assert idx.get("m-old-failed") is None
    assert idx.get("m-old-archived") is None
    assert idx.get("m-old-completed") is not None


def test_prune_ttl_keeps_fresh_terminal_rows(idx: MetadataIndex):
    idx.insert("m-fresh-failed", state=MessageState.FAILED)
    removed = idx.prune_ttl(3600)
    assert removed == 0
    assert idx.get("m-fresh-failed") is not None


# --------------------------------------------------------------------- #
# terminal_messages (archival sweep query)
# --------------------------------------------------------------------- #

def test_terminal_messages_returns_terminal_rows(idx: MetadataIndex):
    idx.insert("m-1", state=MessageState.FAILED)
    idx.insert("m-2", state=MessageState.ARCHIVED)
    idx.insert("m-3", state=MessageState.QUEUED)
    idx.insert("m-4", state=MessageState.COMPLETED)  # not terminal
    terms = idx.terminal_messages()
    ids = {r["message_id"] for r in terms}
    assert ids == {"m-1", "m-2"}


def test_terminal_messages_filtered_by_remote(idx: MetadataIndex):
    idx.insert("m-1", state=MessageState.FAILED, remote="fed-b")
    idx.insert("m-2", state=MessageState.FAILED, remote="fed-c")
    terms = idx.terminal_messages(remote="fed-b")
    assert {r["message_id"] for r in terms} == {"m-1"}


def test_terminal_messages_respects_limit(idx: MetadataIndex):
    for i in range(5):
        idx.insert(f"m-{i}", state=MessageState.FAILED)
    assert len(idx.terminal_messages(limit=2)) == 2


# --------------------------------------------------------------------- #
# archive_payload (file side)
# --------------------------------------------------------------------- #

def test_archive_payload_moves_file_to_archive_flat(fs: Path, tmp_path: Path):
    mid = "m-1"
    src = partition_path(fs, mid)
    ensure_partition_dir(fs, mid)
    src.write_text(json.dumps({"message_id": mid, "payload": "x"}))
    archive = tmp_path / "archive"
    dest = MetadataIndex.archive_payload(fs, mid, archive)
    assert dest is not None
    assert dest == archive / "m-1.json"
    assert dest.exists()
    assert not src.exists()  # moved


def test_archive_payload_missing_returns_none(fs: Path, tmp_path: Path):
    archive = tmp_path / "archive"
    assert MetadataIndex.archive_payload(fs, "ghost", archive) is None


def test_archive_payload_creates_archive_dir(fs: Path, tmp_path: Path):
    mid = "m-1"
    ensure_partition_dir(fs, mid)
    partition_path(fs, mid).write_text("{}")
    archive = tmp_path / "new_archive"
    assert not archive.exists()
    dest = MetadataIndex.archive_payload(fs, mid, archive)
    assert dest is not None
    assert archive.is_dir()


def test_remove_payload_deletes_file(fs: Path):
    mid = "m-1"
    ensure_partition_dir(fs, mid)
    p = partition_path(fs, mid)
    p.write_text("{}")
    assert MetadataIndex.remove_payload(fs, mid)
    assert not p.exists()
    # idempotent-ish: returns False if missing
    assert MetadataIndex.remove_payload(fs, mid) is False


def test_remove_payload_missing_returns_false(fs: Path):
    assert MetadataIndex.remove_payload(fs, "ghost") is False


# --------------------------------------------------------------------- #
# DB file / schema
# --------------------------------------------------------------------- #

def test_db_file_created_on_disk(tmp_path: Path):
    db = tmp_path / "state.db"
    assert not db.exists()
    MetadataIndex(db)
    assert db.exists()


def test_db_persists_across_connections(tmp_path: Path):
    db = tmp_path / "state.db"
    with MetadataIndex(db) as idx:
        idx.insert("m-1", state=MessageState.QUEUED)
    with MetadataIndex(db) as idx2:
        assert idx2.get_state("m-1") == "QUEUED"


def test_in_memory_index():
    idx = MetadataIndex(":memory:")
    idx.insert("m-1", state=MessageState.CREATED)
    assert idx.get_state("m-1") == "CREATED"
    idx.close()


def test_indexes_exist(tmp_path: Path):
    db = tmp_path / "state.db"
    MetadataIndex(db)
    # open a raw connection to inspect sqlite_master
    conn = sqlite3.connect(str(db))
    names = {
        r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        )
    }
    conn.close()
    for expected in (
        "idx_messages_state", "idx_messages_remote",
        "idx_messages_capability", "idx_messages_remote_task_id",
        "idx_messages_created_at", "idx_messages_processed",
    ):
        assert expected in names, f"missing index {expected}"


# --------------------------------------------------------------------- #
# Atomic writes preserved (partition path used by the daemon writes .tmp + replace)
# --------------------------------------------------------------------- #

def test_partitioned_atomic_write_leaves_no_tmp(fs: Path):
    """A write-then-rename into a partition dir leaves no ``.tmp_*`` files."""
    from federation.lifecycle import atomic_write_json
    mid = "m-atomic"
    dest = partition_path(fs, mid)
    atomic_write_json(dest, {"message_id": mid, "payload": "x"})
    assert dest.exists()
    # no leftover temp files anywhere under the partition
    tmps = list(dest.parent.glob(".tmp_*"))
    assert tmps == []


# --------------------------------------------------------------------- #
# Thread-safety smoke (two threads insert + count)
# --------------------------------------------------------------------- #

def test_thread_safety_smoke(idx: MetadataIndex):
    import threading
    errs: list[Exception] = []

    def worker(start: int) -> None:
        try:
            for i in range(start, start + 50):
                idx.insert(f"m-{i}", state=MessageState.QUEUED)
                idx.count_in_flight()
        except Exception as e:  # noqa: BLE001
            errs.append(e)

    threads = [threading.Thread(target=worker, args=(s,)) for s in (0, 50, 100)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errs == []
    assert idx.count_in_flight() == 150


# --------------------------------------------------------------------- #
# close / context manager
# --------------------------------------------------------------------- #

def test_close_is_idempent(idx: MetadataIndex):
    idx.close()
    idx.close()  # no error