"""SQLite metadata index + hash-partitioned file layout (T-103).

The federation node stores **message payloads** as files (inbox/outbox) and
**message metadata** in a small SQLite database. Files are hash-partitioned
by the first two hex chars of ``SHA256(message_id)`` so a single directory
never grows without bound::

    outbox/<SHA256(message_id)[:2]>/<message_id>.json
    inbox/<SHA256(message_id)[:2]>/<message_id>.json
    archive/<message_id>.json            # flat (terminal-state, pruned later)

Schema (master plan, locked)::

    messages(
        message_id      TEXT PRIMARY KEY,
        state           TEXT NOT NULL,        -- MessageState value
        type            TEXT,                 -- "task" | "result"
        remote          TEXT,                 -- peer id / node name
        capability      TEXT,
        retry_count     INTEGER NOT NULL DEFAULT 0,
        created_at      TEXT NOT NULL,        -- RFC3339 / ISO8601 UTC
        updated_at      TEXT NOT NULL,
        size            INTEGER,              -- payload file size in bytes
        remote_task_id  TEXT,                  -- correlation id (our stage_id)
        schema_hash     TEXT,                 -- optional interface fingerprint
        processed       INTEGER NOT NULL DEFAULT 0  -- receiver-side dedupe flag
    )

Index operations (master plan contract):

    insert, get_state, update_state, count_in_flight, count_today,
    increment_retry, mark_processed, prune_ttl

plus a few helpers the daemon/transport need to wire it in:
``get``, ``get_by_remote_task_id``, ``is_processed``, ``archive_terminal``,
``delete``.

Thread-safety: SQLite is opened with ``check_same_thread=False`` and every
operation takes an internal ``threading.Lock`` — the daemon drives it from
four threads (heartbeat / claim / sync / inbox). The lock is held for the
whole statement so we get serialised access without relying on WAL.
"""

from __future__ import annotations

import hashlib
import logging
import os
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from federation.lifecycle import MessageState, TERMINAL_STATES

log = logging.getLogger("federation.index")

# States that count as "in flight": created but not yet terminal and not
# fully completed/archived. COMPLETED is excluded (it's done, just not yet
# archived); ARCHIVED/FAILED/EXPIRED/REJECTED are terminal.
IN_FLIGHT_STATES: frozenset[str] = frozenset(
    {
        MessageState.CREATED.value,
        MessageState.ENCRYPTED.value,
        MessageState.QUEUED.value,
        MessageState.SENT.value,
        MessageState.PENDING_REMOTE_RESPONSE.value,
        MessageState.ACKED.value,
    }
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
    message_id      TEXT PRIMARY KEY,
    state           TEXT NOT NULL,
    type            TEXT,
    remote          TEXT,
    capability      TEXT,
    retry_count     INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    size            INTEGER,
    remote_task_id  TEXT,
    schema_hash     TEXT,
    processed       INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_messages_state          ON messages(state);
CREATE INDEX IF NOT EXISTS idx_messages_remote         ON messages(remote);
CREATE INDEX IF NOT EXISTS idx_messages_capability     ON messages(capability);
CREATE INDEX IF NOT EXISTS idx_messages_remote_task_id ON messages(remote_task_id);
CREATE INDEX IF NOT EXISTS idx_messages_created_at     ON messages(created_at);
CREATE INDEX IF NOT EXISTS idx_messages_processed      ON messages(processed);
"""


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _utcnow_iso() -> str:
    return _utcnow().isoformat()


def _today_prefix() -> str:
    """UTC date prefix (``YYYY-MM-DD``) for ``count_today``."""
    return _utcnow().strftime("%Y-%m-%d")


def _state_value(state: MessageState | str) -> str:
    """Normalise a state to its stored string value."""
    if isinstance(state, MessageState):
        return state.value
    return str(state)


# ---------------------------------------------------------------------------
# Hash partitioning
# ---------------------------------------------------------------------------

def partition_subdir(message_id: str, *, depth: int = 2) -> str:
    """Return the 2-char (default) SHA256 partition subdir for ``message_id``.

    ``SHA256(message_id)[:2]`` → 256 buckets (``00``..``ff``). ``depth`` is
    exposed so a future layout can widen the prefix without a rewrite.
    """
    return hashlib.sha256(message_id.encode("utf-8")).hexdigest()[:depth]


def partition_dir(base: Path | str, message_id: str, *, depth: int = 2) -> Path:
    """Return ``<base>/<partition_subdir>`` (not created)."""
    return Path(base) / partition_subdir(message_id, depth=depth)


def partition_path(
    base: Path | str, message_id: str, *, suffix: str = ".json", depth: int = 2
) -> Path:
    """Return the partitioned file path ``<base>/<xx>/<message_id><suffix>``.

    The directory is **not** created here; callers that write use
    :func:`ensure_partition_dir` (or the atomic writers, which ``mkdir``).
    """
    return partition_dir(base, message_id, depth=depth) / f"{message_id}{suffix}"


def ensure_partition_dir(base: Path | str, message_id: str, *, depth: int = 2) -> Path:
    """Create + return the partition directory for ``message_id``."""
    d = partition_dir(base, message_id, depth=depth)
    d.mkdir(parents=True, exist_ok=True)
    return d


# ---------------------------------------------------------------------------
# MetadataIndex
# ---------------------------------------------------------------------------

class MetadataIndex:
    """SQLite-backed metadata index for federation messages.

    One instance per ``state.db`` (per remote, per the master-plan file
    layout). Thread-safe; the connection is opened with
    ``check_same_thread=False`` and every call is serialised by an internal
    lock so the daemon's four threads can share it.

    Parameters
    ----------
    db_path:
        Path to ``state.db``. Parent dirs are created. Pass ``:memory:`` (or
        a path whose name is ``:memory:``) for an in-memory index used by
        tests — note SQLite ``:memory:`` is process-local but shared across
        threads on the same connection here.
    """

    def __init__(self, db_path: Path | str) -> None:
        self.db_path = Path(db_path) if not isinstance(db_path, str) or db_path != ":memory:" else db_path
        if isinstance(self.db_path, Path):
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(
            str(self.db_path),
            check_same_thread=False,
            isolation_level=None,  # autocommit; we manage txns explicitly below
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(_SCHEMA)
        # PRAGMAs for durability on cheap single-writer workloads.
        try:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
        except sqlite3.OperationalError:
            # WAL may be unavailable (e.g. :memory:) — fall back silently.
            pass

    # ------------------------------------------------------------------ #
    # lifecycle
    # ------------------------------------------------------------------ #

    def close(self) -> None:
        with self._lock:
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001
                pass

    def __enter__(self) -> "MetadataIndex":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ------------------------------------------------------------------ #
    # write operations
    # ------------------------------------------------------------------ #

    def insert(
        self,
        message_id: str,
        *,
        state: MessageState | str = MessageState.CREATED,
        type: str | None = None,
        remote: str | None = None,
        capability: str | None = None,
        size: int | None = None,
        remote_task_id: str | None = None,
        schema_hash: str | None = None,
        created_at: str | None = None,
    ) -> bool:
        """Insert a message row. Idempotent on ``message_id`` (INSERT OR IGNORE).

        Returns ``True`` if a row was inserted, ``False`` if it already
        existed (no overwrite — use :meth:`update_state` to mutate).
        ``created_at`` defaults to now (UTC ISO8601); ``updated_at`` is set
        to the same value.
        """
        if not message_id:
            raise ValueError("message_id must be non-empty")
        ts = created_at or _utcnow_iso()
        with self._lock:
            cur = self._conn.execute(
                "INSERT OR IGNORE INTO messages "
                "(message_id, state, type, remote, capability, retry_count, "
                " created_at, updated_at, size, remote_task_id, schema_hash, processed) "
                "VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, 0)",
                (
                    message_id,
                    _state_value(state),
                    type,
                    remote,
                    capability,
                    ts,
                    ts,
                    size,
                    remote_task_id,
                    schema_hash,
                ),
            )
            return cur.rowcount > 0

    def update_state(
        self, message_id: str, state: MessageState | str
    ) -> bool:
        """Set ``state`` + refresh ``updated_at``. Returns ``True`` if a row
        was updated, ``False`` if the message_id is unknown."""
        with self._lock:
            cur = self._conn.execute(
                "UPDATE messages SET state = ?, updated_at = ? WHERE message_id = ?",
                (_state_value(state), _utcnow_iso(), message_id),
            )
            return cur.rowcount > 0

    def increment_retry(self, message_id: str) -> int:
        """Atomically ``retry_count += 1`` and return the new count.

        Returns ``-1`` if the message_id is unknown (caller should treat as
        "not tracked"). Also refreshes ``updated_at``.
        """
        with self._lock:
            cur = self._conn.execute(
                "UPDATE messages SET retry_count = retry_count + 1, updated_at = ? "
                "WHERE message_id = ?",
                (_utcnow_iso(), message_id),
            )
            if cur.rowcount == 0:
                return -1
            row = self._conn.execute(
                "SELECT retry_count FROM messages WHERE message_id = ?",
                (message_id,),
            ).fetchone()
            return int(row["retry_count"]) if row else -1

    def mark_processed(self, message_id: str) -> bool:
        """Mark a message as processed on the receiver side (dedupe flag).

        Returns ``True`` if the flag was newly set, ``False`` if the message
        was unknown or already processed. Combined with :meth:`is_processed`
        this is the SQLite-backed replay guard for receivers that prefer
        durable dedupe over the in-memory :class:`ReplayGuard` (both can
        coexist; the listener uses the in-memory guard, the index offers a
        durable alternative for crash recovery).
        """
        with self._lock:
            cur = self._conn.execute(
                "UPDATE messages SET processed = 1, updated_at = ? "
                "WHERE message_id = ? AND processed = 0",
                (_utcnow_iso(), message_id),
            )
            return cur.rowcount > 0

    def delete(self, message_id: str) -> bool:
        """Delete a message row. Returns ``True`` if a row was removed."""
        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM messages WHERE message_id = ?", (message_id,)
            )
            return cur.rowcount > 0

    # ------------------------------------------------------------------ #
    # read operations
    # ------------------------------------------------------------------ #

    def _row_to_dict(self, row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        return {
            "message_id": row["message_id"],
            "state": row["state"],
            "type": row["type"],
            "remote": row["remote"],
            "capability": row["capability"],
            "retry_count": row["retry_count"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "size": row["size"],
            "remote_task_id": row["remote_task_id"],
            "schema_hash": row["schema_hash"],
            "processed": bool(row["processed"]),
        }

    def get(self, message_id: str) -> dict[str, Any] | None:
        """Return the full message row, or ``None``."""
        with self._lock:
            return self._row_to_dict(
                self._conn.execute(
                    "SELECT * FROM messages WHERE message_id = ?", (message_id,)
                ).fetchone()
            )

    def get_state(self, message_id: str) -> str | None:
        """Return the state string, or ``None`` if unknown."""
        with self._lock:
            row = self._conn.execute(
                "SELECT state FROM messages WHERE message_id = ?", (message_id,)
            ).fetchone()
            return row["state"] if row else None

    def is_processed(self, message_id: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT processed FROM messages WHERE message_id = ?", (message_id,)
            ).fetchone()
            return bool(row["processed"]) if row else False

    def get_by_remote_task_id(self, remote_task_id: str) -> dict[str, Any] | None:
        """Find a message by its ``remote_task_id`` (reply correlation).

        Returns the first match (remote_task_id is not unique across remotes
        in theory, but for a per-remote index it is). ``None`` if not found.
        """
        with self._lock:
            return self._row_to_dict(
                self._conn.execute(
                    "SELECT * FROM messages WHERE remote_task_id = ? "
                    "ORDER BY created_at DESC LIMIT 1",
                    (remote_task_id,),
                ).fetchone()
            )

    # ------------------------------------------------------------------ #
    # aggregate queries
    # ------------------------------------------------------------------ #

    def count_in_flight(
        self,
        *,
        remote: str | None = None,
        capability: str | None = None,
    ) -> int:
        """Count messages in a non-terminal, non-completed state.

        ``IN_FLIGHT_STATES`` = CREATED, ENCRYPTED, QUEUED, SENT,
        PENDING_REMOTE_RESPONSE, ACKED. Optional ``remote`` / ``capability``
        filters scope the count to one peer/capability (the daemon uses this
        for the heartbeat ``in_flight`` map and the status file).
        """
        clauses = [f"state IN ({_placeholders(IN_FLIGHT_STATES)})"]
        params: list[Any] = list(IN_FLIGHT_STATES)
        if remote is not None:
            clauses.append("remote = ?")
            params.append(remote)
        if capability is not None:
            clauses.append("capability = ?")
            params.append(capability)
        where = " AND ".join(clauses)
        with self._lock:
            row = self._conn.execute(
                f"SELECT COUNT(*) AS n FROM messages WHERE {where}", params
            ).fetchone()
            return int(row["n"]) if row else 0

    def count_today(
        self,
        *,
        remote: str | None = None,
        capability: str | None = None,
    ) -> int:
        """Count messages created today (UTC).

        Uses a prefix match on ``created_at`` (``YYYY-MM-DD``) so the index
        on ``created_at`` is usable. The daemon uses this for fair-use daily
        accounting when an index is wired in.
        """
        prefix = _today_prefix()
        clauses = ["created_at LIKE ?"]
        params: list[Any] = [f"{prefix}%"]
        if remote is not None:
            clauses.append("remote = ?")
            params.append(remote)
        if capability is not None:
            clauses.append("capability = ?")
            params.append(capability)
        where = " AND ".join(clauses)
        with self._lock:
            row = self._conn.execute(
                f"SELECT COUNT(*) AS n FROM messages WHERE {where}", params
            ).fetchone()
            return int(row["n"]) if row else 0

    def count_in_state(
        self,
        state: MessageState | str,
        *,
        remote: str | None = None,
    ) -> int:
        """Count messages in a specific state (used by the status file)."""
        clauses = ["state = ?"]
        params: list[Any] = [_state_value(state)]
        if remote is not None:
            clauses.append("remote = ?")
            params.append(remote)
        where = " AND ".join(clauses)
        with self._lock:
            row = self._conn.execute(
                f"SELECT COUNT(*) AS n FROM messages WHERE {where}", params
            ).fetchone()
            return int(row["n"]) if row else 0

    # ------------------------------------------------------------------ #
    # maintenance
    # ------------------------------------------------------------------ #

    def prune_ttl(self, ttl_seconds: float) -> int:
        """Delete processed rows older than ``ttl_seconds``.

        "Processed" = the receiver has handled the message
        (``processed = 1``). Age is measured against ``created_at`` (when the
        message arrived), mirroring the in-memory :class:`ReplayCache.prune`
        semantics — a TTL is about message age, not "time since last state
        bump". Returns the number of rows deleted. This bounds the index
        size for long-running nodes.

        Also deletes terminal-state rows older than the TTL so the index
        does not accumulate archived/failed/expired/rejected metadata
        forever (aggressive cleanup per the master plan).
        """
        # created_at is ISO8601 with timezone; comparing lexicographically
        # against an ISO8601 cutoff is safe.
        cutoff_dt = datetime.fromtimestamp(
            _utcnow().timestamp() - ttl_seconds, tz=timezone.utc
        )
        cutoff = cutoff_dt.isoformat()
        terminal = [s.value for s in TERMINAL_STATES]
        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM messages WHERE "
                " (processed = 1 AND created_at < ?) "
                " OR (state IN (" + ",".join("?" * len(terminal)) + ") AND created_at < ?)",
                [cutoff, *terminal, cutoff],
            )
            return int(cur.rowcount)

    def terminal_messages(
        self, *, remote: str | None = None, limit: int = 1000
    ) -> list[dict[str, Any]]:
        """Return rows in a terminal state (for archival sweeps).

        The daemon's cleanup cycle calls this, archives the matching payload
        files, then ``delete()``s the rows (or lets ``prune_ttl`` reap them).
        """
        terminal = [s.value for s in TERMINAL_STATES]
        clauses = [f"state IN ({_placeholders(terminal)})"]
        params: list[Any] = list(terminal)
        if remote is not None:
            clauses.append("remote = ?")
            params.append(remote)
        where = " AND ".join(clauses)
        params.append(limit)
        with self._lock:
            rows = self._conn.execute(
                f"SELECT * FROM messages WHERE {where} ORDER BY updated_at ASC LIMIT ?",
                params,
            ).fetchall()
            return [self._row_to_dict(r) for r in rows if r is not None]  # type: ignore[arg-type]

    # ------------------------------------------------------------------ #
    # archive helper (file side — the index owns the "which files to reap"
    # question; this helper performs the atomic move)
    # ------------------------------------------------------------------ #

    @staticmethod
    def archive_payload(
        base_dir: Path | str,
        message_id: str,
        archive_dir: Path | str,
        *,
        suffix: str = ".json",
    ) -> Path | None:
        """Move a partitioned payload file to ``archive_dir`` (flat).

        Returns the archive path on success, ``None`` if the source file did
        not exist. Uses ``os.replace`` (atomic on the same filesystem); falls
        back to copy+delete across filesystems. The archive directory is
        created if needed. This is the "aggressive cleanup/archival after
        terminal state" primitive.
        """
        src = partition_path(base_dir, message_id, suffix=suffix)
        if not src.exists():
            return None
        archive_dir = Path(archive_dir)
        archive_dir.mkdir(parents=True, exist_ok=True)
        dest = archive_dir / f"{message_id}{suffix}"
        try:
            src.replace(dest)
        except OSError:
            try:
                data = src.read_bytes()
                fd, tmp = _mktemp(archive_dir, message_id, suffix)
                try:
                    with os.fdopen(fd, "wb") as f:
                        f.write(data)
                    os.replace(tmp, dest)
                finally:
                    try:
                        src.unlink(missing_ok=True)
                    except OSError:
                        pass
            except OSError as exc:
                log.warning("archive_payload failed for %s: %s", message_id, exc)
                return None
        return dest

    @staticmethod
    def remove_payload(
        base_dir: Path | str, message_id: str, *, suffix: str = ".json"
    ) -> bool:
        """Remove a partitioned payload file. Returns ``True`` if removed."""
        src = partition_path(base_dir, message_id, suffix=suffix)
        if not src.exists():
            return False
        try:
            src.unlink()
            return True
        except OSError:
            return False


def _placeholders(values: Iterable[Any]) -> str:
    """Return a ``?,?,?`` placeholder list for ``IN (...)`` clauses."""
    return ",".join("?" for _ in values)


def _mktemp(archive_dir: Path, message_id: str, suffix: str) -> tuple[int, str]:
    import tempfile
    return tempfile.mkstemp(
        prefix=f".tmp_{message_id}_", dir=str(archive_dir), suffix=suffix
    )