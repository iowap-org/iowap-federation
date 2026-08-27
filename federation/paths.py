"""Federation directory layout.

Per remote connection (T-103 adds archive/ + state.db + hash partitioning)::

    ~/.relay/federation/<remote-relay-name>/
    ├── inbox/      # results/tasks from remote (hash-partitioned after T-103)
    │   └── <SHA256(message_id)[:2]>/<message_id>.json
    ├── outbox/    # task/results to send (hash-partitioned after T-103)
    │   └── <SHA256(message_id)[:2]>/<message_id>.json
    ├── archive/   # terminal-state files (T-102/T-103), flat
    └── state.db   # SQLite metadata index (T-103)
"""

from __future__ import annotations

from pathlib import Path

# Mirrors nodes.common.node_utils.BASE_DIR (~/.relay). Overridable in tests.
BASE_DIR = Path.home() / ".relay"


def federation_root() -> Path:
    return BASE_DIR / "federation"


def remote_dirs(remote_name: str) -> tuple[Path, Path]:
    """Return ``(inbox, outbox)`` for a remote connection. Does not create."""
    root = federation_root() / remote_name
    return root / "inbox", root / "outbox"


def remote_root(remote_name: str) -> Path:
    """Root directory for one remote connection. Does not create."""
    return federation_root() / remote_name


def archive_dir(remote_name: str) -> Path:
    """Return the ``archive/`` dir for a remote connection. Does not create."""
    return remote_root(remote_name) / "archive"


def state_db_path(remote_name: str) -> Path:
    """Return the ``state.db`` SQLite path for a remote connection.

    Does not create parent dirs — :class:`federation.index.MetadataIndex`
    creates them on open.
    """
    return remote_root(remote_name) / "state.db"


def ensure_federation_dirs(remote_name: str) -> tuple[Path, Path]:
    """Create and return ``(inbox, outbox)`` for a remote connection.

    Also ensures the ``archive/`` directory exists (T-103). The partition
    subdirectories under inbox/outbox are created lazily by the writers in
    :mod:`federation.index`.
    """
    inbox, outbox = remote_dirs(remote_name)
    inbox.mkdir(parents=True, exist_ok=True)
    outbox.mkdir(parents=True, exist_ok=True)
    archive_dir(remote_name).mkdir(parents=True, exist_ok=True)
    return inbox, outbox