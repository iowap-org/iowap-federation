"""Federation directory layout.

Per remote connection:
    ~/.relay/federation/<remote-relay-name>/
    ├── inbox/      # results from remote relay → node processes → completes stage
    └── outbox/     # task forwards → wrapper syncs out
"""

from __future__ import annotations

from pathlib import Path

# Mirrors nodes.common.node_utils.BASE_DIR (~/.relay). Overridable in tests.
BASE_DIR = Path.home() / ".relay"


def federation_root() -> Path:
    return BASE_DIR / "federation"


def remote_dirs(remote_name: str) -> tuple[Path, Path]:
    """Return (inbox, outbox) for a remote connection. Does not create."""
    root = federation_root() / remote_name
    return root / "inbox", root / "outbox"


def ensure_federation_dirs(remote_name: str) -> tuple[Path, Path]:
    """Create and return (inbox, outbox) for a remote connection."""
    inbox, outbox = remote_dirs(remote_name)
    inbox.mkdir(parents=True, exist_ok=True)
    outbox.mkdir(parents=True, exist_ok=True)
    return inbox, outbox