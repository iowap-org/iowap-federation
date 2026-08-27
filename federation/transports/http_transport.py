"""HTTP/HTTPS federation transport backend (V1).

T-095 fills in the real HTTP logic. For now this is a stub that raises
NotImplementedError so the factory and protocol contract are testable.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from federation.transport import Transport


class HttpTransport(Transport):
    """HTTP transport — POST/Poll relay, relay_client-based (T-095)."""

    def __init__(self, cfg: dict[str, Any]) -> None:
        # Bypass the Transport base __init__ guard (which only blocks
        # direct instantiation of Transport itself).
        self.cfg = cfg

    def connect(self, url: str, token: str) -> None:
        raise NotImplementedError("HttpTransport.connect — T-095")

    def sync_outbox(self, outbox_dir: Path) -> list[str]:
        raise NotImplementedError("HttpTransport.sync_outbox — T-095")

    def sync_inbox(self, inbox_dir: Path) -> list[str]:
        raise NotImplementedError("HttpTransport.sync_inbox — T-095")

    def discover_capabilities(self) -> list[dict[str, Any]]:
        raise NotImplementedError("HttpTransport.discover_capabilities — T-095")