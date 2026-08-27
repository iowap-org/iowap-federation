"""Pluggable federation transport backend.

The only component that touches the network. Translates files in
inbox/outbox into transport calls (HTTP, E-Mail, P2P). Carries the
remote_task_id ↔ local stage mapping. A new transport = new class +
factory entry + config line — no node changes.

Note: methods raise ``NotImplementedError`` (rather than being decorated
with ``@abstractmethod``) so that a concrete subclass which does not
override a method gets a clear runtime error on call instead of a
``TypeError`` at instantiation time. The base class itself is kept
non-instantiable via an ``__init__`` guard.
"""

from __future__ import annotations

from abc import ABC
from pathlib import Path
from typing import Any


class Transport(ABC):
    """Abstract federation transport backend."""

    def __init__(self) -> None:
        # The base class is a protocol/contract and not instantiable.
        # Concrete backends (HttpTransport, ...) call super().__init__()
        # implicitly via object.__init__, which does not raise.
        if type(self) is Transport:
            raise TypeError("Transport is abstract; instantiate a concrete backend")

    def connect(self, url: str, token: str) -> None:
        """Establish a session to the remote relay. Raise on failure."""
        raise NotImplementedError

    def sync_outbox(self, outbox_dir: Path) -> list[str]:
        """Send pending outbox task files to the remote. Return ids sent."""
        raise NotImplementedError

    def sync_inbox(self, inbox_dir: Path) -> list[str]:
        """Fetch remote results, write them as inbox files. Return new ids."""
        raise NotImplementedError

    def discover_capabilities(self) -> list[dict[str, Any]]:
        """Return the remote relay's advertised capabilities (for approval)."""
        raise NotImplementedError

    def close(self) -> None:
        """Release resources. No-op by default."""
        pass