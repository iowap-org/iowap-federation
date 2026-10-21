"""Transport registry: self-registration + directory tree (T-109).

A transport registers itself by dropping ``registry/<name>.yaml`` into the
federation root; the presence of the file means "registered", removing it
means "unregistered". No config edit is needed to add or remove a transport.

Registering a transport creates its directory tree under the registry's
root (which IS the federation root)::

    <root>/
    ├── registry/<name>.yaml
    ├── inbox/<name>/
    ├── outbox/<name>/
    ├── status/<name>/
    └── media/<name>/incoming/
        media/<name>/outgoing/

On unregister, inbox/outbox are KEPT (in-flight files may still need
draining); status/media are removed and recreated on re-registration.
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

from federation.manifest import TransportManifest, scan_registry

log = logging.getLogger("federation.registry")


class TransportRegistry:
    """Registry of known transports + their directory trees.

    ``root`` is the federation root: all registry trees
    (``registry/``, ``inbox/<name>/``, ...) live under it. The registry
    derives every path from ``self.root`` and never uses the module-level
    paths helpers, so it is testable against any directory without
    monkeypatching ``federation.paths.BASE_DIR``.
    """

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)
        self.transports: dict[str, TransportManifest] = {}

    # -- lifecycle ------------------------------------------------------

    def register(self, name: str, manifest: TransportManifest | None = None) -> None:
        """Register a transport and create its directory tree.

        Existing directories are left alone. Logs INFO ``transport %s registered``.
        """
        raise NotImplementedError

    def unregister(self, name: str) -> None:
        """Remove a transport; keep inbox/outbox, drop status/media.

        Logs INFO ``transport %s unregistered``.
        """
        raise NotImplementedError

    def reconcile(self) -> None:
        """Diff the on-disk registry against the known transports.

        Scans ``self.root / "registry"``; registers new manifests, unregisters
        removed ones.
        """
        raise NotImplementedError

    # -- routing --------------------------------------------------------

    def route_for(self, raw_route: str) -> TransportManifest | None:
        """Return the manifest for a ``<transport>~<recipient>`` route.

        Returns ``None`` when the route is malformed or the transport is not
        registered.
        """
        raise NotImplementedError

    def max_task_bytes(self, raw_route: str) -> int | None:
        """Return the declared task-size limit for a route (``None`` = none)."""
        raise NotImplementedError