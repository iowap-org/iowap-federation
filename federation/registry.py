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
from federation.routing import RouteError, parse_route

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
        manifest = manifest or TransportManifest(name=name)
        self.transports[name] = manifest
        inbox = self.root / "inbox" / name
        outbox = self.root / "outbox" / name
        status = self.root / "status" / name
        media_in = self.root / "media" / name / "incoming"
        media_out = self.root / "media" / name / "outgoing"
        for path in (inbox, outbox, status, media_in, media_out):
            path.mkdir(parents=True, exist_ok=True)
        log.info("transport %s registered", name)

    def unregister(self, name: str) -> None:
        """Remove a transport; keep inbox/outbox, drop status/media.

        Logs INFO ``transport %s unregistered``.
        """
        self.transports.pop(name, None)
        # Inbox/outbox stay: in-flight files may still need draining.
        shutil.rmtree(self.root / "status" / name, ignore_errors=True)
        shutil.rmtree(self.root / "media" / name, ignore_errors=True)
        log.info("transport %s unregistered", name)

    def reconcile(self) -> None:
        """Diff the on-disk registry against the known transports.

        Scans ``self.root / "registry"``; registers new manifests, unregisters
        removed ones.
        """
        manifests = scan_registry(self.root / "registry")
        for name, manifest in manifests.items():
            if name not in self.transports:
                self.register(name, manifest)
        for name in list(self.transports.keys()):
            if name not in manifests:
                self.unregister(name)

    # -- routing --------------------------------------------------------

    def route_for(self, raw_route: str) -> TransportManifest | None:
        """Return the manifest for a ``<transport>~<recipient>`` route.

        Returns ``None`` when the route is malformed or the transport is not
        registered.
        """
        try:
            route = parse_route(raw_route)
        except RouteError:
            return None
        return self.transports.get(route.transport)

    def max_task_bytes(self, raw_route: str) -> int | None:
        """Return the declared task-size limit for a route (``None`` = none)."""
        manifest = self.route_for(raw_route)
        if manifest is None:
            return None
        return manifest.max_task_bytes