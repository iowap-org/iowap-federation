"""Transport manifest: registration + declared limits (T-109).

A transport registers itself by dropping ``registry/<name>.yaml`` into the
federation root. The **presence** of the file means "registered"; removing it
means "unregistered". No config edit is needed to add or remove a transport.

The manifest declares the transport's identity and its **task size limit**
(``max_task_bytes``). That limit is a property of the transport (an e-mail
attachment cap is ~10 MB, a shared folder allows hundreds of MB), and the node
must know it **before** routing a task — otherwise it routes a task the
transport structurally cannot accept.

Layout (mirrors the Hermes bridge adapter's file contract, no code shared)::

    <federation_root>/
    ├── registry/<name>.yaml    # manifest — presence == registered
    ├── inbox/<name>/           # incoming messages (wrapper -> node)
    ├── outbox/<name>/          # outgoing messages (node -> wrapper)
    ├── status/<name>/          # wrapper health heartbeat
    └── media/<name>/incoming/  # attachments
        media/<name>/outgoing/
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

log = logging.getLogger("federation.manifest")

#: Transport names become directory names -> identifier-like only.
_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


class ManifestError(ValueError):
    """Raised when a manifest is malformed."""


@dataclass(frozen=True)
class TransportManifest:
    """A registered transport.

    ``max_task_bytes`` is the maximum size of a single task payload this
    transport can carry (``None`` = no declared limit).
    """

    name: str
    service: str = ""
    target_format: tuple[str, ...] = ()
    capabilities: tuple[str, ...] = ()
    max_task_bytes: int | None = None

    def accepts_target(self, target: str) -> bool:
        """Check a recipient address against the declared target formats.

        With no ``target_format`` declared the transport is permissive.
        Otherwise the target must match at least one declared format:
        ``email`` (contains ``@``), ``phone`` (contains a digit),
        ``chat_id`` (any non-empty address).
        """
        if not target:
            return False
        if not self.target_format:
            return True
        if "email" in self.target_format and "@" in target:
            return True
        if "phone" in self.target_format and any(c.isdigit() for c in target):
            return True
        if "chat_id" in self.target_format:
            return True
        return False


def _as_list(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, (list, tuple)):
        return tuple(str(v) for v in value)
    return (str(value),)


def load_manifest(data: dict[str, Any]) -> TransportManifest:
    """Build a :class:`TransportManifest` from a parsed YAML mapping."""
    if not isinstance(data, dict):
        raise ManifestError(f"manifest must be a mapping, got {type(data).__name__}")

    name = str(data.get("name", "")).strip()
    if not name:
        raise ManifestError("manifest missing 'name'")
    if not _NAME_RE.match(name):
        raise ManifestError(
            f"invalid manifest name {name!r} "
            "(must be an identifier: letters, digits, '.', '_', '-')"
        )

    max_task_bytes = data.get("max_task_bytes")
    if max_task_bytes is not None:
        try:
            max_task_bytes = int(max_task_bytes)
        except (TypeError, ValueError) as exc:
            raise ManifestError(
                f"max_task_bytes must be an integer: {max_task_bytes!r}"
            ) from exc
        if max_task_bytes < 0:
            raise ManifestError(
                f"max_task_bytes must be >= 0: {max_task_bytes}"
            )

    return TransportManifest(
        name=name,
        service=str(data.get("service", "")).strip() or name,
        target_format=_as_list(data.get("target_format")),
        capabilities=_as_list(data.get("capabilities")),
        max_task_bytes=max_task_bytes,
    )


def read_manifest(path: Path) -> TransportManifest:
    """Read + parse a manifest YAML file."""
    with open(path, "r", encoding="utf-8") as f:
        return load_manifest(yaml.safe_load(f) or {})


def scan_registry(registry_dir: Path) -> dict[str, TransportManifest]:
    """Return ``{transport_name: TransportManifest}`` from ``*.yaml`` files.

    A missing directory yields an empty mapping. A malformed manifest is
    skipped with a warning — one broken file must not take down all transports.
    """
    result: dict[str, TransportManifest] = {}
    registry_dir = Path(registry_dir)
    if not registry_dir.exists():
        return result
    for f in sorted(registry_dir.glob("*.yaml")):
        try:
            m = read_manifest(f)
        except (OSError, ManifestError, yaml.YAMLError) as exc:
            log.warning("bad transport manifest %s: %s", f, exc)
            continue
        result[m.name] = m
    return result