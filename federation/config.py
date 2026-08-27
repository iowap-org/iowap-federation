"""Peer configuration loading for the Federation Node.

Reads ``~/.relay/node.yaml`` and exposes the ``federation.peers`` list plus
the node's own capability offer. T-095 only needs:

* the peer record (id, endpoint, token) for the HTTP transport, and
* the local capability offer list for the listener's ``/fed/v1/discover``.

PyYAML is already a dependency (used by the node framework). Keys not present
in the config default to safe empties so tests can pass minimal dicts.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from federation import paths as _paths


@dataclass(frozen=True)
class Peer:
    """A single configured remote FedNode peer."""

    id: str
    endpoint: str
    token: str
    # E2EE keys land in T-101; carried through config unchanged.
    peer_public_key: str | None = None
    my_public_key: str | None = None

    def __post_init__(self) -> None:
        if not self.id:
            raise ValueError("peer id must be non-empty")
        if not self.endpoint:
            raise ValueError(f"peer {self.id!r}: endpoint must be non-empty")
        if not self.token:
            raise ValueError(f"peer {self.id!r}: token must be non-empty")
        # Normalise: no trailing slash on the endpoint, makes url-joining simple.
        if self.endpoint.endswith("/"):
            object.__setattr__(self, "endpoint", self.endpoint.rstrip("/"))

    @property
    def auth_header(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}


@dataclass
class NodeConfig:
    """Resolved Federation Node configuration."""

    node_name: str = "federation"
    peers: list[Peer] = field(default_factory=list)
    # Local capability offer — a list of capability dicts as advertised via
    # ``/fed/v1/discover``. Sourced from ``federation.offer`` in node.yaml.
    offer: list[dict[str, Any]] = field(default_factory=list)
    # The bearer token this node accepts on its own listener. Peers use their
    # own token; we validate incoming requests against the set of peer tokens
    # (any configured peer may talk to us). The ``listener_token`` is an
    # optional override (e.g. for a single shared token); defaults to None,
    # meaning "accept any configured peer token".
    listener_token: str | None = None

    def peer(self, peer_id: str) -> Peer:
        for p in self.peers:
            if p.id == peer_id:
                return p
        raise KeyError(f"unknown peer: {peer_id!r}")

    def accepted_tokens(self) -> set[str]:
        """Tokens this node will accept on its listener."""
        tokens = {p.token for p in self.peers}
        if self.listener_token:
            tokens.add(self.listener_token)
        return tokens


def _node_yaml_path(override: str | Path | None = None) -> Path:
    if override is not None:
        return Path(override)
    return _paths.BASE_DIR / "node.yaml"


def load_node_config(path: str | Path | None = None) -> NodeConfig:
    """Load and validate ``node.yaml`` → ``NodeConfig``.

    Missing file → empty ``NodeConfig`` (lets tests run without a file).
    Missing keys → safe defaults. Invalid peer → ``ValueError``.
    """
    cfg_path = _node_yaml_path(path)
    if not cfg_path.exists():
        return NodeConfig()
    raw = yaml.safe_load(cfg_path.read_text()) or {}
    return load_node_config_from_dict(raw)


def load_node_config_from_dict(raw: dict[str, Any]) -> NodeConfig:
    """Build a ``NodeConfig`` from an already-parsed config dict.

    Separated so tests can avoid touching the filesystem.
    """
    fed_section = raw.get("federation", {}) or {}
    peers_raw = fed_section.get("peers", []) or []
    peers: list[Peer] = []
    for entry in peers_raw:
        peers.append(
            Peer(
                id=entry["id"],
                endpoint=entry["endpoint"],
                token=entry["token"],
                peer_public_key=entry.get("peer_public_key"),
                my_public_key=entry.get("my_public_key"),
            )
        )
    offer = fed_section.get("offer", []) or []
    return NodeConfig(
        node_name=raw.get("node_name", "federation"),
        peers=peers,
        offer=list(offer),
        listener_token=fed_section.get("listener_token"),
    )