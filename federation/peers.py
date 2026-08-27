"""Peer management for the Federation Node (T-097).

Mutates the ``federation.peers`` list inside ``~/.relay/node.yaml`` so operators
don't have to hand-edit YAML. All writes are atomic (tmp + ``os.replace``) and
preserve any other sections present in the config file.

The CLI (``federation/cli.py``) is a thin argparse front-end over the
``PeerManager`` defined here.

Design notes
------------
* Reads/writes the raw YAML dict so unrelated keys (``node_name``, ``offer``,
  ``listener_token``, arbitrary user keys) survive round-trips.
* ``add()`` validates through :class:`federation.config.Peer` so the same
  rules apply as at load time (non-empty id/endpoint/token, trailing-slash
  normalisation).
* ``connect`` / ``disconnect`` toggle an ``active`` flag on the peer record —
  the daemon (T-096) only talks to peers whose ``active`` is truthy. ``connect``
  is a pure metadata operation; live reachability verification is left to the
  daemon's sync loop (and a future ``verify`` subcommand).
* Tokens are **never** returned by ``list()`` in a form meant for display; the
  CLI redacts them. ``PeerManager.list()`` returns the raw records (including
  the token) for internal callers, but the public CLI surface redacts.
"""

from __future__ import annotations

import os
from dataclasses import asdict
from pathlib import Path
from typing import Any

import yaml

from federation import paths as _paths
from federation.config import Peer, _node_yaml_path


class PeerError(Exception):
    """Base class for peer-management errors."""


class PeerExistsError(PeerError):
    """Raised by :meth:`PeerManager.add` when the peer id is already present."""

    def __init__(self, peer_id: str) -> None:
        super().__init__(f"peer {peer_id!r} already exists")
        self.peer_id = peer_id


class PeerNotFoundError(PeerError):
    """Raised when a peer id is unknown to remove/connect/disconnect."""

    def __init__(self, peer_id: str) -> None:
        super().__init__(f"peer {peer_id!r} not found")
        self.peer_id = peer_id


class PeerManager:
    """Read/modify the ``federation.peers`` list in ``node.yaml``.

    Parameters
    ----------
    path:
        Optional override for the config file location. Defaults to
        ``~/.relay/node.yaml`` (via :func:`federation.paths.BASE_DIR`).
    """

    def __init__(self, path: str | Path | None = None) -> None:
        self.path: Path = _node_yaml_path(path)

    # -- internals ---------------------------------------------------------

    def _load_raw(self) -> dict[str, Any]:
        if not self.path.exists():
            return {}
        return yaml.safe_load(self.path.read_text()) or {}

    def _save_raw(self, raw: dict[str, Any]) -> None:
        """Atomically write the full config back, preserving key order."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_name(self.path.name + ".tmp")
        tmp.write_text(yaml.safe_dump(raw, sort_keys=False, default_flow_style=False))
        os.replace(tmp, self.path)

    def _fed_section(self, raw: dict[str, Any]) -> dict[str, Any]:
        """Return the ``federation`` section, creating an empty one if absent.

        Handles the case where the key exists but is explicitly ``None`` (which
        ``yaml.safe_load`` can produce) by replacing it with a fresh dict so
        downstream mutations persist on ``raw``.
        """
        fed = raw.get("federation")
        if fed is None:
            fed = {}
            raw["federation"] = fed
        return fed

    def _peers_list(self, raw: dict[str, Any]) -> list[dict[str, Any]]:
        """Return the live ``peers`` list, creating it if missing/None."""
        fed = self._fed_section(raw)
        peers = fed.get("peers")
        if not isinstance(peers, list):
            peers = []
            fed["peers"] = peers
        return peers

    def _find(self, raw: dict[str, Any], peer_id: str) -> dict[str, Any] | None:
        for p in self._peers_list(raw):
            if p.get("id") == peer_id:
                return p
        return None

    # -- public API --------------------------------------------------------

    def add(
        self,
        id: str,
        endpoint: str,
        token: str,
        pubkey: str | None = None,
        *,
        my_public_key: str | None = None,
    ) -> dict[str, Any]:
        """Add a new peer. Raises :class:`PeerExistsError` on duplicate id.

        Validates via :class:`federation.config.Peer` so endpoint/token rules
        match the loader. The ``pubkey`` positional is stored as
        ``peer_public_key`` (the remote's X25519 public key for E2EE in T-101).
        """
        # Validate first; raises ValueError on bad input (no need to persist).
        peer = Peer(
            id=id,
            endpoint=endpoint,
            token=token,
            peer_public_key=pubkey,
            my_public_key=my_public_key,
        )
        raw = self._load_raw()
        if self._find(raw, id) is not None:
            raise PeerExistsError(id)
        record: dict[str, Any] = {
            "id": peer.id,
            "endpoint": peer.endpoint,  # normalised (no trailing slash)
            "token": peer.token,
        }
        if pubkey is not None:
            record["peer_public_key"] = pubkey
        if my_public_key is not None:
            record["my_public_key"] = my_public_key
        self._peers_list(raw).append(record)
        self._save_raw(raw)
        return record

    def remove(self, id: str) -> dict[str, Any]:
        """Remove a peer by id. Returns the removed record.

        Raises :class:`PeerNotFoundError` if the id is unknown.
        """
        raw = self._load_raw()
        peers = self._peers_list(raw)
        for i, p in enumerate(peers):
            if p.get("id") == id:
                removed = peers.pop(i)
                self._save_raw(raw)
                return removed
        raise PeerNotFoundError(id)

    def list(self) -> list[dict[str, Any]]:
        """Return all peer records (unredacted — internal callers only).

        The CLI layer is responsible for redacting tokens before display.
        """
        raw = self._load_raw()
        fed = raw.get("federation", {}) or {}
        return [dict(p) for p in (fed.get("peers") or [])]

    def get(self, id: str) -> dict[str, Any]:
        """Return one peer record (unredacted). Raises if unknown."""
        raw = self._load_raw()
        p = self._find(raw, id)
        if p is None:
            raise PeerNotFoundError(id)
        return dict(p)

    def connect(self, id: str) -> dict[str, Any]:
        """Mark a peer as active (``active: true``).

        Pure metadata: the daemon's sync loop will only forward to active
        peers. Live reachability verification happens there, not here.
        Returns the updated record (unredacted).
        """
        raw = self._load_raw()
        p = self._find(raw, id)
        if p is None:
            raise PeerNotFoundError(id)
        p["active"] = True
        self._save_raw(raw)
        return dict(p)

    def disconnect(self, id: str) -> dict[str, Any]:
        """Mark a peer as inactive (``active: false``).

        Leaves the record in place so it can be re-connected later.
        """
        raw = self._load_raw()
        p = self._find(raw, id)
        if p is None:
            raise PeerNotFoundError(id)
        p["active"] = False
        self._save_raw(raw)
        return dict(p)


def redact(record: dict[str, Any]) -> dict[str, Any]:
    """Return a display-safe copy of a peer record with the token hidden.

    The token field is replaced by a short presence marker (``"<redacted>"``)
    so operators can see *that* a token is set without learning its value.
    """
    out = dict(record)
    if "token" in out:
        out["token"] = "<redacted>"
    return out