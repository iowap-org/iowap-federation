"""``fed-cli`` — argparse-based CLI for the Federation Node (T-097).

Subcommands
-----------
``fed-cli federation peer add <id> <endpoint> <token> <pubkey>``
    Add a peer to ``node.yaml``. ``<pubkey>`` is the remote's X25519 public key
    (used by E2EE in T-101); pass ``-`` to leave it unset.

``fed-cli federation peer remove <id>``
    Remove a peer.

``fed-cli federation peer list``
    List configured peers. **Tokens are never printed.**

``fed-cli federation peer connect <id>``
    Mark a peer as active.

``fed-cli federation peer disconnect <id>``
    Mark a peer as inactive.

The default config path is ``~/.relay/node.yaml`` (overridable via
``--config``). Exit codes: 0 success, 1 recoverable error (duplicate/unknown
peer, bad input), 2 argparse usage error.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Sequence

from federation.peers import (
    PeerError,
    PeerExistsError,
    PeerManager,
    PeerNotFoundError,
    redact,
)


def _format_record(rec: dict[str, Any]) -> str:
    """Format a single peer record for human display (token redacted)."""
    r = redact(rec)
    parts = [f"id={r.get('id', '?')}", f"endpoint={r.get('endpoint', '?')}"]
    if "peer_public_key" in r and r["peer_public_key"] is not None:
        # Show only a short fingerprint; full keys are noisy on a terminal.
        pk = str(r["peer_public_key"])
        parts.append(f"pubkey={pk[:8]}…" if len(pk) > 8 else f"pubkey={pk}")
    parts.append(f"active={r.get('active', False)}")
    return "  " + "  ".join(parts)


# -- subcommand handlers ---------------------------------------------------


def _cmd_peer_add(args: argparse.Namespace, mgr: PeerManager) -> int:
    pubkey = args.pubkey
    if pubkey == "-":
        pubkey = None
    try:
        rec = mgr.add(
            id=args.id,
            endpoint=args.endpoint,
            token=args.token,
            pubkey=pubkey,
        )
    except PeerExistsError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    print(f"added peer {rec['id']!r} -> {rec['endpoint']}")
    return 0


def _cmd_peer_remove(args: argparse.Namespace, mgr: PeerManager) -> int:
    try:
        rec = mgr.remove(args.id)
    except PeerNotFoundError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    print(f"removed peer {rec['id']!r}")
    return 0


def _cmd_peer_list(args: argparse.Namespace, mgr: PeerManager) -> int:
    peers = mgr.list()
    if not peers:
        print("no peers configured")
        return 0
    print(f"{len(peers)} peer(s):")
    for p in peers:
        print(_format_record(p))
    return 0


def _cmd_peer_connect(args: argparse.Namespace, mgr: PeerManager) -> int:
    try:
        rec = mgr.connect(args.id)
    except PeerNotFoundError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    print(f"connected peer {rec['id']!r}")
    print(_format_record(rec))
    return 0


def _cmd_peer_disconnect(args: argparse.Namespace, mgr: PeerManager) -> int:
    try:
        rec = mgr.disconnect(args.id)
    except PeerNotFoundError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    print(f"disconnected peer {rec['id']!r}")
    print(_format_record(rec))
    return 0


# -- parser construction ----------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="fed-cli",
        description="Federation Node CLI — manage peers and the daemon.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="path to node.yaml (default: ~/.relay/node.yaml)",
    )

    sub = parser.add_subparsers(dest="command", required=True, metavar="<command>")

    fed = sub.add_parser("federation", help="federation-related subcommands")
    fed_sub = fed.add_subparsers(dest="fed_command", required=True, metavar="<fed-command>")

    peer = fed_sub.add_parser("peer", help="manage federation peers")
    peer_sub = peer.add_subparsers(dest="peer_command", required=True, metavar="<peer-command>")

    add = peer_sub.add_parser("add", help="add a peer")
    add.add_argument("id", help="peer id (e.g. fed-b)")
    add.add_argument("endpoint", help="peer listener URL (e.g. https://fed-b:8791)")
    add.add_argument("token", help="bearer token the peer uses to reach us")
    add.add_argument(
        "pubkey",
        help="peer's X25519 public key (use '-' to leave unset until E2EE)",
    )
    add.set_defaults(handler=_cmd_peer_add)

    rm = peer_sub.add_parser("remove", help="remove a peer")
    rm.add_argument("id", help="peer id to remove")
    rm.set_defaults(handler=_cmd_peer_remove)

    ls = peer_sub.add_parser("list", help="list configured peers (tokens redacted)")
    ls.set_defaults(handler=_cmd_peer_list)

    conn = peer_sub.add_parser("connect", help="mark a peer as active")
    conn.add_argument("id", help="peer id to activate")
    conn.set_defaults(handler=_cmd_peer_connect)

    disc = peer_sub.add_parser("disconnect", help="mark a peer as inactive")
    disc.add_argument("id", help="peer id to deactivate")
    disc.set_defaults(handler=_cmd_peer_disconnect)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point. Returns a process exit code."""
    parser = _build_parser()
    args = parser.parse_args(argv)
    mgr = PeerManager(path=args.config)
    handler = args.handler
    try:
        return handler(args, mgr)
    except PeerError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())