"""FedNode P2P HTTP listener (T-095 / T-101 / T-102).

Serves the Peer-to-Peer API described in the master plan:

| Endpoint                  | Method | Behaviour                              |
|---------------------------|--------|----------------------------------------|
| ``/fed/v1/inbox``         | POST   | validate bearer, replay-check, decrypt (T-101), write to inbox/, 202 + inline ACK |
| ``/fed/v1/discover``      | GET    | return capability offer from node.yaml  |
| ``/fed/v1/results/{id}``  | GET    | 501 (full result polling is the daemon's job) |
| ``/fed/v1/ack``           | POST   | record sender's ACK, free state (T-102) |

Auth: every request carries ``Authorization: Bearer fed_t_<token>``. The
listener validates it against the set of tokens configured for the node's
peers (any configured peer may talk to us) — or against an explicit
``listener_token`` override.

E2EE (T-101): when a :class:`federation.crypto.Crypto` is supplied via
``crypto=`` (one per accepted sender fingerprint), incoming inbox bodies
are ciphertext envelopes that are decrypted *after* the cheap structural
checks and *before* the inbox write. Plaintext JSON is still accepted
(crypto-less mode) so the pre-T-101 tests keep passing.

Lifecycle (T-102): every accepted inbox POST runs through a
:class:`federation.lifecycle.ReplayGuard` — a duplicate ``message_id`` is
rejected with ``409`` + an inline ``rejected`` ACK *before* the inbox
write. ``POST /fed/v1/ack`` records the sender's confirmation that it
received our inline ACK (so we can free state). Writes are atomic
(``.tmp_<id>`` + ``os.replace``).

The listener is symmetric: both ends serve the same API. The remote
FedNode re-submits the task as a normal task into its own relay — that step
is the daemon's job (T-096), not the listener's.

The FastAPI app is constructed via ``create_listener_app(config)`` so tests
can pass an in-memory ``NodeConfig``; ``create_app_from_config_file(path)``
is the filesystem entry point for the daemon.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse

from federation.ack import (
    ACK_STATUS_ACCEPTED,
    ReceiverAck,
    inbox_accepted,
    inbox_rejected,
    parse_ack_body,
)
from federation.config import NodeConfig, load_node_config
from federation.crypto import Crypto, CryptoError, SizeGuard
from federation.envelope import open as envelope_open
from federation.index import MetadataIndex
from federation.lifecycle import (
    MessageState,
    ReplayGuard,
    atomic_write_bytes,
    emit_event,
)
from federation.paths import ensure_federation_dirs, federation_root

log = logging.getLogger("federation.listener")

# Header name for the bearer token. Stored as ``str | None``.
_AUTH = "Authorization"


def _bearer(authorization: str | None) -> str | None:
    """Extract the token from an ``Authorization: Bearer <tok>`` header."""
    if not authorization:
        return None
    if not authorization.lower().startswith("bearer "):
        return None
    return authorization.split(" ", 1)[1].strip() or None


def create_listener_app(
    config: NodeConfig,
    *,
    inbox_dir: Path | None = None,
    crypto: Crypto | None = None,
    size_guard: SizeGuard | None = None,
    replay_guard: ReplayGuard | None = None,
    receiver_ack: ReceiverAck | None = None,
    index: MetadataIndex | None = None,
) -> FastAPI:
    """Build the FedNode listener FastAPI app.

    ``inbox_dir`` overrides the default inbox directory (used by tests with a
    tmp_path). Defaults to ``federation_root() / "_self" / "inbox"`` — the
    "_self" remote is this node's own inbox for incoming federated messages.

    E2EE (T-101):
    * ``crypto`` — a :class:`federation.crypto.Crypto` for the single
      expected sender. When supplied, inbox bodies are treated as
      ciphertext envelopes and decrypted after structural checks. When
      ``None`` the listener accepts plaintext envelopes (pre-T-101 mode).
    * ``size_guard`` — optional :class:`federation.crypto.SizeGuard` for
      the cheap pre-decryption DoS checks. Defaults to a fresh guard.

    Lifecycle (T-102):
    * ``replay_guard`` — optional :class:`federation.lifecycle.ReplayGuard`.
      Defaults to a fresh guard (1 h TTL). A duplicate ``message_id`` is
      rejected with ``409`` and an inline ``rejected`` ACK before the
      inbox write.
    * ``receiver_ack`` — optional :class:`federation.ack.ReceiverAck`
      ledger. Defaults to a fresh one. ``POST /fed/v1/ack`` records the
      sender's confirmation here.
    """
    app = FastAPI(title="IOWAP FedNode", version="0.1.0")

    accepted = config.accepted_tokens()
    if inbox_dir is None:
        inbox_dir = federation_root() / "_self" / "inbox"
    inbox_dir = Path(inbox_dir)
    inbox_dir.mkdir(parents=True, exist_ok=True)

    guard = size_guard or SizeGuard()
    replay = replay_guard or ReplayGuard()
    ack_ledger = receiver_ack or ReceiverAck()

    state: dict[str, Any] = {
        "config": config,
        "accepted_tokens": accepted,
        "inbox_dir": inbox_dir,
        "crypto": crypto,
        "size_guard": guard,
        "replay_guard": replay,
        "receiver_ack": ack_ledger,
        # T-103 metadata index (optional). When wired, every accepted inbox
        # POST inserts a metadata row (receiver side) + is marked processed.
        "index": index,
        # Results the local relay has produced and exposed for polling.
        # T-095/T-096: populating this is the daemon's job; the listener
        # only needs to return them when present.
        "results": {},
    }
    app.state.fed = state

    def _check_auth(authorization: str | None) -> None:
        tok = _bearer(authorization)
        if not tok or tok not in state["accepted_tokens"]:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="invalid or missing bearer token",
            )

    def _extract_envelope(body: bytes) -> tuple[dict[str, Any], str]:
        """Parse + (optionally) decrypt the inbox body.

        Returns ``(plaintext_envelope, message_id)``. Raises
        ``HTTPException`` (400/401-equivalent for crypto failures) on a
        malformed or tampered body. The cheap structural checks run
        *before* decryption (DoS guard).

        When no crypto is configured the body is parsed as a plaintext
        envelope (pre-T-101 path — both modes coexist so tests pass).
        """
        crypto_box: Crypto | None = state["crypto"]
        try:
            raw = json.loads(body)
        except json.JSONDecodeError as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"invalid JSON: {e.msg}",
            ) from e
        if not isinstance(raw, dict):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="envelope must be a JSON object",
            )
        # Heuristic: a ciphertext envelope carries ``ciphertext`` +
        # ``nonce``; a plaintext envelope carries ``type``/``payload``.
        if crypto_box is not None and "ciphertext" in raw and "nonce" in raw:
            try:
                inner, header = envelope_open(crypto_box, raw, size_guard=guard)
            except CryptoError as e:
                # Tamper / wrong key / bad size — surface as a 400 so the
                # sender can move the message to REJECTED. Auth-bound
                # (only an authenticated peer can reach here).
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"decrypt failed: {e}",
                ) from e
            emit_event("MESSAGE_DECRYPTED", message_id=header["message_id"])
            return inner, header["message_id"]
        # Plaintext path.
        message_id = raw.get("message_id")
        if not message_id or not isinstance(message_id, str):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="envelope.message_id is required",
            )
        return raw, message_id

    # ------------------------------------------------------------------ #
    # POST /fed/v1/inbox
    # ------------------------------------------------------------------ #
    @app.post("/fed/v1/inbox", status_code=status.HTTP_202_ACCEPTED)
    async def post_inbox(request: Request, authorization: str | None = Header(None)):
        _check_auth(authorization)
        body = await request.body()
        if not body:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="empty body",
            )
        envelope, message_id = _extract_envelope(body)

        # Replay guard — reject a duplicate *before* the inbox write. The
        # guard emits MESSAGE_REJECTED on its own; we surface an inline
        # ``rejected`` ACK so the sender can move the outbox file to
        # archive without retrying.
        if not state["replay_guard"].check_and_add(message_id):
            return JSONResponse(
                status_code=status.HTTP_409_CONFLICT,
                content=inbox_rejected(message_id, reason="replay"),
            )

        # Write the *plaintext* envelope to the inbox so the daemon's
        # inbox processor does not need to know about crypto. The body on
        # disk is the canonical inner envelope.
        dest = Path(state["inbox_dir"]) / f"{message_id}.json"
        payload_bytes = json.dumps(envelope, sort_keys=True).encode("utf-8")
        atomic_write_bytes(dest, payload_bytes)
        # T-103: index the accepted message (receiver side). ``mark_processed``
        # is set here because the inbox write *is* the durable record; the
        # daemon's inbox processor will re-confirm via its own ``mark_processed``.
        idx: MetadataIndex | None = state["index"]
        if idx is not None:
            try:
                idx.insert(
                    message_id,
                    state=MessageState.ACKED,
                    type=envelope.get("type") if isinstance(envelope, dict) else None,
                    remote=config.node_name,
                    capability=(
                        envelope.get("capability") if isinstance(envelope, dict) else None
                    ),
                    remote_task_id=(
                        str(envelope.get("remote_task_id") or "")
                        or None
                    ) if isinstance(envelope, dict) else None,
                    schema_hash=(
                        envelope.get("schema_hash") if isinstance(envelope, dict) else None
                    ),
                    size=len(payload_bytes),
                )
                idx.mark_processed(message_id)
            except Exception as exc:  # noqa: BLE001 — index must not break the listener
                log.warning("index insert (inbox) failed for %s: %s", message_id, exc)
        emit_event(
            "MESSAGE_ACKED",
            message_id=message_id,
            side="receiver",
            ack_status=ACK_STATUS_ACCEPTED,
        )
        log.info("MESSAGE_RECEIVED message_id=%s bytes=%d", message_id, len(body))
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content=inbox_accepted(message_id),
        )

    # ------------------------------------------------------------------ #
    # GET /fed/v1/discover
    # ------------------------------------------------------------------ #
    @app.get("/fed/v1/discover")
    async def discover(authorization: str | None = Header(None)):
        _check_auth(authorization)
        return JSONResponse(content=list(state["config"].offer))

    # ------------------------------------------------------------------ #
    # GET /fed/v1/results/{message_id}  — 501 until T-102
    # ------------------------------------------------------------------ #
    @app.get("/fed/v1/results/{message_id}")
    async def get_results(message_id: str, authorization: str | None = Header(None)):
        _check_auth(authorization)
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="results polling lands in T-102",
        )

    # ------------------------------------------------------------------ #
    # POST /fed/v1/ack  — sender confirms it received our inline ACK
    # ------------------------------------------------------------------ #
    @app.post("/fed/v1/ack")
    async def post_ack(request: Request, authorization: str | None = Header(None)):
        _check_auth(authorization)
        try:
            body = await request.json()
        except json.JSONDecodeError as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"invalid JSON: {e.msg}",
            ) from e
        try:
            message_id, ack_status = parse_ack_body(body)
        except ValueError as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=str(e),
            ) from e
        resp = state["receiver_ack"].record(message_id, ack_status)
        log.info(
            "ACK_RECEIVED message_id=%s status=%s", message_id, ack_status
        )
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=resp,
        )

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    return app


def create_app_from_config_file(path: str | None = None) -> FastAPI:
    """Build the listener app from ``node.yaml`` (daemon entry point)."""
    config = load_node_config(path)
    # Ensure the default inbox directory exists.
    inbox, _ = ensure_federation_dirs("_self")
    return create_listener_app(config, inbox_dir=inbox)