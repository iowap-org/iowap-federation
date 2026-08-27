"""FedNode P2P HTTP listener (T-095).

Serves the Peer-to-Peer API described in the master plan:

| Endpoint                  | Method | T-095 behaviour                         |
|---------------------------|--------|----------------------------------------|
| ``/fed/v1/inbox``         | POST   | validate bearer, write body to inbox/, 202 |
| ``/fed/v1/discover``      | GET    | return capability offer from node.yaml  |
| ``/fed/v1/results/{id}``  | GET    | 501 (T-102)                            |
| ``/fed/v1/ack``           | POST   | 501 (T-102)                            |

Auth: every request carries ``Authorization: Bearer fed_t_<token>``. The
listener validates it against the set of tokens configured for the node's
peers (any configured peer may talk to us) — or against an explicit
``listener_token`` override.

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
import os
import tempfile
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse

from federation.config import NodeConfig, load_node_config
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


def _atomic_write(path: Path, data: bytes) -> None:
    """Write ``data`` to ``path`` atomically (tmp + ``os.replace``)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(
        prefix=f".tmp_{path.stem}_", dir=str(path.parent), suffix=".json"
    )
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def create_listener_app(
    config: NodeConfig,
    *,
    inbox_dir: Path | None = None,
) -> FastAPI:
    """Build the FedNode listener FastAPI app.

    ``inbox_dir`` overrides the default inbox directory (used by tests with a
    tmp_path). Defaults to ``federation_root() / "_self" / "inbox"`` — the
    "_self" remote is this node's own inbox for incoming federated messages.
    """
    app = FastAPI(title="IOWAP FedNode", version="0.1.0")

    accepted = config.accepted_tokens()
    if inbox_dir is None:
        inbox_dir = federation_root() / "_self" / "inbox"
    inbox_dir = Path(inbox_dir)
    inbox_dir.mkdir(parents=True, exist_ok=True)

    state: dict[str, Any] = {
        "config": config,
        "accepted_tokens": accepted,
        "inbox_dir": inbox_dir,
        # Results the local relay has produced and exposed for polling.
        # T-095: empty — populating this is the T-096 daemon's job.
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
        # Structural check BEFORE writing (cheap DoS guard; E2EE payload-size
        # limits land in T-101). Parse JSON and require a message_id.
        try:
            payload = json.loads(body)
        except json.JSONDecodeError as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"invalid JSON: {e.msg}",
            ) from e
        if not isinstance(payload, dict):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="envelope must be a JSON object",
            )
        message_id = payload.get("message_id")
        if not message_id or not isinstance(message_id, str):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="envelope.message_id is required",
            )
        dest = Path(state["inbox_dir"]) / f"{message_id}.json"
        _atomic_write(dest, body)
        log.info("MESSAGE_RECEIVED message_id=%s bytes=%d", message_id, len(body))
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content={"status": "accepted", "message_id": message_id},
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
    # POST /fed/v1/ack  — 501 until T-102
    # ------------------------------------------------------------------ #
    @app.post("/fed/v1/ack")
    async def post_ack(request: Request, authorization: str | None = Header(None)):
        _check_auth(authorization)
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="ack handling lands in T-102",
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