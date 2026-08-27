"""Encrypted envelope helpers for federation messages (T-101).

Wraps the plaintext envelope schema from the master plan::

    {
      "message_id": "uuid4",        # jti — replay dedupe key
      "type": "task" | "result",
      "direction": "forward" | "reply",
      "capability": "image.gen@fed-b",
      "remote_task_id": "...",       # optional correlation id
      "schema_hash": "...",          # optional interface fingerprint
      "created_at": "RFC3339",
      "payload": { ... }
    }

into a *ciphertext envelope* that goes on the wire (the body of
``POST /fed/v1/inbox``)::

    {
      "message_id": "...",          # carried in clear for replay routing
      "sender_fp": "16 hex chars",  # public-key fingerprint (receiver picks key)
      "created_at": "...",          # strict timestamp, before encryption
      "nonce": "base64(24 bytes)",
      "ciphertext": "base64(...)"    # crypto_box(plaintext_envelope_json)
    }

Why expose ``message_id`` and ``sender_fp`` in the clear? The receiver needs
``message_id`` to dedupe replays *before* paying the decrypt cost, and
``sender_fp`` to select the matching local secret key. The payload itself
(stages, task data) stays inside the ciphertext. This matches the plan's
"key fingerprint of sender's pubkey (receiver picks the right key)" and
"File-size / cheap structural checks BEFORE decryption pipeline (DoS
protection)".

The plaintext envelope is JSON-encoded and then crypto_box'd. We use a
*strict* timestamp (RFC3339, UTC) so a receiver can reject stale messages
(T-102 lifecycle sets the exact window; T-101 just stamps + exposes it).

Two entry points:

* :func:`seal` — sender side: build the ciphertext envelope dict.
* :func:`open` — receiver side: validate structure, decrypt, return the
  plaintext envelope dict + the parsed header.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from federation.crypto import (
    Crypto,
    CryptoError,
    DecryptError,
    SizeError,
    SizeGuard,
    encode_public_key,
)

log = logging.getLogger("federation.envelope")

# ---------------------------------------------------------------------------
# Envelope versioning
# ---------------------------------------------------------------------------

ENVELOPE_VERSION = 1


def _utcnow_iso() -> str:
    """Strict RFC3339 UTC timestamp (seconds precision, 'Z' suffix)."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# ---------------------------------------------------------------------------
# Sender side
# ---------------------------------------------------------------------------

def _new_message_id() -> str:
    return str(uuid.uuid4())


def seal(
    crypto: Crypto,
    plaintext_envelope: dict[str, Any],
    *,
    message_id: str | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    """Encrypt ``plaintext_envelope`` → ciphertext envelope ready for the wire.

    ``message_id`` defaults to a fresh uuid4. ``created_at`` defaults to now
    (UTC, RFC3339). Both are also stamped onto the *plaintext* envelope
    before encryption so the receiver can verify they match the clear
    header (tamper-evidence for the routing fields).

    Returns the ciphertext envelope dict with ``v``, ``message_id``,
    ``sender_fp``, ``created_at``, ``nonce``, ``ciphertext``.
    """
    if not isinstance(plaintext_envelope, dict):
        raise TypeError("plaintext_envelope must be a dict")

    mid = message_id or plaintext_envelope.get("message_id") or _new_message_id()
    ts = created_at or plaintext_envelope.get("created_at") or _utcnow_iso()

    # Stamp the routing fields onto the plaintext too, so the receiver can
    # cross-check them against the clear header (defence against a peer
    # that relays a valid ciphertext under a forged message_id/ts).
    inner = dict(plaintext_envelope)
    inner["message_id"] = mid
    inner["created_at"] = ts

    payload_bytes = json.dumps(inner, sort_keys=True, separators=(",", ":")).encode("utf-8")
    nonce, ciphertext = crypto.encrypt(payload_bytes)

    return {
        "v": ENVELOPE_VERSION,
        "message_id": mid,
        "sender_fp": crypto.local_fingerprint,
        "created_at": ts,
        "nonce": _b64(nonce),
        "ciphertext": _b64(ciphertext),
    }


# ---------------------------------------------------------------------------
# Receiver side
# ---------------------------------------------------------------------------

def open(
    crypto: Crypto,
    ciphertext_envelope: dict[str, Any],
    *,
    size_guard: SizeGuard | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Validate + decrypt a ciphertext envelope.

    Returns ``(plaintext_envelope, header)`` where ``header`` is the clear
    routing fields (``message_id``, ``sender_fp``, ``created_at``,
    ``version``). Runs the cheap structural checks before decrypt.

    Raises :class:`CryptoError` (or its subclasses) on any failure. Does
    NOT do replay dedupe — that's the caller's job (see
    :class:`federation.crypto.ReplayCache`); we return the ``message_id``
    in the header so the caller can check the cache.
    """
    if not isinstance(ciphertext_envelope, dict):
        raise CryptoError("ciphertext envelope must be a JSON object")

    guard = size_guard or SizeGuard()
    guard.check_envelope(ciphertext_envelope)

    mid = ciphertext_envelope.get("message_id")
    sender_fp = ciphertext_envelope.get("sender_fp")
    ts = ciphertext_envelope.get("created_at")
    nonce_b64 = ciphertext_envelope.get("nonce")
    ct_b64 = ciphertext_envelope.get("ciphertext")

    if not sender_fp or not isinstance(sender_fp, str):
        raise CryptoError("envelope.sender_fp required")
    if not nonce_b64 or not ct_b64:
        raise CryptoError("envelope.nonce and ciphertext are required")
    if sender_fp != crypto.remote_fingerprint:
        raise CryptoError(
            f"sender_fp mismatch: got {sender_fp!r}, "
            f"expected {crypto.remote_fingerprint!r}"
        )

    try:
        nonce = _b64d(nonce_b64)
        ciphertext = _b64d(ct_b64)
    except CryptoError:
        raise
    except Exception as e:  # pragma: no cover — defensive
        raise CryptoError(f"base64 decode failed: {e}") from e

    plaintext = crypto.decrypt(ciphertext, nonce)
    try:
        inner = json.loads(plaintext.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise DecryptError(f"decrypted payload is not valid JSON: {e}") from e
    if not isinstance(inner, dict):
        raise DecryptError("decrypted payload must be a JSON object")

    # Cross-check the routing fields the sender stamped into the plaintext
    # against the clear header (tamper-evidence for routing fields).
    if inner.get("message_id") != mid:
        raise CryptoError(
            "message_id mismatch: clear header != inner plaintext"
        )
    if inner.get("created_at") != ts:
        raise CryptoError(
            "created_at mismatch: clear header != inner plaintext"
        )

    header = {
        "v": ciphertext_envelope.get("v", ENVELOPE_VERSION),
        "message_id": mid,
        "sender_fp": sender_fp,
        "created_at": ts,
    }
    return inner, header


# ---------------------------------------------------------------------------
# Small base64 helpers (kept local so envelope.py is self-contained for the
# JSON-facing encode/decode; crypto.py owns the wire nonce||ciphertext pack).
# ---------------------------------------------------------------------------

import base64 as _base64  # noqa: E402


def _b64(b: bytes) -> str:
    return _base64.b64encode(b).decode("ascii")


def _b64d(s: str) -> bytes:
    try:
        return _base64.b64decode(s, validate=True)
    except Exception as e:
        raise CryptoError(f"invalid base64: {e}") from e


# ---------------------------------------------------------------------------
# Plaintext envelope builder (convenience — used by the daemon / tests)
# ---------------------------------------------------------------------------

def build_plaintext_envelope(
    *,
    type: str,
    direction: str,
    capability: str,
    payload: dict[str, Any],
    message_id: str | None = None,
    remote_task_id: str | None = None,
    schema_hash: str | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    """Build a plaintext envelope dict matching the master-plan schema.

    ``message_id`` and ``created_at`` default to fresh values; pass them
    through :func:`seal` to encrypt.
    """
    env: dict[str, Any] = {
        "message_id": message_id or _new_message_id(),
        "type": type,
        "direction": direction,
        "capability": capability,
        "created_at": created_at or _utcnow_iso(),
        "payload": payload,
    }
    if remote_task_id is not None:
        env["remote_task_id"] = remote_task_id
    if schema_hash is not None:
        env["schema_hash"] = schema_hash
    return env


def parse_created_at(created_at: str) -> datetime:
    """Parse an RFC3339 timestamp from an envelope (UTC)."""
    # Allow either 'Z' suffix or explicit +00:00.
    s = created_at
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt