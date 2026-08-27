"""End-to-end encryption for federation messages (T-101).

Message-level E2EE using libsodium ``crypto_box``: X25519 (X25519 ECDH) +
XSalsa20-Poly1305 (authenticated symmetric encryption). PyNaCl bindings are
used; ``crypto_box`` here is the *combined* mode
(``nacl.public.Box``, which derives a shared key from an X25519 keypair and
applies XSalsa20-Poly1305 with a caller-supplied 24-byte nonce).

Design decisions
----------------
* ``Crypto(local_sk, remote_pk)`` wraps a single direction (we → remote or
  remote → us — ``Box`` is symmetric once constructed). Either side can both
  encrypt and decrypt with the same ``Box`` as long as the keypair pairing is
  the same. In practice the *sender* encrypts and the *receiver* decrypts;
  each side builds the ``Box`` from its own private key + the peer's public
  key.
* **Nonce:** a fresh 24-byte random nonce per message
  (``os.urandom(24)``). NEVER reused. The nonce is transmitted alongside the
  ciphertext (prepended on the wire: ``nonce || ciphertext``).
* **Key fingerprint:** short, stable identifier of the sender's public key
  so the receiver can pick the right private key out of several configured
  peers. Computed as ``SHA256(pk)[:8].hex()`` (16 hex chars, 64-bit collision
  resistance — enough to route a key, not a security boundary by itself).
* **Replay protection:** a TTL-pruned set of seen ``message_id``s. T-102
  adds the lifecycle state machine; T-101 just supplies the cache primitive.
* **DoS guards:** cheap structural checks (max size, valid base64, nonce
  length) run BEFORE decryption so a peer can't make us burn CPU on junk.

Wire format (base64 of): ``nonce (24 bytes) || ciphertext``.

This module is deliberately self-contained: it depends only on PyNaCl and
the stdlib so it can be unit-tested in isolation.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any

import nacl.exceptions
from nacl.public import PrivateKey, PublicKey, Box
from nacl.utils import EncryptedMessage

log = logging.getLogger("federation.crypto")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# crypto_box nonce size (XSalsa20 = 24 bytes). Pinned for clarity + so the
# wire-format parser does not depend on a magic number.
NONCE_BYTES = 24

# Maximum plaintext size we will accept (16 MiB). Generous for any task
# payload but bounded so a malicious peer can't exhaust memory/disk with a
# multi-GB blob. The check runs on the ciphertext (which is ~16 bytes larger
# than the plaintext due to the Poly1305 tag) BEFORE decryption.
MAX_CIPHERTEXT_BYTES = 16 * 1024 * 1024 + 64

# Base64 overhead factor (4/3) — used for the cheap pre-decode length guard.
_B64_MAX = (MAX_CIPHERTEXT_BYTES * 4 // 3) + 16


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

class CryptoError(Exception):
    """Base class for crypto failures (decrypt failure, tamper, bad size)."""


class DecryptError(CryptoError):
    """Raised when ciphertext cannot be authenticated/decrypted."""


class SizeError(CryptoError):
    """Raised when a blob fails the pre-decryption size guard (DoS)."""


class ReplayError(CryptoError):
    """Raised when a message_id is presented more than once (replay)."""


class NonceReuseError(CryptoError):
    """Raised when a nonce is reused — must never happen with random nonce."""


# ---------------------------------------------------------------------------
# Keypair helpers
# ---------------------------------------------------------------------------

def generate_keypair() -> tuple[bytes, bytes]:
    """Generate a fresh X25519 keypair.

    Returns ``(public_key, secret_key)`` as raw 32-byte ``bytes``. The public
    key is what the peer stores (``peer_public_key`` in node.yaml); the
    secret key is kept locally and never transmitted.
    """
    sk = PrivateKey.generate()
    pk = sk.public_key
    return bytes(pk), bytes(sk)


def public_key_fingerprint(public_key: bytes) -> str:
    """Stable short fingerprint of an X25519 public key.

    ``SHA256(pk)[:8].hex()`` → 16 hex chars. Used by the receiver to route an
    incoming envelope to the right local secret key. This is *not* a
    security boundary — it's a routing hint; the actual authentication is
    the Poly1305 tag on the ciphertext.
    """
    if len(public_key) != 32:
        raise ValueError(
            f"public_key must be 32 bytes, got {len(public_key)}"
        )
    return hashlib.sha256(public_key).digest()[:8].hex()


# ---------------------------------------------------------------------------
# Wire encoding helpers (nonce || ciphertext, base64 over the wire)
# ---------------------------------------------------------------------------

def _b64e(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _b64d(s: str | bytes) -> bytes:
    if isinstance(s, str):
        s = s.encode("ascii")
    try:
        return base64.b64decode(s, validate=True)
    except (binascii.Error, ValueError) as e:
        raise CryptoError(f"invalid base64: {e}") from e


# binascii imported at top of module.


def encode_wire(nonce: bytes, ciphertext: bytes) -> str:
    """Pack ``nonce || ciphertext`` and base64-encode for JSON transport."""
    if len(nonce) != NONCE_BYTES:
        raise ValueError(f"nonce must be {NONCE_BYTES} bytes, got {len(nonce)}")
    return _b64e(nonce + ciphertext)


def decode_wire(blob: str | bytes) -> tuple[bytes, bytes]:
    """Inverse of :func:`encode_wire`. Applies the size guard before returning.

    Returns ``(nonce, ciphertext)``. Raises :class:`SizeError` /
    :class:`CryptoError` on malformed input.
    """
    if isinstance(blob, str) and len(blob) > _B64_MAX:
        raise SizeError("ciphertext exceeds maximum allowed size")
    raw = _b64d(blob)
    if len(raw) < NONCE_BYTES + 16:  # nonce + min Poly1305-tagged ciphertext
        raise CryptoError("ciphertext too short")
    if len(raw) > MAX_CIPHERTEXT_BYTES:
        raise SizeError("decoded ciphertext exceeds maximum allowed size")
    return raw[:NONCE_BYTES], raw[NONCE_BYTES:]


# ---------------------------------------------------------------------------
# Crypto (the Box wrapper)
# ---------------------------------------------------------------------------

class Crypto:
    """Message-level crypto_box encryptor/decryptor for one peer pairing.

    Construct one ``Crypto`` per peer pairing (``Box`` is symmetric: both
    sides build it from ``own_sk + peer_pk``). Use the *same* instance to
    encrypt outbound and to decrypt inbound messages for that peer.

    Parameters
    ----------
    local_sk:
        Our X25519 secret key (32 raw bytes, from :func:`generate_keypair`).
    remote_pk:
        The peer's X25519 public key (32 raw bytes).

    Notes
    -----
    * A fresh random 24-byte nonce is generated for every :meth:`encrypt`
      call. The nonce is never reused (verified by an internal set that
      guards against a broken RNG refeeding the same bytes — extraordinarily
      unlikely with ``os.urandom`` but cheap to check and the plan calls
      for "NEVER reused").
    * The public-key fingerprint of the sender is carried in the envelope
      so the receiver can pick the matching ``Crypto`` (see
      :func:`public_key_fingerprint`).
    """

    def __init__(self, local_sk: bytes, remote_pk: bytes) -> None:
        if len(local_sk) != 32:
            raise ValueError(f"local_sk must be 32 bytes, got {len(local_sk)}")
        if len(remote_pk) != 32:
            raise ValueError(f"remote_pk must be 32 bytes, got {len(remote_pk)}")
        self._local_sk = bytes(local_sk)
        self._remote_pk = bytes(remote_pk)
        self._box = Box(PrivateKey(self._local_sk), PublicKey(self._remote_pk))
        # Track nonces we've generated to assert uniqueness. Not shared with
        # decrypt — the receiver must track its own seen-nonces only if it
        # wants to detect a malicious peer reusing a captured nonce (the
        # crypto_box primitive itself is stateless). Here we guard our own
        # output integrity.
        self._seen_nonces: set[bytes] = set()
        # Peer's public-key fingerprint (so the envelope can carry it).
        self.remote_fingerprint = public_key_fingerprint(self._remote_pk)
        self.local_fingerprint = public_key_fingerprint(
            bytes(PrivateKey(self._local_sk).public_key)
        )

    # -- encrypt ---------------------------------------------------------

    def encrypt(self, plaintext: bytes) -> tuple[bytes, bytes]:
        """Encrypt ``plaintext`` with a fresh random nonce.

        Returns ``(nonce, ciphertext)``. The caller is responsible for
        transmitting both — typically via :func:`encode_wire` or by placing
        them in the envelope dict (``nonce`` + ``ciphertext`` fields).
        """
        nonce = os.urandom(NONCE_BYTES)
        # Assert we never produce a reused nonce. In the astronomically
        # unlikely event ``os.urandom`` returns a value we've already used,
        # raise instead of silently reusing it.
        if nonce in self._seen_nonces:
            raise NonceReuseError("os.urandom returned a duplicate nonce")
        self._seen_nonces.add(nonce)
        ciphertext = self._box.encrypt(plaintext, nonce)
        # nacl's Box.encrypt returns EncryptedMessage whose .ciphertext is
        # the auth-tagged payload (without the nonce prefix). Pull only the
        # ciphertext portion so our wire format is unambiguous.
        if isinstance(ciphertext, EncryptedMessage):
            cipher = bytes(ciphertext.ciphertext)
        else:  # pragma: no cover — defensive
            cipher = bytes(ciphertext)
        return nonce, cipher

    def encrypt_to_wire(self, plaintext: bytes) -> str:
        """Convenience: encrypt and pack as ``nonce||ciphertext`` base64."""
        nonce, cipher = self.encrypt(plaintext)
        return encode_wire(nonce, cipher)

    # -- decrypt ---------------------------------------------------------

    def decrypt(self, ciphertext: bytes, nonce: bytes) -> bytes:
        """Decrypt + authenticate ``ciphertext`` given ``nonce``.

        Raises :class:`DecryptError` on tamper / wrong key / bad nonce.
        """
        if len(nonce) != NONCE_BYTES:
            raise DecryptError(
                f"nonce must be {NONCE_BYTES} bytes, got {len(nonce)}"
            )
        try:
            return self._box.decrypt(ciphertext, nonce)
        except nacl.exceptions.CryptoError as e:
            raise DecryptError(f"decrypt failed: {e}") from e

    def decrypt_from_wire(self, blob: str | bytes) -> bytes:
        """Convenience inverse of :meth:`encrypt_to_wire`."""
        nonce, cipher = decode_wire(blob)
        return self.decrypt(cipher, nonce)


# ---------------------------------------------------------------------------
# Replay cache (T-101 primitive; TTL pruning wired up in T-102 lifecycle)
# ---------------------------------------------------------------------------

@dataclass
class ReplayCache:
    """TTL-pruned set of seen message_ids.

    ``check_and_add(message_id)`` returns ``True`` the first time an id is
    seen and records it; a second call with the same id returns ``False``
    (replay). Entries older than ``ttl`` seconds are pruned on each call
    (amortised — keeps the set bounded under sustained traffic).
    """

    ttl: float = 3600.0  # 1 hour default; T-102 may tune per-message
    _seen: dict[str, float] = field(default_factory=dict)

    def check_and_add(self, message_id: str) -> bool:
        """Return ``True`` if fresh, ``False`` if a replay (already seen)."""
        self.prune()
        now = time.monotonic()
        if message_id in self._seen:
            return False
        self._seen[message_id] = now
        return True

    def prune(self) -> int:
        """Drop entries older than ``ttl``. Returns the number pruned."""
        if not self._seen:
            return 0
        cutoff = time.monotonic() - self.ttl
        stale = [k for k, t in self._seen.items() if t < cutoff]
        for k in stale:
            del self._seen[k]
        return len(stale)

    def __contains__(self, message_id: str) -> bool:
        self.prune()
        return message_id in self._seen

    def __len__(self) -> int:
        return len(self._seen)


# ---------------------------------------------------------------------------
# Structural / DoS pre-decryption guards
# ---------------------------------------------------------------------------

@dataclass
class SizeGuard:
    """Cheap structural checks run BEFORE decryption.

    A peer could send us a multi-gigabyte blob or malformed base64 to make
    us burn CPU. These checks run on the *raw* envelope (still ciphertext)
    so we reject obviously-bad input without touching the crypto box.
    """

    max_ciphertext_bytes: int = MAX_CIPHERTEXT_BYTES

    def check_wire(self, blob: str | bytes) -> None:
        """Validate the base64 wire blob length without decoding."""
        if isinstance(blob, str) and len(blob) > _B64_MAX:
            raise SizeError(
                f"wire blob exceeds max ({len(blob)} > {_B64_MAX} base64 chars)"
            )

    def check_envelope(self, envelope: dict[str, Any]) -> None:
        """Validate envelope fields present + sane BEFORE attempting decrypt.

        Requires ``message_id`` (str), optional ``nonce``/``ciphertext``
        fields (str, base64) with bounded length, optional ``sender_fp``
        (str). Raises :class:`SizeError` / :class:`CryptoError` on bad
        shape. Does NOT touch the crypto box.
        """
        if not isinstance(envelope, dict):
            raise CryptoError("envelope must be a JSON object")
        mid = envelope.get("message_id")
        if not isinstance(mid, str) or not mid:
            raise CryptoError("envelope.message_id required and must be str")
        # Bounded message_id (uuid4 is 36 chars; allow generous headroom).
        if len(mid) > 128:
            raise SizeError("message_id too long")
        ct = envelope.get("ciphertext")
        if ct is not None:
            if not isinstance(ct, str):
                raise CryptoError("ciphertext must be a base64 str")
            if len(ct) > _B64_MAX:
                raise SizeError("ciphertext field exceeds max size")
        nonce = envelope.get("nonce")
        if nonce is not None:
            if not isinstance(nonce, str):
                raise CryptoError("nonce must be a base64 str")
            # base64 of 24 bytes = 32 chars (no padding for 24-byte input).
            if len(nonce) > 64:
                raise SizeError("nonce field too long")


# ---------------------------------------------------------------------------
# Convenience: derive a Crypto from a Peer record
# ---------------------------------------------------------------------------

def crypto_for_peer(local_sk: bytes, peer: Any) -> Crypto:
    """Build a :class:`Crypto` from a :class:`federation.config.Peer`.

    The peer record carries ``peer_public_key`` (the remote's X25519 pk) and
    optionally ``my_public_key`` (for symmetry / logging). We only need the
    remote pk + our local sk.
    """
    pk_b64 = getattr(peer, "peer_public_key", None)
    if not pk_b64:
        raise CryptoError(f"peer {getattr(peer, 'id', '?')!r} has no peer_public_key")
    pk = _decode_key(pk_b64)
    return Crypto(local_sk, pk)


def encode_public_key(pk: bytes) -> str:
    """Encode a 32-byte public key as base64 for storage in node.yaml."""
    return base64.b64encode(pk).decode("ascii")


def decode_public_key(s: str) -> bytes:
    """Decode a base64-encoded public key from node.yaml."""
    return _decode_key(s)


def _decode_key(s: str) -> bytes:
    try:
        raw = base64.b64decode(s, validate=True)
    except (binascii.Error, ValueError) as e:
        raise CryptoError(f"invalid base64 key: {e}") from e
    if len(raw) != 32:
        raise CryptoError(f"public key must decode to 32 bytes, got {len(raw)}")
    return raw