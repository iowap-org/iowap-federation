"""Tests for the E2EE crypto_box layer (T-101).

Covers the plan's acceptance criteria:

* encrypt → decrypt round-trip,
* tampered ciphertext raises,
* wrong key raises,
* nonce is never reused across messages,
* replay cache rejects duplicates and TTL-prunes,
* cheap structural / size guards run BEFORE decryption (DoS),
* envelope seal/open round-trip + header cross-check,
* key fingerprint is stable + routes to the right key.

PyNaCl is a hard dependency (listed in the plan tech stack); these tests
do not skip if it is missing — a missing import is a build error, not a
test condition.
"""

from __future__ import annotations

import base64
import json
import time
from typing import Any

import pytest

from federation.crypto import (
    Crypto,
    CryptoError,
    DecryptError,
    MAX_CIPHERTEXT_BYTES,
    NONCE_BYTES,
    NonceReuseError,
    ReplayCache,
    SizeError,
    SizeGuard,
    crypto_for_peer,
    decode_public_key,
    decode_wire,
    encode_public_key,
    encode_wire,
    generate_keypair,
    public_key_fingerprint,
)
from federation.envelope import (
    ENVELOPE_VERSION,
    build_plaintext_envelope,
    open as envelope_open,
    seal,
)


# --------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------- #

@pytest.fixture
def ab_keypair():
    """Two keypairs: A (sender) and B (receiver)."""
    pk_a, sk_a = generate_keypair()
    pk_b, sk_b = generate_keypair()
    return {
        "pk_a": pk_a, "sk_a": sk_a,
        "pk_b": pk_b, "sk_b": sk_b,
    }


@pytest.fixture
def ab_crypto(ab_keypair):
    """A→B sender Crypto and B→A receiver Crypto (same Box pairing)."""
    send = Crypto(ab_keypair["sk_a"], ab_keypair["pk_b"])
    recv = Crypto(ab_keypair["sk_b"], ab_keypair["pk_a"])
    return send, recv


# --------------------------------------------------------------------- #
# generate_keypair / fingerprint
# --------------------------------------------------------------------- #

class TestKeypair:
    def test_generate_keypair_lengths(self):
        pk, sk = generate_keypair()
        assert len(pk) == 32
        assert len(sk) == 32
        assert pk != sk

    def test_generate_keypair_unique(self):
        keys = {generate_keypair()[0] for _ in range(64)}
        assert len(keys) == 64, "keypairs must be unique"

    def test_fingerprint_stable(self):
        pk, _ = generate_keypair()
        assert public_key_fingerprint(pk) == public_key_fingerprint(pk)

    def test_fingerprint_distinct(self):
        fp = {public_key_fingerprint(generate_keypair()[0]) for _ in range(64)}
        assert len(fp) == 64, "fingerprints must collide only negligibly"

    def test_fingerprint_format(self):
        pk, _ = generate_keypair()
        fp = public_key_fingerprint(pk)
        assert len(fp) == 16  # 8 bytes → 16 hex chars
        int(fp, 16)  # is hex

    def test_fingerprint_rejects_wrong_size(self):
        with pytest.raises(ValueError):
            public_key_fingerprint(b"short")

    def test_public_key_encoding_roundtrip(self):
        pk, _ = generate_keypair()
        encoded = encode_public_key(pk)
        assert decode_public_key(encoded) == pk

    def test_decode_public_key_rejects_bad_length(self):
        with pytest.raises(CryptoError):
            decode_public_key(base64.b64encode(b"too short").decode())


# --------------------------------------------------------------------- #
# Crypto.encrypt / decrypt round-trip
# --------------------------------------------------------------------- #

class TestRoundTrip:
    def test_encrypt_decrypt_roundtrip(self, ab_crypto):
        send, recv = ab_crypto
        nonce, ct = send.encrypt(b"hello world")
        assert len(nonce) == NONCE_BYTES
        assert ct != b"hello world"
        assert recv.decrypt(ct, nonce) == b"hello world"

    def test_wire_roundtrip(self, ab_crypto):
        send, recv = ab_crypto
        blob = send.encrypt_to_wire(b"payload")
        assert isinstance(blob, str)
        assert recv.decrypt_from_wire(blob) == b"payload"

    def test_encrypt_large_payload(self, ab_crypto):
        send, recv = ab_crypto
        data = b"x" * (1024 * 1024)  # 1 MiB
        blob = send.encrypt_to_wire(data)
        assert recv.decrypt_from_wire(blob) == data

    def test_empty_payload_roundtrips(self, ab_crypto):
        send, recv = ab_crypto
        blob = send.encrypt_to_wire(b"")
        assert recv.decrypt_from_wire(blob) == b""

    def test_unicode_payload_roundtrips(self, ab_crypto):
        send, recv = ab_crypto
        data = "héllo wörld 🌍".encode("utf-8")
        assert recv.decrypt_from_wire(send.encrypt_to_wire(data)) == data

    def test_same_crypto_encrypts_and_decrypts(self, ab_keypair):
        """A single Crypto (Box) can both encrypt and decrypt for its pairing."""
        box = Crypto(ab_keypair["sk_a"], ab_keypair["pk_b"])
        nonce, ct = box.encrypt(b"both ways")
        assert box.decrypt(ct, nonce) == b"both ways"


# --------------------------------------------------------------------- #
# Tamper / wrong key → raise
# --------------------------------------------------------------------- #

class TestFailures:
    def test_tampered_ciphertext_raises(self, ab_crypto):
        send, recv = ab_crypto
        nonce, ct = send.encrypt(b"original")
        # Flip a byte in the ciphertext → Poly1305 tag must fail.
        tampered = bytearray(ct)
        tampered[0] ^= 0xFF
        with pytest.raises(DecryptError):
            recv.decrypt(bytes(tampered), nonce)

    def test_tampered_nonce_raises(self, ab_crypto):
        send, recv = ab_crypto
        nonce, ct = send.encrypt(b"original")
        bad_nonce = bytearray(nonce)
        bad_nonce[0] ^= 0x01
        with pytest.raises(DecryptError):
            recv.decrypt(ct, bytes(bad_nonce))

    def test_wrong_key_raises(self, ab_keypair):
        # A third party C holds an unrelated keypair.
        pk_c, sk_c = generate_keypair()
        send = Crypto(ab_keypair["sk_a"], ab_keypair["pk_b"])  # A→B
        wrong = Crypto(sk_c, ab_keypair["pk_b"])  # C thinks it's B's peer
        nonce, ct = send.encrypt(b"secret")
        with pytest.raises(DecryptError):
            wrong.decrypt(ct, nonce)

    def test_decrypt_wrong_nonce_length_raises(self, ab_crypto):
        send, recv = ab_crypto
        _, ct = send.encrypt(b"x")
        with pytest.raises(DecryptError):
            recv.decrypt(ct, b"short")

    def test_decrypt_from_wire_tampered(self, ab_crypto):
        send, recv = ab_crypto
        blob = send.encrypt_to_wire(b"data")
        # Corrupt one base64 char near the end (ciphertext region).
        bad = blob[:-4] + ("AAAA" if blob[-4:] != "AAAA" else "BBBB")
        with pytest.raises((DecryptError, CryptoError)):
            recv.decrypt_from_wire(bad)

    def test_decrypt_from_wire_truncated(self, ab_crypto):
        send, _ = ab_crypto
        blob = send.encrypt_to_wire(b"data")
        # Strip down to just a nonce → too short.
        raw = base64.b64decode(blob)
        with pytest.raises(CryptoError):
            recv_decrypt_from_wire_short = base64.b64encode(raw[:NONCE_BYTES]).decode()
            decode_wire(recv_decrypt_from_wire_short)


# --------------------------------------------------------------------- #
# Nonce uniqueness (the plan: "NEVER reused")
# --------------------------------------------------------------------- #

class TestNonceUniqueness:
    def test_nonce_never_repeated(self, ab_crypto):
        send, _ = ab_crypto
        nonces = set()
        for _ in range(1024):
            nonce, _ = send.encrypt(b"msg")
            assert nonce not in nonces, "nonce reused!"
            nonces.add(nonce)
        assert len(nonces) == 1024

    def test_nonce_is_24_bytes(self, ab_crypto):
        send, _ = ab_crypto
        nonce, _ = send.encrypt(b"x")
        assert len(nonce) == NONCE_BYTES
        assert isinstance(nonce, bytes)

    def test_nonce_reuse_detected_raises(self, ab_crypto):
        """If os.urandom ever returned a duplicate, Crypto must raise."""
        send, _ = ab_crypto
        nonce, _ = send.encrypt(b"first")
        # The nonce is already in the seen-set; force os.urandom to hand it
        # back and assert Crypto raises instead of silently reusing it.
        import federation.crypto as C
        original = C.os.urandom

        def fake_urandom(n, _nonce=nonce, _orig=original):
            if n == NONCE_BYTES:
                return _nonce
            return _orig(n)

        C.os.urandom = fake_urandom
        try:
            with pytest.raises(NonceReuseError):
                send.encrypt(b"dup")
        finally:
            C.os.urandom = original


# --------------------------------------------------------------------- #
# Wire encode/decode + size guard (DoS protection BEFORE decrypt)
# --------------------------------------------------------------------- #

class TestWireAndSizeGuard:
    def test_encode_decode_roundtrip(self, ab_crypto):
        send, _ = ab_crypto
        nonce, ct = send.encrypt(b"abc")
        blob = encode_wire(nonce, ct)
        n2, c2 = decode_wire(blob)
        assert n2 == nonce and c2 == ct

    def test_encode_rejects_bad_nonce_len(self):
        with pytest.raises(ValueError):
            encode_wire(b"short", b"ct")

    def test_decode_rejects_non_base64(self):
        with pytest.raises(CryptoError):
            decode_wire("!!!not base64!!!")

    def test_decode_rejects_too_short(self):
        with pytest.raises(CryptoError):
            decode_wire(base64.b64encode(b"short").decode())

    def test_size_guard_rejects_huge_blob(self):
        guard = SizeGuard()
        # Build a string longer than the limit without allocating a huge
        # real blob — the guard checks the length field before decoding.
        huge = "A" * (MAX_CIPHERTEXT_BYTES * 2)
        with pytest.raises(SizeError):
            guard.check_wire(huge)

    def test_decode_wire_rejects_huge(self):
        huge = "A" * (MAX_CIPHERTEXT_BYTES * 2)
        with pytest.raises(SizeError):
            decode_wire(huge)

    def test_size_guard_envelope_ok(self):
        guard = SizeGuard()
        guard.check_envelope({"message_id": "abc"})

    def test_size_guard_envelope_missing_id(self):
        guard = SizeGuard()
        with pytest.raises(CryptoError):
            guard.check_envelope({})

    def test_size_guard_envelope_non_str_id(self):
        guard = SizeGuard()
        with pytest.raises(CryptoError):
            guard.check_envelope({"message_id": 123})

    def test_size_guard_envelope_id_too_long(self):
        guard = SizeGuard()
        with pytest.raises(SizeError):
            guard.check_envelope({"message_id": "x" * 200})

    def test_size_guard_envelope_ciphertext_too_long(self):
        guard = SizeGuard()
        with pytest.raises(SizeError):
            guard.check_envelope({"message_id": "m", "ciphertext": "A" * (MAX_CIPHERTEXT_BYTES * 2)})

    def test_size_guard_envelope_nonce_too_long(self):
        guard = SizeGuard()
        with pytest.raises(SizeError):
            guard.check_envelope({"message_id": "m", "nonce": "A" * 200})


# --------------------------------------------------------------------- #
# ReplayCache
# --------------------------------------------------------------------- #

class TestReplayCache:
    def test_first_seen_true_second_false(self):
        rc = ReplayCache(ttl=60)
        assert rc.check_and_add("m1") is True
        assert rc.check_and_add("m1") is False
        assert rc.check_and_add("m2") is True

    def test_contains(self):
        rc = ReplayCache(ttl=60)
        rc.check_and_add("m1")
        assert "m1" in rc
        assert "m2" not in rc

    def test_len(self):
        rc = ReplayCache(ttl=60)
        rc.check_and_add("m1")
        rc.check_and_add("m2")
        assert len(rc) == 2

    def test_ttl_prune(self):
        rc = ReplayCache(ttl=0.05)
        rc.check_and_add("old")
        time.sleep(0.1)
        # A new check triggers prune of the stale entry.
        assert rc.check_and_add("new") is True
        assert "old" not in rc

    def test_prune_returns_count(self):
        rc = ReplayCache(ttl=0.05)
        rc.check_and_add("a")
        rc.check_and_add("b")
        time.sleep(0.1)
        n = rc.prune()
        assert n == 2
        assert len(rc) == 0


# --------------------------------------------------------------------- #
# Envelope seal/open
# --------------------------------------------------------------------- #

class TestEnvelope:
    def test_seal_open_roundtrip(self, ab_crypto):
        send, recv = ab_crypto
        pt = build_plaintext_envelope(
            type="task", direction="forward",
            capability="image.gen@fed-b", payload={"prompt": "cat"},
        )
        ct_env = seal(send, pt)
        assert set(ct_env) == {"v", "message_id", "sender_fp", "created_at", "nonce", "ciphertext"}
        assert ct_env["v"] == ENVELOPE_VERSION
        assert ct_env["sender_fp"] == send.local_fingerprint
        inner, header = envelope_open(recv, ct_env)
        assert inner["type"] == "task"
        assert inner["payload"] == {"prompt": "cat"}
        assert header["message_id"] == ct_env["message_id"]
        assert header["sender_fp"] == send.local_fingerprint

    def test_seal_stamps_message_id_and_timestamp(self, ab_crypto):
        send, _ = ab_crypto
        pt = {"type": "task", "direction": "forward", "capability": "c", "payload": {}}
        ct_env = seal(send, pt)
        # The plaintext envelope should carry the stamped id/ts.
        _, recv = ab_crypto
        inner, _ = envelope_open(recv, ct_env)
        assert inner["message_id"] == ct_env["message_id"]
        assert inner["created_at"] == ct_env["created_at"]

    def test_seal_uses_supplied_message_id(self, ab_crypto):
        send, _ = ab_crypto
        pt = build_plaintext_envelope(
            type="task", direction="forward", capability="c", payload={},
        )
        mid = pt["message_id"]
        ct_env = seal(send, pt)
        assert ct_env["message_id"] == mid

    def test_open_rejects_wrong_sender_fp(self, ab_crypto):
        send, recv = ab_crypto
        ct_env = seal(send, build_plaintext_envelope(
            type="task", direction="forward", capability="c", payload={}))
        # Corrupt sender_fp → receiver expects A's fingerprint.
        bad = dict(ct_env)
        bad["sender_fp"] = "deadbeefdeadbeef"
        with pytest.raises(CryptoError):
            envelope_open(recv, bad)

    def test_open_rejects_tampered_ciphertext(self, ab_crypto):
        send, recv = ab_crypto
        ct_env = seal(send, build_plaintext_envelope(
            type="task", direction="forward", capability="c", payload={}))
        bad = dict(ct_env)
        # Flip the last char of the base64 ciphertext.
        ct = bad["ciphertext"]
        bad["ciphertext"] = ct[:-1] + ("A" if ct[-1] != "A" else "B")
        with pytest.raises(DecryptError):
            envelope_open(recv, bad)

    def test_open_rejects_missing_fields(self, ab_crypto):
        _, recv = ab_crypto
        with pytest.raises(CryptoError):
            envelope_open(recv, {"message_id": "m"})

    def test_open_rejects_non_object(self, ab_crypto):
        _, recv = ab_crypto
        with pytest.raises(CryptoError):
            envelope_open(recv, "not a dict")  # type: ignore[arg-type]

    def test_open_rejects_forged_routing_field(self, ab_crypto):
        """Clear header message_id must match the inner plaintext message_id."""
        send, recv = ab_crypto
        pt = build_plaintext_envelope(
            type="task", direction="forward", capability="c", payload={})
        ct_env = seal(send, pt)
        bad = dict(ct_env)
        bad["message_id"] = "different-id"
        with pytest.raises(CryptoError):
            envelope_open(recv, bad)

    def test_open_runs_size_guard_before_decrypt(self, ab_crypto, monkeypatch):
        """The structural check must run before any crypto_box call."""
        send, recv = ab_crypto
        ct_env = seal(send, build_plaintext_envelope(
            type="task", direction="forward", capability="c", payload={}))
        # Make the ciphertext huge in the header (not actually decryptable)
        # and assert decrypt is never called.
        bad = dict(ct_env)
        bad["ciphertext"] = "A" * (MAX_CIPHERTEXT_BYTES * 2)
        called = {"decrypt": False}
        orig_decrypt = recv.decrypt

        def spy(_ct, _n):
            called["decrypt"] = True
            return orig_decrypt(_ct, _n)

        recv.decrypt = spy  # type: ignore[method-assign]
        with pytest.raises((SizeError, CryptoError)):
            envelope_open(recv, bad)
        assert called["decrypt"] is False, "decrypt ran before size guard"

    def test_open_with_replay_cache(self, ab_crypto):
        """Caller-level replay dedupe using the returned header message_id."""
        send, recv = ab_crypto
        rc = ReplayCache(ttl=60)
        ct_env = seal(send, build_plaintext_envelope(
            type="task", direction="forward", capability="c", payload={}))
        # First open: fresh.
        inner1, hdr1 = envelope_open(recv, ct_env)
        assert rc.check_and_add(hdr1["message_id"]) is True
        # Second open of the same ciphertext: decrypt still succeeds (crypto
        # is stateless), but the replay cache rejects the duplicate.
        inner2, hdr2 = envelope_open(recv, ct_env)
        assert rc.check_and_add(hdr2["message_id"]) is False
        assert hdr1["message_id"] == hdr2["message_id"]


# --------------------------------------------------------------------- #
# crypto_for_peer (config integration)
# --------------------------------------------------------------------- #

class TestCryptoForPeer:
    def test_crypto_for_peer(self, ab_keypair):
        from federation.config import Peer

        peer = Peer(
            id="fed-b",
            endpoint="https://fed-b:8791",
            token="fed_t_secret",
            peer_public_key=encode_public_key(ab_keypair["pk_a"]),
        )
        c = crypto_for_peer(ab_keypair["sk_b"], peer)
        assert c.remote_fingerprint == public_key_fingerprint(ab_keypair["pk_a"])

    def test_crypto_for_peer_missing_key(self):
        from federation.config import Peer

        peer = Peer(id="fed-b", endpoint="https://fed-b", token="t")
        with pytest.raises(CryptoError):
            crypto_for_peer(b"\x00" * 32, peer)

    def test_crypto_for_peer_bad_key(self, ab_keypair):
        from federation.config import Peer

        peer = Peer(
            id="fed-b", endpoint="https://fed-b", token="t",
            peer_public_key=base64.b64encode(b"not32bytes").decode(),
        )
        with pytest.raises(CryptoError):
            crypto_for_peer(ab_keypair["sk_b"], peer)


# --------------------------------------------------------------------- #
# Integration: seal with one Crypto, open with another (cross-node)
# --------------------------------------------------------------------- #

class TestCrossNode:
    def test_two_independent_nodes_communicate(self):
        """Two separate keypairs, each node knows the other's public key."""
        pk_a, sk_a = generate_keypair()
        pk_b, sk_b = generate_keypair()
        # Node A's outbound crypto (encrypts to B).
        a_out = Crypto(sk_a, pk_b)
        # Node B's inbound crypto (decrypts from A).
        b_in = Crypto(sk_b, pk_a)
        # And the reverse direction (B → A).
        b_out = Crypto(sk_b, pk_a)
        a_in = Crypto(sk_a, pk_b)

        pt = build_plaintext_envelope(
            type="task", direction="forward",
            capability="image.gen", payload={"q": "hi"},
        )
        # A → B
        ct = seal(a_out, pt)
        inner, _ = envelope_open(b_in, ct)
        assert inner["payload"] == {"q": "hi"}

        # B → A (a reply)
        reply = build_plaintext_envelope(
            type="result", direction="reply",
            capability="image.gen", payload={"answer": 42},
            remote_task_id="stage-xyz",
        )
        ct2 = seal(b_out, reply)
        inner2, _ = envelope_open(a_in, ct2)
        assert inner2["payload"] == {"answer": 42}
        assert inner2["remote_task_id"] == "stage-xyz"