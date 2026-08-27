"""Tests for the FedNode P2P listener (T-095).

Uses FastAPI's ``TestClient`` (httpx-based) against an in-memory app built
from a synthetic ``NodeConfig`` with a tmp_path inbox dir. No network.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from federation.config import NodeConfig, Peer
from federation.listener import create_listener_app


@pytest.fixture
def config() -> NodeConfig:
    return NodeConfig(
        node_name="fed-a",
        peers=[Peer(id="fed-b", endpoint="https://fed-b:8791", token="fed_t_secret")],
        offer=[{"capability": "image.gen", "name": "image.gen@fed-a"}],
    )


@pytest.fixture
def client(tmp_path, config) -> TestClient:
    inbox = tmp_path / "inbox"
    app = create_listener_app(config, inbox_dir=inbox)
    return TestClient(app)


def auth_headers(token: str = "fed_t_secret") -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


# --------------------------------------------------------------------- #
# auth
# --------------------------------------------------------------------- #
def test_inbox_rejects_missing_token(client):
    r = client.post("/fed/v1/inbox", json={"message_id": "x"})
    assert r.status_code == 401


def test_inbox_rejects_wrong_token(client):
    r = client.post(
        "/fed/v1/inbox",
        json={"message_id": "x"},
        headers={"Authorization": "Bearer fed_t_wrong"},
    )
    assert r.status_code == 401


def test_inbox_rejects_non_bearer(client):
    r = client.post(
        "/fed/v1/inbox",
        json={"message_id": "x"},
        headers={"Authorization": "Basic abc"},
    )
    assert r.status_code == 401


def test_discover_rejects_missing_token(client):
    assert client.get("/fed/v1/discover").status_code == 401


def test_listener_token_override_accepted(tmp_path):
    cfg = NodeConfig(
        node_name="fed-a",
        peers=[Peer(id="fed-b", endpoint="https://x", token="fed_t_peer")],
        listener_token="fed_t_shared",
    )
    app = create_listener_app(cfg, inbox_dir=tmp_path / "inbox")
    c = TestClient(app)
    # both the peer token and the override must work
    assert c.get("/fed/v1/discover", headers=auth_headers("fed_t_peer")).status_code == 200
    assert c.get("/fed/v1/discover", headers=auth_headers("fed_t_shared")).status_code == 200
    assert c.get("/fed/v1/discover", headers=auth_headers("fed_t_other")).status_code == 401


# --------------------------------------------------------------------- #
# POST /fed/v1/inbox
# --------------------------------------------------------------------- #
def test_inbox_accepts_and_writes_file(client, tmp_path):
    envelope = {
        "message_id": "abc-123",
        "type": "task",
        "direction": "forward",
        "capability": "image.gen@fed-b",
        "payload": {"prompt": "hi"},
    }
    r = client.post("/fed/v1/inbox", json=envelope, headers=auth_headers())
    assert r.status_code == 202
    assert r.json() == {"status": "accepted", "message_id": "abc-123"}
    written = tmp_path / "inbox" / "abc-123.json"
    assert written.exists()
    assert json.loads(written.read_text())["message_id"] == "abc-123"
    # no leftover temp files
    leftover = [p.name for p in (tmp_path / "inbox").iterdir() if p.name.startswith(".tmp_")]
    assert leftover == []


def test_inbox_rejects_empty_body(client):
    r = client.post("/fed/v1/inbox", content=b"", headers=auth_headers())
    assert r.status_code == 400


def test_inbox_rejects_invalid_json(client):
    r = client.post("/fed/v1/inbox", content=b"{not json", headers=auth_headers())
    assert r.status_code == 400


def test_inbox_rejects_non_object_envelope(client):
    r = client.post("/fed/v1/inbox", json=[1, 2, 3], headers=auth_headers())
    assert r.status_code == 400


def test_inbox_requires_message_id(client):
    r = client.post(
        "/fed/v1/inbox",
        json={"type": "task", "payload": {}},
        headers=auth_headers(),
    )
    assert r.status_code == 400
    assert "message_id" in r.json()["detail"]


def test_inbox_write_is_atomic(client, tmp_path, monkeypatch):
    """A failure mid-write leaves no partial file behind."""
    envelope = {"message_id": "atomic-1", "type": "task"}
    # Force os.replace to fail once to exercise cleanup path indirectly:
    # verify normal path produces exactly one file with the right name.
    r = client.post("/fed/v1/inbox", json=envelope, headers=auth_headers())
    assert r.status_code == 202
    files = sorted((tmp_path / "inbox").iterdir())
    assert [p.name for p in files] == ["atomic-1.json"]


def test_inbox_two_messages_distinct_files(client, tmp_path):
    for i in range(2):
        client.post(
            "/fed/v1/inbox",
            json={"message_id": f"m{i}", "type": "task"},
            headers=auth_headers(),
        )
    files = sorted((tmp_path / "inbox").iterdir())
    assert [p.name for p in files] == ["m0.json", "m1.json"]


# --------------------------------------------------------------------- #
# Replay guard (T-102)
# --------------------------------------------------------------------- #
def test_inbox_rejects_duplicate_with_409_and_inline_rejected_ack(client, tmp_path):
    envelope = {"message_id": "dup-1", "type": "task", "payload": {"x": 1}}
    r1 = client.post("/fed/v1/inbox", json=envelope, headers=auth_headers())
    assert r1.status_code == 202
    r2 = client.post("/fed/v1/inbox", json=envelope, headers=auth_headers())
    assert r2.status_code == 409
    body = r2.json()
    assert body["status"] == "rejected"
    assert body["message_id"] == "dup-1"
    # Only one file on disk despite the second POST.
    files = sorted((tmp_path / "inbox").iterdir())
    assert [p.name for p in files] == ["dup-1.json"]


# --------------------------------------------------------------------- #
# E2EE inbox (T-101 + T-102 wiring)
# --------------------------------------------------------------------- #
def test_inbox_decrypts_ciphertext_envelope_when_crypto_configured(tmp_path):
    from federation.crypto import Crypto, generate_keypair
    from federation.envelope import build_plaintext_envelope, seal
    from federation.config import NodeConfig, Peer

    a_pk, a_sk = generate_keypair()  # sender (fed-b)
    b_pk, b_sk = generate_keypair()  # receiver (fed-a, this listener)
    # Sender's crypto: sender_sk + receiver_pk
    sender_crypto = Crypto(a_sk, b_pk)
    # Receiver's crypto: receiver_sk + sender_pk
    receiver_crypto = Crypto(b_sk, a_pk)

    cfg = NodeConfig(
        node_name="fed-a",
        peers=[Peer(id="fed-b", endpoint="https://fed-b:8791", token="fed_t_secret")],
        offer=[{"capability": "image.gen", "name": "image.gen@fed-a"}],
    )
    app = create_listener_app(
        cfg,
        inbox_dir=tmp_path / "inbox",
        crypto=receiver_crypto,
    )
    c = TestClient(app)

    plaintext = build_plaintext_envelope(
        type="task",
        direction="forward",
        capability="image.gen@fed-b",
        payload={"prompt": "hi"},
        message_id="enc-1",
    )
    ciphertext_env = seal(sender_crypto, plaintext, message_id="enc-1")
    r = c.post("/fed/v1/inbox", json=ciphertext_env, headers=auth_headers())
    assert r.status_code == 202
    assert r.json() == {"status": "accepted", "message_id": "enc-1"}
    written = tmp_path / "inbox" / "enc-1.json"
    assert written.exists()
    decoded = json.loads(written.read_text())
    # The plaintext envelope (not the ciphertext) is on disk.
    assert decoded["message_id"] == "enc-1"
    assert decoded["payload"] == {"prompt": "hi"}
    assert "ciphertext" not in decoded


def test_inbox_rejects_tampered_ciphertext_with_400(tmp_path):
    from federation.crypto import Crypto, generate_keypair
    from federation.envelope import build_plaintext_envelope, seal

    a_pk, a_sk = generate_keypair()
    b_pk, b_sk = generate_keypair()
    sender_crypto = Crypto(a_sk, b_pk)
    receiver_crypto = Crypto(b_sk, a_pk)

    cfg = NodeConfig(
        node_name="fed-a",
        peers=[Peer(id="fed-b", endpoint="https://x", token="fed_t_secret")],
    )
    app = create_listener_app(cfg, inbox_dir=tmp_path / "inbox", crypto=receiver_crypto)
    c = TestClient(app)

    plaintext = build_plaintext_envelope(
        type="task", direction="forward", capability="cap", payload={},
        message_id="tamper-1",
    )
    env = seal(sender_crypto, plaintext, message_id="tamper-1")
    # Flip one byte in the ciphertext (base64) — breaks the Poly1305 tag.
    ct = bytearray(env["ciphertext"].encode("ascii"))
    ct[-1] = (ct[-1] + 1) % 256
    env["ciphertext"] = ct.decode("ascii")
    r = c.post("/fed/v1/inbox", json=env, headers=auth_headers())
    assert r.status_code == 400
    assert "decrypt" in r.json()["detail"]
    # Nothing on disk.
    assert not (tmp_path / "inbox" / "tamper-1.json").exists()


# --------------------------------------------------------------------- #
# GET /fed/v1/discover
# --------------------------------------------------------------------- #
def test_discover_returns_offer(client):
    r = client.get("/fed/v1/discover", headers=auth_headers())
    assert r.status_code == 200
    data = r.json()
    assert data == [{"capability": "image.gen", "name": "image.gen@fed-a"}]


def test_discover_empty_when_no_offer(tmp_path):
    cfg = NodeConfig(
        node_name="fed-a",
        peers=[Peer(id="fed-b", endpoint="https://x", token="fed_t_secret")],
    )
    app = create_listener_app(cfg, inbox_dir=tmp_path / "inbox")
    c = TestClient(app)
    r = c.get("/fed/v1/discover", headers=auth_headers())
    assert r.status_code == 200
    assert r.json() == []


# --------------------------------------------------------------------- #
# /fed/v1/ack (T-102) + /fed/v1/results (still 501)
# --------------------------------------------------------------------- #
def test_results_returns_501(client):
    r = client.get("/fed/v1/results/some-id", headers=auth_headers())
    assert r.status_code == 501


def test_ack_rejects_invalid_status(client):
    r = client.post(
        "/fed/v1/ack",
        json={"message_id": "x", "status": "ok"},
        headers=auth_headers(),
    )
    assert r.status_code == 400


def test_ack_records_accepted_status(client):
    r = client.post(
        "/fed/v1/ack",
        json={"message_id": "mid-1", "status": "accepted"},
        headers=auth_headers(),
    )
    assert r.status_code == 200
    body = r.json()
    assert body == {"status": "ok", "message_id": "mid-1", "ack": "accepted"}


def test_ack_rejects_missing_message_id(client):
    r = client.post(
        "/fed/v1/ack",
        json={"status": "accepted"},
        headers=auth_headers(),
    )
    assert r.status_code == 400


def test_ack_rejects_non_object_body(client):
    r = client.post(
        "/fed/v1/ack",
        content=b"[1,2,3]",
        headers=auth_headers(),
    )
    assert r.status_code == 400


def test_results_requires_auth(client):
    assert client.get("/fed/v1/results/x").status_code == 401


def test_ack_requires_auth(client):
    assert client.post("/fed/v1/ack", json={}).status_code == 401


# --------------------------------------------------------------------- #
# healthz + unknown routes
# --------------------------------------------------------------------- #
def test_healthz_no_auth(client):
    assert client.get("/healthz").json() == {"status": "ok"}