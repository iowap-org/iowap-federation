"""Tests for HttpTransport (T-095).

Uses ``httpx.MockTransport`` to fake the remote FedNode listener — no real
socket, no FastAPI app needed. The mock handler inspects the request and
returns canned responses that mirror the listener's contract.
"""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest

from federation.transports.http_transport import HttpTransport


TOKEN = "fed_t_test_token"
URL = "https://fed-b.example"


# --------------------------------------------------------------------- #
# Mock helpers
# --------------------------------------------------------------------- #
def make_mock(handler) -> httpx.MockTransport:
    return httpx.MockTransport(handler)


def discover_handler(request: httpx.Request) -> httpx.Response:
    """A minimal but faithful fake listener.

    Routes:
      GET  /fed/v1/discover        → 200 [{"capability": "image.gen"}]
      POST /fed/v1/inbox           → 202 (valid) / 401 (bad token)
      GET  /fed/v1/results/<id>    → 200 body / 404
    """
    if request.headers.get("Authorization") != f"Bearer {TOKEN}":
        return httpx.Response(401, json={"detail": "unauthorized"})
    path = request.url.path
    if request.method == "GET" and path == "/fed/v1/discover":
        return httpx.Response(200, json=[{"capability": "image.gen", "name": "image.gen@fed-b"}])
    if request.method == "POST" and path == "/fed/v1/inbox":
        try:
            body = json.loads(request.content)
        except Exception:
            return httpx.Response(400, json={"detail": "invalid json"})
        mid = body.get("message_id")
        if not mid:
            return httpx.Response(400, json={"detail": "no message_id"})
        return httpx.Response(202, json={"status": "accepted", "message_id": mid})
    if request.method == "GET" and path.startswith("/fed/v1/results/"):
        rid = path.rsplit("/", 1)[1]
        if rid == "known":
            return httpx.Response(
                200, json={"message_id": rid, "type": "result", "payload": {"out": 42}}
            )
        return httpx.Response(404)
    return httpx.Response(404)


def make_transport(handler=None) -> HttpTransport:
    t = HttpTransport({"federation": {"transport_type": "http"}})
    t._mock_transport = make_mock(handler or discover_handler)
    return t


# --------------------------------------------------------------------- #
# connect
# --------------------------------------------------------------------- #
def test_connect_stores_session_and_probes_discover():
    t = make_transport()
    t.connect(URL, TOKEN)
    assert t._url == URL  # no trailing slash normalisation needed here
    assert t._token == TOKEN
    assert t._client is not None
    t.close()


def test_connect_normalises_trailing_slash():
    t = make_transport()
    t.connect(URL + "/", TOKEN)
    assert t._url == URL
    t.close()


def test_connect_raises_on_unreachable_network_error():
    def boom(request):
        raise httpx.ConnectError("no route")
    t = make_transport(boom)
    with pytest.raises(httpx.HTTPError):
        t.connect(URL, TOKEN)
    # client must be cleaned up on failure
    assert t._client is None


def test_connect_raises_on_auth_rejected_probe():
    def deny(request):
        return httpx.Response(401, json={"detail": "no"})
    t = make_transport(deny)
    with pytest.raises(ConnectionError):
        t.connect(URL, TOKEN)
    assert t._client is None


def test_connect_requires_url_and_token():
    t = make_transport()
    with pytest.raises(ValueError):
        t.connect("", TOKEN)
    with pytest.raises(ValueError):
        t.connect(URL, "")


# --------------------------------------------------------------------- #
# sync_outbox
# --------------------------------------------------------------------- #
def test_sync_outbox_empty_dir(tmp_path):
    t = make_transport()
    t.connect(URL, TOKEN)
    outbox = tmp_path / "outbox"
    outbox.mkdir()
    assert t.sync_outbox(outbox) == []
    t.close()


def test_sync_outbox_sends_each_file_returns_ids(tmp_path):
    t = make_transport()
    t.connect(URL, TOKEN)
    outbox = tmp_path / "outbox"
    outbox.mkdir()
    for i in range(3):
        (outbox / f"msg-{i}.json").write_text(
            json.dumps({"message_id": f"mid-{i}", "type": "task", "payload": {"i": i}})
        )
    sent = t.sync_outbox(outbox)
    assert sorted(sent) == ["mid-0", "mid-1", "mid-2"]
    # T-102: accepted ACKs move the outbox file to archive/.
    assert len(list(outbox.iterdir())) == 0
    archive = tmp_path / "archive"
    archived = sorted(p.name for p in archive.iterdir())
    assert archived == ["mid-0.json", "mid-1.json", "mid-2.json"]
    t.close()


def test_sync_outbox_skips_files_without_message_id(tmp_path, caplog):
    t = make_transport()
    t.connect(URL, TOKEN)
    outbox = tmp_path / "outbox"
    outbox.mkdir()
    (outbox / "good.json").write_text(json.dumps({"message_id": "good"}))
    (outbox / "bad.json").write_text(json.dumps({"no_id": True}))
    sent = t.sync_outbox(outbox)
    assert sent == ["good"]
    t.close()


def test_sync_outbox_skips_unreadable_json(tmp_path):
    t = make_transport()
    t.connect(URL, TOKEN)
    outbox = tmp_path / "outbox"
    outbox.mkdir()
    (outbox / "good.json").write_text(json.dumps({"message_id": "good"}))
    (outbox / "broken.json").write_text("{not json")
    sent = t.sync_outbox(outbox)
    assert sent == ["good"]
    t.close()


def test_sync_outbox_missing_dir_returns_empty(tmp_path):
    t = make_transport()
    t.connect(URL, TOKEN)
    assert t.sync_outbox(tmp_path / "nope") == []
    t.close()


def test_sync_outbox_ignores_dotfiles(tmp_path):
    t = make_transport()
    t.connect(URL, TOKEN)
    outbox = tmp_path / "outbox"
    outbox.mkdir()
    (outbox / ".lock").write_text("x")
    (outbox / "msg.json").write_text(json.dumps({"message_id": "m1"}))
    sent = t.sync_outbox(outbox)
    assert sent == ["m1"]
    t.close()


def test_sync_outbox_not_connected_raises(tmp_path):
    t = make_transport()
    with pytest.raises(RuntimeError):
        t.sync_outbox(tmp_path)


# --------------------------------------------------------------------- #
# sync_inbox (minimal — full poll loop is T-096)
# --------------------------------------------------------------------- #
def test_sync_inbox_writes_known_result(tmp_path):
    t = make_transport()
    t.connect(URL, TOKEN)
    t.pending_ids = {"known", "unknown"}
    inbox = tmp_path / "inbox"
    new = t.sync_inbox(inbox)
    assert new == ["known"]
    f = inbox / "known.json"
    assert f.exists()
    body = json.loads(f.read_text())
    assert body["payload"]["out"] == 42
    t.close()


def test_sync_inbox_creates_dir(tmp_path):
    t = make_transport()
    t.connect(URL, TOKEN)
    t.pending_ids = {"known"}
    inbox = tmp_path / "deep" / "inbox"
    t.sync_inbox(inbox)
    assert inbox.is_dir()
    t.close()


def test_sync_inbox_no_pending_returns_empty(tmp_path):
    t = make_transport()
    t.connect(URL, TOKEN)
    assert t.sync_inbox(tmp_path / "inbox") == []
    t.close()


def test_sync_inbox_atomic_write(tmp_path):
    t = make_transport()
    t.connect(URL, TOKEN)
    t.pending_ids = {"known"}
    inbox = tmp_path / "inbox"
    t.sync_inbox(inbox)
    # No leftover .tmp_ files
    leftover = [p.name for p in inbox.iterdir() if p.name.startswith(".tmp_")]
    assert leftover == []
    t.close()


# --------------------------------------------------------------------- #
# discover_capabilities
# --------------------------------------------------------------------- #
def test_discover_capabilities_returns_list():
    t = make_transport()
    t.connect(URL, TOKEN)
    caps = t.discover_capabilities()
    assert isinstance(caps, list)
    assert caps[0]["capability"] == "image.gen"
    t.close()


def test_discover_capabilities_raises_on_non_list():
    def bad(request):
        if request.url.path == "/fed/v1/discover":
            return httpx.Response(200, json={"not": "a list"})
        return httpx.Response(404)
    t = make_transport(bad)
    t.connect(URL, TOKEN)
    with pytest.raises(ValueError, match="expected list"):
        t.discover_capabilities()
    t.close()


def test_discover_not_connected_raises():
    t = make_transport()
    with pytest.raises(RuntimeError):
        t.discover_capabilities()


# --------------------------------------------------------------------- #
# close
# --------------------------------------------------------------------- #
def test_close_releases_client():
    t = make_transport()
    t.connect(URL, TOKEN)
    t.close()
    assert t._client is None
    # close is idempotent
    t.close()


# --------------------------------------------------------------------- #
# T-102: ACK-driven archival, E2EE sealing, replay rejection, retry
# --------------------------------------------------------------------- #
def test_sync_outbox_seals_ciphertext_when_crypto_set(tmp_path):
    """When crypto is configured, the wire body is a ciphertext envelope."""
    from federation.crypto import Crypto, generate_keypair
    from federation.envelope import build_plaintext_envelope

    a_pk, a_sk = generate_keypair()
    b_pk, b_sk = generate_keypair()
    sender_crypto = Crypto(a_sk, b_pk)

    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.headers.get("Authorization") != f"Bearer {TOKEN}":
            return httpx.Response(401)
        if request.method == "GET" and request.url.path == "/fed/v1/discover":
            return httpx.Response(200, json=[{"capability": "image.gen"}])
        if request.method == "POST" and request.url.path == "/fed/v1/inbox":
            body = json.loads(request.content)
            captured["body"] = body
            return httpx.Response(
                202, json={"status": "accepted", "message_id": body["message_id"]}
            )
        if request.method == "POST" and request.url.path == "/fed/v1/ack":
            return httpx.Response(200, json={"status": "ok"})
        return httpx.Response(404)

    t = make_transport(handler)
    t.crypto = sender_crypto
    t.connect(URL, TOKEN)
    outbox = tmp_path / "outbox"
    outbox.mkdir()
    plaintext = build_plaintext_envelope(
        type="task", direction="forward", capability="cap", payload={"x": 1},
        message_id="enc-msg",
    )
    (outbox / "enc-msg.json").write_text(json.dumps(plaintext))
    sent = t.sync_outbox(outbox)
    assert sent == ["enc-msg"]
    # The wire body was the ciphertext envelope, not the plaintext.
    assert "ciphertext" in captured["body"]
    assert "nonce" in captured["body"]
    assert captured["body"]["message_id"] == "enc-msg"
    # File archived on accepted ACK.
    assert not (outbox / "enc-msg.json").exists()
    t.close()


def test_sync_outbox_archives_on_rejected_ack(tmp_path):
    """A rejected inline ACK archives the file (terminal)."""
    def handler(request):
        if request.headers.get("Authorization") != f"Bearer {TOKEN}":
            return httpx.Response(401)
        if request.method == "GET" and request.url.path == "/fed/v1/discover":
            return httpx.Response(200, json=[])
        if request.method == "POST" and request.url.path == "/fed/v1/inbox":
            body = json.loads(request.content)
            return httpx.Response(
                202, json={"status": "rejected", "message_id": body["message_id"]}
            )
        return httpx.Response(404)

    t = make_transport(handler)
    t.connect(URL, TOKEN)
    outbox = tmp_path / "outbox"
    outbox.mkdir()
    (outbox / "rej.json").write_text(json.dumps({"message_id": "rej"}))
    sent = t.sync_outbox(outbox)
    # Rejected ACKs are not in the accepted list.
    assert sent == []
    # File archived (terminal).
    archive = tmp_path / "archive"
    assert (archive / "rej.json").exists()
    t.close()


def test_sync_outbox_replay_409_archives_file(tmp_path):
    """A 409 replay response archives the file so we don't loop forever."""
    def handler(request):
        if request.headers.get("Authorization") != f"Bearer {TOKEN}":
            return httpx.Response(401)
        if request.method == "GET" and request.url.path == "/fed/v1/discover":
            return httpx.Response(200, json=[])
        if request.method == "POST" and request.url.path == "/fed/v1/inbox":
            return httpx.Response(409, json={"status": "rejected", "message_id": "dup"})
        return httpx.Response(404)

    t = make_transport(handler)
    t.connect(URL, TOKEN)
    outbox = tmp_path / "outbox"
    outbox.mkdir()
    (outbox / "dup.json").write_text(json.dumps({"message_id": "dup"}))
    sent = t.sync_outbox(outbox)
    assert sent == []
    assert not (outbox / "dup.json").exists()
    assert (tmp_path / "archive" / "dup.json").exists()
    t.close()


def test_sync_outbox_non_202_leaves_file_for_retry(tmp_path):
    """A non-202/non-409 response leaves the file in outbox for retry."""
    def handler(request):
        if request.headers.get("Authorization") != f"Bearer {TOKEN}":
            return httpx.Response(401)
        if request.method == "GET" and request.url.path == "/fed/v1/discover":
            return httpx.Response(200, json=[])
        if request.method == "POST" and request.url.path == "/fed/v1/inbox":
            return httpx.Response(503, json={"detail": "down"})
        return httpx.Response(404)

    t = make_transport(handler)
    t.connect(URL, TOKEN)
    outbox = tmp_path / "outbox"
    outbox.mkdir()
    (outbox / "retry.json").write_text(json.dumps({"message_id": "retry"}))
    sent = t.sync_outbox(outbox)
    assert sent == []
    # File remains for retry.
    assert (outbox / "retry.json").exists()
    t.close()


def test_sync_outbox_post_ack_back_to_receiver(tmp_path):
    """An accepted ACK triggers a POST /fed/v1/ack back to the receiver."""
    seen_ack = []
    def handler(request):
        if request.headers.get("Authorization") != f"Bearer {TOKEN}":
            return httpx.Response(401)
        if request.method == "GET" and request.url.path == "/fed/v1/discover":
            return httpx.Response(200, json=[])
        if request.method == "POST" and request.url.path == "/fed/v1/inbox":
            body = json.loads(request.content)
            return httpx.Response(
                202, json={"status": "accepted", "message_id": body["message_id"]}
            )
        if request.method == "POST" and request.url.path == "/fed/v1/ack":
            seen_ack.append(json.loads(request.content))
            return httpx.Response(200, json={"status": "ok"})
        return httpx.Response(404)

    t = make_transport(handler)
    t.connect(URL, TOKEN)
    outbox = tmp_path / "outbox"
    outbox.mkdir()
    (outbox / "ack.json").write_text(json.dumps({"message_id": "ack"}))
    t.sync_outbox(outbox)
    assert seen_ack == [{"message_id": "ack", "status": "accepted"}]
    t.close()