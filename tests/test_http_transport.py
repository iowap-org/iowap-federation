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
    # files remain (T-102 removes on ACK; T-095 only returns sent ids)
    assert len(list(outbox.iterdir())) == 3
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