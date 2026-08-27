"""Integration tests for the T-103 SQLite metadata index wiring.

These exercise the daemon / transport / listener / fairuse with a real
``MetadataIndex`` (backed by a tmp_path ``state.db``) to verify that:

* the daemon mirrors forward + inbox message metadata into the index,
* the index's ``count_in_flight`` / ``count_today`` back the heartbeat and
  fair-use daily cap,
* the transport mirrors ACK outcomes (accepted → ARCHIVED, rejected →
  REJECTED, exhausted retries → FAILED) into the index,
* the listener inserts + marks-processed every accepted inbox POST,
* hash-partitioned outbox paths are used when an index is wired, and
* terminal-state messages are archived + pruned on the sync sweep.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import httpx
import pytest

from federation import paths as fed_paths
from federation.config import NodeConfig, Peer
from federation.fairuse import FairUse, FairUseRegistry
from federation.federation_node import FederationDaemon
from federation.index import (
    MetadataIndex,
    partition_path,
    partition_subdir,
)
from federation.lifecycle import MessageState, RetryPolicy
from federation.listener import create_listener_app
from federation.transports.http_transport import HttpTransport


# --------------------------------------------------------------------- #
# Shared fakes (mirror test_federation_node.py)
# --------------------------------------------------------------------- #

class FakeRelayClient:
    def __init__(self) -> None:
        self.meta = {"node_id": "fed-a-node"}
        self.base_url = "https://relay-a.local"
        self.heartbeat_calls: list[tuple[list[dict], dict[str, int]]] = []
        self.claim_calls: list[str] = []
        self._claims: list[dict[str, Any]] = []
        self.submit_calls: list[tuple] = []
        self.complete_calls: list[tuple] = []
        self._backoff = 0.0

    def heartbeat(self, caps, in_flight):
        self.heartbeat_calls.append((caps, in_flight))
        return {"status": "ok"}

    def claim(self, capability):
        self.claim_calls.append(capability)
        if self._claims:
            return self._claims.pop(0)
        return None

    def complete(self, task_id, stage_id, result):
        self.complete_calls.append((task_id, stage_id, result))
        return {"status": "ok"}

    def submit_simple_task(self, capability, payload, *, name="", priority=0, owner_node_id=None):
        self.submit_calls.append((capability, payload, name, priority, owner_node_id))
        return {"task_id": f"local-{len(self.submit_calls)}"}

    def get_task(self, task_id):
        return {"task_id": task_id, "stages": []}

    def maybe_refresh_token(self):
        pass

    def _current_backoff(self):
        return self._backoff


class FakeTransport:
    """Records sync calls; outbox/inbox are real dirs so files land on disk."""

    def __init__(self) -> None:
        self.outbox_calls: list[Path] = []
        self.inbox_calls: list[Path] = []
        self.closed = False
        self.index: MetadataIndex | None = None

    def sync_outbox(self, outbox_dir: Path) -> list[str]:
        self.outbox_calls.append(outbox_dir)
        sent: list[str] = []
        if outbox_dir.is_dir():
            for f in sorted(outbox_dir.rglob("*.json")):
                if f.is_file() and not f.name.startswith("."):
                    try:
                        sent.append(json.loads(f.read_text())["message_id"])
                    except Exception:
                        pass
        return sent

    def sync_inbox(self, inbox_dir: Path) -> list[str]:
        self.inbox_calls.append(inbox_dir)
        return []

    def close(self) -> None:
        self.closed = True


# --------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------- #

@pytest.fixture
def isolated_fs(tmp_path, monkeypatch):
    monkeypatch.setattr(fed_paths, "BASE_DIR", tmp_path)
    return tmp_path


@pytest.fixture
def peer() -> Peer:
    return Peer(id="fed-b", endpoint="https://fed-b:8791", token="fed_t_secret")


@pytest.fixture
def fed_config(peer: Peer) -> NodeConfig:
    return NodeConfig(
        node_name="fed-a",
        peers=[peer],
        offer=[{"capability": "image.gen", "name": "image.gen"}],
    )


@pytest.fixture
def idx(tmp_path: Path) -> MetadataIndex:
    return MetadataIndex(tmp_path / "state.db")


@pytest.fixture
def fake_client() -> FakeRelayClient:
    return FakeRelayClient()


def make_daemon(
    client: FakeRelayClient,
    fed_config: NodeConfig,
    isolated_fs: Path,
    idx: MetadataIndex,
    *,
    transport_factory=None,
    fairuse_registry: FairUseRegistry | None = None,
) -> FederationDaemon:
    return FederationDaemon(
        client,
        {"heartbeat_interval": 30},
        fed_config=fed_config,
        forward_caps=["image.gen"],
        transport_factory=transport_factory,
        base_dir=isolated_fs,
        index=idx,
        fairuse_registry=fairuse_registry,
    )


def _write_inbox(peer_id: str, fs: Path, env: dict[str, Any]) -> Path:
    inbox = fs / "federation" / peer_id / "inbox"
    inbox.mkdir(parents=True, exist_ok=True)
    f = inbox / f"{env['message_id']}.json"
    f.write_text(json.dumps(env))
    return f


# --------------------------------------------------------------------- #
# Daemon: forward writes partitioned outbox + inserts index row
# --------------------------------------------------------------------- #

def test_forward_uses_partitioned_outbox_and_inserts_index_row(
    fake_client, fed_config, isolated_fs, idx
):
    fake_client._claims = [
        {"stage_id": "st-1", "task_id": "tk-1", "capability": "image.gen",
         "payload": {"prompt": "cat"}}
    ]
    d = make_daemon(fake_client, fed_config, isolated_fs, idx)
    mid = d._claim_and_forward("image.gen")  # type: ignore[attr-defined]
    # actually returns None; get the mid from the outbox file
    assert mid is None
    outbox = isolated_fs / "federation" / "fed-b" / "outbox"
    # partitioned: outbox/<xx>/<message_id>.json
    files = list(outbox.rglob("*.json"))
    assert len(files) == 1
    f = files[0]
    env = json.loads(f.read_text())
    mid = env["message_id"]
    # the partition subdir matches SHA256(mid)[:2]
    assert f.parent.name == partition_subdir(mid)
    assert f.parent.parent == outbox
    # index row inserted at QUEUED
    row = idx.get(mid)
    assert row is not None
    assert row["state"] == "QUEUED"
    assert row["type"] == "task"
    assert row["remote"] == "fed-b"
    assert row["capability"] == "image.gen"
    assert row["remote_task_id"] == "st-1"
    assert row["retry_count"] == 0


def test_forward_without_index_uses_flat_outbox(fake_client, fed_config, isolated_fs):
    """No index → legacy flat outbox path (backwards compat)."""
    fake_client._claims = [
        {"stage_id": "st-1", "task_id": "tk-1", "capability": "image.gen", "payload": {}}
    ]
    d = FederationDaemon(
        fake_client, {}, fed_config=fed_config, forward_caps=["image.gen"],
        base_dir=isolated_fs,
    )
    d._claim_and_forward("image.gen")
    outbox = isolated_fs / "federation" / "fed-b" / "outbox"
    files = list(outbox.iterdir())
    assert len(files) == 1
    # flat — no partition subdir
    assert files[0].parent == outbox


# --------------------------------------------------------------------- #
# Daemon: inbox processing marks processed + advances forward row
# --------------------------------------------------------------------- #

def test_inbox_forward_marks_processed_in_index(
    fake_client, fed_config, isolated_fs, idx
):
    d = make_daemon(fake_client, fed_config, isolated_fs, idx)
    env = {
        "message_id": "m-fwd-1", "type": "task", "direction": "forward",
        "capability": "image.gen", "remote_task_id": "remote-st-9",
        "payload": {"prompt": "hi"},
    }
    f = _write_inbox("fed-b", isolated_fs, env)
    d._process_inbox_file(fed_config.peers[0], f)

    row = idx.get("m-fwd-1")
    assert row is not None
    assert row["processed"] is True
    assert row["remote"] == "fed-b"


def test_inbox_reply_advances_forward_row_to_completed(
    fake_client, fed_config, isolated_fs, idx
):
    d = make_daemon(fake_client, fed_config, isolated_fs, idx)
    # 1. forward a stage → index row at QUEUED
    fake_client._claims = [
        {"stage_id": "st-42", "task_id": "tk-42", "capability": "image.gen",
         "payload": {"prompt": "x"}}
    ]
    d._claim_and_forward("image.gen")
    fwd_row = idx.get_by_remote_task_id("st-42")
    assert fwd_row is not None
    assert fwd_row["state"] == "QUEUED"
    mid_fwd = fwd_row["message_id"]

    # 2. reply arrives correlating via remote_task_id == st-42
    reply = {
        "message_id": "m-reply-42", "type": "result", "direction": "reply",
        "capability": "image.gen", "remote_task_id": "st-42",
        "payload": {"image": "data"},
    }
    f = _write_inbox("fed-b", isolated_fs, reply)
    action = d._process_inbox_file(fed_config.peers[0], f)
    assert action == "completed"

    # the forward message's index row advanced to COMPLETED
    assert idx.get_state(mid_fwd) == "COMPLETED"
    # the reply message is marked processed
    assert idx.is_processed("m-reply-42") is True


# --------------------------------------------------------------------- #
# Daemon: heartbeat in_flight map uses index count_in_flight
# --------------------------------------------------------------------- #

def test_heartbeat_uses_index_in_flight_count(
    fake_client, fed_config, isolated_fs, idx
):
    d = make_daemon(fake_client, fed_config, isolated_fs, idx)
    # seed the index with two in-flight forwards for image.gen
    idx.insert("m-1", state=MessageState.QUEUED, capability="image.gen", remote="fed-b")
    idx.insert("m-2", state=MessageState.SENT, capability="image.gen", remote="fed-b")
    inflight = d._in_flight_map()
    assert inflight["image.gen"] == 2
    # the synthetic federation cap is in there too
    assert "federation" in inflight


def test_heartbeat_falls_back_to_in_memory_when_no_index(
    fake_client, fed_config, isolated_fs
):
    d = FederationDaemon(
        fake_client, {}, fed_config=fed_config, forward_caps=["image.gen"],
        base_dir=isolated_fs,
    )
    d._in_flight["federation"] = 3
    inflight = d._in_flight_map()
    assert inflight == {"federation": 3}


# --------------------------------------------------------------------- #
# Daemon: sync sweep archives terminal-state payloads + prunes
# --------------------------------------------------------------------- #

def test_sync_sweep_archives_terminal_payloads(
    fake_client, fed_config, isolated_fs, idx
):
    d = make_daemon(
        fake_client, fed_config, isolated_fs, idx,
        transport_factory=lambda peer: FakeTransport(),
    )
    # seed a FAILED message + its partitioned outbox file
    idx.insert("m-failed", state=MessageState.FAILED, remote="fed-b", capability="image.gen")
    outbox = isolated_fs / "federation" / "fed-b" / "outbox"
    p = partition_path(outbox, "m-failed")
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"message_id": "m-failed"}))
    assert p.exists()

    archived = d._archive_terminal(fed_config.peers[0])
    assert archived >= 1
    archive = isolated_fs / "federation" / "fed-b" / "archive"
    assert (archive / "m-failed.json").exists()
    assert not p.exists()


def test_sync_sweep_prunes_old_terminal_metadata(
    fake_client, fed_config, isolated_fs, idx, tmp_path
):
    # short TTL so the sweep reaps the metadata row
    d = make_daemon(
        fake_client, fed_config, isolated_fs, idx,
        transport_factory=lambda peer: FakeTransport(),
    )
    d._ttl_seconds = 0.0  # prune everything immediately
    idx.insert("m-failed", state=MessageState.FAILED, remote="fed-b")
    # bump created_at into the past by re-inserting with an old timestamp
    from datetime import datetime, timedelta, timezone
    old = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    idx.delete("m-failed")
    idx.insert("m-failed", state=MessageState.FAILED, remote="fed-b", created_at=old)
    d._archive_terminal(fed_config.peers[0])
    assert idx.get("m-failed") is None


def test_sync_one_peer_advances_index_to_sent_for_accepted(
    fake_client, fed_config, isolated_fs, idx
):
    transport = FakeTransport()
    d = make_daemon(
        fake_client, fed_config, isolated_fs, idx,
        transport_factory=lambda peer: transport,
    )
    # seed a queued forward + outbox file
    idx.insert("m-q", state=MessageState.QUEUED, remote="fed-b", capability="image.gen")
    outbox = isolated_fs / "federation" / "fed-b" / "outbox"
    p = partition_path(outbox, "m-q")
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"message_id": "m-q"}))

    d._sync_one_peer(fed_config.peers[0])
    assert idx.get_state("m-q") == "SENT"


# --------------------------------------------------------------------- #
# FairUse: index-backed count_today gates claims
# --------------------------------------------------------------------- #

def test_fairuse_index_backed_daily_cap_blocks(fake_client, fed_config, isolated_fs, idx):
    """When an index is wired, count_today reads from it (not the file)."""
    pol = FairUse(
        "image.gen", max_parallel=50, max_daily=2,
        state_dir=isolated_fs, index=idx, remote="fed-b",
    )
    reg = FairUseRegistry(enforce=True, policies={"image.gen": pol})
    d = make_daemon(
        fake_client, fed_config, isolated_fs, idx,
        fairuse_registry=reg,
    )
    # seed the index with 2 forwarded messages created today (so count_today == 2)
    idx.insert("m-1", state=MessageState.QUEUED, capability="image.gen", remote="fed-b")
    idx.insert("m-2", state=MessageState.QUEUED, capability="image.gen", remote="fed-b")
    assert pol.daily_count == 2

    # a 3rd claim must be gated by the daily cap (no client.claim call)
    fake_client._claims = [
        {"stage_id": "st-3", "task_id": "tk-3", "capability": "image.gen", "payload": {}}
    ]
    d._claim_and_forward("image.gen")
    assert fake_client.claim_calls == []
    assert d.tasks_forwarded == 0


def test_fairuse_without_index_uses_file_counter(
    fake_client, fed_config, isolated_fs, idx
):
    """No index → the per-day file counter is the source (regression guard)."""
    pol = FairUse("image.gen", max_parallel=50, max_daily=2, state_dir=isolated_fs)
    assert pol.daily_count == 0
    pol.claim()
    pol.claim()
    assert pol.daily_count == 2
    # 3rd is gated
    assert pol.claim() is False


# --------------------------------------------------------------------- #
# Transport: ACK outcomes mirrored into the index
# --------------------------------------------------------------------- #

URL = "https://fed-b.example"
TOKEN = "fed_t_test_token"


def _accept_handler(req: httpx.Request) -> httpx.Response:
    if req.headers.get("Authorization") != f"Bearer {TOKEN}":
        return httpx.Response(401)
    if req.method == "POST" and req.url.path == "/fed/v1/inbox":
        body = json.loads(req.content)
        return httpx.Response(202, json={"status": "accepted", "message_id": body["message_id"]})
    if req.method == "GET" and req.url.path == "/fed/v1/discover":
        return httpx.Response(200, json=[{"capability": "image.gen"}])
    if req.method == "POST" and req.url.path == "/fed/v1/ack":
        return httpx.Response(200, json={"status": "ok"})
    return httpx.Response(404)


def _make_transport(handler) -> HttpTransport:
    t = HttpTransport({"federation": {}})
    t._mock_transport = httpx.MockTransport(handler)
    return t


def test_transport_accepted_ack_mirrors_archived_in_index(tmp_path, idx):
    t = _make_transport(_accept_handler)
    t.connect(URL, TOKEN)
    t.index = idx
    outbox = tmp_path / "outbox"
    outbox.mkdir()
    mid = "mid-accept"
    (outbox / f"{mid}.json").write_text(json.dumps({"message_id": mid}))
    idx.insert(mid, state=MessageState.QUEUED)

    t.sync_outbox(outbox)
    assert idx.get_state(mid) == "ARCHIVED"
    t.close()


def test_transport_rejected_ack_mirrors_rejected_in_index(tmp_path, idx):
    def reject_handler(req):
        if req.method == "POST" and req.url.path == "/fed/v1/inbox":
            body = json.loads(req.content)
            return httpx.Response(409, json={"status": "rejected", "message_id": body["message_id"]})
        if req.method == "GET" and req.url.path == "/fed/v1/discover":
            return httpx.Response(200, json=[{"capability": "image.gen"}])
        if req.method == "POST" and req.url.path == "/fed/v1/ack":
            return httpx.Response(200, json={"status": "ok"})
        return httpx.Response(404)

    t = _make_transport(reject_handler)
    t.connect(URL, TOKEN)
    t.index = idx
    outbox = tmp_path / "outbox"
    outbox.mkdir()
    mid = "mid-reject"
    (outbox / f"{mid}.json").write_text(json.dumps({"message_id": mid}))
    idx.insert(mid, state=MessageState.QUEUED)

    t.sync_outbox(outbox)
    assert idx.get_state(mid) == "REJECTED"
    t.close()


def test_transport_exhausted_retries_mirrors_failed_in_index(tmp_path, idx):
    """A message that exhausts retries is mirrored to FAILED."""
    # always-500 handler → every send fails; with a 1-attempt policy it FAILs fast
    def fail_handler(req):
        if req.method == "GET" and req.url.path == "/fed/v1/discover":
            return httpx.Response(200, json=[{"capability": "image.gen"}])
        if req.method == "POST" and req.url.path == "/fed/v1/inbox":
            return httpx.Response(500, json={"detail": "boom"})
        return httpx.Response(404)

    t = _make_transport(fail_handler)
    t.connect(URL, TOKEN)
    t.index = idx
    # 1 attempt → first failure exhausts
    t.retry_policy = RetryPolicy(max_attempts=1, sequence=(0.0,))
    outbox = tmp_path / "outbox"
    outbox.mkdir()
    mid = "mid-fail"
    (outbox / f"{mid}.json").write_text(json.dumps({"message_id": mid}))
    idx.insert(mid, state=MessageState.QUEUED)

    t.sync_outbox(outbox)
    assert idx.get_state(mid) == "FAILED"
    # retry_count was incremented in the index
    assert idx.get(mid)["retry_count"] >= 1
    t.close()


# --------------------------------------------------------------------- #
# Listener: accepted inbox POST inserts + marks processed
# --------------------------------------------------------------------- #

def test_listener_inserts_and_marks_processed_on_inbox_post(tmp_path, idx):
    config = NodeConfig(
        node_name="fed-a",
        peers=[Peer(id="fed-b", endpoint="https://b", token="fed_t_secret")],
    )
    inbox = tmp_path / "inbox"
    app = create_listener_app(config, inbox_dir=inbox, index=idx)
    client = httpx.Client(
        base_url="http://test", transport=httpx.MockTransport(lambda r: httpx.Response(404))
    )
    # use the FastAPI TestClient instead
    from fastapi.testclient import TestClient
    tc = TestClient(app)
    mid = "m-listen-1"
    body = {
        "message_id": mid, "type": "task", "direction": "forward",
        "capability": "image.gen", "remote_task_id": "st-x", "payload": {"a": 1},
    }
    resp = tc.post(
        "/fed/v1/inbox", json=body, headers={"Authorization": "Bearer fed_t_secret"}
    )
    assert resp.status_code == 202
    row = idx.get(mid)
    assert row is not None
    assert row["state"] == "ACKED"
    assert row["remote"] == "fed-a"
    assert row["capability"] == "image.gen"
    assert row["remote_task_id"] == "st-x"
    assert row["processed"] is True
    assert row["size"] is not None and row["size"] > 0
    _ = client  # silence


def test_listener_without_index_still_works(tmp_path):
    """No index → listener behaves as before (regression guard)."""
    config = NodeConfig(
        node_name="fed-a",
        peers=[Peer(id="fed-b", endpoint="https://b", token="fed_t_secret")],
    )
    inbox = tmp_path / "inbox"
    app = create_listener_app(config, inbox_dir=inbox)
    from fastapi.testclient import TestClient
    tc = TestClient(app)
    resp = tc.post(
        "/fed/v1/inbox",
        json={"message_id": "m-1", "type": "task", "payload": {}},
        headers={"Authorization": "Bearer fed_t_secret"},
    )
    assert resp.status_code == 202
    assert (inbox / "m-1.json").exists()


# --------------------------------------------------------------------- #
# End-to-end: claim → forward → ACK → archive → index row terminal
# --------------------------------------------------------------------- #

def test_e2e_forward_then_ack_archives_index_row(
    fake_client, fed_config, isolated_fs, idx
):
    """Full forward → sync (fake transport that just reports sent) → the
    index row stays tracked; no archive sweep happens until terminal state."""
    transport = FakeTransport()
    d = make_daemon(
        fake_client, fed_config, isolated_fs, idx,
        transport_factory=lambda peer: transport,
    )
    fake_client._claims = [
        {"stage_id": "st-1", "task_id": "tk-1", "capability": "image.gen", "payload": {}}
    ]
    d._claim_and_forward("image.gen")
    outbox = isolated_fs / "federation" / "fed-b" / "outbox"
    files = list(outbox.rglob("*.json"))
    assert len(files) == 1
    mid = json.loads(files[0].read_text())["message_id"]
    assert idx.get_state(mid) == "QUEUED"

    d._sync_one_peer(fed_config.peers[0])
    # fake transport reports "sent" → index advanced to SENT
    assert idx.get_state(mid) == "SENT"


# --------------------------------------------------------------------- #
# Status file includes index-backed counts
# --------------------------------------------------------------------- #

def test_status_file_includes_index_block(fake_client, fed_config, isolated_fs, idx):
    d = make_daemon(fake_client, fed_config, isolated_fs, idx)
    idx.insert("m-1", state=MessageState.QUEUED, capability="image.gen", remote="fed-b")
    idx.insert("m-2", state=MessageState.ARCHIVED, capability="image.gen", remote="fed-b")
    idx.insert("m-3", state=MessageState.FAILED, capability="image.gen", remote="fed-b")
    d._write_status()
    status = json.loads((isolated_fs / "federation_status.json").read_text())
    assert "index" in status
    assert status["index"]["in_flight"] == 1  # only m-1 (QUEUED)
    assert status["index"]["today"] == 3
    assert status["index"]["archived"] == 1
    assert status["index"]["failed"] == 1


def test_status_file_no_index_block_when_not_wired(
    fake_client, fed_config, isolated_fs
):
    d = FederationDaemon(
        fake_client, {}, fed_config=fed_config, forward_caps=["image.gen"],
        base_dir=isolated_fs,
    )
    d._write_status()
    status = json.loads((isolated_fs / "federation_status.json").read_text())
    assert "index" not in status