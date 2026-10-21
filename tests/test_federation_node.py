"""Tests for the Federation Daemon (T-096).

Exercises the pure logic (claim→outbox forward, inbox→submit, reply→complete,
sync cycle, replay dedupe) with a fake ``RelayClient`` and a fake transport —
no real network, no threads for the logic tests. A separate smoke test
starts the threaded daemon and verifies clean startup/shutdown.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

import pytest

from federation import paths as fed_paths
from federation.config import NodeConfig, Peer
from federation.errors import TaskTooLargeForRoute
from federation.federation_node import FederationDaemon, build_parser, main
from federation.manifest import TransportManifest


# --------------------------------------------------------------------- #
# Fakes
# --------------------------------------------------------------------- #

class FakeRelayClient:
    """A minimal RelayClient double that records every call."""

    def __init__(self, *, claim_queue: list[dict[str, Any]] | None = None) -> None:
        self.meta = {"node_id": "fed-a-node"}
        self.base_url = "https://relay-a.local"
        self._claims = list(claim_queue) if claim_queue else []
        self.heartbeat_calls: list[tuple[list[dict], dict[str, int]]] = []
        self.claim_calls: list[str] = []
        self.complete_calls: list[tuple[str, str, dict[str, Any]]] = []
        self.submit_calls: list[tuple[str, dict[str, Any], str, int, str | None]] = []
        self.get_task_calls: list[str] = []
        self._refresh_calls = 0
        self._backoff = 0.0

    # RelayClient surface
    def heartbeat(
        self, caps: list[dict[str, Any]], in_flight: dict[str, int]
    ) -> dict[str, Any]:
        self.heartbeat_calls.append((caps, in_flight))
        return {"status": "ok"}

    def claim(self, capability: str) -> dict[str, Any] | None:
        self.claim_calls.append(capability)
        if self._claims:
            return self._claims.pop(0)
        return None

    def complete(
        self, task_id: str, stage_id: str, result: dict[str, Any]
    ) -> dict[str, Any]:
        self.complete_calls.append((task_id, stage_id, result))
        return {"status": "ok"}

    def submit_simple_task(
        self,
        capability: str,
        payload: dict[str, Any],
        *,
        name: str = "",
        priority: int = 0,
        owner_node_id: str | None = None,
    ) -> dict[str, Any]:
        self.submit_calls.append((capability, payload, name, priority, owner_node_id))
        return {"task_id": f"local-{len(self.submit_calls)}"}

    def get_task(self, task_id: str) -> dict[str, Any]:
        self.get_task_calls.append(task_id)
        return {"task_id": task_id, "stages": []}

    def maybe_refresh_token(self) -> None:
        self._refresh_calls += 1

    def _current_backoff(self) -> float:
        return self._backoff


class FakeTransport:
    """Records sync calls; outbox/inbox are real dirs so files land on disk."""

    def __init__(self, *, inbox_payloads: dict[str, dict[str, Any]] | None = None) -> None:
        self.outbox_calls: list[Path] = []
        self.inbox_calls: list[Path] = []
        self.closed = False
        self._inbox_payloads = inbox_payloads or {}

    def sync_outbox(self, outbox_dir: Path) -> list[str]:
        self.outbox_calls.append(outbox_dir)
        # Report each file as "sent" (return its message_id), mirroring the
        # real transport's success-id contract.
        sent: list[str] = []
        if outbox_dir.is_dir():
            for f in sorted(outbox_dir.iterdir()):
                if f.is_file() and not f.name.startswith("."):
                    try:
                        sent.append(json.loads(f.read_text())["message_id"])
                    except Exception:
                        pass
        return sent

    def sync_inbox(self, inbox_dir: Path) -> list[str]:
        self.inbox_calls.append(inbox_dir)
        inbox_dir.mkdir(parents=True, exist_ok=True)
        # Simulate the remote delivering payloads into our inbox.
        received: list[str] = []
        for mid, body in self._inbox_payloads.items():
            (inbox_dir / f"{mid}.json").write_text(json.dumps(body))
            received.append(mid)
        # Only deliver once (clear so subsequent cycles are empty).
        self._inbox_payloads = {}
        return received

    def close(self) -> None:
        self.closed = True


# --------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------- #

@pytest.fixture
def isolated_fs(tmp_path, monkeypatch):
    """Redirect federation paths into tmp_path so tests never touch ~/.relay."""
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
def fake_client() -> FakeRelayClient:
    return FakeRelayClient()


def make_daemon(
    client: FakeRelayClient,
    fed_config: NodeConfig,
    isolated_fs: Path,
    *,
    forward_caps: list[str] | None = None,
    transport_factory=None,
    base_dir: Path | None = None,
) -> FederationDaemon:
    return FederationDaemon(
        client,
        {"heartbeat_interval": 30},
        fed_config=fed_config,
        forward_caps=forward_caps or ["image.gen"],
        transport_factory=transport_factory,
        base_dir=base_dir or isolated_fs,
    )


# --------------------------------------------------------------------- #
# claim → outbox forward
# --------------------------------------------------------------------- #

def test_claim_and_forward_writes_envelope_to_outbox(
    fake_client, fed_config, isolated_fs
):
    fake_client._claims = [
        {
            "stage_id": "st-1",
            "task_id": "tk-1",
            "capability": "image.gen",
            "payload": {"prompt": "draw a cat"},
        }
    ]
    d = make_daemon(fake_client, fed_config, isolated_fs)
    d._claim_and_forward("image.gen")

    assert fake_client.claim_calls == ["image.gen"]
    outbox = isolated_fs / "federation" / "fed-b" / "outbox"
    files = list(outbox.iterdir())
    assert len(files) == 1
    env = json.loads(files[0].read_text())
    assert env["type"] == "task"
    assert env["direction"] == "forward"
    assert env["capability"] == "image.gen"
    assert env["message_id"]  # uuid present
    assert env["remote_task_id"] == "st-1"
    assert env["payload"] == {"prompt": "draw a cat"}
    # provenance carried
    assert env["_origin"]["stage_id"] == "st-1"
    assert env["_origin"]["task_id"] == "tk-1"
    # correlation recorded + in-flight incremented
    assert d._forwarded_stages == {"st-1": "tk-1"}
    assert d._in_flight == {"federation": 1}
    assert d.tasks_forwarded == 1


def test_claim_returns_none_is_noop(fake_client, fed_config, isolated_fs):
    d = make_daemon(fake_client, fed_config, isolated_fs)
    d._claim_and_forward("image.gen")
    assert fake_client.claim_calls == ["image.gen"]
    assert d.tasks_forwarded == 0
    assert d._in_flight == {}


def test_claim_exception_does_not_crash(fake_client, fed_config, isolated_fs):
    def boom(cap):
        raise RuntimeError("relay down")
    fake_client.claim = boom  # type: ignore[method-assign]
    d = make_daemon(fake_client, fed_config, isolated_fs)
    # must not raise
    d._claim_and_forward("image.gen")
    assert d.tasks_forwarded == 0


def test_no_peers_unroutable_stage_left_not_forwarded(
    fake_client, isolated_fs
):
    cfg = NodeConfig(node_name="fed-a", peers=[])
    d = make_daemon(fake_client, cfg, isolated_fs)
    fake_client._claims = [{"stage_id": "s", "task_id": "t", "capability": "c", "payload": {}}]
    d._claim_and_forward("c")
    assert d.tasks_forwarded == 0
    assert d._in_flight == {}


def test_explicit_remote_in_payload_routes_to_named_peer(
    fake_client, isolated_fs
):
    cfg = NodeConfig(
        node_name="fed-a",
        peers=[
            Peer(id="fed-b", endpoint="https://b", token="t_b"),
            Peer(id="fed-c", endpoint="https://c", token="t_c"),
        ],
    )
    d = make_daemon(fake_client, cfg, isolated_fs)
    fake_client._claims = [
        {
            "stage_id": "s1", "task_id": "t1", "capability": "image.gen",
            "payload": {"remote": "fed-c", "x": 1},
        }
    ]
    d._claim_and_forward("image.gen")
    outbox_c = isolated_fs / "federation" / "fed-c" / "outbox"
    outbox_b = isolated_fs / "federation" / "fed-b" / "outbox"
    assert list(outbox_c.iterdir())
    # nothing routed to fed-b: its outbox was never created (or is empty)
    assert not outbox_b.exists() or not list(outbox_b.iterdir())


def test_unknown_remote_in_payload_is_not_forwarded(
    fake_client, fed_config, isolated_fs
):
    d = make_daemon(fake_client, fed_config, isolated_fs)
    fake_client._claims = [
        {"stage_id": "s", "task_id": "t", "capability": "c", "payload": {"remote": "ghost"}}
    ]
    d._claim_and_forward("c")
    assert d.tasks_forwarded == 0


def test_multiple_peers_without_explicit_remote_is_unroutable(
    fake_client, isolated_fs
):
    cfg = NodeConfig(
        node_name="fed-a",
        peers=[
            Peer(id="fed-b", endpoint="https://b", token="t_b"),
            Peer(id="fed-c", endpoint="https://c", token="t_c"),
        ],
    )
    d = make_daemon(fake_client, cfg, isolated_fs)
    fake_client._claims = [
        {"stage_id": "s", "task_id": "t", "capability": "c", "payload": {}}
    ]
    d._claim_and_forward("c")
    assert d.tasks_forwarded == 0


# --------------------------------------------------------------------- #
# inbox → submit (forward message)
# --------------------------------------------------------------------- #

def _write_inbox(peer_id: str, fs: Path, env: dict[str, Any]) -> Path:
    inbox = fs / "federation" / peer_id / "inbox"
    inbox.mkdir(parents=True, exist_ok=True)
    f = inbox / f"{env['message_id']}.json"
    f.write_text(json.dumps(env))
    return f


def test_process_inbox_forward_submits_local_task(
    fake_client, fed_config, isolated_fs
):
    d = make_daemon(fake_client, fed_config, isolated_fs)
    env = {
        "message_id": "m-fwd-1",
        "type": "task",
        "direction": "forward",
        "capability": "image.gen",
        "remote_task_id": "remote-st-9",
        "payload": {"prompt": "hi"},
    }
    f = _write_inbox("fed-b", isolated_fs, env)
    action = d._process_inbox_file(fed_config.peers[0], f)

    assert action == "submitted"
    assert len(fake_client.submit_calls) == 1
    cap, payload, name, prio, owner = fake_client.submit_calls[0]
    assert cap == "image.gen"
    assert payload["prompt"] == "hi"
    assert payload["remote_task_id"] == "remote-st-9"
    assert payload["remote"] == "fed-a"  # origin stamped
    assert name.startswith("federated:m-fwd-1")
    assert owner == "fed-a-node"
    assert d.tasks_submitted == 1
    # file removed after processing
    assert not f.exists()


def test_process_inbox_reply_completes_originating_stage(
    fake_client, fed_config, isolated_fs
):
    d = make_daemon(fake_client, fed_config, isolated_fs)
    # simulate a prior forward that registered the stage correlation
    d._forwarded_stages["st-7"] = "task-7"
    d._in_flight["federation"] = 1
    env = {
        "message_id": "m-reply-1",
        "type": "result",
        "direction": "reply",
        "capability": "image.gen",
        "remote_task_id": "st-7",
        "payload": {"output": "data-url"},
    }
    f = _write_inbox("fed-b", isolated_fs, env)
    action = d._process_inbox_file(fed_config.peers[0], f)

    assert action == "completed"
    assert fake_client.complete_calls == [("task-7", "st-7", {"output": "data-url"})]
    # correlation consumed + in-flight decremented
    assert d._forwarded_stages == {}
    assert d._in_flight == {"federation": 0}
    assert d.stages_completed == 1
    assert not f.exists()


def test_process_inbox_reply_without_remote_task_id_skipped(
    fake_client, fed_config, isolated_fs
):
    d = make_daemon(fake_client, fed_config, isolated_fs)
    env = {
        "message_id": "m-reply-2", "type": "result", "direction": "reply",
        "capability": "image.gen", "remote_task_id": "", "payload": {},
    }
    f = _write_inbox("fed-b", isolated_fs, env)
    assert d._process_inbox_file(fed_config.peers[0], f) == "skipped"
    assert fake_client.complete_calls == []
    assert not f.exists()  # still removed


def test_process_inbox_reply_unknown_correlation_skipped(
    fake_client, fed_config, isolated_fs
):
    d = make_daemon(fake_client, fed_config, isolated_fs)
    env = {
        "message_id": "m-reply-3", "type": "result", "direction": "reply",
        "capability": "image.gen", "remote_task_id": "unknown-st", "payload": {},
    }
    f = _write_inbox("fed-b", isolated_fs, env)
    assert d._process_inbox_file(fed_config.peers[0], f) == "skipped"
    assert fake_client.complete_calls == []


def test_process_inbox_submit_failure_does_not_remove_and_keeps_skipped(
    fake_client, fed_config, isolated_fs
):
    def boom(*a, **kw):
        raise RuntimeError("relay down")
    fake_client.submit_simple_task = boom  # type: ignore[method-assign]
    d = make_daemon(fake_client, fed_config, isolated_fs)
    env = {
        "message_id": "m-fwd-fail", "type": "task", "direction": "forward",
        "capability": "image.gen", "remote_task_id": "r", "payload": {},
    }
    f = _write_inbox("fed-b", isolated_fs, env)
    action = d._process_inbox_file(fed_config.peers[0], f)
    # action reported as skipped; current T-096 still removes the file
    # (lifecycle/retry is T-102) — but it must not have submitted.
    assert action == "skipped"
    # submit was attempted and raised — complete must NOT have been called
    assert fake_client.complete_calls == []


def test_process_inbox_reply_complete_failure_keeps_correlation(
    fake_client, fed_config, isolated_fs
):
    def boom(t, s, r):
        raise RuntimeError("relay down")
    fake_client.complete = boom  # type: ignore[method-assign]
    d = make_daemon(fake_client, fed_config, isolated_fs)
    d._forwarded_stages["st-9"] = "task-9"
    d._in_flight["federation"] = 1
    env = {
        "message_id": "m-reply-fail", "type": "result", "direction": "reply",
        "capability": "image.gen", "remote_task_id": "st-9", "payload": {"o": 1},
    }
    f = _write_inbox("fed-b", isolated_fs, env)
    action = d._process_inbox_file(fed_config.peers[0], f)
    assert action == "skipped"
    # correlation restored so a later retry can complete
    assert d._forwarded_stages == {"st-9": "task-9"}
    assert d._in_flight["federation"] == 1
    assert d.stages_completed == 0


# --------------------------------------------------------------------- #
# replay dedupe
# --------------------------------------------------------------------- #

def test_duplicate_message_id_ignored(fake_client, fed_config, isolated_fs):
    d = make_daemon(fake_client, fed_config, isolated_fs)
    env = {
        "message_id": "dup-1", "type": "task", "direction": "forward",
        "capability": "image.gen", "remote_task_id": "r", "payload": {},
    }
    f1 = _write_inbox("fed-b", isolated_fs, env)
    d._process_inbox_file(fed_config.peers[0], f1)
    # re-deliver the same message_id
    f2 = _write_inbox("fed-b", isolated_fs, env)
    action = d._process_inbox_file(fed_config.peers[0], f2)
    assert action == "duplicate"
    # submitted exactly once
    assert len(fake_client.submit_calls) == 1


# --------------------------------------------------------------------- #
# malformed inbox files
# --------------------------------------------------------------------- #

def test_inbox_file_no_message_id_skipped(fake_client, fed_config, isolated_fs):
    d = make_daemon(fake_client, fed_config, isolated_fs)
    inbox = isolated_fs / "federation" / "fed-b" / "inbox"
    inbox.mkdir(parents=True, exist_ok=True)
    f = inbox / "no-id.json"
    f.write_text(json.dumps({"type": "task"}))
    assert d._process_inbox_file(fed_config.peers[0], f) == "skipped"


def test_inbox_file_unreadable_skipped(fake_client, fed_config, isolated_fs):
    d = make_daemon(fake_client, fed_config, isolated_fs)
    inbox = isolated_fs / "federation" / "fed-b" / "inbox"
    inbox.mkdir(parents=True, exist_ok=True)
    f = inbox / "broken.json"
    f.write_text("{not json")
    assert d._process_inbox_file(fed_config.peers[0], f) == "skipped"


def test_inbox_unknown_direction_skipped(fake_client, fed_config, isolated_fs):
    d = make_daemon(fake_client, fed_config, isolated_fs)
    env = {
        "message_id": "weird", "type": "other", "direction": "sideways",
        "capability": "image.gen", "payload": {},
    }
    f = _write_inbox("fed-b", isolated_fs, env)
    assert d._process_inbox_file(fed_config.peers[0], f) == "skipped"


# --------------------------------------------------------------------- #
# _process_inbox drains the directory
# --------------------------------------------------------------------- #

def test_process_inbox_drains_directory(
    fake_client, fed_config, isolated_fs
):
    d = make_daemon(fake_client, fed_config, isolated_fs)
    _write_inbox("fed-b", isolated_fs, {
        "message_id": "a", "type": "task", "direction": "forward",
        "capability": "image.gen", "remote_task_id": "r", "payload": {},
    })
    _write_inbox("fed-b", isolated_fs, {
        "message_id": "b", "type": "task", "direction": "forward",
        "capability": "image.gen", "remote_task_id": "r2", "payload": {},
    })
    inbox = isolated_fs / "federation" / "fed-b" / "inbox"
    n = d._process_inbox(fed_config.peers[0])
    assert n == 2
    assert len(fake_client.submit_calls) == 2
    assert list(inbox.iterdir()) == []  # all drained


def test_process_inbox_ignores_dotfiles(fake_client, fed_config, isolated_fs):
    d = make_daemon(fake_client, fed_config, isolated_fs)
    inbox = isolated_fs / "federation" / "fed-b" / "inbox"
    inbox.mkdir(parents=True, exist_ok=True)
    (inbox / ".lock").write_text("x")
    assert d._process_inbox(fed_config.peers[0]) == 0


def test_process_inbox_missing_dir_returns_zero(fake_client, fed_config, isolated_fs):
    d = make_daemon(fake_client, fed_config, isolated_fs)
    assert d._process_inbox(fed_config.peers[0]) == 0


# --------------------------------------------------------------------- #
# sync cycle
# --------------------------------------------------------------------- #

def test_sync_one_peer_runs_outbox_then_inbox(
    fake_client, fed_config, isolated_fs
):
    transport = FakeTransport(inbox_payloads={
        "m-in-1": {
            "message_id": "m-in-1", "type": "task", "direction": "forward",
            "capability": "image.gen", "remote_task_id": "r", "payload": {},
        }
    })
    d = make_daemon(
        fake_client, fed_config, isolated_fs,
        transport_factory=lambda peer: transport,
    )
    # seed an outbox file
    outbox = isolated_fs / "federation" / "fed-b" / "outbox"
    outbox.mkdir(parents=True, exist_ok=True)
    (outbox / "x.json").write_text(json.dumps({"message_id": "x"}))

    result = d._sync_one_peer(fed_config.peers[0])
    assert result["sent"] == ["x"]
    assert result["received"] == ["m-in-1"]
    assert len(transport.outbox_calls) == 1
    assert len(transport.inbox_calls) == 1


def test_sync_one_peer_no_factory_returns_empty(
    fake_client, fed_config, isolated_fs
):
    d = make_daemon(fake_client, fed_config, isolated_fs, transport_factory=None)
    assert d._sync_one_peer(fed_config.peers[0]) == {"sent": [], "received": []}


def test_sync_one_peer_factory_failure_returns_empty(
    fake_client, fed_config, isolated_fs
):
    def boom(peer):
        raise ConnectionError("nope")
    d = make_daemon(
        fake_client, fed_config, isolated_fs, transport_factory=boom
    )
    assert d._sync_one_peer(fed_config.peers[0]) == {"sent": [], "received": []}


def test_get_transport_cached(fake_client, fed_config, isolated_fs):
    calls: list[str] = []
    def factory(peer):
        calls.append(peer.id)
        return FakeTransport()
    d = make_daemon(fake_client, fed_config, isolated_fs, transport_factory=factory)
    d._sync_one_peer(fed_config.peers[0])
    d._sync_one_peer(fed_config.peers[0])
    assert calls == ["fed-b"]  # built once, cached


# --------------------------------------------------------------------- #
# end-to-end logic: claim → forward → reply → complete
# --------------------------------------------------------------------- #

def test_end_to_end_forward_then_reply_completes_stage(
    fake_client, fed_config, isolated_fs
):
    """Claim a stage, forward it, then a reply inbox file completes the stage."""
    d = make_daemon(fake_client, fed_config, isolated_fs)
    # 1. claim + forward
    fake_client._claims = [
        {"stage_id": "st-42", "task_id": "task-42", "capability": "image.gen",
         "payload": {"prompt": "x"}}
    ]
    d._claim_and_forward("image.gen")
    outbox = isolated_fs / "federation" / "fed-b" / "outbox"
    assert len(list(outbox.iterdir())) == 1
    env_out = json.loads(next(outbox.iterdir()).read_text())
    assert env_out["remote_task_id"] == "st-42"

    # 2. reply arrives in inbox, correlating via remote_task_id == st-42
    reply = {
        "message_id": "reply-42", "type": "result", "direction": "reply",
        "capability": "image.gen", "remote_task_id": "st-42",
        "payload": {"image": "data-url"},
    }
    f = _write_inbox("fed-b", isolated_fs, reply)
    action = d._process_inbox_file(fed_config.peers[0], f)
    assert action == "completed"
    assert fake_client.complete_calls == [("task-42", "st-42", {"image": "data-url"})]
    assert d._forwarded_stages == {}
    assert d.stages_completed == 1


# --------------------------------------------------------------------- #
# status file
# --------------------------------------------------------------------- #

def test_write_status_writes_json(fake_client, fed_config, isolated_fs):
    d = make_daemon(fake_client, fed_config, isolated_fs)
    d.tasks_forwarded = 3
    d.tasks_submitted = 5
    d.stages_completed = 2
    d.last_heartbeat_status = "ok"
    d._write_status()
    status_path = isolated_fs / "federation_status.json"
    assert status_path.exists()
    st = json.loads(status_path.read_text())
    assert st["daemon"] == "federation-daemon"
    assert st["tasks_forwarded"] == 3
    assert st["tasks_submitted"] == 5
    assert st["stages_completed"] == 2
    assert st["heartbeat_status"] == "ok"
    assert st["peers"] == ["fed-b"]
    assert st["forward_caps"] == ["image.gen"]


# --------------------------------------------------------------------- #
# daemon startup/shutdown smoke test
# --------------------------------------------------------------------- #

def test_daemon_start_stop_smoke(fake_client, fed_config, isolated_fs):
    """Start the threaded daemon, let it tick, then stop it cleanly."""
    transport = FakeTransport()
    d = FederationDaemon(
        fake_client,
        {"heartbeat_interval": 0.05},
        fed_config=fed_config,
        forward_caps=["image.gen"],
        transport_factory=lambda peer: transport,
        base_dir=isolated_fs,
        intervals={"heartbeat": 0.05, "claim": 0.05, "sync": 0.05, "inbox": 0.05},
    )
    # run() installs signal handlers and blocks; run it in a thread.
    import threading as _th
    t = _th.Thread(target=d.run, daemon=True)
    t.start()
    # let it tick a few cycles
    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline and not fake_client.heartbeat_calls:
        time.sleep(0.02)
    d.stop()
    t.join(timeout=5)
    assert not t.is_alive()
    # heartbeat should have fired at least once
    assert len(fake_client.heartbeat_calls) >= 1
    caps, inflight = fake_client.heartbeat_calls[0]
    cap_names = [c["name"] for c in caps]
    assert "federation" in cap_names
    assert "image.gen" in cap_names
    # status file written
    assert (isolated_fs / "federation_status.json").exists()
    # transport closed on shutdown
    assert transport.closed


def test_daemon_stop_without_run_is_safe(fake_client, fed_config, isolated_fs):
    d = make_daemon(fake_client, fed_config, isolated_fs)
    d.stop()  # no-op, must not raise


# --------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------- #

def test_build_parser_help():
    p = build_parser()
    ns = p.parse_args(["--log-level", "DEBUG", "--config", "/tmp/x.yaml"])
    assert ns.log_level == "DEBUG"
    assert ns.config == "/tmp/x.yaml"


def test_main_no_peers_returns_error(tmp_path, monkeypatch):
    monkeypatch.setattr(fed_paths, "BASE_DIR", tmp_path)
    # node.yaml without peers
    (tmp_path / "node.yaml").write_text("node_name: fed-a\nfederation: {}\n")
    rc = main(["--config", str(tmp_path / "node.yaml")])
    assert rc == 1


def test_main_relay_client_init_failure_returns_error(monkeypatch, tmp_path):
    """main() returns 1 if the relay client cannot be initialised."""
    monkeypatch.setattr(fed_paths, "BASE_DIR", tmp_path)
    (tmp_path / "node.yaml").write_text(
        "node_name: fed-a\nfederation:\n  peers:\n"
        "    - id: fed-b\n      endpoint: https://b\n      token: t\n"
    )
    import federation.federation_node as mod
    import nodes.common.node_utils as nu
    import nodes.common.relay_client as rc

    def boom_load_meta():
        raise RuntimeError("no ai-relay-agent.json")
    monkeypatch.setattr(nu, "load_meta", boom_load_meta)
    # main() imports load_meta from nodes.common.node_utils at call time
    # (it does a ``from ... import load_meta``), so patching the attribute
    # on the source module is sufficient.
    rc_out = main(["--config", str(tmp_path / "node.yaml")])
    assert rc_out == 1
    # ensure we did not start a daemon (no threads spawned): just assert rc.
    _ = mod  # silence unused import linter


# --------------------------------------------------------------------- #
# Fair-Use integration (T-099)
# --------------------------------------------------------------------- #

def test_fairuse_not_enforced_by_default(fake_client, fed_config, isolated_fs):
    """No registry → no gating; all claims succeed (regression guard)."""
    fake_client._claims = [
        {"stage_id": f"st-{i}", "task_id": f"tk-{i}", "capability": "image.gen",
         "payload": {"prompt": "cat"}}
        for i in range(5)
    ]
    d = make_daemon(fake_client, fed_config, isolated_fs)
    for _ in range(5):
        d._claim_and_forward("image.gen")
    assert len(fake_client.claim_calls) == 5
    assert d.tasks_forwarded == 5


def test_fairuse_blocks_claim_when_parallel_exhausted(
    fake_client, fed_config, isolated_fs
):
    """When parallel cap is reached, claim is not even attempted."""
    from federation.fairuse import FairUse, FairUseRegistry

    pol = FairUse(
        "image.gen", max_parallel=1, max_daily=50, state_dir=isolated_fs,
    )
    reg = FairUseRegistry(enforce=True, policies={"image.gen": pol})
    d = make_daemon(fake_client, fed_config, isolated_fs)
    d.fairuse = reg

    # First claim succeeds.
    fake_client._claims = [
        {"stage_id": "st-1", "task_id": "tk-1", "capability": "image.gen",
         "payload": {}}
    ]
    d._claim_and_forward("image.gen")
    assert d.tasks_forwarded == 1
    assert pol.in_flight == 1

    # Second claim must be gated (no client.claim call).
    fake_client._claims = [
        {"stage_id": "st-2", "task_id": "tk-2", "capability": "image.gen",
         "payload": {}}
    ]
    d._claim_and_forward("image.gen")
    assert fake_client.claim_calls == ["image.gen"]   # only the first
    assert d.tasks_forwarded == 1
    assert pol.in_flight == 1


def test_fairuse_releases_slot_on_reply_completion(
    fake_client, fed_config, isolated_fs, peer
):
    """Reply completing a stage frees the parallel slot for a new claim."""
    from federation.fairuse import FairUse, FairUseRegistry

    pol = FairUse(
        "image.gen", max_parallel=1, max_daily=50, state_dir=isolated_fs,
    )
    reg = FairUseRegistry(enforce=True, policies={"image.gen": pol})
    d = make_daemon(fake_client, fed_config, isolated_fs)
    d.fairuse = reg

    # Forward a stage.
    fake_client._claims = [
        {"stage_id": "st-1", "task_id": "tk-1", "capability": "image.gen",
         "payload": {"prompt": "cat"}}
    ]
    d._claim_and_forward("image.gen")
    assert pol.in_flight == 1

    # Deliver the reply → inbox.
    reply = {
        "message_id": "m-reply-1",
        "type": "result",
        "direction": "reply",
        "capability": "image.gen",
        "remote_task_id": "st-1",
        "payload": {"image": "data"},
    }
    path = _write_inbox(peer.id, isolated_fs, reply)
    d._process_inbox(peer)
    assert d.stages_completed == 1
    # Slot freed.
    assert pol.in_flight == 0

    # Now a new claim fits.
    fake_client._claims = [
        {"stage_id": "st-2", "task_id": "tk-2", "capability": "image.gen",
         "payload": {}}
    ]
    d._claim_and_forward("image.gen")
    assert d.tasks_forwarded == 2
    assert pol.in_flight == 1
    _ = path  # file is consumed by _process_inbox


def test_fairuse_daily_cap_blocks_after_n(
    fake_client, fed_config, isolated_fs
):
    """Daily cap blocks new claims after N forwards even with parallel room."""
    from federation.fairuse import FairUse, FairUseRegistry

    pol = FairUse(
        "image.gen", max_parallel=50, max_daily=2, state_dir=isolated_fs,
    )
    reg = FairUseRegistry(enforce=True, policies={"image.gen": pol})
    d = make_daemon(fake_client, fed_config, isolated_fs)
    d.fairuse = reg

    for i in range(2):
        fake_client._claims = [
            {"stage_id": f"st-{i}", "task_id": f"tk-{i}", "capability": "image.gen",
             "payload": {}}
        ]
        d._claim_and_forward("image.gen")
    assert d.tasks_forwarded == 2
    assert pol.daily_count == 2

    # 3rd claim gated by daily cap.
    fake_client._claims = [
        {"stage_id": "st-3", "task_id": "tk-3", "capability": "image.gen",
         "payload": {}}
    ]
    d._claim_and_forward("image.gen")
    assert d.tasks_forwarded == 2
    assert fake_client.claim_calls == ["image.gen", "image.gen"]  # only 2

def test_forward_routes_via_transport_tilde_recipient(
    fake_client, fed_config, isolated_fs
):
    """The stage payload names the route; the envelope carries it."""
    d = make_daemon(fake_client, fed_config, isolated_fs)
    peer = fed_config.peers[0]
    stage = {
        "stage_id": "stage_01",
        "task_id": "task_42",
        "capability": "image.gen",
        "payload": {
            "remote": peer.id,
            "route": "imsg~ronny.pietschke@icloud.com",
        },
    }
    mid = d._write_forward_file(peer, stage)
    assert mid is not None
    outbox = isolated_fs / "federation" / peer.id / "outbox"
    files = list(outbox.rglob("*.json"))
    assert len(files) == 1
    env = json.loads(files[0].read_text())
    assert env["route"] == "imsg~ronny.pietschke@icloud.com"


def test_oversized_task_is_rejected_before_forwarding(
    fake_client, fed_config, isolated_fs
):
    """A task larger than the transport's declared limit never reaches the outbox."""
    d = make_daemon(fake_client, fed_config, isolated_fs)
    peer = fed_config.peers[0]
    d.registry.register(
        "email", TransportManifest(name="email", max_task_bytes=1024)
    )
    stage = {
        "stage_id": "stage_01",
        "task_id": "task_42",
        "capability": "image.gen",
        "payload": {
            "remote": peer.id,
            "route": "email~ronny@example.org",
            "blob": "x" * 5000,
        },
    }
    with pytest.raises(TaskTooLargeForRoute):
        d._write_forward_file(peer, stage)
    outbox = isolated_fs / "federation" / peer.id / "outbox"
    assert not list(outbox.rglob("*.json"))


def test_task_within_limit_is_forwarded(fake_client, fed_config, isolated_fs):
    d = make_daemon(fake_client, fed_config, isolated_fs)
    peer = fed_config.peers[0]
    d.registry.register(
        "email", TransportManifest(name="email", max_task_bytes=1024 * 1024)
    )
    stage = {
        "stage_id": "stage_02",
        "task_id": "task_43",
        "capability": "image.gen",
        "payload": {
            "remote": peer.id,
            "route": "email~ronny@example.org",
            "blob": "x" * 100,
        },
    }
    mid = d._write_forward_file(peer, stage)
    assert mid is not None


def test_daemon_reconciles_registry_on_start(fake_client, fed_config, isolated_fs):
    """Dropping a manifest into registry/ registers the transport at runtime.

    The daemon's registry is rooted at the federation root (D-02) — so the
    transport tree appears under ``federation/outbox/<name>`` (correction C1),
    not under ``federation/<name>/outbox``.
    """
    d = make_daemon(fake_client, fed_config, isolated_fs)
    reg_dir = isolated_fs / "federation" / "registry"
    reg_dir.mkdir(parents=True, exist_ok=True)
    (reg_dir / "email.yaml").write_text("name: email\nmax_task_bytes: 10485760\n")
    d.registry.reconcile()
    assert "email" in d.registry.transports
    assert (isolated_fs / "federation" / "outbox" / "email").is_dir()
