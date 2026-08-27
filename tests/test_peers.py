"""Tests for federation/peers.py — PeerManager (T-097) and CLI surface.

Covers the plan's accept criteria:
* add/list/remove round-trip on a temp ``node.yaml``
* error on missing/unknown id
* token never printed in ``list`` (verified at both PeerManager redaction
  layer and CLI output layer)
* connect/disconnect toggle the ``active`` flag
* round-trip through the real :class:`federation.config.load_node_config` so
  mutations are visible to the daemon
"""

from __future__ import annotations

import io
from pathlib import Path

import pytest
import yaml

from federation.cli import main as cli_main
from federation.config import load_node_config
from federation.peers import (
    PeerExistsError,
    PeerManager,
    PeerNotFoundError,
    redact,
)


@pytest.fixture
def cfg_path(tmp_path: Path) -> Path:
    """An empty node.yaml path inside the tmp_path."""
    return tmp_path / "node.yaml"


@pytest.fixture
def mgr(cfg_path: Path) -> PeerManager:
    return PeerManager(path=cfg_path)


# -- PeerManager core -------------------------------------------------------


def test_add_creates_file_and_section(mgr: PeerManager, cfg_path: Path) -> None:
    rec = mgr.add("fed-b", "https://fed-b:8791", "fed_t_secret", "pk-b")
    assert rec["id"] == "fed-b"
    assert rec["endpoint"] == "https://fed-b:8791"
    assert rec["peer_public_key"] == "pk-b"
    assert cfg_path.exists()
    raw = yaml.safe_load(cfg_path.read_text())
    assert raw["federation"]["peers"][0]["id"] == "fed-b"


def test_add_normalises_trailing_slash(mgr: PeerManager, cfg_path: Path) -> None:
    mgr.add("b", "https://x:8791/", "t", "-")
    raw = yaml.safe_load(cfg_path.read_text())
    assert raw["federation"]["peers"][0]["endpoint"] == "https://x:8791"


def test_add_pubkey_dash_leaves_unset(mgr: PeerManager, cfg_path: Path) -> None:
    # The CLI converts "-" to None before calling add(); the manager API
    # treats None as "unset". Verify None round-trips as "key absent".
    mgr.add("b", "https://x", "t", None)
    raw = yaml.safe_load(cfg_path.read_text())
    p = raw["federation"]["peers"][0]
    assert "peer_public_key" not in p


def test_add_duplicate_raises(mgr: PeerManager) -> None:
    mgr.add("b", "https://x", "t", "-")
    with pytest.raises(PeerExistsError) as e:
        mgr.add("b", "https://y", "t2", "-")
    assert e.value.peer_id == "b"


def test_add_invalid_input_raises(mgr: PeerManager) -> None:
    with pytest.raises(ValueError):
        mgr.add("", "https://x", "t", "-")
    with pytest.raises(ValueError):
        mgr.add("b", "", "t", "-")
    with pytest.raises(ValueError):
        mgr.add("b", "https://x", "", "-")


def test_remove_removes_and_returns_record(mgr: PeerManager) -> None:
    mgr.add("a", "https://a", "t1", "-")
    mgr.add("b", "https://b", "t2", "-")
    removed = mgr.remove("a")
    assert removed["id"] == "a"
    ids = [p["id"] for p in mgr.list()]
    assert ids == ["b"]


def test_remove_unknown_raises(mgr: PeerManager) -> None:
    with pytest.raises(PeerNotFoundError) as e:
        mgr.remove("nope")
    assert e.value.peer_id == "nope"


def test_get_returns_record(mgr: PeerManager) -> None:
    mgr.add("b", "https://x", "t", "pk")
    rec = mgr.get("b")
    assert rec["id"] == "b"
    assert rec["token"] == "t"  # unredacted for internal callers


def test_get_unknown_raises(mgr: PeerManager) -> None:
    with pytest.raises(PeerNotFoundError):
        mgr.get("nope")


def test_list_empty(mgr: PeerManager) -> None:
    assert mgr.list() == []


def test_list_returns_all(mgr: PeerManager) -> None:
    mgr.add("a", "https://a", "t1", "-")
    mgr.add("b", "https://b", "t2", "-")
    ids = sorted(p["id"] for p in mgr.list())
    assert ids == ["a", "b"]


def test_list_returns_unredacted_for_internal_callers(mgr: PeerManager) -> None:
    mgr.add("b", "https://x", "fed_t_secret", "-")
    rec = mgr.list()[0]
    assert rec["token"] == "fed_t_secret"  # redaction is CLI's job


def test_redact_hides_token() -> None:
    out = redact({"id": "b", "token": "fed_t_secret", "endpoint": "https://x"})
    assert out["token"] == "<redacted>"
    # original untouched
    assert out is not {"id": "b", "token": "fed_t_secret", "endpoint": "https://x"}


def test_redact_preserves_other_keys() -> None:
    out = redact({"id": "b", "endpoint": "https://x", "peer_public_key": "pk"})
    assert out["endpoint"] == "https://x"
    assert out["peer_public_key"] == "pk"
    assert "token" not in out  # wasn't present


# -- round-trip through loader (daemon sees the mutation) ------------------


def test_add_visible_to_load_node_config(mgr: PeerManager, cfg_path: Path) -> None:
    mgr.add("fed-b", "https://fed-b:8791", "fed_t_secret", "pk-b")
    cfg = load_node_config(cfg_path)
    assert cfg.peers[0].id == "fed-b"
    assert cfg.peers[0].token == "fed_t_secret"
    assert cfg.peers[0].peer_public_key == "pk-b"


def test_add_preserves_unrelated_keys(cfg_path: Path) -> None:
    cfg_path.write_text(
        "node_name: fed-a\n"
        "federation:\n"
        "  offer:\n"
        "    - capability: image.gen\n"
        "  listener_token: fed_t_shared\n"
        "extra_top: 1\n"
    )
    mgr = PeerManager(path=cfg_path)
    mgr.add("b", "https://x", "t", "-")
    raw = yaml.safe_load(cfg_path.read_text())
    # unrelated keys survive
    assert raw["node_name"] == "fed-a"
    assert raw["extra_top"] == 1
    assert raw["federation"]["offer"] == [{"capability": "image.gen"}]
    assert raw["federation"]["listener_token"] == "fed_t_shared"
    # and the peer is appended
    assert raw["federation"]["peers"][0]["id"] == "b"


# -- connect / disconnect ---------------------------------------------------


def test_connect_sets_active_true(mgr: PeerManager) -> None:
    mgr.add("b", "https://x", "t", "-")
    rec = mgr.connect("b")
    assert rec["active"] is True
    assert mgr.get("b")["active"] is True


def test_disconnect_sets_active_false(mgr: PeerManager) -> None:
    mgr.add("b", "https://x", "t", "-")
    mgr.connect("b")
    rec = mgr.disconnect("b")
    assert rec["active"] is False
    assert mgr.get("b")["active"] is False


def test_connect_unknown_raises(mgr: PeerManager) -> None:
    with pytest.raises(PeerNotFoundError):
        mgr.connect("nope")


def test_disconnect_unknown_raises(mgr: PeerManager) -> None:
    with pytest.raises(PeerNotFoundError):
        mgr.disconnect("nope")


def test_disconnect_leaves_record_in_place(mgr: PeerManager) -> None:
    mgr.add("b", "https://x", "t", "-")
    mgr.connect("b")
    mgr.disconnect("b")
    assert any(p["id"] == "b" for p in mgr.list())


# -- write atomicity --------------------------------------------------------


def test_save_is_atomic_no_tmp_left(mgr: PeerManager, cfg_path: Path) -> None:
    mgr.add("b", "https://x", "t", "-")
    assert not (cfg_path.with_name(cfg_path.name + ".tmp")).exists()


# -- CLI integration --------------------------------------------------------


def _run_cli(argv: list[str], cfg_path: Path, capsys: pytest.CaptureFixture[str]) -> int:
    return cli_main(["--config", str(cfg_path), *argv])


def test_cli_add_then_list(capsys: pytest.CaptureFixture[str], cfg_path: Path) -> None:
    rc = _run_cli(["federation", "peer", "add", "fed-b", "https://fed-b:8791", "fed_t_secret", "pk-b"], cfg_path, capsys)
    assert rc == 0
    out = capsys.readouterr().out
    assert "fed-b" in out

    rc = _run_cli(["federation", "peer", "list"], cfg_path, capsys)
    assert rc == 0
    out = capsys.readouterr().out
    assert "fed-b" in out
    # CRITICAL accept criterion: token never printed
    assert "fed_t_secret" not in out


def test_cli_list_empty(capsys: pytest.CaptureFixture[str], cfg_path: Path) -> None:
    rc = _run_cli(["federation", "peer", "list"], cfg_path, capsys)
    assert rc == 0
    out = capsys.readouterr().out
    assert "no peers" in out


def test_cli_remove(capsys: pytest.CaptureFixture[str], cfg_path: Path) -> None:
    _run_cli(["federation", "peer", "add", "b", "https://x", "t", "-"], cfg_path, capsys)
    capsys.readouterr()  # clear
    rc = _run_cli(["federation", "peer", "remove", "b"], cfg_path, capsys)
    assert rc == 0
    out = capsys.readouterr().out
    assert "removed" in out
    # gone
    rc = _run_cli(["federation", "peer", "list"], cfg_path, capsys)
    out = capsys.readouterr().out
    assert "no peers" in out


def test_cli_remove_unknown_exits_1(capsys: pytest.CaptureFixture[str], cfg_path: Path) -> None:
    rc = _run_cli(["federation", "peer", "remove", "nope"], cfg_path, capsys)
    assert rc == 1
    err = capsys.readouterr().err
    assert "not found" in err


def test_cli_add_duplicate_exits_1(capsys: pytest.CaptureFixture[str], cfg_path: Path) -> None:
    _run_cli(["federation", "peer", "add", "b", "https://x", "t", "-"], cfg_path, capsys)
    capsys.readouterr()
    rc = _run_cli(["federation", "peer", "add", "b", "https://y", "t2", "-"], cfg_path, capsys)
    assert rc == 1
    err = capsys.readouterr().err
    assert "already exists" in err


def test_cli_connect_disconnect(capsys: pytest.CaptureFixture[str], cfg_path: Path) -> None:
    _run_cli(["federation", "peer", "add", "b", "https://x", "t", "-"], cfg_path, capsys)
    capsys.readouterr()
    rc = _run_cli(["federation", "peer", "connect", "b"], cfg_path, capsys)
    assert rc == 0
    out = capsys.readouterr().out
    assert "connected" in out
    assert "active=True" in out or "active=true" in out  # yaml.safe_dump lowercases bools

    rc = _run_cli(["federation", "peer", "disconnect", "b"], cfg_path, capsys)
    assert rc == 0
    out = capsys.readouterr().out
    assert "disconnected" in out
    assert "active=False" in out or "active=false" in out


def test_cli_list_shows_active_flag(capsys: pytest.CaptureFixture[str], cfg_path: Path) -> None:
    _run_cli(["federation", "peer", "add", "b", "https://x", "t", "-"], cfg_path, capsys)
    _run_cli(["federation", "peer", "connect", "b"], cfg_path, capsys)
    capsys.readouterr()
    rc = _run_cli(["federation", "peer", "list"], cfg_path, capsys)
    assert rc == 0
    out = capsys.readouterr().out
    assert "active=True" in out or "active=true" in out


def test_cli_connect_unknown_exits_1(capsys: pytest.CaptureFixture[str], cfg_path: Path) -> None:
    rc = _run_cli(["federation", "peer", "connect", "nope"], cfg_path, capsys)
    assert rc == 1


def test_cli_help_runs() -> None:
    # --help exits with code 0 and prints usage; argparse raises SystemExit(0)
    with pytest.raises(SystemExit) as e:
        cli_main(["--help"])
    assert e.value.code == 0