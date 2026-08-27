"""Tests for federation/config.py — peer + node config loading (T-095)."""

from __future__ import annotations

from pathlib import Path

import pytest

from federation.config import NodeConfig, Peer, load_node_config, load_node_config_from_dict


def test_peer_normalises_trailing_slash():
    p = Peer(id="fed-b", endpoint="https://fed-b:8791/", token="t")
    assert p.endpoint == "https://fed-b:8791"


def test_peer_requires_id_endpoint_token():
    with pytest.raises(ValueError, match="id"):
        Peer(id="", endpoint="https://x", token="t")
    with pytest.raises(ValueError, match="endpoint"):
        Peer(id="x", endpoint="", token="t")
    with pytest.raises(ValueError, match="token"):
        Peer(id="x", endpoint="https://x", token="")


def test_peer_auth_header():
    p = Peer(id="b", endpoint="https://x", token="fed_t_xyz")
    assert p.auth_header == {"Authorization": "Bearer fed_t_xyz"}


def test_load_from_dict_full():
    raw = {
        "node_name": "fed-a",
        "federation": {
            "peers": [
                {
                    "id": "fed-b",
                    "endpoint": "https://fed-b:8791",
                    "token": "fed_t_secret",
                    "peer_public_key": "pk",
                    "my_public_key": "mpk",
                }
            ],
            "offer": [{"capability": "image.gen"}],
            "listener_token": "fed_t_shared",
        },
    }
    cfg = load_node_config_from_dict(raw)
    assert cfg.node_name == "fed-a"
    assert len(cfg.peers) == 1
    assert cfg.peers[0].peer_public_key == "pk"
    assert cfg.peers[0].my_public_key == "mpk"
    assert cfg.offer == [{"capability": "image.gen"}]
    assert cfg.listener_token == "fed_t_shared"


def test_load_from_dict_defaults():
    cfg = load_node_config_from_dict({})
    assert cfg.node_name == "federation"
    assert cfg.peers == []
    assert cfg.offer == []
    assert cfg.listener_token is None


def test_load_from_dict_invalid_peer_raises():
    # Missing 'token' → KeyError from the dict access; missing endpoint/value
    # → ValueError from Peer validation. Either is an acceptable hard fail.
    with pytest.raises((KeyError, ValueError)):
        load_node_config_from_dict({"federation": {"peers": [{"id": "x", "endpoint": "https://x"}]}})


def test_node_config_peer_lookup():
    cfg = NodeConfig(peers=[Peer(id="fed-b", endpoint="https://x", token="t")])
    assert cfg.peer("fed-b").endpoint == "https://x"
    with pytest.raises(KeyError):
        cfg.peer("nope")


def test_accepted_tokens_combines_peers_and_override():
    cfg = NodeConfig(
        peers=[Peer(id="a", endpoint="https://x", token="t1")],
        listener_token="t2",
    )
    assert cfg.accepted_tokens() == {"t1", "t2"}


def test_load_node_config_missing_file_returns_empty(tmp_path, monkeypatch):
    # point at a non-existent file in tmp_path
    monkeypatch.setattr("federation.paths.BASE_DIR", tmp_path)
    cfg = load_node_config()
    assert cfg.peers == []
    assert cfg.node_name == "federation"


def test_load_node_config_reads_yaml(tmp_path, monkeypatch):
    (tmp_path / "node.yaml").write_text(
        """
node_name: fed-a
federation:
  peers:
    - id: fed-b
      endpoint: https://fed-b:8791
      token: fed_t_secret
  offer:
    - capability: image.gen
"""
    )
    monkeypatch.setattr("federation.paths.BASE_DIR", tmp_path)
    cfg = load_node_config()
    assert cfg.node_name == "fed-a"
    assert cfg.peers[0].id == "fed-b"
    assert cfg.peers[0].token == "fed_t_secret"
    assert cfg.offer == [{"capability": "image.gen"}]