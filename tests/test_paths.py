from pathlib import Path
from federation.paths import (ensure_federation_dirs, federation_root,
                              registry_dir, remote_dirs, transport_dirs)


def test_federation_root(tmp_path, monkeypatch):
    monkeypatch.setattr("federation.paths.BASE_DIR", tmp_path)
    assert federation_root() == tmp_path / "federation"


def test_remote_dirs(tmp_path, monkeypatch):
    monkeypatch.setattr("federation.paths.BASE_DIR", tmp_path)
    inbox, outbox = remote_dirs("fed-b")
    assert inbox == tmp_path / "federation" / "fed-b" / "inbox"
    assert outbox == tmp_path / "federation" / "fed-b" / "outbox"


def test_ensure_federation_dirs_creates(tmp_path, monkeypatch):
    monkeypatch.setattr("federation.paths.BASE_DIR", tmp_path)
    inbox, outbox = ensure_federation_dirs("fed-b")
    assert inbox.is_dir()
    assert outbox.is_dir()


import pytest


@pytest.fixture
def isolated_fs(tmp_path, monkeypatch):
    monkeypatch.setattr("federation.paths.BASE_DIR", tmp_path)
    return tmp_path


def test_registry_dir_is_under_federation_root(isolated_fs):
    assert registry_dir() == isolated_fs / "federation" / "registry"


def test_transport_dirs_live_under_federation_root(isolated_fs):
    inbox, outbox, status, media = transport_dirs("email")
    assert inbox == isolated_fs / "federation" / "inbox" / "email"
    assert outbox == isolated_fs / "federation" / "outbox" / "email"
    assert status == isolated_fs / "federation" / "status" / "email"
    assert media == isolated_fs / "federation" / "media" / "email"