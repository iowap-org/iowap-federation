from pathlib import Path
from federation.paths import federation_root, remote_dirs, ensure_federation_dirs


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