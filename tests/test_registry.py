"""Tests for the TransportRegistry (T-109).

Phase 2 (scaffold): these tests are RED on purpose — the methods raise
NotImplementedError. Phase 3 implements the registry behind them; the test
content must not change.
"""

from federation.registry import TransportRegistry


def test_registry_creates_directory_tree_on_register(tmp_path):
    reg = TransportRegistry(tmp_path)
    reg.register("email")
    for sub in ("inbox/email", "outbox/email", "status/email",
                "media/email/incoming", "media/email/outgoing"):
        assert (tmp_path / sub).is_dir(), sub


def test_registry_unregister_keeps_inbox_and_outbox(tmp_path):
    reg = TransportRegistry(tmp_path)
    reg.register("email")
    reg.unregister("email")
    # In-flight files may still need draining -> inbox/outbox stay.
    assert (tmp_path / "inbox" / "email").is_dir()
    assert (tmp_path / "outbox" / "email").is_dir()
    # status/media are recreated on re-registration -> removed.
    assert not (tmp_path / "status" / "email").exists()


def test_registry_reconcile_registers_and_deregisters(tmp_path):
    reg = TransportRegistry(tmp_path)
    (tmp_path / "registry").mkdir()
    (tmp_path / "registry" / "email.yaml").write_text("name: email\n")
    reg.reconcile()
    assert "email" in reg.transports
    (tmp_path / "registry" / "email.yaml").unlink()
    reg.reconcile()
    assert "email" not in reg.transports