"""Tests for federation/manifest.py — transport manifests (T-109)."""

from __future__ import annotations

import pytest

from federation.manifest import (
    ManifestError,
    TransportManifest,
    load_manifest,
    scan_registry,
)


def test_manifest_parses_max_task_bytes():
    m = load_manifest(
        {
            "name": "email",
            "service": "email",
            "target_format": ["email"],
            "max_task_bytes": 10485760,
        }
    )
    assert m.name == "email"
    assert m.max_task_bytes == 10485760
    assert m.target_format == ("email",)


def test_manifest_max_task_bytes_defaults_to_none():
    m = load_manifest({"name": "email"})
    assert m.max_task_bytes is None
    assert m.service == "email"  # defaults to the name


def test_manifest_rejects_negative_max_task_bytes():
    with pytest.raises(ManifestError, match="max_task_bytes"):
        load_manifest({"name": "email", "max_task_bytes": -1})


def test_manifest_rejects_path_traversal_in_name():
    # names become directory names -> no traversal, no separators
    for bad in ("../evil", "a/b", ""):
        with pytest.raises(ManifestError):
            load_manifest({"name": bad})


def test_scan_registry_reads_manifests(tmp_path):
    reg = tmp_path / "registry"
    reg.mkdir()
    (reg / "email.yaml").write_text("name: email\nmax_task_bytes: 10485760\n")
    (reg / "broken.yaml").write_text("name: '../evil'\n")
    found = scan_registry(reg)
    assert "email" in found
    assert found["email"].max_task_bytes == 10485760
    assert "broken" not in found  # bad manifest is skipped, not fatal