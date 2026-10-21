"""Tests for federation/routing.py — `<transport>~<recipient>` routes (T-109)."""

from __future__ import annotations

import pytest

from federation.routing import Route, RouteError, parse_route


def test_parse_route_splits_transport_and_recipient():
    r = parse_route("imsg~ronny.pietschke@icloud.com")
    assert r.transport == "imsg"
    assert r.recipient == "ronny.pietschke@icloud.com"


def test_parse_route_rejects_missing_separator():
    with pytest.raises(RouteError, match="separator"):
        parse_route("ronny.pietschke@icloud.com")


def test_route_str_roundtrip():
    r = Route(transport="nextcloud", recipient="peter")
    assert str(r) == "nextcloud~peter"
    assert parse_route(str(r)) == r


def test_route_recipient_may_not_contain_separator():
    with pytest.raises(RouteError):
        Route(transport="email", recipient="a~b")


def test_route_transport_must_be_identifier_like():
    # transport names become directory names -> no path separators, no traversal
    for bad in ("../evil", "a/b", "", " "):
        with pytest.raises(RouteError):
            Route(transport=bad, recipient="p")