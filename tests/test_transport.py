import pytest
from federation.transport import Transport, create_transport
from federation.transports.http_transport import HttpTransport


def test_transport_is_abstract():
    with pytest.raises(TypeError):
        Transport()  # abstract — cannot instantiate


def test_transport_methods_raise_not_implemented():
    class Concrete(Transport):
        pass

    t = Concrete()
    with pytest.raises(NotImplementedError):
        t.connect("https://x", "tok")
    with pytest.raises(NotImplementedError):
        t.sync_outbox(None)
    with pytest.raises(NotImplementedError):
        t.sync_inbox(None)
    with pytest.raises(NotImplementedError):
        t.discover_capabilities()
    # close() is a no-op by default, not NotImplementedError
    assert t.close() is None

def test_create_transport_http():
    t = create_transport({"federation": {"transport_type": "http"}})
    assert isinstance(t, HttpTransport)


def test_create_transport_default_is_http():
    t = create_transport({})
    assert isinstance(t, HttpTransport)


def test_create_transport_unknown_raises():
    with pytest.raises(ValueError, match="Unknown transport_type"):
        create_transport({"federation": {"transport_type": "quic"}})


def test_create_transport_email_not_implemented():
    with pytest.raises(ValueError, match="not implemented"):
        create_transport({"federation": {"transport_type": "email"}})


# --------------------------------------------------------------------- #
# Explicit transport_type parameter (T-109)
# --------------------------------------------------------------------- #


def test_create_transport_accepts_explicit_type():
    t = create_transport({}, transport_type="http")
    assert isinstance(t, HttpTransport)


def test_create_transport_defaults_to_http_when_unspecified():
    t = create_transport({})
    assert isinstance(t, HttpTransport)


def test_create_transport_rejects_unknown_type():
    try:
        create_transport({}, transport_type="quic")
    except ValueError as e:
        assert "quic" in str(e)
    else:
        raise AssertionError("expected ValueError")


def test_create_transport_email_still_not_implemented():
    # Concrete transports are not decided yet (T-112) — email stays a
    # declared-but-unimplemented transport type.
    try:
        create_transport({}, transport_type="email")
    except ValueError as e:
        assert "not implemented" in str(e)
    else:
        raise AssertionError("expected ValueError")
