import pytest
from federation.transport import Transport


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