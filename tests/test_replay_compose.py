"""composed_replay_handlers: entry-point union, collision policy, loud refusal (DEC 426658f1)."""

import pytest

import cjm_context_graph_layer.journal as journal_mod
from cjm_context_graph_layer.journal import apply_wires, composed_replay_handlers, wires_handlers


class _FakeEp:
    """A minimal importlib.metadata.EntryPoint stand-in: (name, factory)."""
    def __init__(self, name, factory):
        self.name = name
        self._factory = factory

    def load(self):
        return self._factory


def test_union_composes_and_same_object_collision_is_legal(monkeypatch):
    """Registries union; one verb from two cores is legal when both bind the SAME object.

    The real case: transcription and decomp both register `derivation` to the layer's
    shared `apply_wires` — identity comparison is what keeps that collision checkable."""
    def eps(group):
        assert group == journal_mod.REPLAY_GROUP
        return [_FakeEp("a", lambda: wires_handlers("x", "shared")),
                _FakeEp("b", lambda: wires_handlers("y", "shared"))]
    monkeypatch.setattr(journal_mod, "entry_points", eps)
    h = composed_replay_handlers()
    assert set(h) == {"x", "y", "shared"} and h["shared"] is apply_wires


def test_different_handler_collision_refuses_naming_both_owners(monkeypatch):
    """One verb bound to two DIFFERENT handlers refuses loudly, naming both owners."""
    async def other(queue, graph_id, op):
        raise AssertionError("never dispatched")
    def eps(group):
        return [_FakeEp("a", lambda: {"v": apply_wires}),
                _FakeEp("b", lambda: {"v": other})]
    monkeypatch.setattr(journal_mod, "entry_points", eps)
    with pytest.raises(ValueError, match=r"'v'.*'a'.*'b'.*DIFFERENT"):
        composed_replay_handlers()
