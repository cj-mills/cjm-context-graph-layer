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


def test_replay_restores_temporal_provenance(tmp_path, monkeypatch):
    # 0d50b921 residual: the LAYER replay variant (workflow dbs) — genesis args
    # carry their own op's journaled ts (per-op, even within one flush batch),
    # and domain handlers run inside the PROVENANCE_TS window.
    import asyncio
    import json
    from types import SimpleNamespace

    from cjm_context_graph_layer.journal import GENESIS_NODE, replay_journal
    from cjm_context_graph_layer.ops import PROVENANCE_TS

    jpath = tmp_path / "wf.writes.jsonl"
    ops = [
        {"verb": GENESIS_NODE, "ts": 111.0,
         "args": {"id": "n1", "label": "X", "properties": {}}},
        {"verb": GENESIS_NODE, "ts": 222.0,
         "args": {"id": "n2", "label": "X", "properties": {}, "created_at": 5.0}},
        {"verb": "domain-op", "ts": 333.0, "args": {}},
    ]
    jpath.write_text("".join(json.dumps(o) + "\n" for o in ops))

    flushed = []

    async def fake_extend(queue, graph_id, nodes, edges):
        flushed.extend(nodes)
        return SimpleNamespace(nodes_added=len(nodes), nodes_verified=0,
                               edges_added=len(edges), edges_existing=0)
    monkeypatch.setattr(journal_mod, "extend_graph", fake_extend)

    seen = []

    async def handler(queue, graph_id, op):
        seen.append(PROVENANCE_TS.get())

    counts = asyncio.run(replay_journal(None, "g", str(jpath),
                                        handlers={"domain-op": handler}))
    assert counts["nodes_added"] == 2 and counts["domain-op"] == 1
    by_id = {n["id"]: n for n in flushed}
    assert by_id["n1"]["created_at"] == 111.0 and by_id["n1"]["updated_at"] == 111.0
    assert by_id["n2"]["created_at"] == 5.0  # a carried stamp wins (setdefault)
    assert seen == [333.0]  # the handler ran inside its op's provenance window
    assert PROVENANCE_TS.get() is None  # window reset after replay
