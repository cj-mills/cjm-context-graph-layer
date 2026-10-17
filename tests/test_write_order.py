"""The workflow lane on the write order (ruling 6683f49e, eeda5977 leg L3 slice E1): journal_extend
plans by presence and writes through the runner, the unjournaled default refused; a nested writer
joins the enclosing one; the declared registries checked; a replay stamps the cursor; compaction
journals its op before the storage move, under the family's lock."""

import asyncio
from types import SimpleNamespace

import cjm_context_graph_layer.journal as journal_mod
import pytest
from cjm_context_graph_layer.compact import compact_family, CompactRefusal, scan_references
from cjm_context_graph_layer.journal import (graph_writer, journal_extend, MissingRegistry,
                                             replay_journal, UnjournaledWrite, wires_handlers,
                                             workflow_handlers)
from cjm_context_graph_layer.ops import GraphCursorStore
from cjm_context_graph_primitives.journal import read_journal
from cjm_context_graph_primitives.writer import FAULT_HOOK, ORIGIN, read_tail, stamp_cursor
from cjm_substrate.core.queue import JobStatus


class MemGraph:
    """An in-memory graph store behind a fake queue: the storage methods the write order uses."""

    def __init__(self):
        self.nodes, self.edges, self.cursors, self.methods = {}, {}, {}, []

    async def submit(self, graph_id, task=None, method=None, **kw):
        self.methods.append(method)
        self._result = self._run(method, kw)
        return "j"

    async def wait_for_job(self, jid):
        return SimpleNamespace(status=JobStatus.completed, result=self._result, error=None)

    def _run(self, method, kw):
        if method == "query_nodes":
            ids = kw["query"].get("ids") or []
            return SimpleNamespace(nodes=[SimpleNamespace(**self.nodes[i]) for i in ids
                                          if i in self.nodes])
        if method == "query_edges":
            return SimpleNamespace(rows=[{"id": i} for i in kw["query"].get("ids") or []
                                         if i in self.edges])
        if method in ("add_nodes", "add_edges"):
            into = self.nodes if method == "add_nodes" else self.edges
            items = kw["nodes"] if method == "add_nodes" else kw["edges"]
            for w in items:
                into[w["id"]] = {**w, "sources": w.get("sources") or []}
            return [w["id"] for w in items]
        if method == "delete_nodes":
            gone = [i for i in kw["node_ids"] if self.nodes.pop(i, None) is not None]
            for eid in [e for e, w in self.edges.items()
                        if w["source_id"] in gone or w["target_id"] in gone]:
                del self.edges[eid]
            return len(gone)
        if method == "journal_cursors":
            return dict(self.cursors)
        if method == "record_journal_cursor":
            self.cursors[kw["family"]] = kw["position"]
            return True
        raise AssertionError(f"MemGraph: unexpected method {method}")

    def state(self):  # What a comparison reads: every node and edge, as stored
        return self.nodes, self.edges


async def drop(queue, graph_id, op):  # A compaction op's apply: delete what moved
    await journal_mod.graph_task(queue, graph_id, "delete_nodes", node_ids=op["deletes"])


HANDLERS = {**wires_handlers("put"), "drop": drop}


class Crash(BaseException):
    """A simulated crash: nothing on the write path catches it."""


def node(i):
    return {"id": f"n{i}", "label": "Probe", "properties": {"i": i}}


def edge(a, b):
    return {"id": f"e{a}{b}", "source_id": f"n{a}", "target_id": f"n{b}",
            "relation_type": "NEXT", "properties": {}}


def extend(g, j, nodes, edges=()):
    return journal_extend(g, "g", nodes, list(edges), journal_path=j, verb="put", actor="test",
                          handlers=HANDLERS)


def journaled_nodes(j):  # Every node id the family's ops carry as wires
    return {n["id"] for op in read_journal(j) for n in (op.get("wires") or {}).get("nodes") or []}


def fresh(tmp_path):
    g, j = MemGraph(), str(tmp_path / "w.writes.jsonl")
    asyncio.run(stamp_cursor(j, GraphCursorStore(g, "g")))
    return g, j


def test_journal_extend_plans_by_presence_and_writes_through_the_runner(tmp_path):
    g, j = fresh(tmp_path)
    res = asyncio.run(extend(g, j, [node(1), node(2)], [edge(1, 2)]))
    [op] = read_journal(j)
    assert res.nodes_added == 2 and res.edges_added == 1 and sorted(g.nodes) == ["n1", "n2"]
    # the journaled wires carry the op's ts -- the value the apply stamped and a replay stamps
    assert {n["created_at"] for n in op["wires"]["nodes"]} == {op["ts"]} == {g.nodes["n1"]["created_at"]}
    assert g.cursors["w.writes.jsonl"]["ordinal"] == 1
    # every wire present: no change, nothing journaled (17daa86b)
    again = asyncio.run(extend(g, j, [node(1), node(2)], [edge(1, 2)]))
    assert again.nodes_added == 0 and again.nodes_verified == 2 and len(read_journal(j)) == 1
    # a partial delta journals only the absent wires
    asyncio.run(extend(g, j, [node(2), node(3)], [edge(2, 3)]))
    last = read_journal(j)[-1]
    assert [n["id"] for n in last["wires"]["nodes"]] == ["n3"] and g.cursors["w.writes.jsonl"]["ordinal"] == 2


def test_no_journal_path_refuses_unless_explicitly_unjournaled(tmp_path):
    g = MemGraph()
    with pytest.raises(UnjournaledWrite, match="never written unjournaled by default"):
        asyncio.run(journal_extend(g, "g", [node(1)], []))
    assert g.nodes == {}
    res = asyncio.run(journal_extend(g, "g", [node(1)], [], unjournaled=True))
    assert res.nodes_added == 1 and "n1" in g.nodes and "journal_cursors" not in g.methods


def test_a_crash_after_append_is_healed_by_the_next_extend(tmp_path):
    g, j = fresh(tmp_path)

    def hook(point):
        if point == "after_append":
            raise Crash(point)
    token = FAULT_HOOK.set(hook)
    try:
        with pytest.raises(Crash):
            asyncio.run(extend(g, j, [node(1)]))
    finally:
        FAULT_HOOK.reset(token)
    assert "n1" not in g.nodes and len(read_journal(j)) == 1  # journaled, never applied
    asyncio.run(extend(g, j, [node(2)]))                     # the next writer heals first
    assert sorted(g.nodes) == ["n1", "n2"] and g.cursors["w.writes.jsonl"]["ordinal"] == 2


def test_journal_extend_joins_an_enclosing_gesture_window(tmp_path):
    g, j = fresh(tmp_path)
    g.methods.clear()

    async def gesture():
        async with graph_writer(g, "g", j, HANDLERS) as w:
            await extend(g, j, [node(1)])
            await extend(g, j, [node(2)])
            return w

    w = asyncio.run(asyncio.wait_for(gesture(), 5))
    assert g.methods.count("journal_cursors") == 1  # one cursor read: one heal, one unit of work
    assert w.position.ordinal == 2 and sorted(g.nodes) == ["n1", "n2"]


class _Ep:
    def __init__(self, name):
        self.name = name

    def load(self):
        return lambda: wires_handlers(f"{self.name}-op")


def test_the_declared_registries_are_checked_and_the_registry_cached(monkeypatch):
    names = ["correction", "transcription"]
    monkeypatch.setattr(journal_mod, "entry_points", lambda group: [_Ep(n) for n in names])
    workflow_handlers.cache_clear()
    try:
        with pytest.raises(MissingRegistry, match=r"decomp \(cjm-transcript-decomp-core\)"):
            graph_writer(MemGraph(), "g", "w.writes.jsonl")  # refused as the writer opens
        names.append("decomp")
        h = workflow_handlers()
        assert set(h) == {"correction-op", "decomp-op", "transcription-op"}
        assert workflow_handlers() is h  # one scan per process
        with pytest.raises(TypeError):
            h["x"] = None                # read-only: one mapping serves every writer
    finally:
        workflow_handlers.cache_clear()


def test_a_replay_stamps_the_cursor_and_refuses_an_offset_past_it(tmp_path):
    g, j = fresh(tmp_path)
    for i in range(3):
        asyncio.run(extend(g, j, [node(i)]))
    b = MemGraph()
    asyncio.run(replay_journal(b, "g", j, handlers=HANDLERS, cursors=GraphCursorStore(b, "g")))
    end = read_tail(j, ORIGIN)[-1][1]
    assert b.cursors["w.writes.jsonl"] == end.to_dict() and b.state() == g.state()
    with pytest.raises(RuntimeError, match="would be skipped"):
        asyncio.run(replay_journal(g, "g", j, handlers=HANDLERS, offset=5,
                                   cursors=GraphCursorStore(g, "g")))


def _drop_op(plan):
    return [{"verb": "drop", "args": {"archive": plan.archive_path}, "deletes": ["n1"]}]


def test_compaction_journals_its_op_before_the_storage_move(tmp_path):
    g, j = fresh(tmp_path)
    asyncio.run(extend(g, j, [node(1)]))
    asyncio.run(extend(g, j, [node(2)]))
    arch = str(tmp_path / "archive")

    async def compact():
        async with graph_writer(g, "g", j, HANDLERS) as w:
            return await compact_family(w, arch, ["n1"], _drop_op, label="t", actor="test")

    rep = asyncio.run(asyncio.wait_for(compact(), 5))
    assert rep.ops_archived_whole == 1 and rep.archive_path and sorted(g.nodes) == ["n2"]
    assert scan_references(j, ["n1"]) == []  # the drop's deletes are the tolerated residue
    cursor = g.cursors["w.writes.jsonl"]
    assert cursor == read_tail(j, ORIGIN)[-1][1].to_dict() and cursor["ordinal"] == 2
    b = MemGraph()                            # live == replay of the compacted family
    asyncio.run(replay_journal(b, "g", j, handlers=HANDLERS))
    assert b.state() == g.state()


def test_a_compaction_crashed_before_its_move_replays_equal(tmp_path):
    g, j = fresh(tmp_path)
    asyncio.run(extend(g, j, [node(1)]))
    asyncio.run(extend(g, j, [node(2)]))

    def hook(point):
        if point == "after_append":
            raise Crash(point)

    async def compact():
        async with graph_writer(g, "g", j, HANDLERS) as w:
            return await compact_family(w, str(tmp_path / "archive"), ["n1"], _drop_op, label="t")

    token = FAULT_HOOK.set(hook)
    try:
        with pytest.raises(Crash):
            asyncio.run(asyncio.wait_for(compact(), 5))
    finally:
        FAULT_HOOK.reset(token)
    assert "n1" in g.nodes and "n1" in journaled_nodes(j)
    asyncio.run(extend(g, j, [node(3)]))  # the next writer heals the drop
    b = MemGraph()                         # the uncompacted family: adds n1, then deletes it
    asyncio.run(replay_journal(b, "g", j, handlers=HANDLERS))
    assert sorted(g.nodes) == ["n2", "n3"] and b.state() == g.state()


def test_a_compaction_moving_wires_with_no_op_refuses(tmp_path):
    g, j = fresh(tmp_path)
    asyncio.run(extend(g, j, [node(1)]))

    async def compact():
        async with graph_writer(g, "g", j, HANDLERS) as w:
            return await compact_family(w, str(tmp_path / "archive"), ["n1"], lambda plan: [],
                                        label="t")

    with pytest.raises(CompactRefusal, match="no op journaled"):
        asyncio.run(asyncio.wait_for(compact(), 5))
    assert "n1" in g.nodes and "n1" in journaled_nodes(j)
