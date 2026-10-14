"""Tests for cjm_context_graph_layer.ops — pure helpers.

Projected from the ops notebook's test cell at the c25780e8 flip (the async
extend_graph path is exercised live by the cores' loop-back harnesses)."""

from cjm_context_graph_layer.ops import ExtendResult, node_identity_mismatch, partition_by_presence


def test_partition_by_presence():
    absent, present = partition_by_presence([{"id": "a"}, {"id": "b"}], {"b"})
    assert [n["id"] for n in absent] == ["a"] and [n["id"] for n in present] == ["b"]


def test_node_identity_mismatch():
    existing = {"label": "Source", "sources": [{"content_hash": "sha256:x"}]}
    new_ok = {"label": "Source", "sources": [{"content_hash": "sha256:x"}]}
    new_label = {"label": "Doc", "sources": [{"content_hash": "sha256:x"}]}
    new_hash = {"label": "Source", "sources": [{"content_hash": "sha256:y"}]}
    assert node_identity_mismatch(existing, new_ok) is None
    assert "label mismatch" in node_identity_mismatch(existing, new_label)
    assert "content-hash mismatch" in node_identity_mismatch(existing, new_hash)

    # typed-object tolerance (GraphNode-shaped)
    class _FakeNode:
        label = "Source"
        sources = [type("S", (), {"content_hash": "sha256:x"})()]
    assert node_identity_mismatch(_FakeNode(), new_ok) is None


def test_extend_result_defaults():
    r = ExtendResult()
    assert r.nodes_added == 0 and r.added_edge_ids == []


def test_graph_task_stamps_update_node_inside_window():
    # 0d50b921 residual: inside the replay provenance window an update_node
    # payload carries the reserved `updated_at` (the op's journaled ts); outside
    # the window payloads ride untouched (live writes keep capability now()).
    import asyncio
    from types import SimpleNamespace

    from cjm_context_graph_layer.ops import PROVENANCE_TS, graph_task
    from cjm_substrate.core.queue import JobStatus

    class FakeQueue:
        def __init__(self):
            self.submitted = []

        async def submit(self, graph_id, **kw):
            self.submitted.append(kw)
            return "j1"

        async def wait_for_job(self, jid):
            return SimpleNamespace(status=JobStatus.completed, result=True, error=None)

    q = FakeQueue()
    token = PROVENANCE_TS.set(777.0)
    try:
        asyncio.run(graph_task(q, "g", "update_node", node_id="n1", properties={"x": 1}))
    finally:
        PROVENANCE_TS.reset(token)
    asyncio.run(graph_task(q, "g", "update_node", node_id="n1", properties={"x": 2}))
    assert q.submitted[0]["properties"] == {"x": 1, "updated_at": 777.0}
    assert q.submitted[1]["properties"] == {"x": 2}


def test_observe_writes_records_every_write_and_reads_a_delete_first():
    # Design amendment 9ee4e346: a step that runs once per window reads the window's writes
    # from the observer at graph_task; a delete names ids only, so its rows are read first.
    import asyncio
    from types import SimpleNamespace

    from cjm_context_graph_layer.ops import graph_task, observe_writes
    from cjm_context_graph_primitives.query import EdgeQueryResult, NodeQueryResult
    from cjm_substrate.core.queue import JobStatus

    doomed_node = {"id": "s1", "label": "Section", "properties": {"anchor": "setup"}}
    doomed_edge = {"id": "e9", "source_id": "a", "target_id": "b",
                   "relation_type": "SUPERSEDES", "properties": {}}

    class FakeQueue:
        def __init__(self):
            self.methods = []

        async def submit(self, graph_id, **kw):
            self.methods.append(kw["method"])
            return kw["method"]

        async def wait_for_job(self, jid):
            result = True
            if jid == "query_nodes":
                result = NodeQueryResult.from_dict({"nodes": [doomed_node]})
            elif jid == "query_edges":
                result = EdgeQueryResult.from_dict({"edges": [doomed_edge]})
            return SimpleNamespace(status=JobStatus.completed, result=result, error=None)

    q = FakeQueue()

    async def go():
        await graph_task(q, "g", "add_nodes", nodes=[{"id": "n1", "label": "Note",
                                                       "properties": {"site_refs": ["/x/"]}}])
        with observe_writes() as writes:
            await graph_task(q, "g", "add_edges", edges=[{"id": "e1", "relation_type": "REFERENCES",
                                                          "source_id": "a", "target_id": "b"}])
            await graph_task(q, "g", "update_node", node_id="n1", properties={"site_refs": []})
            await graph_task(q, "g", "get_node", node_id="n1")   # a read: not recorded
            await graph_task(q, "g", "delete_nodes", node_ids=["s1"], cascade=True)
            await graph_task(q, "g", "delete_edges", edge_ids=["e9"])
        return writes

    writes = asyncio.run(go())
    # outside the block nothing is recorded; inside, one record per write, reads left out
    assert [w["method"] for w in writes] == ["add_edges", "update_node", "delete_nodes", "delete_edges"]
    assert writes[0]["edges"][0]["relation_type"] == "REFERENCES"
    assert writes[1]["nodes"] == [{"id": "n1", "label": None, "properties": {"site_refs": []}}]
    assert writes[2]["nodes"][0]["label"] == "Section" and writes[3]["edges"][0]["relation_type"] == "SUPERSEDES"
    # each delete was read just before it ran
    assert q.methods[-4:] == ["query_nodes", "delete_nodes", "query_edges", "delete_edges"]


def test_journal_extend_stamps_adds_with_the_journaled_ts(tmp_path):
    # The op clock (design 8f6f2343): a live journal_extend opens ONE window, so the nodes
    # and edges it adds carry created_at / updated_at equal to the op ts it journals — the
    # value replay stamps on the same wires.
    import asyncio
    from types import SimpleNamespace

    from cjm_context_graph_layer.journal import journal_extend
    from cjm_context_graph_primitives.journal import read_journal
    from cjm_substrate.core.queue import JobStatus

    class FakeQueue:
        def __init__(self):
            self.submitted = []

        async def submit(self, graph_id, **kw):
            self.submitted.append(kw)
            return "j1"

        async def wait_for_job(self, jid):  # presence queries find nothing; adds succeed
            result = (SimpleNamespace(nodes=[], edges=[])
                      if self.submitted[-1]["method"] == "query_nodes" else ["n1"])
            return SimpleNamespace(status=JobStatus.completed, result=result, error=None)

    q = FakeQueue()
    j = str(tmp_path / "w.jsonl")
    asyncio.run(journal_extend(q, "g", [{"id": "n1", "label": "Seg", "properties": {}}], [],
                               journal_path=j, verb="graph-extend", actor="test"))
    ts = read_journal(j)[0]["ts"]
    added = next(kw for kw in q.submitted if kw["method"] == "add_nodes")
    assert [(n["created_at"], n["updated_at"]) for n in added["nodes"]] == [(ts, ts)]


def test_graph_task_folds_each_call_into_the_armed_run():
    # The instrument (design detail 681604c1): the one storage door records every call by
    # method -- rows each way, the proxy's measurements off the job, and whole scope under the
    # phase it ran in; with no run armed it records nothing.
    import asyncio
    from types import SimpleNamespace

    from cjm_context_graph_layer.ops import graph_task
    from cjm_context_graph_primitives.query import NodeQuery, NodeQueryResult, OrderBy
    from cjm_context_graph_primitives.timings import begin_run, end_run, span
    from cjm_substrate.core.queue import JobStatus

    class FakeQueue:
        async def submit(self, graph_id, **kw):
            return "j1"

        async def wait_for_job(self, jid):
            return SimpleNamespace(status=JobStatus.completed, error=None,
                                   result=NodeQueryResult(nodes=[], rows=[{"id": "a"}, {"id": "b"}]),
                                   metrics={"bytes_in": 40, "worker_s": 0.01})

    q = FakeQueue()
    asyncio.run(graph_task(q, "g", "query_nodes", query=NodeQuery(label="X").to_dict()))  # unarmed
    run = begin_run("cli", "t")
    try:
        async def calls():
            with span("verb"):
                await graph_task(q, "g", "query_nodes", query=NodeQuery(label="X").to_dict())
                await graph_task(q, "g", "query_nodes", query=NodeQuery(ids=["a", "b"]).to_dict())
                await graph_task(q, "g", "query_nodes", query=NodeQuery(label="X", count=True).to_dict())
        asyncio.run(calls())
    finally:
        end_run(run)
    row = run.storage["query_nodes"]
    assert row["calls"] == 3 and row["rows_in"] == 2 and row["rows_out"] == 6
    assert row["bytes_in"] == 120 and abs(row["worker_s"] - 0.03) < 1e-9
    assert list(run.whole) == ["verb :: query_nodes"] and run.whole["verb :: query_nodes"]["calls"] == 1
    assert "at_limit" not in row and "paged" not in row

    # paging evidence: a page that came back full, and a keyset continuation
    run = begin_run("cli", "t")
    try:
        asyncio.run(graph_task(q, "g", "query_nodes", query=NodeQuery(label="X", limit=2).to_dict()))
        asyncio.run(graph_task(q, "g", "query_nodes",
                               query=NodeQuery(label="X", limit=2, after="b").to_dict()))
    finally:
        end_run(run)
    assert run.storage["query_nodes"]["at_limit"] == 2 and run.storage["query_nodes"]["paged"] == 1

    # an ordered window and an existence probe are never a cut answer
    run = begin_run("cli", "t")
    try:
        asyncio.run(graph_task(q, "g", "query_nodes",
                               query=NodeQuery(label="X", limit=2, order_by=OrderBy("ts")).to_dict()))
        asyncio.run(graph_task(q, "g", "query_nodes", query=NodeQuery(label="X", limit=1).to_dict()))
        asyncio.run(graph_task(q, "g", "query_nodes", query=NodeQuery(ids=["a", "b"], limit=2).to_dict()))
    finally:
        end_run(run)
    assert "at_limit" not in run.storage["query_nodes"]


def test_graph_task_asks_each_query_shape_for_its_plan_once_per_run():
    # The plan guard (design detail 3bd8673d): the storage door asks the backend's plan once
    # per query SHAPE (ids and values blanked), with the run disarmed, and folds each call whose
    # plan reads a table whole under its phase; a backend that cannot answer costs nothing.
    import asyncio
    from types import SimpleNamespace

    from cjm_context_graph_layer.ops import graph_task
    from cjm_context_graph_primitives.query import NodeQuery, NodeQueryResult, PropertyPredicate
    from cjm_context_graph_primitives.timings import begin_run, end_run, span
    from cjm_substrate.core.queue import JobStatus

    class FakeQueue:
        def __init__(self):
            self.methods = []

        async def submit(self, graph_id, **kw):
            self.methods.append(kw["method"])
            self.last = kw
            return "j1"

        async def wait_for_job(self, jid):
            if self.last["method"] == "explain_query":
                q = self.last["query"]
                scans = ["nodes:label"] if q.get("where") else ([] if q.get("ids") else ["nodes"])
                return SimpleNamespace(status=JobStatus.completed, error=None,
                                       result={"plan": ["…"], "scans": scans})
            return SimpleNamespace(status=JobStatus.completed, error=None,
                                   result=NodeQueryResult(rows=[]))

    q = FakeQueue()
    run = begin_run("cli", "t")
    try:
        async def calls():
            with span("verb"):
                for slot in ("s1", "s2", "s3"):  # one shape, three values: one plan
                    await graph_task(q, "g", "query_nodes", query=NodeQuery(
                        label="Assertion", where=[PropertyPredicate("slot_id", "eq", slot)]).to_dict())
                await graph_task(q, "g", "query_nodes", query=NodeQuery(ids=["a"]).to_dict())
                await graph_task(q, "g", "query_nodes", query=NodeQuery(ids=["b", "c"]).to_dict())
                await graph_task(q, "g", "query_nodes", query=NodeQuery(project=["id"]).to_dict())
        asyncio.run(calls())
    finally:
        end_run(run)
    assert q.methods.count("explain_query") == 3 and q.methods.count("query_nodes") == 6
    assert run.storage["query_nodes"]["calls"] == 6 and "explain_query" not in run.storage
    assert {k: v["calls"] for k, v in run.plan_scans.items()} == {
        "verb :: query_nodes :: nodes:label": 3, "verb :: query_nodes :: nodes": 1}
    assert run.phases["plans"]["calls"] == 3
    assert run.event()["plan_scans"]["verb :: query_nodes :: nodes"]["calls"] == 1


def test_store_covariates_disarmed_and_fail_open():
    """The store half of the covariates (design detail 4389a579): one get_schema, measured
    with the run disarmed (its storage table holds the run's own calls alone), its cost the
    `covariates` phase; a failing store answers {error}, never raises."""
    import asyncio
    from types import SimpleNamespace

    from cjm_context_graph_layer.ops import store_covariates
    from cjm_context_graph_primitives.timings import begin_run, end_run
    from cjm_substrate.core.queue import JobStatus

    class FakeQueue:
        def __init__(self, fail=False):
            self.fail = fail

        async def submit(self, graph_id, **kw):
            return "j1"

        async def wait_for_job(self, jid):
            if self.fail:
                return SimpleNamespace(status=JobStatus.failed, error="boom", result=None)
            return SimpleNamespace(status=JobStatus.completed, error=None, result={
                "counts": {"Segment": 5, "Decision": 2}, "edge_count": 9, "db_bytes": 4096})

    run = begin_run("gesture", "app:g")
    try:
        cov = asyncio.run(store_covariates(FakeQueue(), "g"))
        bad = asyncio.run(store_covariates(FakeQueue(fail=True), "g"))
    finally:
        end_run(run)
    assert cov == {"nodes": 7, "edges": 9, "db_bytes": 4096,
                   "kinds": {"Decision": 2, "Segment": 5}}
    assert "boom" in bad["error"]
    assert run.storage == {} and run.phases["covariates"]["calls"] == 2
