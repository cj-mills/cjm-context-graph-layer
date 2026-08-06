"""Tests for cjm_context_graph_layer.ops — pure helpers.

Projected from the ops notebook's test cell at the c25780e8 flip (the async
extend_graph path is exercised live by the cores' loop-back harnesses)."""
from cjm_context_graph_layer.ops import (ExtendResult, node_identity_mismatch,
                                         partition_by_presence)


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
