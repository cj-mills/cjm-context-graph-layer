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
