"""The streamed element-for-element comparison (ruling f8b9c728; finding b562d5f7 (2) + (3)).

The in-memory half pins what the comparison reports (a time drift, a property drift, a one-sided
id, a path moved on disk) and how a cause attributes (by id, by an originated edge, by a time lag
before an epoch -- never beyond it). The streamed half drives the keyset reader over a fake store
that models the capability's paging contract and requires the same report as the in-memory form."""

import asyncio
from types import SimpleNamespace

import pytest
from cjm_context_graph_layer import graphdiff
from cjm_context_graph_layer.graphdiff import (diff_graphs, diff_stores, IdAttribution,
                                               LagAttribution, MarkAttribution, merge_sorted,
                                               render_diff)
from cjm_context_graph_layer.rebuild import OP_CLOCK_EPOCH, WORKFLOW_ATTRIBUTION


def _node(nid, t=1.0, **props):
    return {"id": nid, "label": "Decision", "properties": props, "sources": [],
            "created_at": t, "updated_at": t}


def _edge(eid, s, t, ts=1.0):
    return {"id": eid, "source_id": s, "target_id": t, "relation_type": "REFERENCES",
            "properties": {}, "created_at": ts, "updated_at": ts}


def test_identical_graphs_are_clean():
    a = [_node("n1", statement="x")]
    res = diff_graphs(a, [], [dict(a[0])], [])
    assert res["clean"] and res["attributed"] == {}


def test_time_and_property_drift_are_named_by_class_and_field():
    res = diff_graphs([_node("n1", 1.0, statement="x")], [],
                      [_node("n1", 1.08, statement="y")], [])
    assert not res["clean"] and res["nodes"]["differing"] == 1
    fields = {r["field"] for r in res["nodes"]["rows"]}
    assert fields == {"created_at", "updated_at", "properties.statement"}
    assert all(r["class"] == "Decision" for r in res["nodes"]["rows"])


def test_one_sided_ids_are_counted_by_class():
    res = diff_graphs([_node("n1"), _node("n2")], [], [_node("n1")], [])
    assert res["nodes"]["only_a"] == {"count": 1, "by_class": {"Decision": 1}, "sample": ["n2"]}
    assert not res["clean"]


def test_property_order_never_reads_as_drift():
    # the live writers store properties in insertion order, the replay in sort_keys order
    a = [{**_node("n1"), "properties": {"x": 1, "y": 2}}]
    b = [{**_node("n1"), "properties": {"y": 2, "x": 1}}]
    assert diff_graphs(a, [], b, [])["clean"]


def test_path_map_normalizes_a_corpus_moved_on_disk():
    a = [_node("n1", path="/old/site/posts/a.md")]
    b = [_node("n1", path="/new/site/posts/a.md")]
    assert not diff_graphs(a, [], b, [])["clean"]
    assert diff_graphs(a, [], b, [], [("/old/site", "/new/site")])["clean"]


def test_an_id_cause_takes_its_rows_and_the_edges_they_originate():
    na = [_node("post", 1.0), _node("born", 1.0)]
    nb = [_node("post", 2.0), _node("born", 3.0)]                        # born drifts on its own
    ea = [_edge("out", "post", "born", 1.0), _edge("in", "born", "post", 1.0)]
    eb = [_edge("out", "post", "born", 2.0), _edge("in", "born", "post", 2.0)]
    moved = IdAttribution("source moved", frozenset({"post"}))
    res = diff_graphs(na, ea, nb, eb, attribution=[moved])
    assert not res["clean"]
    assert res["nodes"]["differing"] == 1 and res["nodes"]["attributed"]["source moved"]["differing"] == 1
    # an edge the moved row ORIGINATES is its row; an edge pointing INTO it is not
    assert res["edges"]["attributed"]["source moved"]["differing"] == 1 and res["edges"]["differing"] == 1
    assert res["edges"]["rows"][0]["samples"][0]["id"] == "in"
    assert res["attributed"] == {"source moved": 2}


def test_an_id_cause_takes_one_sided_rows():
    moved = IdAttribution("source moved", frozenset({"gone"}))
    res = diff_graphs([_node("n1"), _node("gone")], [], [_node("n1")], [], attribution=[moved])
    assert res["clean"] and res["nodes"]["attributed"]["source moved"]["only_a"]["count"] == 1


EPOCH = 100.0
LAG = LagAttribution("pre-op-clock", max_lag=1.0, before=EPOCH)


@pytest.mark.parametrize("a, b, attributed", [
    (50.0, 50.27, True),     # the class: A a moment before B, before the epoch
    (50.27, 50.0, False),    # B before A is never the class
    (50.0, 51.5, False),     # beyond the lag
    (150.0, 150.27, False),  # after the epoch the op clock holds: any lag is drift
    (50.0, 50.0, None),      # equal: no difference at all
])
def test_a_lag_cause_admits_only_its_window(a, b, attributed):
    res = diff_graphs([_node("n1", a)], [], [_node("n1", b)], [], attribution=[LAG])
    got = res["nodes"]["attributed"].get("pre-op-clock", {}).get("differing", 0)
    if attributed is None:
        assert res["clean"] and not got
    else:
        assert res["clean"] is attributed and bool(got) is attributed


def test_a_lag_cause_never_hides_a_property_drift_on_the_same_element():
    res = diff_graphs([_node("n1", 50.0, s="x")], [], [_node("n1", 50.27, s="y")], [],
                      attribution=[LAG])
    assert not res["clean"]
    assert [r["field"] for r in res["nodes"]["rows"]] == ["properties.s"]
    lag = res["nodes"]["attributed"]["pre-op-clock"]
    assert lag["differing"] == 1 and {r["field"] for r in lag["rows"]} == {"created_at", "updated_at"}


@pytest.mark.parametrize("blob_a, col_a, blob_b, attributed", [
    (100.0, 100.001, 50.0, True),   # live's blob copies its own column, the replay's is earlier
    (100.0, 102.0, 50.0, False),    # not a copy: the column is too far after it
    (100.0, 99.0, 50.0, False),     # a column before its copy is no copy
    (50.0, 50.001, 100.0, False),   # B later than A is never the mark
])
def test_a_mark_cause_admits_only_a_copy_of_its_own_column(blob_a, col_a, blob_b, attributed):
    mark = MarkAttribution("reserved key in blob", field="properties.updated_at",
                           column="updated_at", within=1.0)
    a = {**_node("s1", 1.0, updated_at=blob_a), "updated_at": col_a}
    b = {**_node("s1", 1.0, updated_at=blob_b), "updated_at": col_a}
    res = diff_graphs([a], [], [b], [], attribution=[mark])
    assert res["clean"] is attributed
    assert bool(res["nodes"]["attributed"].get(mark.name, {}).get("differing")) is attributed


def test_the_workflow_lane_attributes_its_two_history_classes():
    t = OP_CLOCK_EPOCH - 86400  # a day before the op clock landed
    seg_a, seg_b = _node("seg", t), _node("seg", t + 3.6)  # a slow write: 3.6 s, inside 5 s
    sess_a = {**_node("sess", t, updated_at=t + 10.0), "updated_at": t + 10.001}
    sess_b = {**_node("sess", t, updated_at=t), "updated_at": t + 10.001}
    res = diff_graphs([seg_a, sess_a], [], [seg_b, sess_b], [], attribution=WORKFLOW_ATTRIBUTION)
    assert res["clean"]
    assert res["attributed"] == {"pre-op-clock": 1, "reserved key in blob": 1}


def test_the_merge_refuses_an_out_of_order_stream():
    with pytest.raises(ValueError, match="not in id order"):
        list(merge_sorted([_node("b"), _node("a")], []))


def _store(nodes, edges, calls):
    """A fake graph_task modeling the capability's keyset contract: id order, the rows after
    the cursor, then the limit."""
    def page(rows, q):
        rows = sorted(rows, key=lambda w: w["id"])
        if q.get("after") is not None:
            rows = [w for w in rows if w["id"] > q["after"]]
        return rows[: q["limit"]]

    async def graph_task(queue, graph_id, method, query):
        calls.append((graph_id, method, query.get("after")))
        src, field = (nodes, "nodes") if method == "query_nodes" else (edges, "edges")
        return {field: page(src[graph_id], query)}
    return graph_task


def test_the_streamed_comparison_equals_the_in_memory_one(monkeypatch):
    na = [_node(f"n{i:02d}", 1.0) for i in range(11)] + [_node("only-a")]
    nb = [_node(f"n{i:02d}", 2.0 if i == 7 else 1.0) for i in range(11)] + [_node("zz-only-b")]
    ea = [_edge(f"e{i:02d}", "n00", f"n{i:02d}") for i in range(7)]
    eb = [_edge(f"e{i:02d}", "n00", f"n{i:02d}") for i in range(1, 7)]
    calls = []
    monkeypatch.setattr(graphdiff, "graph_task",
                        _store({"A": na, "B": nb}, {"A": ea, "B": eb}, calls))
    res = asyncio.run(diff_stores(SimpleNamespace(queue=None, graph_id="A"),
                                  SimpleNamespace(queue=None, graph_id="B"), page=3))
    assert res == diff_graphs(na, ea, nb, eb)
    assert res["nodes"]["differing"] == 1 and res["edges"]["only_a"]["sample"] == ["e00"]
    # keyset pages: 12 nodes / 3 per page = 4 full pages + the short one that ends the pass
    assert [c for c in calls if c[:2] == ("A", "query_nodes")] == [
        ("A", "query_nodes", a) for a in (None, "n02", "n05", "n08", "only-a")]


def test_render_names_each_cause_apart_from_drift():
    res = diff_graphs([_node("n1", 50.0)], [], [_node("n1", 50.27)], [], attribution=[LAG])
    text = render_diff({**res, "a": "live.db", "b": "rebuilt.db"})
    assert text.startswith("## Rebuild diff — CLEAN · row(s) attributed: pre-op-clock 1")
    assert "↪ pre-op-clock: only in A 0 · only in B 0 · differing 1" in text
