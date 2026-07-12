"""Tests for cjm_context_graph_layer.grammar — relations, attribution, edge builders.

Projected from the grammar notebook's two test cells at the c25780e8 flip."""
from cjm_context_graph_layer.grammar import (ROOT_KINDS, OverlayRelations, SpineRelations,
                                             attribution, grouped_spine_edges, make_edge,
                                             spine_edges)


def test_spine_edge_builders():
    es = spine_edges("p", ["c1", "c2", "c3"])
    rels = [e["relation_type"] for e in es]
    assert rels.count("STARTS_WITH") == 1 and rels.count("PART_OF") == 3 and rels.count("NEXT") == 2
    assert es[0]["source_id"] == "p" and es[0]["target_id"] == "c1"
    # deterministic: identical call -> identical edge ids
    assert [e["id"] for e in es] == [e["id"] for e in spine_edges("p", ["c1", "c2", "c3"])]

    ges = grouped_spine_edges([("a1", ["s1", "s2"]), ("a2", ["s3"])])
    grels = [e["relation_type"] for e in ges]
    assert grels.count("STARTS_WITH") == 2 and grels.count("PART_OF") == 3
    # global NEXT crosses the group boundary: s2 -> s3
    nexts = [(e["source_id"], e["target_id"]) for e in ges if e["relation_type"] == "NEXT"]
    assert nexts == [("s1", "s2"), ("s2", "s3")]
    assert spine_edges("p", []) == [] and grouped_spine_edges([]) == []


def test_relations_attribution_and_make_edge():
    assert SpineRelations.all() == ["NEXT", "PART_OF", "STARTS_WITH"]
    assert OverlayRelations.all() == ["SUPERSEDES", "DERIVED_FROM", "PRODUCED"]
    assert "ingested" in ROOT_KINDS

    att = attribution("agent:claude", method="connection-search")
    assert att["actor"] == "agent:claude" and att["method"] == "connection-search"
    assert isinstance(att["asserted_at"], float)
    att2 = attribution("human", asserted_at=123.0)
    assert att2["asserted_at"] == 123.0 and "method" not in att2

    e = make_edge("a", "b", SpineRelations.NEXT)
    assert e["id"] == make_edge("a", "b", SpineRelations.NEXT)["id"], "deterministic edge id"
    assert e["relation_type"] == "NEXT" and e["properties"] == {}
    e2 = make_edge("a", "b", "NEXT", properties={"role": "x"}, edge_id="explicit")
    assert e2["id"] == "explicit" and e2["properties"] == {"role": "x"}
