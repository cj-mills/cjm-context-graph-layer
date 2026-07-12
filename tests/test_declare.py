"""Tests for cjm_context_graph_layer.declare — provenance-by-declaration.

Projected from the declare notebook's test cell at the c25780e8 flip."""
from cjm_context_graph_layer.declare import Derivation, derivation_to_graph


def test_derivation_to_graph_node_and_edges():
    d = Derivation(actor="host:decomp", method="alignment-fold/v1",
                   input_ids=["t1", "t2"], output_ids=["src"],
                   composition_id="comp-1", job_ids=["j1"], properties={"segments": 3579})
    node, edges = derivation_to_graph(d)
    assert node["label"] == "Derivation"
    assert node["properties"]["actor"] == "host:decomp"
    assert node["properties"]["method"] == "alignment-fold/v1"
    assert node["properties"]["composition_id"] == "comp-1"
    assert node["properties"]["segments"] == 3579
    assert len(edges) == 3
    rels = sorted(e["relation_type"] for e in edges)
    assert rels == ["DERIVED_FROM", "DERIVED_FROM", "PRODUCED"]
    roles = {e["target_id"]: e["properties"]["role"] for e in edges}
    assert roles == {"t1": "input", "t2": "input", "src": "output"}


def test_derivation_ids_explicit_and_generated():
    d = Derivation(actor="host:decomp", method="m", input_ids=["a"], output_ids=["b"])
    n2, _ = derivation_to_graph(d, derivation_id="evt-1")
    assert n2["id"] == "evt-1"
    # events are asserted, not re-derivable: default ids are unique per call
    assert derivation_to_graph(d)[0]["id"] != derivation_to_graph(d)[0]["id"]
