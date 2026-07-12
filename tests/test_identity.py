"""Tests for cjm_context_graph_layer.identity — deterministic UUIDv5 identity.

Projected from the identity notebook's test cell at the c25780e8 flip."""
import uuid

import pytest

from cjm_context_graph_layer.identity import canonical_part, derive_edge_id, derive_node_id


def test_determinism_and_distinctness():
    a = derive_node_id("source", "sha256:abc")
    b = derive_node_id("source", "sha256:abc")
    c = derive_node_id("source", "sha256:abd")
    assert a == b, "same tuple -> same id"
    assert a != c, "different tuple -> different id"
    assert uuid.UUID(a).version == 5


def test_canonical_parts_and_boundaries():
    # float canonicalization is repr-stable
    assert canonical_part(300.2) == repr(300.2)
    a = derive_node_id("source", "sha256:abc")
    assert derive_node_id("audio-segment", a, 0.0, 300.2) == derive_node_id("audio-segment", a, 0.0, 300.2)

    # part-boundary collisions impossible via the unit separator
    assert derive_node_id("k", "ab", "c") != derive_node_id("k", "a", "bc")

    # bools and odd types rejected: identity inputs must be deliberate
    for bad in (True, None, [1]):
        with pytest.raises(TypeError):
            canonical_part(bad)


def test_edge_identity():
    e1 = derive_edge_id("n1", "n2", "NEXT")
    assert e1 == derive_edge_id("n1", "n2", "NEXT")
    assert e1 != derive_edge_id("n2", "n1", "NEXT")
