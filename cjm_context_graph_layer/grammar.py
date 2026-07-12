"""The domain-neutral context-graph grammar: spine relations (NEXT / PART_OF / STARTS_WITH, recurring fractally at every layer), overlay relations (SUPERSEDES / DERIVED_FROM / PRODUCED), root kinds, and the standardized attribution fields."""

import time
from typing import Any, Dict, List, Optional, Tuple

from cjm_context_graph_layer.identity import derive_edge_id


class SpineRelations:
    """Structural spine relations, reused fractally at every layer
    (Source -> AudioSegment -> Segment today; series -> episode tomorrow)."""
    NEXT = "NEXT"               # Sequential order among siblings
    PART_OF = "PART_OF"         # Containment (child -> parent)
    STARTS_WITH = "STARTS_WITH" # Entry point (parent -> first child)

    @classmethod
    def all(cls) -> list:  # All spine relation types
        """All spine relation types."""
        return [cls.NEXT, cls.PART_OF, cls.STARTS_WITH]


class OverlayRelations:
    """Overlay/derivation relations — the trust grammar shared by every
    workflow's graph extensions."""
    SUPERSEDES = "SUPERSEDES"     # Newer overlay node -> the prior one it replaces (append-only undo)
    DERIVED_FROM = "DERIVED_FROM" # Derived/overlay node -> the node(s) it derives from / consumed
    PRODUCED = "PRODUCED"         # Derivation event -> the node(s) it produced

    @classmethod
    def all(cls) -> list:  # All overlay relation types
        """All overlay relation types."""
        return [cls.SUPERSEDES, cls.DERIVED_FROM, cls.PRODUCED]


# The three provenance-root kinds (where-graph-begins resolution): knowledge
# enters the graph anchored one of these ways.
ROOT_KINDS = ("ingested", "asserted", "derived")


def attribution(
    actor: str,                          # Who asserted/produced this (e.g. "human", "agent:claude", "capability:whisper")
    method: Optional[str] = None,        # How (e.g. "transcribe", "alignment-fold/v1")
    asserted_at: Optional[float] = None, # Unix timestamp; None = now
) -> Dict[str, Any]:  # Standardized attribution property dict
    """Standardized attribution fields for derived/asserted nodes.

    Every derivation/assertion carries the same three fields, so audit reads
    are uniform across workflows (P13's hand-rolled Connection attribution
    graduated into the grammar).
    """
    out: Dict[str, Any] = {"actor": actor, "asserted_at": asserted_at if asserted_at is not None else time.time()}
    if method is not None:
        out["method"] = method
    return out


def make_edge(
    source_id: str,                            # Edge source node id
    target_id: str,                            # Edge target node id
    relation_type: str,                        # Relation type (SpineRelations / OverlayRelations / domain)
    properties: Optional[Dict[str, Any]] = None,  # Optional edge properties (e.g. {"role": "foreshadow"})
    edge_id: Optional[str] = None,             # Explicit id; None = deterministic from the triple
) -> Dict[str, Any]:  # Edge wire dict
    """Build an edge wire dict with a deterministic id by default."""
    return {
        "id": edge_id or derive_edge_id(source_id, target_id, relation_type),
        "source_id": source_id,
        "target_id": target_id,
        "relation_type": relation_type,
        "properties": properties or {},
    }


def spine_edges(
    parent_id: str,        # Parent node id
    child_ids: List[str],  # Ordered child node ids
) -> List[Dict[str, Any]]:  # Edge wire dicts
    """The uniform spine pattern at any layer: PART_OF child->parent for each
    child + NEXT chain among children + STARTS_WITH parent->first child."""
    edges: List[Dict[str, Any]] = []
    if child_ids:
        edges.append(make_edge(parent_id, child_ids[0], SpineRelations.STARTS_WITH))
    for i, cid in enumerate(child_ids):
        edges.append(make_edge(cid, parent_id, SpineRelations.PART_OF))
        if i < len(child_ids) - 1:
            edges.append(make_edge(cid, child_ids[i + 1], SpineRelations.NEXT))
    return edges


def grouped_spine_edges(
    groups: List[Tuple[str, List[str]]],  # (parent id, ordered child ids) per group, groups in spine order
) -> List[Dict[str, Any]]:  # Edge wire dicts
    """Spine edges for a fine layer grouped under coarse parents.

    PART_OF goes to the OWNING parent; STARTS_WITH per parent -> its first
    child (the coarse-seam jump anchor); the NEXT chain is GLOBAL across group
    boundaries — fine continuity crosses coarse boundaries (agent span reads).
    """
    edges: List[Dict[str, Any]] = []
    flat: List[str] = []
    for parent_id, child_ids in groups:
        if child_ids:
            edges.append(make_edge(parent_id, child_ids[0], SpineRelations.STARTS_WITH))
        for cid in child_ids:
            edges.append(make_edge(cid, parent_id, SpineRelations.PART_OF))
            flat.append(cid)
    for i in range(len(flat) - 1):
        edges.append(make_edge(flat[i], flat[i + 1], SpineRelations.NEXT))
    return edges
