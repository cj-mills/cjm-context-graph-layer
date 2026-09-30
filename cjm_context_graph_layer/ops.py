"""Queue-touching layer operations: the shared graph_task helper (task channel), idempotent emission (emit-if-absent + verify-if-present), and extend_graph — the one primitive every graph-extending workflow commits through. Deterministic ids (see identity) make idempotency a presence check instead of a search."""

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple

from cjm_context_graph_primitives.journal import PROVENANCE_TS
from cjm_context_graph_primitives.query import (EdgeQuery, EdgeQueryResult, NodeQuery,
                                                NodeQueryResult)
from cjm_substrate.core.queue import JobQueue, JobStatus

# Importing the typed query/result classes IS the host-side wire registration
# (F8); the tuple keeps the SIDE-EFFECT imports referenced so the canonical
# emit cannot prune them (DEC 7288c788).
_REGISTERED_WIRE_KINDS = (NodeQueryResult, EdgeQueryResult)

GRAPH_TASK = "graph-storage"  # The graph-storage adapter task (explicit task channel, stage 4)

# The OP CLOCK window (finding 0d50b921; design 8f6f2343 moved it into primitives and
# re-exports it here, one variable for replay and live): replay opens it at each op's
# journaled ts, a live write opens it with ONE clock read (`op_clock`), and extend_graph /
# graph_task stamp it as created_at / updated_at on what they add and update — so a live db
# and its rebuild carry the same times. Outside any window the storage capability stamps
# now() (unjournaled writes: ingest scratch, probes).


# THE WRITE OBSERVER (design amendment 9ee4e346): a derived step that runs once per op-clock
# window (the site-link resolve) needs to know whether the window wrote any of its inputs.
# graph_task is the one door every storage write passes, so an observer opened here sees
# them all. Off by default (no cost); an inner observer takes the writes of its block (a
# replay window inside a CLI invocation owns its own step).
WRITE_OBSERVER: ContextVar[Optional[List[Dict[str, Any]]]] = ContextVar("graph_write_observer", default=None)

WRITE_METHODS = frozenset({"add_nodes", "add_edges", "update_node", "update_edge",
                           "delete_nodes", "delete_edges", "import_graph"})


@contextmanager
def observe_writes() -> Iterator[List[Dict[str, Any]]]:  # The block's writes, appended as they run
    """Collect every storage write made inside the block, one record per write:
    `{method, nodes: [{id, label, properties}], edges: [{id, relation_type, properties}]}`.

    An add or import carries what it writes; an update carries its id and the properties it
    merges (label None); a DELETE carries what it is about to remove, read just before —
    a delete names ids only, and a step deciding whether its inputs moved needs their kinds."""
    writes: List[Dict[str, Any]] = []
    token = WRITE_OBSERVER.set(writes)
    try:
        yield writes
    finally:
        WRITE_OBSERVER.reset(token)


def _wire(item: Any) -> Dict[str, Any]:  # A node / edge as a wire dict
    return item.to_dict() if hasattr(item, "to_dict") else dict(item)


def _node_row(n: Any) -> Dict[str, Any]:  # {id, label, properties} of a node
    d = _wire(n)
    return {"id": d.get("id"), "label": d.get("label"), "properties": d.get("properties") or {}}


def _edge_row(e: Any) -> Dict[str, Any]:  # {id, relation_type, properties} of an edge
    d = _wire(e)
    return {"id": d.get("id"), "relation_type": d.get("relation_type"),
            "properties": d.get("properties") or {}}


async def _observed(
    queue: JobQueue,                # Started job queue
    graph_id: str,                  # Graph-storage capability instance id
    method: str,                    # A write method (WRITE_METHODS)
    kwargs: Dict[str, Any],         # The write's kwargs
) -> Dict[str, Any]:  # The write's observer record
    """The record `observe_writes` keeps for one write (a delete reads its doomed rows first)."""
    rec: Dict[str, Any] = {"method": method, "nodes": [], "edges": []}
    if method == "add_nodes":
        rec["nodes"] = [_node_row(n) for n in kwargs.get("nodes") or []]
    elif method == "add_edges":
        rec["edges"] = [_edge_row(e) for e in kwargs.get("edges") or []]
    elif method == "import_graph":
        data = _wire(kwargs.get("graph_data") or {})
        rec["nodes"] = [_node_row(n) for n in data.get("nodes") or []]
        rec["edges"] = [_edge_row(e) for e in data.get("edges") or []]
    elif method == "update_node":
        rec["nodes"] = [{"id": kwargs.get("node_id"), "label": None,
                         "properties": dict(kwargs.get("properties") or {})}]
    elif method == "update_edge":
        rec["edges"] = [{"id": kwargs.get("edge_id"), "relation_type": None,
                         "properties": dict(kwargs.get("properties") or {})}]
    elif method == "delete_nodes" and kwargs.get("node_ids"):
        res = await graph_task(queue, graph_id, "query_nodes",
                               query=NodeQuery(ids=list(kwargs["node_ids"])).to_dict())
        rec["nodes"] = [_node_row(n) for n in (res.nodes or [])]
    elif method == "delete_edges" and kwargs.get("edge_ids"):
        res = await graph_task(queue, graph_id, "query_edges",
                               query=EdgeQuery(ids=list(kwargs["edge_ids"])).to_dict())
        rec["edges"] = [_edge_row(e) for e in (res.edges or [])]
    return rec


async def graph_task(
    queue: JobQueue,  # Started job queue
    graph_id: str,    # Graph-storage capability instance id
    method: str,      # Adapter method (e.g. "query_nodes", "add_nodes")
    **kwargs,         # Typed-method kwargs (wire dicts ok; the in-worker adapter normalizes)
) -> Any:  # Typed task result (wire-decoded host-side)
    """Invoke a graph-storage adapter method through the queue's task channel.

    THE shared copy: decomp-core and correction-core's per-core helpers migrate
    onto this one (graph ops stay on the queue path for telemetry/cancellation
    per D7/Thread-5 lock 5).
    """
    # The op clock (0d50b921 residual, design 8f6f2343): an update inside a window —
    # replay at the op's journaled ts, or a live write unit's one clock read — stamps
    # updated_at to that ts. The reserved `updated_at` key rides the wire dict; the
    # storage capability applies it as the column. Outside a window: capability now().
    if method in ("update_node", "update_edge"):
        ts = PROVENANCE_TS.get()
        if ts is not None and "properties" in kwargs:
            kwargs = {**kwargs, "properties": {**kwargs["properties"], "updated_at": ts}}
    writes = WRITE_OBSERVER.get()
    if writes is not None and method in WRITE_METHODS:
        writes.append(await _observed(queue, graph_id, method, kwargs))
    jid = await queue.submit(graph_id, task=GRAPH_TASK, method=method, **kwargs)
    job = await queue.wait_for_job(jid)
    if job.status != JobStatus.completed:
        raise RuntimeError(f"{graph_id} {method} {job.status}: {job.error}")
    return job.result


class GraphIntegrityError(RuntimeError):
    """An emitted node collided with an existing node of different identity content.

    Raised by verify-if-present: same deterministic id but mismatched label or
    provenance content hashes means the identity tuple and the content have
    diverged — never overwrite silently."""
    pass


def _source_hashes(sources: Optional[List[Any]]) -> Set[str]:
    """Content-hash set from a node's sources (typed SourceRefs or wire dicts)."""
    out: Set[str] = set()
    for s in sources or []:
        h = s.get("content_hash") if isinstance(s, dict) else getattr(s, "content_hash", None)
        if h:
            out.add(h)
    return out


def node_identity_mismatch(
    existing: Any,            # Existing node (typed GraphNode or wire dict)
    new: Dict[str, Any],      # New node wire dict being emitted
) -> Optional[str]:  # Mismatch description, or None when compatible
    """Verify-if-present check: label + sources content-hash set must match."""
    ex_label = existing.get("label") if isinstance(existing, dict) else getattr(existing, "label", None)
    if ex_label != new.get("label"):
        return f"label mismatch: existing {ex_label!r} != new {new.get('label')!r}"
    ex_sources = existing.get("sources") if isinstance(existing, dict) else getattr(existing, "sources", None)
    ex_hashes, new_hashes = _source_hashes(ex_sources), _source_hashes(new.get("sources"))
    if ex_hashes != new_hashes:
        return f"sources content-hash mismatch: existing {sorted(ex_hashes)} != new {sorted(new_hashes)}"
    return None


def partition_by_presence(
    items: List[Dict[str, Any]],  # Wire dicts carrying "id"
    existing_ids: Set[str],       # Ids already present in the graph
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:  # (absent, present)
    """Split wire dicts into absent (to add) and present (to verify)."""
    absent = [it for it in items if it["id"] not in existing_ids]
    present = [it for it in items if it["id"] in existing_ids]
    return absent, present


@dataclass
class ExtendResult:
    """Outcome of one idempotent extend_graph commit."""
    nodes_added: int = 0      # Nodes newly created
    nodes_verified: int = 0   # Nodes already present, identity-verified
    edges_added: int = 0      # Edges newly created
    edges_existing: int = 0   # Edges already present (skipped)
    added_node_ids: List[str] = field(default_factory=list)  # Ids of created nodes
    added_edge_ids: List[str] = field(default_factory=list)  # Ids of created edges


async def extend_graph(
    queue: JobQueue,              # Started job queue
    graph_id: str,                # Graph-storage capability id
    nodes: List[Dict[str, Any]],  # Node wire dicts (deterministic ids for layer-0; generated for decisions)
    edges: List[Dict[str, Any]],  # Edge wire dicts
) -> ExtendResult:  # Counts + created ids
    """Idempotently extend the graph: emit-if-absent + verify-if-present.

    Deterministic ids make idempotency a batched presence check (2 reads + at
    most 2 writes per call — the C17 lesson applied to the write path): nodes
    already present are verified against the new emission (label + provenance
    content hashes) and a mismatch raises `GraphIntegrityError` LOUDLY; absent
    nodes/edges are added. Cache-hit re-emission therefore collides into a
    verified no-op (stress item 4), and a re-derived spine reproduces — never
    duplicates — its layer-0 (stress item 1).
    """
    result = ExtendResult()
    # The op clock (0d50b921, design 8f6f2343): inside a window — replay, or a live
    # write unit — stamp its ts onto everything ADDED, so live and rebuild agree.
    # setdefault keeps any explicitly-carried stamp; outside a window, capability now().
    ts = PROVENANCE_TS.get()

    if nodes:
        res = await graph_task(queue, graph_id, "query_nodes",
                               query=NodeQuery(ids=[n["id"] for n in nodes]).to_dict())
        existing = {gn.id: gn for gn in (res.nodes or [])}
        absent, present = partition_by_presence(nodes, set(existing))
        for n in present:
            msg = node_identity_mismatch(existing[n["id"]], n)
            if msg:
                raise GraphIntegrityError(f"node {n['id']}: {msg}")
        result.nodes_verified = len(present)
        if absent:
            if ts is not None:
                for n in absent:
                    n.setdefault("created_at", ts)
                    n.setdefault("updated_at", ts)
            added = await graph_task(queue, graph_id, "add_nodes", nodes=absent)
            result.added_node_ids = list(added or [])
            result.nodes_added = len(result.added_node_ids)

    if edges:
        eres = await graph_task(queue, graph_id, "query_edges",
                                query=EdgeQuery(ids=[e["id"] for e in edges], project=["id"]).to_dict())
        existing_eids = {r["id"] for r in (eres.rows or [])}
        absent_edges = [e for e in edges if e["id"] not in existing_eids]
        result.edges_existing = len(edges) - len(absent_edges)
        if absent_edges:
            if ts is not None:
                for e in absent_edges:
                    e.setdefault("created_at", ts)
                    e.setdefault("updated_at", ts)
            added = await graph_task(queue, graph_id, "add_edges", edges=absent_edges)
            result.added_edge_ids = list(added or [])
            result.edges_added = len(result.added_edge_ids)

    return result
