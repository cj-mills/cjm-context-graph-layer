"""Queue-touching layer operations: the shared graph_task helper (task channel), idempotent emission (emit-if-absent + verify-if-present), and extend_graph — the one primitive every graph-extending workflow commits through. Deterministic ids (see identity) make idempotency a presence check instead of a search."""

import json
import time
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple

from cjm_context_graph_primitives.journal import PROVENANCE_TS
from cjm_context_graph_primitives.query import (EdgeQuery, EdgeQueryResult, NodeQuery,
                                                NodeQueryResult)
from cjm_context_graph_primitives.timings import CURRENT_RUN, record_storage
from cjm_context_graph_primitives.writer import JournalPosition
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


def _whole_scope(
    method: str,              # The storage method
    kwargs: Dict[str, Any],   # Its kwargs
) -> bool:  # True when the call reads a whole kind or the whole graph
    """Whether a storage call is WHOLE-SCOPE: a typed query, or an export filter, naming no ids,
    endpoints or predicate -- its cost is O(kind) or O(graph), whatever it returns. A count
    reads no rows and is never whole; a keyset page of a whole pass is (each page counts)."""
    if method == "find_nodes_by_label":
        return True
    if method == "export_graph":
        q = kwargs.get("filter_query")
    elif method in ("query_nodes", "query_edges"):
        q = kwargs.get("query")
    else:
        return False
    if q is None:
        return True
    q = _wire(q)
    scoping = ("ids", "source", "related", "where", "source_id", "target_id", "source_ids",
               "target_ids", "source_related", "target_related")
    return not q.get("count") and not any(q.get(k) for k in scoping)


def _rows_in(
    method: str,              # The storage method
    kwargs: Dict[str, Any],   # Its kwargs
) -> int:  # Elements the call carries in: written, or named by id
    """The rows a storage call sends: the elements it writes, else the ids it names."""
    if method == "import_graph":
        data = _wire(kwargs.get("graph_data") or {})
        return len(data.get("nodes") or []) + len(data.get("edges") or [])
    for key in ("nodes", "edges", "node_ids", "edge_ids"):
        if kwargs.get(key):
            return len(kwargs[key])
    q = kwargs.get("query")
    if q is not None and _wire(q).get("ids"):
        return len(_wire(q)["ids"])
    return 1 if method in ("update_node", "update_edge", "get_node", "get_edge") else 0


def _rows_out(
    result: Any,  # A storage call's (wire-decoded) result
) -> int:  # The elements / rows it returned
    """The rows a storage call returned: a list's length, else its nodes + edges + rows."""
    if isinstance(result, list):
        return len(result)
    n = 0
    for attr in ("nodes", "edges", "rows"):
        v = result.get(attr) if isinstance(result, dict) else getattr(result, attr, None)
        if isinstance(v, list):
            n += len(v)
    return n


def _page_counters(
    method: str,              # The storage method
    kwargs: Dict[str, Any],   # Its kwargs
    rows_out: int,            # The rows it returned
) -> Dict[str, bool]:  # {"at_limit": ..., "paged": ...}, the keys only when true
    """A typed read's paging evidence: `at_limit` when an UNORDERED read returned exactly its
    limit (above 1) -- an arbitrary subset may have been cut; an ordered top-N is a window by
    design, a limit of 1 an existence probe -- and `paged` when it continued a keyset pass. A
    run whose at_limit calls outnumber its paged ones cut an answer it never continued -- the
    silent-cap guard (finding a107e2f3)."""
    q = kwargs.get("query") if method in ("query_nodes", "query_edges") else None
    if q is None:
        return {}
    q = _wire(q)
    out = {}
    limit = q.get("limit") or 0
    ids = q.get("ids") or []
    if limit > 1 and rows_out == limit and not q.get("order_by") and (not ids or limit < len(ids)):
        out["at_limit"] = True  # a batch by id is bounded by its ids, never an arbitrary cut
    if q.get("after"):
        out["paged"] = True
    return out


def _query_shape(
    method: str,  # query_nodes | query_edges
    query: Any,   # The typed query (or its wire dict)
) -> str:  # The query's PLAN SHAPE: its structure, the values that never change a plan blanked
    """A typed query reduced to what its plan depends on: ids, endpoints, predicate values and
    page cursors become presence flags, so the storage door asks the backend for a plan once per
    shape per run (the plan guard, design detail 3bd8673d) -- never once per call."""
    def blank(key: str, v: Any) -> Any:
        if key in ("ids", "source_ids", "target_ids", "source_id", "target_id", "after", "value",
                   "values", "limit", "offset"):
            return bool(v)
        if isinstance(v, dict):
            return {k: blank(k, x) for k, x in sorted(v.items())}
        if isinstance(v, list):
            return [blank(key, x) for x in v]
        return v
    return method + json.dumps(blank("", _wire(query)), sort_keys=True, default=str)


async def _plan_scans(
    queue: JobQueue,          # Started job queue
    graph_id: str,            # Graph-storage capability instance id
    method: str,              # query_nodes | query_edges
    kwargs: Dict[str, Any],   # The call's kwargs (its `query`)
    run: Any,                 # The armed run (its plan cache)
) -> List[str]:  # What the query's plan reads whole ([] when nothing, or when unanswerable)
    """The plan guard (design detail 3bd8673d, 681604c1): the backend's plan for this query's
    shape, asked once per shape per run with the run DISARMED (its own cost is the `plans`
    phase, never a storage row). Fail-open: a backend without `explain_query` answers []."""
    query = kwargs.get("query")
    if query is None:
        return []
    key = _query_shape(method, query)
    if key not in run.plans:
        token = CURRENT_RUN.set(None)
        t = time.perf_counter()
        try:
            jid = await queue.submit(graph_id, task=GRAPH_TASK, method="explain_query", query=query)
            job = await queue.wait_for_job(jid)
            res = job.result if job.status == JobStatus.completed else None
            res = res if isinstance(res, dict) else _wire(res or {})
            run.plans[key] = list(res.get("scans") or [])
        except Exception:
            run.plans[key] = []
        finally:
            CURRENT_RUN.reset(token)
            run.add("plans", time.perf_counter() - t)
    return run.plans[key]


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
    t = time.perf_counter()
    jid = await queue.submit(graph_id, task=GRAPH_TASK, method=method, **kwargs)
    job = await queue.wait_for_job(jid)
    if job.status != JobStatus.completed:
        raise RuntimeError(f"{graph_id} {method} {job.status}: {job.error}")
    run = CURRENT_RUN.get()
    if run is not None:
        # The instrument (design detail 681604c1): the one storage door folds every call into
        # the armed run -- seconds submit to result, rows each way, whole scope, what its plan
        # reads whole (asked once per query shape, 3bd8673d), and what the proxy measured on the
        # job (wire bytes, the worker's own seconds, post / decode).
        seconds = time.perf_counter() - t
        rows_out = _rows_out(job.result)
        scans = (await _plan_scans(queue, graph_id, method, kwargs, run)
                 if method in ("query_nodes", "query_edges") else None)
        record_storage(method, seconds, whole=_whole_scope(method, kwargs), scans=scans,
                       rows_in=_rows_in(method, kwargs), rows_out=rows_out,
                       **_page_counters(method, kwargs, rows_out),
                       **(getattr(job, "metrics", None) or {}))
    return job.result


async def store_covariates(
    queue: JobQueue,  # Started job queue
    graph_id: str,    # Graph-storage capability instance id
) -> Dict[str, Any]:  # {nodes, edges, db_bytes, kinds}, or {error}
    """The store half of the instrument's growth covariates (design detail 681604c1): the nodes,
    edges and bytes of the store and the nodes per kind, from one get_schema -- O(graph), so a
    caller measures it beside its run, never inside every one (the gesture seam measures it at
    open and on a timer, design detail 4389a579). Measured with the run DISARMED, so the armed
    run's storage table holds its own calls alone; its cost is the run's `covariates` phase.
    Never raises: the instrument must not break the work it measures."""
    run = CURRENT_RUN.get()
    token = CURRENT_RUN.set(None)
    t = time.perf_counter()
    try:
        schema = await graph_task(queue, graph_id, "get_schema")
        kinds = dict(sorted((schema.get("counts") or {}).items()))
        return {"nodes": sum(kinds.values()), "edges": schema.get("edge_count"),
                "db_bytes": schema.get("db_bytes"), "kinds": kinds}
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}
    finally:
        CURRENT_RUN.reset(token)
        if run is not None:
            run.add("covariates", time.perf_counter() - t)


@dataclass
class GraphCursorStore:
    """A graph store's applied cursors as a writer's CursorStore (design detail 65f4a6c9 (2c)): the
    capability's journal_cursors through the one storage door, beside the graph and never in it --
    so neither the op clock nor the write observer sees a cursor write."""

    queue: JobQueue  # Started job queue
    graph_id: str    # Graph-storage capability instance id

    async def read(
        self,
        family: str,  # The journal family (its live tail's file name)
    ) -> Optional[JournalPosition]:  # The db's cursor for it, or None when it holds none
        held = (await graph_task(self.queue, self.graph_id, "journal_cursors") or {}).get(family)
        return JournalPosition.from_dict(held) if held else None

    async def record(
        self,
        family: str,                # The journal family
        position: JournalPosition,  # Just past the last record applied
    ) -> None:
        await graph_task(self.queue, self.graph_id, "record_journal_cursor",
                         family=family, position=position.to_dict())


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


def stamp_op_time(
    element: Dict[str, Any],  # A node or edge wire dict about to be ADDED
    ts: Optional[float],      # The op clock's time (None: no window, the capability stamps now())
) -> Dict[str, Any]:  # The same dict, stamped in place
    """Stamp the op clock's time onto an element about to be added — the one function live
    writes and replay share (design 8f6f2343). A carried time is kept; an explicit None is
    ABSENT, never a carried time (finding b562d5f7: `setdefault` kept the correction core's
    `"created_at": null`, so the capability stamped now() and a rebuild restamped 42.5k
    nodes to the rebuild's own time)."""
    if ts is not None:
        for key in ("created_at", "updated_at"):
            if element.get(key) is None:
                element[key] = ts
    return element


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
    # A carried stamp is kept, an explicit None is absent (`stamp_op_time`); outside a
    # window, capability now().
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
            for n in absent:
                stamp_op_time(n, ts)
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
            for e in absent_edges:
                stamp_op_time(e, ts)
            added = await graph_task(queue, graph_id, "add_edges", edges=absent_edges)
            result.added_edge_ids = list(added or [])
            result.edges_added = len(result.added_edge_ids)

    return result
