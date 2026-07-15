"""Journal replay for workflow graphs — the genesis baseline + the pluggable verb registry.

The workflow-graph journal paradigm (DEC ccbab9f5): the graph db is a rebuildable
PROJECTION; the append-only sidecar journal is the source of truth. This module owns the
GRAPH-GENERIC half — the one-time whole-db genesis baseline (`genesis-node`/`genesis-edge`
ops, the M3-recipe migration precedent) and the replay driver. Domain cores register their
own verb handlers on replay (replay stays DOMAIN-OWNED); the op envelope + append
discipline live in `cjm_context_graph_primitives.journal`.
"""

from typing import Any, Callable, Dict, List, Optional

from cjm_context_graph_primitives.journal import append_op, read_journal
from cjm_context_graph_primitives.query import EdgeQuery, NodeQuery

from .ops import extend_graph, graph_task

GENESIS_NODE = "genesis-node"  # One node's whole-db baseline op (args = the node wire dict)
GENESIS_EDGE = "genesis-edge"  # One edge's whole-db baseline op (args = the edge wire dict)
_SCAN_LIMIT = 10_000_000       # Explicit whole-graph read limit (the full_graph_view recipe)


async def genesis_export(
    queue: Any,          # Started job queue
    graph_id: str,       # Graph-storage capability id
    journal_path: str,   # Sidecar journal path (next to the db)
    actor: str,          # Provenance actor for the baseline (e.g. "import:pass1-baseline")
) -> Dict[str, int]:  # {"nodes": N, "edges": M} — the baseline op counts journaled
    """One-time whole-db GENESIS BASELINE: journal every node + edge as genesis ops.

    The M3-recipe migration precedent applied to a workflow graph (DEC ccbab9f5
    point 4): a pre-journal db's entire state becomes journal ops, so from here on
    `replay_journal` alone reconstructs it and the db is a DISPOSABLE projection.
    REFUSES a journal that already holds a genesis op — the baseline is one-time
    (a re-export would double the log); appends ride the bulk path (dedup=False),
    guarded by that refusal. Two whole-graph scans, zero per-node round-trips."""
    if any(op.get("verb") in (GENESIS_NODE, GENESIS_EDGE) for op in read_journal(journal_path)):
        raise ValueError(f"genesis_export: {journal_path} already holds a genesis baseline")
    res = await graph_task(queue, graph_id, "query_nodes",
                           query=NodeQuery(limit=_SCAN_LIMIT).to_dict())
    nodes = res.nodes or []
    for n in nodes:
        append_op(journal_path, {"verb": GENESIS_NODE, "id": f"{GENESIS_NODE}:{n.id}",
                                 "actor": actor, "args": n.to_dict()}, dedup=False)
    eres = await graph_task(queue, graph_id, "query_edges",
                            query=EdgeQuery(limit=_SCAN_LIMIT).to_dict())
    edges = eres.edges or []
    for e in edges:
        append_op(journal_path, {"verb": GENESIS_EDGE, "id": f"{GENESIS_EDGE}:{e.id}",
                                 "actor": actor, "args": e.to_dict()}, dedup=False)
    return {"nodes": len(nodes), "edges": len(edges)}


async def replay_journal(
    queue: Any,         # Started job queue
    graph_id: str,      # Graph-storage capability id
    journal_path: str,  # Sidecar journal path
    handlers: Optional[Dict[str, Callable[..., Any]]] = None,  # Domain verb -> async handler(queue, graph_id, op)
    batch: int = 500,   # extend_graph flush size for genesis ops
) -> Dict[str, int]:  # Genesis add/verify counts + a count per handled domain verb
    """Re-apply every journaled op in append order — the db-from-journal rebuild.

    Genesis ops flush through `extend_graph` in batches, so replay is IDEMPOTENT:
    onto a fresh db everything adds; onto a live one, present nodes verify and
    collide into no-ops (the M3 convergence semantics). Domain verbs dispatch to
    their registered handler — replay stays DOMAIN-OWNED (DEC ccbab9f5 point 1) —
    and pending genesis batches flush BEFORE a handler runs, so every op sees its
    predecessors applied. An unregistered verb raises LOUDLY: silently skipping an
    op would rebuild a db missing knowledge the journal holds."""
    handlers = handlers or {}
    counts: Dict[str, int] = {"nodes_added": 0, "nodes_verified": 0,
                              "edges_added": 0, "edges_existing": 0}
    pending_nodes: List[Dict[str, Any]] = []
    pending_edges: List[Dict[str, Any]] = []

    async def flush() -> None:
        if pending_nodes or pending_edges:
            r = await extend_graph(queue, graph_id, list(pending_nodes), list(pending_edges))
            counts["nodes_added"] += r.nodes_added
            counts["nodes_verified"] += r.nodes_verified
            counts["edges_added"] += r.edges_added
            counts["edges_existing"] += r.edges_existing
            pending_nodes.clear()
            pending_edges.clear()

    for op in read_journal(journal_path):
        verb = op.get("verb", "")
        if verb == GENESIS_NODE:
            pending_nodes.append(op["args"])
        elif verb == GENESIS_EDGE:
            pending_edges.append(op["args"])
        else:
            await flush()
            if verb not in handlers:
                raise ValueError(f"replay_journal: no handler for verb {verb!r} — "
                                 f"refusing to silently drop journaled knowledge")
            await handlers[verb](queue, graph_id, op)
            counts[verb] = counts.get(verb, 0) + 1
        if len(pending_nodes) + len(pending_edges) >= batch:
            await flush()
    await flush()
    return counts
