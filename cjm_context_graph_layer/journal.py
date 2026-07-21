"""Journal replay for workflow graphs — the genesis baseline + the pluggable verb registry.

The workflow-graph journal paradigm (DEC ccbab9f5): the graph db is a rebuildable
PROJECTION; the append-only sidecar journal is the source of truth. This module owns the
GRAPH-GENERIC half — the one-time whole-db genesis baseline (`genesis-node`/`genesis-edge`
ops, the M3-recipe migration precedent) and the replay driver. Domain cores register their
own verb handlers on replay (replay stays DOMAIN-OWNED); the op envelope + append
discipline live in `cjm_context_graph_primitives.journal`.
"""

from importlib.metadata import entry_points
from typing import Any, Callable, Dict, List, Optional

from cjm_context_graph_primitives.journal import append_op, read_journal
from cjm_context_graph_primitives.query import EdgeQuery, NodeQuery

from .ops import extend_graph, ExtendResult, graph_task

GENESIS_NODE = "genesis-node"  # One node's whole-db baseline op (args = the node wire dict)
GENESIS_EDGE = "genesis-edge"  # One edge's whole-db baseline op (args = the edge wire dict)
_SCAN_LIMIT = 10_000_000       # Explicit whole-graph read limit (the full_graph_view recipe)
REPLAY_GROUP = "cjm_context_graph_layer.replay"  # Entry-point group each core's registry factory joins


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
                                 f"refusing to silently drop journaled knowledge "
                                 f"(is the core owning it installed in this env? "
                                 f"composed_replay_handlers unions every installed registry)")
            await handlers[verb](queue, graph_id, op)
            counts[verb] = counts.get(verb, 0) + 1
        if len(pending_nodes) + len(pending_edges) >= batch:
            await flush()
    await flush()
    return counts


async def apply_wires(
    queue: Any,             # Started job queue
    graph_id: str,          # Graph-storage capability id
    op: Dict[str, Any],     # A journaled op carrying {"wires": {"nodes": [...], "edges": [...]}}
) -> None:
    """The generic replay handler for wire-carrying ops (the single wires-replay authority).

    Re-applies exactly what the op recorded through the idempotent extend —
    present wires verify-collide into no-ops, so replay onto ANY db state
    converges. Register it per domain verb via `wires_handlers`."""
    w = op.get("wires") or {}
    await extend_graph(queue, graph_id, w.get("nodes") or [], w.get("edges") or [])


def sidecar_journal_path(
    db_path: str,  # The workflow graph db path (e.g. .../context_graph.db)
) -> str:  # The sidecar write-journal path next to it (.../context_graph.writes.jsonl)
    """The db's sidecar journal path (DEC ccbab9f5 point 3: placement is per-workflow,
    NEXT TO the db it is the source of truth for). Derived, never configured — one
    less ambient default (the explicit-db-path guardrail extended to the journal)."""
    if db_path.endswith(".db"):
        return db_path[: -len(".db")] + ".writes.jsonl"
    return db_path + ".writes.jsonl"


def wires_handlers(
    *verbs: str,  # Domain verbs whose ops carry wires
) -> Dict[str, Any]:  # verb -> apply_wires, ready to merge into a replay handler registry
    """Convenience registry: every named verb replays via `apply_wires`.

    Keeps `replay_journal`'s loud-unknown-verb guarantee intact — verbs are
    registered EXPLICITLY, never inferred from op shape."""
    return dict.fromkeys(verbs, apply_wires)


async def journal_extend(
    queue: Any,                   # Started job queue
    graph_id: str,                # Graph-storage capability id
    nodes: List[Dict[str, Any]],  # Node wire dicts to extend with
    edges: List[Dict[str, Any]],  # Edge wire dicts to extend with
    journal_path: Optional[str] = None,  # Sidecar journal — append the DELTA op on success (None = unjournaled)
    verb: str = "graph-extend",   # Domain verb for the journaled op
    actor: str = "",              # Who produced the wires (e.g. "pipeline:cjm-transcription-core")
    run: Optional[str] = None,    # Run id — pins the run manifest (derivation identity, DEC ccbab9f5 pt 8)
    args: Optional[Dict[str, Any]] = None,  # Small semantic summary for the op (the dataset row)
) -> ExtendResult:  # The extend result (adds + verified counts)
    """Idempotent extend + journal the DELTA — the pipeline-write append-through.

    Journals ONLY the wires this call actually ADDED: verified-present wires were
    added by an earlier journaled op (or the genesis baseline), so re-runs over
    cached content collide into verified no-ops AND leave the journal untouched —
    the Derivation no-spam principle applied to the write journal. Appends ride
    the bulk lane (fresh deterministic ids collide at REPLAY, not append time)."""
    res = await extend_graph(queue, graph_id, nodes, edges)
    if journal_path and (res.nodes_added or res.edges_added):
        added_n = set(res.added_node_ids)
        added_e = set(res.added_edge_ids)
        op: Dict[str, Any] = {"verb": verb, "actor": actor,
                              "args": dict(args or {}),
                              "wires": {"nodes": [n for n in nodes if n["id"] in added_n],
                                        "edges": [e for e in edges if e["id"] in added_e]}}
        if run:
            op["run"] = run
        append_op(journal_path, op, dedup=False)
    return res


def composed_replay_handlers() -> Dict[str, Any]:  # verb -> async handler(queue, graph_id, op)
    """Union every installed core's replay registry (DEC 426658f1 — the d066826a fix).

    Scans the `REPLAY_GROUP` entry-point group: each domain core exports a
    ZERO-ARG factory returning its verb->handler dict, so registration stays
    explicit and domain-owned (DEC ccbab9f5) while ONE call composes a registry
    that can replay the SHARED workflow journal. The union is complete exactly
    when the env holds every emitting core (the 08e98a66 one-usage-env); a
    missing core's verbs stay absent and `replay_journal` refuses loudly.
    Collisions: the same handler OBJECT is legal — both pipeline cores register
    `derivation` to the shared `apply_wires` — different objects refuse, naming
    both owners (two cores claiming one verb with different semantics is a bug)."""
    handlers: Dict[str, Any] = {}
    owners: Dict[str, str] = {}
    for ep in sorted(entry_points(group=REPLAY_GROUP), key=lambda e: e.name):
        for verb, handler in ep.load()().items():
            if verb in handlers and handlers[verb] is not handler:
                raise ValueError(
                    f"composed_replay_handlers: verb {verb!r} registered by both "
                    f"{owners[verb]!r} and {ep.name!r} with DIFFERENT handlers — refusing")
            handlers[verb] = handler
            owners[verb] = ep.name
    return handlers
