"""Journal replay for workflow graphs — the genesis baseline + the pluggable verb registry.

The workflow-graph journal paradigm (DEC ccbab9f5): the graph db is a rebuildable
PROJECTION; the append-only sidecar journal is the source of truth. This module owns the
GRAPH-GENERIC half — the one-time whole-db genesis baseline (`genesis-node`/`genesis-edge`
ops, the M3-recipe migration precedent) and the replay driver. Domain cores register their
own verb handlers on replay (replay stays DOMAIN-OWNED); the op envelope + append
discipline live in `cjm_context_graph_primitives.journal`.
"""

import functools
import time
from contextlib import asynccontextmanager, nullcontext
from importlib.metadata import entry_points
from types import MappingProxyType
from typing import Any, AsyncContextManager, AsyncIterator, Callable, Dict, List, Mapping, Optional

from cjm_context_graph_primitives.journal import (append_op, family_lock, op_clock, op_now,
                                                  read_journal)
from cjm_context_graph_primitives.query import EdgeQuery, NodeQuery
from cjm_context_graph_primitives.timings import record_replay, record_series, span, timed
from cjm_context_graph_primitives.writer import (CursorMismatch, CursorStore, family_name,
                                                 JournalWriter, ORIGIN, read_tail)

from .ops import (extend_graph, ExtendResult, graph_task, GraphCursorStore, plan_extend,
                  PROVENANCE_TS, stamp_op_time)

GENESIS_NODE = "genesis-node"  # One node's whole-db baseline op (args = the node wire dict)
GENESIS_EDGE = "genesis-edge"  # One edge's whole-db baseline op (args = the edge wire dict)
_SCAN_LIMIT = 10_000_000       # Explicit whole-graph read limit (the full_graph_view recipe)
REPLAY_GROUP = "cjm_context_graph_layer.replay"  # Entry-point group each core's registry factory joins
# THE WORKFLOW LANE'S DECLARED REGISTRIES (ruling 6683f49e (2)): every core that journals onto the
# shared workflow journal, by its REPLAY_GROUP entry-point name -> the distribution that installs it.
# A writer opening on the lane needs them all: an env missing one could not apply (heal) or replay
# (rebuild) that core's ops.
WORKFLOW_REGISTRIES = {"correction": "cjm-transcript-correction-core",
                       "decomp": "cjm-transcript-decomp-core",
                       "transcription": "cjm-transcription-core"}


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


async def apply_op(
    queue: Any,                    # Started job queue
    graph_id: str,                 # Graph-storage capability id
    op: Dict[str, Any],            # One journaled op
    handlers: Dict[str, Callable[..., Any]],  # Domain verb -> async handler(queue, graph_id, op)
) -> str:  # The verb applied
    """Apply ONE journaled op -- the dispatch replay runs per op and a live writer runs after its
    append (design detail 65f4a6c9 (1): one applier, live and rebuild). A genesis op extends at
    its journaled ts (`stamp_op_time`); a domain verb runs its registered handler inside the op's
    provenance window, so whatever it mints re-stamps to that ts. An unregistered verb raises
    LOUDLY: silently skipping an op would leave a db missing knowledge the journal holds."""
    verb = op.get("verb", "")
    if verb in (GENESIS_NODE, GENESIS_EDGE):
        wire = stamp_op_time(op["args"], op.get("ts"))
        await extend_graph(queue, graph_id, [wire] if verb == GENESIS_NODE else [],
                           [wire] if verb == GENESIS_EDGE else [])
        return verb
    if verb not in handlers:
        raise ValueError(f"apply_op: no handler for verb {verb!r} — "
                         f"refusing to silently drop journaled knowledge "
                         f"(is the core owning it installed in this env? "
                         f"composed_replay_handlers unions every installed registry)")
    token = PROVENANCE_TS.set(op.get("ts"))
    try:
        await handlers[verb](queue, graph_id, op)
    finally:
        PROVENANCE_TS.reset(token)
    return verb


async def _replay_records(
    queue: Any,         # Started job queue
    graph_id: str,      # Graph-storage capability id
    journal_path: str,  # The family the records came from (the instrument's key)
    records: List[Dict[str, Any]],  # The records to apply, in append order
    handlers: Mapping[str, Callable[..., Any]],  # Domain verb -> async handler(queue, graph_id, op)
    batch: int,         # extend_graph flush size for genesis ops
    start: int,         # The family position of the first record (the instrument's series index)
) -> Dict[str, int]:  # Genesis add/verify counts + a count per handled domain verb
    """`replay_journal`'s apply pass over records already read."""
    counts: Dict[str, int] = {"nodes_added": 0, "nodes_verified": 0,
                              "edges_added": 0, "edges_existing": 0}
    pending_nodes: List[Dict[str, Any]] = []
    pending_edges: List[Dict[str, Any]] = []

    async def flush() -> None:
        if pending_nodes or pending_edges:
            with span("genesis_flush"):
                r = await extend_graph(queue, graph_id, list(pending_nodes), list(pending_edges))
            counts["nodes_added"] += r.nodes_added
            counts["nodes_verified"] += r.nodes_verified
            counts["edges_added"] += r.edges_added
            counts["edges_existing"] += r.edges_existing
            pending_nodes.clear()
            pending_edges.clear()

    for position, op in enumerate(records, start=start):
        record_replay(journal_path, op)  # the replay's logical size (ruling 9af0e42e (3))
        verb = op.get("verb", "")
        if verb in (GENESIS_NODE, GENESIS_EDGE):
            # Replay provenance (0d50b921): stamp the op's journaled ts onto what it
            # adds — per-op HERE, not via the PROVENANCE_TS window, because a flush
            # batch spans many ops' timestamps. A carried stamp is kept, an explicit None
            # is absent (`stamp_op_time`, the function live writes share — b562d5f7);
            # pre-ts journal records stay capability now()-stamped.
            args = stamp_op_time(op["args"], op.get("ts"))
            (pending_nodes if verb == GENESIS_NODE else pending_edges).append(args)
        else:
            await flush()
            # One applier, live and rebuild (65f4a6c9 (1)): the handler runs inside the op's
            # provenance window, so whatever it mints re-stamps to op ts.
            t = time.perf_counter()
            await apply_op(queue, graph_id, op, handlers)
            # The instrument (681604c1): a verb's cost against its journal position -- flat
            # when healthy, rising when an op's cost grows with the history before it
            record_series(f"replay:{verb}", position, time.perf_counter() - t)
            counts[verb] = counts.get(verb, 0) + 1
        if len(pending_nodes) + len(pending_edges) >= batch:
            await flush()
    await flush()
    return counts


@timed("replay_journal")
async def replay_journal(
    queue: Any,         # Started job queue
    graph_id: str,      # Graph-storage capability id
    journal_path: str,  # Sidecar journal path
    handlers: Optional[Mapping[str, Callable[..., Any]]] = None,  # Domain verb -> async handler(queue, graph_id, op)
    batch: int = 500,   # extend_graph flush size for genesis ops
    offset: int = 0,    # Skip the first N ops: a delta replay of the journal's tail (idempotent)
    cursors: Optional[CursorStore] = None,  # The db's applied cursors: stamped at the replay's close (None = none kept)
) -> Dict[str, int]:  # Genesis add/verify counts + a count per handled domain verb
    """Re-apply every journaled op in append order — the db-from-journal rebuild.

    Genesis ops flush through `extend_graph` in batches, so replay is IDEMPOTENT:
    onto a fresh db everything adds; onto a live one, present nodes verify and
    collide into no-ops (the M3 convergence semantics). Domain verbs dispatch to
    their registered handler — replay stays DOMAIN-OWNED (DEC ccbab9f5 point 1) —
    and pending genesis batches flush BEFORE a handler runs, so every op sees its
    predecessors applied. An unregistered verb raises LOUDLY: silently skipping an
    op would rebuild a db missing knowledge the journal holds. Temporal provenance
    rides the replay (0d50b921): genesis args carry their op's journaled ts, and
    the PROVENANCE_TS window is open around every domain handler.

    THE APPLIED CURSOR (design detail 65f4a6c9 (2c), ruling 6683f49e (3)): the family is read
    ONCE with each record's position, and with `cursors` the replay stamps the position of the
    last record it read -- every rebuild leaves the db's cursor current, never past a record it
    did not apply. A db that already holds a cursor is one a live writer may hold, so the replay
    takes the family's lock (a fresh build holds none: writes never wait on a build), and an
    `offset` past the cursor's true place refuses -- the ops between would be skipped."""
    handlers = handlers or {}
    family = family_name(journal_path)
    held = await cursors.read(family) if cursors is not None else None
    async with (family_lock(journal_path) if held is not None else nullcontext()):
        with span("read_journal") as measured:  # the instrument (681604c1): a whole-history read
            tail = read_tail(journal_path, ORIGIN)
            measured["ops"] = len(tail)
        if held is not None and held.ordinal:
            # The cursor's TRUE place, counted off the family -- never its stored ordinal
            at = next((i + 1 for i, (_, pos) in enumerate(tail)
                       if (pos.segment, pos.offset, pos.digest) == (held.segment, held.offset, held.digest)),
                      None)
            if at is None:
                raise CursorMismatch(f"{family}: no record ends where the db's cursor stands -- the "
                                     f"family was rewritten under it; rebuild to re-stamp it")
            if at < offset:
                raise RuntimeError(f"replay offset {offset}: the db's cursor stands after record {at} "
                                   f"-- the {offset - at} op(s) between would be skipped, never applied")
        counts = await _replay_records(queue, graph_id, journal_path,
                                       [record for record, _ in tail[offset:]], handlers, batch, offset)
        if cursors is not None:
            await cursors.record(family, tail[-1][1] if tail else ORIGIN)
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


def graph_writer(
    queue: Any,         # Started job queue
    graph_id: str,      # Graph-storage capability id
    journal_path: str,  # The graph's journal family (its live tail)
    handlers: Optional[Mapping[str, Callable[..., Any]]] = None,  # Domain verb -> async handler(queue, graph_id, op); None = the workflow lane's registry
) -> JournalWriter:  # `async with` it: lock, heal, then plan and write
    """The write order on a workflow graph (design detail 65f4a6c9): a JournalWriter whose cursors
    live in the graph store beside the graph and whose applier is replay's own `apply_op`. By
    default the applier dispatches through `workflow_handlers` -- the declared registries checked
    as the writer opens (ruling 6683f49e (2)). Opened inside an open writer on the same family and
    graph it joins that writer's unit of work (6683f49e (1))."""
    handlers = workflow_handlers() if handlers is None else handlers
    return JournalWriter(journal_path, GraphCursorStore(queue, graph_id),
                         lambda op: apply_op(queue, graph_id, op, handlers))


class UnjournaledWrite(ValueError):
    """A write on a journal-sourced graph with no journal path and no explicit `unjournaled=True`
    (design detail 65f4a6c9 (3)): refused, never silently left out of the source of truth."""


def refuse_unjournaled(
    what: str,  # The write refused (its verb), named in the message
) -> UnjournaledWrite:  # The refusal, for the caller to raise
    """The one refusal of an unjournaled default (design detail 65f4a6c9 (3)): every write path of
    a journal-sourced graph raises this when it holds no journal path and no explicit
    `unjournaled=True`."""
    return UnjournaledWrite(
        f"{what}: no journal path -- a journal-sourced graph is never written unjournaled by "
        f"default; pass unjournaled=True for a scratch build or a test (design detail 65f4a6c9 (3))")


async def journal_extend(
    queue: Any,                   # Started job queue
    graph_id: str,                # Graph-storage capability id
    nodes: List[Dict[str, Any]],  # Node wire dicts to extend with
    edges: List[Dict[str, Any]],  # Edge wire dicts to extend with
    journal_path: Optional[str] = None,  # Sidecar journal -- the DELTA op is journaled, then applied (None refuses unless `unjournaled`)
    verb: str = "graph-extend",   # Domain verb for the journaled op (its replay handler is the apply)
    actor: str = "",              # Who produced the wires (e.g. "pipeline:cjm-transcription-core")
    run: Optional[str] = None,    # Run id — pins the run manifest (derivation identity, DEC ccbab9f5 pt 8)
    args: Optional[Dict[str, Any]] = None,  # Small semantic summary for the op (the dataset row)
    unjournaled: bool = False,    # Explicitly write with no journal (scratch builds, tests, ingest's own build)
    handlers: Optional[Mapping[str, Callable[..., Any]]] = None,  # Replay registry for the apply (None = the workflow lane's)
) -> ExtendResult:  # The extend result (adds + verified counts)
    """Idempotent extend + journal the DELTA — the pipeline-write append-through, on the write
    order (design detail 65f4a6c9, ruling 6683f49e (1)).

    PLANS by presence (`plan_extend`): every wire already present is no change and journals
    nothing (17daa86b) -- re-runs over cached content collide into verified no-ops and leave the
    journal untouched (the Derivation no-spam principle). Otherwise the ABSENT wires' op is
    APPENDED, then APPLIED through the replay's own handler for `verb`, and the db's cursor
    advanced -- under the family's writer, which an enclosing writer (an app's gesture window)
    is joined by. One op clock window spans the plan and the write (design 8f6f2343): the
    journaled wires carry the op's ts, exactly as the apply and a replay stamp them.

    Nothing writes a journal-sourced graph unjournaled by default (65f4a6c9 (3)): with no
    `journal_path` the call refuses unless `unjournaled=True`, which extends with no op."""
    if not journal_path:
        if not unjournaled:
            raise refuse_unjournaled(f"journal_extend ({verb})")
        with op_clock():
            return await extend_graph(queue, graph_id, nodes, edges)
    with op_clock():
        return await _journal_extend(queue, graph_id, nodes, edges, journal_path,
                                     verb, actor, run, args, handlers)


async def _journal_extend(
    queue: Any, graph_id: str, nodes: List[Dict[str, Any]], edges: List[Dict[str, Any]],
    journal_path: str, verb: str, actor: str, run: Optional[str],
    args: Optional[Dict[str, Any]], handlers: Optional[Mapping[str, Callable[..., Any]]],
) -> ExtendResult:  # The extend result (adds + verified counts)
    """`journal_extend`'s journaled body, run inside its op clock window: plan -> append ->
    apply -> advance under the family's writer (joining an enclosing one)."""
    async with graph_writer(queue, graph_id, journal_path, handlers) as w:
        plan = await plan_extend(queue, graph_id, nodes, edges)
        if plan.nodes or plan.edges:
            op: Dict[str, Any] = {"verb": verb, "actor": actor, "args": dict(args or {}),
                                  "wires": {"nodes": plan.nodes, "edges": plan.edges}}
            if run:
                op["run"] = run
            await w.write(op, dedup=False)
        return plan.result()


def write_window(
    queue: Any,                    # Started job queue
    graph_id: str,                 # Graph-storage capability id
    journal_path: Optional[str],   # The graph's journal family (None refuses unless `unjournaled`)
    *,
    unjournaled: bool = False,     # Explicitly write with no journal (scratch builds, tests)
    handlers: Optional[Mapping[str, Callable[..., Any]]] = None,  # Replay registry (None = the workflow lane's)
    what: str = "write_window",    # The verb it opens for, named in a refusal
    unit: bool = False,            # A human gesture's window: its writes land as ONE atomic unit (ruling 3de795e4)
) -> AsyncContextManager[Optional[JournalWriter]]:  # `async with` it around a verb's plan and its writes
    """A core verb's unit of work on a workflow graph (ruling 6683f49e (1)): the family's writer --
    lock, heal -- so the verb PLANS against a db that holds its whole journal, and every journaled
    write inside JOINS it (one cursor read, one heal); an enclosing window, an app's gesture, is
    joined in turn. An explicitly unjournaled verb opens no writer; with neither a journal path nor
    `unjournaled=True` the window refuses before the verb reads anything.

    With `unit=True` the window is an ATOMIC UNIT (ruling 3de795e4) -- an app's gesture, a core's
    multi-op human verb: every op written inside is buffered and the unit lands at the window's
    close, appended whole, then applied, so a crash leaves all of it or none. A plan inside reads
    the db as the window opened (an op's effect is visible to the gesture's later plans only through
    the earlier op's own plan). A unit window inside a unit joins it. Pipeline chains never open
    one: they write op by op, a retry completing them (547f4123)."""
    if journal_path:
        writer = graph_writer(queue, graph_id, journal_path, handlers)
        return _unit_window(writer) if unit else writer
    if not unjournaled:
        raise refuse_unjournaled(what)
    return nullcontext()


@asynccontextmanager
async def _unit_window(
    writer: JournalWriter,  # The family's writer (joining an open one)
) -> AsyncIterator[JournalWriter]:  # The writer, its writes buffered into one unit
    async with writer as w:
        async with w.unit():
            yield w


async def journal_op(
    queue: Any,                    # Started job queue
    graph_id: str,                 # Graph-storage capability id
    op: Dict[str, Any],            # A PLANNED op: its verb's replay handler is its apply
    journal_path: Optional[str] = None,  # The graph's journal family (None refuses unless `unjournaled`)
    *,
    unjournaled: bool = False,     # Explicitly write with no journal (scratch builds, tests)
    handlers: Optional[Mapping[str, Callable[..., Any]]] = None,  # Replay registry (None = the workflow lane's)
) -> Dict[str, Any]:  # The op as journaled (as applied, when unjournaled)
    """Journal one planned op, then apply it -- the write order for an op whose apply is its verb's
    replay handler (a correction, a curation act, a spine fact; ruling 6683f49e (1)): APPEND ->
    APPLY through the handler replay dispatches to -> ADVANCE, under the family's writer, which an
    enclosing `write_window` is joined by. A planned op is total, so its apply cannot refuse; no
    content dedup (17daa86b) -- a verb that can be a no-change write catches it at its plan. One op
    clock window spans it, so the op's ts is the time its apply stamps, live and on replay.
    Explicitly unjournaled (65f4a6c9 (3)), the same handler applies it at the window's ts and
    nothing is appended."""
    with op_clock():
        if journal_path:
            async with graph_writer(queue, graph_id, journal_path, handlers) as w:
                return await w.write(op, dedup=False)
        if not unjournaled:
            raise refuse_unjournaled(f"journal_op ({op.get('verb')})")
        applied = {**op, "ts": op.get("ts") or op_now()}
        await apply_op(queue, graph_id, applied,
                       handlers if handlers is not None else installed_handlers())
        return applied


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


class MissingRegistry(RuntimeError):
    """The env lacks a replay registry the workflow lane declares: refused when a writer opens,
    naming the core to install -- never at the first op only that core can apply."""


@functools.lru_cache(maxsize=None)
def installed_handlers() -> Mapping[str, Any]:  # verb -> async handler(queue, graph_id, op), read-only
    """Every installed core's replay registry, composed once per process (read-only). The applier of
    an explicitly unjournaled write, which never heals and never rebuilds, so it needs only the core
    owning its own verb; `workflow_handlers` is this union checked against the declared registries."""
    return MappingProxyType(composed_replay_handlers())


@functools.lru_cache(maxsize=None)
def workflow_handlers() -> Mapping[str, Any]:  # verb -> async handler(queue, graph_id, op), read-only
    """The workflow lane's replay registry (ruling 6683f49e (2)): the composed union, checked
    against the DECLARED registries the shared workflow journal needs (`WORKFLOW_REGISTRIES`) and
    cached per process -- a writer opens per write, the entry-point scan runs once. A missing
    registry refuses here, naming the core to install: never later, at the first op only that
    core can apply (a heal after a crash), and never as an env that silently cannot rebuild.
    Read-only, since one mapping serves every writer of the process."""
    installed = {ep.name for ep in entry_points(group=REPLAY_GROUP)}
    missing = [f"{name} ({dist})" for name, dist in WORKFLOW_REGISTRIES.items()
               if name not in installed]
    if missing:
        raise MissingRegistry(
            f"workflow_handlers: this env lacks the workflow lane's replay registr"
            f"{'y' if len(missing) == 1 else 'ies'} {', '.join(missing)} -- install the core into "
            f"it (an env per role, ruling 80685121): a writer here could neither heal nor rebuild "
            f"the shared workflow journal")
    return installed_handlers()
