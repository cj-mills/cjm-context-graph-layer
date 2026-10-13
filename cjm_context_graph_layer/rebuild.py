"""Rebuild a workflow graph db from its journal — the runnable disaster-recovery driver.

The `cjm-workflow-rebuild` console entrypoint (DEC 426658f1, executes finding
d066826a): before this, NO runnable path could rebuild a workflow db from the
shared journal — each core's verb registry lived in its own package. The driver
stands up the substrate runtime (CapabilityManager + JobQueue) with the graph
capability pointed at the TARGET db, composes every installed core's registry
via `composed_replay_handlers`, and replays the journal's segment family.
Disaster-recovery contract: target a FRESH db path (an existing target refuses
without `--onto-existing`; replay is idempotent, so healing a live db is legal
but opt-in). Paths are REQUIRED-EXPLICIT — no ambient defaults in a recovery
tool (the explicit-graph-db-path guardrail).

The proof is the layer's streamed comparison (`graphdiff`, ruling f8b9c728):
`--against LIVE` compares the rebuild with the live db element for element —
ids, every property, sources, both time columns — and `cjm-workflow-diff`
compares any two workflow dbs. The workflow lane's known cause is DATA here
(`WORKFLOW_ATTRIBUTION`); a difference nothing accounts for fails the proof.
"""

import argparse
import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional, Sequence

from cjm_context_graph_primitives.journal import journal_segments
from cjm_substrate.core.manager import CapabilityManager
from cjm_substrate.core.queue import JobQueue

from .graphdiff import (Attribution, diff_stores, LagAttribution, MarkAttribution, PAGE,
                        render_diff, STREAM_HISTORY)
from .journal import composed_replay_handlers, replay_journal

# The op clock's landing in the workflow writers (build 78a60b1a, design 8f6f2343): the
# correction, decomp, transcription cores and this layer took it in one commit each at
# 2026-09-29 20:03:29 -0700. Before it a live write stamped its own clock and the op's ts
# was read a moment AFTER the write, so the history's live times precede the journaled ones
# by the write's duration (median 0.25 s, max 3.67 s measured, finding b562d5f7 (3); bound 5 s,
# ruling 0669404b (1)); after it they are equal. Before graph-sqlite 7417e3c (2026-08-05; a worker
# ran it until at least 2026-08-28) update_node also kept the reserved updated_at key in the
# properties blob: live holds the copy of its own column, the replay the birth value (ruling
# 0669404b (2), 131 CorrectionSession nodes).
OP_CLOCK_EPOCH = 1790737409.0
WORKFLOW_ATTRIBUTION = (
    LagAttribution("pre-op-clock", max_lag=5.0, before=OP_CLOCK_EPOCH),
    MarkAttribution("reserved key in blob", field="properties.updated_at", column="updated_at",
                    within=1.0),
)


@dataclass
class GraphStore:
    """A started graph: the queue + the capability id to address it."""

    queue: JobQueue   # Started job queue (the task channel)
    graph_id: str     # Graph-storage capability instance id


@asynccontextmanager
async def open_store(
    db_path: str,          # The graph db (must exist when `readonly`)
    manifests_dir: str,    # Capability manifests directory (workspace or explicit)
    graph_capability: str = "cjm-capability-graph-sqlite",  # Graph-storage capability name
    readonly: bool = False,  # Open the db read-only (URI mode=ro: any write raises at SQLite)
    max_history: int = 100,  # Completed jobs the queue retains, results included
) -> AsyncIterator[GraphStore]:  # The started store
    """Stand up the graph capability on `db_path` and a started queue — the cores' own runtime
    pattern, so a rebuild writes through the SAME worker path every live write used. Graph ops
    are ambient work (DEC 8bf656c0): their per-op accounting stays out of the substrate journal."""
    manager = CapabilityManager(search_paths=[Path(manifests_dir)])
    manager.discover_manifests()
    discovered = {m.name: m for m in manager.discovered}
    meta = discovered.get(graph_capability)
    if meta is None:
        raise ValueError(f"open_store: capability {graph_capability!r} not found in "
                         f"{manifests_dir} (discovered: {sorted(discovered)})")
    config: Dict[str, Any] = {"db_path": db_path}
    if readonly:
        if not Path(db_path).exists():
            raise ValueError(f"open_store: a read-only graph must exist: {db_path}")
        config["readonly"] = True
    if not manager.load_capability(meta, config=config):
        raise ValueError(f"open_store: failed to load capability {graph_capability!r}")
    if hasattr(manager, "set_observability_class"):
        manager.set_observability_class(graph_capability, "ambient")
    queue = JobQueue(deps=manager, max_history=max_history)
    await queue.start()
    try:
        yield GraphStore(queue=queue, graph_id=graph_capability)
    finally:
        await queue.stop()
        manager.unload_capability(graph_capability)


async def rebuild_db(
    db_path: str,           # TARGET graph db path (fresh, or existing when healing)
    journal_path: str,      # The journal's live-tail path (segment family resolved from it)
    manifests_dir: str,     # Capability manifests directory (workspace or explicit)
    graph_capability: str = "cjm-capability-graph-sqlite",  # Graph-storage capability name
    handlers: Optional[Dict[str, Any]] = None,  # Override registry (None = composed union)
) -> Dict[str, int]:  # replay_journal's counts: genesis adds/verifies + per-verb tallies
    """Rebuild `db_path` from the journal — substrate standup + composed replay.

    Idempotent end to end: genesis ops verify-collide, wire ops re-extend,
    `session-status` replays last-wins."""
    async with open_store(db_path, manifests_dir, graph_capability) as gx:
        return await replay_journal(gx.queue, gx.graph_id, journal_path,
                                    handlers=handlers if handlers is not None
                                    else composed_replay_handlers())


async def diff_dbs(
    db_a: str,           # Graph A (by convention the LIVE db), opened read-only
    db_b: str,           # Graph B (by convention its REBUILD), opened read-only
    manifests_dir: str,  # Capability manifests directory
    graph_capability: str = "cjm-capability-graph-sqlite",  # Graph-storage capability name
    attribution: Sequence[Attribution] = WORKFLOW_ATTRIBUTION,  # The lane's known causes
    page: int = PAGE,    # Elements per keyset page
) -> Dict[str, Any]:  # The graphdiff report + the two paths
    """Compare two workflow dbs element for element, both read-only, streamed side by side."""
    async with open_store(db_a, manifests_dir, graph_capability, readonly=True,
                          max_history=STREAM_HISTORY) as ga, \
            open_store(db_b, manifests_dir, graph_capability, readonly=True,
                       max_history=STREAM_HISTORY) as gb:
        res = await diff_stores(ga, gb, attribution=attribution, page=page)
    return {**res, "a": db_a, "b": db_b}


def main(argv: Optional[List[str]] = None) -> int:  # Process exit code (3 = the proof found differences)
    """The `cjm-workflow-rebuild` console entrypoint — one command, fresh db from journal.

    Every path is REQUIRED-EXPLICIT (recovery tools get no ambient defaults);
    an existing `--db` target refuses without `--onto-existing`. `--against LIVE`
    proves the rebuild against the live db once it is built."""
    parser = argparse.ArgumentParser(
        prog="cjm-workflow-rebuild",
        description="Rebuild a workflow graph db from its journal (segment-family aware).")
    parser.add_argument("--db", required=True, help="TARGET db path (fresh; see --onto-existing)")
    parser.add_argument("--journal", required=True,
                        help="Journal live-tail path (rotated segments resolved automatically)")
    parser.add_argument("--manifests-dir", required=True, help="Capability manifests directory")
    parser.add_argument("--graph-capability", default="cjm-capability-graph-sqlite",
                        help="Graph-storage capability name (default: %(default)s)")
    parser.add_argument("--onto-existing", action="store_true",
                        help="Allow an EXISTING --db target (idempotent replay heals it)")
    parser.add_argument("--against", default=None, metavar="LIVE_DB",
                        help="After the rebuild, compare it with LIVE_DB element for element "
                             "(read-only); exit 3 on any unattributed difference")
    args = parser.parse_args(argv)
    if Path(args.db).exists() and not args.onto_existing:
        parser.error(f"target db exists: {args.db} — rebuild targets a FRESH path "
                     f"(pass --onto-existing to heal a live db; replay is idempotent)")
    segments = journal_segments(args.journal)
    if not segments:
        parser.error(f"no journal found at {args.journal} (no live tail, no rotated segments)")
    print(f"rebuilding {args.db} from {len(segments)} journal segment(s)")
    counts = asyncio.run(rebuild_db(args.db, args.journal, args.manifests_dir,
                                    graph_capability=args.graph_capability))
    for key in sorted(counts):
        print(f"  {key}: {counts[key]}")
    if args.against is None:
        return 0
    res = asyncio.run(diff_dbs(args.against, args.db, args.manifests_dir,
                               graph_capability=args.graph_capability))
    print(render_diff(res))
    return 0 if res["clean"] else 3


def diff_main(argv: Optional[List[str]] = None) -> int:  # Process exit code (3 = differences)
    """The `cjm-workflow-diff` console entrypoint — two existing workflow dbs, compared
    element for element with the workflow lane's attribution (both opened read-only)."""
    parser = argparse.ArgumentParser(
        prog="cjm-workflow-diff",
        description="Compare two workflow graph dbs element for element (the rebuild proof).")
    parser.add_argument("a", help="Graph A (by convention the LIVE db)")
    parser.add_argument("b", help="Graph B (by convention its REBUILD)")
    parser.add_argument("--manifests-dir", required=True, help="Capability manifests directory")
    parser.add_argument("--graph-capability", default="cjm-capability-graph-sqlite",
                        help="Graph-storage capability name (default: %(default)s)")
    parser.add_argument("--page", type=int, default=PAGE, help="Elements per keyset page")
    parser.add_argument("--format", choices=("human", "agent"), default="human")
    args = parser.parse_args(argv)
    res = asyncio.run(diff_dbs(args.a, args.b, args.manifests_dir,
                               graph_capability=args.graph_capability, page=args.page))
    print(render_diff(res, args.format))
    return 0 if res["clean"] else 3
