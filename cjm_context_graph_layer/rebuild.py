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
"""

import argparse
import asyncio
from pathlib import Path
from typing import Any, Dict, List, Optional

from cjm_context_graph_primitives.journal import journal_segments
from cjm_substrate.core.manager import CapabilityManager
from cjm_substrate.core.queue import JobQueue

from .journal import composed_replay_handlers, replay_journal


async def rebuild_db(
    db_path: str,           # TARGET graph db path (fresh, or existing when healing)
    journal_path: str,      # The journal's live-tail path (segment family resolved from it)
    manifests_dir: str,     # Capability manifests directory (workspace or explicit)
    graph_capability: str = "cjm-capability-graph-sqlite",  # Graph-storage capability name
    handlers: Optional[Dict[str, Any]] = None,  # Override registry (None = composed union)
) -> Dict[str, int]:  # replay_journal's counts: genesis adds/verifies + per-verb tallies
    """Rebuild `db_path` from the journal — substrate standup + composed replay.

    Mirrors the cores' own runtime pattern (manager -> graph capability with a
    `db_path` load-config override -> started queue) so the rebuild writes through
    the SAME worker path every live write used. Idempotent end to end: genesis
    ops verify-collide, wire ops re-extend, `session-status` replays last-wins."""
    manager = CapabilityManager(search_paths=[Path(manifests_dir)])
    manager.discover_manifests()
    discovered = {m.name: m for m in manager.discovered}
    meta = discovered.get(graph_capability)
    if meta is None:
        raise ValueError(f"rebuild_db: capability {graph_capability!r} not found in "
                         f"{manifests_dir} (discovered: {sorted(discovered)})")
    if not manager.load_capability(meta, config={"db_path": db_path}):
        raise ValueError(f"rebuild_db: failed to load capability {graph_capability!r}")
    queue = JobQueue(deps=manager)
    await queue.start()
    try:
        return await replay_journal(queue, graph_capability, journal_path,
                                    handlers=handlers if handlers is not None
                                    else composed_replay_handlers())
    finally:
        await queue.stop()
        manager.unload_capability(graph_capability)


def main(argv: Optional[List[str]] = None) -> int:  # Process exit code
    """The `cjm-workflow-rebuild` console entrypoint — one command, fresh db from journal.

    Every path is REQUIRED-EXPLICIT (recovery tools get no ambient defaults);
    an existing `--db` target refuses without `--onto-existing`."""
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
    return 0
