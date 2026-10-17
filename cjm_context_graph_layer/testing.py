"""The write order's test kit, shared by every workflow core's suite.

`MemGraph` -- an in-memory graph store -- stands behind a fake job queue and answers the storage
methods the write order and the cores' appliers call: the presence reads `plan_extend` makes, adds,
property merges, deletes and the journal cursors a `JournalWriter` keeps beside the graph. A test
passes it where a started queue goes; `state()` is what a live-vs-replay comparison reads. Any other
method raises, so a write path that grows a new storage call fails loudly here instead of passing
over a silent None.

THE PER-VERB CRASH PROOF (eeda5977 L3 slice E3, rulings 6683f49e + 3de795e4) -- `ProofStep`,
`run_proof`, `crash_variants`: each core declares a scenario of its own verbs; the harness runs
every step in a CHILD PROCESS on a real graph store and kills it by the armed crash point (a real
death, no cleanup) -- a unit torn mid-append, after an append, after an apply, at each op boundary.
A human step is then HEALED by the next writer (a torn unit, which landed nothing, is the user's
redo: re-run); a pipeline step is RE-RUN, its plan finding what the crash left (547f4123). Each
run's live db must equal its journal's replay element for element (`diff_dbs`, f8b9c728), and a
crashed run's journal must equal the uncrashed run's, op for op. A step finds what an earlier step
minted in the JOURNAL (`ProofContext.record`): each step's records carry its name as their session.
"""

import asyncio
import copy
import importlib
import inspect
import json
import os
import re
import signal
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence, Tuple

from cjm_substrate.core.queue import JobStatus


class MemGraph:
    """An in-memory graph store behind a fake queue: the storage methods the write order uses."""

    def __init__(self):
        self.nodes: Dict[str, Dict[str, Any]] = {}    # id -> node wire, as stored
        self.edges: Dict[str, Dict[str, Any]] = {}    # id -> edge wire, as stored
        self.cursors: Dict[str, Dict[str, Any]] = {}  # family -> applied cursor (its wire form)
        self.methods: List[str] = []                  # Every storage method called, in order

    async def submit(self, graph_id, task=None, method=None, **kw):
        self.methods.append(method)
        self._result = self._run(method, kw)
        return "j"

    async def wait_for_job(self, jid, timeout=None):
        return SimpleNamespace(status=JobStatus.completed, result=self._result, error=None)

    def _run(self, method, kw):
        if method == "query_nodes":
            q = kw["query"]
            ids = q.get("ids")
            rows = [self.nodes[i] for i in ids if i in self.nodes] if ids is not None else list(self.nodes.values())
            if q.get("label"):
                rows = [n for n in rows if n.get("label") == q["label"]]
            return SimpleNamespace(nodes=[SimpleNamespace(**n) for n in rows])
        if method == "query_edges":
            return SimpleNamespace(rows=[{"id": i} for i in kw["query"].get("ids") or []
                                         if i in self.edges])
        if method == "get_node":
            n = self.nodes.get(kw["node_id"])
            return copy.deepcopy(n) if n is not None else None
        if method in ("add_nodes", "add_edges"):
            into = self.nodes if method == "add_nodes" else self.edges
            items = kw["nodes"] if method == "add_nodes" else kw["edges"]
            for w in items:
                into[w["id"]] = {**copy.deepcopy(w), "sources": w.get("sources") or []}
            return [w["id"] for w in items]
        if method == "update_node":
            n = self.nodes.get(kw["node_id"])
            if n is None:
                return False
            n["properties"] = {**(n.get("properties") or {}), **copy.deepcopy(kw["properties"])}
            return True
        if method == "delete_nodes":
            gone = [i for i in kw["node_ids"] if self.nodes.pop(i, None) is not None]
            for eid in [e for e, w in self.edges.items()
                        if w["source_id"] in gone or w["target_id"] in gone]:
                del self.edges[eid]
            return len(gone)
        if method == "delete_edges":
            return len([i for i in kw["edge_ids"] if self.edges.pop(i, None) is not None])
        if method == "journal_cursors":
            return dict(self.cursors)
        if method == "record_journal_cursor":
            self.cursors[kw["family"]] = kw["position"]
            return True
        raise AssertionError(f"MemGraph: unexpected method {method}")

    def state(self) -> Tuple[Dict[str, Any], Dict[str, Any]]:  # What a comparison reads: every node and edge, as stored
        return self.nodes, self.edges


# ---- the per-verb crash proof -------------------------------------------------------------------

PROOF_SESSION = "proof:"  # A step's records carry `proof:<step name>` as their session stamp
TIME_KEYS = frozenset({"ts", "created_at", "updated_at", "started_at", "asserted_at", "observed_at",
                       "retired_ts", "compacted_at"})  # Fields a run's own clock writes
_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


@dataclass(frozen=True)
class ProofStep:
    """One step of a core's crash-proof scenario: an async `call(ctx)` -- module-level, so a child
    process imports it -- that writes through the core's verbs on `ctx`'s store and journal."""

    name: str                # Unique in the scenario; the session its records carry
    call: Callable[["ProofContext"], Awaitable[Any]]  # The verb call, run in a child process
    ops: Tuple[str, ...]     # The verbs it journals, in order -- the proof asserts exactly these
    unit: bool = False       # Its ops land as ONE atomic unit (a human gesture, ruling 3de795e4)
    retried: bool = False    # A pipeline step: re-run after any crash (it plans by presence, 547f4123)

    @property
    def appends(self) -> int:  # How many appends it makes (one per unit, else one per op)
        return 1 if self.unit else len(self.ops)


@dataclass
class ProofContext:
    """What a step's call gets: the started store, the run's journal, and the run's earlier records."""

    queue: Any               # Started job queue on the run's live db
    graph_id: str            # Graph-storage capability id
    journal: str             # The run's writes journal (the db's sidecar)
    run_dir: Path            # The run's directory (scratch files a step needs: archives, inputs)

    def records(self, step: str, verb: Optional[str] = None) -> List[Dict[str, Any]]:  # A step's journaled records
        from cjm_context_graph_primitives.journal import read_journal
        return [r for r in read_journal(self.journal) if r.get("session") == PROOF_SESSION + step
                and (verb is None or r.get("verb") == verb)]

    def record(self, step: str, verb: Optional[str] = None) -> Dict[str, Any]:  # A step's first record (of `verb`)
        found = self.records(step, verb)
        if not found:
            raise LookupError(f"proof: step {step!r} journaled no {verb or 'record'}")
        return found[0]


def crash_variants(
    steps: Sequence[ProofStep],
) -> List[Tuple[str, int]]:  # (crash point, arrival) per crashed run: every op boundary the steps hold
    """Every boundary a crash can fall on, as the runs that crash each step there: a unit torn
    mid-append (a step whose unit holds two ops or more), after its k-th append, after its k-th
    apply. A step with fewer boundaries than k is crashed at its last."""
    torn = [("torn_append", 1)] if any(s.unit and len(s.ops) > 1 for s in steps) else []
    appends = max([s.appends for s in steps] + [1])
    applies = max([len(s.ops) for s in steps] + [1])
    return (torn + [("after_append", k) for k in range(1, appends + 1)]
            + [("after_apply", k) for k in range(1, applies + 1)])


def _crash_of(
    step: ProofStep,
    variant: Optional[Tuple[str, int]],
) -> Optional[Tuple[str, int]]:  # The fault this step takes in that run, or None
    if variant is None or not step.ops:
        return None
    point, k = variant
    if point == "torn_append":
        return (point, 1) if step.unit and len(step.ops) > 1 else None
    return point, min(k, step.appends if point == "after_append" else len(step.ops))


def _reap(pid: int) -> None:
    """A crashed host's capability worker outlives it (finding f1734c73): stop every process whose
    command line names the dead host as its --ppid."""
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue
        try:
            argv = (proc / "cmdline").read_bytes().split(b"\0")
        except OSError:
            continue
        if b"--ppid" in argv and argv[argv.index(b"--ppid") + 1:argv.index(b"--ppid") + 2] == [str(pid).encode()]:
            try:
                os.kill(int(proc.name), signal.SIGTERM)
            except ProcessLookupError:
                pass


def _child(
    run_dir: Path, manifests: str, step: ProofStep,
    fault: Optional[Tuple[str, int]] = None,
    heal: bool = False,  # Open the family's writer and close it: the next writer's heal, nothing else
) -> Dict[str, Any]:  # {"returncode", "out", "err", "healed", "sealed"}
    from cjm_context_graph_primitives.writer import JOURNAL_FAULT_ENV
    fn = step.call
    env = {k: v for k, v in os.environ.items() if k != JOURNAL_FAULT_ENV}
    env.update(CJM_SESSION=PROOF_SESSION + step.name, CJM_WORKSPACE=str(run_dir))
    if fault:
        env[JOURNAL_FAULT_ENV] = f"{fault[0]}@{fault[1]}"
    argv = [sys.executable, "-m", "cjm_context_graph_layer.testing", str(run_dir), manifests,
            fn.__module__, fn.__qualname__, str(Path(inspect.getfile(fn)).parent), "heal" if heal else "run"]
    proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
    out, err = proc.communicate(timeout=600)
    if fault:
        _reap(proc.pid)
    res: Dict[str, Any] = {"returncode": proc.returncode, "out": out, "err": err}
    for line in out.splitlines():
        if line.startswith("PROOF "):
            res.update(json.loads(line[len("PROOF "):]))
    return res


def _normalized(
    journal: str,  # A run's writes journal
    run_dir: Path,
) -> List[Any]:  # Its records with the run's clock, directory, unit ids and minted ids abstracted
    from cjm_context_graph_primitives.journal import read_journal, UNIT

    def scrub(v):
        if isinstance(v, dict):
            return {k: ("{t}" if k in TIME_KEYS else scrub(x)) for k, x in v.items() if k != UNIT}
        if isinstance(v, list):
            return [scrub(x) for x in v]
        return v

    text = json.dumps([scrub(r) for r in read_journal(journal)], sort_keys=True).replace(str(run_dir), "{run}")
    seen: Dict[str, str] = {}
    return json.loads(_UUID.sub(lambda m: seen.setdefault(m.group(0), f"{{id{len(seen)}}}"), text))


def run_proof(
    run_dir: Path,                            # A fresh directory for this run
    manifests: str,                           # Capability manifests directory (the graph capability)
    steps: Sequence[ProofStep],               # The scenario, in order
    variant: Optional[Tuple[str, int]] = None,  # Crash every step at this boundary (None = uncrashed)
) -> Dict[str, Any]:  # {"journal": normalized records, "counts": per-step op counts, "diff": diff_dbs report}
    """Run a scenario once, every step in a child process, each crashed at `variant` and then healed
    or re-run; then rebuild the journal into a fresh db and diff the live db against it element for
    element. Raises AssertionError on any broken contract."""
    from cjm_context_graph_layer.rebuild import diff_dbs, rebuild_db
    from cjm_context_graph_primitives.journal import read_journal
    from cjm_context_graph_primitives.writer import FAULT_EXIT

    for step in steps:  # a human write that is not one unit could be half-landed by a crash
        assert step.unit or step.retried or len(step.ops) <= 1, (
            f"{step.name}: a step journaling several ops is a unit (a human gesture) or retried (a pipeline)")
    run_dir.mkdir(parents=True, exist_ok=True)
    journal = str(run_dir / "live.writes.jsonl")
    counts: List[int] = []
    for step in steps:
        fault = _crash_of(step, variant)
        if fault:
            r = _child(run_dir, manifests, step, fault=fault)
            assert r["returncode"] == FAULT_EXIT, (
                f"{step.name}: no crash at {fault} ({r['returncode']}): {r['err'][-2000:] or r['out'][-2000:]}")
            if step.retried or fault[0] == "torn_append":
                r = _child(run_dir, manifests, step)
                assert r["returncode"] == 0, f"{step.name} (re-run): {r['err'][-3000:]}"
                if fault[0] == "torn_append":
                    assert r.get("sealed", 0) > 0, f"{step.name}: the torn unit was not sealed off"
            else:
                r = _child(run_dir, manifests, step, heal=True)
                assert r["returncode"] == 0, f"{step.name} (heal): {r['err'][-3000:]}"
                want = (len(step.ops) if fault[0] == "after_append" and step.unit else
                        len(step.ops) - fault[1] + 1 if step.unit else 1)
                assert r.get("healed") == want, f"{step.name}: healed {r.get('healed')}, not {want}"
        else:
            r = _child(run_dir, manifests, step)
            assert r["returncode"] == 0, f"{step.name}: {r['err'][-3000:]}"
        # a step's records by their session stamp, never a count: a compaction moves earlier ones out
        new = [o for o in read_journal(journal) if o.get("session") == PROOF_SESSION + step.name]
        assert [o["verb"] for o in new] == list(step.ops), f"{step.name} journaled {[o['verb'] for o in new]}, not {list(step.ops)}"
        if step.unit and len(step.ops) > 1:
            assert len({o.get("unit") for o in new}) == 1 and None not in {o.get("unit") for o in new}, (
                f"{step.name}: its ops are not one unit")
        counts.append(len(new))
    rebuilt = str(run_dir / "rebuilt.db")
    asyncio.run(rebuild_db(rebuilt, journal, manifests))
    report = asyncio.run(diff_dbs(str(run_dir / "live.db"), rebuilt, manifests))
    assert report["clean"], f"live != its replay: {json.dumps(report, default=str)[:4000]}"
    return {"journal": _normalized(journal, run_dir), "counts": counts, "diff": report}


async def _child_main(argv: List[str]) -> Dict[str, Any]:  # The child's PROOF line
    run_dir, manifests, module, qualname, tests_dir, mode = argv
    from cjm_context_graph_layer.journal import graph_writer
    from cjm_context_graph_layer.rebuild import open_store
    from cjm_context_graph_primitives.journal import committed_length
    sys.path.insert(0, tests_dir)
    fn: Any = importlib.import_module(module)
    for part in qualname.split("."):
        fn = getattr(fn, part)
    rd = Path(run_dir)
    journal = str(rd / "live.writes.jsonl")
    async with open_store(str(rd / "live.db"), manifests) as g:
        ctx = ProofContext(g.queue, g.graph_id, journal, rd)
        if mode == "heal":
            async with graph_writer(g.queue, g.graph_id, journal) as w:
                return {"healed": w.healed, "sealed": w.sealed}
        torn = (os.path.getsize(journal) - committed_length(journal)) if Path(journal).exists() else 0
        await fn(ctx)  # its first writer seals the torn end off before anything else
        assert committed_length(journal) == os.path.getsize(journal), "the step left a torn end"
        return {"sealed": torn}


if __name__ == "__main__":
    print("PROOF " + json.dumps(asyncio.run(_child_main(sys.argv[1:]))), flush=True)
