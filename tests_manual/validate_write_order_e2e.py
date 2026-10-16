#!/usr/bin/env python
"""The write order's end-to-end proof (design detail 65f4a6c9, build c4e2e2a2).

A real crash between append and apply on a real SQLite graph store, healed by the next
process, must equal the uncrashed run ELEMENT FOR ELEMENT:
- db A takes three wire-carrying ops through `graph_writer`, uncrashed;
- db B takes op-0, then a child process armed with CJM_JOURNAL_FAULT=after_append dies
  (exit 86) with op-1 journaled and unapplied; the next process heals it and writes op-2;
- the two journals, `diff_dbs` over the two dbs, and the two applied cursors must agree.

The crashed child's capability worker is reaped by its recorded --ppid afterwards, and
counted: a worker that outlives a host that died hard is finding f1734c73.

Usage (any env holding the layer + the graph capability's manifest):
    python tests_manual/validate_write_order_e2e.py <manifests_dir> <scratch_dir>
Exit 0 = clean, 1 = a difference (printed).
"""

import asyncio
import os
import subprocess
import sys
from pathlib import Path

import psutil
from cjm_context_graph_layer.journal import graph_writer, wires_handlers
from cjm_context_graph_layer.ops import GraphCursorStore
from cjm_context_graph_layer.rebuild import diff_dbs, open_store
from cjm_context_graph_primitives.journal import read_journal
from cjm_context_graph_primitives.writer import FAULT_EXIT, JOURNAL_FAULT_ENV, stamp_cursor

HANDLERS = wires_handlers("put-wires")


def op(i):  # One wire-carrying op at a fixed ts, so both runs stamp the same times
    nodes = [{"id": f"n{i}", "label": "Probe", "properties": {"i": i}}]
    edges = ([{"id": f"e{i}", "source_id": f"n{i - 1}", "target_id": f"n{i}",
               "relation_type": "NEXT", "properties": {}}] if i else [])
    return {"verb": "put-wires", "id": f"op-{i}", "ts": 1000.0 + i, "args": {"i": i},
            "wires": {"nodes": nodes, "edges": edges}}


async def write(manifests, db, journal, ids, stamp=False):  # Records the last writer's heal count
    async with open_store(db, manifests) as g:
        if stamp:
            await stamp_cursor(journal, GraphCursorStore(g.queue, g.graph_id))
        healed = 0
        for i in ids:
            async with graph_writer(g.queue, g.graph_id, journal, HANDLERS) as w:
                healed = w.healed
                await w.write(op(i))
        return healed


async def cursor(manifests, db, family):  # A db's applied cursor for one family
    async with open_store(db, manifests, readonly=True) as g:
        return await GraphCursorStore(g.queue, g.graph_id).read(family)


def reap_orphans(child_pid):  # Workers still alive whose recorded --ppid is the dead child
    found = 0
    for proc in psutil.process_iter(["pid", "cmdline"]):
        cmd = proc.info["cmdline"] or []
        if "--ppid" in cmd and cmd[cmd.index("--ppid") + 1:cmd.index("--ppid") + 2] == [str(child_pid)]:
            found += 1
            proc.terminate()
            try:
                proc.wait(10)
            except psutil.TimeoutExpired:
                proc.kill()
    return found


def main(argv):
    if argv[0] == "child":  # op-0, then op-1 with the crash armed after its append
        manifests, tmp = argv[1], Path(argv[2])
        asyncio.run(write(manifests, str(tmp / "b.db"), str(tmp / "b.writes.jsonl"), [0], stamp=True))
        os.environ[JOURNAL_FAULT_ENV] = "after_append"
        asyncio.run(write(manifests, str(tmp / "b.db"), str(tmp / "b.writes.jsonl"), [1]))
        return 2  # unreachable when the crash point fires
    manifests, tmp = argv[0], Path(argv[1])
    tmp.mkdir(parents=True, exist_ok=True)
    for f in tmp.iterdir():
        if f.is_file():
            f.unlink()
    a_db, a_j, b_db, b_j = (str(tmp / n) for n in ("a.db", "a.writes.jsonl", "b.db", "b.writes.jsonl"))
    asyncio.run(write(manifests, a_db, a_j, [0, 1, 2], stamp=True))
    child = subprocess.Popen([sys.executable, __file__, "child", manifests, str(tmp)])
    rc = child.wait()
    orphans = reap_orphans(child.pid)
    crashed = [r["id"] for r in read_journal(b_j)]
    healed = asyncio.run(write(manifests, b_db, b_j, [2]))
    strip = lambda rs: [{k: v for k, v in r.items() if k != "session"} for r in rs]
    journals = strip(read_journal(a_j)) == strip(read_journal(b_j))
    report = asyncio.run(diff_dbs(a_db, b_db, manifests))
    ca, cb = (asyncio.run(cursor(manifests, db, f)) for db, f in
              ((a_db, "a.writes.jsonl"), (b_db, "b.writes.jsonl")))
    same_cursor = (ca.segment, ca.offset, ca.ordinal, ca.digest) == (cb.segment, cb.offset, cb.ordinal, cb.digest)
    print(f"child exit {rc} (armed: {FAULT_EXIT}) · journal after the crash {crashed} · "
          f"orphaned workers reaped {orphans}")
    print(f"healed {healed} · journals equal {journals} · diff clean {report['clean']} "
          f"(nodes {report['nodes']['count_a']}/{report['nodes']['count_b']}, "
          f"edges {report['edges']['count_a']}/{report['edges']['count_b']}) · cursors equal {same_cursor}")
    ok = (rc == FAULT_EXIT and crashed == ["op-0", "op-1"] and healed == 1 and journals
          and report["clean"] and same_cursor)
    print("CLEAN" if ok else "DIFFERENCE")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
