#!/usr/bin/env python
"""The adoption and the op-first compaction, end to end on a real SQLite graph store (ruling
6683f49e (3)-(4), eeda5977 leg L3 slice E1).

- db L takes three wire-carrying ops through `graph_writer`, then an UNJOURNALED residue (a node
  and an edge) -- the 31c22bf8 class;
- `adopt_db` refuses, listing exactly the two live-only ids; a drop list naming fewer refuses; the
  exact list drops them and stamps the replayed end, and a fresh rebuild then equals L element
  for element;
- a field changed on L outside the journal makes adoption refuse outright (not the projection);
- `compact_family` journals a drop op before moving n0's wires to the archive; a fresh rebuild of
  the compacted family equals L element for element, and L's cursor stands at the family's end.

Usage (any env holding the layer + the graph capability's manifest):
    python tests_manual/validate_adopt_e2e.py <manifests_dir> <scratch_dir>
Exit 0 = clean, 1 = a failed expectation (printed).
"""

import asyncio
import sys
from pathlib import Path

from cjm_context_graph_layer.compact import compact_family, scan_references
from cjm_context_graph_layer.journal import graph_writer, wires_handlers
from cjm_context_graph_layer.ops import extend_graph, graph_task, GraphCursorStore
from cjm_context_graph_layer.rebuild import adopt_db, AdoptRefusal, diff_dbs, open_store, rebuild_db
from cjm_context_graph_primitives.writer import family_name, ORIGIN, read_tail, stamp_cursor


async def drop(queue, graph_id, op):  # A compaction op's apply: delete what moved
    await graph_task(queue, graph_id, "delete_nodes", node_ids=op["deletes"], cascade=True)


HANDLERS = {**wires_handlers("put-wires"), "drop": drop}


def op(i):  # One wire-carrying op at a fixed ts
    nodes = [{"id": f"n{i}", "label": "Probe", "properties": {"i": i}}]
    edges = ([{"id": f"e{i}", "source_id": f"n{i - 1}", "target_id": f"n{i}",
               "relation_type": "NEXT", "properties": {}}] if i else [])
    return {"verb": "put-wires", "id": f"op-{i}", "ts": 1000.0 + i, "args": {"i": i},
            "wires": {"nodes": nodes, "edges": edges}}


async def build(manifests, db, journal):
    async with open_store(db, manifests) as g:
        await stamp_cursor(journal, GraphCursorStore(g.queue, g.graph_id))
        for i in range(3):
            async with graph_writer(g.queue, g.graph_id, journal, HANDLERS) as w:
                await w.write(op(i))
        await extend_graph(g.queue, g.graph_id,  # the unjournaled residue
                           [{"id": "r1", "label": "Residue", "properties": {}}],
                           [{"id": "er1", "source_id": "n0", "target_id": "r1",
                             "relation_type": "SMOKE", "properties": {}}])


async def adopt(manifests, db, journal, scratch, drop_ids=None):
    try:
        return await adopt_db(db, journal, manifests, scratch, drop=drop_ids, handlers=HANDLERS)
    except AdoptRefusal as e:
        return e


async def touch(manifests, db):  # A field changed outside the journal
    async with open_store(db, manifests) as g:
        await graph_task(g.queue, g.graph_id, "update_node", node_id="n1", properties={"i": 99})


async def compact(manifests, db, journal, archive):
    async with open_store(db, manifests) as g:
        async with graph_writer(g.queue, g.graph_id, journal, HANDLERS) as w:
            rep = await compact_family(
                w, archive, ["n0"],
                lambda plan: [{"verb": "drop", "args": {"archive": plan.archive_path},
                               "deletes": ["n0"]}],
                label="e2e", actor="validate_adopt_e2e")
        cursor = await GraphCursorStore(g.queue, g.graph_id).read(family_name(journal))
    return rep, cursor


def main(manifests: str, scratch_dir: str) -> int:
    d = Path(scratch_dir)
    d.mkdir(parents=True, exist_ok=True)
    live, journal = str(d / "live.db"), str(d / "live.writes.jsonl")
    failures = []

    def expect(ok, what):
        print(("ok    " if ok else "FAIL  ") + what)
        if not ok:
            failures.append(what)

    asyncio.run(build(manifests, live, journal))
    r = asyncio.run(adopt(manifests, live, journal, str(d / "s1.db")))
    expect(isinstance(r, AdoptRefusal) and "er1" in str(r) and "r1" in str(r)
           and not Path(d / "s1.db").exists(),
           "refused without --drop, listing the residue, scratch removed")
    r = asyncio.run(adopt(manifests, live, journal, str(d / "s2.db"), ["r1"]))
    expect(isinstance(r, AdoptRefusal) and "exactly" in str(r), "refused a drop list naming fewer")
    r = asyncio.run(adopt(manifests, live, journal, str(d / "s3.db"), ["r1", "er1"]))
    end = read_tail(journal, ORIGIN)[-1][1]
    expect(not isinstance(r, AdoptRefusal) and r["dropped"] == ["er1", "r1"]
           and r["cursor"] == end.to_dict(), f"adopted, dropped the residue, cursor at the end: {r if isinstance(r, AdoptRefusal) else r['cursor']}")
    asyncio.run(rebuild_db(str(d / "rb1.db"), journal, manifests, handlers=HANDLERS))
    res = asyncio.run(diff_dbs(live, str(d / "rb1.db"), manifests))
    expect(res["clean"], "a fresh rebuild equals the adopted db element for element")

    diverged = str(d / "diverged.db")
    Path(diverged).write_bytes(Path(live).read_bytes())
    asyncio.run(touch(manifests, diverged))
    r = asyncio.run(adopt(manifests, diverged, journal, str(d / "s4.db")))
    expect(isinstance(r, AdoptRefusal) and "not the projection" in str(r),
           "refused a db with a field changed outside the journal")

    rep, cursor = asyncio.run(compact(manifests, live, journal, str(d / "archive")))
    expect(rep.nodes_archived == 1 and rep.archive_path is not None, f"compacted: {rep.summary()}")
    expect(scan_references(journal, ["n0"]) == [], "no kept op names the retired id")
    expect(cursor == read_tail(journal, ORIGIN)[-1][1], "the cursor re-stamped at the family's end")
    asyncio.run(rebuild_db(str(d / "rb2.db"), journal, manifests, handlers=HANDLERS))
    res = asyncio.run(diff_dbs(live, str(d / "rb2.db"), manifests))
    expect(res["clean"], f"a fresh rebuild of the compacted family equals live "
                         f"(nodes {res['nodes']['count_a']}/{res['nodes']['count_b']}, "
                         f"edges {res['edges']['count_a']}/{res['edges']['count_b']})")
    print("CLEAN" if not failures else f"{len(failures)} FAILED")
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2]))
