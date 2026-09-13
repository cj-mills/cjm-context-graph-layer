"""Journal compaction (ruling a7617bd4): retired wires MOVE to an archive family; the
four invariants — no dangling reference outside the archive, kept ∪ archived == original,
the live tail is never rewritten, idempotent."""

import json
import sqlite3
from pathlib import Path

import pytest

from cjm_context_graph_layer.compact import (CompactRefusal, archive_segments, compact_journal,
                                             next_archive_path, scan_references)
from cjm_context_graph_layer.journal import GENESIS_EDGE, GENESIS_NODE
from cjm_context_graph_layer.rebuild import digests_equal, idset_digest
from cjm_context_graph_primitives.journal import journal_segments, read_journal

R1 = "11111111-1111-4111-8111-111111111111"   # retired segment
R2 = "22222222-2222-4222-8222-222222222222"   # retired segment
L1 = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"   # live segment
T1 = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"   # a Transcript both spines slice


def _node(i, **props):
    return {"id": i, "label": "Segment", "properties": props, "sources": []}


def _edge(i, s, t, rel="PART_OF"):
    return {"id": i, "source_id": s, "target_id": t, "relation_type": rel, "properties": {}}


def _family(tmp_path, *, with_dangling=False):
    """Two cold segments + a live tail. 0001: genesis for T1 + a wholly-retired spine op (R1, R2)
    + a live spine op (L1). 0002: a MIXED backfill op (edges from R1 and L1 to T1) + a delete op
    naming R2 (legal residue). Tail: one unrelated op (never rewritten)."""
    jp = tmp_path / "wf.writes.jsonl"
    seg1 = [
        {"verb": GENESIS_NODE, "ts": 1.0, "args": {"id": T1, "label": "Transcript", "properties": {}}},
        {"verb": "spine-extension", "actor": "pipeline", "run": "decomp_r1", "ts": 2.0,
         "args": {"source_id": "src", "segments": 2},
         "wires": {"nodes": [_node(R1, index=0), _node(R2, index=1)],
                   "edges": [_edge("e-r1-r2", R1, R2, "NEXT")]}},
        {"verb": "spine-extension", "actor": "pipeline", "run": "decomp_r2", "ts": 3.0,
         "args": {"source_id": "src", "segments": 1},
         "wires": {"nodes": [_node(L1, index=0)], "edges": []}},
    ]
    seg2 = [
        {"verb": "spine-extension", "actor": "cli", "ts": 4.0, "args": {"act": "provenance-backfill"},
         "wires": {"nodes": [], "edges": [_edge("e-r1-t1", R1, T1, "DERIVED_FROM"),
                                         _edge("e-l1-t1", L1, T1, "DERIVED_FROM")]}},
        {"verb": "collection-curation", "actor": "human", "ts": 5.0, "args": {"act": "x"},
         "deletes": {"node_ids": [R2], "edge_ids": []}, "updates": [], "wires": {"nodes": [], "edges": []}},
    ]
    if with_dangling:
        seg2.append({"verb": "text-correction", "actor": "human", "ts": 6.0,
                     "args": {"segment_id": R1, "text": "fixed"},
                     "wires": {"nodes": [{"id": "c1", "label": "Correction", "properties": {}}],
                               "edges": [_edge("e-c1-r1", "c1", R1, "CORRECTS")]}})
    tail = [{"verb": "session-start", "actor": "human", "ts": 7.0, "args": {"scope": ["src"]}}]
    (tmp_path / "wf.writes.0001.jsonl").write_text("".join(json.dumps(o, sort_keys=True) + "\n" for o in seg1))
    (tmp_path / "wf.writes.0002.jsonl").write_text("".join(json.dumps(o, sort_keys=True) + "\n" for o in seg2))
    jp.write_text("".join(json.dumps(o, sort_keys=True) + "\n" for o in tail))
    return str(jp)


def _wire_multiset(ops):
    nodes, edges = [], []
    for op in ops:
        w = op.get("wires") or {}
        nodes += sorted(n["id"] for n in w.get("nodes") or [])
        edges += sorted(e["id"] for e in w.get("edges") or [])
        if op.get("verb") == GENESIS_NODE:
            nodes.append(op["args"]["id"])
    return sorted(nodes), sorted(edges)


def test_compaction_moves_retired_wires_and_keeps_the_rest(tmp_path):
    jp = _family(tmp_path)
    before = read_journal(jp)
    arch = tmp_path / "archive"
    rep = compact_journal(jp, str(arch), [R1, R2], label="t1", actor="test")
    assert not rep.dry_run and rep.archive_path and Path(rep.archive_path).exists()
    assert rep.ops_archived_whole == 1 and rep.ops_split == 1
    assert rep.nodes_archived == 2 and rep.edges_archived == 2  # NEXT(R1,R2) + DERIVED_FROM(R1,T1)
    assert rep.retired_ids_seen == 2 and rep.bytes_freed > 0
    # invariant 2: kept ∪ archived == original (wire multiset)
    after = read_journal(jp)
    archived = read_journal(rep.archive_path)
    assert _wire_multiset(after + archived) == _wire_multiset(before)
    # the kept side never names a retired id outside deletes (invariant 1)
    assert scan_references(jp, [R1, R2]) == []
    kept_verbs = [o["verb"] for o in after]
    assert "spine-extension" in kept_verbs and "collection-curation" in kept_verbs
    # the mixed backfill op kept only the live edge and carries the split marker
    split = [o for o in after if o.get("compaction")]
    assert len(split) == 1 and [e["id"] for e in split[0]["wires"]["edges"]] == ["e-l1-t1"]
    assert split[0]["compaction"]["archived_edges"] == 1
    # the archived half shares the envelope and records where it came from
    arch_split = [o for o in archived if o.get("compaction")]
    assert arch_split[0]["ts"] == 4.0 and arch_split[0]["compaction"]["from"] == "wf.writes.0002.jsonl"
    # the delete naming R2 is legal residue, kept verbatim
    assert any(o.get("deletes", {}).get("node_ids") == [R2] for o in after)
    # manifest beside the archive
    man = json.loads(Path(rep.manifest_path).read_text())
    assert man["label"] == "t1" and man["nodes_archived"] == 2 and "wf.writes.0001.jsonl" in man["segments_rewritten"]


def test_live_tail_is_rotated_never_rewritten(tmp_path):
    jp = _family(tmp_path)
    tail_text = Path(jp).read_text()
    rep = compact_journal(jp, str(tmp_path / "archive"), [R1, R2], label="t2")
    # invariant 3: the tail was closed into a cold segment first; its bytes are intact there
    assert not Path(jp).exists()
    segs = journal_segments(jp)
    assert Path(segs[-1]).read_text() == tail_text
    assert rep.segments_scanned == 3


def test_dangling_reference_refuses_before_writing(tmp_path):
    jp = _family(tmp_path, with_dangling=True)
    snapshot = {s: Path(s).read_text() for s in journal_segments(jp)}
    arch = tmp_path / "archive"
    with pytest.raises(CompactRefusal, match="text-correction"):
        compact_journal(jp, str(arch), [R1, R2], label="t3")
    # nothing written: no archive, cold segments byte-identical (the tail was rotated, that is all)
    assert archive_segments(jp, str(arch)) == []
    for s, text in snapshot.items():
        if Path(s).exists():
            assert Path(s).read_text() == text
    # a dry run REPORTS instead
    rep = compact_journal(jp, str(arch), [R1, R2], label="t3", dry_run=True)
    assert rep.dry_run and len(rep.dangling) == 1 and rep.dangling[0].verb == "text-correction"
    assert rep.archive_path is None


def test_idempotent_and_archive_numbering(tmp_path):
    jp = _family(tmp_path)
    arch = str(tmp_path / "archive")
    first = compact_journal(jp, arch, [R1, R2], label="a")
    assert first.archive_path.endswith("wf.writes.retired.0001.jsonl")
    second = compact_journal(jp, arch, [R1, R2], label="b")
    assert second.ops_archived_whole == 0 and second.ops_split == 0 and second.archive_path is None
    assert next_archive_path(jp, arch).endswith("wf.writes.retired.0002.jsonl")
    # an empty retired set is a no-op report
    assert compact_journal(jp, arch, [], label="c").ops_scanned == 0


def test_dry_run_touches_nothing(tmp_path):
    jp = _family(tmp_path)
    snapshot = {s: Path(s).read_text() for s in journal_segments(jp)}
    rep = compact_journal(jp, str(tmp_path / "archive"), [R1, R2], label="dry", dry_run=True)
    assert rep.nodes_archived == 2 and rep.segments_rewritten == 2 and rep.archive_path is None
    assert {s: Path(s).read_text() for s in journal_segments(jp)} == snapshot
    assert not (tmp_path / "archive").exists()


def test_idset_digest_equality_is_id_set_equality(tmp_path):
    def mk(path, node_ids, edge_ids):
        con = sqlite3.connect(path)
        con.execute("CREATE TABLE nodes (id TEXT PRIMARY KEY, label TEXT)")
        con.execute("CREATE TABLE edges (id TEXT PRIMARY KEY, source_id TEXT, target_id TEXT)")
        con.executemany("INSERT INTO nodes VALUES (?, 'X')", [(i,) for i in node_ids])
        con.executemany("INSERT INTO edges VALUES (?, 'a', 'b')", [(i,) for i in edge_ids])
        con.commit()
        con.close()
    a, b, c = (str(tmp_path / f"{n}.db") for n in "abc")
    mk(a, ["n1", "n2"], ["e1"])
    mk(b, ["n2", "n1"], ["e1"])       # same sets, different insertion order
    mk(c, ["n1", "n2", "n3"], ["e1"])
    da, db, dc = idset_digest(a), idset_digest(b), idset_digest(c)
    assert da["nodes"] == 2 and da["edges"] == 1
    assert digests_equal(da, db) and not digests_equal(da, dc)
