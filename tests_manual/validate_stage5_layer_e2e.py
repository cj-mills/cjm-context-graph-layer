#!/usr/bin/env python
"""Stage-5 stress suite — the CR-18 graph-aware layer's standing validation.

Items (per the ratified stage-5 stress list; see
cjm-substrate/claude-docs/stage-5-evidence.md):

  A. (operational, recorded in the ledger run log) re-derivation reference
     survival + emission idempotency at corpus scale — deterministic ids make
     full re-runs verify-collide (0 added) and the Connection's refs resolve.
  B. CONCURRENT SUPERSESSION (the first SEMANTIC race): two sessions
     supersede the same correction concurrently; both SUPERSEDES edges land
     append-only; the effective view is DETERMINISTIC (latest created_at
     wins); layer-0 untouched.
  C. OVERLAY-PROJECTION PARITY HARNESS: random conflict-free
     prune/replace/supersede interleavings — the layer projection agrees with
     an oracle implementing the pre-stage-5 in-core algorithm; invariants:
     pruned never reappear, superseded never win, projection idempotent and
     input-order-independent.
  D. (operational) decision-preservation drill — /tmp/stage5_decision_drill.py
     (11/11 hash re-anchor; recorded in the ledger).

Run from a repo whose .cjm/manifests carries cjm-graph-plugin-sqlite (the
correction-core repo root), in an env with the layer + schema libs:

    conda run -n cjm-transcript-correction-core python \
        /path/to/cjm-context-graph-layer/tests_manual/validate_stage5_layer_e2e.py

Part B uses a SCRATCH graph DB (synthetic spine); the corpus is never touched.
"""
import argparse
import asyncio
import random
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List
from uuid import uuid4

from cjm_substrate.core.manager import CapabilityManager
from cjm_substrate.core.queue import JobQueue
from cjm_context_graph_layer.ops import graph_task, extend_graph
from cjm_context_graph_layer.grammar import OverlayRelations, make_edge, spine_edges, grouped_spine_edges
from cjm_context_graph_layer.edits import SpineEdit, SpineUnit, resolve_active, project_effective_spine
from cjm_transcript_graph_schema.schema import SourceNode, AudioSegmentNode, SegmentNode

GRAPH_ID = "cjm-graph-plugin-sqlite"


def check(name, cond):
    print(("  PASS " if cond else "  FAIL ") + name)
    return bool(cond)


# ---------------------------------------------------------------- Part C (pure)
def oracle_projection(units: List[SpineUnit], actives: List[Dict[str, Any]]) -> List[SpineUnit]:
    """The pre-stage-5 in-core algorithm: prune set + text overrides (conflict-free input)."""
    pruned, overrides = set(), {}
    for c in actives:
        if c["op"] == "prune":
            pruned.update(c["targets"])
        elif c["op"] == "replace_text":
            overrides[c["targets"][0]] = c["payload"]["text"]
    return [SpineUnit(u.id, overrides.get(u.id, u.text)) for u in units if u.id not in pruned]


def part_c_parity(seed: int = 5, rounds: int = 200) -> bool:
    rng = random.Random(seed)
    ok = True
    for r in range(rounds):
        n = rng.randint(3, 40)
        units = [SpineUnit(f"s{i}", f"text {i}" if rng.random() > 0.1 else "") for i in range(n)]
        # Build a realistic history: every re-edit supersedes the prior active
        # (conflict-free actives — the real workflow's invariant).
        history: List[Dict[str, Any]] = []
        supersedes: List[tuple] = []
        active_by_seg: Dict[str, str] = {}
        t = 0.0
        for _ in range(rng.randint(0, 25)):
            t += 1.0
            cid = f"c{len(history)}"
            if rng.random() < 0.25:
                targets = rng.sample([u.id for u in units], k=rng.randint(1, max(1, n // 4)))
                history.append({"id": cid, "op": "prune", "targets": targets,
                                "payload": {}, "created_at": t})
            else:
                seg = rng.choice(units).id
                if seg in active_by_seg:
                    supersedes.append((cid, active_by_seg[seg]))
                history.append({"id": cid, "op": "replace_text", "targets": [seg],
                                "payload": {"text": f"edit-{cid}"}, "created_at": t})
                active_by_seg[seg] = cid
        active_ids = resolve_active([h["id"] for h in history], supersedes)
        actives = [h for h in history if h["id"] in active_ids]
        edits = [SpineEdit(edit_id=h["id"], op=h["op"], targets=h["targets"],
                           payload=h["payload"], created_at=h["created_at"]) for h in actives]

        layer_out = project_effective_spine(units, edits)
        oracle_out = oracle_projection(units, actives)
        agree = [(u.id, u.text) for u in layer_out] == [(u.id, u.text) for u in oracle_out]
        # invariants
        pruned_ids = {t for h in actives if h["op"] == "prune" for t in h["targets"]}
        no_pruned = all(u.id not in pruned_ids for u in layer_out)
        superseded_ids = {old for _, old in supersedes}
        no_superseded_text = all(not u.text.startswith("edit-") or
                                 u.text.removeprefix("edit-") not in superseded_ids
                                 for u in layer_out)
        idempotent = [(u.id, u.text) for u in project_effective_spine(units, edits)] == \
                     [(u.id, u.text) for u in layer_out]
        shuffled = list(edits)
        rng.shuffle(shuffled)
        order_independent = [(u.id, u.text) for u in project_effective_spine(units, shuffled)] == \
                            [(u.id, u.text) for u in layer_out]
        if not (agree and no_pruned and no_superseded_text and idempotent and order_independent):
            print(f"  round {r}: agree={agree} no_pruned={no_pruned} "
                  f"no_superseded={no_superseded_text} idem={idempotent} order={order_independent}")
            ok = False
    return check(f"C: parity harness ({rounds} random interleavings)", ok)


# ---------------------------------------------------------------- Part B (runtime)
async def part_b_concurrent_supersession(manifests_dir: str) -> bool:
    scratch = Path(tempfile.mkdtemp(prefix="stage5_scratch_")) / "scratch_graph.db"
    manager = CapabilityManager(search_paths=[Path(manifests_dir)])
    manager.discover_manifests()
    meta = {m.name: m for m in manager.discovered}[GRAPH_ID]
    assert manager.load_capability(meta, config={"db_path": str(scratch)}), "graph load failed"
    queue = JobQueue(deps=manager)
    await queue.start()
    ok = True
    try:
        # Synthetic Source-rooted spine (deterministic ids via the schema lib).
        src = SourceNode(content_hash="sha256:stress-b", path="/tmp/stress-b.mp3")
        aseg = AudioSegmentNode(source=src.id, index=0, start=0.0, end=10.0,
                                model_input_path="/tmp/b.wav", model_input_hash="sha256:bwav")
        segs = [SegmentNode(audio_segment=aseg.id, vad_config_hash="vcfg",
                            chunk_start=float(i), chunk_end=float(i) + 0.9, index=i,
                            start_time=float(i), end_time=float(i) + 0.9,
                            text=f"segment {i}", audio_hash="sha256:bwav", source=src.id)
                for i in range(5)]
        nodes = [src.to_graph_node(), aseg.to_graph_node()] + [s.to_graph_node() for s in segs]
        edges = spine_edges(src.id, [aseg.id]) + grouped_spine_edges([(aseg.id, [s.id for s in segs])])
        await extend_graph(queue, GRAPH_ID, nodes, edges)
        target = segs[2].id

        def correction(text, created_at, supersedes=None):
            cid = str(uuid4())
            node = {"id": cid, "label": "Correction",
                    "properties": {"correction_type": "text_content", "status": "applied",
                                   "session_id": str(uuid4()), "created_at": created_at,
                                   "payload": {"operation": "replace_text", "source_id": src.id,
                                               "segment_id": target, "new_text": text}},
                    "sources": []}
            e = [make_edge(cid, target, "CORRECTS")]
            if supersedes:
                e.append(make_edge(cid, supersedes, OverlayRelations.SUPERSEDES))
            return cid, node, e

        c0, n0, e0 = correction("base edit", 100.0)
        await extend_graph(queue, GRAPH_ID, [n0], e0)

        # THE RACE: two sessions supersede c0 CONCURRENTLY.
        ca, na, ea = correction("session A wins?", 200.0, supersedes=c0)
        cb, nb, eb = correction("session B wins?", 201.0, supersedes=c0)
        await asyncio.gather(extend_graph(queue, GRAPH_ID, [na], ea),
                             extend_graph(queue, GRAPH_ID, [nb], eb))

        # Both SUPERSEDES edges landed append-only.
        from cjm_context_graph_primitives.query import EdgeQuery, NodeQuery
        res = await graph_task(queue, GRAPH_ID, "query_edges",
                               query=EdgeQuery(relation_type="SUPERSEDES",
                                               target_ids=[c0], project=[]).to_dict())
        ok &= check("B: both SUPERSEDES edges landed (append-only)", len(res.rows or []) == 2)

        # Layer-0 untouched.
        seg_node = await graph_task(queue, GRAPH_ID, "get_node", node_id=target)
        props = seg_node.properties if hasattr(seg_node, "properties") else seg_node["properties"]
        ok &= check("B: layer-0 segment text untouched", props["text"] == "segment 2")

        # Deterministic effective view: c0 excluded; latest created_at (cb) wins.
        units = [SpineUnit(s.id, s.text) for s in segs]
        corrs = [dict(n0["properties"], id=c0), dict(na["properties"], id=ca),
                 dict(nb["properties"], id=cb)]
        active_ids = resolve_active([c["id"] for c in corrs], [(ca, c0), (cb, c0)])
        ok &= check("B: superseded correction excluded", c0 not in active_ids and len(active_ids) == 2)
        edits = [SpineEdit(edit_id=c["id"], op="replace_text",
                           targets=[c["payload"]["segment_id"]],
                           payload={"text": c["payload"]["new_text"]},
                           created_at=c["created_at"]) for c in corrs if c["id"] in active_ids]
        out1 = project_effective_spine(units, edits)
        out2 = project_effective_spine(units, list(reversed(edits)))
        eff = next(u.text for u in out1 if u.id == target)
        ok &= check("B: effective view deterministic (latest created_at wins)",
                    eff == "session B wins?" and
                    [(u.id, u.text) for u in out1] == [(u.id, u.text) for u in out2])
    finally:
        await queue.stop()
        manager.unload_capability(GRAPH_ID)
    return ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifests-dir",
                    default="/mnt/SN850X_8TB_EXT4/Projects/GitHub/cj-mills/cjm-transcript-correction-core/.cjm/manifests")
    args = ap.parse_args()
    ok = part_c_parity()
    ok &= asyncio.run(part_b_concurrent_supersession(args.manifests_dir))
    print("STAGE-5 STRESS", "ALL CHECKS PASSED" if ok else "FAILURES")
    return 0 if ok else 1


sys.exit(main())
