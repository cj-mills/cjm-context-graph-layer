#!/usr/bin/env python
"""Stage-5 stress item 5: decision-preservation drill across the Document->Source reset.

Re-anchors the Nanjing Connection (exported from the pre-stage5 corpus) onto the
rebuilt Source-rooted corpus:
- each old SourceRef's content_hash (over the consumed VOXTRAL segment text) is
  probed via SourcePredicate(content_hash=...) — content-hash-PRIMARY identity
  working exactly as CR-19 designed: the old GraphNodeRef locators dangle, the
  hashes re-anchor;
- disambiguation (if a hash matches >1 segment) uses the exported old segment's
  index + text;
- the Connection keeps its ORIGINAL generated node id (asserted decisions keep
  their identity across resets); DERIVED_FROM edges rebuild deterministically
  against the new target ids, carrying the old role properties;
- verification: 11/11 hash-verify against live new-segment text + the reverse
  index (find_nodes_by_source) surfaces the Connection through the NEW refs.

Run from cjm-transcript-correction-core repo root in its env.
"""
import asyncio
import json
import sys
from pathlib import Path

from cjm_plugin_system.core.manager import PluginManager
from cjm_plugin_system.core.queue import JobQueue
from cjm_context_graph_primitives.provenance import SourceRef
from cjm_context_graph_primitives.locators import GraphNodeRef
from cjm_context_graph_primitives.slices import FullContent
from cjm_context_graph_primitives.query import NodeQuery, SourcePredicate
from cjm_context_graph_primitives.graph import GraphNode
from cjm_context_graph_layer.ops import graph_task, extend_graph
from cjm_context_graph_layer.grammar import OverlayRelations, make_edge

CORPUS = "/mnt/SN850X_8TB_EXT4/Projects/GitHub/cj-mills/cjm-transcript-decomp-core/.cjm/data/cjm-graph-plugin-sqlite/context_graph.db"
GRAPH_ID = "cjm-graph-plugin-sqlite"
EXPORT = json.load(open("/tmp/stage5_decision_export.json"))


def node_props(node):
    if isinstance(node, GraphNode):
        return node.properties
    return (node or {}).get("properties") or {}


async def main():
    manager = PluginManager(search_paths=[Path(".cjm/manifests")])
    manager.discover_manifests()
    meta = {m.name: m for m in manager.discovered}[GRAPH_ID]
    assert manager.load_plugin(meta, config={"db_path": CORPUS}), "graph load failed"
    queue = JobQueue(deps=manager)
    await queue.start()
    ok = True
    try:
        conn = next(n for n in EXPORT["nodes"] if n["label"] == "Connection")
        old_edges = [e for e in EXPORT["edges"]
                     if e["source_id"] == conn["id"] and e["relation_type"] == "DERIVED_FROM"]
        roles = {e["target_id"]: e["properties"] for e in old_edges}
        old_targets = EXPORT["old_target_segments"]

        new_refs, new_edges, mapping = [], [], []
        for ref in conn["sources"]:
            h = ref["content_hash"]
            old_nid = (ref.get("locator") or {}).get("node_id")
            old_info = old_targets.get(old_nid) or {}
            q = NodeQuery(label="Segment", source=SourcePredicate(content_hash=h),
                          project=["index", "text"])
            res = await graph_task(queue, GRAPH_ID, "query_nodes", query=q.to_dict())
            rows = res.rows or []
            if len(rows) > 1 and old_info:
                rows = [r for r in rows if r.get("index") == old_info.get("index")
                        and r.get("text") == old_info.get("text")] or rows
            if not rows:
                print(f"  DANGLING (loud): hash {h[:24]}… old node {old_nid[:8]} -> NO new match")
                ok = False
                continue
            new_id = rows[0]["id"]
            mapping.append((old_nid, new_id, h))
            new_refs.append(SourceRef(locator=GraphNodeRef(node_id=new_id), content_hash=h,
                                      slice=FullContent("text")).to_dict())
            new_edges.append(make_edge(conn["id"], new_id, OverlayRelations.DERIVED_FROM,
                                       properties=roles.get(old_nid) or {}))
        print(f"re-anchored {len(mapping)}/{len(conn['sources'])} refs "
              f"({sum(1 for o, n, _ in mapping if o != n)} moved to new node ids)")

        node = {"id": conn["id"], "label": "Connection",
                "properties": dict(conn["properties"]), "sources": new_refs}
        res = await extend_graph(queue, GRAPH_ID, [node], new_edges)
        print(f"committed: +{res.nodes_added} node ({res.nodes_verified} verified), "
              f"+{res.edges_added} edges ({res.edges_existing} existing)")

        # Verification 1: hash-verify every new ref against LIVE segment text.
        verified = 0
        for _, new_id, h in mapping:
            seg = await graph_task(queue, GRAPH_ID, "get_node", node_id=new_id)
            text = str(node_props(seg).get("text") or "")
            if SourceRef.compute_hash(text.encode()) == h:
                verified += 1
            else:
                print(f"  HASH MISMATCH on {new_id[:8]}")
                ok = False
        print(f"hash-verify: {verified}/{len(mapping)}")

        # Verification 2: the reverse index surfaces the Connection via a NEW ref.
        probe = new_refs[0]
        found = await graph_task(queue, GRAPH_ID, "find_nodes_by_source", source_ref=probe)
        found_ids = [(n.id if isinstance(n, GraphNode) else n.get("id")) for n in (found or [])]
        rev_ok = conn["id"] in found_ids
        print(f"reverse-index lookup surfaces the Connection: {rev_ok}")
        ok = ok and verified == len(mapping) == len(conn["sources"]) and rev_ok

        # Idempotency: replay the drill commit -> verified no-op.
        res2 = await extend_graph(queue, GRAPH_ID, [node], new_edges)
        print(f"replay: +{res2.nodes_added} node ({res2.nodes_verified} verified), "
              f"+{res2.edges_added} edges ({res2.edges_existing} existing)")
        ok = ok and res2.nodes_added == 0 and res2.edges_added == 0
    finally:
        await queue.stop()
        manager.unload_plugin(GRAPH_ID)
    print("DRILL", "PASSED" if ok else "FAILED")
    return 0 if ok else 1


sys.exit(asyncio.run(main()))
