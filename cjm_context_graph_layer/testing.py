"""An in-memory graph store for the write order's tests -- one fake every workflow core's suite shares.

`MemGraph` stands behind a fake job queue and answers the storage methods the write order and the
cores' appliers call: the presence reads `plan_extend` makes, adds, property merges, deletes and
the journal cursors a `JournalWriter` keeps beside the graph. A test passes it where a started
queue goes; `state()` is what a live-vs-replay comparison reads. Any other method raises, so a
write path that grows a new storage call fails loudly here instead of passing over a silent None.
"""

import copy
from types import SimpleNamespace
from typing import Any, Dict, List, Tuple

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
