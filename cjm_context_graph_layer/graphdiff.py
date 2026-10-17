"""Two graphs compared element for element, streamed: the one standing check for every lane (ruling f8b9c728).

A live graph and its rebuild from the same journals must be EQUAL: the same node and edge ids,
and on every shared id the same label, properties, sources and created_at / updated_at (design
8f6f2343). This module is that comparison for every graph the layer serves -- the dev lane's
`rebuild-diff`, `cjm-workflow-rebuild --against`, the decomp core's compaction proof. It reads
both graphs through the graph-storage capability in id-ordered keyset pages and merges them, so
memory is one page per side whatever the graph's size (the transcript graph's 2.5M elements, the
instrument's 32x tiers, 681604c1). An id-set digest says only THAT two graphs differ, and passed
over a rebuild that restamped 42.5k nodes' times (finding b562d5f7); this says where.

There is no exclusion list. A difference a known cause accounts for is ATTRIBUTED to it -- listed
under the cause's name, apart from drift, still counted and sampled, never dropped (ruling
6752db0a(9)). The causes are DATA each lane passes: an `IdAttribution` names the elements a cause
accounts for (the dev lane's moved sources, a9176261), a `LagAttribution` a time-field class (the
transcript lane's pre-op-clock history), a `MarkAttribution` a property copying its element's own
time column (the transcript lane's reserved key left in the blob). Paths are the one
normalization: `path_map` rewrites a
moved corpus's prefix on both sides before comparing (19f699d0)."""

import asyncio
from dataclasses import dataclass
from typing import (Any, AsyncIterable, AsyncIterator, Dict, FrozenSet, Iterable, Iterator, List,
                    Optional, Sequence, Tuple, Union)

from cjm_context_graph_primitives.query import EdgeQuery, NodeQuery

from .ops import graph_task

SAMPLES_PER_FIELD = 5  # Differing ids shown per (kind, class, field) row, and per one-sided bucket
PAGE = 5000            # Elements per keyset page: a transport bound, never a cap
# Completed jobs a store opened for a stream retains. The queue keeps each finished job WITH its
# result (100 by default), so a streamed read held 100 pages per side (3.2 GB on the transcript
# graph); a stream has a page and its prefetch in flight per store, and a job evicted before its
# waiter registers is lost, so a few is the bound.
STREAM_HISTORY = 4
DRIFT = None           # The bucket key of an unattributed difference

# Per kind: the wire key naming an element's class, and its identity-bearing fields
KINDS = {"node": ("label", ["label"]),
         "edge": ("relation_type", ["source_id", "target_id", "relation_type"])}


@dataclass(frozen=True)
class IdAttribution:
    """The elements a known cause accounts for, by id: every difference on them is the cause's."""

    name: str                   # The cause, as the report names it (e.g. "source moved")
    ids: FrozenSet[str]         # Element ids it accounts for
    originated: bool = True     # An edge whose source_id is in `ids` is the cause's row too

    def admits(
        self,
        kind: str,               # "node" | "edge"
        w: Dict[str, Any],       # The element's wire dict (either side)
    ) -> bool:  # True when the cause accounts for this element
        """The element's id is named, or (`originated`) it is an edge leaving a named element:
        a link resolved from a moved post's body is that post's row; an edge INTO it is not."""
        return w["id"] in self.ids or (self.originated and kind == "edge"
                                       and w.get("source_id") in self.ids)


@dataclass(frozen=True)
class LagAttribution:
    """A time field whose A value precedes B's by under `max_lag` seconds, on an element written
    before `before`: a history whose stamp was read a moment before its op's clock."""

    name: str                                       # The cause, as the report names it
    max_lag: float                                  # B - A must lie in (0, max_lag) seconds
    before: float                                   # A's value must precede this epoch
    fields: Tuple[str, ...] = ("created_at", "updated_at")  # The time fields it may account for

    def admits(
        self,
        field: str,          # The differing field
        va: Any,             # Its value in A
        vb: Any,             # Its value in B
        wa: Dict[str, Any],  # A's element (a lag reads the two values alone)
        wb: Dict[str, Any],  # B's element
    ) -> bool:  # True when the cause accounts for this one difference
        """Only a time field, only A before B, only by less than the lag, only before the epoch."""
        if field not in self.fields or not all(isinstance(v, (int, float)) for v in (va, vb)):
            return False
        return 0 < vb - va < self.max_lag and va < self.before


@dataclass(frozen=True)
class MarkAttribution:
    """A property carrying a copy of its element's own time column in A, where B holds an
    earlier value: the mark a writer leaves when it stored a reserved key in the properties
    blob as well as in the column -- a class no date bounds when a writer's install, not a
    commit, decided it (ruling 0669404b (2))."""

    name: str      # The cause, as the report names it
    field: str     # The compared field it may account for (e.g. "properties.updated_at")
    column: str    # A's time column the field copies (e.g. "updated_at")
    within: float  # The copy may precede the column by up to this many seconds (stamped apart)

    def admits(
        self,
        field: str,           # The differing field
        va: Any,              # Its value in A
        vb: Any,              # Its value in B
        wa: Dict[str, Any],   # A's element
        wb: Dict[str, Any],   # B's element
    ) -> bool:  # True when the cause accounts for this one difference
        """Only its field; A's value a copy of A's own column (0 <= column - value < within);
        B's value earlier than A's."""
        col = wa.get(self.column)
        if field != self.field or not all(isinstance(v, (int, float)) for v in (va, vb, col)):
            return False
        return 0 <= col - va < self.within and vb < va


Attribution = Union[IdAttribution, LagAttribution, MarkAttribution]


def wire(
    x: Any,  # A GraphNode / GraphEdge, or its wire dict
) -> Dict[str, Any]:  # The wire dict
    """The wire dict of a typed graph element (task results may arrive typed or as dicts)."""
    return x.to_dict() if hasattr(x, "to_dict") else dict(x)


def normalize(
    v: Any,                               # Any JSON-shaped value
    path_map: Sequence[Tuple[str, str]],  # (old prefix, new prefix) pairs, applied in order
) -> Any:  # The value with every mapped path prefix rewritten
    """Rewrite mapped path prefixes in every string, recursively (keys stay as they are)."""
    if not path_map:
        return v
    if isinstance(v, str):
        for old, new in path_map:
            if v.startswith(old):
                return new + v[len(old):]
        return v
    if isinstance(v, dict):
        return {k: normalize(x, path_map) for k, x in v.items()}
    if isinstance(v, list):
        return [normalize(x, path_map) for x in v]
    return v


def compared_fields(
    w: Dict[str, Any],          # A normalized node or edge wire dict
    structural: Iterable[str],  # The element's identity-bearing top-level fields
) -> Dict[str, Any]:  # field name -> value, properties flattened one level as `properties.<key>`
    """The compared fields of one element: its structural fields, each property key on its
    own row (so a report says WHICH property drifts), sources whole, both time columns."""
    out: Dict[str, Any] = {f: w.get(f) for f in structural}
    for k, v in (w.get("properties") or {}).items():
        out[f"properties.{k}"] = v
    if "sources" in w:
        out["sources"] = w.get("sources") or []
    out["created_at"] = w.get("created_at")
    out["updated_at"] = w.get("updated_at")
    return out


class _Bucket:
    """The differences one cause (or drift) holds for one kind: one-sided ids by class, the
    count of shared ids differing, and per (class, field) a count with samples."""

    def __init__(self, kind: str, collect: bool = False):
        self.kind = kind
        self.collect = collect  # Keep EVERY one-sided id (`ids`), not only the sample
        self.sides = {"only_a": {"count": 0, "by_class": {}, "sample": []},
                      "only_b": {"count": 0, "by_class": {}, "sample": []}}
        if collect:
            for s in self.sides.values():
                s["ids"] = []
        self.differing = 0
        self.rows: Dict[Tuple[str, str], Dict[str, Any]] = {}

    def one_sided(self, side: str, eid: str, cls: str) -> None:
        s = self.sides[side]
        s["count"] += 1
        s["by_class"][cls] = s["by_class"].get(cls, 0) + 1
        if len(s["sample"]) < SAMPLES_PER_FIELD:
            s["sample"].append(eid)
        if self.collect:
            s["ids"].append(eid)

    def fields(self, eid: str, cls: str, fields: List[Tuple[str, Any, Any]]) -> None:
        self.differing += 1
        for f, va, vb in fields:
            row = self.rows.setdefault((cls, f), {"kind": self.kind, "class": cls, "field": f,
                                                 "count": 0, "samples": []})
            row["count"] += 1
            if len(row["samples"]) < SAMPLES_PER_FIELD:
                row["samples"].append({"id": eid, "a": va, "b": vb})

    def report(self) -> Dict[str, Any]:
        sides = {k: {**v, "by_class": dict(sorted(v["by_class"].items(), key=lambda kv: -kv[1]))}
                 for k, v in self.sides.items()}
        return {**sides, "differing": self.differing,
                "rows": sorted(self.rows.values(), key=lambda r: (-r["count"], r["class"], r["field"]))}

    def total(self) -> int:
        return self.sides["only_a"]["count"] + self.sides["only_b"]["count"] + self.differing


class KindDiff:
    """The comparison of one element kind, fed one id-aligned pair at a time (either side may
    be absent) -- the accumulator both the in-memory and the streamed merge drive."""

    def __init__(
        self,
        kind: str,                                   # "node" | "edge"
        path_map: Sequence[Tuple[str, str]] = (),    # (old prefix, new prefix) path rewrites
        attribution: Sequence[Attribution] = (),     # The lane's known causes, in precedence order
        collect: bool = False,                       # Keep every one-sided id, not only a sample (an adoption's exact drop list)
    ):
        self.kind = kind
        self.collect = collect
        self.class_of, self.structural = KINDS[kind]
        self.path_map = list(path_map)
        self.by_id = [a for a in attribution if isinstance(a, IdAttribution)]
        self.by_field = [a for a in attribution if not isinstance(a, IdAttribution)]
        self.names = [a.name for a in attribution]
        self.buckets: Dict[Optional[str], _Bucket] = {DRIFT: _Bucket(kind, collect)}
        self.count_a = self.count_b = 0

    def _bucket(self, name: Optional[str]) -> _Bucket:
        if name not in self.buckets:
            self.buckets[name] = _Bucket(self.kind, self.collect)
        return self.buckets[name]

    def _id_cause(self, w: Dict[str, Any]) -> Optional[str]:
        return next((a.name for a in self.by_id if a.admits(self.kind, w)), DRIFT)

    def add(
        self,
        wa: Optional[Dict[str, Any]],  # A's element with this id (None: only in B)
        wb: Optional[Dict[str, Any]],  # B's element with this id (None: only in A)
    ) -> None:
        """Compare one id: a one-sided id goes to the cause that names it, else drift; on a
        shared id every differing field goes to the id cause, else to the first field cause
        that admits it, else drift -- an element counts once in each bucket it reaches."""
        self.count_a += wa is not None
        self.count_b += wb is not None
        if wa is None or wb is None:
            w, side = (wa, "only_a") if wb is None else (wb, "only_b")
            self._bucket(self._id_cause(w)).one_sided(side, w["id"], str(w.get(self.class_of) or "?"))
            return
        na, nb = normalize(wa, self.path_map), normalize(wb, self.path_map)
        fa, fb = compared_fields(na, self.structural), compared_fields(nb, self.structural)
        fields = [(f, fa.get(f), fb.get(f)) for f in sorted(set(fa) | set(fb)) if fa.get(f) != fb.get(f)]
        if not fields:
            return
        cls = str(na.get(self.class_of) or nb.get(self.class_of) or "?")
        cause = self._id_cause(wa)
        if cause is not DRIFT:
            self._bucket(cause).fields(wa["id"], cls, fields)
            return
        split: Dict[Optional[str], List[Tuple[str, Any, Any]]] = {}
        for f, va, vb in fields:
            name = next((a.name for a in self.by_field if a.admits(f, va, vb, na, nb)), DRIFT)
            split.setdefault(name, []).append((f, va, vb))
        for name, fs in split.items():
            self._bucket(name).fields(wa["id"], cls, fs)

    def report(self) -> Dict[str, Any]:  # {count_a, count_b, only_a, only_b, differing, rows, attributed: {name: bucket}}
        """Drift at the top level (the drift map), each cause's bucket under `attributed`."""
        attributed = {n: self.buckets[n].report() for n in self.names if n in self.buckets}
        return {"count_a": self.count_a, "count_b": self.count_b, **self.buckets[DRIFT].report(),
                "attributed": attributed}

    def clean(self) -> bool:  # No unattributed difference
        return self.buckets[DRIFT].total() == 0

    def attributed(self) -> Dict[str, int]:  # cause -> elements it accounts for (one-sided + differing)
        return {n: self.buckets[n].total() for n in self.names if n in self.buckets}


def _in_order(
    side: str,  # "A" | "B", for the refusal
) -> Any:  # A checker: call with each id, raises unless ids strictly ascend
    """The merge's precondition, checked: a store that broke id order would read as one-sided
    ids, so an out-of-order id refuses loudly instead."""
    last: List[Optional[str]] = [None]

    def check(eid: str) -> None:
        if last[0] is not None and eid <= last[0]:
            raise ValueError(f"graph {side} is not in id order: {eid!r} after {last[0]!r}")
        last[0] = eid
    return check


def merge_sorted(
    a: Iterable[Dict[str, Any]],  # A's elements in ascending id order
    b: Iterable[Dict[str, Any]],  # B's elements in ascending id order
) -> Iterator[Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]]:  # id-aligned pairs
    """Merge two id-ordered element streams into (A, B) pairs, None where a side lacks the id."""
    ia, ib = iter(a), iter(b)
    ca, cb = _in_order("A"), _in_order("B")
    wa, wb = next(ia, None), next(ib, None)
    while wa is not None or wb is not None:
        if wb is None or (wa is not None and wa["id"] < wb["id"]):
            ca(wa["id"])
            yield wa, None
            wa = next(ia, None)
        elif wa is None or wb["id"] < wa["id"]:
            cb(wb["id"])
            yield None, wb
            wb = next(ib, None)
        else:
            ca(wa["id"])
            cb(wb["id"])
            yield wa, wb
            wa, wb = next(ia, None), next(ib, None)


async def amerge_sorted(
    a: AsyncIterable[Dict[str, Any]],  # A's elements in ascending id order
    b: AsyncIterable[Dict[str, Any]],  # B's elements in ascending id order
) -> AsyncIterator[Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]]:  # id-aligned pairs
    """`merge_sorted` over two async streams."""
    ia, ib = a.__aiter__(), b.__aiter__()
    ca, cb = _in_order("A"), _in_order("B")

    async def nxt(it):
        try:
            return await it.__anext__()
        except StopAsyncIteration:
            return None
    wa, wb = await nxt(ia), await nxt(ib)
    while wa is not None or wb is not None:
        if wb is None or (wa is not None and wa["id"] < wb["id"]):
            ca(wa["id"])
            yield wa, None
            wa = await nxt(ia)
        elif wa is None or wb["id"] < wa["id"]:
            cb(wb["id"])
            yield None, wb
            wb = await nxt(ib)
        else:
            ca(wa["id"])
            cb(wb["id"])
            yield wa, wb
            wa, wb = await nxt(ia), await nxt(ib)


def _report(
    nodes: KindDiff,  # The node comparison, fed
    edges: KindDiff,  # The edge comparison, fed
) -> Dict[str, Any]:  # {nodes, edges, clean, attributed: {cause: elements}}
    attributed: Dict[str, int] = {}
    for d in (nodes, edges):
        for n, c in d.attributed().items():
            attributed[n] = attributed.get(n, 0) + c
    return {"nodes": nodes.report(), "edges": edges.report(),
            "clean": nodes.clean() and edges.clean(), "attributed": attributed}


def diff_graphs(
    nodes_a: Iterable[Any], edges_a: Iterable[Any],  # Graph A's elements (typed or wire dicts, any order)
    nodes_b: Iterable[Any], edges_b: Iterable[Any],  # Graph B's elements
    path_map: Optional[Sequence[Tuple[str, str]]] = None,  # (old prefix, new prefix) path rewrites
    attribution: Sequence[Attribution] = (),         # The lane's known causes
) -> Dict[str, Any]:  # {nodes, edges, clean, attributed}
    """The comparison over elements already in memory (tests, small graphs): sorted by id,
    then the same merge and accumulator the streamed comparison drives."""
    out = []
    for kind, a, b in (("node", nodes_a, nodes_b), ("edge", edges_a, edges_b)):
        d = KindDiff(kind, path_map or (), attribution)
        sa, sb = (sorted(map(wire, x), key=lambda w: w["id"]) for x in (a, b))
        for wa, wb in merge_sorted(sa, sb):
            d.add(wa, wb)
        out.append(d)
    return _report(*out)


async def stream_elements(
    gx: Any,           # A started graph handle: `.queue` + `.graph_id`
    kind: str,         # "node" | "edge"
    page: int = PAGE,  # Elements per keyset page
) -> AsyncIterator[Dict[str, Any]]:  # Every element of the kind, in ascending id order
    """Every node (or edge) of a graph through the storage capability, in id-ordered keyset
    pages (`after` = the last id read); the next page is fetched while this one is consumed."""
    method, query, field = (("query_nodes", NodeQuery, "nodes") if kind == "node"
                            else ("query_edges", EdgeQuery, "edges"))

    async def fetch(after: Optional[str]) -> List[Dict[str, Any]]:
        res = await graph_task(gx.queue, gx.graph_id, method,
                               query=query(limit=page, after=after).to_dict())
        rows = getattr(res, field) if hasattr(res, field) else res.get(field)
        return [wire(x) for x in rows or []]

    pending = asyncio.ensure_future(fetch(None))
    try:
        while pending is not None:
            rows = await pending
            pending = asyncio.ensure_future(fetch(rows[-1]["id"])) if len(rows) == page else None
            for w in rows:
                yield w
    finally:
        if pending is not None:
            pending.cancel()


async def diff_stores(
    ga: Any,  # Graph A, started (`.queue` + `.graph_id`); by convention the LIVE graph
    gb: Any,  # Graph B, started; by convention its REBUILD
    path_map: Optional[Sequence[Tuple[str, str]]] = None,  # (old prefix, new prefix) path rewrites
    attribution: Sequence[Attribution] = (),  # The lane's known causes
    page: int = PAGE,                          # Elements per keyset page
    collect: bool = False,                     # Keep every one-sided id (each side's `ids`), not only a sample
) -> Dict[str, Any]:  # {nodes, edges, clean, attributed}
    """Compare two graphs element for element, streamed: O(page) memory per side (plus, with
    `collect`, the one-sided ids -- an adoption's drop list must name every one)."""
    out = []
    for kind in ("node", "edge"):
        d = KindDiff(kind, path_map or (), attribution, collect)
        async for wa, wb in amerge_sorted(stream_elements(ga, kind, page),
                                          stream_elements(gb, kind, page)):
            d.add(wa, wb)
        out.append(d)
    return _report(*out)


def _short(v: Any, width: int = 70) -> str:
    """One value, clipped for a report line."""
    s = repr(v)
    return s if len(s) <= width else s[: width - 1] + "…"


def render_head(
    res: Dict[str, Any],  # A diff report
) -> str:  # "## Rebuild diff — CLEAN · …"
    """The verdict line: CLEAN or DIFFERENCES, and what each cause accounts for."""
    head = f"## Rebuild diff — {'CLEAN' if res['clean'] else 'DIFFERENCES'}"
    causes = " · ".join(f"{n} {c}" for n, c in (res.get("attributed") or {}).items() if c)
    return head + (f" · row(s) attributed: {causes}" if causes else "")


def render_kinds(
    res: Dict[str, Any],  # A diff report
) -> List[str]:  # Report lines: per kind the counts, one-sided ids by class, the drift map, the causes
    """The drift map by kind, class and field; rows a cause accounts for print under each kind
    as `↪ <cause>`, apart from drift."""
    lines: List[str] = []
    for kind in ("nodes", "edges"):
        d = res[kind]
        lines.append(f"\n### {kind.capitalize()} — A {d['count_a']} · B {d['count_b']} · "
                     f"only in A {d['only_a']['count']} · only in B {d['only_b']['count']} · "
                     f"differing {d['differing']}")
        for side, name in (("only_a", "only in A"), ("only_b", "only in B")):
            if d[side]["count"]:
                cls = " · ".join(f"{c} {n}" for c, n in d[side]["by_class"].items())
                lines.append(f"- {name}: {cls}")
        for r in d["rows"]:
            lines.append(f"- **{r['class']}.{r['field']}** × {r['count']}")
            for s in r["samples"][:2]:
                lines.append(f"    `{s['id'][:8]}` A {_short(s['a'])} · B {_short(s['b'])}")
        for cause, m in (d.get("attributed") or {}).items():
            if m["only_a"]["count"] or m["only_b"]["count"] or m["differing"]:
                lines.append(f"- ↪ {cause}: only in A {m['only_a']['count']} · "
                             f"only in B {m['only_b']['count']} · differing {m['differing']}")
                for r in m["rows"]:
                    lines.append(f"    ↪ {r['class']}.{r['field']} × {r['count']}")
    return lines


def render_diff(
    res: Dict[str, Any],  # A diff report (with `a` / `b` paths when the caller set them)
    fmt: str = "human",   # "human" | "agent"
) -> str:  # The rendered report
    """Markdown (the verdict, the two graphs, the drift map by kind), or JSON for agents."""
    if fmt == "agent":
        import json
        return json.dumps(res, default=str)
    lines = [render_head(res)]
    lines += [f"{k.upper()} `{res[k]}`" for k in ("a", "b") if res.get(k)]
    return "\n".join(lines + render_kinds(res))
