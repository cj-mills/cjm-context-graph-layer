"""Journal COMPACTION — move retired wires out of the active journal family into an archive (ruling a7617bd4).

The write journal is append-only and NEVER deleted (DEC ccbab9f5 point 1), so retired
on-graph mass — superseded decomposition spines, every one a ~1.6 MB wire-carrying op —
accumulates forever: the transcription workflow journal reached 2.4 GB with 59% of it
in spines nothing references (census 2026-09-13). Compaction is the FIRST sanctioned
mutation of a journal, and it is a MOVE, not a delete: every archived op lands, byte-
identical where whole and envelope-faithful where split, in an archive segment family
beside the journal (`<stem>.retired.NNNN.jsonl` under an archive dir the backup mirror
does not glob), with a manifest naming what moved and from where. Provenance is kept,
the active family shrinks, and a rebuild replays less.

Invariants the verb enforces (the static proof; the optional full proof is a rebuild
compared by id-set digest, `rebuild.idset_digest`):
  1. NOTHING OUTSIDE THE ARCHIVE REFERENCES A RETIRED ID after compaction — a kept op
     that still names a retired node (a correction on it, a mark, a nudge) REFUSES the
     whole run before any byte is written; deletes are the tolerated exception (a delete
     of an absent node is a replay no-op by contract).
  2. kept ∪ archived == original, op by op: a wholly-retired op moves verbatim; a MIXED
     op (a provenance backfill spanning live and retired segments) is split into a kept
     part and an archived part that share the envelope and partition the wires.
  3. The live tail is never rewritten: the family is ROTATED first so every target op
     sits in an immutable cold segment; a segment that changes underfoot between read
     and rewrite refuses.
  4. Idempotent: a second run over the same ids archives nothing.

Domain cores decide WHAT is retired (the decomp core's spine retirement facts); this
module owns the journal-file discipline only — it never touches a db.
"""

import hashlib
import json
import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from cjm_context_graph_primitives.journal import journal_segments, rotate_journal

from .journal import GENESIS_EDGE, GENESIS_NODE

_UUID_RE = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
ARCHIVE_MARK = "retired"  # `<stem>.retired.NNNN.jsonl` — the archive family's infix


class CompactRefusal(RuntimeError):
    """Compaction refused before writing: a kept op still references a retired id
    (invariant 1) or a segment changed underfoot (invariant 3)."""


@dataclass
class DanglingRef:
    """One kept op that names a retired id — the reason a compaction refuses."""
    segment: str          # Cold segment path the op lives in
    line: int             # 1-based line number within that segment
    verb: str             # The op's verb
    ids: List[str]        # The retired ids it names (sample, ≤ 8)


@dataclass
class CompactReport:
    """What a compaction did (or, dry-run, would do)."""
    label: str
    dry_run: bool
    journal_path: str
    archive_path: Optional[str] = None          # The archive segment written (None = dry run / nothing to move)
    manifest_path: Optional[str] = None         # Its sidecar manifest
    segments_scanned: int = 0
    segments_rewritten: int = 0
    ops_scanned: int = 0
    ops_archived_whole: int = 0                 # Wholly-retired ops moved verbatim
    ops_split: int = 0                          # Mixed ops partitioned kept/archived
    nodes_archived: int = 0
    edges_archived: int = 0
    bytes_before: int = 0
    bytes_after: int = 0
    retired_ids_requested: int = 0
    retired_ids_seen: int = 0                   # Requested ids that appeared as wire nodes somewhere in the family
    dangling: List[DanglingRef] = field(default_factory=list)  # Non-empty ONLY on a dry run (a live run refuses)

    @property
    def bytes_freed(self) -> int:
        return self.bytes_before - self.bytes_after

    def summary(self) -> str:  # One-paragraph human summary
        head = "DRY RUN: " if self.dry_run else ""
        return (f"{head}{self.label}: {self.ops_archived_whole} op(s) moved whole + {self.ops_split} split "
                f"({self.nodes_archived} node(s), {self.edges_archived} edge(s)) out of {self.ops_scanned} "
                f"across {self.segments_scanned} segment(s); {self.segments_rewritten} rewritten; "
                f"{self.bytes_freed / 1e6:.1f} MB freed of {self.bytes_before / 1e6:.1f}; "
                f"retired ids seen {self.retired_ids_seen}/{self.retired_ids_requested}"
                + (f"; archive {self.archive_path}" if self.archive_path else "")
                + (f"; {len(self.dangling)} DANGLING reference(s) — a live run would refuse" if self.dangling else ""))


def _stem(journal_path: str) -> str:  # "context_graph.writes" for ".../context_graph.writes.jsonl"
    name = Path(journal_path).name
    return name[: -len(".jsonl")] if name.endswith(".jsonl") else name


def archive_segments(
    journal_path: str,  # The journal's live-tail path (names the family)
    archive_dir: str,   # Where archive segments live
) -> List[str]:  # Existing archive segments for this journal, in mint order
    """The archive family beside a journal: `<stem>.retired.NNNN.jsonl` under `archive_dir`."""
    stem = _stem(journal_path)
    d = Path(archive_dir)
    if not d.exists():
        return []
    return sorted(str(p) for p in d.glob(f"{stem}.{ARCHIVE_MARK}.[0-9][0-9][0-9][0-9].jsonl") if p.is_file())


def next_archive_path(journal_path: str, archive_dir: str) -> str:  # The next archive segment path (not created)
    stem = _stem(journal_path)
    existing = archive_segments(journal_path, archive_dir)
    taken = [int(Path(p).name[len(stem) + len(ARCHIVE_MARK) + 2:][:4]) for p in existing]
    return str(Path(archive_dir) / f"{stem}.{ARCHIVE_MARK}.{max(taken, default=0) + 1:04d}.jsonl")


def _wire_partition(
    op: Dict[str, Any],
    retired: Set[str],
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], int, int]:
    """Partition a wire-carrying op's wires into (kept_op, archived_op, n_nodes, n_edges).

    kept_op is None when everything moved; archived_op is None when nothing did.
    An edge moves when EITHER endpoint is retired (a dangling edge can never be
    replayed onto an absent node)."""
    w = op.get("wires") or {}
    nodes = list(w.get("nodes") or [])
    edges = list(w.get("edges") or [])
    keep_n = [n for n in nodes if n.get("id") not in retired]
    move_n = [n for n in nodes if n.get("id") in retired]
    keep_e = [e for e in edges if e.get("source_id") not in retired and e.get("target_id") not in retired]
    move_e = [e for e in edges if e.get("source_id") in retired or e.get("target_id") in retired]
    if not move_n and not move_e:
        return op, None, 0, 0
    envelope = {k: v for k, v in op.items() if k != "wires"}
    archived = {**envelope, "wires": {"nodes": move_n, "edges": move_e}}
    if not keep_n and not keep_e:
        return None, archived, len(move_n), len(move_e)
    kept = {**envelope, "wires": {"nodes": keep_n, "edges": keep_e}}
    return kept, archived, len(move_n), len(move_e)


def _genesis_retired(op: Dict[str, Any], retired: Set[str]) -> bool:
    a = op.get("args") or {}
    if op.get("verb") == GENESIS_NODE:
        return a.get("id") in retired
    if op.get("verb") == GENESIS_EDGE:
        return a.get("source_id") in retired or a.get("target_id") in retired
    return False


def _references(text: str, retired: Set[str]) -> List[str]:
    """Retired ids named anywhere in an op's serialized text (sample ≤ 8)."""
    hits: List[str] = []
    for m in _UUID_RE.finditer(text):
        s = m.group(0)
        if s in retired and s not in hits:
            hits.append(s)
            if len(hits) >= 8:
                break
    return hits


def _kept_reference_text(op: Dict[str, Any]) -> str:
    """The op as text for the dangling-reference scan, with `deletes` blanked —
    a delete of an absent node is a tolerated replay no-op, so a delete naming a
    retired id is legal residue, not a dangling reference."""
    if "deletes" in op:
        op = {k: v for k, v in op.items() if k != "deletes"}
    return json.dumps(op, sort_keys=True)


def compact_journal(
    journal_path: str,          # The journal's live-tail path (the family is resolved from it)
    archive_dir: str,           # Archive directory (created); MUST lie outside the backup mirror's globs
    retired_ids: Iterable[str], # Node ids whose wires move (edges touching them move too)
    *,
    label: str,                 # What this compaction is (rides the manifest + split-op markers)
    dry_run: bool = False,      # Scan + report; write nothing, rotate nothing
    actor: str = "",            # Who ran it (manifest attribution)
) -> CompactReport:
    """Move every wire on the retired ids out of the journal family into ONE new archive segment.

    Order of operations on a live run: rotate the tail (invariant 3) -> scan every cold
    segment, classifying ops and collecting dangling references -> REFUSE if any kept
    op names a retired id (invariant 1; deletes exempt) -> write the archive segment +
    manifest -> rewrite each changed segment atomically (tmp + os.replace), refusing if
    its size/mtime moved since the scan. A dry run performs the scan only and reports
    the dangling references instead of refusing.
    """
    retired: Set[str] = {str(i) for i in retired_ids}
    report = CompactReport(label=label, dry_run=dry_run, journal_path=journal_path,
                           retired_ids_requested=len(retired))
    if not retired:
        return report
    if not dry_run:
        rotate_journal(journal_path)  # close the tail: targets now sit in immutable cold segments
    segments = [s for s in journal_segments(journal_path) if s != journal_path]  # never the live tail
    if dry_run and Path(journal_path).exists():
        segments.append(journal_path)  # a dry run may READ the tail to count what a rotation would expose

    archived_lines: List[str] = []
    rewrites: Dict[str, Tuple[List[str], Tuple[int, float]]] = {}  # seg -> (kept lines, (size, mtime) at scan)
    seen: Set[str] = set()
    for seg in segments:
        st = os.stat(seg)
        stamp = (st.st_size, st.st_mtime)
        report.segments_scanned += 1
        report.bytes_before += st.st_size
        kept: List[str] = []
        changed = False
        with open(seg, "r", encoding="utf-8", newline="") as fh:
            for lineno, raw in enumerate(fh, start=1):
                if not raw.strip():
                    kept.append(raw)
                    continue
                report.ops_scanned += 1
                op = json.loads(raw)
                verb = op.get("verb", "")
                if _genesis_retired(op, retired):
                    archived_lines.append(raw if raw.endswith("\n") else raw + "\n")
                    report.ops_archived_whole += 1
                    if verb == GENESIS_NODE:
                        report.nodes_archived += 1
                        seen.add((op.get("args") or {}).get("id"))
                    else:
                        report.edges_archived += 1
                    changed = True
                    continue
                kept_op, archived_op, n_n, n_e = _wire_partition(op, retired)
                if archived_op is not None:
                    for n in archived_op["wires"]["nodes"]:
                        seen.add(n.get("id"))
                    report.nodes_archived += n_n
                    report.edges_archived += n_e
                    changed = True
                    if kept_op is None:
                        archived_lines.append(raw if raw.endswith("\n") else raw + "\n")
                        report.ops_archived_whole += 1
                        continue
                    archived_op["compaction"] = {"split": True, "label": label,
                                                 "from": Path(seg).name, "line": lineno}
                    archived_lines.append(json.dumps(archived_op, sort_keys=True) + "\n")
                    kept_op = dict(kept_op)
                    kept_op["compaction"] = {"split": True, "label": label, "archived_nodes": n_n,
                                             "archived_edges": n_e}
                    report.ops_split += 1
                    kept_text = json.dumps(kept_op, sort_keys=True) + "\n"
                else:
                    kept_text = raw
                    kept_op = op
                refs = _references(_kept_reference_text(kept_op), retired)
                if refs:
                    report.dangling.append(DanglingRef(segment=seg, line=lineno, verb=verb, ids=refs))
                kept.append(kept_text)
        if changed:
            rewrites[seg] = (kept, stamp)
            report.segments_rewritten += 1
            report.bytes_after += sum(len(x.encode("utf-8")) for x in kept)
        else:
            report.bytes_after += st.st_size
    report.retired_ids_seen = len(seen & retired)

    if report.dangling and not dry_run:
        sample = "; ".join(f"{Path(d.segment).name}:{d.line} {d.verb} -> {', '.join(i[:8] for i in d.ids)}"
                           for d in report.dangling[:6])
        raise CompactRefusal(
            f"compaction {label!r} REFUSED: {len(report.dangling)} kept op(s) still reference retired "
            f"ids (the retire gate should have caught these dependents) — {sample}"
            + (" …" if len(report.dangling) > 6 else ""))
    if dry_run or not archived_lines:
        return report

    # Write the archive + manifest BEFORE any segment is rewritten: a crash between the
    # two leaves duplicates (idempotent under replay), never a loss.
    Path(archive_dir).mkdir(parents=True, exist_ok=True)
    archive_path = next_archive_path(journal_path, archive_dir)
    with open(archive_path, "w", encoding="utf-8", newline="") as out:
        out.writelines(archived_lines)
        out.flush()
        os.fsync(out.fileno())
    digest = hashlib.md5("".join(archived_lines).encode("utf-8")).hexdigest()
    manifest = {
        "label": label, "actor": actor, "ts": time.time(), "journal": journal_path,
        "archive": archive_path, "archive_md5": digest,
        "retired_ids_requested": report.retired_ids_requested, "retired_ids_seen": report.retired_ids_seen,
        "ops_archived_whole": report.ops_archived_whole, "ops_split": report.ops_split,
        "nodes_archived": report.nodes_archived, "edges_archived": report.edges_archived,
        "bytes_before": report.bytes_before, "bytes_after": report.bytes_after,
        "segments_rewritten": sorted(Path(s).name for s in rewrites),
    }
    manifest_path = archive_path[: -len(".jsonl")] + ".manifest.json"
    Path(manifest_path).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    report.archive_path = archive_path
    report.manifest_path = manifest_path

    for seg, (kept, stamp) in rewrites.items():
        st = os.stat(seg)
        if (st.st_size, st.st_mtime) != stamp:
            raise CompactRefusal(f"segment {seg} changed underfoot during compaction {label!r} "
                                 f"(archive {archive_path} already written; the rewrite of THIS "
                                 f"segment was skipped — re-run: replay stays idempotent)")
        tmp = seg + ".compact-tmp"
        with open(tmp, "w", encoding="utf-8", newline="") as out:
            out.writelines(kept)
            out.flush()
            os.fsync(out.fileno())
        os.replace(tmp, seg)
    return report


def scan_references(
    journal_path: str,          # The journal's live-tail path
    retired_ids: Iterable[str], # Ids that must NOT be named anywhere in the family
) -> List[DanglingRef]:  # Every op naming one of them (deletes exempt)
    """The post-compaction static check: the family names none of the retired ids."""
    retired = {str(i) for i in retired_ids}
    out: List[DanglingRef] = []
    for seg in journal_segments(journal_path):
        with open(seg, "r", encoding="utf-8", newline="") as fh:
            for lineno, raw in enumerate(fh, start=1):
                if not raw.strip():
                    continue
                quick = _references(raw, retired)
                if not quick:
                    continue
                op = json.loads(raw)
                refs = _references(_kept_reference_text(op), retired)
                if refs:
                    out.append(DanglingRef(segment=seg, line=lineno, verb=op.get("verb", ""), ids=refs))
    return out
