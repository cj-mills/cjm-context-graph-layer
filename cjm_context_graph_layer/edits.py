"""The spine-edit operation vocabulary (prune / replace_text / boundary_shift) + supersession resolution + the effective-view projection. These are generic operations on any NEXT-chained text spine; correction workflows carry them in overlay-node payloads, and the projection interprets them at read time (migrates correction-core C11/C16 onto the layer)."""

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Set, Tuple

# Reserved spine-edit operation vocabulary (reserve-enum-values-up-front):
# boundary_shift is locked in NOW per the where-graph-begins resolution even
# though no driver produces it yet — the persisted decision preserves the
# alignment-error-vs-transcription-error distinction.
EDIT_OPS = ("prune", "replace_text", "boundary_shift")


class SpineEditError(ValueError):
    """A spine edit could not be validated or applied (loud, never silent)."""
    pass


@dataclass
class SpineUnit:
    """Minimal projection unit: one spine position with its effective text."""
    id: str    # Layer-0 segment node id
    text: str  # Effective text at this position


@dataclass
class SpineEdit:
    """One spine-edit decision, as carried in an overlay node's payload.

    `op` semantics:
    - `prune`: drop `targets` from the effective view (payload unused).
    - `replace_text`: payload `{"text": ...}` replaces each target's text.
    - `boundary_shift`: payload `{"boundary_after": <left segment id>,
      "text": <moved WORDS, no boundary separators>, "direction": "push"|"pull"}`
      moves whole words across the boundary between two adjacent FIXED positions
      (push = from the end of the left unit to the start of the right; pull =
      the mirror). Junctions are whitespace-NORMALIZED: the receiving side joins
      with a single space, the vacated side collapses its boundary whitespace
      (the corpus stores stripped segment texts — finding 58b2e0a0 falsified
      pure concat-invariance). Word-stream 1:1 alignment is maintained — count
      and positions never change.
    """
    edit_id: str                                  # Carrying overlay node id (supersession anchor)
    op: str                                       # One of EDIT_OPS
    targets: List[str] = field(default_factory=list)   # Layer-0 segment node ids the edit applies to
    payload: Dict[str, Any] = field(default_factory=dict)  # Op-specific payload (see above)
    created_at: float = 0.0                       # Decision timestamp (application order + latest-wins tiebreak)

    def __post_init__(self):
        if self.op not in EDIT_OPS:
            raise SpineEditError(f"unknown spine-edit op: {self.op!r} (known: {EDIT_OPS})")

    def to_dict(self) -> Dict[str, Any]:  # Payload-ready dict
        """Serialize for carriage in an overlay node payload."""
        return {"edit_id": self.edit_id, "op": self.op, "targets": list(self.targets),
                "payload": dict(self.payload), "created_at": self.created_at}

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "SpineEdit":  # Reconstructed edit
        """Reconstruct from a payload dict."""
        return cls(edit_id=d["edit_id"], op=d["op"], targets=list(d.get("targets") or []),
                   payload=dict(d.get("payload") or {}), created_at=float(d.get("created_at") or 0.0))


def resolve_active(
    edit_ids: Iterable[str],                      # Candidate overlay node ids
    supersedes_pairs: Iterable[Tuple[str, str]],  # (superseder_id, superseded_id) SUPERSEDES edges
) -> Set[str]:  # Active (non-superseded) ids
    """Resolve the active set under append-only supersession.

    An id is superseded iff it is the TARGET of any SUPERSEDES edge — chains
    resolve naturally (C supersedes B supersedes A leaves only C active), and
    nothing is ever mutated (the C16 semantics, now layer-owned).
    """
    superseded = {target for _, target in supersedes_pairs}
    return {eid for eid in edit_ids if eid not in superseded}


def project_effective_spine(
    units: List[SpineUnit],   # Ordered layer-0 spine (immutable input)
    edits: List[SpineEdit],   # ACTIVE edits to apply (resolve supersession first)
) -> List[SpineUnit]:  # New effective spine (input never mutated)
    """Project the effective view: layer-0 + active edits, resolved at read time.

    Edits apply in (created_at, edit_id) order over the evolving text state, so
    later decisions see earlier ones' effects and replace_text latest-wins
    emerges from ordering. Prunes drop positions at the end (a boundary_shift
    or replace recorded before a later prune still applies cleanly).
    boundary_shift is STRICT on the moved words (modulo boundary whitespace):
    if the current text no longer carries them at the boundary, the projection
    fails loudly rather than guessing (SpineEditError). Junctions normalize to
    a single space (58b2e0a0: word-stream invariance replaced concat-invariance
    once the stripped-text corpus convention falsified the junction premise).
    """
    order = {u.id: i for i, u in enumerate(units)}
    texts = {u.id: u.text for u in units}
    pruned: Set[str] = set()

    for e in sorted(edits, key=lambda e: (e.created_at, e.edit_id)):
        if e.op == "prune":
            pruned.update(e.targets)
        elif e.op == "replace_text":
            new_text = e.payload.get("text", "")
            for t in e.targets:
                if t in texts:
                    texts[t] = new_text
        elif e.op == "boundary_shift":
            left = e.payload.get("boundary_after")
            moved = (e.payload.get("text") or "").strip()
            direction = e.payload.get("direction", "push")
            if not moved:
                raise SpineEditError(f"boundary_shift: empty moved text ({e.edit_id})")
            if left not in order:
                raise SpineEditError(f"boundary_shift: unknown boundary_after {left!r}")
            idx = order[left]
            if idx + 1 >= len(units):
                raise SpineEditError("boundary_shift: no unit after the boundary")
            right = units[idx + 1].id
            if direction == "push":
                base = texts[left].rstrip()
                if not base.endswith(moved):
                    raise SpineEditError(f"boundary_shift push: left text does not end with the moved words ({e.edit_id})")
                texts[left] = base[: len(base) - len(moved)].rstrip()
                rtext = texts[right].lstrip()
                texts[right] = f"{moved} {rtext}" if rtext else moved
            elif direction == "pull":
                base = texts[right].lstrip()
                if not base.startswith(moved):
                    raise SpineEditError(f"boundary_shift pull: right text does not start with the moved words ({e.edit_id})")
                texts[right] = base[len(moved):].lstrip()
                ltext = texts[left].rstrip()
                texts[left] = f"{ltext} {moved}" if ltext else moved
            else:
                raise SpineEditError(f"boundary_shift: unknown direction {direction!r}")

    return [SpineUnit(u.id, texts[u.id]) for u in units if u.id not in pruned]
