# cjm-context-graph-layer

<!-- generated from the context graph by `cjm-context-graph readme` — do not edit by hand; edit the graph (the urge to hand-edit = move it on-graph) -->

Domain-neutral graph-aware layer for context graphs: deterministic node identity, spine and overlay grammar, spine-edit operations with effective-view projection, supersession resolution, idempotent emission/extension, and provenance-by-declaration.

## Modules

- **`cjm_context_graph_layer.declare`** — Provenance-by-declaration: host logic stays readable Python in the workflow core and DECLARES its provenance contributions as a Derivation event node (+ DERIVED_FROM input edges, PRODUCED output edges). This recovers audit completeness without the substrate executing host logic (pass-2 Thread 4's false-dichotomy resolution). The substrate stays untouched: declarations read composition/job ids from the outside.
- **`cjm_context_graph_layer.edits`** — The spine-edit operation vocabulary (prune / replace_text / boundary_shift) + supersession resolution + the effective-view projection. These are generic operations on any NEXT-chained text spine; correction workflows carry them in overlay-node payloads, and the projection interprets them at read time (migrates correction-core C11/C16 onto the layer).
- **`cjm_context_graph_layer.grammar`** — The domain-neutral context-graph grammar: spine relations (NEXT / PART_OF / STARTS_WITH, recurring fractally at every layer), overlay relations (SUPERSEDES / DERIVED_FROM / PRODUCED), root kinds, and the standardized attribution fields.
- **`cjm_context_graph_layer.identity`** — Deterministic node/edge identity: UUIDv5 over canonical identity tuples (stage-5 ratified rule: a node's id derives from what makes it THE same node across re-derivation, never from its correctable content).
- **`cjm_context_graph_layer.journal`** — Journal replay for workflow graphs — the genesis baseline + the pluggable verb registry.
- **`cjm_context_graph_layer.ops`** — Queue-touching layer operations: the shared graph_task helper (task channel), idempotent emission (emit-if-absent + verify-if-present), and extend_graph — the one primitive every graph-extending workflow commits through. Deterministic ids (see identity) make idempotency a presence check instead of a search.

## API

### `cjm_context_graph_layer.declare`

- `Derivation` _class_ — One host-logic transformation event, declared for the audit trail.
- `derivation_to_graph` _function_ — Materialize a declaration as one event node + DERIVED_FROM / PRODUCED edges.

### `cjm_context_graph_layer.edits`

- `SpineEdit` _class_ — One spine-edit decision, as carried in an overlay node's payload.
- `SpineEditError` _class_ — A spine edit could not be validated or applied (loud, never silent).
- `SpineUnit` _class_ — Minimal projection unit: one spine position with its effective text.
- `project_effective_spine` _function_ — Project the effective view: layer-0 + active edits, resolved at read time.
- `resolve_active` _function_ — Resolve the active set under append-only supersession.

### `cjm_context_graph_layer.grammar`

- `OverlayRelations` _class_ — Overlay/derivation relations — the trust grammar shared by every
- `SpineRelations` _class_ — Structural spine relations, reused fractally at every layer
- `attribution` _function_ — Standardized attribution fields for derived/asserted nodes.
- `grouped_spine_edges` _function_ — Spine edges for a fine layer grouped under coarse parents.
- `make_edge` _function_ — Build an edge wire dict with a deterministic id by default.
- `spine_edges` _function_ — The uniform spine pattern at any layer: PART_OF child->parent for each

### `cjm_context_graph_layer.identity`

- `canonical_part` _function_ — Render one identity-tuple part canonically.
- `derive_edge_id` _function_ — Derive a deterministic edge id from (source, target, relation).
- `derive_node_id` _function_ — Derive a deterministic node id from a kind + identity tuple.

### `cjm_context_graph_layer.journal`

- `apply_wires` _function_ — The generic replay handler for wire-carrying ops (the single wires-replay authority).
- `genesis_export` _function_ — One-time whole-db GENESIS BASELINE: journal every node + edge as genesis ops.
- `journal_extend` _function_ — Idempotent extend + journal the DELTA — the pipeline-write append-through.
- `replay_journal` _function_ — Re-apply every journaled op in append order — the db-from-journal rebuild.
- `sidecar_journal_path` _function_ — The db's sidecar journal path (DEC ccbab9f5 point 3: placement is per-workflow,
- `wires_handlers` _function_ — Convenience registry: every named verb replays via `apply_wires`.

### `cjm_context_graph_layer.ops`

- `ExtendResult` _class_ — Outcome of one idempotent extend_graph commit.
- `GraphIntegrityError` _class_ — An emitted node collided with an existing node of different identity content.
- `extend_graph` _function_ — Idempotently extend the graph: emit-if-absent + verify-if-present.
- `graph_task` _function_ — Invoke a graph-storage adapter method through the queue's task channel.
- `node_identity_mismatch` _function_ — Verify-if-present check: label + sources content-hash set must match.
- `partition_by_presence` _function_ — Split wire dicts into absent (to add) and present (to verify).

## Dependencies

**Depends on:** `cjm-context-graph-primitives`, `cjm-substrate`
**Used by:** `cjm-context-graph-projection`, `cjm-dev-graph-schema`, `cjm-markdown-decompose-core`, `cjm-notebook-decompose-core`, `cjm-transcript-correction-core`, `cjm-transcript-correction-tui`, `cjm-transcript-decomp-core`, `cjm-transcript-graph-schema`, `cjm-transcription-core`
