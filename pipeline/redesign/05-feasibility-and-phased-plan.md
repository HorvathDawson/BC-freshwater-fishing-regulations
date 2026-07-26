# 05 — Feasibility & Phased Plan

## Is it possible? Yes.

The section model is **graph contraction + linear referencing** layered on the existing FWA
data — both are standard and the codebase already has the pieces:
- directed connectivity graph (`graph_builder.py`),
- lake-outlet resolution (`feature_resolver._wbk_to_fids`),
- name propagation heuristics (`annotate_unnamed_context`, `DisplayNameResolver`),
- the `Landmark{fid}` / `location_text` seam explicitly reserved for split resolution,
- hand-listed segment overrides (`overrides.json`) proving the manual-split workflow.

### Honest note on what actually gets faster

The 2.37 GB graph is **already build-time only**, and tributary relationships are **already
denormalized** into `tributary_reg_ids`. So the redesign does **not** shrink the shipped
client payload much by itself. The wins are:
1. **Simpler, faster enrichment** — match to sections, walk a small tree, instead of
   per-reg BFS over 2.37 GB with seed-relative WSC exclusion sets.
2. **Correctness + maintainability** — hand splits become first-class data; the
   `(gnis,location,wbk)` atom is legible; barriers become structural not runtime.
3. **Fewer grouping keys** — section replaces the dynamic `(wsc,name,reg_set)` reach key.
4. **The full micro-graph is only needed once**, to derive the contracted tree + fid→section
   mapping; after that the build can operate on the small tree.

If the real pain is *build time / memory* of the 2.37 GB + 5.4 GB artifacts, the contracted
tree helps phases 2-5 but the atlas/tiles geometry cost is unchanged (tiles still need every
fid). Set expectations accordingly.

## Risks / things to verify before committing

1. **Directionality kills backtracking? (G6.2)** — the biggest simplification hinges on the
   contracted tree being cleanly directed so an upstream walk cannot go down the mainstem.
   Verify on real confluences (Adams/Fraser) with a test before deleting the excluded-WSC
   logic. If FWA data has direction anomalies, keep a reduced form.
2. **fid → section stability** — section_id must be stable across builds so client caches /
   deep links survive. Hash from stable inputs (gnis + ordered split ids + wbk), never from
   fid order alone.
3. **Client migration** — the 6-key contract + tier0 shape is consumed by `webapp/`
   (`Map.tsx`, `featureUtils.ts`, `SearchBar.tsx`, mobile). Any schema change is a lockstep
   change. Prefer to keep the output schema identical at first (sections as an internal
   representation that still emits today's reaches/shards).
4. **Under-lake correctness** — lakes as barrier nodes must not orphan under-lake sections
   or double-count them in tributary walks.
5. **2300 edge handling at section granularity** — folding 2300 runs into sections must keep
   the enter-but-don't-exit rule; add a targeted test.

## Phased plan (each phase independently shippable & testable)

Ordered so the risky/foundational parts land first and the client contract changes last.

### Phase A — Contracted tree + section builder (internal only, no output change)
- New module e.g. `pipeline/graph/section_builder.py`: read the existing gpickle, apply
  **name overrides first**, combine micro-segments into named-stream chains, split on lakes
  (under-lake vs open), identify confluence nodes, build the contracted directed tree with
  barriers baked in (lake nodes, 2300 markers).
- Output an intermediate artifact: `sections.pkl` = `{section_id → section, fid → section_id,
  contracted_tree}`.
- Port name inheritance from `annotate_unnamed_context`.
- **Tests:** confluence collapse, under-lake separation, name inheritance parity vs current,
  directionality/no-backtracking on Adams+Fraser.
- **Verify:** section count is "much smaller" than fid count; spot-check a dozen rivers.

### Phase B — Hand-split system (`04`)
- Split-definition schema + loader (extend `overrides.json` or a new `splits.json`).
- Anchor resolver (lake/confluence/fid/landmark/point/border → position), storing resolved
  fids back. Implement `Landmark.fid` resolution.
- Section cutter + `location_identifier` auto-generator (the `04` table).
- **Tests:** 1/2/N split cases produce correct labels; Adams River yields the two real
  sections; uniqueness validation (extend `test_overrides_validation.py`).

### Phase C — Tributary walk over the tree (replace Phase 3 BFS)
- New `enrich_tributaries` that walks the contracted tree upstream from a section / lake,
  honoring lake barriers, 2300 rule, `barrier:true` splits, and `tributary_only`.
- Keep `effective_includes_tributaries` unchanged (it's parse-level).
- **Tests:** parity with current `test_tributary_logic.py` / `test_tributary_override.py`
  outputs on a fixed set of regs (golden-file compare old vs new assignments).

### Phase D — Section-based matching (simple vs complex)
- Route regs: **simple** = resolvable to a section by gnis/wbk (+ location_identifier);
  **complex** = override-only ID types / admin / multi-boundary. Fold the duplicate-override
  split rows into real section matches (retire the Adams-style duplicates).
- Base/provincial regs (Phase 4) apply to sections by spatial intersection — preserve
  `direct/admin/zone_wide` routing.
- Zones as **attributes**, not geometry splits (G8).
- **Tests:** section resolution parity; base-reg coverage unchanged.

### Phase E — Output: reach = section × reg_set, keep client contract stable
- Build reaches by partitioning each section where reg_set differs (G10). Emit the **same
  6-key index** + tier0 shape so `webapp/` needs no change initially. Ship **fid→section**
  in place of / alongside fid→reach.
- Recompute per-section minzoom (G7). Drop `filter_unnamed_depth` (G3).
- **Tests:** `test_r2_sharder.py` / `test_mobile_sharder.py` still pass; golden-file diff of
  tier0 before/after within tolerance.
- **Verify end-to-end:** run `python -m pipeline all` on a region subset, load the webapp,
  confirm Adams River shows two sections with correct regs and tributaries.

### Phase F — (optional) simplify gazette + retire dead code
- Simplify reg-side `_natural_search` to target sections.
- Remove `filter_unnamed_depth`, excluded-WSC machinery (if Phase A verified it's
  redundant), and the duplicate override split rows.
- Leave the anglerinfo live-data chain alone unless separately scoped (G4).

## Verification strategy (whole redesign)

- **Golden-file parity:** capture current `tier0.json` + a sample of fid→reg assignments as
  fixtures; assert the new pipeline reproduces them (modulo intended split improvements)
  before flipping any default.
- **Per-phase tests** as listed; reuse the existing `pipeline/tests/` harness and synthetic
  graphs (`test_tributary_logic.py` already builds one).
- **Manual spot-checks** on known-hard cases: Adams River (lake split), Wigwam (fid split),
  Fraser (huge confluence / backtracking), Seabird Island (name override), a `tributary_only`
  lake reg, a 2300 connector reach.
- **End-to-end:** `python -m pipeline all` on a subset + webapp smoke test.

## Suggested starting point when picking back up

Start with **Phase A** on the branch `redesign/stream-sections`. It is pure addition (new
module + new intermediate artifact), changes no output, and de-risks the core assumption
(that the contracted tree is clean and directional). Everything else builds on it.
