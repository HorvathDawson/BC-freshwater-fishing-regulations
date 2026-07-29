# 15 — Phase 5 (Match) — Plan & Handoff

**Status:** DESIGN COMPLETE, not yet implemented. All decisions locked (Q1/Q2/Q3 below).
Concretizes `08-matching-and-invariants.md` into an executable plan. Supersedes the memo-era
per-reg framing in favor of a **split-into-uniform-reaches → link-rules-by-selector** model.
Derived from four expert design investigations (corrections taxonomy, core resolver, tributary+
MU overlay, and a 4-way Q1/Q2 dive). Self-contained: a fresh agent can resume from this file.

---

## 1. Context / why

Phase 5 ("match") resolves parsed regulation rows onto the new **sections**. Everything upstream
is built & tested: the inverted stream graph (nodes = stream pieces cut at lakes + curated
splits, or lake nodes; edges = "flows into"), `(name,source)` name tuples, curated `splits.json`,
border/`out_of_bc`, tributary reachability (guarded ancestors: WSC-descendant filter +
`EDGE_TYPE=2300` barrier), and the EXCEPT set-difference algebra (`stream_sections/tributaries.py`).

Legacy truth = flat `pipeline/matching/overrides.json` (480 corrections) over a generated
`output/pipeline/matching/match_table.json` (1395 rows: 951 name-resolved, 444 `not_found`). Those
outputs are known-correct → keep as a **golden oracle / test corpus**, but the SHAPE is redesigned
for data integrity and to fix two modelling gaps the legacy system papered over:
- **Rule-level spatial scope** (Coquihalla — §3): one entry's rules cover different reaches.
- **Per-rule tributary inclusion** (Anderson Lake — §2b): only one rule of an entry includes tribs.

Data reality (measured): override field usage — gnis_ids 298, name_variants 235, waterbody_keys
99, canonical_name 45, skip 38, fwa_watershed_codes 34, variant_of 17, admin_targets 6,
ungazetted 5, only_within_zones 4, blue_line_keys 2, linear_feature_ids 2, waterbody_poly_ids 2.
Legacy override precedence (most-stable first): gnis_ids > waterbody_keys > fwa_watershed_codes >
blue_line_keys > linear_feature_ids > waterbody_poly_ids; admin spatial; only_within_zones prunes;
override beats base (resolved in `MatchTable` at key `(region, name_verbatim, frozenset(mus))`).

---

## 2. The model — "split into uniform reaches, then link rules by selector"

Organize around FOUR kinds of regulation match, and do ALL splitting BEFORE matching so splits
define the reaches; then attach each **RULE** (not whole reg) to the sections it covers.

**A section = a maximal reach of uniform regulatory context.** Split every stream up front at:
- lakes + BC border — DONE
- **curated `mu_boundary` splits** — only Fraser-type named per-zone differences (NOT auto-MU; §5 Q1)
- **national-park + ecological-reserve boundaries** — hard closures only (§5 Q2)
- **curated rule-landmark points** (from rule `location_text`; e.g. Othello Tunnels — §3)
- EXCEPT confluences — DONE (`tributaries.py`)

Then each RULE attaches to sections through ONE selector, tagged with provenance:
1. **MU selector** → base/zone regs → an `mu_id → base_reg_set` overlay (NOT on sections).
2. **Area selector** → closures split + gate; non-closure area/access regs = notice-on-intersection.
3. **Waterbody selector** → named regs → the waterbody's sections, narrowed by the rule's
   `location_text` to the reach between landmark splits.
4. **Tributary selector** → inherited → precomputed closure of the waterbody's sections (per-rule).

A section's reg set = union of attached rules. Uniform context ⇒ NO within-section conflict
("one reg set per section" is automatic). Coquihalla's 5 landmark-scoped rules just work.

**Storage stays lean:** base regs stored ONCE per MU (overlay); non-closure area/access notices
stored ONCE per polygon and referenced by intersecting sections; only waterbody-specific +
inherited rules attach directly. No per-reach flattening.

### 2b. CORRECTED tributary semantics (PIN AS SPEC)
- Entry-level `includes_tributaries=true` ⇒ EVERY rule includes tributaries; the ONLY way to
  remove tributary water is an EXCEPT clause inside a rule. (No per-rule opt-out.)
- Entry-level absent/false ⇒ an individual RULE may still include tributaries. Example —
  "ANDERSON" LAKE (1-3): only the *Trout/kokanee catch-and-release* rule says `[Includes
  Tributaries]`; the gear rules do not. Only that rule propagates.
- `tributary_only` ⇒ all rules apply to tributaries but NOT the mainstem (mainstem excluded).
- EXCEPT inside a rule ⇒ subtract those sub-basins from THAT rule's closure.
- ⇒ tributary inclusion is a per-RULE property ⇒ attachment is at RULE granularity, not reg.
  (Legacy `effective_includes_tributaries` in `pipeline/enrichment/feature_resolver.py:33` is a
  useful oracle for the entry/rule interaction, but its "all rules opt out ⇒ no expansion" branch
  must NOT carry over — global-true forces all rules on.)

---

## 3. Rule-level spatial scope — the Coquihalla case

A single named-waterbody entry can contain rules that apply to DIFFERENT reaches. Coquihalla
River (GNIS 19983):
- No Fishing **upstream of** the N entrance of the upper-most railway tunnel (Nov1–Jun30)
- Fly-only + bait ban **upstream of** the N entrance of the upper-most railway tunnel (Jul1–Oct31)
- No Fishing **downstream of** the S entrance of the lower-most railway tunnel (Apr1–Oct31)
- No Fishing **at Othello Tunnels, from** the upper N entrance **to** the lower S entrance (~700 m)
- Trout/char (incl. steelhead) C&R + bait ban **downstream of** the S entrance (Nov1–Mar31)

⇒ the two tunnel-entrance landmarks become curated `point` splits; the 5 rules then attach to the
upstream / tunnel-interior / downstream sections via the **waterbody selector** narrowed by each
rule's `location_text`. Proof that the atom must be the RULE + its scope.

### 3b. Othello splits — APPLIED to `stream_sections/splits.json`
Two `point` anchors added (targeting Coquihalla by `gnis_id 19983`; `_target_blks` resolves
gnis_id → blk[s]), coords `[lon, lat]`, `is_lonlat: true`, `proximity_m: 200`:
- `coquihalla_othello_upper_tunnel` — coord `[-121.36979, 49.37443]`, label "the upper Othello
  railway tunnel (N entrance)".
- `coquihalla_othello_lower_tunnel` — coord `[-121.36458, 49.36894]`, label "the lower Othello
  railway tunnel (S entrance)".
Source: page-24 regulation map. NOT yet built/verified — run `stream_sections/build.py` on the
Coquihalla extent and confirm both cuts land (`splits.resolved.json` + gpkg `split_points`) and
that the river yields upstream / tunnel-interior / downstream sections.

---

## 4. Kept from the design investigations (carry + expand)

### 4a. Correction taxonomy + gotchas report — high value
Replace the flat `overrides.json` with first-class typed `Correction` records (new module
`stream_sections/corrections.py`), reusing `NameTuple`/`NameSource`. ~16 typed categories, each
with slug/severity/confidence + machine-checkable assertions; classify all 480 + synthesize the
444 `not_found` as `unresolved_not_found`. Categories (≈counts): name_gnis_mismatch(55),
parenthetical_qualifier(131), mu_boundary(40), tributary_link(37), combined_multi(19),
multipolygon_gis(18), ungazetted_from_map(79), ungazetted_custom(3), admin_polygon(6),
reach_segment(6), zone_scoped(4), out_of_region(8, Haida Gwaii), ambiguous_candidate(10),
todo_subdivision(7), skip_crosslist(38), + unresolved_not_found(444).
Build-time **gotchas report** (`gotchas_report.json`/`.csv`): real synopsis errors (name≠gnis +
MU-boundary + out-of-region), spelling deltas, ambiguous multi-candidate, unresolved/not_found
(ratcheted so it can't grow), combined, ungazetted, TODO. FAIL loud on: zero-resolved non-skip
correction, name_ne_gazette that no longer holds, dup correction_id, non-unique
`location_identifier` within a gnis, dangling `variant_of`.
EXPAND for the reframe: add a **"rule landmark unresolved"** gotcha — a rule says "upstream of X"
but no split exists at X ⇒ report + prompt to author a split (how Coquihalla-type gaps surface).

### 4b. Unified locator abstraction — the waterbody selector
One typed `Locator` enum {section_id, gnis, name, blk, wsc, lfid, wbk, wbk_poly, admin_poly,
coord} → `frozenset[section_id]` via a `SectionIndex` (by_gnis / by_name / by_blk[measure-ordered]
/ by_wsc / by_member_fid / by_lake_wbk / poly_to_wbk / boundary_by_label). Combined entries =
longer `targets` tuple (union). `location_identifier` **narrowing** matches SectionBoundary
labels (the SAME vocabulary that builds the label): whole / upstream_of / downstream_of / between
/ at_lake, resolved via `tributaries.piece_above` + `sections_in_reach` by route measure. FAIL
LOUD: NarrowingUnresolved / NarrowingAmbiguous / RegSetConflict. Geometry decoupled — STRtrees
built only if an admin_poly/coord locator exists. All emitted collections `tuple(sorted(...))`.

### 4c. Tributary + MU — reuse graph algebra, reshape the rest
`tributaries.py` is DONE (ancestors, with_tributaries, reach_except, lake_tributaries,
piece_above, sections_in_reach) and tested — do NOT reimplement. New = the thin per-RULE attach
step + corrected semantics (§2b). Precompute lake tributary closures onto lake sections at graph
time (zero graph walk at match time). Base regs three-branch (docs/07 Q3): direct-target base
regs → sections as named regs; admin/park → area selector; zone-wide/provincial → `_resolve_mu_set`
(zone_ids→MUs + include−exclude, `base_reg_assigner.py:292`) → MU overlay.

---

## 5. Decisions (Q1/Q2/Q3)

### Q3 — DECIDED: tributary inheritance stops at NAMED LAKES
Keep `EDGE_TYPE=2300` barrier (already in graph build: `StreamNode.is_barrier` +
`ancestors(guarded=True)`). Extend the guarded ancestor walk so a named-lake node halts upward
propagation (replaces legacy `lake_barrier_wbks`). Golden test on Atnarko/Hunlen.

### Multi-zone base-reg DISPLAY (resolves the fragile-click concern)
When a clicked section spans multiple MUs (`len(mu_ids) > 1`), the client must NOT show only the
clicked point's zone:
- Show **all** spanned zones' base regs in the panel, each labelled (e.g. "Region 2 / Region 3").
- **Simultaneously highlight the MU polygon(s) + the boundary line** crossing the highlighted
  stream, so the user sees where regs change.
- The clicked point only pre-selects/scrolls to the relevant zone; it never hides the others.
This makes the straddle a feature (zones visible on the map) and is why we DON'T split on MU.

### Q1 — DECIDED: overlay + curated exceptions (+ merge_group_id)
Investigation (2 memos: pro-split 70% + skeptic 85%) converged on: base regs are an
`mu_id → base_reg_set` **overlay**, NEVER flattened onto sections; `Section.mu_ids` (plural) is
honest for straddlers; do NOT blind-cut where a river IS the boundary (collinear weave gives an
arbitrary, non-deterministic median cut; braiding puts sibling channels in different MUs;
`section_id` churns on WMU re-fetch).
- **DECISION:** overlay-by-default (docs/07) + curated `mu_boundary` splits ONLY where a NAMED
  water genuinely differs by zone (Fraser Zone 2≠3, already in `splits.json`). The client shows
  the base regs for the ONE clicked/located MU — never both. No auto-MU cutting.
- **ADOPT INDEPENDENTLY — `merge_group_id`:** build-time union-find over graph adjacency where
  `SectionRegs.reg_set_index` is equal + same stream ⇒ a stable hash of sorted member section_ids.
  Client renders one dissolved clickable feature; the MU overlay still switches at the real line.
  De-clutters ANY over-splitting (lakes/landmarks too).
- **FUTURE (gated):** auto-split clean transverse MU crossings, fail-closed on collinear overlap,
  merged for display — only after measuring how much of the 44,841 km internal MU boundary is
  river-following overlap vs clean crossing.

### Q2 — DECIDED: split only hard closures; NOTICE-on-intersection for everything else
One rule: **split geometry only for hard on/off fishing closures; for every other area/admin
layer, attach a NOTICE to any section whose geometry intersects the polygon at all** (coarse but
correct for advisories — how land-use already behaves). Three tiers:
- **Tier 1 — CLOSURE SPLIT (geometry).** New `AnchorType.area_boundary` + `BoundaryKind.area`,
  reusing the `border.py` polygon-boundary splitter. The closure is a first-class GATE flag on the
  inside section (e.g. `SectionRegs.area_closure = reg_id`), NOT a `named_reg_id`, and EXCLUDED
  from the tributary inheritance walk. Two flavours of the SAME anchor:
  - **BLANKET** — national parks (`parks_nat`) + true ecological reserves (`parks_bc` `OI`). Cut
    ALL streams at the boundary; every inside piece is closed.
  - **CURATED (reg-driven, tributary-scoped)** — when a SPECIFIC reg names an area as a closure,
    author an `area_boundary` split on THAT polygon, scoped to the named water + its tributaries.
    - **Garibaldi / Pitt River:** "No fishing within Garibaldi Park [Includes Tributaries]." Cut
      Pitt River AND its tributary closure at the **park polygon boundary** (NOT a single point —
      tributaries cross the boundary in many places, and some leave the Pitt then re-enter the
      park; only cutting at the boundary geometry catches them). Inside pieces (mainstem + inside
      tributary pieces) get the closure gate. Keep coord (49.73752, -122.75042) in `_concern` as a
      human-verify reference. ⇒ the area-split MUST support **tributary expansion of the cut**
      (a point split cannot do this).
    - **Chilkoot Trail NHS:** the ONE relevant polygon in the 5,696-feature `historic_sites` layer
      (strict federal no-fishing, contact Parks Canada). Do NOT process that layer — author a
      single curated `area_boundary` closure split for Chilkoot Trail waters.
- **Tier 2 — FISHING-AXIS NOTICE (no split, BUILD-TIME attach-on-intersection):** WMA, provincial
  parks (`parks_bc` PP/PA/RC), watersheds. Feature-specific, non-closure (Creston WMA = permit;
  Liard Watershed = extra regs; Kikomun = lakes-only; Bowron carve-out). At BUILD time, every
  section whose geometry intersects the polygon (STRtree pass) gets that polygon's notice on the
  WHOLE section — NOT resolved by clicked point. (Rationale: a section that poked into a WMA
  highlights entirely; a click on the outside part must still show the notice.) Named area-scoped
  regs (Strathcona, Creston) resolve via `admin_targets` containment → ordinary `named_reg_id`s.
  WMA is NOT split (permit ≠ closure; 35 splits buy precision we don't need). If a specific WMA
  ever carries a hard closure, curate a Tier-1 closure split for it.
- **Tier 3 — ACCESS-AXIS NOTICE (no split, BUILD-TIME attach, separate banner):** land_access /
  parcels (private/crown) / **aboriginal lands** (treated as land-use, NOT a fishing reg). Models
  "can I get there," not "can I fish here"; OSM-soft. Every intersecting section gets the access
  notice at build time — today's land-use behavior. Never merged into the fishing-reg stack.
- **PRECEDENCE — two orthogonal axes:** Axis A (access/tenure, Tier 3) = always-visible banner,
  never resolved against fishing regs. Axis B (fishing) = closure-gate (Tier-1, most-restrictive)
  → named water → tributary-inherited → **area/admin notice (Tier-2)** → MU base (least-specific).
- **CORRECTNESS FLAG:** never render "closed" for PROVINCIAL parks — only national parks +
  eco-reserves are blanket closures.
- **Notice storage:** intersecting Tier-2/3 polygon notices stored as REFERENCES (polygon/reg id)
  via an STRtree intersection pass — not flattened text. Small set (WMA ~101 BLKs, land_access
  ~12 sampled).
- **New/hard bit (medium confidence):** subdividing a LAKE NODE where a Tier-1 boundary bisects a
  lake (the `lake` anchor cuts streams AT a lake, not the lake polygon). Rare.
- Data readiness: all target layers already fetched + loaded as `AdminRecord`s (parks_nat 102,
  parks_bc 930, wma 35, land_access 136, aboriginal 929). Caveat: stale `land_parcels_private`
  (1.29M) + `osm_admin_boundaries` sit unused in the GPKG — don't reintroduce.
- **Layer separation (for client toggling):** partition `parks_bc` (`TA_PARK_ECORES_PA_SVW`) into
  SEPARATE output layers by `PROTECTED_LANDS_CODE` — ecological_reserves / provincial_parks /
  protected_areas / recreation_areas — from the one fetch. Enables independent layer toggles and
  makes "split eco-reserves only" clean.
- **Waterfalls → obstacles (full replacement):** the OSM `waterfalls` layer is fully superseded by
  `WHSE_FISH.FISS_OBSTACLES_PNT_SP` (obstacles). Remove `waterfalls`, don't just alias it.

---

## 5b. Client display model (render order) + open judgment calls

**Render order for a clicked section (top shown first):**
```
[ Axis A banner ]  access/tenure notice (Tier-3): land-use / private / crown / aboriginal — if any
[ CLOSURE GATE ]   Tier-1 closure (national park / eco-reserve / Garibaldi / Chilkoot) — "Closed"
[ THIS WATER ]     named-water regs  +  tributary-inherited (provenance-tagged)
[ AREA NOTICE ]    Tier-2 admin notices: WMA permit, named watershed, provincial-park note
[ GENERAL ]        MU base regs — ALL spanned zones if len(mu_ids)>1, + MU polygons highlighted
```
Axis A never resolves against Axis B; the closure gate greys/contextualises the tiers below it
("regs that would apply if opened under permit").

**Open judgment calls (NOT yet locked — flag for review):**
- Multi-zone base-reg display (show all spanned zones + highlight MUs) — PROPOSED fix for the
  fragile-click concern; confirm UX.
- Lake-node bisection where a Tier-1 boundary bisects a lake — medium confidence; may curate/defer.
- Label wording for `mu_boundary` / area splits + `location_identifier` phrasing.
- Whether `merge_group_id` ships in v1 or as a follow-up.
- Authoring source: keep flat `overrides.json` and generate `corrections.json`, vs author
  corrections natively (migration is lossless either way).

## 6. Build order (decisions locked)

**Stage 0 — Geometry/split additions:**
- ✅ Othello point splits applied to `splits.json` (§3b). *(done; not yet built/verified)*
- Add `AnchorType.area_boundary` + `BoundaryKind.area` (`models.py`); generalize `border.py`
  splitter to cut streams at a polygon boundary. Support BOTH: BLANKET (national parks +
  true-eco-reserves, cut all streams) and CURATED reg-driven (cut a named water + its TRIBUTARY
  closure at a specific polygon boundary). Lake-node bisection deferred/curated.
- Author curated closure splits: **Garibaldi Park** boundary scoped to Pitt River + tributaries;
  **Chilkoot Trail NHS** waters. (`splits.json`, `area_boundary` anchor, tributary-scoped.)
- Partition `parks_bc` into separate layers by `PROTECTED_LANDS_CODE` (`data/fetch_data.py` /
  `freshwater_atlas.py`); remove `waterfalls` in favour of `obstacles`.
- Named-lake **tributary barrier** (Q3) in the guarded ancestor walk (`graph.py`/`tributaries.py`).
- (No auto-MU splitting; curated `mu_boundary` only.)

**Stage 1 — Corrections as typed data:** `stream_sections/corrections.py` + one-off
`corrections_from_overrides.py` migrating 480 + synthesizing 444 → `corrections.json`. Golden
category-histogram test.

**Stage 2 — Waterbody selector / resolver:** new `stream_sections/matching/` — `SectionIndex` +
`Locator` dispatch + `location_identifier` narrowing (`piece_above`/`sections_in_reach`). Fail-loud.

**Stage 3 — Rule-level attachment (4 selectors) → `SectionRegs`:**
- Per-RULE scope: `location_text` → `Narrowing`; per-rule tributary flag per §2b.
- Selector 3 (waterbody) → `named_reg_ids` per rule.
- Selector 4 (tributary) → per-rule expansion over precomputed `tributary_section_ids` + EXCEPT
  (`reach_except`); named-lake barrier; `tributary_only` guard (mainstem excluded).
- Selector 2 (area) → Tier-1 closure gate; Tier-2/3 notices via STRtree intersection.
- Selector 1 (MU) → `mu_id → base_reg_set` overlay via `_resolve_mu_set` (NOT on sections).
- Assemble `SectionRegs` (dedup `reg_set_index`, provenance); fail-loud `RegSetConflict`.

**Stage 4 — Overlays / notices / merge groups:** MU overlay artifact; Tier-2/3 notice tables;
`merge_group_id` union-find (adjacency ∩ equal `reg_set_index` ∩ same stream).

**Stage 5 — Gotchas report:** `corrections_report.py` → `gotchas_report.json`/`.csv` (+
rule-landmark-unresolved); fail-loud gates; not_found ratchet.

**Stage 6 — Tests + golden parity (§7).**

---

## 7. Verification

- **Golden parity:** map legacy `resolve_features` fids/wbks → owning section_ids; assert new
  resolver's section coverage equals it for the 951 name rows + 480 overrides; 444 not_found stay
  not_found. Compare COVERAGE sets (base regs no longer per-section is an intentional diff).
- **Segment cases (new):** Coquihalla 5 rules → upstream/tunnel/downstream sections; Adams River
  up/down of Adams Lake → 2 sections; Atnarko/Bella Coola EXCEPT via `reach_except`.
- **Area/notice tests:** national-park boundary splits a stream inside/outside; inside section
  carries closure GATE (not a `named_reg_id`) and is excluded from tributary inheritance; Tier-2
  WMA/watershed/historic notice attaches on intersection; Tier-3 land-use notice on intersection;
  a PROVINCIAL park does NOT get a "closed" gate.
- **MU/overlay/merge tests:** base reg → `mu_id → base_reg_set` (nothing on sections); straddling
  section keeps `mu_ids` plural; `merge_group_id` joins adjacent identical-`reg_set_index` same-
  stream pieces and is byte-deterministic across two builds.
- **Fail-loud tests:** unknown label, dup label, reg-set conflict, rule-landmark-unresolved,
  zero-resolved correction.
- **Determinism:** build twice → byte-identical `SectionRegs` + report.
- Fill the 4 skipped stubs in `stream_sections/tests/test_section_regs.py`; port
  `test_tributary_logic` / `test_tributary_override`; add tributary-onto-sections, per-rule
  tributary flag (Anderson Lake), `tributary_only` guard, lake-inlet, named-lake-barrier.

---

## 8. Key files
- New: `stream_sections/corrections.py`, `corrections_from_overrides.py`,
  `stream_sections/matching/` (index + resolver + attachment), `corrections_report.py`.
- Edit: `stream_sections/models.py` (AnchorType.area_boundary, BoundaryKind.area,
  SectionRegs.area_closure, Section merge_group_id), `border.py`/`anchors.py`/`sectionizer.py`
  (area-boundary splitter), `graph.py`/`tributaries.py` (named-lake barrier + lake trib precompute).
- Reuse (don't reimplement): `tributaries.py` (algebra), `MatchTable` (override precedence),
  `base_reg_assigner._resolve_mu_set` (MU set).
- Oracle (tests only): `pipeline/enrichment/{feature_resolver,base_reg_assigner}.py`,
  `pipeline/matching/{overrides.json,match_table.py}`.

---

## 9. Test-Driven Specification — the 4 selectors + the review blockers

Added after two advanced reviews (design/correctness + codebase-feasibility) measured the plan against the
real corpus (`output/pipeline/parsing/synopsis_parsed.json` 1395 entries 1:1 with `match_table.json`,
`pipeline/matching/overrides.json` 480). Every count below is **measured**. Companion references:
`docs/16-corrections-memo.md` (the 480 legacy corrections, classified — the "do we represent every known
error" oracle) and `output/v2/locators_to_curate.md` (622 locator strings / 360 regs, grouped by the anchor
kind to hand-author — the point-curation worklist).

**How to read each block:** `TEST` = the assertion to write first (TDD). `EXPECT` = correct behaviour.
`CURRENT` = what the plan as written in §1–§8 produces. `WHY` = why it's like that today. Severity tag:
🔴 blocker / 🟠 should-fix / ✅ holds. Selectors 1–4 are the model; H1–H5 are the holes the reviews found.

### 9.1 Selector 1 — MU / base-reg overlay  ✅ (with straddle caveat)

**In plain terms.** Lots of rules aren't about one river — they're the *general* rules for a whole Management
Unit (a numbered region-zone), e.g. "in Region 4-24, the trout limit is 2." Instead of copying those general
rules onto every single stream piece in the zone, store them **once per MU** and look them up when needed. If
a river sits on the line between two MUs, keep BOTH zone labels on that piece and show both sets of general
rules — don't chop the river at the invisible boundary. (Chopping there would be arbitrary and would make the
piece's ID change every time the boundary map is re-downloaded.)

- **TEST:** a base/zone reg on a stream that straddles two MUs (Fraser near Hope, blk 356364114, MUs 2-18 & 3-14) ⇒ `_resolve_mu_set` yields the zone's MU set; the section keeps `mu_ids` **plural**; nothing base-reg is written onto the section; clicking either side shows that side's regs + both MU polygons highlighted.
- **EXPECT:** `mu_id → base_reg_set` overlay is the ONLY store; `Section.mu_ids == (2-18, 3-14)`; `SectionRegs` carries no base reg text.
- **CURRENT:** matches — §5 Q1 + §4c. Feasibility ✅: `_resolve_mu_set` (`base_reg_assigner.py:292`) is reusable (coupled to `BaseRegulationDef`+`zone_mu_map`); `section_id = {blk}:{int(down_m)}` is deterministic.
- **WHY:** overlay-not-flatten avoids arbitrary median cuts on collinear river-is-the-boundary weave and `section_id` churn on WMU re-fetch (Q1). ⚠️ Caveat (H4/Fraser below): a *named* reg that differs by zone on one section has no representation without the curated `mu_boundary` split.

### 9.2 Selector 2 — Area (closure split + notice)  ✅ structurally / 🟠 gaps

**In plain terms.** Some rules come from an *area* drawn on a map — a park, a wildlife management area, a land
parcel — not from a named water. There are two flavours: **hard closures** (no fishing inside a national park)
actually **cut** the stream at the area's boundary so the inside becomes its own "closed" piece; **soft
notices** (needs a permit / advisory) don't cut anything — they just get **attached** to any stream piece that
touches the area. Rule of thumb: cut geometry only for real on/off closures; everything else is a coarse
notice. (Provincial parks are notices, NOT closures — unless a specific reg names one a closure, like
Garibaldi.)

- **TEST (Tier-1 closure):** national-park boundary cuts a stream inside/outside; inside piece carries `area_closure` GATE (not a `named_reg_id`) and is EXCLUDED from tributary inheritance. **TEST (Tier-2/3 notice):** a section intersecting a WMA/land-parcel polygon gets that polygon's notice on the WHOLE section via an STRtree pass, resolved at build time (not by clicked point). **TEST (correctness flag):** a PROVINCIAL park does NOT get a "closed" gate — UNLESS a specific reg names it a closure (Garibaldi).
- **EXPECT:** closure = most-restrictive gate; notice = advisory reference; provincial-park blanket ≠ closure.
- **CURRENT:** §5 Q2 tiers. Feasibility ⚠️: `stream_sections/build.py` loads **no** admin polygons today (only streams/lakes/manmade/wmu/obstacles) — the STRtree passes are net-new plumbing; the `parks_bc` partition by `PROTECTED_LANDS_CODE` is already ~done in the legacy atlas.
  - **Clarifying "partition ≠ split" (the worry):** partitioning `parks_bc` by `PROTECTED_LANDS_CODE` just
    means emitting the ONE fetched park layer as SEPARATE output layers by type — ecological_reserves /
    provincial_parks / protected_areas / recreation_areas. It does **NOT** mean cutting streams at every park
    type. It is the *opposite* — it is what LETS us split only the right subtype: geometry closure-splits go
    ONLY on national parks (`parks_nat`) + true ecological reserves (blanket), and on a specific provincial
    park **only** when a reg names it a closure (curated, e.g. Garibaldi). Provincial parks / rec areas /
    protected areas get a NOTICE on intersection, never a cut. The feasibility note was narrow: the legacy
    atlas already classifies each park by that code, so this sub-task is trivial plumbing, not new logic.
- **WHY:** split only hard closures (geometry is expensive + only closures change fishability); everything else is coarse-but-correct notice-on-intersection, mirroring land-use. 🟠 Gaps: `SectionRegs.area_closure` is **single-valued** — a section in a national park AND a curated closure loses one (H5); the "never closed for provincial parks" flag must distinguish blanket-PP (never) from curated-reg-PP (closed) — see 9.10 Garibaldi.

### 9.3 Selector 3 — Waterbody (named regs, narrowed by location_text)  ✅ / 🟠 label bridge

**In plain terms.** The common case: a rule names a specific water ("Adams River"), and often a **sub-stretch**
of it ("upstream of Adams Lake"). Step one — find the water. Step two — narrow to the exact reach the words
describe, using the split/lake boundaries already cut into that river. The trap (the 🟠 below): the words in
the regulation and the label we wrote on the split are different strings, so matching them by text is
unreliable — which is why §9.12 proposes binding rules to splits by ID instead.

- **TEST:** "Adams River upstream of Adams Lake" ⇒ the lake already split the blk; `piece_above(blk, "Adams Lake")` returns the upper node; the rule attaches there only. "Coquihalla downstream of the S tunnel entrance" ⇒ resolves to the downstream section.
- **EXPECT:** each named rule lands on exactly the reach its `location_text` names.
- **CURRENT:** §4b. Feasibility ⚠️ **MISLEADING** (H-label): `piece_above` (`tributaries.py:80`) does an **exact `label ==`** compare, but authored labels ("the upper Othello railway tunnel (N entrance)") ≠ rule prose ("upstream of the N entrance of the upper-most railway tunnel"). No bridge exists ⇒ narrowing silently fails / `rule-landmark-unresolved` fires constantly. **This already bites the new Wigwam split** (label "km 42 on the Bighorn (Ram) FSR" vs reg wordings "access road adjacent to km 42" / "rec site adjacent to km 42").
- **WHY:** the plan assumed the resolver matches "the same vocabulary that builds the label" — true for AUTO lake/split identifiers, false for curated point landmarks. The synopsis prose and the map-derived label are simply different strings written by different people; no amount of string-matching is reliable.
- **FIX DIRECTION — curated regs bind rule→split by ID, not by string (DECISION to lock).** Do NOT try to
  auto-match a rule's `location_text` prose to a split label for the complex/curated regs. Instead: **for any
  reg that carries a specific in-rule locator, match it by hand in two levels** — (1) match the PARENT water
  (gnis/blk), then (2) for each of that parent's rules that names a landmark, the curator explicitly binds the
  rule to a curated split's stable `id` (e.g. rule "upstream of the N tunnel entrance" → `split_id:
  coquihalla_othello_upper_tunnel`, side: upstream). The rule references the split `id` directly; the label
  string is display-only and never has to match the prose. This removes the vocabulary-bridge problem entirely
  for curated cases and is the natural authoring model for the 360 regs in `docs/17-locators-to-curate.md`.
  Auto-narrowing by label is then reserved ONLY for the simple auto-generated lake/split identifiers where the
  vocabulary genuinely is shared. (See §9.12.)

### 9.4 Selector 4 — Tributary (per-rule closure)  ✅ algebra / 🔴 named-lake barrier not done

**In plain terms.** When a rule says **[Includes Tributaries]**, it applies not just to the named river but to
all the little streams feeding it. This selector expands a rule to cover those feeder streams. The only twist:
the expansion must STOP at a named lake (a rule on the lower river shouldn't leak up past a lake into a whole
separate upstream system).

**What "closure" means here** (plain terms): the **tributary closure** of a water = the *complete* set of
every stream that eventually drains into it — walk upstream from the water and collect ALL its ancestors
(tributaries, tributaries-of-tributaries, …). "Closure" is the graph/set-theory sense: keep pulling in
upstream pieces until there are no more. So "Lake X [Includes Tributaries]" = Lake X **plus its whole
tributary closure**. A **precomputed** closure means: compute that set ONCE at graph-build time and store it
on the lake/section, so that at match time you just read the stored set instead of re-walking the graph for
every reg.

- **TEST:** "ATNARKO/BELLA COOLA [Includes Tributaries] EXCEPT Hunlen Cr upstream of Hunlen Falls, …" ⇒ `reach_except(base, excepts)` = closure minus the excepted upper pieces. **TEST (Q3 barrier):** a tributary walk from an Atnarko section must HALT at a named lake (Hunlen/Turner) — no leak into the above-lake catchment.
- **EXPECT:** set-difference closure; walk stops at named lakes + `EDGE_TYPE=2300`.
- **CURRENT:** `tributaries.py` algebra ✅ done & tested. Feasibility ⚠️: the guarded walk stops at `EDGE_TYPE=2300` ONLY — **named-lake halting is NOT implemented** (`graph.ancestors(guarded=True)` traverses lake nodes today). Q3 is a genuine extension (low-moderate effort: add a lake predicate to the `ancestors` stop condition; every `tributaries.py` caller inherits it).
- **WHY / the #8c gap:** §5 Q3 decided the named-lake barrier but §6 lists it as Stage-0 work not yet built.
  Bigger: the plan says lake tributary closures are "precomputed at graph time" as if a hook exists — it does
  NOT. The field `Section.tributary_section_ids` exists but is **never populated**, and in fact **`Section`
  objects are not materialized by the current build at all** (build.py outputs graph nodes + geometry + gpkg,
  no sectionizing-to-`Section` stage, no `mu_ids`/`tributary_section_ids` fill). So "precompute the closure
  onto lake sections" is entirely new work, not a small add. The `lake_tributaries()` function to COMPUTE it
  exists (`tributaries.py:57`); the storage/precompute step is what's missing.

### 9.5 🔴 BLOCKER H1 — Rules change by DATE, not just by place; "one reg set per section" is false

**In plain terms.** The whole plan rests on one idea: chop each river at every point where the rules
change *location*, and then each chopped piece ("section") has exactly ONE tidy set of rules. But rules
don't only change by location — **they also change by the calendar**. On the *exact same stretch* of river,
the winter rule can differ from the summer rule. Nothing in the plan knows about time, so it can't tell
"two rules, different seasons" apart from "two rules that clash."

**The concrete example (the plan's own showcase breaks).** The Coquihalla reach *upstream of the upper
tunnel entrance* has two rules on it:
- **No Fishing** — Nov 1 → Jun 30
- **Fly-fishing only** — Jul 1 → Oct 31

Same stretch of water. These do NOT conflict — one is winter, one is summer. A person standing there in
March follows the first; in August, the second. The *downstream of the lower entrance* reach stacks **three**
(closure Apr1–Oct31 + catch-&-release Nov1–Mar31 + a bait ban). Cowichan does the same (No Fishing Jul4–14,
then a *different* No Fishing Jul15–Aug31). Pitt River even has a **sub-daily** window ("one hour after
sunset to one hour before sunrise").

**Why this is a problem for the plan.** The plan says a section's rule set = the union of its rules, and
that "uniform context ⇒ one reg set per section is automatic" (§2 line 55). With no notion of time it does
one of two wrong things:
- **(a) mashes them together** — the section shows "No Fishing" AND "Fly-fishing allowed" as if both are
  true at once, which is nonsense to the reader; or
- **(b) its safety check `RegSetConflict` fires** — it sees two different rules on one section, decides
  that's a contradiction, and fails the build — even though there's no real contradiction, just two seasons.

Either way Coquihalla — the flagship case the plan uses to prove itself — does **not** "just work."

**How big:** measured — **371 of 1395** regs carry date-scoped rules; **64 regs put ≥2 different date
windows on the same stretch**. This is the single biggest hole.

- **TEST:** attach two date-disjoint rules to one section ⇒ the section holds BOTH as co-residents (kept
  separate, NOT merged, NOT flagged a conflict); the client renders "Closed *Nov1–Jun30*" and "Fly-only
  *Jul1–Oct31*" as two dated entries.
- **EXPECT:** the atom that attaches to a section is **(rule + its date window)**, and "date" is a real field
  a section can hold several of. Disjoint windows never trigger `RegSetConflict`.
- **CURRENT:** 🔴 `SectionRegs` has NO date field and §5b's render order has no date row, so the model can't
  represent "same place, different seasons."
- **WHY:** the reframe optimized for *spatial* uniformity and treated a whole rule as the atom; it never
  modelled two rules coexisting in time on one reach. **Fix:** make date a first-class dimension of the reg
  set (co-resident by window), and render seasonal closures as "Closed *dates*", never as a permanent gate.

### 9.6 🔴 BLOCKER H2 — "[Includes Tributaries]" is per-RULE, but §2b forces it entry-wide

**In plain terms.** A reg can say **[Includes Tributaries]**, meaning its rules also apply to all the little
streams feeding the main river, not just the main channel. The question is: when the *entry as a whole* is
marked "includes tributaries," does that force **every** rule in the entry onto the tributaries? §2b said
YES — "global-true forces all rules on. (No per-rule opt-out.)" (line 72). **The real data says NO:** some
entries are marked true overall, yet contain an individual rule that explicitly says "mainstem only" /
"tributaries not included." Applying §2b literally would push those mainstem-only rules onto every tiny
tributary — closing water the reg never meant to close.

**The concrete examples** (8 entries are entry-`true` AND have a rule explicitly `false`):
- **PITT RIVER** — entry says includes-tributaries=true, but its night-closure rule literally reads *"No
  Fishing in the Lower Pitt River (CPR Bridge → Pitt Lake) … (tributaries not included)."* The night closure
  is **mainstem only**; the entry-true would wrongly extend it up every creek.
- **FINDLAY CREEK** — *"Trout/char catch-and-release **(mainstem only)**"* — that one rule opts out.
- **GOAT RIVER** — same "mainstem only" phrasing on one rule.
- **STELLAKO RIVER** — two rules, opposite flags: *"No Fishing Nov 15–May 14"* (mainstem only, false) vs
  *"Class II water [Includes Tributaries]"* (true). One entry, one rule includes tribs, one doesn't.
- Also **ALOUETTE, LYNN, SKOOKUMCHUCK** (+ Stellako counted twice).

Separately, **217 entries** are entry-true with every rule's own flag left blank — which proves the entry
flag is meant as a **default for rules that don't say**, not a hammer that overrides rules that DO say.

- **TEST:** entry-true + one rule with `includes_tributaries=false` ⇒ that rule attaches to the MAINSTEM
  ONLY; the entry's other rules still expand to tributaries. (Golden: Stellako — winter closure mainstem-only,
  Class-II rule tributary-wide, in the same entry.)
- **EXPECT:** precedence is **explicit rule-flag > entry default**. Entry-true = "the default for rules that
  don't state their own flag," never "force all rules on."
- **CURRENT:** 🔴 §2b line 72 forbids the per-rule opt-out that the data plainly uses. Feasibility ⚠️: the
  legacy helper `effective_includes_tributaries` (`feature_resolver.py:33`) collapses the whole entry to ONE
  bool (`any()` over rules) — it can't express per-rule, so it can't be "ported with one branch removed."
  **Discard it and read each rule's own `Rule.includes_tributaries` field** (already present in the parsed
  data, values true / false / null-inherit).
- **WHY:** §2b was over-correcting a legacy BUG (legacy let "all rules opt out ⇒ nothing expands") and swung
  to the opposite absolute ("nothing can opt out"). The correct rule is the middle: per-rule-explicit wins,
  entry flag fills the blanks.

### 9.7 🔴 BLOCKER H3 — some regs scope to "just the inlet/outlet streams" or "within N metres", and the model has no way to say that

**In plain terms.** The plan offers a rule only three ways to pick its water: the whole named water, a reach
between two splits, or the *entire* upstream tributary closure (everything that drains in). But real regs use
scopes that are none of those:
- **"a lake's inlet & outlet streams"** = only the streams *immediately* flowing in and out of the lake — NOT
  the whole upstream catchment, and NOT the lake itself.
- **"within 100 m of the mouth of the inlet stream" / "within 100 m radius of the weir at the outlet"** = a
  small circle around a point.

Neither fits the three offered scopes, so the plan would either grab far too much water (the full closure
sweeps every creek in the watershed) or the wrong thing (the lake's own sections).

**The concrete example.** `WHITESWAN LAKE'S INLET & OUTLET STREAMS` (and Whitetail) — currently `not_found`.
It means: close the streams right at the lake's mouth(s), **EXCEPT** the outlet stream below the falls 2.4 km
down (that excepted reach then has its own seasonal + quota rules). Correct handling: `lake_inlets(lake) ∪
lake_outlets(lake)` for the immediate streams (the functions already exist, `tributaries.py:47/52`), then the
falls split (`whiteswan_outlet_falls`, blk 356560775, already added) carves the exception.

**Why it's a problem.** The `Locator` set and the 5-word narrowing vocabulary (whole / upstream_of /
downstream_of / between / at_lake) expose neither an "immediate inlet/outlet" operator nor a "point + radius"
one. Measured: **31** inlet/outlet entries and **77** rules whose location text has no reach keyword (21 are
point-radius, the rest are map-only descriptions).

- **EXPECT:** add a **`lake_io`** operator (immediate inlets ∪ outlets) and a **`buffer`** operator (point ±
  radius); and an explicit "map-only / can't auto-resolve" bucket for the vague 77 (so they get curated, not
  silently attached to the whole water).
- **CURRENT:** 🔴 no operator for either; the tributary selector over-grabs, the waterbody selector under-fits.
  ⚠️ The parser also over-tags Whiteswan `includes_tributaries=true, tributary_only=true`, which contradicts
  the "immediate inlets/outlets only" reading — logged in `docs/16` as a correction.
- **WHY:** the 4-selector model quantized scope into whole-water / reach / full-closure; "just the immediate
  lake mouths" and "a circle around a point" are distinct shapes that fell between those buckets.

### 9.8 🔴 BLOCKER H4 — one reg entry can cover several waters that each need DIFFERENT handling

**In plain terms.** Some entries name more than one water at once. The plan handles this by just lumping all
the target waters into one big pile ("union of targets") and applying the entry's rule to the whole pile. That
works only if the members are interchangeable — but often they aren't:
- a **landmark** in the rule ("upstream of the CPR Bridge") only makes sense on ONE of the members (the
  mainstem), not on all of them;
- a member can be **subtracted** ("does not include Sumas River") — the pile model has no "minus";
- members can be **different kinds** (a lake AND a river), so a river landmark or a tributary flag can't apply
  to the lake member.

**The concrete examples:**
- **`FRASER RIVER (upstream of the CPR Bridge at Mission)`** — resolves to **13** separate GNIS targets (the
  Fraser is braided into many channels) AND has a landmark. "Upstream of the bridge" is only defined on the
  main channel; the tool can't tell which of the 13 the bridge measures against.
- **`CHILLIWACK / VEDDER RIVERS (does not include Sumas River)`** — a **negative member**: subtract a whole
  connected named river. The union+EXCEPT algebra can't express "minus a connected water."
- **`CHILLIWACK LAKE, UPPER PITT RIVER`** — a lake and a river in one entry; any river-only narrowing is
  meaningless for the lake.

**Why it's a problem.** §4b says a combined entry = "a longer `targets` tuple (union)", but `piece_above` /
`sections_in_reach` key on a **single blk**, so a landmark over a 13-member union is undefined; and EXCEPT only
subtracts sub-basins, not a sibling named water. Measured: 18 multi-GNIS rows + 22 multi-target overrides.

- **EXPECT:** `targets` becomes a **list of per-member scopes**, each member carrying its OWN optional landmark
  narrowing and tributary flag; plus a **negative-member** operator ("minus this named water").
- **CURRENT:** 🔴 flat union of equal members; no per-member landmark, no negative member.
- **WHY:** the plan pictured a combined entry as a bag of interchangeable waters; real ones have per-member
  scope, per-member landmarks, and exclusions. (This is the same shape the §9.12 curation model handles
  naturally — each member gets its own parent + rule bindings.)

### 9.9 🟠 SHOULD-FIX H5 — one rule can need THREE selectors at once, and the plan contradicts itself on closures

**In plain terms.** The plan assumes each rule picks its water through exactly ONE selector (waterbody, OR
tributary, OR area). But a single rule can need all three at the same time — and the plan also states two
opposite instructions for how area-closures interact with tributaries, so it's undecided.

**The concrete example — one rule, three selectors:** Pitt River's *"No Fishing within Garibaldi Park"* is:
- **waterbody** (it's the Pitt River), AND
- **tributary-inheriting** (the entry is includes-tributaries=true, so it covers Pitt's feeder streams too),
  AND
- **area-scoped** (only the part *inside the Garibaldi polygon*).

So the correct water = (Pitt + its tributaries) **∩** (inside Garibaldi Park). The plan has no way to *compose*
selectors like this — it fused this case by hand instead of defining the operation. Measured: **32** named-water
rules reference a park/area (Englishman River Park, Little Qualicum Falls PP, Misty Lake eco-reserve, Strathcona…).

**The self-contradiction.** §5 says two opposite things about whether a closure travels up tributaries:
- line 182: the closure gate is **"EXCLUDED from the tributary inheritance walk"** (closures do NOT propagate);
- line 193: the Garibaldi closure **"MUST support tributary expansion of the cut"** (this closure DOES cover
  tributaries).

Both can't be the rule. The plan needs ONE stated policy.

- **TEST:** the Garibaldi rule ⇒ (Pitt + its tributaries) ∩ (inside Garibaldi polygon) is closed; a Pitt
  tributary that leaves the park and re-enters is still caught (see the Garibaldi plan section for the geometry
  proof).
- **EXPECT:** define **selector composition** explicitly (area gate intersected with a waterbody/tributary set),
  and state ONE rule for closure-vs-tributary propagation.
- **CURRENT:** 🟠 no composition operator; the exclude/include contradiction is unresolved. Related: the closure
  slot `SectionRegs.area_closure` is single-valued, so a section inside a national park AND a curated closure
  loses one.
- **WHY:** area was designed as a standalone selector; the Garibaldi case forced an area∩tributary fusion that
  was bolted on without reconciling the "closures don't inherit" rule. **RESOLVED + IMPLEMENTED in §10:** the
  area∩tributary intersection is now a build-time geometric fact (`StreamNode.in_areas`, set by the shipped
  `area_boundary` split), and the exclude/include contradiction dissolves — §5 line 193 was about the SPLIT
  (tributary-scoped cut, done) and §5 line 182 about MATCHING (the closure is not a tributary-inheritance edge).
  What remains is only wiring the reg onto the `in_areas` sections at match time (§10.5).

### 9.10 Worked examples — expected section topology (build-verifiable)
Each is unbuilt until `build.py` runs on the extent; these are the golden assertions.
- **Coquihalla (gnis 19983):** the two Othello point splits ⇒ 3 reaches: `upstream of upper tunnel` / `between … tunnel-interior ~700 m` / `downstream of lower tunnel`. Rule 4 ("at Othello Tunnels from N to S") → interior. ✅ structural; 🔴 fails H1 (seasonal stacking on the up/down reaches).
- **Whiteswan outlet (blk 356560775):** falls point split ⇒ a `downstream of the falls` section distinct from the closed remainder; base entry needs the H3 `lake_io` op. Falls verified 6.7 m off the blue line (FISS obstacle 21225).
- **Pitt / Garibaldi (gnis 7551, park PP `GARIBALDI PARK` 1887 km²):** MEASURED — mainstem = 27 km in-park, **1** boundary crossing (a point split would do the mainstem); Pitt tributary system (WSC `100-025956`) = **20 blks cross the boundary, 21 crossings, 1 leaves-and-re-enters**. ⇒ the `area_boundary` polygon cut IS justified (too many crossings for points; 1 re-enter a point can't catch). REFINEMENT: flag inside pieces by **point-in-polygon ∩ Pitt-WSC-membership**, NOT a graph-ancestor walk (else the re-enter blk is missed). Garibaldi is a **provincial** park with a curated hard closure ⇒ the "never closed for PP" flag must exempt curated-reg closures.
- **Fraser Zone 2/3 (blk 356364114):** the `mu_boundary` split near Hope ⇒ ONE cut; overlay keeps `mu_ids` plural elsewhere. Exposes H4 (a named zone-differing reg has no per-zone representation without this split).
- **Wigwam (gnis 2311, blk-divide ≈ LFID 706869683):** km-42 point split ⇒ `downstream of` + `upstream of` reaches for the two opposing regs. Exposes the 9.3 label-bridge gap (two reg wordings, one label).

### 9.11 Fail-loud gaps to add (from the reviews)

**In plain terms.** The plan already fails the build loudly when it *can't* resolve something. But there are
cases where it resolves to the WRONG thing without complaining — silent wrong answers are worse than loud
failures. These are the checks to add so the build screams instead of shipping bad data:

- **Mis-resolved (not just unresolved) split:** a `point`/`area_boundary` at `proximity_m 200` can snap to the wrong braided blk and still "resolve" — add a geometric sanity gate (offset distribution / expected-blk check). Affects the new Wigwam + Whiteswan splits.
- **Partial combined resolution:** assert ALL N targets of a combined entry resolved (Fraser's 13 gnis could silently drop members).
- **No-landmark `location_text`:** 77 rules have location text with no reach keyword — bucket as non-resolvable, don't silently whole-water-attach.
- **No temporal-conflict gate:** with no date model (H1), overlapping windows on one reach can't be detected.

### 9.12 Curation model — manual parent→rule matching (proposed decision)

The label-vocabulary gap (§9.3) and the sheer variety of in-rule locators (H3/H4, 622 locator strings across
360 regs) point to one conclusion: **for any regulation that carries a specific spatial locator inside a
rule, don't try to resolve it automatically — curate it by hand in two levels.**

1. **Match the PARENT water** — bind the entry to its `gnis_id`/`blk`(s) (the ordinary waterbody match). This
   is the coarse "which river/lake is this" step and is already what overrides/name_variants do.
2. **Match each RULE within that parent** — for every rule that names a landmark ("upstream of X", "within
   Garibaldi Park", "from A to B", "within 100 m of the outlet"), the curator:
   - authors the needed split/anchor in `splits.json` (point / confluence / line / lake / area_boundary /
     buffer / lake_io), giving it a stable `id`; and
   - **binds the rule to that split `id` explicitly** (with a side: upstream_of / downstream_of / between /
     inside), rather than relying on the resolver to string-match the rule prose to a label.

**Why this shape:**
- It **kills the label-vocabulary bridge problem** (§9.3): the rule points at a split `id`, so the human-
  readable label never has to match the synopsis wording. (Directly fixes the Wigwam "km 42" vs "access road
  / rec site" mismatch, and the Coquihalla "N tunnel entrance" phrasing mismatch.)
- It gives a **natural home for the hard shapes** the auto-model can't do: a combined entry (H4) becomes a
  per-member list where each member has its own parent + its own rule bindings; a lake-inlet/outlet scope (H3)
  is just a rule bound to a `lake_io` operator on the parent; an area closure (H5) is a rule bound to an
  `area_boundary` split.
- It makes `docs/17-locators-to-curate.md` the **worklist**: each row is one (parent, rule-locator) pair to
  bind. `coordinate`/`falls`/`dam`/`bridge` rows are turnkey; `map_or_vague` rows are the human judgment calls.

**Auto-narrowing stays** ONLY for the simple auto-generated lake/split identifiers where the vocabulary is
genuinely shared (e.g. "downstream of Adams Lake" ↔ the lake boundary label) — there the string-match is safe.

- **OPEN QUESTION for review:** the authoring format for a rule→split binding. Option A: extend the
  correction/override record with a `rule_bindings: [{rule_index, split_id, side}]` list. Option B: a separate
  `rule_locators.json`. Either is lossless; A keeps a reg's spatial curation in one place.
- **TRADE-OFF:** more manual work up front (≈360 regs, but most are one obvious point) in exchange for
  correctness and zero silent mis-resolution — which matches the plan's fail-loud philosophy.

---

## 10. Garibaldi / Pitt River — area closure (SPLIT DONE → matching is what's left)

**Status: the SPLIT is IMPLEMENTED, tested, and verified on real data.** It is a curated, RULE-AGNOSTIC
`area_boundary` split (§5 Q2 "Tier-1 CURATED"). This section documents (10.1–10.4) *how the split works
today*, and (10.5–10.6) the only remaining piece — how Phase-5 MATCHING attaches the "No Fishing within
Garibaldi Park" reg to the sections the split flagged. The H5 contradiction (§9.9) is resolved (10.5).

### 10.1 The problem (plain terms)
The reg is one rule on the **PITT RIVER** entry: *"No Fishing within Garibaldi Park"*, and the entry is
`[Includes Tributaries]`. So the water to close = **the Pitt River AND its tributaries, but only the part
inside the Garibaldi Park polygon**. Two things make this hard:
1. The Pitt **mainstem** crosses the park boundary just once — but its **tributaries cross it in ~20 places**,
   and **one tributary leaves the park and comes back in**. You can't do this with a single point split.
2. The closure is defined *over tributaries*, which collides with the plan's other rule that closures do NOT
   travel up tributaries (§5 line 182 vs 193). We resolve that below.

### 10.2 Measured facts (verified on `data/bc_fisheries_data.gpkg` with `.venv` geopandas)
| Fact | Value |
|---|---|
| Garibaldi polygon | `parks_bc`, `PROTECTED_LANDS_NAME='GARIBALDI PARK'`, code `PP` (provincial), **1886.7 km²** |
| Pitt mainstem (GNIS 7551) | trimmed WSC **`100-025956`**; **27.0 km** inside; boundary ∩ mainstem = **1 crossing** |
| Pitt WSC-descendant blks (park bbox) | **586** distinct blks |
| Blks crossing the boundary | **20** (21 crossings); **exactly 1** (`356350872`) crosses ≥2× |
| The re-enter blk `356350872` | mouth OUTSIDE, source OUTSIDE, **one INSIDE segment m≈279→1088** |
| Reference coord (49.73752,-122.75042) | 2 m from the boundary (a crossing verify-point) |
| Bisected lakes | 7 total; only 2 on the Pitt system, both <0.5 ha ponds (Glacier Lake 2.3 km² is off-Pitt) |
| Approach-(b) simulation | cutting all Pitt-descendant blks + point-in-polygon flags **1402 inside pieces / 554 blks**, and **`356350872` IS caught** |

### 10.3 THE key decision — how to pick "inside & closed" pieces
Two candidate methods:
- **(a) graph-ancestor walk:** take `ancestors(pitt_in_park_node)` and gate those.
- **(b) geometry ∩ WSC-membership:** take every blk whose trimmed WSC starts with `100-025956`, cut each at
  the park boundary, then gate every resulting piece whose midpoint is INSIDE the polygon.

**DECISION: (b). (a) is wrong.** Proof from the re-enter blk `356350872`: its mouth confluences into the
network **outside** the park, so it is an ancestor of the *outside* Pitt node, not the in-park node — the
ancestor walk (a) never visits it, yet its middle segment is unambiguously inside and must be closed. (b)
catches it (simulation confirmed). Two more independent reasons (a) fails: the guarded walk **stops at
`EDGE_TYPE=2300` barriers and named lakes** (Q3), dropping inside tributaries behind them; and the graph's
edges are braid-filtered, hiding geometrically-inside side channels. And (a) doesn't even save work — you
still have to cut and point-in-polygon each ancestor blk. **(b) subsumes (a) and is robust.** (Matches §9.10.)

**Scope = Pitt WSC descendants, NOT "all streams in the park."** The rule lives under the Pitt entry, so it
closes Pitt + its tributaries inside the park — not other systems (Cheakamus, Garibaldi Lake) that have their
own entries. "All streams in the park" is the BLANKET flavour reserved for national parks + eco-reserves.
⇒ Garibaldi is a **provincial** park closed **only** by this curated reg, so the "never render closed for PP"
flag (§5) must **exempt curated closures**.

### 10.4 How the split works now (implemented — factual)
Authored once in `splits.json` as `garibaldi_pitt`. At build time the `area_boundary` anchor:
1. **Scope** — selects Pitt River + every WSC descendant: `_target_blks(sd, chains, descendants=True)` matches
   `trim_wsc(blk).startswith("100-025956")` (dash-guarded). Targets `wsc`, NOT `gnis` (7551 is mainstem-only).
2. **Cut** — for each in-scope blk, cuts it wherever it crosses the Garibaldi polygon boundary
   (`resolve_split_defs` area branch → `_points(geom ∩ poly.boundary)` → one `SplitPoint` per crossing;
   applied by the same `split_graph_at`, proximity-pickup OFF).
3. **Flag inside** — `border.mark_inside_area` sets `StreamNode.in_areas += ("Garibaldi Park",)` on every piece
   whose midpoint is inside the polygon (point-in-polygon — the source of truth). It shares one
   `_pieces_in_polygon` helper with `mark_out_of_bc` (same test, opposite polarity).
4. **Identifier** — `StreamNode.location_identifier` returns **"within Garibaldi Park"** whenever `in_areas` is
   set; outside pieces read "downstream of / upstream of Garibaldi Park" from their bounds. So a
   boundary→headwater reach INSIDE the park correctly reads "within", not the misleading "upstream of".

**RULE-AGNOSTIC:** the split stores only the geometric fact (`in_areas`) — no reg id. Verified on the real
Garibaldi extract: **570** `within Garibaldi Park` pieces, the mainstem one ~30 km in-park piece, and the
leave-and-re-enter blk `356350872` correctly cut `out·in·out`. Code: `models.py` (`AnchorType.area_boundary`,
`BoundaryKind.area`, `StreamNode.in_areas`, `location_identifier`), `border.py` (`mark_inside_area` +
`_pieces_in_polygon`), `anchors.py` (descendant target + resolve branch), `build.py` (`get_area_polys` +
wiring), `tests/test_area_boundary.py` (9 tests, incl. the re-enter and ends-in-park cases). Inspect in QGIS —
`graph.gpkg` layers `streams` (`in_areas` column, `location_identifier`), `split_points`
(`anchor_type='area_boundary'`), `areas` (the polygon). The shipped entry (note: NO `closure_reg_id` — the reg
is matched separately):
```jsonc
{ "id": "garibaldi_pitt", "wsc": "100-025956", "stream_name": "Pitt River", "label": "Garibaldi Park",
  "anchor": { "type": "area_boundary", "area_layer": "parks_bc",
              "area_name_field": "PROTECTED_LANDS_NAME", "area_name": "GARIBALDI PARK",
              "wsc_descendants": true } }
```

### 10.5 The MATCH — what's left to build
The split produced the *structural fact*; matching layers the *regulation* on top. When Phase-5 processes the
PITT RIVER entry's rule *"No Fishing within Garibaldi Park"*:
- **Attach by `in_areas`, not geometry.** The rule attaches to every section whose `in_areas` contains
  `"Garibaldi Park"` — a cheap flag read, no match-time point-in-polygon. The area named in the rule's
  `location_text` maps to the split's `label` / `area_name`.
- **The "Closed" gate is a MATCH-TIME concept.** The reg→section attachment lives in the match output
  (`SectionRegs`), NOT on the split. `in_areas` is geometric membership; the closure reg is layered onto the
  sections that carry it.
- **H5 contradiction — RESOLVED (both §5 lines were right, at different stages):**
  - §5 line 193 ("MUST support tributary expansion of the cut") is about the **SPLIT geometry** — done: the cut
    spans Pitt + its WSC descendants inside the park.
  - §5 line 182 ("closure EXCLUDED from the tributary inheritance walk") is about **MATCHING** — also correct:
    each in-park piece carries `in_areas` *because geometry put it there*, and the closure does NOT propagate as
    an inheritance edge to downstream/other sections. Tributary-scoping is a build-time geometric fact, not a
    match-time inheritance. No contradiction remains.
- **Provincial-park correctness flag:** Garibaldi is a `PP` provincial park, so the "never render closed for
  provincial parks" flag (§5) must **exempt** this curated, reg-named closure. Matching decides "closed" from
  the reg attached to the `in_areas` pieces, not from the park-type layer.

### 10.6 Open MATCH decisions (for when we build the matcher)
- **Area→reg linkage:** how the correction/reg record names the area so the matcher binds
  `in_areas="Garibaldi Park"` → the "No Fishing within Garibaldi Park" rule. Fits the §9.12 curation model (a
  rule bound to its split by `label`/id).
- **Multi-area sections:** `in_areas` is already a tuple, so a section inside two curated closures keeps both;
  the match output's closure set must likewise be a set (the H5 / §9.2 single-valued `area_closure` concern).
- **Generalization:** the same split shape does Chilkoot Trail NHS (`area_layer:"historic_sites"` + a WSC/GNIS
  scope); the BLANKET flavour (national parks / eco-reserves, ALL streams) is a separate layer-driven path, not
  a `splits.json` entry.
