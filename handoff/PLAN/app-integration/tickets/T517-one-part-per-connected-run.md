# T517 — One part per connected run of same-rule sections, always in sync with the paint

Status: OPEN — paused 2026-11-08 for the owner's fresh audit of part naming / sectioning (coordinator); WIP in
`git stash` "T517 WIP: contiguous parts…" (derived.part_rows + schema UNIQUE). In-season work paused separately (notice
audit). Supersedes T509's "What is left" (`short` on `part_stretch`, dropped). Part of T495 gate 1.

Owner 2026-11-08: "Yes one part per connected adjacent stretch. And we need to make sure it will always be synced to the
painted colors. They can never diverge." Also: salmon paint must match the salmon rules the same way; data size roughly
flat; old-release fallbacks may go (in-season part paused).

## Measured (live bundle `data/generated/bundle/bundle.sqlite`, release r-6dce77a24e6a)
- **Dry run of the split** (`derived.part_rows` cutting each rule-set group by `topology.stretches`, numbered by km from
  the mouth): parts 20,708 → 20,933 (+225, +1.1 %); 160 waters gain parts; 472 waters change which part an index names
  (river order renumbers every multi-part water whose old order was not river order). `part_stretch` today: 869 rows on
  182 waters; after, only T228 salmon pieces would remain. The Fraser goes 17 → 20 parts, tidal piece first.
- Fixture tests with the WIP (derived, part_geo, part_line, release, slices, regs_store, …): 228 pass. One live-bundle
  test fails with or without it: `test_part_geo::test_the_fraser_reaches_the_sea…` (the Fraser's exit is now Morey
  Channel `gnis:14905`, a T497 effect on the live bundle, not this change).
- **In-season spill, live staging** (links name r-6dce77a24e6a, so phones read the per-section `places`): 0 of 32
  notices land on a piece they do not cover. 9 notice/part pairs sit on a part of several pieces; every piece has
  covered sections. Fallback (mark walk, phones on another release): no spill on today's data either. 5 of the 14 partial
  extents resolve and match a piece-level walk; 9 have labels that are not part marks, so they show whole-water "partial".
  Ends mid-part: 5 cases, all `some` (Fraser part 2 stretch 0 25/28 sections; Pitt parts 0 and 2; Kaslo; Snehumption).
  The spill is a hazard in how the code is built, not a bug users see today.
- **Salmon:** `salmon_section` has 287 chains over 1.95 M sections. `salmon_edge_gap` (a chain change with no named split)
  has 1 row, so T228 pieces with mixed chains are the only salmon-paint divergence path today.

## Superseded plan notes (before the audit)
Place key, `short_label` per run, app side (waterGroup / mergeNav / partName, SEO rulesKey, picker grouping,
targetFor, problem reports), contract bump, paint-sync build check + mutation test, golden-tap declarations, side build,
size before/after, shots. All wait for the audited design.

## Design (audit 2026-11-08 on the live bundle; follow-ups answered)
**Main defect found:** `topology.by_chain` cuts a part into non-touching pieces at every lake when the salmon chain
varies: 35 parts → 187 map entries (Dean "Anahim Lake → Source" 26 pieces, Tusulko 21, Pelican 16, Bulkley). T464's
mark-less stretches and `fallsBackTo` are patches for it. Names/ends are computed in ~12 places (7 Python, 5+ app; the
app parses Python prose). Screen vs bundle disagree (Fraser map "Mission → Landstrom", header "2 stretches"; 68 bare
shorts; 409 marks drawn "X / Y"; 2 marks named after gauges).

- **Part = maximal connected run** of sections with the same key = today's key + `chain`. "Connected" = T204
  `stretches` (a lake is another item; the river run stays one part — Fraser part 0 "through 3 lakes"). Every part is
  then uniform for regular, access and salmon paint by construction.
- **Columns:** `grp` (today's rule-set group: picker, full label, closed-merge stay per grp → picker length unchanged,
  Fraser 15); `run` (chain-free T204 run index). UNIQUE(ord, rep_sid) replaces the group-key UNIQUE. part_ix = river
  order (km, mouth first; no-km last); `part_geo.rank` dropped. No `place` column (in-season uses FWA ranges, T520).
- **Names: one Python function `run_ends`**, cleaned end words (`_end_short` / `end_name`; "CPR Bridge at Mission").
  Ships `name_s` per part + a precomputed merged name per (grp, run); two end strings only on the 35 multi-chain runs
  (salmon mode composes "lo → hi"). Skip `kind=gauge` split tokens: use the regulation's own split at that place
  (`same_place_as`), else the nearest named feature. A chain change with no named split (Klinaklini, 1 today) is named
  by the run's end, never fails the build — but Python must never emit "" (fail loudly on no nameable token).
  `part_mark` label = the same words as the lower part's top end and the upper part's bottom end (build asserts).
- **App only displays.** `name ?? lo + " → " + hi`. Regular mode joins parts with equal (grp, run); salmon mode also
  needs equal `salmonKey`. Delete `stretchEnds`, `lowEnd`, the `markLabel` fallback, `fallsBackTo` (prove with
  every-part-reachable), stretch fields + `stretchOf`. `skips` stays. `targetFor` ties → lowest part_ix.
- **SEO `rulesKey`** per grp: rule content + each run's two end tokens (split_id / confluence:gnis / mouth / source /
  lake) + a split's curated position (`split.at`). A moved cut shows on /changes; renumbering and snapping noise do not.
- **Paint sync: one release-build check on the shipped outputs** (decode status_index / paint + `salmon-*.bin`): every
  part uniform in status profile, >500‰ no-access tier and chain; no two mark-joined parts with equal keys (maximal);
  every section in exactly one part. Mutation tests.
- **Delete (Python):** `by_chain`, `chain_label`, `split_at`/`split_label` (if `water_map_export` drops them),
  stretch code in `merge_marks`, `part_stretch`, multi-stretch branches of `runs_label`/`short_label`, `hint_s`.
- **Size:** ≈ +260 part rows, −869 `part_stretch` rows and their sid lists, fewer strings → flat or smaller. Measure.

## Output parity (what the owner sees)
- Same: Fraser map list 19 entries in the same order; the 184 T204 waters; picker and parts sheet lengths; paint; links.
- Intended: the 35 fragmented T228 parts 187 → ~80 entries (Dean ~31 → ~9); the 114 bare "N stretches" names get
  real ends; header/sheet show the run's own name, not "2 stretches"; the 409 unlabelled marks get end words; 3 Fraser
  names lose the raw split wording ("The CPR Bridge…" → "CPR Bridge at Mission"); part_ix moves in ~472 waters (SEO,
  golden taps, reports re-keyed in the same change).
- Measure in the side build: (g) side channels whose chain differs from the stem become own parts (Tomahnous kind).

## Gates on the side build (with T510 + T519)
Rule bindings per section identical to live except T510's 13 sections (+ any slough-fold lakes, listed); paint-sync
check passes; size flat; every part reachable; parity list above confirmed with counts; shots (Fraser, Dean, one
formerly bare water) light/dark. Nothing promoted.
