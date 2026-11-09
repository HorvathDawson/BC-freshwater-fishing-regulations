# T517 — One part per connected run of same-rule sections, always in sync with the paint

Status: OPEN — side build done, owner review before promote: side release `data/generated/side/t517/deliver/release/r-d62af06225a2` (with T510 + T519); paint-sync passes; nothing promoted. Code 5f96628fe…(see Results). Part of T495 gate 1.
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
- **App only displays.** `name ?? lo + " → " + hi`. ONE MODE (owner 2026-11-08: "I only want a single mode and stretches cover both types of
  regulation changes while adapting to new DFO rules"): two neighbouring parts join only when grp, run AND the LIVE
  `salmonKey` (release chains + the feed's rules) are equal — the same list, navigation and names in every mode. A
  dormant DFO place's cut stays joined while its live salmon equals the neighbour's and separates on its own when the
  feed activates it. With no reading yet nothing joins across a part. A live-joined span is named by the shipped words:
  the whole run's `run_name`, else the first part's `lo` → the last part's `hi` (the app composes only that). Delete `stretchEnds`, `lowEnd`, the `markLabel` fallback, `fallsBackTo` (prove with
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
- Intended (one mode): the regular map can list MORE stretches than today wherever live salmon rules differ along a
  run (≈29 waters by the audit); counted in the side build's parity report.

## Gates on the side build (with T510 + T519)
Rule bindings per section identical to live except T510's 13 sections (+ any slough-fold lakes, listed); paint-sync
check passes; size flat; every part reachable; parity list above confirmed with counts; shots (Fraser, Dean, one
formerly bare water) light/dark. Nothing promoted.

## Salmon feed adaptivity (checked 2026-11-08, code + live data)
- **The chain holds EVERY curated DFO place, dormant ones too.** `deliver/salmon.build` puts every location of every
  curated region file in `locs` ("EVERY location, ruled today or not (T211 part 2)") and chains each placed section by
  the places whose extent covers it; the feed's rules only fill verdicts. The reach run places all 352 curated places
  (151 active + 107 active-unmapped + 26 dormant-with-extent + 68 dormant-unmapped; `dfo_salmon.jsonl.gz` 352 rows), so
  a dormant place's edges are chain changes today. Pinned: `test_salmon_file` (a dormant location sits in its chain).
- So with `chain` in the part key the cut is already there for a dormant place: an activation from the live feed changes
  only salmon status and paint (`regs.dfo_salmon.live` ships rules per location; the app re-composes the release's
  chains), never the parts or marks. Salmon-mode navigation joins on the feed's `salmonKey`, so it separates the day
  DFO lists the place again, with no release. Pinned: `waterNav.test.ts` "a DORMANT DFO place the live feed turns
  active separates on the map with no release" (marks between the pieces stay).
- **A brand-new extent with no curated geography** is not silent: `live.py` holds its rules in `unplaced.json`, sends an
  URGENT notice ("new location on … — Place it in DFO review"), and lists it on the live queue (T366;
  `test_dfo_live::test_a_place_matching_no_curated_place_is_urgent_and_listed`). It paints nothing until a curator places
  it, and the next release cuts there.

## Gauge cuts keep every name (owner 2026-11-08)
`answers.display.end_variants`: the regulation's own name is said (`end_words`: Cruickshank's Strathcona Park line,
not "08HB074 · Cruickshank River Near The Mouth"); the gauge's own name (and any same-place cut / official spelling) is
kept as an alternate on the mark (`part_mark.alt`, a JSON list, release water file) for search and the alias list. Not
done: the app's search does not read marks yet.

## Results (side build 2026-11-08, `data/generated/side/t517`, release r-d62af06225a2)
- **Bindings:** every section's rule bindings identical to live except 6 — T510's 2 rule-changing connectors, its 2
  slough-fold lakes, and one Columbia section id moved (`356570372:1834527` → `:1835154`, same rules). Details:
  `shots/T510-moved.md`.
- **Paint sync** (`release.paint_sync`, on the shipped files, in release `build()` before LATEST): 20,975 parts, 65,129
  sections — each part one status profile, one no-access tier, one salmon chain; no two meeting parts share a key; every
  section in one part. The live release FAILS it (T228 parts with two salmon chains), as expected. It caught three
  real defects on the way: spurious marks between two runs of one key (now never made), and its own no-access read.
- **Size** (gzip): release 21,587,945 → 21,834,278 (+1.1 %); water 2,500,340 → 2,636,159 (+5.4 %: the T520 `display.ends`
  table is ~106 KB of it, names/run strings the rest); geo 630,325 → 595,727 (−5.5 %, `part_stretch` gone); core flat;
  paint flat. Bundle 158.21 → 158.15 MB; export 6.92 → 6.95 MB (+0.5 %). Rule sets stay shared (`set_id`), never copied.
- **Parity:** parts 20,708 → 20,975 (+267); Fraser map list 19 → 19, same order, no "stretch k of n"; Kanaka 3 → 3;
  every picker the same length AND the same words (19,793 waters); bare "N stretches" names 180 → 0; marks without words
  409 → 112; multi-chain runs 34 (76 parts); Dean map entries 34 → 10 on the release's salmon (one mode: fewer where the
  live feed reads pieces alike); waters whose parts renumber: ~487 (golden taps: 87 of 121 states moved, 17 waters
  `recut` T517). Gauge-named marks 2 → 2: Cruickshank (its Strathcona Park line is an area split with no place on the
  water, so no regulation split stands within 0.2 km) and the Dean's 08FC003 (nothing named nearby; the nearest-by-km
  rule named a cut 44 km away, so it is bounded) — both keep the gauge's own name.
- **Exits (T519):** 3 change — the Fraser now reaches the sea (not Morey Channel); Inkaneep Creek exits into its lake
  (was a channel of its own name); the upper Jeperson Side Channel no longer names the lower one (both on the Fraser's
  code; the lower is the smaller).
- **`display.ends`** (T520's one name source; answers display static, `regs-store` static row `display.ends`):
  `{token: [words, other names…]}` for every split (cut, area, region line, lake edge) and every run end token
  (confluence, lake inlet/outlet, mouth, source, continues); a last `"to:<words>"` where the lower end reads otherwise
  ("the source"). Derived in-season points are not tokens of the bundle: not in it.
- **Not done (left as follow-ups):** `part_geo.rank` and the stretch plumbing in the app's route/place/carry/frameView
  stay (inert: every part is one stretch now); `hint_s` stays; app search does not read `part_mark.alt` yet; SEO
  `rulesKey` keys by group + end tokens but not a split's curated position (not in the release).
- Shots (side release, web, light/dark): `shots/T517-fraser-{light,dark}.png`, `shots/T517-dean-{light,dark}.png`,
  `shots/T517-fortnelson-{light,dark}.png` (formerly "2 stretches · stretch 1 of 2"-style bare names).
