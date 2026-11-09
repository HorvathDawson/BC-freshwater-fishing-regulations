# T517 — One part per connected run of same-rule sections, always in sync with the paint

Status: OPEN — both reviews' fixes + owner 11-09 rules done on the side build, owner review before promote: side release `data/generated/side/t517/deliver/release/r-9dbd77824c29` (with T510 + T519); contract water/3 + geo/2 + display 20 + inseason-links/3; names = end tokens composed by one app function; paint sync and name check pass on the shipped files; nothing promoted. See "Second review fixes". Part of T495 gate 1.
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
- **Suites on the side build** (env: `RELEASE_DIR` for the app; `UI_EXPORT_BUNDLE`/`ANSWERS_EXPORT_DIR`/… for pytest):
  app vitest 6,935 pass, 3 fail not T517 (`check-boundaries`: in-season `inseason.test.ts` imports node:fs;
  `water-frame` ×2: T516's `marginHorizontal: GUTTER`); pipeline 6,499 pass, 2 fail not T517 (`test_review_screen`
  corpus tiers 452≠445, T185 curation; `test_curation_review_screen` new `POST /api/inseason/resolve`, T520). App fixes
  found on the way (733b7e5c6): mark confidence words said once; header/Parts sheet say the run's `run_name`; picking
  list row 0 from the whole-water view now opens it. Open for the owner: the Dean's Anahim run (grp 4, run 0) has a
  lake between its two pieces and no mark, so they never join even when salmon reads alike.
- Commits (first pass): c4ce4b8b4 (T510), 5f96628fe … c412c8a9d (pipeline + app), 036ba4ba4 (fixtures, tickets, shots), 733b7e5c6
  (app tests), e254e4ade (pipeline tests).


## Lake gap rule (owner 2026-11-08, final; replaces the HOLD on review finding 5 and the named/unnamed draft)
Different rules on the two sides of a lake: separate pieces, always. The SAME rules: one entry, across ANY lake, named
or not. A same-key run already crosses lakes as one part (T204: a lake on the course is not another part); where the
DFO salmon chain cut a run into pieces either side of a lake, `topology` writes a mark ("A lake") between the two
neighbouring pieces of one (grp, run), so `mergeNav` joins them by the one live rule (grp + run + live salmon key) like
any other neighbours, and they separate on their own when the live salmon differs. The paint sync counts every lake gap
on a stem as adjacency (`paint_sync.lake_gaps`). Today: 2 joins added — Dean grp 4 pieces 7↔8 and Pelican Creek 0↔1
(both lakes unnamed); 0 named-lake gaps lie between pieces of one run, so the final rule moved no count. Rejected by the
owner: cutting every run at every named lake (+12.5 % parts, 1,770 waters).

## Names from ends (owner 2026-11-09)
- A part ships only its two END TOKENS (`lo`, `hi`; `short` only for a part with no two ends: a lake, a whole water,
  side channels only, a polygon). `display.ends` turns tokens into words; ONE app function, `spanName`
  (`app/packages/core/src/ends.ts`), composes every name — one part, or a live-joined span (first piece's lo → last
  piece's hi) — for the header, map list, Parts sheet, solo checks, share/SEO and in-season "Where". `run_name`,
  `dup`, the merged-name composition and `hint` are gone (display 20).
- No "(n)", no "Within", no two parts of a water with one name. The 25 numbered names were 4 groups, none a missed join
  (alternating rule sets, or pieces apart by outside-B.C. pieces / unnamed lakes): Within-area pieces (Wap, Canyon,
  Carmanah, Hoodas — "Within" was also FALSE for the pieces outside the reserve), B.C.-border pieces (Blanchard,
  Petitot, Sleigh), unnamed-lake pieces (Meldrum, Riske). A CROSSING end (an area's boundary, the B.C. border, a region
  line, a river polygon, an unnamed lake) is said near what stands there (`topology.crossing_near`: the nearest named
  water feature within 2 km along the stem; at the border, else the nearest B.C. town within 10 km from the bundle's
  `place`, OSM, B.C. only), else at its km from the mouth — only where the part's two ends would say the same words or
  its name is another part's. A gauge cut with no regulation split and no named feature within 2 km is said by its km
  (the Dean's 08FC003: "km 121.9"; Tanswanket Creek's drawn sections are 13.2 km off).
- Visible names changed vs r-d73eba6565e1: 111 of 20,746 keyed parts, all intended: 55 former "Within <area>", 25
  former "(n)", 31 "X boundary → X boundary" crossings now told apart (e.g. "Region 5 boundary (km 24.7) → Region 5
  boundary (km 33.3)", "Codd Wetland Ecological Conservancy Area boundary near the McKenzie Creek confluence → … near
  the Donegani Creek confluence"), and the Dean's gauge end. No town was within 10 km of the 3 border waters'
  crossings, so they read "B.C. border (km …)" or near a creek.
- FAIL FAST: `answers.display.name_check` runs in the answers step the moment parts and ends exist (duplicate, "(n)",
  gauge code, missing/one-sided end token, token with no words, "Within") and stops the build; standalone on a built
  release in 0.2 s: `python -m pipeline.deliver.answers.display.name_check <release dir>` (r-38de2e92be26: 0
  problems). Mutation tests: `test_name_check.py`.
- Size: names were interned strings that compress well — water 2,620,499 -> 2,619,970 gz (−529 B), release total
  −96 B. Flat, not smaller.

## Fallbacks swept (owner 2026-11-09)
Pipeline, made loud: unknown end token words (was `token.replace("_", " ")`); a run end on a water with no name (was
its id); `gauge_near` unbounded (now 2 km, else km words); a part end with no km; an exit with no drawn part (0 today);
the "no salmon_section table → no chain" guards (dead: the schema always holds it). Kept, documented + tested:
`order_parts` puts parts with no km last (side channels, lake parts); a bare unnamed lake / tributary token says "a
lake" / "a tributary confluence" (FWA has no name for it). App (`waterGroup`, `navRows`, `waterMapStartIx`,
`waterMapView`, `navSections`/`partBounds`, `partNameOf`, Parts sheet, header, `targetFor`, frameView/place): made
loud — throw, the view shows the failure and reports it; kept with tests: no salmon reading → nothing joins; an unkeyed
(outside B.C.) part is named by its water; marks touching a tidal piece drop with it; a frame with no part opens at the
mouth; `targetFor` with no tapped section opens the first keyed part; partBounds uses the midpoint without a box;
steps follow part order where no mark says; receiving-water mismatch is reported and drawn grey.

## Review fixes (2026-11-08, side release r-d73eba6565e1)
Side builds this round: deliver stage only each time (atlas, reach and tiles "up to date": nothing upstream changed).
1. Maximal runs: Wap Creek 1/3, Nimpkish 2/4, Skeena 1/3 were ONE run (a side channel inside one rule set braids past
   another set's short stem piece and touches both) — `derived.join_touching` joins stretches whose sections touch
   (parts 20,975 -> 20,972). `paint_sync` now proves maximality on the bundle's `section_touch` (+ unnamed-lake gaps),
   never on the marks the build chose, and coverage against `item_section` (missing, twice, wrong water); mutation test
   through the build's own parts (`test_paint_sync::test_the_build_s_own_parts_pass_and_without_the_run_join_they_fail`).
   It named exactly those 3 pairs on r-d62af06225a2.
2-3, 8 (app): a joined entry knows all its pieces (`NavPart.pieces`); header, Parts sheet, solo checks and the receiving
   water read the same live `mergeNav` structure (`entryNames`, one name source); stretch plumbing, `fallsBackTo`,
   old-release probes removed.
4. Contract: water/2 (no `part.hint_s`, `part_mark.down_stretch/up_stretches`, `part_line.stretch`), geo/2 (no
   `part_geo.rank`), water-slice/3, display 19, regs-store/5; the app refuses a mismatch with a shown failure.
6. No numbered names at all (see "Names from ends"). The 41 NULL shorts are the unkeyed (outside B.C.) parts: the app
   shows the water's name, never lists them in the Parts sheet.
7. A gauge cut a part ends at is said by the nearest named feature within 2 km (`topology.gauge_near` -> export split
   `near`), else its km: Cruickshank "Near the Comox Creek confluence", Skeena "Above Babine River", the Dean "km
   121.9"; gauge names only in `part_mark.alt`. Gauge-coded names 6 -> 0.
9. The prototype water map check is back (`test_part_geo::test_the_chilliwack_and_the_cowichan_meet_as_the_prototype_
   draws_them`: runs joined by (grp, run), tidal off, marks = the prototype's words). The Kanaka T204 test was not lost:
   it is `test_kanaka_is_four_runs…` (T204's stretches are parts since T517; both marks still asserted).
10. 107 unlabelled marks, every one where a side-channel part meets another (a channel leaves at no named place): a known
   count, pinned (`test_part_geo::UNLABELLED_MARKS`); 5 more (two parts meeting across a lake) now say the lake. The
   `mark_words` assert is unchanged.
- Bindings vs live: the same 6 sections (T510's), nothing else (`binding_diff`, 123 rules all within those 6).
- Size (gzip): release 21,587,945 live -> 21,796,497 (+0.97 %; -37,781 vs r-d62af06225a2); water 2,619,970; geo 595,092.

## Second review (11-09, fresh Opus, read-only on 9fdc8b8e3 + working tree)
MAJOR: (1) 5/32 in-season ends are derived tokens missing from `display.ends` (lake outlet/inlet, region:a|b, trib:gnis) →
"Where" fails and the notice falls back to the whole water; publish.py must refuse tokens not in the target release's
display.ends; a words failure must drop only "Where", not the sections. (2) Boundary-mark labels are a second stored
naming path (topology.py mark_words, before `_names` re-places @near/@km clashes; app markLabel "a / b" fallback) → ship
mark end token, app names it via endWords. (3) banner.ts:60 / inputs.ts:150 (`?? ""`), header.ts:85 and sheets/parts.ts:41
fall back to stored labels → name via spanName/partNameOf, throw on missing facts. (4) ui inseason onPage uses one piece,
not the joined entry → build from merged entry's pieces.
MINOR: (5) ends.ts "Within" branch dead → delete, throw on lo==hi. (6) part.short_labels + run_ends branch dead → delete.
(7) parts with no main stem still ship stored `short` → OWNER CALL. (8) wire name_check on the SHIPPED release next to
paint_sync in build.py; one TS↔Python parity fixture. (9) paint_sync check() untested on shipped files; silent drop at
paint_sync.py:171; water-<r> slices unchecked. (10) build check that every multi-piece run is mark-connected. (11) joined
span ending in a side channel would throw (hi from last piece) → refuse in build or use highest stem piece. (12) salmonKeyOf
must throw for unloaded regions (chainOf default lets unread pieces join). (13) ui inseason `!table → []` hides banners →
show whole-water + failure. (14) drop ALSO_READS paint/4. (15) bump water/3 or note display 20 carries lo_s/hi_s tokens.


## Second review fixes (11-09, side release r-d6a596c32bd1)
- (1) `display.ends` holds words for EVERY token anything ships — one list (`display.section`): part ends, mark tokens,
  and the curated in-season notices' ends (`inseason.matches.end_tokens`; derived `lake:<wbk>:inlet|outlet`,
  `trib:<item>`, `region:A|B`, `mu:A|B` get words in `part.end_name`; the export keeps a split a notice names). +8
  tokens (6 in-season, 2 `near:`), 9,888 -> 9,896. Publish refuses a token the release lacks (`publish.ends_problems`,
  the guard) and ships no `words` (inseason-links/3). App: a token with no words drops only "Where", reported.
- (2) Marks ship a TOKEN (`part_mark.token`, water/3), never words: the parts' shared end (`release.build.mark_token`:
  equal ends; an unkeyed side takes the keyed side's; across a lake the lake; two cuts at one place the lower part's
  top), else the token the parts' runs share (`topology.mark_words`, the bundle's `part_mark.token`). 103 of 1,083
  marks have none: each a side channel meeting its river at no named place. App: `endWords(token)`; no "a / b".
- (3)(4)(5)(12)(13)(14) app: no stored-label fallbacks (header, Parts sheet, banner); in-season coverage on the joined
  entry; "Within" gone; unloaded salmon regions throw; an unreadable ends table shows the notice without "Where";
  paint/4 dropped.
- (7) owner: a part with no main stem ships no ends and says its water's name; where a water has several (only Old
  Kootenay River Channel today: two side channels of unmeasured ends) each says "<water> near <the nearest other named
  water within 2 km, else B.C. town within 10 km>" (`topology.part_near`, lo = hi = `near:<token>`); a no-stem piece of a
  multi-piece run carries its side channels' ends (Klinaklini).
- (6) dead `short_label(s)`, `_run_short`, run_ends' area branch deleted. (8) `name_check.check_release` on the SHIPPED
  water file beside the paint sync (names, run marks, run ends, slices); one TS<->Python fixture
  (`app/packages/core/src/ends.parity.json`, read by `ends.parity.test.ts` and `test_name_check.py`). (9) paint sync:
  no silent drop; tested on a built release. (10)(11) run rules in `name_check.run_problems`. (15) water/3.
- Bindings vs live: the same 6 sections (T510's), nothing else. NOTE: this round's `build --side` re-ran the reach stage
  (another lane's reach code, T524) and its new DFO gate stopped it after it had rewritten `reaches/full_next`; the
  rewritten bindings diff to the same 6. The release was then made by the deliver stage alone on that reach output.
- Size (gzip): release 21,587,945 live -> 21,801,735 (+0.99 %); water 2,500,340 -> 2,622,751.
- Cultus Lake (T360 "parts not available" on HEAD): not reproduced on the side build — all 19,806 water groups load
  through `loadWaterGroup`, and Cultus Lake (one part) opens on its Regulations as a solo water (`shots/T517-cultus-light.png`).
- Shots (web, light, side release): `shots/T517-{fraser,dean,squamish,cultus}-light.png`.

## Final round (11-09 midday)
- Release r-9dbd77824c29 (deliver only, `--no-step-cache`; r-d6a596c32bd1 rebuilt with no cache came out byte-identical):
  the web slices declared `water-slice/2` from a second hand-kept copy in `slices.py` while the contract said /4 —
  the slices now take every format from the contract. Size (gzip): release 21,801,726 (+0.99 % vs live), water
  2,622,744. Name check 0 and paint sync pass on the shipped files.
- `answers.encode.decode` refuses a section of another version than the code reads, by name (a display 18 file read
  by display 20 code failed as KeyError 'ends' — `test_card_gate_in_chain` on plain HEAD, live answers).
- The regs fixture cut keeps each water a kept water flows into whole (its map group, T459) and core holds the item
  row of where THOSE flow (`navRows` refuses an exit into an item core lacks).
- Cultus Lake "parts of this water are not available" (T360 test bed on HEAD 229c464de): the APK was built from the
  main tree with this lane's uncommitted water/3 / paint/5 readers and read staging (water/1) — T360's own note (4).
  Not a data defect: on the side build every one of 19,806 groups loads (`loadWaterGroup`) and Cultus Lake opens as a
  solo water (`shots/T517-cultus-light.png`). Committing the lane as one set with its release removes the mismatch.
- Follow-up (not done): the app still READS paint/4 — the three frozen fixture releases
  (`app/packages/data/dev/fixture-release/r-6d85601a9b99`, `r-6dfd7c1cf86a`, `r-cc200b480be5`) ship paint/4; re-cut
  them with paint/5, then drop the paint/4 reader (review finding 14).
