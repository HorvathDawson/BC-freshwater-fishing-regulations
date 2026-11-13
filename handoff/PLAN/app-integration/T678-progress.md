# T678 progress (Claude, 2026-11-11 night)

Ticket: `T678-tidal-in-the-build.md` (owner answers at its top are binding). Nothing promoted, nothing released.

## Done (commits on redesign/stream-sections)
- `addbfd819` step 1: `pipeline/atlas/splits/coast.py`, the cut rule. Rank 1 = DFO-written split rows (`coast.authority`:
  a split row's `source`/`authority` field starting `dfo`; a row with neither is UNCLASSIFIED, governs as today and is
  listed in `coast_owner.json`; any other value = no authority) + the owner's `whole` rows. Rank 2 = coast crossing: top
  of the line's lowest edge-1450 run, cross-checked with the geometric crossing (sea = outside the FWA land mass, plus
  tidal rivers' polygons); over 50 m apart, or no geometry, = the owner's line. Stub under 25 m at the top = tidal
  whole. Fraser code `100-000000`: named non-slough lines joining tidal water = tidal whole; sloughs and unnamed = owner.
  FWA rivers + land mass: `fetch_data.py` `fwa_rivers` / `fwa_land` into `data/source/fwa_coast.gpkg` (own file:
  the main gpkg's stamp is an input of every shipped atlas). Config key `data_accessor.coast_gpkg_path`.
- step 2: the build. `build.cut_shores` = the coast cut, now BEFORE `cut_length`. `waters/shore_held.json` (184 lines,
  today's result) written by `python -m pipeline.atlas.splits.coast_held --atlas data/generated/atlas/full --out
  data/curated/waters/shore_held.json` (a `decided` row survives a rerun). `build_registry` writes
  `tidal_sections.json` (THE record) and proves rank 1 covers the same water (`coast.water_cover`). Reach CLI, tiles,
  reach stage key and review app read the record on a coast-cut atlas (`coast.built_by_coast_rule`); an older atlas
  is read the old way, printed. `sidecars --tidal-only` removed.

## Not done (next, in order)
1. Read the candidate build's diffs (below) with the owner; promote only on the owner's word.
2. At promotion: move `inputs.T678_INPUTS_AT_PROMOTION` into `ATLAS_INPUTS` (+ `--backfill`), drop the
   `NOT_ATLAS_INPUTS` shore_held row. Added earlier, every shipped atlas reads as behind and release preflight stops.
3. Plan 5: `tidal` as a field on the graph's nodes (today only the record carries it: graph.pkl is written before
   the registry the record needs).
4. Plan 2: the rank-1 resolvers (`reach.outside.tidal_cut_owner` / `tidal_whole_owner`) still live in the reach and the
   atlas still builds the mid-build registry (`water_registry(areas=False)`) to ask them before the coast cut.
5. Plan 6: a record row for polygon lines the prune took (`shore.py:535` silent skip) — not written.
6. Plan 8: store fact for key-0 tidal sections (deliver/store) — not started (T679 lane owns deliver/bundle).
7. Retire `shore.polygon_rule`, `HELD_*` constants, `atlas_tidal`, `inside_tidal.json` readers once no pre-T678 atlas
   is read (the build no longer calls them).
8. The other lane's split classification: `coast.authority` reads `source` (or `authority`) on split rows; confirm the
   field name when their commit lands, then re-run `coast_held` (San Juan's new split makes 354087703 rank 1).

## The candidate build (one run)
Snapshot of `data/curated` at the step-2 commit (`CURATED_ROOT`), script in the session scratchpad `t678/run_build.sh`:
`python -m pipeline.atlas.build --full --out data/generated/atlas/full_t678` then
`python -m pipeline.atlas.reach.cli --build data/generated/atlas/full_t678 --out <scratch>/reaches`, via
`scripts/heavy.sh t678-atlas 50 -- …`. Diffs: scratchpad `t678/diffs.py LIVE CAND LIVE_REACH CAND_REACH OUT`
(cut_diff.json, tidal_diff.json, rule_length_diff.json).

## Candidate build result (2026-11-12, `data/generated/atlas/full_t678`, not promoted)
Third run (run 1 stopped on an undecidable line missing from the held file, Power House Creek, now held; run 2 on the
new `tidal_split_authority.json` curated key missing from the snapshot). Atlas 1,840 s, reach 715 s, rc 0; the
rank-1 coverage proof held (59 lines/lakes). Diffs (scratchpad `t678/diff/`): coast/shore cuts 226 live -> 219
(100 same, 116 moved, all under 50 m, median 22 m; 3 added, 10 removed); lines tidal whole 215 -> 109; tidal water
456.3 km -> 412.4 km (split pieces 195.8 -> 177.6 km: the 10 non-DFO splits fall to the coast crossing); 201 rules
change bound length by over 1 m, 197 gain (171 province/zone rules +43.9 km each; water rules: Nass +9.6 km,
Chemainus +8.7 km, Campbell r6 +2.4 km, ...), 4 lose (Tlell r1/r2/r5 -67 m, Salmon r3 -15 m). Owner list:
`coast_owner.json` in the candidate, 194 lines (182 estuaries drawn as river polygons, 3 crossings disagree, 3 no
geometry, Kitimat, five item-C lines) + 10 splits without DFO authority.

## Second pass (2026-11-12, owner answers of the day) — candidate `data/generated/atlas/full_t678b`, not promoted
Code + data: 9b0d6f4ae (the gauge hold below: the commit after it). Build: atlas 1,789 s + reach 693 s, rc 0, curated
snapshot at 9b0d6f4ae (`CURATED_ROOT`; script and logs in the session scratchpad `t678b/`). Diffs (`t678b/diff/`,
script `t678b/diffs2.py`): cut_diff, tidal_diff, rule_length_diff (vs live and vs the first candidate), pond_fold_diff,
d5_dfo_diff, d3_nearer_diff, gauge_moves, owner_list (.json + .md, lat/lon per line).
- The 10 owner splits govern again (`tidal_split_authority.json` format 2, `authority: owner`): vs the first candidate
  Nass -9.6 km, Chemainus -8.7 km, Campbell r6 -2.4 km, Sooke -361 m, Qualicum -108 m (back to live's reading).
- San Juan side channel 354087703 tidal below the Deering Road bridge (681 m, a `cut` row); san_juan_river.r2 binds
  681 m less (the one closure that shrinks vs live — the owner's decision). Main stem stays at its mouth. 354087702 (the
  447 m estuary piece) is now governed by rank 1 (`held_now_governed`), tidal as before.
- Kitimat 360885824 decided by the mouth line (not cut); the four creek pieces kept at FWA's edge-1450 crossing
  (`decided` rows). Tlell: already the DFO 1.5 km sign split (rank 1), unchanged. Marble: mouth, Varney Bay note.
- Shore cuts: live 226 -> 217 (99 same, 116 moved all < 50 m, 2 added, 11 removed); vs first candidate 0 moved, 2 removed
  (354087703, now a rank-1 cut; Yorkson 356250588, owner split). Tidal water 456.3 km live -> 437.2 km (first candidate
  412.4).
- Rules over 1 m vs live: 194 (187 gain, 7 lose); 22 water rules named in rule_length_diff.json. Dropped bindings: the
  three lake twins z1 trout_quota.r5b, z5 trout_char_quota.r6b, z6 trout_char_quota.r9b — every lake they bound was a
  steelhead river's widening pond, now the river (D1); unresolved with a typed reason (no_sections).
- D1: 16,119 ponds folded into 4,258 stream items; 7 left and listed (one two-streams pond, six with a cut on the edge:
  five gauges + Gagnon Creek signs). lake:329461947 joined the Okanagan River Oxbows. 397 rules change pond count (365
  gain, 32 lose: the lake-only zone rules).
- Dewdney Slough: a variant of gnis:21105 in the registry. SIDE EFFECT, for the owner: the gnis-targeted name kept four
  Fraser-code lines (355991890/895/899/902, 1.6 km) the leaf prune used to drop; they are now Nicomen Slough and
  nicomen_slough.r1 binds +1,626 m. Narrowing the variant to the slough's own lines would undo it (next build).
- D5: 7 DFO places keep the joining water out now (Ash River at the Stamp, Lamprey Creek at the Morice x4, Boulder Creek
  at the Nanaimo x2): Stamp ~709 km and Morice ~289 km less walked water.
- D3: 2,866 receivers removed by the flow-back rule; trib_nearer 95 -> 98 pairs (3 newly ordered: John Hart Lake's and
  Lower Campbell Lake's tributaries under the Campbell, Tanya Lake's under the Takia); none lost. Lands with the bundle.
- Gauges (bundle, lands with the next deliver): 10 of the owner's 29 move to the section beginning at their cut; 17
  (cut on a lake outlet / another boundary) keep the projection; 11 cuts on ANOTHER blue line than the projection (Peace
  near Taylor, Seymour above Lakehead, Klinaklini East Channel, Eve below Kunnum, Bowron, Quesnel near Quesnel, Thompson
  at Kamloops, Seton, Fraser at Steveston, Fraser at Albion, Elk near Natal) are HELD on the projection: not in the 29.
- D4 guard: 69 accepted reads, 0 lost, 0 new on this run.
- Owner list (owner_list.md): 187 lines still open — 177 estuaries, 3 crossings disagree, 3 no geometry, 4 item C. No
  Fraser slough / unnamed Fraser line reaches the method on this atlas (none is held).

## Restart 2026-11-12
Done (pushed):
- a5fe40636: `full_t678b` promoted, with `--accept-boundary-loss` (16,119 D1 pond boundaries + 23 moved shore ids; no
  curated rule names any of them). The T678 inputs moved into `ATLAS_INPUTS`; `--backfill` now adds only the keys a
  record is missing.
- Staging feeds worker deployed (447 tests; gauges, dfo and inseason jobs all 200).
- a14abad36, fd4a2c54b: CI was red. Seven guards fixed at the cause, plus T681's MapLegend corner radius. New
  fast-fail item: the source-scanning tools/ gates now run when ui-native changes. CI was green on fd4a2c54b.
- f7f4fcf (T679 D1 fix): not-fishing water is never a widening (`graph.widenings`), and the fold raises if a water item
  holds one. The release's bundle stopped on Little Sheep Creek because the fold had taken 4 sewage/tailings ponds.
  The atlas was rebuilt and promoted again; it differs from t678b only in those 4 creeks.
  - The pre-T678 atlas is kept at `data/generated/atlas/full.pre_t678`. The promote refuses to delete a `.prev` the
    old bundle was cut from.
  - The atlas is current with its 13 inputs.
- 9a2580e: app 0.7.7. Version bump, a draft changelog entry, and an app_reads row.

Half-done: the release stops in deliver at `card_check` (12 problems, all on one key):
- The water is Qualcho Creek (gnis:27799). Its pond `lake:329283082`, folded by D1, straddles the Region 5/6 line, so it
  takes both zone tables (T183).
- The card's bucket "Up to 2 a day between them — all streams in Regions 5 and 6" lists RB 2 and EB/GB/CT/LT 2.
  Region 6's `trout_char_quota.r4` "1 trout from streams July 1-Oct 31" makes the true answer 1 (the engine is right).
- The fault is in `pipeline/deliver/answers/rows/groups.py`. The straddler bucket and kind numbers take Region 5's
  "2 from streams" and ignore the peer table's stricter dated stream cap. `_peer_totals` brings in only the
  replaced totals.
- Intended fix: the bucket and kind rows take the stricter of the two tables' stream caps in force on that day.
  Add a test on this case. No code has been written yet.
- Reproduce in 0 s:
  `python -m pipeline.deliver.answers.card_check --bundle <dir>/bundle.sqlite --export-dir <dir> --items gnis:27799`
  with `<dir>` holding the deliver-cache bundle/out, verdicts/out and export/out files.

Next commands:
1. Fix groups.py, run the card_check above, then run fast-fail. Commit and push.
2. `.venv/bin/python -m pipeline release --staging` (it resumes; reach and tiles are already built for this atlas).
3. After the release: spot-checks (D3 trib_nearer pairs, the 10 gauge moves, Pend d'Oreille, San Juan, Tatshenshini,
   Dewdney search, the three lake twins), then `pnpm -C app check`, then
   `deploy/staging/build-apk.sh --play` (T681 is included at d84c664e1), then the emulator smoke and the CI check.

## Card gate fixed 2026-11-12 (step 1 done)
- The root cause was not in groups.py. The group already held Region 6's r4 "1 trout from streams Jul 1-Oct 31" as a
  sub-cap, and the bucket's "Up to 2 between them" is right (brook and lake trout are char: Region 6's cap does not
  hold them). What was wrong was each kind's own number. Region 5's table wins the total, so r4 sits outside the
  winner's chain. `rows/evaluate.py` folds it into each trout's own daily (1, correct) and leaves no fact line.
  `rows/conds.fish_bands` built the bands from the row's 2 and the row's facts only, so RB, GB and CT said 2.
- Fix (mechanical under RULINGS Z4, "every cap … seasonal stream caps … the stricter"):
  `conds.A_KIND_NEVER_SAYS_MORE_THAN_ITS_OWN_ANSWER`. A kind's bands never exceed the reader's decided daily for that
  fish. The card now says: up to 2 a day between them from streams in Regions 5 and 6. Brook and lake trout 2. "Brown
  trout, cutthroat trout or rainbow trout: 1 between them" (Jul-Oct).
- Test: `test_t183_two_regions_card.py` has the shape `qualcho_5_6` (real rule set in `fixtures/t678_qualcho_5_6.json`)
  and a mutation pin (flag off → the gate's "kind ['RB'] says 2").
- Measured on the whole candidate (every named and unnamed part, flag on vs off, per fish): 9 changes, all in rule set
  1695 = Qualcho Creek only (RB/GB/CT 2 → 1 on the Jul 1, Aug 1 and Oct 1 segments). No other card moves.
- card_check `--items gnis:27799`: 0 problems. Fast-fail passed.

## Release stopped at the size gate 2026-11-12 (owner decision needed)
- Rehearsal (`--dry-run --rehearse-follows`) was clean through the build: preflight, fresh-clone tests at fad6ebab7.
  There was no candidate yet, so the follows were not rehearsed. CI green on fad6ebab7.
- The real `release --staging` ran deliver (3m52s). The card gate passed on every card (16,081 groups on 7,198
  distinct cards, 0 problems). `deliver release` then refused the package (log
  `data/generated/build-logs/2026-11-12T125409/deliver.log`):
  `SIZE GATE: conditions: 963,726 B gzip > 950,000` · `core: 1,508,127 > 1,450,000` · `web first_search: 1,508,127 >
  1,500,000 (core)`.
- Cause, measured (bundle live vs candidate): D1. The 16,119 folded ponds' sections now belong to named rivers.
  `item_section` 65,237 → 81,357 rows (+16,120); core gz +119,818 (+8.6 %). `section_gauge` 108,075 → 114,140 rows
  (+6,065: a folded pond now takes its river's gauge); conditions gz +47,677 (+5 %). Other files move a little and stay
  under their gates (water 2,580,783 / 2,650,000, down 1,166,509 / 1,250,000).
- Not touched: the gates (`release/build.py` `SIZE_GATES`, `release/contract.py` `WEB_BUDGET["first_search"]`). Raising
  a budget is the owner's call. Options: (a) raise core to about 1.65 MB, conditions to about 1.06 MB and first_search
  to about 1.65 MB, each with this measurement as its reason (as T252/T388 did); the first search on the web
  downloads ~120 KB more. (b) A ticket to ship the folded ponds' sections more compactly (not designed). (c) Undo D1
  (not recommended: it is the owner's ruling).
- Held: APK 0.7.7. Three of its four changelog notes describe this data release (tidal ends, ponds follow the river,
  Dewdney search), so it waits for the release.
- Resume after the decision: `.venv/bin/python -m pipeline release --staging` (clim/reach/tiles are up to date; deliver
  reruns, ~4 min).

## Size decision applied 2026-11-12 (owner: "Raise now, shrink later …")
- RULINGS O5: `core` 1,450,000 → 1,650,000, `conditions` 950,000 → 1,060,000 (`release/build.py` SIZE_GATES), web
  `first_search` 1,500,000 → 1,650,000 (`release/contract.py`, `pack.generated.ts` re-emitted). Nothing else moved.
- T686 opened: shrink shipped data across the board (survey of the live release's 52 files by table in the ticket).
- OWNER-TODO row ANSWERED. Next: release resume (dry-run rehearsal, then the real run), spot-checks, APK 0.7.7.
- Commit ef9232881 (pushed; pipeline CI green). Fresh-copy tests at that HEAD: rc 0.
- RELEASE NOT RESUMED (2026-11-12): the rehearsal (`--dry-run --rehearse-follows`) would stop twice, both on the T685
  lane's UNCOMMITTED, in-progress work in the shared checkout: (1) preflight: `pipeline/atlas/splits/coast.py`
  uncommitted (also dirty: `pipeline/atlas/build.py`, `inputs.py`, `coast_held.py`, `common/curated.py`, `config.yaml`,
  `data/curated/waters/tidal_limits.json`, `shore_held.json`, two tests); (2) fast-fail: `test_next_rebuild.py` (3) and
  `test_tidal_cut.py` (1) fail on that dirty tree. The build plan would also RUN reach and tiles (code changed) and
  warns the atlas is behind tidal_limits.json / shore_held.json — i.e. it would ship T685's half-built work, not the
  existing atlas. Resume only once the T685 lane has committed (and its atlas promoted) or its tree is clean:
  `.venv/bin/python -m pipeline release --staging --dry-run --rehearse-follows`, then without `--dry-run`. APK 0.7.7
  stays held behind this release (its changelog describes this data).

## Release + APK 0.7.7 lane (2026-11-12, after T685 promote)
- 05cec1dfa: T685 DONE (atlas promoted), RULINGS E25 note (Turner/Noons stays fresh, owner). CI green (pipeline on
  05cec1dfa, app on 12f567f55).
- Rehearsal (`--dry-run --rehearse-follows`): no stop; fresh-copy tests passed at 05cec1dfa; plan reach/tiles/deliver RUN.
- Real `release --staging --push` started.
- Build ok (1,668 s): release r-0a9930fb8826. Log: "gauge placement: 2 station(s) on the line the owner named
  (08MH028 -> 356364114:545, 08NK016 -> 356570562:108141)"; 10 moved (the owner's list). Size gate passed.
- Binding diff (546 rules, 52 licensing, 51 DFO, 290 tidal, 3 unresolved = the lake twins) classified section by
  section (scratch `rel/classify.py`, `recut.py`): rules = folded ponds (D1) or one of 419 re-cut lines; those lines =
  shore cuts moved/added/removed (coast rule), San Juan's Deering Road cut, 209 lines whose tidal status changed (T685),
  and 4 Dewdney/Nicomen lines (documented side effect); DFO "other" = exactly D5's 7 places; licensing = ponds + re-cut.
  Nothing unexplained -> `--accept-diff` given.
- Follows stopped twice, both D1, fixed at the cause: (1) taps.py declarations for the golden parts the fold grew
  (Fording, Dean, Kitimat, upper Fraser; afb4a72cd); (2) waterMap T345 Coquitlam test names its folded pond (8282c93aa).
  Two golden pages changed (a handle renumber; upper Fraser "3 lakes" -> "1 lake"): accepted. Walkthrough/seo
  timeouts seen once under load, passed alone and on the rerun.
- RELEASED: r-0a9930fb8826 = LATEST, on staging (smoke 18/18 + 1 WARN: the staging SITE still serves r-483b295de57c;
  the site deploy is separate). One smoke flake (rule-book PDF timed out at the 11.7 MB download prompt; passed on rerun).
- Spot-checks (agent, read-only): all PASS. Notes: the 10 owner gauges already sat at their cuts in r-483b (only
  08MH028/08NK016 moved in this release); Nakina's note is not in the release files, it travels in
  private/feeds/dfo/entries/region-6.json on staging R2 (verified identical to curated, note + spatial_caveat).
- Next: APK 0.7.7 (changelog revised to this data: tidal rule, Nakina).
- APK 0.7.7 build 29 (87d2eeac0, built from b4dd0fac1; patched MapLibre 7bf3dc29…): Play INTERNAL, completed; owner
  emailed. Checks: `pnpm -C app check` (7,529 tests), fast-fail, app CI green on b4dd0fac1 and 87d2eeac0; no skip flags.
- Emulator smoke NOT run: Maestro cannot see the emulator ("emulator-5580 is not connected") because the owner's VS Code
  listens on 127.0.0.1:5555 (adb lists it as an offline emulator-5554). Not touched. Every Maestro flow fails until that
  port is free; the `install` scenario reached the notification prompt, then failed on the tap.
- Screenshots driven by adb + uiautomator instead (scratch shots-adb.mjs): /Users/dawson.horvath/bcfish-session-backup-2026-11-12/apk-077/.
  Findings: (1) Deas Slough Map view keeps the previous camera (Okanagan) and says "The parts of this water are not
  available" — a wholly tidal water has no parts; needs a ticket. (2) Search "Vedder River" -> Open lands on Chilliwack
  River part 1 (Vedder Crossing -> Tamihi). (3) Part warning not captured (Elk Lake / Chilko show no warning in view).

## After build 29 (2026-11-12, agent: staging site, walk test, Vedder search, T670 G7)
- Staging SITE redeployed on r-0a9930fb8826 (`deploy/staging/site-all.sh`, web export from 9ccbe46f7 + SEO pages on
  LATEST): staging smoke 19 pass, 0 warn, 0 fail ("site release matches the manifest release").
- `test_dfo_salmon::test_a_watershed_walk_passes_through_its_lakes_but_collects_streams`: STALE EXPECTATION, not a
  regression. The 657 "lake sections" are D1's folded ponds (each owned by one stream item; 0 on `full.pre_t678`). The
  test read the polygon's drawn shape; it now reads the water kind (`reach.water_kind.kind_of`, rule 55), pins that the
  folded ponds are carried, and reads the graph's own registry (4a4af38eb).
- T687 (Vedder search): the search row "Vedder River" opens the Vedder correctly; the book pointer above it ("Vedder
  River: see Chilliwack / Vedder Rivers") opened the target row's first water, the Chilliwack. Fixed in the bundle: a
  pointer opens the water it names (5 pointers move: Vedder, Bear Lake, Cedar Lake, Mara Lake, Waneta Reservoir;
  5c294120d). Lands with the next release.
- T670 G7 (RULINGS Li13): the licence answers already carry the classified-water terms (requirements[].terms); the app
  now draws them under the licence item, "From the rule book:" + the book's words (fab4c0f6f). No release needed; shows
  with the next app build. Held for the owner: the basic licence's two administrative terms (not drawn).

## Pipeline-side small tickets (2026-11-12, agent; no release, atlas or APK)
- Done: T631 item 1 (197686263: the release check diffs a DFO-only change; keyed by run files, not the rules digest
  alone), T560 item 2 (cc50a2379: paint sync uses the status index's decoder and the build's no-access threshold),
  T560 item 6 / T601 item 2 Python half (63a3e6173: `manifest.live`, 8 hand readers of LATEST moved, guard test),
  T629 items 3-4 (4734dbdca: WILD paragraph sourced and out of the transcription; our notes moved out; T629 DONE),
  T540 item 2 (116872b43: `scripts/status_archive.py`, STATUS 125 -> 119 KB, fast-fail size guard).
- Closed as already done: T601 items 1, 3, 4 (T547/T557/T554), T606 item 4 (T547), T677 items 4-7 (T678/T685).
- Rides the next release: nothing new ships from these (T629's files are not bundle inputs; the rest is tooling). Rides
  the next atlas: T606 item 2, T560 items 3/5, T631 item 5.
- "Not done" list above: items 1-2 done (promoted); 3, 4, 5, 7 need an atlas rebuild (rides the next atlas); 6 (store
  fact for key-0 tidal sections, = T672 1b) needs no pipeline change: the release already lists every tidal section in
  `undrawn.json` `tidal`, so it is an app read (next app wave); 8 waits on the other lane's split-classification field
  (`coast.authority` reads `source`/`authority`) and a `coast_held` rerun with the next atlas.
- Held: T606 item 3 (needs deliver's read set), T593 (owner's scrape; new stored field re-keys DFO entries / marks),
  T661 gate install (another lane in app/; mid-run commit question), T540 ledger-cell cap, T677 items 1/3.

## Lane 2026-11-12 (evening): T688, emulator smoke, part warning, store shots, owner to-do
- T688 (Deas Slough Map view) DONE, 92849a835. Cause: a wholly tidal water lists no part (T278), so `waterMapView` had no
  view model: no `fit` (camera kept the Okanagan) and the bar printed "The parts of this water are not available" (and a
  frame naming the tidal piece threw "part N is not in its group"). Fix: `loadTidalWater` (data), `useTidalWaterMap` /
  `waterMapTidalView` (ui), the Map view frames the tidal pieces' boxes and the bar says "Tidal water" + the tidal card's
  first paragraph + "Show regulations for this water". Test `WaterMapView.tidal.test.tsx` (fails before: "not available").
  Targeted suites green (228 files, 2,291 tests, unfiltered), typecheck, knip, boundaries, tickets guard. Wording is the
  owner's to confirm (default built in). Emulator check owed on the next APK.
- Emulator smoke RAN against build 29 (APK stamp 87d2eeac0, `--allow-stale` with reason) on emulator-5580, heavy lane 2.
  Cause of "not connected": Maestro lists devices through the adb server and ONE offline entry (the phantom
  `emulator-5554` from the owner's VS Code listener on 5555) throws away the whole list. Fix (testbed only, no port or
  process touched): Maestro's hidden `--host 127.0.0.1 --port <console+1>` connects straight to the emulator
  (`lib/device.mjs maestroDeviceArgs`, used by `lib/maestro.mjs` and the hierarchy fallback). Report in
  bcfish-session-backup-2026-11-12/smoke-b29/.
  PASS: install (first run 951 s, updates log all ok), satellite 3/3, edge 10/10, part-steps 4/4, switch 2/2.
  FAIL: regs-open 9/10 (lake "Cultus": the test bed's [target] probe logged no tap target; the map itself framed the lake,
  shot 016) and water-nav 6/7 (step 7 "Maestro Android driver did not start up in time", a driver start timeout, the first
  six steps passed). Neither looks like an app defect; rerun both before reading anything into them.
- Part warning captured: Shuswap Lake ("Part of this lake is closed today", Salmon Arm Bay closure Sep 1-Dec 31) and
  Quesnel Lake ("Part of this lake closes at times"): apk-077/18-part-warning-shuswap-lake.png/.txt and 18b-...quesnel-lake.
  (T623 F4 `partClosedOf`: lakes with a closure on a part the map cannot draw; Elk Lake / Chilko had none.)
- Store shots: `node app/tools/store-shots.mjs --only play-phone,play-tablet7,play-tablet10` shoots the STAGING WEB site
  (headless Chrome), not the APK, so they show whatever release the staging site serves (still r-483b295de57c until the
  site deploy). Copied to bcfish-session-backup-2026-11-12/store-077/ (the repo's store/ and raw/ dirs are gitignored).
- Owner to-do: row "Phone check of build 29 (app 0.7.7)" in handoff/PLAN/OWNER-TODO.md (not yet added to the Owner To-Do page).
