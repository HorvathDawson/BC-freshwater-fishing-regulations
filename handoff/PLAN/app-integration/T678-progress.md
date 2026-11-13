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

## App wave 2026-11-12 (night): ledger tickets, smoke rerun, store shots, owner to-do
- Done: T603 items 5-6 (46afd5ad1: an emulator run on a non-production build reports as `test`; the `/data/local/tmp`
  SecurityException is `expected`; worker not deployed), T671 D3 render test (f21981ea2; mutation checked), T670 G7 web pages
  (353429f7a: `seo/model.ts` carries the "You need" book terms; test fails before), T560 item 6 / T601 item 2 JS half
  (444c65675: it was already in the tree since ff787ac34; T601 now DONE; stale "newest" comments fixed).
- Default recorded: T467 (f5f1d61ed): colour-blind check stays on all themes; ticket stays OPEN for the shoreline look
  (Work 1 is not built, so it was not "only waiting on the default").
- Held / left: T672 1b (8085d13cd): NOT an app read. `undrawn.json` is a build-side file and is not shipped in the release
  (the earlier note above was wrong); the app cannot tell an unnamed tidal section from one outside B.C.; needs a pipeline
  fact (e.g. a bit per section in `regs-sid`). T631 item 2 app half: no bundle flag exists yet (pipeline first). T465 (a
  label-collision rule to pick, shots) and T504 (prototype, owner picks) are design work, left. T603 items 7-8: new emulator
  scenarios / device work on the next APK.
- Emulator rerun (build 29 APK, lane 2, `--no-r2`): water-nav 7/7 PASS (the step-7 driver start-up was a flake).
  regs-open 9/10 again, same step (lake "Cultus": no `[target]` line): repeatable, not a flake; cause undecided (probe or lake
  colour); ticket T689.
- Store shots re-shot from the staging site now on r-0a9930fb8826 (play-phone, play-tablet7, play-tablet10, 21 images) into
  `bcfish-session-backup-2026-11-12/store-077/`. The `appstore-*` folders there are the OLD set (not re-shot). "Nov 11" on the
  Vedder shot: the old shots were taken from the site built on Nov 11 (the site still served r-483b); the site build uses the
  B.C. clock (`bcNow`, T551) and now says "Today, Nov 12". No defect found; no ticket.
- Owner to-do: 8 rows added to the Owner To-Do page (`todos`, group "Look and pick", orders 6-13) and OWNER-TODO.md;
  the build 29 row now says Vedder is T687.

## T689 lane 2026-11-12 (night): regs-open lake step
- Cause: the test bed's tap-target probe, not the app. Since T478 (2026-11-07) `findTarget` skipped every point with any still
  water under it, and a lake is all still water; the logcat shows `[target] {"none":true}` (the scenario's "no [target] line"
  was the wrong words). Last pass of the step: 2026-11-06, before T478. Four hand taps on Cultus Lake on build 29 each opened
  its regulations (44–58 ms): no user is affected.
- Fixed in 606037505: `isTapTarget` (sections.ts) keeps T478 for another water's still water and accepts the lit lake's own;
  test fails before. `pnpm -C app check` (lane 2, unfiltered) passed: 622 files, 7,546 tests. The 10/10 rerun needs the next
  test-bed APK (none built here). Evidence run: regs-open 9/10 again on build 29, `{"none":true}` logged.
- "Not fishable": wrong reading of the screen. Cultus Lake is "Own rules" in r-0a9930fb8826 (no closure); own-rules orange at
  0.6 over the basemap's cyan water mixes to khaki (computed (176,163,91), on screen (174,166,97)); the "Not fishable" row is a
  fixed key row. Same colour on the 11-04 / 11-06 shots. Whether an own-rules lake may look this close to the "Not fishable"
  swatch is a look question for the owner (not decided).

## Next-atlas small items (2026-11-12 evening, agent) — candidate `data/generated/atlas/full_next`, NOT promoted
- Code (0b23c1403, pushed): T606 item 2 (lake-edge clash suffixes numbered by place; a lake drops a flowing name any
  stream item owns), T560 item 3 (`wsc.code_owner`, one owner of a shared code), T560 item 5 (`tolerances.meets_at`,
  one area slug). Tests `test_next_atlas_items.py`, mutation-pinned.
- Build: `scripts/heavy.sh next-atlas 50 -- python -m pipeline build --atlas` (32 min, rc 0). Parity: items 21,279,
  sections 4,612,466, boundaries 11,361, all unchanged; 6 items changed: Teslin River (gnis:2670) — its two "Hutsigola
  Lake" edge ids swap (`teslin_river__hutsigola_lake` now lake:329560236, `_2` lake:329560232; named by nothing
  curated); Kluskoil, Tsacha, Tundra and both Euchiniko Lakes lose the search names 'West Road River' and
  'BLACKWATER RIVER'. section_handles, tidal_sections, splits.resolved, region_home, item_points, shore/coast records,
  split_anchors and braid records byte-identical.
- Reach on it (`reach.cli --build …/full_next --out data/generated/reaches/nextatlas_cand`, 10 min, rc 0) and the
  binding diff against the live release's baseline (`binding_diff data/generated/release/baseline/r-0a9930fb8826
  data/generated/reaches/nextatlas_cand`): NO BINDING CHANGED (rules, licensing, DFO places, tidal, outside). Standing
  (as live): 2 withheld pieces, Chipmunk/Toms Lake scope unresolved, the 3 accepted Skeena channels.
- Angler-visible: searching "West Road River" / "Blackwater River" finds only the river, not five lakes as well.
  Nothing else moves.
- Release dry run: `pipeline release` builds from the served atlas and has no option for a side atlas; it can only
  rehearse this after the promote.
- Held / left: T631 item 5 (owner's 11-11 ruling keeps the Skeena channels out; default close); T678 plan 2, 5, 6
  (medium) and the legacy-reader retirement (after the next promote) — reasons in T678's ticket; item 8 settled by T685.
- Promote + release (main session, after the owner sees this): `.venv/bin/python -m pipeline.atlas.promote
  data/generated/atlas/full_next --dry-run`, then without `--dry-run` (no accept flag expected: no boundary lost, no
  curated id moved); then `.venv/bin/python -m pipeline release --staging --dry-run --rehearse-follows`, then
  `.venv/bin/python -m pipeline release --staging --push`.

## Night lane 2026-11-12 (agent): T672 1b, T669 item 1, Skeena channels, plan 6 count — no atlas, release or APK
- T672 1b DONE in code (83b1477ba; T678 "not done" item 6): the store writes `section_tidal` (the bundle's `tidal` sids,
  one byte per section), shipped in `regs-sid` (ADDITIVE, still regs-sid/1; `water` untouched); the app reads it on a
  key-less section and draws the existing tidal banner. regs-sid +2,934 B gz measured on r-0a9930fb8826's data. A
  release built from the live bundle (test_release.py) passes contract + size gates. Shows with the next release.
- T669 item 1 DONE (91398f801): one shared case table both strictest readers run; test only.
- T631 item 2 HELD (not small: bundle table + release file + new page words). T600, T602 not started (each M, needs a
  full-build proof); T667 slices not started.
- T631 item 5 (Skeena channels): NOT tidal (tidal boundary m 6,405; cut m 123,961). By geometry and FWA codes the three
  channels lie wholly above the cut; the "straddle" is graph edge 778568 (x/y at …099's mouth, measure m 119,499, 3.35 km
  off). Options A–D + pictures in the ticket and `bcfish-session-backup-2026-11-12/skeena-channels/`. Nothing changed.
- Plan 6 count (owner leans to a manual list): 1,204 unnamed lines with no graph piece in tidal water (1,133 FWA 1450
  construction lines inside tidal river polygons, 71 in the sea). Recommendation: no list (no output can change);
  OWNER-TODO 11/13 and the To-Do page.
- Final release must rebuild: deliver (store → regs-sid gains `section_tidal`; status index for T672 item 2) — the
  release command does this; the app build reads it. Nothing here needs an atlas or reach rerun.

## 2026-11-12 (owner answers by dialog; app lane: T692, T693; tickets T694, T695)
- Owner answers recorded in OWNER-TODO (ANSWERED 2026-11-12): T631 Skeena side channels: A, stay outside the place (nothing
  changed); plan 6 pruned tidal lines: no list (1,204 lines stay a count, no file); T688 wording stays; T677 Ucona keep;
  candidate atlas (`full_next`) is promoted with the final build; T673 parked until after closed testing.
- T692 (c4f101908): the tidal fill stays drawn and labelled "Tidal water"; a tap on it does nothing (sheet, wiring and
  strings removed, web and native). A tidal page asks no salmon: "SALMON (DFO) Closed" came from the app (the salmon card
  answers closed for a water DFO does not list), not from pipeline data; nothing for the pipeline lane. RULINGS P52.
- T693 (df0136892): an in-season notice matched to a water this release does not hold falls into the bell and is reported
  once (gap 1). Gap 2 (a "new" mark on an amended matched notice) is T694, after closed testing. RULINGS O6.
- T695 (ticket only, after closed testing): fold more small widenings into the stream and draw the stream line instead of
  the unnamed polygons (the 456, Q124/T153), real lakes stay lakes; measure first; links T686 and D1. RULINGS E27 (Q124: keep as built now).
- Pond fold (D1) and the 456: the census is older than D1; which of the 456 are still lakes on the live release is the
  first thing T695 measures.

## 2026-11-12 (owner answers: Whiteswan T690, vessel parts T209, silent gauges T691, diversion gauges T696)
- T690 (c41d54742, RULINGS E26): Whiteswan Lake's inlet & outlet streams. Printed p.40 checked against the coordinator's
  reading: no difference. r1 (No Fishing) now names all 15 inlets (Cave Creek, Inlet Creek, 356560775 above the lake and
  12 unnamed lines named "Whiteswan Lake inlet stream" in name_variants); r2-r4 only below the existing falls cut (599.2 m,
  2,424 m below the lake). No new cut. Rides the final atlas build (the 12 become items): r1 2 → 16 sections; on the live
  atlas it binds 4 (Cave and Inlet creeks join now). The T637 inlet name stays.
- T209 (c63427bbd, RULINGS P51): undrawn-part vessel rules drawn in the Vessel restrictions section with the book's locator
  ("No vessels — in swimming areas, as buoyed and signed"); the caveat no longer repeats them. App only (core), no release
  rebuild; shows on the next app build. 22 rules (the ten + 12 named-place ones, Claude's reading).
- T691 (1935c1dc9, RULINGS P53): gauge cuts follow `may_speak`; 32 new cuts ride the final atlas build. Until then a
  deliver on an older atlas (live `full`, current `full_next`) stops at the gauge record check — expected. Five stations
  need an owner match call (OWNER-TODO). Watch 08PA004 (1,015 m off its line: likely no cut).
- T696 (RULINGS P54, nothing built): "Canal gauge also serves river" fills nothing — of the 28 gaugeless sections none is a
  diversion gauge's (measured on `full`, shed climb guard on vs off). Two questions with the owner.
- Checks: affected pipeline tests 291 passed (unfiltered); `pnpm -C app check` stops at typecheck on
  `app/packages/map/src/web/useInput.ts:166` (committed by the T692 lane, c4f101908), not on these files.

## 2026-11-12 — T697: Kanaka Creek's stretches and split names (T204 follow-up)
- Owner 2026-11-12 on the T204 phone-check line: "No it has to use split names like we said before ??". The earlier rule:
  T509 owner 2026-11-08 "Ends only" (no "· stretch k of n") and T517 owner 2026-11-09 (every part named "lo → hi" from
  its two end tokens by `spanName`, no count, no duplicates).
- The line was stale (2026-10-27, before T509/T517). On r-0a9930fb8826 Kanaka reads "CPR Bridge (tidal boundary) → 112
  Avenue bridge", "112 Avenue bridge → Cliff Park Falls" (closed all year), "Cliff Park Falls → A lake outlet".
  Province-wide 562 waters of several entries: 0 named by a count. Labels are made in the app (`partName` → `spanName`).
- 77e856247: province-wide guard in `app/tools/stretch-names.test.ts` (mutation-checked). No release or app change needed.
- For the owner: the unnamed "A lake outlet" end (proposed: said near the nearest named feature, as crossings are); the
  Parts sheet's "Closed all year · N stretches" second line (45 waters; the sheet is reachable only by start link).

## 2026-11-12 — T670 fee line + T319/T244 second store-shot set (proposal)
- **T670 (owner "Show the fee only")**: the basic licence "You need" item shows the disabled-resident $1.14 sentence (book's words, under "From the rule book:") for a B.C. resident adult; "one annual angling licence per year" stays off. The app does not ask about disability, so the line is read from the stored answer of the disabled resident adult profile (record picked by `fee_cad`). Commit 1ce54b2fa; tests fail before (core licence.test, live t670.release G8, web t670.web.release G8); RULINGS Li13b; OWNER-TODO default 6 now ANSWERED. Shows with the next app build / web deploy (staging still has the old Licence tab).
- **T319 second set**: new scenes in `app/tools/play-shots.mjs` (10 regs bar+table, 11 conditions chart with forecast, 12/13 Conditions map Change/Forecast on the same camera, 14/15 river map + one section's rules), and `compose` shots in `store-shots.mjs`/`store-shots.json` (real captures stacked under label bands, nothing drawn). Proposed order: map; year-at-a-glance rules; sections + one section's rules; flow chart with forecast; Change/Forecast on the map; salmon; runs; offline (8, Play's cap). Images (gitignored) in `~/bcfish-session-backup-2026-11-12/store-077b/play-phone/`; store-077 untouched. Dropped for room: Search, the old Conditions pane.
- Limits: the web chart's hint line reads "Scroll to zoom, drag to move…" (web wording; the phone says its own); the sections picture does not mark which line is the picked section; staging's Licence tab does not yet carry the fee line.

## 2026-11-12 — T697 follow-up and T698 (owner answers)
- Kanaka's top part read "Cliff Park Falls → A lake outlet". No cut stands there: the creek's blue line ends at an unnamed
  lake (24.77 km, nothing of the creek above), so the lake is the creek's head; the atlas writes a lake edge as
  `lake_outlet`, and the display said it as such. Owner: "mouth to cut the cut to head … unnamed lakes should be unnamed
  lake near x" (RULINGS P55). 434449f3d (answers display, rides the next release): own-end unnamed lake → "Source" /
  "Mouth" (62 tops on the live export); a mid-water unnamed lake → "Unnamed lake near X" (`crossing_near`), else
  "Unnamed lake (km N)" (5 ends on Slack, Gagnon, Jones creeks; nothing named within 2 km). Kanaka will read "CPR Bridge
  (tidal boundary) → 112 Avenue bridge", "112 Avenue bridge → Cliff Park Falls", "Cliff Park Falls → Source".
- T698 caa3a0133: the Parts sheet removed ("Remove the screen", RULINGS P56): key, builder, component, block, words and the
  `choicesOnDay` read; `sheet=parts` start link refused and said. Full app suite: 7,559 pass, 2 failed — back.test's
  T58 Parts-row walk (removed with the sheet, then 26/26) and `methods-licence.render.test.tsx` x5p1 Licence (T670 G7),
  the Licence-tab lane's, not this change. knip clean, typecheck clean. Pipeline affected tests: 483 pass + 6 new; 2 fail on HEAD too (`test_part_geo`
  unlabelled-mark count 110≠106, `test_t207_split_confidence` contract pack), not this change.
- Held: the 5 mid-water ends say a km ("Unnamed lake (km 2.3)") because nothing named stands within 2 km.

## 2026-11-12 (agent: T696 Theodosia built, T209 Harrison follow-up, tidal style comment) — no atlas, release or APK
- T696 (8436b380e, RULINGS P54): owner "Leave gaugeless" (the 28) and "Show 08GC005 above the canal". Authored
  `data/curated/gauges/diversion_stations.json` + `pipeline/gauges/consume/diversion.py`: the bundle places 08GC005 at
  its intake (panels, route, `gauge.sid`), climbs only, takes 08GC006/08GC007 off every section above the intake;
  `gauge.diversion` (conditions/1 still). App: key "08GC005 · 70% · measures a diversion" + `diversionNote`. Live-atlas
  scratch run: the main-stem panel (1195804) 08GC007 lead → 08GC005 lead + 08GC008; 180 match rows 08GC006 → 08GC005.
  Owner question left: a cut at the intake (~160 m above the 08GC006 cut where 1195804 begins). Pictures in
  `~/bcfish-session-backup-2026-11-12/theodosia/`.
- T209 follow-up (81598325e): printed p.23 Harrison line holds both alternatives (r3 speed, new r4 no powered boats);
  the app draws one chip "Speed restriction or no powered boats — in small bays along the river as signed" (and joins
  the Okanagan/Kalamalka/Skaha pairs the same way). Sweep of the other 21: nothing else dropped.
- Tidal layer `$comment` no longer says tap target (81598325e); style rebuilt, outputs unchanged.
- The final release must rebuild: reach run + deliver (the corpus changed: harrison_river.r4; the bundle gets
  `gauge.diversion` and the panels), then the app build. No atlas rebuild for these (unless the owner wants the intake cut).
  Until then `test_t100_area_caveat.py::test_the_export_carries_the_caveats` (needs_bundle) fails 83 vs 84 — expected.


## 2026-11-12 (night) — Licence render test, full app check, store shot 2

- Licence render test (`methods-licence.render.test.tsx`, "T670 G7, RULINGS Li13"): the failure was a stale expectation, not the
  presenter. 1ce54b2fa (Li13b) intentionally adds the p.5 reduced-fee sentence ("If you are a resident of B.C. and disabled: ...
  $1.14.") as a second "From the rule book:" box on the basic licence item for a B.C. resident adult. The test now asserts both
  boxes for adult|resident (fee line, then the p.7 classified term), the non-resident's classified term alone, and that across
  every fixture the only book text apart from the Elk's classified terms is the fee line, on adult|resident only (a non-classified
  water carries no classified term). Not weakened. Commit 1e75ce6.
- Full `pnpm -C app check` (heavy.sh lane 2, unfiltered): exit 0; Test Files 623 passed | 5 skipped (628); Tests 7561 passed | 27 skipped (7588).
- Store shot 2 (`10-regs-bar-table`, Vedder River): scene now scrolls the page so the SALMON (DFO) section (its own year bar,
  Open today, Salmon row) sits under the tabs with the tail of the game-fish table (Protected species) above it. Live staging,
  today Nov 12 (salmon open). Line now "...and what you can keep, salmon included"; title unchanged. The trout bar and salmon bar do
  not both fit in one 360x640 view (the game-fish table is 8 rows), so the view shows the salmon bar + row with trout/char-table rows above.
  Old file kept as `02-10-regs-bar-table.prev.png` in `store-077b/play-phone/`.


## 2026-11-12 (night) — T667 slices 3–6: tests that need fewer hand edits per release

- Slice 3 needed nothing more: T666 already gives `sync.mjs` / `taps.py` a `--release` (or `RELEASE_DIR`), the pipeline's
  three LATEST tests ask `under_test.release`, and the parity falls back to the committed fixture (never absent).
- 716999ebc: the app's no-silent-skips list is keyed by helper. A new release test that skips through `liveRelease()` /
  `parityGate()` needs no list line; 18 files left the list, the 19 other skip sites keep exact counts. 15 near-misses
  still fail, and a planted skip in a real release test failed the gate.
- 0f7b29678: answers section versions are no longer typed in tests: the writer is held to the app's reader and the SPEC,
  the shipped display to the writer. Writer and reader mutations each fail (the reader was never checked before).
- c79b4516b: one table of the rulings page v36 predates (`app/fixtures/reference/v36-rulings.json`), read by the TS
  parities and the Python year-strip test (was 11 hand tables, 3 of them in both languages). Nine mutations fail.
- fb91259ef: the year-strip test's tapped-state count (121) is a relation; two mutations fail.
- Left, and why: `smoke.txt` derived (another lane is in it), the v36 inventory before any suite is retired, the atlas /
  reach-run counts (need an atlas to prove a replacement), the env contract (shared `conftest.py`), the small release.
- Fixture re-cut by hand after the next release, before → now: version pins in two tests (3 + 1 numbers) → none; the
  year-strip state count → none; a ruling on page v36 → one JSON entry (was 3–5 tables in two languages); a new release
  test's skip line → none. Still by hand: `smoke.txt` for a new test file, and the live atlas/corpus counts in
  `test_one_water_kind` / `test_review_queue` when the atlas or corpus moves them.


## 2026-11-13 — T700: Regulations header said another part's name (owner phone, Capilano River)

- Owner: Map "Part 3 of 3 · Capilano Lake → Source", Regulations "Cable Pool Bridge footbridge → Capilano Lake outlet".
- A naming defect, not a selection one and not a T517 combined stretch: parts 2 (below the lake, ends at its outlet) and
  3 (above it, starts at the lake) share one closed-all-year picker choice but are two map entries; `partLabels` named
  the choice's first part. The page loaded part 3 and its own key (1140, shared with part 2): no wrong rules shown.
- Every way in that lands on such a part showed it (Map pick, step, Show regulations, tab switch, tap); search lands on
  the lowest keyed part. 304 parts on 206 waters on r-0a9930fb8826.
- 2b49a643b: header names `inp.partIx`; `header.test.ts` Capilano shape (failed before); `tools/stretch-names.test.ts`
  (T700) province-wide guard over every part by part open, tap and search, plus the key read (reverting the fix fails it).
- Left for the owner (ticket): the long label still shows the book's wording when the part is its choice alone
  (Capilano part 1 "From the CN Railway Bridge … up to the Cable Pool Bridge footbridge" vs map "CN Railway Bridge … →
  Cable Pool Bridge footbridge"; 659 parts on 445 waters): keep, or the map's name always.

## 2026-11-13 (agent: vessel entries, HEAD test failures, part ends, Gagnon, final atlas candidate) — NOT promoted, no release
- T209 (2841fb605): owner "Keep them as two entries": the one-chip "or" join of 81598325e undone; each "or" vessel line is two
  entries with the locator (Harrison, Okanagan, Kalamalka, Skaha); harrison_river.r4 stays. RULINGS P51.
- HEAD test failures (0bfcfb84c): Fishing Creek's folded pond silent = a pass-order DEFECT in `panels.build` (polygon
  fallback ran before the gap fill; now repeated, `REPEAT_POLYGON_FALLBACK`, mutation-pinned; one section province-wide;
  clears at the next deliver). Stale pins set to the real invariant: `test_part_geo` 106 → 110 (D1 fold: Biddy, Gravel, Hill,
  Exchamsiks), Nicomen polygon → adjacent line (same final panel), smoke id for the T697 test file. `test_t207_split_confidence`
  passes at HEAD and at caa3a0133/8436b380e/434449f3d: a dirty-tree transient, nothing to fix. `test_t100_area_caveat` 83/84:
  the corpus (harrison r4) is ahead of the release; clears at the next deliver.
- T697 part ends (040b79d16, subagent): owner "Yes, read the real boundary" — Slack "Region 8 boundary", Jones "B.C. border",
  a cut aliased onto a lake edge keeps its name (also Duncan "Duncan Dam", Mitchell weir, Babine counting weir). Gagnon's
  top end (km 3.66) has no real boundary yet. RULINGS P55 follow-up. Rides the next deliver.
- Gagnon (NOT BUILT, owner question): the 58 ha lake wbk 329465442 is FWA "lake" (Williston "reservoir") behind what looks
  like a rail/road embankment, joined only by the creek's 74 m channel — contradicts "the unnamed lake is part of Williston".
  OWNER-TODO + To-Do row `gagnon-williston`; picture ~/bcfish-session-backup-2026-11-12/lake-ends/gagnon_creek.jpg.
- Final atlas `full_next` (30 min, rc 0; includes T606 item 2, T560 items 3/5, T690, T691; T696 intake cut answered "Leave
  it"): items 21,279 → 21,291 (+12 Whiteswan inlet streams); boundaries 11,361 → 11,382 (+26 gauge cuts, length cuts re-spaced
  on the Fraser x4, Nechako, Muller, Iskut: 6 gone, 1 new); 49 items changed (27 waters gain a gauge boundary; 5 lakes lose
  "West Road/Blackwater River" search names; Teslin's two Hutsigola edge ids swap; area counts). Sliver gate clean.
  T691 watch: 08HDX05 joins 08HD005's cut; 08MH152 joins the border cut; 08LG028 joins Douglas Lake's outlet edge; 08NH126 the
  Duncan Dam end; 08PA004 (1,015 m) and 08JB013 (520 m) no cut (over `PROXIMITY_M` 400 m).
- Reach `data/generated/reaches/nextatlas_final` vs `release/baseline/r-0a9930fb8826`: 301 rules, 2 licensing, 13 DFO places,
  0 tidal/outside, 1 diagnostic. By metres (scratch `cls/classify.py`): 205 rules, 2 licensing, 12 DFO = same water under new
  section ids (the gauge/length re-cuts). The rest: harrison_river.r4 (new rule, = r3's 8 sections), Whiteswan r1 (16
  sections, T690), and 94 zone/provincial rules + White River r2-r4 + DFO 4:region:section gain 7 short unnamed headwater
  lines (271-923 m) above two newly named Whiteswan inlets (now kept as sections; zone rules only, not r1 — Claude's reading:
  an inlet's tributary is not the lake's inlet). Nothing unexplained. Diagnostic: harrison r4's ambiguous cut, as r3.
- Promote dry run: rc 0; boundary_loss 0; no curated boundary id gone or moved; no curated item id dead/absorbed. The plain
  promote needs NO accept flag. The release's binding diff will stop for `--accept-diff` (the changes above).
- Fast-fail (FAST_FAIL_ALL=1): all passed (41 s).

## 2026-11-13 — More: Settings screen, Freshwater Atlas, update lines (T703)
Owner's phone report (build 29, the More tab). Done in code, not yet in a build:
- Staleness (`core/src/staleness.ts`): Freshwater Atlas and Basemap moved from an age line (730 / 365 days) to `dated`:
  their date and "Changes rarely", never "Likely out of date" from age. No signal exists that a newer atlas is published
  (the feed's notices cover HYDAT only), so nothing can mark it out of date. Synopsis (lapse date), Map tiles (the pair)
  and HYDAT (feed notice; 18-month fallback) were already facts and are unchanged.
- Settings: a first row on More opens `SettingsPage` (Data / Privacy / Display); the four controls moved unchanged
  (mobile data, low-bandwidth on the web, crash reports, palette). Words naming their home now say "More → Settings".
- More above the table: the "Checked ..." line and every healthy source's line are gone; the button, a failed source, a
  release the app cannot use, and the in-season card stay (ticket T703 lists why). `check-platform.mjs` allowance moved
  from MoreScreen to SettingsPage (the same one inline web check).
- The one-off e2e `tools/updates-e2e.mjs` now reads the kept `data-updates-notice` (a script, not in the suite).


## 2026-11-13 — T667 slice 5b: page v36 inventory and retirement (owner: before closed testing)

- The page v36 suites now run page v36's own CODE over the release under test, not its frozen data: `page-oracle.mjs` +
  `page_blocks.py` (Python reference decoder) write `app/fixtures/golden/page-oracle.json` (rules by durable key; re-cut
  by sync.mjs; a change stops the release). First cut vs frozen hydrate.json: 73 states differ, each one a v36-rulings
  entry. Re-cut twice: byte-identical.
- 5618e63bc settle, 3e933cc04 card, 34a4e0f61 describe (page decode over all 3,401 release rules), afb6a5eda Python
  year-strip test; hydrate.json, hydrate.mjs, v36-rulings.json deleted. Inventory table (14 families: 6 re-homed, 5
  retired, 4 kept) and every mutation in the ticket. Two notes-wording mutants survive the parity before and after (no
  release rule has them); core unit tests catch them.
- Hand work per approved ruling: was one v36-rulings.json entry (plus reader code in two languages for a new shape, and
  literal re-pins); now none — a moved golden answer is a page-oracle.json diff the release stops on.
- Left: a smoke.txt-named node id (`test_the_ruling_moves_only_the_nov_jun_keep_days`) kept until D6; slice 8 planned in
  the ticket (build after the conftest / ci_no_build lane).

- Pipeline suite, chunked, unfiltered (`scripts/heavy.sh … bash scripts/pytest-chunked.sh`, then `-- -m slow`):
  fast 7,409 passed / 12 failed / 275 deselected; slow 268 passed / 6 failed / 1 skipped.
  Clear at the next deliver (bundle behind the code/corpus): test_stream_polygon_panels (Fishing Creek, 0bfcfb84c),
  test_t100_area_caveat (harrison r4), test_part_names_unnamed_lake (040b79d16), test_release contract (T696 `gauge.diversion`).
  Stale since r-0a9930fb8826 / T678 D1 (pins to re-measure; T678 owner): test_part_runs x2, test_side_channel_water Nicomen
  16 vs 10, test_t89 x2 (Endako 29 vs 20; the three lake twins r5b/r6b/r9b bind no water — documented D1 effect),
  test_review_queue P3 99 vs 103; slow: test_one_water_kind x4, test_unnamed_trib_sweep (committed sweep moved),
  test_watershed_parts Skeena lakes — not checked one by one. Today's lanes: test_gear_omission (kept gap harrison r3
  'powered_boats' closed by r4, T209 81598325e), test_t117_walks (diversion.py reads up_adj unclassified, T696 8436b380e).


## 2026-11-13 (night) — T704: store pictures from ONE command

- **Already scripted** (T319, 2c1d9f04a, 72db95bd9, 2503910a8, 7da218e7c): `app/tools/play-shots.mjs` (headless Chrome scenes on the STAGING web site) and `app/tools/store-shots.mjs` (framing at the store's pixels, stacked "compose" shots). **Done by hand**: running them, the choice of crops, copying into the backup folder; nothing pinned the clock, and nothing checked that a pane held its content (a blank or stale pane would have been framed).
- **Command**: `node app/tools/store-shots/run.mjs` (flags `--out DIR`, `--at ISO`, `--site URL`; none needed). Source: the staging web site, as the approved set (same code as the app, no emulator/APK, same on any machine). Out: `app/dist/store-shots/play-phone/` = the 8 PNGs at 1080x1920 + `report.json`. Slow when the machine is loaded (about 5 min a scene at load 25).
- **Pinned**: page clock (default noon B.C. today, `PIN_AT`), light theme, 360x640 @3, notices seeded seen, spot seed, waters by URL, Conditions camera 10 km. **Not pinned, checked**: live feeds and the site's release (no frozen feed snapshot was built; an empty feed fails a check).
- **Checks** (`app/tools/store-shots/checks.mjs`, test `checks.test.ts`, run on the page before each capture; a fail captures nothing, frames nothing, exit 1, `report.json` names the scene): shot 2 salmon above the game-fish calendar; chart has the CLEVER forecast on; Conditions maps at 10 km with coloured reaches and the Change / day controls; river has more than one part and one part's rules open; map / runs / offline panes present; no "still loading" wording.
- **Shot 2 order** now salmon on top (`store-shots.json`).
- **Run 2026-11-13 against staging**: 01, 04, 05, 09, 11, 12, 13, 14, 15, 18 pass; **19 (salmon block) FAILS by design: the staging site still serves salmon BELOW the game-fish table (predates 0cba57a73)**. So shot 2 cannot come out salmon-on-top until the site is redeployed (not done: no deploy). No framed set was produced for that reason; the passing raw captures are in `~/bcfish-session-backup-2026-11-12/store-auto/work/raw/`, `report.json` there. Shot 2's crop fractions (0.41..) were tuned for the old layout and need one look after the first run on the new site. Content of the other 7 shots equals the approved set's (same scenes, same URLs); the chart shot's "forecast on it" is checked as the CLEVER toggle on (not a pixel or series-length check).
- Listed as a standalone step in `handoff/PLAN/release/PRODUCTION.md` "Every later release"; no build depends on it (owner: "just an easy to run command").


## 2026-11-13 — T673 one in-season file for every release; T694 + T706 the notification inbox

- 785fc9914 (T673): `feeds/inseason/matches.json` is `inseason-links/4`, with no release in it. A part of a water is FWA blue
  line key + from/to metres; a phone covers every section of its own release whose line `[from, to)` the range touches
  (one parity fixture for core and Python). A river's lake pieces are all or none. The publisher's off-LATEST refusal is
  replaced by "every range touches its water on LATEST, and is exact on the release it was resolved on" (shifted-measure
  mutation refused). `notice_waters.json` re-resolved on r-0a9930fb8826: only `lakes` lists → `true`, one stamp; no match
  changed. Build 29 (reads /3 only) keeps its last links, else the bell; no old-format copy is published.
- Main session at the final build: deploy the feeds worker, then `deploy/staging/sync-inputs.sh` (publishes /4), then
  the app. Until the sync, staging still serves /3 and a new app sends every notice to the bell (reported).
- 10ad08512, 8205a5c8a (T694 + T706; the commit subjects say "T700", a number another lane took meanwhile): the bell
  opens an inbox of in-season notices (New / Earlier, Mark all as read, unread-only badge, kept to end + 30 days, 200 max);
  "Updated" only when the feed links an amended row (`changed` + `previous`). Shots from a fixture feed on staging waters
  (staging has no new/amended rows and still serves /3): `/Users/dawson.horvath/bcfish-session-backup-2026-11-12/notifications/`.
- Tests unfiltered: pipeline `test_inseason_publish.py` 32 passed (+ review/feed-format/sync/follows 68 total); app core /
  data / ui / ui-native in-season 95 passed; feeds worker 447 passed. Full `pnpm -C app check`: see the final report.

## 2026-11-13 — T707 pinch zoom momentum (owner, build 29: "No zoom flick on map in app")

- Cause: nothing of ours switched it off; MapLibre Android 13.6.1's own scale momentum gate (bytecode,
  `MapGestureDetector$ScaleGestureListener.onScaleEnd`) reads the LAST touch event's span change against the FIRST
  pointer's speed only. Emulator A/B (`zoom-flick`, raw multi-touch as root): MapLibre's gate carried 11/12 two-finger
  flicks and 5/12 one-hand ones (thumb still); 3/6 with sparser touches.
- 1468cfba5: the binding patch carries a pinch itself (`tunePinch`: both fingers' span rate, still within 40 ms, MapLibre's
  size/curve, settle held to its end); MapLibre's off. Fix APK: 24/24, held pinch 0; double tap, two-finger tap and quick
  zoom pass. Lockfile: patch hash only. Guard `app/tools/pinch-momentum.test.ts` (mutation-checked). iOS read only:
  MLNMapView's own pinch momentum, no binding switch. Native: rides the next APK.


## 2026-11-13 — T706 test bed: the inbox sheet handled once, in `appUp`

- `appUp` (`scenarios/common.mjs`) detects the auto-opened notification inbox sheet by testID `inseason-notice`, records a
  finding, closes it with Done, fails if it will not close; zoom-flick's local copy and T360's old "In-season changes"
  branch are gone. Run on lane 2 (t707 APK, stale allowed, `--no-r2`): install + regs-open + zoom-flick 3/3 passed (the
  finding appeared in install). Static + test-bed unit tests, knip, check-tickets pass. No "opens once" assertion (needs
  feed data the test bed cannot arrange). Note in T706.

## 2026-11-13 (agent: suite pins, T600, Gagnon, junctions) — IN PROGRESS, NOT promoted
- Job 1 DONE (8ee53d251): the 14 stale/slow failures set to their real invariant, each cause read (D1 fold, T664 oxbow
  takes, T209 r4, T696 diversion); the three steelhead lake twins are no regression (all 55 old sections bound by the
  stream sibling, same words, take 0). The four "clear at next deliver" tests are checked on a scratch deliver from the
  candidate (pending, below).
- Job 2 DONE (8319d401a, 3f5529ffe): T600 one curated writer (`pipeline/common/curated_write.py`), 17 call sites moved,
  review app's writes.py + marks.append handed over, guard `test_curated_writer.py`, AGENTS 36b.
- Gagnon DONE in data (c24844fab): RULINGS P57, OWNER-TODO answered, `waters/pond_calls.json` keeps the unnamed lake a
  lake, mouth cut on Williston, 400 m cut removed. T700 answer recorded (b8893242f).
- Junctions code (c24844fab): cause B `blk_chains.upstream_measure` (1,317 features / 371 lines), cause A prune keeps
  the moved junction's x/y and one junction per water+river. First candidate build showed 222 live pieces REMOVED
  (braid channels ending in a lake, kept before by the gap rule): the duplicate check ran before the gap check. Fixed
  (dedupe only after the gap check); atlas rebuild 2 running (heavy lane), then reach, diffs, id map, dry run, suite.

## 2026-11-13 (agent, continued) — junction candidate DONE: `full_next` + reach `nextatlas_junctions3`, NOT promoted
- Candidate = previous content (T606 item 2, T560 3/5, T690, T691) + junction fix + Gagnon P57. Commits: c24844fab
  (both causes, Gagnon, pond_calls), b059769b9 (gap-order fix) + 650e45bc5 (its pin), stale Skeena acceptances + Gagnon allow-list (2dc9e179c), Gagnon label + OPEN pin (cf229aad5).
- Build 1 removed 222 live pieces (the one-junction check ran before the gap rule) — caught by the id map, fixed, rebuilt.
- Promote dry run (rebuild 3): rc 0, no accept flag; items 21,279 -> 21,291, sections +74, boundaries +5; gone: lake 14
  (ponds on Comox creeks now folded by D1 — their widening test only passes on true measures), length 70 (length cuts
  re-placed on corrected junctions; no curated rule names one).
- Binding diff vs the previous candidate run (`nextatlas_final`): 317 rules, 9 licensing, 19 DFO, 1 unresolved. By geometry
  (`geo_attr.py`): 164 rules + 12 DFO + 9 licensing the SAME water (re-split / renumbered); the rest, every one attributed:
  91 rules lose a leaf-pruned stub (3 unnamed 905/920 headwater stubs whose TRUE length is under the 500 m tidal-code leaf
  rule — kept only by the inflated measure); 91 rules gain 5 pieces the old run flagged OUTSIDE B.C. by the inflated
  measure (now inside; they had no rules); 57 rules + 6 DFO re-cut on the Fraser, 360884603, 360884856 (same ends,
  contiguous: same water); 6 rules on the 14 folded ponds (lake rules off, stream rules on, D1); Black Creek r1 gains its
  unnamed tributary 920-599397-406278 (654 m; the walk now reaches it through the true junction); Trent r1 the same
  shape; Gagnon r1 -> unresolved `no_sections` (P57: the exemption covers no water). Nothing unexplained. Vs the release
  baseline: 385 rules = these 317 + 68 already attributed in the previous section.
- Kitsumkalum: 360220099's second "mouth" at Skeena m 119,499 was a re-homed duplicate (FWA: its mouth projects to
  125,471, 0 m off); with one junction per water the three side channels lie above the cut — out of the place as the owner
  ruled, place binds the same 4 sections; their 3 acceptances removed (stale).
- Id map (`~/bcfish-session-backup-2026-11-12/junctions/candidate-2026-11-13/idmap.json`): 277 live ids gone; 0 renamed
  same-geometry, 27 merged into one new piece, 28 split into new pieces (both proven by geometry), 38 unproven (27 on
  inflated-URM lines: pieces cut at wrong measures, plus the leaf-pruned stubs). NO owner mark keys a section id (marks
  key entries, blks, wbks; the 18 tidal marks on these lines are at measure 0), so no marks.jsonl rename is needed.
- Deliver-pending tests checked on a SIDE run of the candidate (`pipeline build --side`, reach/tiles/deliver, vintage
  agrees): test_stream_polygon_panels, test_t100_area_caveat, test_release (contract), test_t89 PASS there;
  test_part_names_unnamed_lake passes with OPEN emptied (Gagnon is one part, "The mouth (at Williston Lake) → Source").
- Fast-fail (FAST_FAIL_ALL=1): all passed. Full chunked suite, unfiltered, on the LIVE tree: fast 7,431 passed / 6 failed
  / 273 deselected; slow 274 passed / 0 failed / 1 skipped. The 6 all read the live atlas/bundle and clear at promote +
  deliver: the 5 above (pass on the side set) + test_one_acceptance_gate (the new Gagnon cut is not in the live registry;
  the candidate registry gates it clean).

## 2026-11-13 (release lane, overnight) — STOPPED at follows: candidate r-adc4e248a163 NOT promoted
- `pnpm -C app check` (heavy.sh): clean, 7,604 passed / 27 skipped. Dry run (`--dry-run --rehearse-follows`): no stop;
  fresh-copy tests passed at 892cdcc41.
- Order change (staging only): the release's upload runs `sync-inputs.sh`, which refuses inseason-links/4 while the worker
  lists /3 (`sync-inputs.sh --dry-run` said so: "Deploy the worker … first"). So the STAGING feeds worker went first:
  `deploy/staging/feeds.sh` (447 worker tests passed; gauges + DFO 200; in-season 500 "matches.json is /3, not /4", expected),
  then `deploy/staging/sync-inputs.sh` (13 objects, /4 with 38 notices resolved on r-0a9930fb8826), then POST /run/inseason
  200. /status.json: inputs list /4; gauges, dfo, inseason all `ok`, failures 0.
- Build ok (1,573 s): r-adc4e248a163. Binding diff vs r-0a9930fb8826 compared entry by entry with
  `~/bcfish-session-backup-2026-11-12/junctions/candidate-2026-11-13/bdiff_base.json`: IDENTICAL (385 rules, 1 unresolved,
  9 licensing, 23 DFO places — the evidence file says 23, not 19 — 1 tidal/outside table, 5 diagnostics, standing 3; every
  key and value equal; the .txt summaries equal). Accepted with `--accept-diff …/checks/r-adc4e248a163/binding_diff.json`.
- Follows: in-season re-resolved on the candidate, committed 89ffd83d5 (not pushed yet). Then the fixtures re-cut STOPPED:
  `taps: PART_GROWN x15p0_1010 -> 19: the store's part is (… Region 5 boundary up to the top of the river (through 1 lake), 113),
  PART_GROWN_IS names (…, 112)`. Checked read-only: Fraser part 19 is the same part on both releases (label, ends
  region_line:5 → source, 1,407.21 km, not closed), one more section = the attributed Fraser junction re-cut. The fix is the
  declaration `app/tools/regs-recut/taps.py` PART_GROWN_IS 112 → 113 (+ the PART_GROWN reason), as afb4a72cd did for D1;
  the lane's edit was REFUSED by the permission system (guard declaration), so nothing was changed and the chain stopped.
- State: LATEST = r-0a9930fb8826 (unchanged), CANDIDATE = r-adc4e248a163, staging data unchanged. NOTE: curated
  notice_waters.json now names the candidate, so a bare `sync-inputs.sh` refuses until the promote. Not done: suites,
  site deploy, app build/smoke/Play upload, store shots. Resume: make the taps.py declaration change (owner or main session),
  commit, then rerun `.venv/bin/python -m pipeline release --staging --push` (accept-diff and push are remembered).
- Resumed after e536aa195 (owner: "Yes, edit and resume"; Fraser part 19 = 113). Pushed 89ffd83d5..e536aa195 (preflight
  wants HEAD on origin). Rerun: preflight ok, build reran deliver only (curated notice_waters.json changed; same id
  r-adc4e248a163), checks ok (diff accepted), follows STOPPED AGAIN at the re-cut, on a DIFFERENT golden:
  `x17p7_1010`'s golden part ('From the fishing boundary signs 100 m upstream of adult fish counting fence up to the
  Nilkitkwa Lake outlet', 1) is not found. Read-only: Babine River part 5 is the same piece on both releases (99.05 km mark,
  1 section, open); only its LABEL changed, "… up to the Nilkitkwa Lake outlet" -> "… up to the juvenile counting weir"
  (T697 part ends, 040b79d16, "Babine counting weir": a cut aliased onto a lake edge keeps its name; it rode this
  deliver; no binding change). Needs a declaration in taps.py (a relabelled golden part) — per the coordinator's limit
  (any other changed golden: stop) NOT made. LATEST still r-0a9930fb8826; CANDIDATE r-adc4e248a163; nothing deployed
  beyond the feeds worker + /4 above.
- Owner 2026-11-13 "Accept same-piece changes". DECLARATIONS ADDED (68ba6efb3, app/tools/regs-recut/taps.py), for review:
  1. `x17p7_1010` (Babine part 5) PART_GROWN (5, …) + PART_GROWN_IS label '… up to the Nilkitkwa Lake outlet' -> '… up to
     the juvenile counting weir', 1 section. Cause: 040b79d16 `bundle.spans.end_token` (RULINGS P55 follow-up, owner
     2026-11-12 "Yes, read the real boundary"; its message names "Babine juvenile weir"): the hi end token went
     `lake_outlet:wbk:329026685` -> `babine_river__juvenile_counting_weir` (curated cut on Nilkitkwa Lake's outlet edge).
     Proof: same 99.05 km mark, 1 section, open, lo end unchanged; akey tuple equal but for renumbered ids; every
     section on the Babine entries' lines has the same rule set in both reach runs (only the 14 D1 Comox pond sections
     differ in that sweep); no Babine rule/licensing/DFO entry in the binding diff.
  2. `x19p1_1010` (Skeena Exchamsiks -> Zymagotitz) PART_GROWN index 3 -> 4, PART_GROWN_IS 59 -> 60 sections. Cause: the
     junction fix's re-placed automatic length cuts on 360887278 (44465 -> 45193, new 77497 inside this stretch): present
     in reach `nextatlas_junctions3`/`full`, absent in `nextatlas_final` (the run before the junction fix). Proof: same
     ends (Exchamsiks cut -> Zymagotitz cut), 115.77 km mark, same rule-set group (ruleset/licensing/seg equal, ids
     renumbered). Index shift: group 0's old part 2 (46 sections) is now two runs either side of the Reserve, parts 1
     (44) and 3 (2), same key; the Reserve moved 1 -> 2.
  Observed, not declared (no stop): `x15p2_1010` (PART_RECOUNTED, Fraser CPR Bridge -> Landstrom Bar) 27 -> 34 sections,
  same ends and km; its declared text (40 -> 36) is now stale.
- Rerun: follows STOPPED at "2 golden pages changed" (token c9c415db01f9, NOT accepted):
  (a) `x19p0_1010` Skeena page goes own -> CLOSED (superior_closures.r3 wins every fish). Cause found: its PART_TIDAL
      declaration taps the freshwater remainder at FIXED index 2 ("the group's lowest part"); after the run split, index 2
      is the closed Ecological Reserve and the group's lowest part is 1 (44 sections, same key 871 as before). So the
      fixture now taps the wrong part — very likely an artifact, but it is an open/closed change and the split's cause
      (why the 46-section run no longer joins across the Reserve) is not traced in code; per the owner's rule, stopped.
      Proposed fix for the owner/main session: PART_TIDAL `x19p0_1010` index 2 -> 1 (reason: T678 junctions, run split), then
      rerun; expected: the page back to `own` with the same rows (unverified).
  (b) `edge_unnamed.json`: one handle renumber 917240 -> 917242 (renumber only).
  The re-cut files are left uncommitted in the working tree (page-oracle.json, taps.json, edge_unnamed.json) for reading.
- Main session: retarget x19p0 only once the split is traced. TRACED (746c205ed):
  Reserve: superior_closures.r3 binds the same 3,792 sections in both runs (ids equal; not in the binding diff).
  Split: old atlas graph (full.prev) had an edge from EVERY piece of side channel 360216962 (0, 96, 1752, 3840) to
  mainstem section 360887278:44465; the new graph keeps only the mouth (360216962:0 -> 360887278:45193) — c24844fab
  cause A, one junction per water and river. The spurious 360216962:3840 -> 360887278:44465 edge (3840 lies ABOVE the
  Reserve) made the pieces either side of the Reserve one run. Order on 360887278 (m), before -> after:
    before: [0 tidal]…6405 | part 2 (46, key 871): 6405-63480 + (via the false edge) 66554-67917 & 360216962:3840-4386 |
            part 1 Reserve 63480-66554 (closed) | part 3 67917-115773 (59) …
    after:  part 1 (44, key 871) 6405-63480 + side lines | part 2 Reserve 63480-66554 (closed, same sections) |
            part 3 (2, key 871) 66554-67917 + 360216962:3840-4386 | part 4 67917-115773 (60) …
  Same rows: parts 1 and 3 carry key 871, the old part 2's key. DECLARATIONS (owner review list, all in taps.py):
  3. `x19p0_1010` PART_TIDAL freshwater part 2 -> 1 (cause/proof above). Re-cut: page-oracle.json UNCHANGED — the Skeena
     page is open with exactly the committed rows.
  4. `x15p2_1010` PART_RECOUNTED reason updated: 27 -> 34 sections, same ends (CPR Bridge -> Landstrom Bar). Cause: T691
     (owner 2026-11-12 "silent doesn't mean retired"): gauge cuts at 94,833 / 107,496 / 111,136 / 122,095 / 132,550 /
     141,142 / 148,096 m replace length cuts 93,821 / 116,370 / 135,998, and 3 braid lines cut beside them
     (355992048:3190, 355992485:996, 355997500:1169), present since reach `nextatlas_final`. Proof: the group's 99-rule set
     is identical; old group 33 sections -> 40 (parts 1 + 3).
  Remaining fixture diff = declared counts/labels, Skeena part indices +1, id renumbers, edge_unnamed handle
  917240 -> 917242: accepted with `--accept-fixtures 1bc035bd444b`.
- Follows then STOPPED on 7 test failures on the candidate (tests not touched; LATEST not moved, nothing pushed by it):
  stretch-names (Kanaka "Cliff Park Falls → A lake outlet" now "→ Source": T697/P55, which names exactly this),
  t670.release + seo t670.web.release x2 (Mat Creek gnis:14254 part "mouth to lake_outlet" no longer exists: T697 part ends
  renamed its top), waterMap.test (Skeena "tidal_boundary to exchamsiks" part: the traced split), WaterMapView x2 (the
  Skeena part "Tidal boundary → Exchamsiks River confluence" is now "Skeena River Ecological Reserve boundary near the
  Exchamsiks River confluence → Exchamsiks River confluence"; the back/chooser walk follows). All look like pins of the
  old part names; they need test edits, which this lane may not make.
  After this stop the release put the re-cut fixtures back itself: the tree is clean (only the other lane's
  dfo-suggestions.json). The ledger still records the re-cut on the candidate, so a rerun's preflight may pair the
  goldens with r-adc4e248a163 and stop ('goldens cut from LATEST', seen once); then re-cut in place first
  (`node app/tools/regs-recut/sync.mjs --release data/release/r-adc4e248a163`) and rerun.
- Main session lifted "no test edits" for the 7 pins; the permission system REFUSED the test edit (second write to
  waterMap.test.ts). Not worked around: the one applied edit was saved as
  `~/bcfish-session-backup-2026-11-12/junctions/candidate-2026-11-13/waterMap.test.patch` and reverted (tree clean; the
  release had already committed its re-cut fixtures, 974840dca). Proposed pins (unrun), each a pure rename of the same water:
  1. stretch-names.test.ts:45 Kanaka "Cliff Park Falls → A lake outlet" -> "Cliff Park Falls → Source" (T697/P55; part 3
     same 24.77 km, 3 sections, open, ruleset 526 / akey 184 both releases; hi token lake_outlet -> source).
  2-4. seo/t670.web.release.test.ts:57 and conformance/src/regs/t670.release.test.ts:73 Mat Creek gnis:14254 ends
     ["mouth","lake_outlet"] -> ["mouth","source"] (T697/P55; its one part: 10.47 km, 5 sections, open, ruleset 654 /
     akey 261 both; the seo "guard" case fails on the same lookup).
  5. waterMap.test.ts (the patch): SK_TIDAL_UP -> SK_RESERVE / SK_RESERVE_UP by ends (Reserve boundaries; Reserve-near-
     Exchamsiks -> Exchamsiks cut, key 871 as the old main-stem option); indices found by waterMapStartIx, the "not a way
     up" choice is the stretch below the Reserve; the regs case reads index SK_EXCHAMSIKS_UP - 1 (keeps "map index is not
     part_ix"); one comment to update (the refused edit).
  6. WaterMapView.test.tsx:248 chooser main-stem option -> "Skeena River Ecological Reserve boundary near the Exchamsiks
     River confluence → Exchamsiks River confluence" (still a 2-way fork, main stem first).
  7. WaterMapView.test.tsx:276 `part={1}` -> `part={await partOf("skeenaReserve")}` (by ends; part 1 is now the stretch
     below the Reserve, whose ▲ is no fork). Expected list unchanged.
- Main session made the 7 pins (de89e8d7d; owner "Yes, edit and resume"), plus one more for the owner's review list:
  8. waterMap.test.ts Coquitlam "Coquitlam Watershed boundary → A lake outlet" -> "→ Source" (T697/P55 rename, same sections).
- RELEASED r-adc4e248a163 to STAGING (rerun of `release --staging --push`): preflight ok, follows ok (whole app check +
  7 pipeline files on the candidate), promote LATEST = r-adc4e248a163, staging upload + smoke 18 pass / 1 warn (the staging
  SITE still serves r-0a9930fb8826: site deploy pending) / 0 fail. In-season after the promote: notice_waters.json names
  r-adc4e248a163, staging matches.json is inseason-links/4 with 38 notices, worker /status inputs /4, gauges/dfo/inseason ok,
  failures 0.
- Step 3, full pipeline suite, chunked, unfiltered: fast 7,434 passed / 3 FAILED / 273 deselected; slow 273 passed /
  1 FAILED / 1 skipped. STOPPED before any deploy (site, app). Tests NOT edited. The four, each a pin the release moved:
  a. test_export_codec::test_only_named_lake_edges_ship: two lake_edge splits ship that no extent / in-season end /
     same_place_as names: babine_river__juvenile_counting_weir, mitchell_river__100_m_above_mitchell_lake_weir. Cause:
     040b79d16 (T697/P55 follow-up) — a cut aliased onto a lake edge keeps its name, so these two now name part ends and
     the export ships them. The docstring already says "an extent, a run, …" but the assertion has no part-end term:
     a test change (add the part-end tokens to the allowed set) or an export change — a judgement, not a pure re-pin.
  b. test_fix_round_rows::…binds_nothing_above_it[gnis:15333 Harrison]: `== 3` rules on every section below the lake;
     the entry has 4 now (harrison_river.r4, T209 2841fb605, owner "Keep them as two entries"); r1-r4 all bind the same
     8 sections. Pin 3 -> 4 for Harrison (or "every rule of the entry"); Paul Lake stays 3.
  c. test_review_queue::test_the_prechecks_measured_over_the_corpus: unresolved 29 vs pin 25 (tolerance 3). The one new
     unresolved since reach `nextatlas_final` (28) is gagnon_creek.r1 `no_sections` (P57, in the accepted diff). Re-pin 25 -> 29.
  d. (slow) test_unnamed_trib_sweep::…committed_result…: the committed sweep result is stale — unnamed_streams 1,179,620 ->
     1,179,617 and the bundle digests; the other 10 fields equal (unexplained/by_rule unchanged). Fix is the documented
     regenerate: `scripts/heavy.sh sweep 40 -- .venv/bin/python -m pipeline.tools.audit.unnamed_trib_sweep --write-result`,
     then commit with the -3 explained (likely the junction fix's 3 leaf-pruned unnamed stubs; not verified one by one).
- Owner 2026-11-13 dialog on the 4 suite failures. Done (6c1d…, see git log "three stale pins"): Harrison 3 -> 4 (Paul
  stays 3), review_queue unresolved 25 -> 29, export_codec lake edges also allowed as a run's end (mutation-checked outside
  the suite: an unnamed lake edge fails; the two weirs with their run ends removed fail). Those three pass unfiltered.
- STOPPED on (c), the sweep: regenerated with the documented command (rc 0; only the bundle digests and unnamed_streams
  1,179,620 -> 1,179,617 change), but the -3 could NOT be confirmed as the junction fix's 3 pruned stubs, so NOT committed
  (copy: ~/bcfish-session-backup-2026-11-12/junctions/candidate-2026-11-13/t57_trib_sweep.regenerated.json). The sweep's
  count is the bundle's `unnamed_part` stream sections; the r-0a9930fb8826 bundle is gone (overwritten by the deliver), so
  the -3 cannot be listed id by id. A registry-based count (sections in no non-area item; `unnamed_diff.py` beside it)
  over the two atlases gives -27 net over 41 lines, not 3: 16 lines lose their only unnamed section (4 Comox 354… on
  inflated-URM lines: 354126477, 354137285, 354140926, 354142131; 12 on 356… lines), 18 Comox lines lose one piece each
  (pieces cut at the inflated measure merge back), 7 356… lines gain one. So the net is a mix (junction fix merges and
  stubs, plus the 356… moves, likely T690 Whiteswan naming / D1 folds — not traced), not exactly three stubs.
