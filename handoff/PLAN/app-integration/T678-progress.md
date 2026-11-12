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
