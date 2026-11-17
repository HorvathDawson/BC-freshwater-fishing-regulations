# Finishing the local rebuild (2026-11-15, T753 / T743)

Follows `promote-2026-11-15.md` (deliver stopped on Kotcho Lake) and `side-rebuild-2026-11-15.md` (the Seton cut was
skipped with no message). Both defects are fixed in code with tests, and one more atlas was built and promoted locally.
**The deliver step now passes the answers check that refused Kotcho. It then stops one step later, on a different and
older data finding: Stikine tributary pieces, §6.** The bundle and LATEST did not move. Nothing was released, uploaded
or deployed.

Tags: **[ran]** a command or a script over built files; **[read]** code or data read; **[inferred]** my reading.
Logs: `data/generated/side/dfo-cuts23b/logs/`.

## 1. Kotcho Lake (A) [ran]

**Rule (`pipeline/atlas/registry/lake_loops.py`, criterion 5, `LOOPS_NEVER_DIVIDE_A_LAKE`).** A lake loop joins its
lake only when every piece of it is in exactly the lake's **cut areas**. Cut areas are the areas whose `areas.json` def
cuts lines at its edge (`cut: clean` or `first_last`: ecological reserves, national parks, the Chilkoot, regions, MU
groups, sign zones, restricted land), plus any curated area-boundary label. The lake's `outside_items` are taken out of
both sides. If the pieces and the lake differ, the loop stays with its river and is listed as not moved
(`AREAS_DIFFER`, with `lake_areas` and `loop_areas`).

- Why the registry: rules bind later, but membership is fixed in the atlas (`node.in_areas`, stamped by
  `mark_membership` before the registry is written). A lake is never cut, so it holds every area it touches. A line is
  in an area only where the cutter puts it inside. A loop with different cut areas would give the lake a second answer.
  That is "a lake divided into areas", and this repo defers those (no lake splits).
- Membership-only areas (`cut: false`: provincial parks, WMAs, watersheds, indigenous land, permit land) do not hold a
  loop, because the build never cuts a line at their edge. A difference in one is listed on the moved row
  (`membership_only_areas_differ`). Today that is Sproat Lake's two shore parks, which no rule names. If a rule ever
  names such an area and a loop's lake is in it, the answers step would refuse again, loudly. That would be the time to
  widen the test.
- The registry built before the shore cuts has no area flags yet. It decides by topology alone and reports
  `areas_checked: false`. Only the tidal answer reads that registry.
- Tests: `test_lake_loops.py` +9 (the Kotcho shape, one piece outside, joins when equal, a region line through the lake,
  the membership-only case, outside_items, no flags, the mutation pin, prefixes).

**Result in the promoted atlas:** 7 lines move and Kotcho does not. The 7 are Sproat, Moat (Cruickshank), Tsintsunko,
Isintok, Garnet (Eneas), Tatlatui (Firesteel) and Nanika, with the same pieces as before. That is 10 pieces and 702.8 m.
- Kotcho reads: lake `[ecological_reserves:kotcho_lake_ecological_reserve, region:7b]`, loop `359013342:6118`
  `[region:7b]`.
- Near misses: the old list plus Kotcho, nothing else.
- In this build's bundle, each of the 7 lakes is one part with one rule set. Kotcho Lake is its polygon only.

## 2. The Seton cut (B) [ran]

**(1) The picker (`splits/anchors.py`, `A_TRIBUTARY_IS_ITS_LINE_THAT_MEETS_THE_PARENT`).** A confluence anchor given
by WSC now takes, in order:
1. the line whose mouth meets the parent (within the split's own `proximity_m`);
2. then one that carries an FWA gnis;
3. then the largest by magnitude and length.

`confluence_mouths`, which feeds the braid sweep, uses the same pick.

Check: all **106** curated confluence cuts were resolved with the old picker and the new one, on the build's own chains
(scratch `rf/conf_diff.py`). Old: 105 points; new: 106. **Only `fraser_river__seton_river` changed**: from none to
`356364114` m 329127.2, tributary blk 355995374 → 356355483. Its braid-sweep mouth moved with it. No other cut moved.

**(2) No silent skip.** `resolve_split_defs(unplaced=…)` records every split def that places no point, with a reason.
- The atlas build prints them and writes `split_unplaced.json` (`[]` when none). The build treats them as it treats
  `split_conflicts`: a loud report, and the build goes on.
- `pipeline.atlas.promote` lists them in the dry run (the parity step) and **refuses** a candidate with an authored
  unplaced cut (`--accept-unplaced-cuts` after reading it). It does the same when the record is missing, unless the
  candidate comes with `--no-marker`.
- Gauge cuts are listed, never refused.

New finding the report made visible: **3 gauge cuts have never been placed** in the last three atlases, all with "point
more than 400 m from every target line": 08HD024, 08ME002, 08MH032. 08MH032 is the T67 pruned-loop station. These are
standing gaps, not new ones.

Tests: `test_anchors.py` +6 (the Seton shape, named beats nameless, the mutation pin listing the cut, reasons, the
record, the promote refusal). Promote fixtures in 3 tests now write `split_unplaced.json`.

`POLICY_VERSION` 40 → 41, with a changelog line and both flags pinned in `test_reach_builder.py`. AGENTS rule 12 and
`pipeline/docs/MAP.md` each have a line.

Code commit: **3bbb20082** (made before the main chat's hold).

## 3. Side atlas + parity [ran]
- `heavy.sh atlas-side-b` (lane 1): rc 0 in 32 min, into `data/generated/side/dfo-cuts23b/atlas/full_next`.
- `atlas.inputs`: current with its 14 inputs.
- Build log: "0 curated cut(s) and 3 gauge cut(s) could NOT be placed".
- Parity against the promoted atlas:
  - items 21,294 → 21,294, 6 changed;
  - boundaries 11,379 → 11,380 (+1: the Seton cut);
  - one new section id, `356364114:329127`, and none gone;
  - changed items: Fraser 249 → 250 sections and 93 → 94 cuts; Kotcho River 30 → 31; Kotcho Lake 2 → 1; and the three
    area items that hold the new Fraser piece (basin 100-, PFMA 29, Region 3).
  - **Exactly the two expected differences.**
- Side reach run (rc 0, 11 min), binding diff against the live run: 109 rules, 2 licensing records, 2 DFO places
  (`3:default:region`, `3:fraser-river:mainstem-waters-of-the-fraser-rive`). Unresolved, tidal and diagnostics: 0.
  **Every one of the 113 changes is on `356364114:329127` (the Seton piece) or `359013342:6118` (Kotcho's loop).**
  - Kotcho's loop leaves the lake rules: ice huts r1/r2, bait r5, the lake hook rule.
  - It takes 9 stream and zone rules as the river's.

## 4. Promote, reach [ran]
- `atlas/full.prev` (b9de120e) is what the live bundle and LATEST were cut from. Promote would have refused to delete
  it, so I first renamed it to **`atlas/full.b9de120e`**.
- Promote: rc 0. `full` (e0308…) → `full.prev`, side → `full` (handles `ff8d805a057908a9`). `atlas.inputs`: current.
- The live reach run d9d10087 was copied whole to **`reaches/full.pre_rf`**. LATEST's run is still kept at
  `release/baseline/r-f749475d0962` and at `reaches/full.pre_t743`.
- `pipeline build --hold-latest` reran reach. The new live digest is `161e24a51fcc29ed`, identical to the side run, so
  the live binding diff (`binding_diff_live.json`) is the same 113 attributed changes.
- I used `--hold-latest` because a plain `pipeline build` moves LATEST when deliver passes.

## 5. Mark 2570 (the Fraser at the Seton): NOT landed [ran dry run]
`apply_proposals --only mark-dfo-3_fraser_river_in_that_portion_of_the_fraser_rive --dry-run` on the promoted atlas:
"0 → 3 sections, 0.0 → 4.37 km, no extent → bound". This tool always commits, and the main chat asked for **no commits
in the main checkout** (HEAD frozen for the Play build). So it waits. When HEAD is free:
```
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.review.apply_proposals --only mark-dfo-3_fraser_river_in_that_portion_of_the_fraser_rive
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.review.apply --check      # then only the 2 T359 tidal waits
```
`apply --check` now: the 2 T359 waits plus this one "would land" (rc 1).

## 6. pipeline build: deliver STOPPED, on the Stikine (not Kotcho) [ran]
- Reach: ok. Tiles: ok in 16 min, file by file into `data/generated/tiles`. Deliver: rc 1 after 14.5 min.
- What passed: the bundle, verdicts, status index, export, **answers (0 problems: Kotcho is clean)**, the store, and
  the paint sync.
- What failed: the release step's T517 part-name check (`answers/display/name_check.run_problems`), on 6 waters:
  Buckley Creek, Iskut River, Chukachida River, Horsefeed Creek, Sutlahine River and Mess Creek.
  - Each has one extra part of 1 to 3 sections with the same rules but salmon chain 136 (`6:e:section` → region) where
    the rest of the water has chain 140 (`6:stikine-river:including-tributaries-see-the-tahl`). These sections are
    `360229004:0`, `360223806:0`, `360223824:0`, `360214528:2794` and others.
  - The check finds "run gap: pieces … not all joined by marks" and "run ends: … hi=None — its joined name cannot be
    said".
- **Cause: not A or B.** The Stikine place was `no_extent` in LATEST's run (`full.pre_t743`) and is bound now, with
  69,100 sections, through its T185 landing after 11-13. The 2026-11-15 side report lists Stikine among the 83 landings.
  The place leaves out a few detached pieces of its tributaries. The previous build never reached this step because it
  stopped at answers. [ran: the place is absent / no_extent / bound in the three runs; the pieces are not in it]
- I did not fix it; it is outside this brief. The bundle in `data/generated/bundle` is unchanged (on b9de120e).
  **LATEST is still r-f749475d0962.** The staging folder was removed by deliver. This build's bundle is in
  `deliver-cache/bundle/out/`.

**Vintage:** the new tiles and this build's bundle share handles `ff8d805a057908a9` and registry `b229a478ba7c2eaa`. The
live pairing is mixed again: tiles and reach are on the new atlas, the bundle on b9de120e. Do not ship these tiles with
the old bundle.

**Gauges** (this build's bundle, `section_gauge`):
- Kispiox 08EB004: the piece below its new home (0–13691) reads 08EB004. Harmless.
- Hope 08MF005: its home is `356364114:158750–182273`. The piece below, 156517–158750, reads 08MF076 (Fraser above
  Hunter Creek) as its gauge downstream, as it always has.
  - The station's own point is at 158685, so the 66 m between the point and the bridge cut are water that flows past
    Hope but read the gauge 9 km below.
  - That is wrong for those 66 m by the build's own convention (a section reads the next gauge downstream). For an
    angler it does not matter: the same flow, no tributary in between, and 216,600 vs 217,000 km².
  - Proper fix: in placement, not in `matches.json`. A station should not join an authored cut that lies UPSTREAM of its
    point (sectionizer pick-up, `MERGE_M["gauge"]`). It should join only one downstream, or keep its own cut when that
    leaves no sliver. That is a code item. No match must change.

**T753 remaining pieces:** in this build's bundle, each of the 7 lakes is one part with one rule set across its polygon
and its loop pieces. They carry the lake's rules.

## 7. Gates [ran]
- `scripts/fast-fail.sh` (FAST_FAIL_ALL), before the build: 33 of 34. The 1 failure is "review marks have landed": mark
  2570, §5.
- `node app/tools/check-tickets.mjs`: ok.
- Pipeline CI tier (the brief's marker expression, unfiltered, through heavy.sh): **6,457 passed, 1 failed**, 1,484
  deselected (5 min 15 s). The one failure is the known local-only `test_dfo_suggest.py::test_the_hunt_file_covers_its_targets_and_names_real_cuts`.
- Data-tier tests: **not run**. There is no new bundle; deliver stopped. They would test the old bundle against the new
  atlas.
- Marks audit `--since` the 2026-11-15 live audit JSON: 264 findings, **0 new, 0 resolved**.
  - **The "24 lower-Fraser side channels dropped" (section F) are newly visible, not new.** The 4 lower-Fraser places
    (CPR bridge at Mission → Hope / Alexandra) were `no_extent` until the 27 landings. The audit checks a place's side
    channels only once it has a cut window. When they landed bound to the Fraser item alone (as the owner saved them,
    two of them worded "mainstem waters"), the side channels showed up as dropped.
  - Those channels: Seabird Island North, Herrling Island, Jeperson ×2, Strawberry and Maria sloughs, Nicomen Slough
    (which has its own DFO water).
  - No data moved to cause them.

## What is live locally now
- Atlas: `atlas/full` = this build (ff8d805a). `full.prev` = e0308 (the T743 build). `full.b9de120e` = what LATEST and
  the live bundle were cut from.
- Reach: `reaches/full` = 161e24a5. Backups: `full.pre_rf` (d9d10087), `full.pre_t743` (4f1bdab9, LATEST's run), and
  `release/baseline/r-f749475d0962`.
- Tiles: the new atlas (ff8d805a), file by file. `tiles.pre_t743/` holds LATEST's.
- Bundle: unchanged (b9de120e). LATEST: r-f749475d0962.

## Rollback (exact)
```
# atlas back to the T743 build:
mv data/generated/atlas/full data/generated/atlas/full.rf && mv data/generated/atlas/full.prev data/generated/atlas/full
# ...or all the way back to LATEST's atlas: then also
#   mv data/generated/atlas/full data/generated/atlas/full.t743 && mv data/generated/atlas/full.b9de120e data/generated/atlas/full
# reach back to d9d10087 (or full.pre_t743 for LATEST's):
rm -rf data/generated/reaches/full && cp -a data/generated/reaches/full.pre_rf data/generated/reaches/full
# tiles: file by file from tiles.pre_t743 (LATEST's), never the directory (promote-2026-11-15.md has the lines)
# code: git revert 3bbb20082
```

## Not done
- No release, staging, upload or deploy. Nothing under deploy/prod. LATEST not moved. Nothing pushed.
- Mark 2570 not landed (no-commit hold).
- Deliver did not pass (the Stikine).
- Data-tier tests not run.
- This report and the T753 ticket note are uncommitted (no-commit hold).
- Disk: deleted `side/dfo-cuts23/{reach,bundle-check,curated}`; its logs are kept. 44 Gi free.

## For the owner (plain words)
1. **Kotcho Lake: decided by rule, nothing to answer.** The little loop of channel stays with Kotcho River, because it
   lies outside the ecological reserve the lake is in. Making it part of the lake would have split the lake into "closed"
   and "open" bits. The other 7 loops (Sproat and the rest) still join their lakes.
2. **Fraser at the Seton River: built.** Your Lillooet stretch can be saved as soon as commits are allowed again.
3. **New blocker: six Stikine tributaries.** Buckley, Iskut, Chukachida, Horsefeed, Sutlahine and Mess each have a few
   small pieces that your "Stikine River including tributaries" salmon place does not reach, so the app cannot name
   those pieces. Options: (a) include them in the Stikine place; (b) accept them as separate bits under the region
   default; (c) leave the build held. Suggested: (a), after a look at the map.
4. **Hope gauge (unchanged question).** 66 m just above the Hope gauge now read the gauge 9 km below. It is the same
   water and flow. OK to leave until the placement fix? Default: yes.
5. **Three gauges have never cut their river:** 08HD024, 08ME002 and 08MH032 each sit more than 400 m from their river
   line. Worth a match check one day. Default: leave.
