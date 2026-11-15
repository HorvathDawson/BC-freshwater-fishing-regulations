# Side rebuild with the cleaned cut list (2026-11-15, T743)

Owner, 2026-11-15: "also we could rebuild the cuts and make sure nothing causes issues? … we just really want to clean it all
up and then have questions ready for me later." Input: `cut-cleanup-2026-11-15.md` §5 (REBUILD INPUT), run up to its
"# STOP" line. Nothing was promoted, nothing `git apply`d, no release, no upload, LATEST untouched (r-f749475d0962),
tiles dir untouched, `data/curated` untouched (`git status` clean there), marks.jsonl and dfo-suggestions.json not written.

Tags: **[ran]** a command or a script over the built files; **[read]** code or data read; **[inferred]** my reading.

## Verdict

**Safe to promote the atlas, with one hold: do not land mark 2570 (the Fraser at the Seton) until its cut is fixed.**

- Every difference from the live atlas and the live reach run is attributed (§2, §4): 0 unattributed sections, 0
  unattributed rule bindings, 0 unattributed DFO places.
- **One of the 23 new cuts was NOT built: `fraser_river__seton_river`.** It is in the side `cuts.json` and in
  `split_anchors.json`, but `splits.resolved.json` has no row for it, and the build printed nothing [ran]. The cause is code
  [read + ran], `pipeline/atlas/splits/anchors.py`:
  - `_tributary_chain` picks the line with the largest `stream_magnitude` among the lines that share the anchor's
    `tributary_wsc` (`100-233886`). That is an unnamed line, blk 355995374 (magnitude 3314), whose first point is 654 m
    from the Fraser line.
  - The Seton River's own line is blk 356355483 (magnitude 3192). It ends exactly on the Fraser (0.0 m).
  - 654 m is more than `proximity_m` (500 m), so the confluence branch adds no candidate and the cut drops with no
    message.

  I did not patch it (brief: report, don't patch). Promoting is still safe, because nothing in the curated tree names this
  cut yet. But mark 2570's place (`3:fraser-river:in-that-portion-of-the-fraser-rive`) runs "between the NEW split
  `fraser_river__seton_river` and `fraser_river__signs_4_km_below_lillooet`". `apply --check` lists it as "would land"
  anyway: the check does not look for the cut in the build [ran]. Landing it now would give a place whose lower end does
  not exist in the atlas.
- Everything else matches the plan: 22 new cuts, the Puntledge weir, the moved 75 m cut, Pallant's tidal boundary at the
  bridge, T753 exactly as its dry run, the sliver / straddle / unplaced gates clean, and gauges frozen.

## Where it is

`data/generated/side/dfo-cuts23/` (4.3 GB):
- `curated/`: the patched curated root; the main chat made it with `proposals side`, rc 0, `logs/side.log`.
- `atlas/full_next/`: the side atlas, COMPLETE.json present.
- `reach/`: the side reach run, digest d9d100875aa9a306.
- `binding_diff.json`.
- `logs/`: atlas, parity, parity-full, inputs, reach, binding_diff, attrib.json (the full attribution), section-diff,
  apply-check, audit-side(.md/.json), audit-side-since(.md/.json).

Run with `CURATED_ROOT=$SIDE/curated`.

## Disk

- At the start: 20 Gi free (98%); just before deleting, APFS showed 38 Gi.
- After deleting: 65 Gi.
- After the build: 58 Gi.

Deleted (~27.6 GB), by explicit path, after checking that nothing references them (no symlink, no code path, no live
release, every heavy lane free):

| path | MB |
|---|---|
| `data/generated/side/next-rebuild` | 4,342 |
| `data/generated/side/t679` | 4,772 |
| `data/generated/side/rc3` | 743 |
| `data/generated/side/next-rc` | 5,338 |
| `data/generated/atlas/full.pre_t678` | 3,585 |
| `data/generated/atlas/full_t678` | 3,586 |
| `data/generated/reaches/t685_base`, `t685_next`, `nextatlas_cand`, `nextatlas_final`, `nextatlas_junctions3` | 742 each |
| `data/generated/reaches/baseline-r-a178299f6005` | 739 |
| `data/release/r-0a9930fb8826`, `r-25a89ce9f8e6`, `r-303b4007a70a`, `r-483b295de57c`, `r-6dce77a24e6a`, `r-923208749d0a`, `r-a178299f6005`, `r-adc4e248a163`, `r-d14bdb180f55` | ~32 each |

Kept:
- `atlas/full` and `atlas/full.prev`;
- `reaches/full` (digest 4f1bdab9 = the live release's `reach_digest`);
- `reaches/full_prev` and `full_next` (small, named in a release.py hint);
- `side/t691` (the live release's build; its tiles and deliver);
- releases r-f749475d0962 (live) and r-a9914d6b25b7 (previous);
- `basemap`, `tiles`.

## Checks

### 1. The build [ran]
- `check_curated --waters`: 0 errors, 0 warnings.
- `pipeline.atlas.build --full` through `heavy.sh atlas-side` (lane 1): rc 0 in 32 min, 1,945 s.
- `pipeline.atlas.inputs`: "current with its 14 inputs".
- Skipped or refused, verbatim from the log. These are the build's standing counts, not new:
  - `added-stream connectors: 281 edges (8 skipped)`
  - `braid sweep: 63 braid cut(s), 4 junction re-home(s); cases {'cut': 35, 'junction at the cut (already on its side)': 1, 'refused: another water': 9, 'refused: line misses': 1, 'refused: one side': 21, 'rehomed: junction <10 m': 2}`
  - `coast cuts: … sweep cases {'refused: another water': 2, 'refused: one side': 4}`
  - `length cap: 667 braid-spanned junction(s) skipped`
  - `graph.gpkg: skipped — pass --write-gpkg` (the default)
  - `resolved 1033 curated split point(s) (75 picked up existing boundaries)`
  - `sliver gate: no cut left a piece under 5 m (461 shorter pieces are FWA's own)`
- `summary.txt` says `INTEGRITY: … fids w/o a node=728357 -> FAIL`. The live atlas has the same line (the pruned
  headwater leaves); it is standing, not new.
- `split_conflicts.json` is `[]`.
- **Not printed anywhere: the Seton cut dropped** (Verdict).
- Before the build, `scripts/fast-fail.sh` failed on one test anchor, `pipeline/tests/test_review_notes_1115.py:33`
  (an invented digest). That is the T755 lane's file and does not touch the atlas inputs.

### 2. Parity against the live atlas [ran]
`promote --dry-run` and `build_parity`:
- items: 21,294 → 21,294; 0 added, 0 removed, 57 changed;
- sections: 4,612,577 → 4,612,647 (+70);
- boundaries: 11,366 → 11,379 (+13).

**Boundary ids gained (29 resolved rows):**
- 22 of the 23 new cuts; `fraser_river__seton_river` is missing;
- `puntledge_river__puntledge_lower_hatchery_weir` (m 4001.0);
- 7 length-cap re-spacings on cut lines (below).

**Boundary ids lost (17), every one attributed:**
- **lake 3** (`sproat_river__lake_329016376`, `cruickshank_river__lake_329097416`, `firesteel_river__lake_329139181`):
  T753's pond-edge boundaries, as its ticket lists.
- **length 11, gained 7**: the 25 km length cap re-spaces on 8 lines that took a new cut (356361760, 356363937,
  360875115, 360876170, 360879315, 360883243, 360885942, 360886024).
- **curated 3**:
  - `gauge__08EB004` and `gauge__08MF005` joined the new cuts 42.6 m and 65.8 m away (a minted gauge point that meets an
    authored place takes its cut: Kispiox resort signs, Hwy 1 bridge at Hope). Their resolved rows move 13648.5 → 13691.1
    and 158685.1 → 158751.0.
  - `pallant_creek__mouth`: the 10-28 automatic tidal mouth row at m 0, replaced by the owner's row at the bridge.
  - No curated file, mark, DFO suggestion or review-app file names any of these ids [ran].
- **Moved:** `puntledge_river__signs_75_m_…` 4000.28 → 3926.04 (bf1a29567).

**Sections: every added or removed section id is on an expected blk** [ran]:
- the 19 blks in the report's §5 check 1 (Pallant's 360874940 has the same ids, now tidal);
- the 8 T753 loop blks.

0 unexpected.

**The 57 changed items:**
- 26 river items with a new or moved cut;
- 8 T753 rivers and 8 T753 lakes;
- Pallant (a boundary row only);
- 22 area items whose membership changed by those same pieces (Cowichan River Park, Skutz 8, Exchamsiks protected area,
  Tweedsmuir, Nisga'a, Kitsumkaylum 1, Emory Creek Park, Yelakin 4, regions 1/2/3/5/6, MU groups, basins, PFMA 3/4/29).

### 3. End cuts and slivers [ran]
- The sliver gate passes (no `sliver_gate.json`). No new cut is at measure 0 or on a line's end.
- Shortest new pieces:
  - Kitsumkalum CN bridge: 22.5 m down to the line's mouth (owner Q7 in the clean-up report);
  - Pinkut: 24.5 m from the signs up to the fence (the authored 25 m);
  - Exchamsiks bridge: 186 m above the next boundary;
  - Puntledge: 75 m (the 75 m cut → weir) and 450 m;
  - Shegunia: about 100 m between its pair.
- No half-broken water: no item lost all its sections, every river kept sections.
- Measure-0 cuts already in the live atlas (`burton_creek__hwy_6_bridge`, `pennask_creek__pennask_lake`) are unchanged.

### 4. Bindings, the trib walk included [ran]
`binding_diff` live → side: 293 rules, 0 unresolved changes, 10 licensing records, 111 DFO places, 1 tidal table,
1 diagnostic. A full per-rule streaming diff (script `attrib.py`, result `logs/attrib.json`):

- **Rules: 293 of 293 change only on the cut blks or the T753 loop blks.** No trib walk moved. The largest are the zone
  rules (+31 −26 province-wide, +20 −11 Region 6): the cut pieces, counted once per rule.
  - T753 as its ticket said: `sproat_river.r2`/`r2x` −2, `cruickshank_river.r1` −2, Williston r4 −2;
    `sproat_lake.r1-r3` +2, `tatlatui_lake.r1` +2; set lining / `bait.r5` +3 / +11 for lake pieces.
  - **Puntledge r4: 451.6 → 525.9 m** (planned ~452 → ~527).
  - The corpus is unchanged (`entries_digest` the same).
- **DFO places: 111 moved.**
  - **83** moved because their curated record changed after the live run (8a1ce30f1, 2026-11-13). These are the T185
    write-through landings since then, both lanes: Cowichan, Stamp, Bulkley ×2, Bella Coola, Morice 2739, Redsand,
    Kitsumkalum Lake, Stikine, Taku and the rest. They are not effects of this build.
  - Of the **28** with an unchanged record, **26** move only on expected blks:
    - Sproat `whole` 4 → 2 sections (the T753 check);
    - Morice Lake −1 (the Nanika piece);
    - Pallant `whole` −1 (360874940:0, the 56.6 m piece now tidal; a `whole` place cannot keep a tidal piece, T632);
    - the Puntledge pair;
    - Kispiox, Kitsumkalum, Zymagotitz, Zymoetz, Ecstall, Exchamsiks, Exstew, Atnarko and Lakelse `whole`;
    - region defaults.
  - **2** move elsewhere. Both are complements whose siblings landed after the live run:
    - `1:little-qualicum-river:all-waters-unless-indicated-below` unresolved → bound, 5 sections (its sibling
      9aa2f0615 landed);
    - `1:stamp-river:except-in-those-areas-and-times-li` bound (2 sections) → **unresolved, `no_sections`**: the 8 Stamp
      siblings that landed now cover what it held. Owner question 3.
- Tidal: `360874940:0` is now owned by `split:pallant_creek__bridge_near_mouth` (Pallant, planned).
- Diagnostic: the Shuswap "lower upstream from white triangle" `ambiguous_cut` on `shuswap_river__mabel_lake` now reads
  93618.86 (from none). Mabel Lake's outlet is not on a cut blk. The place's record changed since the live run, so this
  comes from that landing [inferred].
- Reach report vs live: unplaced 2, the same two (Noons, Seymour `flows_to_the_sea_unjoined`); unresolved 18, same;
  Tlell r5 tidal, same; scope-unresolved Chipmunk / Toms, same.

### T753 against its ticket [ran]
`lake_loop_report.json` **moves are exactly the dry run**:
- 8 lines, 11 pieces, 754.6 m, all 8 joined a named lake;
- the 8 blks and the section ids are identical.

The near-miss list has 448 rows (the dry run has 462). The 14 dry-run rows no longer listed are all named creeks:
13 "leaves the lakes" and 1 "another stream joins". None is a move. My guess is that the dry run ran on an earlier state
of the code or of the names [inferred, not traced].

Bundle checks (lake items holding line sections: spans, tiles, area_ha) were **not run**; the bundle is not in this route.

### 5. Gauges [ran]
- `data/curated/gauges/matches.json` is unchanged in git and byte-identical in the side root.
- The atlas `gauge_matches.json` stamp is identical: 425 offered (398 auto, 23 confirmed, 4 bind), the same as live.
- No match moved or was lost. Two gauge **cut points** moved onto the new cuts (§2): 08EB004 Kispiox, 43 m up, and
  08MF005 Fraser at Hope, 66 m up.
- Which section the station reads is decided in the bundle (not run). 08MF005 now sits in the 2.2 km piece below the
  Hope bridge, not at the start of its own piece. Check `section_gauge` for both stations when the bundle runs.

### 6. `review.apply --check` against the side build [ran]
`--check --waiting-ok --build <side> --marks <live marks.jsonl>`:
- **28 would land, 2 waiting** (the allowed T359 tidal DEM points 354151091 / 360848786); 0 refused, 0 stale.
- rc 1 only because "would land" is not a wait.
- These are the same 30 places as the last audit's section I (63 rows there: each place listed both as "a cut changes"
  and as "waits on new split").
- **All 51 waiting marks are covered.** Every mark that waited on a side rebuild now lands except the 2 tidal waits,
  with one exception: mark 2570 (Seton) shows "would land" but its cut is not in the atlas (Verdict).

### 7. Marks audit on the side build [ran]
`audit --build <side>` and `--since` the last run (`audit-after.json`): 299 → **265** findings.

| section | last run | side | note |
|---|---|---|---|
| A saved more than once | 4 | 4 | |
| B wrong/unsure | 5 | 5 | |
| C tributaries | 115 | 115 | |
| D rest/others | 20 | 20 | |
| E mouth/tidal | 7 | 6 | Pallant resolved (its bridge is now the tidal boundary) |
| F side channels | 26 | 26 | |
| G group marks | 8 | 8 | |
| H one-bank | 2 | 2 | |
| I not landed | 63 rows | 30 | same 30 places, now "would land" (28) + T359 (2) |
| J unmarked | 37 | 37 | |
| K offsets | 12 | 12 | |

### 8. Reach and bundle [ran / not run]
- Reach run: **ran**, `heavy.sh reach-side`, rc 0, 11 min.
- Binding diff: **ran**.
- Bundle, tiles, deliver: **not run**. The report's route stops before them, and the bundle belongs to the release that
  follows the promote.

## Promote commands for the main chat (after reading this)

```
SIDE=$PWD/data/generated/side/dfo-cuts23
unset CURATED_ROOT
# the 23 patches, Seton included: cuts.json must equal the side copy the atlas was built from (inputs digest)
for p in data/generated/review/proposals/split_*.patch; do git apply "$p"; done
diff data/curated/waters/cuts.json $SIDE/curated/waters/cuts.json                     # must be empty
.venv/bin/python -m pipeline.atlas.promote $SIDE/atlas/full_next --accept-boundary-loss   # see note
rtk git commit -m "T743: 23 DFO cuts (22 built; Seton held, anchor fault)" -- data/curated/waters/cuts.json
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.review.apply dfo --dry-run
# --side-checked for each mark-dfo-… the dry run lists as waiting on a side rebuild, EXCEPT the Seton place
#   3:fraser-river:in-that-portion-of-the-fraser-rive (mark 2570): hold it until the Seton cut resolves
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.review.apply --check              # 2 T359 tidal + the held Seton mark
scripts/heavy.sh build 50 -- .venv/bin/python -m pipeline build
```

Notes:
- Parity shows a **net boundary gain (+13)**. The dry run says "4 CUT-POINT(S) LOST across 4 item(s)" (Firesteel, Sproat,
  Cruickshank: T753; Pallant: the tidal mouth row). If promote stops on that per-item loss, `--accept-boundary-loss` is
  justified by §2. Try without it first.
- `--accept-moved-boundaries` should not be needed: no curated rule names a lost or moved id [ran].
- The Seton fix is code or data in `anchors.py` `_tributary_chain`: prefer the line whose mouth lies on the parent, or
  give the cut `tributary_blk: 356355483`. Then rebuild (the next atlas build; a reach-only rerun cannot add a cut), then
  land 2570.
- The resolver also drops a confluence cut silently when no candidate is in range. Under "no silent fallbacks" that wants
  a printed line or a gate. That is a code item; nothing was changed here.

## Questions for the owner (plain language)

1. **Fraser at the Seton River (mark 2570).** The map line we used for "the Seton River's mouth" was the wrong one: an
   unnamed channel 650 m away, not the Seton River itself, so the cut at the Seton mouth did not get made. The fix is
   mechanical (point the cut at the Seton River's own line). Can we fix it and rebuild before your Lillooet stretch goes
   live? Until then that stretch stays as it is today.
2. **Fraser gauge at Hope and Kispiox gauge.** The new Hope bridge cut is 66 m above the Hope gauge, and the new Kispiox
   resort signs cut is 43 m above the Kispiox gauge, so the gauge cut points now sit on those cuts. The gauges still read
   the same stations; only where the piece boundary falls moved. OK? (Default: yes.)
3. **Stamp River "except in those areas and times listed".** After your eight other Stamp places were saved, this
   "everything else" place has nothing left on the map (the others cover all of it). Is that right, or should it cover
   some part (a stretch, a lake) the others do not? (Default: leave it empty.)
4. **Pallant Creek.** With the bridge as the tidal boundary, the 57 m below the bridge is tidal now, and the "whole Pallant
   Creek" salmon place no longer includes it (tidal water is not freshwater). That is what you asked for; just confirming.
