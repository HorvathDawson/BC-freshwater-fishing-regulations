# Next rebuild: one side atlas for T664 + T665 + the nine DFO cuts (runbook)

Written 2026-11-11 by the T664 lane; completed the same day by the rebuild lane (T665's part, the id resync, the stops).
**It was run on 2026-11-11 up to step 6; the numbers are in `next-rebuild-results.md`.** Nothing is promoted. The side
atlas was built twice: the first build showed that the shore rule judged outlets on lines that are not cut yet (10
cuts for nothing, 6 missing), the rule was fixed, and the second build is the candidate. The first is kept beside it
(`atlas/full_next.first`) as evidence and can be deleted.
Run it top to bottom and read each step's check before going on. Heavy steps go through `scripts/heavy.sh`
(`scripts/heavy.sh --status` first), one at a time. Claude may run every step except 10b (the parser: human only);
steps 7 to 9 change live data and wait for the owner's go.

The side build reads a COPY of the curated tree, never `data/curated`:
```
SIDE=$PWD/data/generated/side/next-rebuild        # curated/ (the copy), atlas/full_next, reach/, logs/
export CURATED_ROOT=$SIDE/curated                 # every command of steps 1-6 runs with it; steps 7-9 run WITHOUT it
```

## 0. Before anything
```
rtk git status                                    # the code is committed: T664 25471b393, T665 f1f928029 + 227c53b08,
                                                  # the rebuild lane a3518f5d2, 2a21460e0, 76f067600, 56b18e8bb
node app/tools/check-tickets.mjs
scripts/fast-fail.sh
.venv/bin/python -c "from pathlib import Path; from pipeline.common.curated import make_root; make_root(Path('$SIDE/curated'))"
```
`make_root` copies the whole live tree (15 MB). A reused `$SIDE/curated` is overwritten file by file: delete it first.

## 1. Inputs, in this order (all on the copy)
a. **The nine DFO cuts** (`cuts.json`, +202 lines):
   `patch -p3 -d $SIDE/curated < handoff/PLAN/app-integration/T664-pending/nine-cuts.cuts.json.patch`
   Only if the owner changed a mark since 2026-11-11: re-make it with
   `python -m pipeline.regs.review.proposals side <the 9 mark-dfo-… ids> --out $SC/side9` and diff against the patch.
b. **T664, the eight still-water oxbow polygons** (owner 2026-11-11: streams, part of the one water):
   `patch -p3 -d $SIDE/curated < handoff/PLAN/app-integration/T664-pending/oxbow-still-water.name_variants.patch`
   It adds `one_water_takes` (8 `wbk:` ids) to the Oxbows name record. The `one_water: true` flag itself is already live.
c. **T665, the tidal polygon rule: no curated input.** It reads the DFO polygon (`tidal_boundary` in the build gpkg)
   and `waters/tidal_limits.json` as it is. Its lists are code, in `pipeline/atlas/splits/shore.py`:
   - `HELD_FOR_OWNER` (8 main rivers), `HELD_UP_THE_CHANNEL` (53), `HELD_UP_THE_CHANNEL_SINCE` (2: Cohilukthan and
     Deas sloughs), `HELD_A_BRAID_SPANS_THE_EXIT` (1: Kainet Creek), `HELD_NOT_ON_THE_OWNERS_MAP` (29: Kitimat, Bella
     Coola, Kemano, Kwatna, Quaal and 24 more that the owner's dry-run pages showed as "not cut"). 93 lines
     (`shore.held_lines()`), none cut by the polygon rule.
   - `HELD_KEEPS_ITS_CUT` (30 of them): the held lines that carry a T660 cut in build 27 keep it where it is, so a held
     line is exactly as build 27 has it. A ticked line is removed from its held list (and its kept cut goes with it).
   - `T665-tidal-limits-mouth-rows.patch` (24 `mouth` rows superseded) is NOT applied: reviewed data, the owner's call,
     and the build is the same either way (a `mouth` alias stays at the line end).
   - `dewdney-slough.name_variants.patch` is a proposal and NOT part of this build.

Check: `.venv/bin/python -m pipeline.tools.audit.check_curated --waters` → 0 errors, 0 warnings (2026-11-11).

## 2. Side atlas (one build, about 30 minutes; deterministic)
```
mkdir -p $SIDE/atlas $SIDE/logs
scripts/heavy.sh atlas-side 50 -- .venv/bin/python -u -m pipeline.atlas.build --full --out $SIDE/atlas/full_next \
    > $SIDE/logs/atlas.log 2>&1
```
(`python -m pipeline build --atlas` runs the same build into `data/generated/atlas/full_next`, but reads the live
curated tree unless `CURATED_ROOT` is set, and `--side DIR --atlas` goes on to reach, tiles and deliver.)

The build stops by itself, naming the file to read, when:
- a shore cut meets a braid of the creek's own water (`braid_sweep_shore.json`). Dry run 3 found one, Kainet Creek
  (360871314, exit 16 m up), now held. Any other one: hold that line in `HELD_A_BRAID_SPANS_THE_EXIT` with the reason
  and rebuild. Never widen `LENGTH_CUT_OK`. (Un-holding the 29 `HELD_NOT_ON_THE_OWNERS_MAP` lines brings 24 such cases
  with it: main rivers whose estuary braids cross the polygon exit.)
- a cut leaves a stream piece under 5 m (`sliver_gate.json`). Dry run 3: none within 5 m of a boundary or line end;
  the closest is 356070273 at 5.95 m.
- the tidal water the shore cuts were decided on is not the written registry's (`shore_tidal_mismatch.json`, no
  registry written). It means a registry pass after `build.water_registry` moved a tidal section. Fix that pass's
  order; do not switch `THE_ATLAS_CUTS_BY_THE_REACHS_TIDAL_SET` off (the graph's own reading misses 54 sections).

Read in the log (`$SIDE/logs/atlas.log`), expected from dry run 3 on build 27:
- `shore cuts: 226 creek(s) cut at the tidal polygon … 215 line(s) tidal whole (inside_tidal.json)` — 196 polygon
  cuts + 30 kept held cuts. The live atlas has 532 T660 shore cuts.
- `one water (T664) Okanagan River Oxbows: 52 item(s) into wsc:300-432687-459615, 88 section(s); listed still water
  taken as stream: 8; kept apart (still water): 0`, and `full_next/one_water_report.json`.
- `shore cuts were decided on the registry's tidal water: 100 section(s), the same set` (build 27's reach run counts
  124 for the same water on 85 blue lines: 24 of its sections were pieces T660 shore cuts made inside tidal water,
  13 on lines tidal whole and 11 below a tidal limit; those cuts are gone).
- `COMPLETE.json` written last.

## 3. Parity and the id dry run (no write)
```
.venv/bin/python -m pipeline.atlas.promote $SIDE/atlas/full_next --dry-run > $SIDE/logs/parity.txt
.venv/bin/python -m pipeline.atlas.inputs --build $SIDE/atlas/full_next        # every input hash current (against the copy)
```
Expected, and anything else is a real stop (the measured numbers are in `next-rebuild-results.md`):
- **Items**: removed = the 52 absorbed oxbow ids, nothing else; added = none.
- **Sections**: every section id that is gone or new lies on a blue line that (a) carries one of the nine cuts, or
  (b) had a T660 shore cut in build 27 or has a polygon cut now (`shore_cuts.json`). No `lake:` section changes.
- **Boundaries**: lost ids are all `shore:` (the T660 rule replaced; promote prints the lost ids by family), plus the
  oxbow items' own `…__lake_<wbk>` edges onto polygons the one item now holds. A lost id of family `curated`,
  `length`, `area`, `border` or another `lake` is a real stop. New ids: `shore:` and the nine cut ids.
  **The T665 lane's "363 dropped shore cuts will need `--accept-boundary-loss`" is not how the guard works**: promote
  compares the count of boundaries in `registry.json`, and only named waters have registry items (92 of build 27's
  532 shore cuts). Measured 2026-11-11: 27,522 → 27,491, net −31, so the flag IS needed. The expected losses are
  exactly 77 `shore:` ids, `length:356364690:23466`, `length:360874795:17123` (the length cap no longer needs them
  beside the new Thompson and Nakina cuts) and `okanagan_river_oxbows__lake_328985789`. Never pass the flag unread.
- **The 93 held lines**: the same section ids as build 27, line by line (`handoff/…/next-rebuild-results.md` §held).
- **The id dry run** (T631 item 3, printed by the same command): before step 4 it lists the 52 absorbed oxbow ids,
  named by `regulations/synopsis/region-8.json` and `inseason/notice_waters.json` (`ABSORBED`), and no `GONE` id.
  After step 4 it lists none. Promote refuses while it lists any.

## 4. Id resync on the copy (registry change → entry resync; produces the reviewable patch)
Entry ids are `<region>:<row slug>@<MU>` and carry no item id, so no entry is orphaned and `prune_remapped` no longer
exists. What moves is `matched` and the in-season extents.
```
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.parsing.backfill_matched --registry $SIDE/atlas/full_next/registry.json --dry-run
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.parsing.backfill_matched --registry $SIDE/atlas/full_next/registry.json --restamp r8:okanagan_river_oxbows@8-1
.venv/bin/python handoff/PLAN/app-integration/T664-pending/resync_notice.py $SIDE/curated/inseason/notice_waters.json $SIDE/atlas/full_next/registry.json
.venv/bin/python -m pipeline.regs.matching.matcher --registry $SIDE/atlas/full_next/registry.json
diff -u data/curated/regulations/synopsis/region-8.json $SIDE/curated/regulations/synopsis/region-8.json
diff -u data/curated/inseason/notice_waters.json $SIDE/curated/inseason/notice_waters.json
```
Check: the dry run lists only the oxbow entry; the two diffs equal `T664-pending/t664-references.patch` (`matched`
53 → 1; notice `is-bb5426c61ff3` 53 → 1 extent plus a `set` history row); the matcher passes; step 3's dry run now
lists no id. The live files are untouched: backups are not needed until step 7.

## 5. Reach run on the side atlas + per-rule binding diff (AGENTS 63)
```
scripts/heavy.sh reach-side 50 -- env PYTHONPATH="$PWD" .venv/bin/python -m pipeline.atlas.reach.cli \
    --build $SIDE/atlas/full_next --out $SIDE/reach > $SIDE/logs/reach.log 2>&1
.venv/bin/python -m pipeline.atlas.reach.binding_diff data/generated/reaches/full $SIDE/reach --json $SIDE/binding_diff.json
```
The reach run stops by itself on a piece that straddles a cut (`straddle_gate`), on any unplaced piece the owner has
not accepted (`unplaced_gate`, rules and DFO places), and on an absorbed id in the corpus. `binding_diff` exits 1 when
anything differs; here it must differ. Expected, and every other difference is explained before going on:
- `okanagan_river_oxbows.r1`: 89 → 89, the same sections.
- The eight oxbow polygons (`lake:328944522, 328948249, 328957729, 328985789, 329459157, 329459219, 329459234,
  329460279`): lake / still-water zone rules lose them, stream zone rules gain them, including Region 8's spring
  stream closure, which the oxbow row's exemption lifts there as on the rest of the water.
- The nine cuts: the rules on Nicomekl, Serpentine, Nakina, Thompson, Harrison and Nicomen keep their water, with new
  ids where a cut split a section. Each lost id reappears as two. The nine DFO places do not move yet (step 8).
- T665: rules lose the sections that became tidal (the pieces below the polygon cuts, the 215 whole lines) and gain
  back 15 short strips that were tidal in build 27 (0.3 km). A dropped T660 cut into fresh water changes ids only,
  with one known exception: the Ucona River's first 33 m join `strathcona_park_waters.r1`. Read the diff BY PLACE
  (`tools/measure_geo.py` in the side build): an id diff cannot see a rule that kept `B:0` while `B:0` grew.
  Counts per rule and water: the results file.
- Standing: unplaced pieces are only the accepted ones (`unplaced_piece_calls.json`); no dead override.

## 6. STOP. Read `next-rebuild-results.md`; the owner decides
Everything up to here wrote only under `$SIDE`. Steps 7 to 9 change live data.

## 7. Promote (owner's go; run WITHOUT `CURATED_ROOT`)
```
unset CURATED_ROOT
SC=<a scratch dir>; mkdir -p $SC/backup && cp data/curated/regulations/synopsis/region-*.json data/curated/inseason/notice_waters.json \
    data/curated/waters/cuts.json data/curated/waters/name_variants.json $SC/backup/                      # AGENTS A2
git apply handoff/PLAN/app-integration/T664-pending/nine-cuts.cuts.json.patch
git apply handoff/PLAN/app-integration/T664-pending/oxbow-still-water.name_variants.patch
git apply handoff/PLAN/app-integration/T664-pending/t664-references.patch
diff -r data/curated $SIDE/curated                  # nothing but files changed by someone else since the copy: read each
.venv/bin/python -m pipeline.atlas.inputs --build $SIDE/atlas/full_next       # current against the LIVE tree now
.venv/bin/python -m pipeline.atlas.promote $SIDE/atlas/full_next --dry-run    # parity = step 3; no dead or absorbed id
.venv/bin/python -m pipeline.atlas.promote $SIDE/atlas/full_next --accept-boundary-loss   # net -31: results §1 lists the 80 lost ids
git commit -m "…" -- data/curated/waters/cuts.json data/curated/waters/name_variants.json \
    data/curated/regulations/synopsis/region-8.json data/curated/inseason/notice_waters.json
```
`promote` renames `$SIDE/atlas/full_next` to `data/generated/atlas/full` (same volume) and the old atlas to
`full.prev`. If `diff -r` shows a curated atlas input that changed since the copy (cuts, names, tidal limits, sloughs),
the side atlas is behind it: rebuild from step 0, do not promote.

## 8. Land the nine DFO marks (they leave NOT_LANDED_OK)
```
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.review.apply dfo --dry-run      # = T664-pending/nine-cuts.dfo-places.patch
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.review.apply dfo --side-checked <id> …   # one commit per proposal
```
Then remove the nine `dfo|…` keys from `NOT_LANDED_OK` in `pipeline/regs/review/apply.py`, and run
`python -m pipeline.regs.review.apply --check`: exit 0, with the two T359 tidal keys still allowed. The `--dry-run`
diff must equal the dfo-places patch. If it differs, a mark changed: the marks are the authority.

## 9. Full build on the promoted atlas, then the release
```
scripts/heavy.sh build 50 -- .venv/bin/python -m pipeline build      # reach + tiles + deliver (labels: one oxbow name)
```
Check: the binding diff against the live release (the release command prints it) holds the step-5 changes plus the
nine DFO places, now bound. Read it per place: Nicomekl and Serpentine 23 m each; Nakina 1 km (whole width; the mark
cannot say "centre line to northern bank"); Thompson Kamloops Lake→Goldpan and Goldpan→Skihist; Harrison lake→bridge
and bridge→Fraser; Nicomen bridge→Fraser and Siddle→Fraser. Then in the app: searching "Okanagan River Oxbows" gives
ONE hit, one card, one map name. The in-season notice's waters and stamp are re-resolved by the release upload
(`pipeline.regs.inseason.resolve --release …`). Re-cut the app fixtures (`node app/tools/regs-recut/sync.mjs`).
Then `handoff/PLAN/runbooks/release.md`.

## 10. Hand-offs
a. Owner: the held lines (93: the 61 of `T665-main-rivers-map.html` + 32 added by this lane, not yet on a map
   page), the four creeks the reach's tidal set newly cuts, the Brem River (not cut), the Nakina half-width, the
   `mouth`-rows patch, the Dewdney Slough search name. All in `next-rebuild-results.md` "Owner items".
b. **Human only (credits): `bash pipeline/regs/parsing/run_parse.sh parse-missing`.** Expect nothing to parse, since
   no entry id moved. Give the owner the command; never run it.
