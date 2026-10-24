# Braid splitting: assessment (T67), 2026-10-22

Read-only. No code, curated data or live outputs were changed, and no heavy job was run. All counts come from
the live files: `data/generated/reaches/full/straddle.jsonl` (written 2026-10-22 17:07, after T56),
`data/generated/bundle/bundle.sqlite`, and atlas `full` (graph.pkl dated 2026-10-10). The October research
used that same atlas, so its numbers still hold.

## 0. The "missing" handoff exists, in the wrong directory

`BRAIDS-HANDOFF.md` and all the braid research from rounds 10 to 14 were written to
`~/.claude/projects/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/handoff/PLAN/`
instead of the repo's `handoff/PLAN/`. None of it was ever committed. The files there:

- `BRAIDS-HANDOFF.md`: the paused state, the owner's decisions, and the open questions.
- `r14_braid_split_plan.md`: the implementation plan, with the exact rule, census, tests and rollout.
- `r13_braids_uniform.md`, `r12_braids_research.md`, `r10_braids_progress.md`.
- `r12_braids/`, `r13_braids/`, `r14_braids/`: the scripts plus `cases.json`, which holds 754 cut/braid cases.
- `braids_review/index.html`: the review page, published at https://claude.ai/artifact/Lei1Bt4cCpnvEwessMVyQK

**Action:** copy these into the repo's `handoff/PLAN/braids/` so the work stops being lost. The rest of this
assessment builds on them and checks them against today's live data.

## 1. The problem today

Curated splits are book landmarks: a bridge, a sign, a confluence, or "5 km below X". Each one cuts only the
river's main blue line. A braid (side channel) that leaves the main line on one side of the cut and rejoins it
on the other side is not cut, so the reach step has to put the whole channel on one side.

The live rule is E15 (R10, 2026-10-16): the channel goes wholly to the side that holds most of its length, and
the section is marked as straddling. The mark lives only in the reach output and the bundle. The app never
reads it (`app/packages` has no reference to it). So the reader sees one uniform answer along the whole
channel. For part of that channel's length the answer is wrong: a closure reads open, or open water reads
closed.

**Live extent:**

- `straddle.jsonl`: 80 rows, 41 sections (76 projected, 4 follow a braid), on the Peace, Adams, Ash,
  Atnarko/Bella Coola, Bulkley, Chowade, Chuckwalla, Dean, Morice, Salmon, Skeena, Thompson and Zymoetz.
- The bundle's `section_straddle`: 38 rows, 18 sids. These are the pieces with length on both sides.
- The R14 census sorts the curated cases by risk:
  - 8 high, where a closure is wrong over 300 m or more: Peace Halfway, Adams ×2, Chowade ×2, Skeena
    Cedarvale, Morice Lamprey, and South Atnarko.
  - 7 medium and 4 low.
  - 6 sit on cuts no rule binds yet.

**Three real examples.** Coordinates are where the cut line crosses the braid. They are converted from BC
Albers (EPSG:3005). Metres are from R14 `cases.json`.

1. **Peace River, 5 km below the Halfway River confluence.**
   - Split: `peace_river__halfway_river_into_peace_river_d5000m`.
   - Braid: section `359012853:0`, a 4,750 m channel. It crosses the cut line at about 56.2373, -121.3850.
   - Rule: peace_river.r5, "No fishing, Mar 1–Jun 30, from 5000 m downstream to 5000 m upstream of the
     Halfway River confluence".
   - Live placement: 72.7 % of the braid lies outside the closure, so the whole channel goes outside.
   - What the reader sees: about 1.39 km of channel inside the closure reads open from March to June.
2. **Adams River, at the salmon-viewing platforms.**
   - Both cuts of the `between` sit on the same braid. Splits `adams_river__signs_200_m_{up,down}stream_of_
     salmon_viewing_platforms`.
   - Braid: section `355996658:0`, 2,845 m. It crosses the cut lines at about 50.9016, -119.5773 and
     50.8994, -119.5698.
   - Rule: adams_river_downstream.r1, "No fishing, Sep 1–Oct 31, between the signs".
   - Live placement: 79.8 % lies outside, so the whole channel goes outside.
   - What the reader sees: about 717 m of side channel between the signs reads open during the salmon-run
     closure.
3. **Skeena River at Cedarvale.**
   - Split: `skeena_river__cedarvale`.
   - Braid: section `360243437:0`, 1,557 m. It crosses the cut line at about 55.0151, -128.3245.
   - Rule: skeena_nass_winter_closure.r3, "No fishing Jan 1–May 31, Skeena mainstem above Cedarvale".
   - Live placement: 68.8 % lies outside.
   - What the reader sees: 488 m above Cedarvale reads open in the winter closure.

**The opposite error also occurs.** On the Morice at Lamprey Creek (about 54.1884, -127.0901, sections
`360213366:0` and `360213368:0`), rules r4 and r5 cover about 368–444 m of braid that lies outside "Gosnell to
Lamprey" (Jan 1–Aug 31 closure, and Sep fly-only). That water is over-closed.

**Two marks are spurious.** On Atnarko `360225339:0` and Zymoetz `360245483:0`, the cut sits within 1 m of the
braid's junction. The "minority" there is an artefact of projecting the braid onto the main line.

**Majority side never picks the wrong side outright.** In R12, 38 of 38 placements agree with the side the
tangent split would give the larger part. The defect is the minority length, which can be anything up to
about 1.7 km (Peace at Site C).

## 2. Where splitting happens now

Everything that cuts is in the ATLAS BUILD. The reach step only places pieces; it never cuts. Deliver/bundle
only carries the result.

| stage | file:line | what it does with braids |
|---|---|---|
| atlas: graph | `pipeline/atlas/graph/prune.py:287` `prune_mainstem_loops` (called `pipeline/atlas/build.py:590`), `graph/nests.py:104` | Deletes pure braid loops (about 40k pieces). Braids carrying a tributary survive, and these are the straddlers. |
| atlas: resolve cuts | `pipeline/atlas/splits/anchors.py:218` `resolve_split_defs` | Projects each anchor onto its host line. **Point and gauge anchors only**: a perpendicular sweep cuts other channels, at `anchors.py:266` (`perpendicular_cut` `:166`, `_spans` `:153`, ±50 m window `:144`, ±1,500 m line `:143`). The sweep is GNIS/WSC-scoped and has no topology gate. Confluence, lake and offset cuts never sweep. |
| atlas: apply cuts | `pipeline/atlas/build.py:751-764` → `pipeline/atlas/splits/sectionizer.py:481` `split_graph_at` / `:395` `_split_one` (lake alias `:279`) | Cuts each `SplitPoint` on its blk at its measure, and re-homes incoming and outgoing edges. |
| atlas: length cuts | `pipeline/atlas/build.py:885-893` (`length_splits.junction_cuts`) | Automatic confluence cuts. 325 of them have a straddler, and no rule binds any of those. |
| reach | `pipeline/atlas/reach/extent.py:280` `_by_measure`; straddle detection `:395-407`, `:450-458`; E15 `:496` flag, `:565` `_majority_side`; `position.py:89` `share_within`; `io.py:47` `STRADDLE_TABLE`; `cache.py:87` POLICY_VERSION 21 | Places each uncut braid wholly on one side and marks it. |
| deliver | `pipeline/deliver/bundle/schema.sql:478` `section_straddle`, `rules.py:1583` `straddle_crossing`, `read.py:369` `straddles` | Carries the mark. Nothing in the app reads it. |

## 3. Is the build step the right place, and is it easier than the slough work?

**Yes, the build step is the right place, and the evidence agrees.** The cut is a geometric fact about the
landmark: "this channel crosses the sign line here". It belongs with the other cuts in the atlas, as one
`SplitPoint` per crossing that carries the split's OWN id. Then every downstream reader, whether a reach rule,
`between`, `upstream_of`, a tributary walk, tiles or the bundle, sees ordinary sections, with no special case
and no mark. The R13 simulation on 68 cases gave:

- 0 unplaced pieces and 0 marks (59 marks before);
- all 8 high-risk braids fixed;
- about 16 more sections in all.

The owner's rule (R14) is "any piece that attaches to both sides of a split is cut along the median tangent".
The plan already makes it exact:

- The cut line is the perpendicular to the M±100 m chord, clipped at its first re-crossing of the host.
- A topology gate qualifies a piece only if it lies on a path from the host below the cut to the host above it.
- Only the host's own registry item is cut.
- A piece is cut once, at the crossing nearest the cut point.
- Within 10 m of a junction, or when the line misses, nothing is cut and the junction is re-homed by end
  geometry.
- An edge-consistency pass runs after the cut.

**What would change:**

- **New module:** `pipeline/atlas/splits/braid_sweep.py`, run between `resolve_split_defs` and `split_graph_at`
  (`build.py` ~751–758). It emits extra `SplitPoint`s with `concern: "braid cut"` and the junction re-homes.
- **Edge-consistency pass:** runs right after `split_graph_at`. Without it the simulation left 14 pieces
  unplaced.
- **Deleted:**
  - the anchors perpendicular sweep (`anchors.py:143-166, 260-282`);
  - E15 entirely: `extent.py:496` and `:565-627`, `position.share_within`, `io.STRADDLE_TABLE`, the bundle's
    `section_straddle`, `rules.straddle_crossing` and `read.straddles`;
  - `test_reach_straddling_braids.py`, which gets rewritten.
- **Added in the reach:**
  - `straddles_the_cut` becomes a build GATE that must stay at 0;
  - a POLICY_VERSION bump to 22;
  - a RULINGS row superseding E15, and an update to AGENTS 12.

**Cost:**

| step | needed? | cost |
|---|---|---|
| Code and fixtures | yes | about 2 days (R14 estimate) |
| Atlas rebuild | **yes**, because the cuts change sections | about 21 min through heavy.sh (summary.txt: 20.8 min; curated splits are 372 s of it) |
| Reach run | yes | about 4 min, about 30 GB RAM, about 15 GB output |
| Bundle | yes | the usual pipeline.deliver run |
| Tiles | **yes**, because section geometry and ids change (tiles and bundle must be the same vintage) | about 1 h, human-run |
| App | no code change | the app never read the mark. The dev fixture `app/packages/data/dev/bundle.sqlite` still has the `section_straddle` table, so drop it or leave it. |

**Easier than the slough work?** Partly.

- **Easier to decide.** The rule is owner-accepted, the census is done (754 cases), and the test list and
  rollout are written. The slough/canal work (T55) is still a finding exercise, and its output is owner review
  of a list of candidates.
- **Smaller in data.** About 16 sections on curated cuts, and about 25 curated plus gauge cuts in all.
- **Not cheaper to ship.** It needs the full atlas → reach → bundle → tiles chain, the same as a registry fold.
  The two tracks could share one atlas rebuild. **I recommend separate side builds** anyway: binding changes
  must be attributed rule by rule, and mixing a registry fold with new cuts makes the binding diff ambiguous.
- **Riskier in the code it touches.** It runs inside the sectionizer, where earlier changes have already broken
  things twice (braid edge direction; end cuts).

**Risks, and how each is contained:**

- **A split fix changes the tributary walk** (the Vedder flooded the Chilliwack). New cut pieces on a braid
  carry tributaries (15 of 18 straddlers do). Contain it with a per-rule binding diff of the side reach against
  live (`pipeline/tools/audit/audit_split_binding.py`, in the T56 `binding-diff-twin.json` form). Every change must be
  one of:
  - a row on the R14 page's rule tables;
  - a tributary of a re-cut braid half.
- **End cuts halve a water.** A braid cut lands on the braid's line, not the host's. If a crossing falls at or
  near a braid's END, `_by_measure` seeding breaks the same way it did on the Bella Coola. The 10 m end
  exclusion plus re-homing is meant to prevent this. Contain it with a fixture (R14 test 4) and by re-running
  `test_reach_end_cuts.py`. After the build, list every braid cut within 25 m of a line end.
- **Atlas determinism.** Memory says two builds differ on about 50 blocks (measured 2026-09-14). AGENTS 59 and
  RULINGS E11 (2026-10-06) say the build has been deterministic since that date, pinned by
  `test_no_slivers.py::test_two_builds_of_identical_inputs_are_identical` (ATLAS_BUILD_TWIN) and
  `test_prune.py::test_the_braid_prune_does_not_depend_on_the_hash_seed`. The live atlas (10-10) postdates the
  fix.
  - Do not assume either claim. If the parity diff shows anything outside the expected set, build a twin of
    HEAD (one more 21-minute build) and subtract it before investigating.
  - The memory note should be updated either way.
- **Gauge cut ids move.** 12 of 13 gauge braid cuts already exist at ±50 m. At ±100 m they shift by a few to
  tens of metres, so their section ids change, and so do gauge panels and pins. `matches.json` is frozen, and
  the build and the bundle both read it, so no gauge rematch is needed. Diff `section_gauge` anyway.
- **Long cut lines.** The cut line runs 840 m (Peace Halfway), 982 m (Peace 5110) and 1.75 km (Exchamsiks).
  These are safe only behind the topology gate. The owner still has to decide whether to cap the line length.
- **Bulges and bends** (Serpentine ditches, Campbell, South Atnarko): the gate never cuts a network that touches
  the river on one side only. A mutation pin turns the gate off and checks that the bulge fixture fails.

## 4. Phased plan with acceptance checks

**Phase 0: recover and decide (no code).**

1. Copy the misplaced braid files into `handoff/PLAN/braids/` and commit.
2. Get owner answers on the 4 open R14 questions:
   - **Q1:** keep the 2300-only ditch lines as the river.
   - **Q2:** leave automatic length cuts out of scope (about 265 sections, no answer changes).
   - **Q3** (fold 339 river-shaped lakes): this moves to T55, the slough/canal track. It is not part of the
     braid work.
   - **Q4:** cap the cut line or not. Recommendation: no cap. The topology gate and the "own item" check
     already block every bad line in the census. Exchamsiks needs its host blk curated either way.
3. Re-run R14 `census.py` on the current atlas through heavy.sh, alone. The atlas has not changed since
   2026-10-10, so expect the same 19 cut / 6 junction / 12 gated / 8 other-water. A difference means the
   census inputs drifted, and the plan must be refreshed first.

Accept when the files are in the repo, the answers are recorded in decisions.md, and the census matches R14.

**Phase 1: the build step, test-first (no data).**

1. Write `braid_sweep.py` and the edge-consistency pass.
2. Write the 12 R14 fixtures: braid across a cut; braid across both cuts; network with an uncrossed partner;
   junction within 10 m; bent host; bulge; edge x/y 1 km off; another item's network; re-crossing clip;
   multi-crossing; shuffled order; reach gate.
3. Write the 3 mutation pins: gate off fails 5, edge pass off leaks on 1, item check off fails 8.
4. Remove E15 and the anchors sweep. Bump POLICY_VERSION. Add the RULINGS row and update AGENTS 12.

Accept when every fixture fails on HEAD and passes after, and the fast suite is green (run unfiltered,
because rtk has reported a false pass before).

**Phase 2: side atlas (heavy.sh, one job).**

Build to `data/generated/atlas/braids` and run `pipeline/tools/audit/build_parity.py` against `full`. Accept only
this expected set:

- about 19 curated braid pieces are cut, about 16 net new sections;
- about 10 gauge braid ids move;
- about 13 junctions are re-homed;
- nothing else, or what remains matches a twin-of-HEAD diff (see the determinism risk).

**Phase 3: side reach and binding diff.**

Run a reach-only pass of the side atlas to scratch, then a per-rule bound-set diff against live reaches.
Accept when:

- `straddle` output is empty, and the reach gate reports `straddles_the_cut` = 0;
- every changed binding is a row in the R14 rule tables, or a tributary of a re-cut half. Expect, for example:
  Peace r5 +about 1.39 km, Adams r1 +717 m, Skeena r3 +488 m, Chowade r1 +734/−893 m, Morice r4/r5 −368 m,
  Atnarko r2 −1,072 m (South Atnarko);
- `between` and `upstream_of` rules through any braid cut within 25 m of a line end are checked by hand;
- `rule_ambiguous_cut` does not grow. A braid cut must never add a second candidate measure to a host split.

**Phase 4: bundle, tiles and app (tiles are human-run).**

1. Build the bundle from the side atlas and reaches. Diff `section_gauge`.
2. Run the card gate `python -m pipeline.deliver.answers.card_check`. It must stay at 0.
3. Build tiles from the SAME atlas, and check that the vintage matches the bundle.
4. Re-snap the regs page. Only the waters R14 lists may change: Peace, Adams, Chowade, Dean, Morice, Skeena,
   Bulkley, Chuckwalla, Thompson, Zymoetz, Atnarko/Bella Coola and Serpentine.
5. Run the slow suite once at the end.

Accept when all of these are green and the three example waters above read correctly on their crossing dates:

- Peace, Apr 15: the braid is closed above the line;
- Adams, Sep 15: the braid half between the signs is closed;
- Skeena, Mar 1: the braid above Cedarvale is closed.

**Phase 5: promote.**

Promote the atlas, reaches, bundle and tiles together, keeping the old ones as `*_prev`. Update memory: replace
`braids-paused` with the shipped state, and correct `atlas-build-not-deterministic`.

**Effort:** about 2 days of code and tests, about half a day of side builds and diffs (one atlas build, one
reach run, possibly one twin build), and 1 hour of human-run tiles.
