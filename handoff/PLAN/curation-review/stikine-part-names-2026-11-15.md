# The Stikine part-name stop (2026-11-15)

Follows `rebuild-finish-2026-11-15.md` §6. Commit hold: nothing committed, nothing under `app/` touched, no release,
no upload, no deploy, LATEST not moved (`--hold-latest`). Tags: **[ran]**, **[read]**, **[inferred]**.
Logs: `data/generated/side/stikine-deadend/logs/`.

## 1. What the check requires [read]
`pipeline/deliver/answers/display/name_check.py`, `run_problems` (T517, review findings 10 and 11), run by the release
step on the shipped water file (`check_release`):
- **run gap**: the pieces of one run (same rules `grp` + `run`, split only by the salmon chain) must all be joined by
  part marks. The app joins neighbours only across a mark, so a run that is not joined can never read as one.
- **run ends**: a run of several pieces must have a lower end on its first piece and an upper end on its last. Its
  joined name is "first lo → last hi"; with `hi=None` it cannot be said.

Part names are not curated per water. They are composed from end tokens (`display.ends`, `part.end_tokens`). A piece
with no main-stem measure (`section_span.lo_m` NULL, `off_stem=1`) has no ends of its own. So this check cannot be
satisfied by naming the piece. It exists to catch a water split by a salmon chain where it should not be.

## 2. The six waters [ran]
Every failing piece is one off-stem section with no measure, salmon chain 136 (`6:e:section`, the region default).
The rest of the water has chain 140 (Stikine) or 139 (Taku). **Horsefeed and Sutlahine are Taku tributaries, not
Stikine.** Handles are 1-based (`ids[h-1]`); the ids below are checked against `atlas/full/section_handles.txt`.

| water (ord) | piece outside | m | at | graph shape |
|---|---|---|---|---|
| Buckley Creek (225) | `360229004:0` | 138 | 57.883 N 130.821 W | Buckley Lake's 2nd outlet (lake_out), no flow out |
| Iskut River (443) | `360214528:2794`, `360223824:0` | 730, 226 | 56.736 N 131.588 W | a lake and channel draining into each other and nowhere (cycle); a lake + outlet with no flow out, not connected to the network |
| Chukachida River (2205) | `360223806:0` | 133 | 57.690 N 127.451 W | unnamed lake's only outlet, ends; unconnected |
| Horsefeed Creek (5590) | `360227198:0` | 71 | 59.025 N 132.715 W | same (Taku) |
| Sutlahine River (5914) | `360219993:49` | 158 | 58.608 N 132.821 W | a lake's 2nd outlet into a lake with no outlet (Taku) |
| Mess Creek (7032) | `360221627:0` | 74 | 57.315 N 130.921 W | unnamed lake's only outlet, ends; unconnected |

Every piece carries its water's own watershed code (e.g. `360223824:0` is coded 600-059339, the Iskut's), is inside
B.C., and is not in `outside_bc`. None is a lake loop (T753), a braid, a border crossing or a region line.

**The places and the owner's marks** (`data/curated/review/marks.jsonl`, read only):
- line 2627: `6:stikine-river:including-tributaries-see-the-tahl`, verified 2026-11-15. Words: "Stikine River /
  including tributaries ( See the Tahltan River )". Binding: `whole` gnis:15430 + tributaries.
- line 2629: `6:taku-river:including-tributaries-see-the-naki`, verified 2026-11-15. Binding: `whole` gnis:8231 +
  tributaries.
- Before the fix: Stikine bound 69,100 sections (walk 68,695 + 425 direct, 20 outside B.C. removed), Taku 23,360.
  Both were `no_extent` in LATEST's run (`full.pre_t743`), so no earlier build saw the split.

**Cause: (a), a gap in the walk.** DFO's words take every tributary. The walk climbs UP from the reach, so it never
reaches a piece that has no flow out: a dead-end second outlet hangs below its lake, and an unconnected piece hangs
from nothing. `code_runs` gives a lake its second outlet only when that outlet crosses a code divide, not when it just
ends. Province-wide the graph has 21,821 codes with a sink and 270 closed loops.

## 3. The fix (general, reach-only) [ran]
`pipeline/atlas/graph/tributaries.py`, **`A_DEAD_END_GOES_WITH_ITS_WATER`**: when the walk is done, a sink it did not
reach (a node with no flow out, or a closed loop with none: `dead_ends`, cached per graph) is collected, with what
feeds it (climbed by `may_enter`), when every other piece of its watershed code was collected. The guards:
- never on the reach's own code (a stretch: the end may lie outside it);
- never past a carve-out or own water;
- never round a refusal: a piece of the code the walk refused (a lake its secondary outlet may not climb into, a
  bigger river) blocks it;
- a real river mouth never qualifies, because a walk reaches a collected water's mouth from below.

`expand` keeps the streams only, as before. No per-water exception. `POLICY_VERSION` 41 → 42, flag pinned in
`test_reach_builder.py`. AGENTS rule 63 has a line.

Tests: new `pipeline/tests/test_walk_dead_ends.py` (7). It covers the Buckley, Iskut-loop and lone-piece shapes,
`expand`, the switch-off pin (3 fail when off), the reach's own code, a carve-out, and `dead_ends`.
`test_may_enter.py` has 2 expectations updated for the rule:
- the `_two_outlets` dead-end outlet `b:0` now goes with its lake;
- the run fixture's lone `run:0` gets its own code, so the RUN mutation still bites.

The "no way round a refusal" guard is pinned by `test_the_order_guard_s_exemption_is_the_lake_s_main_outlet`
(second case) and `test_both_readers_ask_lake_main_outlet`.

## 4. Results
Reach rerun and `pipeline build --hold-latest`, both through `heavy.sh` on lane 1. Two runs were made: the first was
before the refusal guard was added; the numbers below are from the second. [ran]
- **Name check: passes.** "20,670 keyed parts — every name its own, every multi-piece run joined by marks with two
  ends". It ran on the whole release, so no other water would fail it.
- **Binding diff** (`reaches/full.pre_deadend` 161e24a5 → `reaches/full` d8cefe97, `logs/binding_diff2.json`):
  - 113 rules +826 sections, 26 licensing records +97, 35 DFO places +297.
  - **0 sections lost, 0 rescoped, 0 unresolved or outcome changes**, tidal and outside tables unchanged.
  - Stikine +137, Taku +30, and all 7 pieces above are in.
  - One DFO change is not this fix: `3:fraser-river:in-that-portion…` no_extent → bound (+3). That is mark 2570,
    landed by the main chat at 18:47 (commit 03411e9d7) before this build.
  - Largest: Williston r4 +174, Similkameen r1-r3 +125 each, Skeena r1 +70, Nass r1 +63, Peace (Hwy 29 to Site C)
    r2/r3 +37.
- **The same shape elsewhere:** the 34 other DFO places that gained sections had the same gap. None failed the check,
  because only Stikine and Taku left a named water split by its salmon chain.
- **NEW STOP: deliver still rc 1, at the release step's SIZE GATE.** `salmon-6`: 379,295 B gzip > 320,000. Nothing
  was packaged.
  - LATEST's `salmon-6` is 314,700 B.
  - [inferred] The growth comes from Stikine + Taku being bound at all (92,460 sections taken off the region
    default; those two places were `no_extent` in LATEST), not from this fix: +167 sections. The first run, before
    the refusal guard, measured 379,291.
  - The previous build never reached this gate, because the name check stopped it first.
  - Not changed: a size budget is not mine to set.

**Gates** [ran, unfiltered]:
- pipeline CI tier (the brief's marker expression, whole repo): **6,465 passed, 0 failed**, 1,484 deselected. The known
  `test_dfo_suggest` hunt-file failure passes too now that 2570 has landed.
- `scripts/fast-fail.sh` (FAST_FAIL_ALL): **34 of 34 passed**. Mark 2570 has landed; only the 2 T359 tidal waits are
  listed.
- `check-tickets`: ok.
- Marks audit `--since dfo-cuts23b/logs/audit-rf.json`: 264 findings, 1 new, 1 resolved. Both are mark 2570 landing:
  its "would land" finding is resolved, and a new C5_blank says the place has no tributary choice, as saved.
- Data-tier tests: **not run**. Deliver stopped, so `data/generated/bundle` is still the old b9de120e bundle.

## What is live locally
- Atlas `atlas/full` ff8d805a (unchanged).
- Reach `reaches/full` d8cefe97 (new). Backup `reaches/full.pre_deadend` (161e24a5), plus the older ones from
  `rebuild-finish`.
- Tiles: ff8d805a (unchanged; the tiles stage was up to date).
- This build's bundle: `deliver-cache/bundle/out/bundle.sqlite` (ff8d805a, reach d8cefe97).
- **Live bundle: unchanged** (b9de120e, reach 4f1bdab9). LATEST: r-f749475d0962. No CANDIDATE, nothing released.
- Tiles and live bundle are still mixed vintage. Do not ship these tiles with the old bundle.

## Rollback
```
rm -rf data/generated/reaches/full && cp -a data/generated/reaches/full.pre_deadend data/generated/reaches/full
git checkout -- pipeline/atlas/graph/tributaries.py pipeline/atlas/reach/cache.py pipeline/tests/test_may_enter.py \
    pipeline/tests/test_reach_builder.py AGENTS.md && rm pipeline/tests/test_walk_dead_ends.py
```
(Or flip `A_DEAD_END_GOES_WITH_ITS_WATER = False` and rerun reach.)

## Files changed (uncommitted)
- `pipeline/atlas/graph/tributaries.py`
- `pipeline/atlas/reach/cache.py`
- `pipeline/tests/test_walk_dead_ends.py` (new)
- `pipeline/tests/test_may_enter.py`
- `pipeline/tests/test_reach_builder.py`
- `AGENTS.md`
- this file

Suggested commit messages:
- Code: `Stikine part names: a dead end of a collected water goes with it (A_DEAD_END_GOES_WITH_ITS_WATER); POLICY_VERSION
  42; Buckley/Iskut/Chukachida/Mess/Horsefeed/Sutlahine whole in their DFO place`
- This doc: `Handoff: Stikine part-name stop fixed in the walk; deliver now stops at the salmon-6 size gate`

## Owner questions (plain words)
1. **Six Stikine/Taku tributaries: fixed by rule, nothing to answer.** Each had one short channel (71-730 m) that
   FWA draws going nowhere. The walk now gives such a channel to its own water. Your Stikine and Taku "including
   tributaries" saves are unchanged.
2. **NEW: the region 6 salmon file is now too big for its size limit.** It is 379 kB against a 320 kB limit, because
   your Stikine and Taku saves now cover about 92,000 more sections. Options:
   - (a) raise the `salmon-6` gate. Your 11-12 "raise now, shrink later" rule (O5) would put it about 10 % over the
     measurement, near 420 kB. It is in `pipeline/deliver/release/build.py` `SIZE_GATES`, and the other regions' gates
     can stay as they are.
   - (b) keep the build held until the file is made smaller.
   - Suggested: (a). Until then deliver stops at this gate, so no new bundle and no staging release.
