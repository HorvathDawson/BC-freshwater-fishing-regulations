# T768 — App follow tests pin part counts and names by hand, and three got weaker on 2026-11-16

Status: DONE — the made-up-part-end proof is met and rerunnable (`app/tools/part-end-proof.mjs`, 1716bcb87, f8916d70f);
the three weaker tests are mutation-checked; T993 item 10 (ff946c86e, c5d66fccc). See "Done, 2026-11-22" at the end.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T768 | App follow tests type part counts and names by hand (twelve stopped the r-46e4355a7277 release, 39918037e still types "of 21"), three got weaker in that commit, and `release-perf.test.ts` + two live-release tests never run where they should. One content pin per water in one file; the rest read the group | DONE (made-up part end fails only the content pin: `tools/part-end-proof.mjs`; + T993 item 10) | b4a664be8, 5d928a1fb, 36559a549, ff946c86e, c5d66fccc, 1716bcb87, f8916d70f | the general audit 2026-11-16 (`audit-2026-11-16/guards.md`); the app chat's own commit |

Source: `handoff/PLAN/audit-2026-11-16/guards.md` and `code.md`. Area: app tests.

## What is wrong
- Twelve app tests stopped the staging release of r-46e4355a7277 because they typed the shape of four waters
  ("Part 2 of 18", whole lists of part names). 39918037e moved them to the new shape but still types "of 21",
  "Part 5 of 6", "Part 7 of 7": the next new part end stops a release again, in up to seven files.
- Three tests changed in 39918037e got weaker: `WaterScreen.unnamed.test.tsx` picks its reach by the property it then
  asserts (the old tie between the closure's rule and the receiving water is gone); `WaterMode.test.tsx` checks the
  unnamed reach's name only against a pattern; `sectionNames.test.ts` checks `unnamedStreams()` for exclusion and
  order only.
- Tests that never run where they should: `app/tools/release-perf.test.ts` (14 tests) is skipped even in the release's
  strict run (it needs BUNDLE as well as RELEASE_DIR); `app/tools/stretch-names.test.ts` and
  `app/conformance/src/regs/salmonLazy.release.test.ts` never run on GitHub (no live release there).

## Fix
1. ONE content pin per water, in one file: `app/conformance/src/regs/waterGroup.fixture.test.ts` keeps each fixture
   water's list of part names (a new part end is seen and attributed there, once). Every other test (the bar in
   `ui/src/waterMap.test.ts`, `WaterMapView.test.tsx`, `StepInPlace.shell.test.tsx`) takes "Part k of N" and the names
   from the water's group (`loadWaterGroup`, `partName`), finding parts by their ends.
2. The three weaker tests: the unnamed reach's name is compared with the golden taps' name for that handle (the
   pipeline's own answer); the closure's rule is tied to the named water of the store that owns its entry, and the
   page says "Tributary of" that water; `unnamedStreams()` is compared whole with the golden unnamed taps that have
   a stream line.
3. The skipped tests: make `release-perf.test.ts` run in the release's strict run or say on the release's output that
   it did not; decide with the main chat whether the two live-release tests get a place in the release follows list.

## Done when
A made-up extra part end on a fixture water fails exactly one test file (the content pin), and the three tests above
fail when their subject is wrong (a mutation each).

## Done so far, 2026-11-16 04:50 (app chat; test files and the node-only test driver, nothing the app bundles)
- The tests of 39918037e no longer type a part count or a cut-dependent name: `StepInPlace.shell.test.tsx` (the Skeena
  walk), `WaterMapView.test.tsx` (Skeena, Cowichan, Zymoetz) and the Cowichan bar in `ui/src/waterMap.test.ts` take
  "Part k of N" and the names from the water's group (`partWhere`, `partName`), finding parts by their ends; what
  makes each case what it is stays held by ends (the Ecological Reserve, the km-less side channel, the mouth and
  lake outlet). The content pin stays in `conformance/src/regs/waterGroup.fixture.test.ts` alone.
- The three weaker tests: the unnamed reach's name is the golden taps' name for that handle (new reader
  `regsFixtureUnnamedTaps()`); the closure's rules are tied to the one named water of the store that owns their
  entry, and the page says "Tributary of" that water (the two lines that asserted the pick's own property are gone);
  `unnamedStreams()` is compared whole with the golden unnamed taps that have a stream line.
- A real fault found by that last test's mutation: `unnamedStreams()` took any slot with a line, and a slot no row
  names is a filler (a line spanning nothing) wherever its block has other rows. It now needs a line that spans
  something. Proven by removing one tap's line: the list loses exactly that tap.
- Checked: the seven files 95 tests pass on the fixture; `pnpm run check:fast` rc 0.

## Still open
- Older typed counts in the same files ("of 5" Chilliwack, "of 3" Coquitlam, "of 2" Vedder; about 60 places), and
  `core/src/waterNav.test.ts`.
- The proof asked for under "Done when": a made-up extra part end on a fixture water fails exactly one file.
- Fix item 3 (the tests that never run where they should).

## Built (branch app-fix/t765-t768)
- The older typed counts ("of 5" Chilliwack, "of 2" Vedder, "of 3" Coquitlam / Kootenay Lake) are gone from
  `ui/src/waterMap.test.ts`, `ui-native` `WaterMapView.test.tsx`, `StepInPlace.shell.test.tsx`, `WaterMode.test.tsx`,
  `apps/mobile/src/back.test.tsx` and `core/src/waterNav.test.ts`: counts, "Part k of N" and part names come from the
  water's group (`loadWaterGroup`, `partWhere`, `partName`, `partRowAria`). Left typed on purpose: `waterMap.start.test.ts`
  and the `waterNav.test.ts` row tables (made-up rivers / local copies, self-consistent), tool tests quoting strings.
- Proof attempt (a made-up end spliced into the Chilliwack's rows, temporary, reverted): it failed MANY files, not one
  (the fixture pin, `waterMap.test.ts` (marks and per-part lists still typed), `WaterMapView`, `WaterMode`,
  `WaterMapSplit`, `WaterMapFraming`, `TapCue`, `NavRail`, `MapDay`, `Spots`, `surfaces`, `water-frame-*`,
  `stretch-names`; the mutation also broke the ends table, so some are artefacts). So the "exactly one file" acceptance
  is NOT met: the remaining typed marks / part lists in those files are still to do (a follow-up).
- Item 3 (measured on r-46e4355a7277): `tools/stretch-names.test.ts` + `conformance/src/regs/salmonLazy.release.test.ts`
  together 7 s; they read only the release dir (plus the committed `fixtures/dfo-feed-2026-11-13.json`), no curated or
  bundle. Both ALREADY follow the release: `pipeline/release_follows.py` finds them by rule (`liveReleaseAt` /
  `liveRelease`) and `python -m pipeline.release_follows` lists them; they cannot run on the CI host (no release there),
  by design. No change needed. `release-perf.test.ts` reads the BUNDLE (a data/generated read), so it stays out of the
  guaranteed run; it now says `NOT RUN: RELEASE_DIR is set but BUNDLE is not` in its test name and on the console
  instead of silently skipping 14 tests.

## Done (branch worktree-agent-a2d88a582cedab642, 0.8.1 lane C)
More typed content now read from the water's group (`loadWaterGroup`, `markLabel`, `partName`, `partRowAria`): in
`ui/src/waterMap.test.ts` the Chilliwack's ▲ climb length, the part_section rows, the marks (rings and labelled ends),
the mark card (words, up/down part names and part_ix), the parts list rows, `section`/`part` actions at the top part, and
the part / whole-river frames (checked to hold the parts' own boxes instead of typed bounds); in
`ui-native/.../WaterMapView.test.tsx` the bar's name, the marks, the mark card text, the back test's open-mark label, the
number of ▲ taps and the top fit key. Typed content left: the Skeena names (T750, ends), part-0 stream keys and bbox
(`CHW0_LIT`, geometry), `waterNav.test.ts` row tables. The "made-up part end fails exactly one file" proof is still NOT
run; fix item 3 was already settled.

## Done, 2026-11-22 (lane test/T768-follow-tests; with T993 plan items 10 and 12)
- **The proof, as a tool** (1716bcb87): `scripts/heavy.sh t768-proof 30 -- node app/tools/part-end-proof.mjs` writes a
  copy of the regs fixture with ONE made-up part end (the Chilliwack's Slesse-signs → Ecological Reserve part cut at
  Chilliwack Lake: `lake_outlet` / `lake_inlet`, both existing end tokens; `part`, `part_section`, `part_line`,
  `part_mark`, the picker choice, `part_geo` and the manifest follow) and runs the whole app suite on it through
  `REGS_FIXTURE_MUTANT` (the node-only driver honours it only for a folder holding the tool's `MUTANT.json`). Exit 0 iff
  the one counted failure is the content pin; `tools/regs-fixture.test.ts` (bytes = a rebuild from the source release)
  is listed as provenance, never counted. `tools/part-end-proof.test.ts` pins that the mutant reads whole (one more
  part, every other water as committed), the verdict, and the variable's guard.
- **What it found** (run 1: 4 files besides the pin; run 2: 1; run 3, 780 files: **only the pin**): `waterMap.test.ts`
  typed the whole part list by ends (`CH_ENDS`), the four boundary marks and the five parts' crossings; `WaterMode`
  named the Slesse part by both its ends (a new end inside a part removes such a part: both now name it by a place on
  it, `partAt`); `WaterMapView` typed part numbers into expected calls (4, 2, 1: now the group's); `golden.test` held
  the copy's folder shape (the copy now sits at `…/regs-release/<id>`). f8916d70f.
- **The three weaker tests, a mutation each** (in `core/src/regs/header.ts`, reverted): the unnamed reach's name with
  its region digit swapped ("Region 1" for "Region 2", which the old `/^Unnamed stream · Regions? \d/` accepted) fails
  `WaterMode` T53 and both `WaterScreen.unnamed` tests; "Tributary of" worded "Flows into" fails `WaterScreen.unnamed`
  (2 tests); `unnamedStreams()` was proven on 11-16 (above).
- **T993 item 10.** The anchor scanner's new `absent` kind (ff946c86e): a release-following test may not assert a
  value's SHAPE (`not.toHaveProperty`, `"f" in x` / `hasOwn` false, `Object.keys(x)` equal to a typed list); every
  spelling and the near misses pinned, mutation-checked (detection off: 2 tests fail); the 3 live hits rewritten. A
  column the writer adds without a mini re-cut: `test_mini_release.py` pins that `--check` fails naming the file and
  `python -m pipeline.tools.mini_release --write` (c5d66fccc); proven end to end with a planted `water_exit` column in
  `pipeline/deliver/release/build.py` (exit 1, the re-cut named; reverted). Before a push it runs in fast-fail's mini
  chain (any pipeline source) and in `scripts/before_push.py` (pipeline-ci's step).
- **T993 item 12.** `WaterMode` "across waters" is not a live flake: every CI log since T849 (36559a549, 11-18 21:04Z)
  that lists it, 9 runs, has it passing in 1.3–2.3 s; the post-T849 `WaterScreen.unnamed` failures (11-21) were the
  T980 fixture-shape error, not a timeout (fixed in 8c1228707).
- Not done, moved to **T1002**: a whole record typed (`expect(g.exit).toEqual({…})`) and `toBeUndefined()` on a release field are not
  scannable without refusing legitimate value pins (header of `check-test-anchors.mjs`); the pipeline scanner
  (`pipeline/tests/anchors.py`, another lane's file this round) has no `absent` kind; the in-text rule misses a part
  number typed after a short-named slot (`onPart={(i, p) => …\`part ${i} ${p}\`}` let "part gnis:8634 4" through).
