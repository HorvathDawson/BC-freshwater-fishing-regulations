# T768 — App follow tests pin part counts and names by hand, and three got weaker on 2026-11-16

Status: PARTIAL — merged (ac3eb388f, in 0.8.2); left (Claude): the made-up-part-end proof is NOT met, see below.

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
