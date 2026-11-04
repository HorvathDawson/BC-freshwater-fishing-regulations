# After 0.5.0: start here

Written 2026-11-04, as 0.5.0 was being cut. It replaces `NEXT-SESSION-0.5.0.md` once 0.5.0 is on Play. It is one chat
for the whole tree: pipeline, worker, core, ui, ui-native, map.

## Paste this to start the chat

> Read `handoff/PLAN/NEXT-SESSION-after-0.5.0.md`, then the STATUS.md top entry. 0.5.0 is out. Do "First" in order,
> then the work list; start ticket work without asking, ask me only for decisions. Keep replies short and STATUS.md
> current.

## Where 0.5.0 left things
- **Release:** the last 0.5.0 data release is in `data/release/LATEST`. It is on staging R2 with `minAppVersion` 0.5.0
  (`deploy/staging/upload.sh`). The STATUS top entry names it.
- **App:** 0.5.0 (`app.json`, `package.json`); `versionCode` comes from `deploy/staging/build-apk.sh --play`.
- **Staging:** site `staging.canifishthis.ca` deployed with 0.5.0. The feeds worker (staging, the only one) is deployed
  and writes `dfo-feed/3`, which 0.4.0 can't read, so 0.4.0 installs lose DFO salmon until they update.
- **Play:** the AAB goes to internal and Closed testing. The Console steps are the owner's (T371, `play/`). The owner
  may start Closed testing with fewer than 12 testers and add more later.
- **Tests before release (owner 2026-11-04):** a lean run, not the full suites: the app tests `--changed` since the
  morning of 11-04 and the pipeline tests of the modules changed that day.
- **Crashes and memory were in 0.5.0:** T345 fixed (8a5b5bd; its last path confirmed by the test bed's water-nav), T352
  done (crash/2 carries the water, screen, install id), T360 measured and fixed over five rounds, T351 waits for a
  0.5.0 crash report (they now say where). STATUS names what the final emulator run confirmed.

## First, in order (Claude's own, no owner step)
1. **Read the reports.** Testers' problem reports and crash reports land on the staging worker (`/status`, `GET
   /crashes` with the run token in `app/workers/feeds/.run-token`, the daily digest). Read them, and turn each real
   problem into a ticket. `OWNER-PHONE.md` lists what testers may trip on (fling, copy text, whitefish swipe, closed
   water, Change percentiles, forecast days, Salmon card).
2. **The full suites, once, quietly in the background** (the pre-release run was lean). App:
   `scripts/heavy.sh app-suite 50 -- bash -c 'cd app && npx vitest run'`. Pipeline:
   `scripts/heavy.sh pytest 50 -- .venv/bin/python -m pytest -q pipeline/tests`. Then `pnpm -C app check`. Run them
   unfiltered (`rtk proxy`). Fix what's red, or ticket it. Known flake: `Walkthrough.test.tsx` "Layers … Next puts
   them back" fails under the parallel suite and passes alone (2 s `waitFor`s).
3. **`graphify update .`**: the graph was not updated after the 0.5.0 changes.

## Then the work, roughly in this order
1. **New crashes from testing:** T351 when a 0.5.0 crash report names its screen; any new one from Closed testing.
2. **T377 rest:** the T367 leftovers after the forecast-day list (tappable/collapsible legend etc., see the ticket).
3. **T178 weights:** the worker now scores CLEVER vs COFFEE monthly (`/run/skill`, `private/feeds/skill.json`). After
   one or two months of scores, propose the weights. Changing them is a reviewed change, not automatic.
4. **T389:** unnamed-reach links without the `lines` download (stream key in the tiles; needs a tiles rebuild, so
   batch it with the next atlas rebuild).
5. **T383:** per-water time zones. 0.5.0 uses one B.C. clock (UTC−7, "PT"), core `clock.ts`.
6. **iOS:** T313 (simulator), T336, T88 (signed build + TestFlight), T362–T365.
7. **T239:** the annual synopsis update runbook + book diff tool + dry run (the next synopsis is the trigger).
8. **Pipeline hygiene:** T306, T341, T305, T304, T331. **Audits:** T333–T335 (the owner scopes them first).
9. **T250:** Expo SDK upgrade (re-apply the MapLibre RN patch; the fling hunk is in it), best before the next store
   release.
10. **Production launch (T86, T240):** production is not used yet; every build reads staging. When it launches, the
    production feeds worker ships WITH the app that reads its formats, never ahead (`dfo-feed/3` needs 0.5.0+).

## Decided on 2026-11-04 (don't re-ask)
- B.C. time is one constant (UTC−7, "PT") until T383. Device clock reads go only through core `clock.ts`.
- On the web, a named water's link place comes from `part_line` in the water file (T388). Only unnamed reaches fetch
  `lines`.
- Licence tab: no sources sheet (`licsrc:` removed); guiding is a closed fold; the page foot is fine print; each
  Conservation Surcharge Stamp has its own glossary entry; licence conditions keep "Dolly Varden" capitalised and say
  "or" for several fish.
- The atlas build is deterministic (T65): a build-to-build diff is real signal.
- No separate owner phone walk: internal and Closed testing cover it (T77 closed).
- Old releases are pruned to the live one + the previous one.

## Rules that still hold (AGENTS.md has all of them)
- The LLM parser is human-only. Prod R2 and the prod deploy are human-only. Staging upload, staging deploy and the
  staging feeds worker deploy are allowed.
- Every heavy job goes through `scripts/heavy.sh`, one at a time (start gate ≤50%).
- Commit with `git commit -- <paths>`. Never `git stash`, never `git add -A`, never edit `marks.jsonl`, never
  pattern-kill processes. `main` is never touched.
- No silent fallbacks (`reportFailure`); one load path per screen; ask before filtering data content; everything
  added for review is a proposal.
- At most 2 agent lanes at once.
