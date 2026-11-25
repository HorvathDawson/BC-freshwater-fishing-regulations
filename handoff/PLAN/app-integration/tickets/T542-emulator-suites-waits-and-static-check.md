# T542 — Emulator suites: wait on conditions, and a static check that every id and label they use exists

Status: DONE 2026-11-20 (5107823ca) — all six suites green with the condition waits; wall times recorded under "Wall time" below.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T542 | Emulator suites wait on conditions; static check that every id and label a step uses exists | DONE | 5107823ca | six-suite emulator run + wall time (command in the ticket) |

Source: workflow review 2026-11-09 (verified against the code). Rank 10.

## Today
- Fixed sleeps dominate the release suites: `app/tools/testbed/scenarios/offline.mjs` 16 (`:57,74,94` 4 s; `:152` 15 s),
  `water-nav.mjs` 12 (`:121` 8 s), against few condition waits (install 0, gestures 0, water-nav 1, offline 6).
  `water-nav.mjs:66` and `gestures.mjs:69,131` tap computed coordinates.
- Nothing reads the scenarios outside an emulator run (the only test is `lib/channel.test.mjs`), so an id or label a
  step depends on can change with no failure until a heavy run. Chat 9 fixed three stale steps after the fact; the bell
  step predating T263 is the kind a static check catches (the © chip overlap and the Retry race need a run).
- `feeds.mjs:25`'s 65 s sleep is deliberate (must pass the 60 s foreground timer): keep it, say why in place.

## Work
1. Static check (vitest, seconds; in fast-fail when `app/` or `testbed/` changes): every testID and literal screen text a
   scenario taps or waits for exists in the app source.
2. Replace fixed sleeps in install, offline, feeds, big-update, gestures, water-nav with condition waits with a time
   limit, except documented timer waits.
3. A failed step's report names the commit that last touched the step and the screen file it reads.

## Acceptance
- Renaming a testID used by a scenario fails fast-fail. Suite wall time before/after recorded here; all six green.

Depends: T534 (same test-bed files). Size: M. Owner: no.

## Built (2026-11-10, 5107823ca)
- `app/tools/testbed-static.test.ts` (vitest, ~1 s, in fast-fail whenever it or an app/tools file it covers changes; in `pnpm check`): reads every scenario, flow and the runner; every testID (`ID:`, `hasId`, `resource-id`, flow `id:`) must be in the app source (a template id such as `water-view-regs` needs its fixed prefix in a `testID={`…`}` template AND `regs` as a quoted key); every literal piece of text a step taps (`TEXT/TAB/LINE`) or waits for (`waitText`, `checkNow`, flow `text:`) must be in the app source, except a short allowance list that says why (OS dialog, release-data names, text the scenario types) and fails when stale. Mutation tests pin that a renamed id is not found.
- `lib/waits.mjs` (`ctx.until / settle / timer / hasId`) and `common.mjs toBackground`: install, offline, feeds, big-update, gestures and water-nav no longer sleep a fixed time except as a poll interval; `until` has a time limit and names what never came; a `soft` wait is always followed by the step's own assertion; `settle` ends when the screen (words, or pixels with `frame`) stops changing, never later than the old sleep. Kept as `ctx.timer(ms, why)`: feeds 65 s (must pass the 60 s foreground timer, said in place), offline 15 s (nothing on screen shows the throttled check ended), install 5 s (the finish being measured), water-nav 2 x 4 s (a crash arrives a beat late). The test pins that no other `device.sleep(` is in those six files.
- `lib/blame.mjs` + `run.mjs`/`report.mjs`: a failed step's report line names the scenario file, the commit that last touched it, and the app files holding the ids and labels the step used.
- Tapped coordinates (`water-nav` `input tap` at the `[target]` probe, gestures swipes) stay: the map canvas has no element to tap.
- Proof: that test file (20 tests), `node --check` on every scenario. Wall time before/after: NOT measured (no emulator). Ceiling: 47 fixed sleeps totalling ~204 s in these six files before (some inside poll loops, which stay); 93 s of documented timers remain.
- Not verified (needs the emulator): all six suites green with the condition waits. Command: `scripts/heavy.sh testbed 45 -- bash app/tools/testbed/session.sh --build -- node app/tools/testbed/run.mjs install feeds big-update offline gestures water-nav` (T534's stamp is built in), then record wall time here. Risk to watch: a `settle` that returns early on a still-loading screen, and `toBackground` waiting for the tab bar words to leave.
- Needs a change in scripts/ (not mine): none. The static test is picked up by `vitest_changed` when it changes; to run it when only an APP file changes, add `app/tools/testbed-static.test.ts` to `vitest_changed`'s list in scripts/fast-fail.sh when `app/apps/` or `app/packages/ui-native/` files changed.

## Wall time (recorded 2026-11-20 from app/tools/testbed/out/runs reports)

| suite | before (runs 11-08 to 11-10 UTC, before 5107823ca) | after (run 2026-11-11T22-25-01, commit 104e6513c) |
|---|---|---|
| install | 195 s (11-09T22-54) | 191 s |
| feeds | 267 s (11-10T00-09) | 293 s |
| big-update | 429 s (11-10T00-22) | 430 s |
| offline | 524 s (11-09T23-09) | 492 s |
| gestures | 124 s (11-08T02-32) | 101 s |
| water-nav | 470 s (11-08T02-04) | 341 s FAIL, then green 564 s (11-11T23-01); green since: 362 s (11-19T22-16), 376 s (11-20T02-59) |

Not like-for-like (different days and builds; the waits mostly sit on downloads and map loads, which vary). Total wall
time is about unchanged; the gain is reliability (no fixed sleep shorter than the screen it waits for) and the static
check. All six green.
