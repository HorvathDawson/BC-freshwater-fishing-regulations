# T736 — Staging check: the rule-book step fails when the app thinks the connection is slow

Status: PARTIAL — step 1 (the check tool) done 2f73c4076; left (Claude): step 2, the app's speed reading, unproven: no evidence yet that the app is wrong

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T736 | Staging check: the rule-book step fails when the app thinks the connection is slow (it asks before the 11.7 MB download; the check never answers; the line was not slow) | PARTIAL | 2f73c4076 | left (Claude): step 2 only if evidence shows the meter is wrong. step 1 done: the smoke presses Download it and says so; meter reading read, not changed (no evidence). staging … (detail: ticket file) |

Source: staging redeploy of b2dba0297, 2026-11-14. `node app/tools/smoke-staging.mjs` failed 1 of 19 twice ("phone: rule
book link opens the synopsis PDF", 60 s timeout), then passed 19 of 19 on a third run (46.8 s for that step) with the
machine still loaded (load average about 27).

## What was seen
The failing shot (`~/bcfish-clean-latest/app/dist/smoke-staging/FAIL-08-pdf.png`) shows the app in its slow-connection
mode ("Slow connection — showing what we have") asking "The rule book is 11.7 MB. Download it / Not now"; the check never
pressed it. The line was not slow: curl fetched the same PDF in 7.5 s. The app switches at 1 Mbit/s measured over its own
downloads; the guess, not proven, is that a busy machine made headless Chrome's downloads read under that.

## Done
- Step 1 (2f73c4076, 2026-11-22): `app/tools/smoke-rulebook.mjs` (`waitForRuleBook`) presses "Download it" when
  `synopsis-offer` shows and the line ends "(asked first (slow-connection mode))"; tests `app/tools/smoke-rulebook.test.ts`.

## Work (left)
2. App, only if evidence shows the meter is wrong. Code reading (not evidence): `packages/data/src/net/meter.ts` counts
   "busy" time from `start()` (before the request is made) until the last download ends, over bytes that reached
   `chunk()`, so request latency and a main thread slow to call `reader.read()` both count as time with no bytes: a busy
   device CAN read slower than the line. To settle it, log the meter's busy ms vs first-byte ms on a loaded run, then
   (if so) start the busy clock at the first byte. A phone under load should not be told its connection is slow.

## Acceptance
- The check passes on a loaded machine, and names the prompt when it met it.
- If the app's reading was at fault: a unit test where a slow consumer on a fast line does not trip slow-connection mode.

## Ledger detail (moved out of the ledger row, T540, 2026-11-22)
left (Claude): step 2 only if evidence shows the meter is wrong. step 1 done: the smoke presses Download it and says so; meter reading read, not changed (no evidence). staging redeploy of b2dba0297, 2026-11-14: failed twice, passed 19/19 on the third run; check tool first, then whether a busy device can trip slow-connection mode
