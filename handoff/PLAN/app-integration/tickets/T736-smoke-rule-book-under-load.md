# T736 — Staging check: the rule-book step fails when the app thinks the connection is slow

Status: PARTIAL — step 1 (the check tool) done 2f73c4076; left (Claude): step 2, the app's speed reading, unproven: no evidence yet that the app is wrong

Source: staging redeploy of b2dba0297, 2026-11-14. `node app/tools/smoke-staging.mjs` failed 1 of 19 twice ("phone: rule
book link opens the synopsis PDF", 60 s timeout), then passed 19 of 19 on a third run (46.8 s for that step) with the
machine still loaded (load average about 27).

## What was seen
The failing shot (`~/bcfish-clean-latest/app/dist/smoke-staging/FAIL-08-pdf.png`) shows the app in its slow-connection
mode ("Slow connection — showing what we have") asking "The rule book is 11.7 MB. Download it / Not now". The check
waits for the PDF and never presses "Download it". The line itself was not slow: curl fetched the same PDF in 7.5 s. The
app switches at 1 Mbit/s measured over its own downloads; the guess, not proven, is that a busy machine made headless
Chrome's downloads read under that.

## Work
1. Check tool: when the size prompt shows, press "Download it" and say so in the step's line (not a silent pass:
   print "asked first (slow-connection mode)").
2. App: find out whether the speed reading can be dragged under 1 Mbit/s by a busy device rather than a slow line
   (time spent parsing or queued counted as transfer time). If so, measure transfer only. A phone under load should not
   be told its connection is slow.
3. A test for whichever of the two changes.

## Acceptance
- The check passes on a loaded machine, and names the prompt when it met it.
- If the app's reading was at fault: a unit test where a slow consumer on a fast line does not trip slow-connection mode.

## Notes
- 2026-11-22, step 1 DONE (2f73c4076): the rule-book step lives in `app/tools/smoke-rulebook.mjs` (`waitForRuleBook`); when
  `synopsis-offer` shows it presses "Download it" and the step's line ends "(asked first (slow-connection mode))". A failure
  says whether the prompt could not be pressed, or was pressed and nothing opened, or there was no prompt at all. Tests:
  `app/tools/smoke-rulebook.test.ts` (a fake page: fast line, the prompt, unpressable prompt, no prompt). The smoke tool
  had no tests before.
- Step 2, read not changed: `packages/data/src/net/meter.ts` counts "busy" time from `start()` (before the request is
  made) until the last download ends, over bytes that reached `chunk()`. So request latency and a main thread that is slow
  to call `reader.read()` both count as time with no bytes: a busy device CAN read slower than the line. That is a code
  reading, not evidence from a run; the ticket's only evidence is a guess. App left alone; to settle it, log the meter's
  busy ms vs first-byte ms on a loaded run, then (if so) start the busy clock at the first byte and unit-test a slow
  consumer on a fast line.
