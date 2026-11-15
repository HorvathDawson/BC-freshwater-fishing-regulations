# T736 — Staging check: the rule-book step fails when the app thinks the connection is slow

Status: OPEN (after closed testing; small; check tool first, app second)

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
