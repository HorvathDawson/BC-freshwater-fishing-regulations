# T603 — The emulator test bed covers saved spots and the two-days line

Status: OPEN

Source: owner 2026-11-10, on build 25's emulator run leaving saved-spot chips unchecked: "Emulator should test the
saved spots too, we could mock it?"

## Today
- The test bed's emulator starts with no saved spots, so nothing on the Spots screen is ever checked (chips, lead-up
  strip, alert editor and alert text, the T598 short wording).
- The T582 line "Regulations day: <day>, marked on the chart" / "<day> is past the forecast" was not seen on the
  emulator: the run left both days on today.
- Seen once on build 25: the Conditions route map drew blank dark green while scrolling.

## Work
1. A test bed flow saves a spot through the app's own UI (the real path, on a gauged water such as the Fraser at
   Hope), then checks the spot row, chips, lead-up strip, alert editor presets and one alert line. If a spot needs
   history the emulator cannot have, seed the app's own store through a test-only entry that writes the same records
   the UI writes (one seeding helper, checked against the store's schema), never a second fake screen.
2. A flow sets the regulations day to a day inside the forecast window, then to one past it, and checks the Conditions
   line and the chart mark each time.
3. Scroll the Conditions pane up and down ten times and check the route map still draws (catch the blank map); if it
   reproduces, file the fix as its own ticket with the capture.
4. Shots of each into the run's output; the flows join the standing walkthrough.

## Acceptance
- A test bed run fails if the saved-spot chips, the alert editor wording or the two-days line are missing or cut off.

Depends: T582, T598. Size: S–M. Owner: no.

## Added 2026-11-10 (T595 leftovers)
5. A staging APK running on an emulator reports as `test` (app-side emulator detection sets the test tag), so emulator
   installs of the owner build stop filing real error kinds.
6. Class the kind `SecurityException: Unsupported path /data/local/tmp/…` (an emulator-only path) as `expected`.

## Added 2026-11-10 (T621: the map changes that need a device)
7. Check on the emulator: the + / − zoom buttons on a bare phone map (G15), the card a titled pin opens (G19), and
   that a tap where two waters overlap picks the nearest (G4).
8. T621 leftovers, each needs a device first: G5 (the route overlay reads atlas tiles through a web-only range fetch;
   the phone needs a native tile reader, so the route trace is web-only today and listed as an open gap in
   `map-props-gate.test.ts`); G21 (the web's `within` mask on the phone: confirm the patched native MapLibre accepts
   a `within` filter before shipping it, since an unsupported filter fails the whole style; about 0.7 MB per style swap).

## Run 2026-11-11 (build 26, version 0.7.4, emulator n1_pixel, the patched-MapLibre build of the same commit)
Build 26 is on Play internal. Its patched map library was checked (the build id in the bundle matches the patched copy).
The ticket's own work (spots flow, two-days flow, scroll check, items 5-8) was NOT done: this run was a look at what
changed since build 25. Shots: `handoff/PLAN/app-integration/T603-shots/` (files start `b26-`).
- **Passed:** Runs names its own day ("Runs: Nov 7-11 (today)", T624); the app opens on today; Fraser River part 2
  shows "2 in-season changes · see" near the top of its Regulations page (T565); the update rows say when each feed was
  checked and last updated (T555, the ordinary state); the Conditions estimate reads "Lower than 7 days in 10"
  (no percentile wording).
- **Failed:** sharing a water gave the app's own start link (`canifishthis.ca/?regs=...`), not `/water/<slug>`. The
  phone reads the water's slug from the production site's `/water-slugs/` files, and the production site does not have
  them yet (it answers with its web page), so the fallback is used. It needs the production site deploy (owner).
  The "no page for this water" line the fallback should say was not seen (shot taken after the sheet closed).
- **Not checked:** the alert editor wording (T563: reached "1. Pick a gauge" only, tapping a gauge in the list did not
  register in my script); a gauge older than 6 hours (T550: the whole live-gauge feed read 6-7 h old, but no gauge
  quiet-with-last-report line was reached); refused or ready update states (T555: needs a bad or newer release);
  saved spots, the two-days line, the blank-map scroll check, items 5-8.
- **Other:** `scripts/fast-fail.sh` failed on `test_review_gate.py::test_the_live_tree_prints_...` (passes alone, 29/29;
  fails only inside the smoke run, it sees `/nonexistent/ci-no-build` from another test), so the build used
  `--no-fast-fail` with that reason.
