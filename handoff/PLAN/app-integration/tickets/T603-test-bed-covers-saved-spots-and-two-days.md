# T603 — The emulator test bed covers saved spots and the two-days line

Status: PARTIAL — items 1–4 built (1df1ed1af) and run on the emulator on the kept APK (commit 104e6513c): saved-spots, two-days, route-scroll all pass; left (Claude): items 5–8 (the emulator `test` tag, the SecurityException kind, the T621 device checks) and a rerun of the three on the next APK

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

## Built 2026-11-11 (items 1–4; code 1df1ed1af)
Three scenarios in `app/tools/testbed/scenarios/`, in the standing order after `spots` (`scenarios/index.mjs`), each in the
README table:
- **`saved-spots`** (item 1): a spot saved through the app's own UI, online, "At my location" on the Fraser at Hope, the
  day before. **No seeding helper was needed**: the capture reads the week before by itself (weather and the gauge's
  daily means), so the UI path fills everything. Checked: the Spots row and its lines, the visit chip, Add a visit, the
  five rating stars, the week-before strip (weather day columns; 7 daily means of 08MF005 each with its short place,
  e.g. "9+ in 10 days higher"; the trend line), no "percentile" anywhere (T598). Then the alert editor on a gauge card
  (Coquitlam River at Port Coquitlam): title, Crosses above / below, A value / A level for the day, the five presets
  ("9 in 10 days higher | 8 in 10 days higher | Median | 8 in 10 days lower | 9 in 10 days lower") and Custom, the
  summary line for a value and for a level; one alert saved ("1 alert on this gauge"), its line read whole in More →
  Notifications, removed; the spot deleted.
- **`two-days`** (item 2): the map's day strip set to today, tomorrow, 11 days on; a gauged water's Conditions each
  time. Today: no line, the chart marks Today only. Tomorrow: the whole sentence "Regulations day: Thu 12 Nov, marked on
  the chart." and the chart's regulations-day mark inside the plot. 11 days on: "Sun 22 Nov is past the forecast." and
  no mark. Conditions' own day stays "Conditions: today" throughout.
- **`route-scroll`** (item 3): ten rounds of a slow drag, three flings down, four back up; the route map's box measured
  while the pane moves, the moment it is back and at rest (share of the box that is one flat colour; drawn ≈ 40–45 %,
  blank ≥ 98.5 %). One flat colour at rest fails; flat only while moving is a finding with its shot.
- `lib/screen.mjs` (+ `screen.test.mjs`, 9 tests): the screen as uiautomator nodes. "Missing or cut off" means: not in
  the tree, not drawn, no words, off the screen's side, a chip's label past its chip, or a one-line label squeezed
  under 8 px a character. **Limit:** a label the phone ellipsizes inside a box wide enough for it (e.g. the donor row's
  "Coquitlam River at Port Coq…") cannot be seen from uiautomator, which reports the whole string; the step shots are
  the check for that.
- Test-bed bug fixed on the way: the `open-result` flow's "Open .*" tapped a saved spot's map pin ("Open spot <title>")
  instead of the search result as soon as one spot existed.

**Recorded run** `app/tools/testbed/out/runs/2026-11-12T01-10-46/` (kept APK, commit 104e6513c, `--allow-stale`,
`--no-r2`, emulator n1_pixel, release r-303b4007a70a): install 4/4 (a fresh install first), saved-spots 12/12,
two-days 5/5, route-scroll 3/3. In route-scroll the first frame back was "still gliding" in all 10 rounds, so only the
moving and resting frames were judged. An earlier pass of the same three on an app that already held data:
`out/runs/2026-11-12T00-17-43-T603-dev/`.

Shots (kept APK, the earlier pass): `../T603-shots/t603-*.jpg` (week-before strip, alert presets, the two chart states, the
route map after the scrolls, the alert confirmation left off screen).

### Seen on the way (app, not fixed here: other lanes own the app)
- **The blank route map did not reproduce**: 10 of 10 rounds drew (moving ≈ 40 %, rest 45 % one colour) on the kept
  APK, Coquitlam River. No ticket filed.
- **After "Save alert" the confirmation can be off screen** (seen once by hand, `../T603-shots/t603-alert-saved-off-screen.jpg`):
  with the editor scrolled so Save sits low on the screen, saving collapses the editor and the pane is left showing the
  gauge list; "Saved. Manage alerts in More → Notifications." and "1 alert on this gauge" are above the fold. Not seen
  when Save is higher on the screen (the scenario's run). The scenario reports it as a finding when it happens.
- **A saved spot's pin is in the accessibility tree while off the map**: with the spot at Hope and the map on Coquitlam,
  the main map still holds a button "Open spot <title>" 3 px square at the map's top-left corner (uiautomator
  `spot-mark:spot:…` bounds [0,136][3,139]); a screen reader can reach it and a tap-by-label opens the spot.
- `gauge-offline`'s way in is stale (not run here): "Vedder River" now lists as "see Chilliwack / Vedder Rivers", and
  its `tap-text Conditions` lands on the Conditions TAB once the water did not open. It needs `openWater` (common.mjs).

## 2026-11-11, later
- The two app bugs above (the alert confirmation off screen, the off-map spot pin in the accessibility tree) are T674:
  fixed and pinned in component tests (04572e8d3), not yet seen on the emulator. The next run of `saved-spots` should
  save an alert with Save at the foot of the screen and read the confirmation at rest, and check the uiautomator dump
  holds no `spot-mark:` node while the spot is off the map.
- `gauge-offline`'s way in is `openWater` now (Coquitlam River by default; `GAUGE_WATER` + `GAUGE_ROW` name another).
  The file loads (`node --check`, the scenario index imports); it was NOT run on the emulator (a side atlas build held
  heavy lane 1; no emulator was started for it).

