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
