# T603 — The emulator test bed covers saved spots and the two-days line

Status: PARTIAL — items 1–4 built (1df1ed1af) and run on the emulator on the kept APK (commit 104e6513c): saved-spots, two-days, route-scroll all pass; items 5 and 6 built 2026-11-12; left (Claude): items 7–8 (the T621 device checks) and a rerun of the three on the next APK

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T603 | The emulator test bed covers saved spots (saved through the UI or one seeding helper) and the two-days line; scroll check for the blank route map | PARTIAL — left (Claude): items 7–8 (T621 device checks); rerun the three on the next APK; items 5–6 built 2026-11-12 (emulator `test` tag, SecurityException `expected`) | 1df1ed1af | owner 2026-11-10; scenarios `saved-spots` (UI path, no seeding needed), `two-days`, `route-scroll` + `lib/screen.mjs`; all pass on the kept APK (104e6513c); the blank route map … (detail: ticket file) |

Source: owner 2026-11-10, on build 25's emulator run leaving saved-spot chips unchecked: "Emulator should test the
saved spots too, we could mock it?"

Done: items 1–4 (1df1ed1af): scenarios `saved-spots` (UI path on the Fraser at Hope, no seeding helper needed),
`two-days`, `route-scroll` in `app/tools/testbed/scenarios/` + `lib/screen.mjs` ("missing or cut off"; limit: a label the
phone ellipsizes inside a wide-enough box is not visible to uiautomator, the step shots check that). Recorded run
`app/tools/testbed/out/runs/2026-11-12T01-10-46/` (kept APK 104e6513c, release r-303b4007a70a): saved-spots 12/12,
two-days 5/5, route-scroll 3/3 (earlier pass on an app holding data: `out/runs/2026-11-12T00-17-43-T603-dev/`); the
blank route map did not reproduce. Shots `../T603-shots/t603-*.jpg`, `b26-*`.
Items 5–6 (2026-11-12): emulator reports as `test` (`apps/mobile/src/emulator.{d.ts,native.ts,web.ts}`, `config.ts`
`TEST_REPORTS`); `SecurityException: Unsupported path /data/local/tmp/…` is `expected` (`workers/feeds/src/crashes.ts`
`EXPECTED`); the worker is NOT deployed (owner 11-03: with the app release, never ahead of it).

## Work (left)
1. Rerun `saved-spots`, `two-days`, `route-scroll` on the next APK. With T674 (04572e8d3, fixed in component tests, not
   yet seen on the emulator): save an alert with Save at the foot of the screen and read the confirmation at rest
   ("Saved. Manage alerts in More → Notifications."), and check the uiautomator dump holds no `spot-mark:` node while the
   spot is off the map. `gauge-offline` now enters by `openWater` (Coquitlam River by default; `GAUGE_WATER` +
   `GAUGE_ROW` name another).
7. Three new emulator scenarios on the next APK: the + / − zoom buttons on a bare phone map (G15), the card a titled pin
   opens (G19), and that a tap where two waters overlap picks the nearest (G4).
8. T621 leftovers, each needs a device first: G5 (the route overlay reads atlas tiles through a web-only range fetch;
   the phone needs a native tile reader, so the route trace is web-only today and listed as an open gap in
   `map-props-gate.test.ts`); G21 (the web's `within` mask on the phone: confirm the patched native MapLibre accepts
   a `within` filter before shipping it, since an unsupported filter fails the whole style; about 0.7 MB per style swap).

Seen on build 26 (2026-11-11, not this ticket's work): sharing a water gave `canifishthis.ca/?regs=...`, not
`/water/<slug>` (the production site lacked `/water-slugs/`; needs the production site deploy, owner); `scripts/fast-fail.sh`
failed on `test_review_gate.py::test_the_live_tree_prints_...` only inside the smoke run (sees `/nonexistent/ci-no-build`
from another test).

## Acceptance
- A test bed run fails if the saved-spot chips, the alert editor wording or the two-days line are missing or cut off.

Depends: T582, T598. Size: S–M. Owner: no.

## Ledger detail (moved out of the ledger row, T540, 2026-11-22)
owner 2026-11-10; scenarios `saved-spots` (UI path, no seeding needed), `two-days`, `route-scroll` + `lib/screen.mjs`; all pass on the kept APK (104e6513c); the blank route map did not reproduce (10/10); `open-result` no longer taps a saved spot's pin
