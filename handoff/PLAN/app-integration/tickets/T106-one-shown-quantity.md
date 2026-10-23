# T106 — One shown quantity: the headline infers discharge vs level instead of reading the declared one

Status: DONE — 17e9d43d (app; plus a one-line publisher fix that lands on the next feed publish, see As built).

Source: owner 2026-10-22 (unify checks that decide the same thing); unification audit 2026-10-22, finding gauges 2 =
cross 4 (CONFIRMED on `data/generated/gauges/feeds/08ME030.json`).

## Decision being unified
"Which quantity does a station's reading show: discharge or level?"

## Where it is decided today
- `app/packages/ui/src/panel.ts:741` uses `shown = c.discharge != null ? "discharge" : "level"`, which ignores the
  declared `parameter`.
- `pipeline/gauges/feed/publish.py:754` sets `parameter`. This is the authority.
- `app/packages/data/src/spots/words.ts:87,265` and `app/packages/data/src/feed/http.ts:315` also infer it.

How they differ: `08ME030.json` has `now = {discharge: 5.46, level: 4.162, percentile: 0.432, parameter: "level"}`, so
the sheet prints "5.46 m³/s" next to the level percentile. 357 stations carry both quantities.

## Work
- Core `shownParameter(row)` / `readingIn(row, quantity)` that reads the declared `parameter` and never infers. All
  four places call it.
- Pin: an app grep test that no module outside core tests `discharge != null` / `level != null` to pick a quantity.

## Acceptance
On 08ME030 the sheet shows the level value with the level percentile (test on the fixture). Proof: the 357 two-quantity
stations each show the declared quantity (scripted check over the feed files).

Rebuild: none (app only). Depends: none. Size: S. Owner: no.

## As built (17e9d43d)
- **Core** `app/packages/core/src/conditions.ts`: `shownParameter(row)` returns the declared `parameter`, or
  `"discharge"` when nothing is declared (an old feed file, a spot saved before T106: the publisher's own default).
  It never looks at which value is present. `readingIn(row, q)` returns that quantity's value or null, and never the
  other quantity's. Types: `ShownQuantity`, `DeclaresQuantity`.
- **Carried**: `Reading.parameter` (data, `Parameter | null`, set by `httpFeed.now` from `now.parameter`), and
  `Conditions.parameter` (ui hooks: `useConditions` and `stationReading`). `SpotReading.parameter?` is frozen at
  capture.
- **Callers, all through core**: the Conditions headline (`ui/panel.ts`: the figure, unit and quantity come from the
  declared quantity; the stage line only goes beside a discharge); `httpFeed.observations().parameter`; the map
  label and index (`ui/gaugePoints.ts`); `data/panel.ts` `reading`; `gaugeSeries` in the bundle and release sources;
  the spot list row (`spotRowLines`) and the old-spot reading on `SpotScreen`, both via a new
  `spotReadingFigure`; the lead-up strip (`leadUpQuantity(days, declared)`, and `leadUpView(lu, spot.reading)`);
  and `visitConditions` (the like-visit flow).
- **Publisher** `pipeline/gauges/feed/publish.py` `shown_parameter`. This is the authority, pulled out into one
  function. Changed in one case: where no envelope can rank either quantity, it declares the quantity the station
  reported. 9 level-only stations with no record declared `"discharge"` with no discharge, which would have left the
  headline with nothing to show. They show nothing in the headline until the next feed publish rewrites them.
- **Pin** `app/tools/one-shown-quantity.test.ts`: no module outside `packages/core` (apps + packages, not tests)
  tests `discharge`/`level` for null to choose a quantity (ternary or early return), falls back from one quantity's
  value to the other's (`discharge ?? level`), or writes the undeclared default again (`.parameter ?? "discharge"`).
  Each pattern is shown to catch the spelling it replaced.
- **Tests**: `conditions.render.test.tsx` (08ME030's `now` gives the whole headline `4.162 m`, level, no stage,
  level standing; declared discharge gives `5.46 m³/s` with the stage), `http.test.ts` (whole `Reading` for
  08ME030; an undeclared file), `words.test.ts` (five whole row strings), `leadupWords.test.ts`,
  `likeVisit.test.ts`, `pipeline/tests/test_feed_shown_parameter.py`.
- **Mutations**:
  - Putting `c.discharge != null ? "discharge" : "level"` back in `ui/panel.ts` fails two tests: the pin
    (`ui/panel.ts: ternary`) and the 08ME030 render test.
  - Changing `shown_parameter`'s last line back to `else "discharge"` fails `test_no_envelope_declares_what_it_reported`.
  - Swapping its first two checks fails `test_discharge_first_where_both_rank`.
- **Proof over the feed files**: a scratch vitest ran `httpFeed.now`, then `stationReading`, then
  `shownParameter`/`readingIn` over `data/generated/gauges/feeds/*.json` as of 2026-10-23. Of 435 station files,
  363 carry both quantities (the ticket said 357). All 363 show the declared quantity and its value. One (08ME030,
  declared level) would have shown the wrong quantity under the old inference.
- **Checks**:
  - `REGS_STORE_REQUIRED=1 pnpm -C app check` (heavy.sh): typecheck clean and 7216 passed. 6 failed:
    - 5 are store-pairing failures that do not touch this change: `regs.sqlite` store 43115d8c is not the store
      the oracles were cut from, a230776; the fix is to re-run `app/design/regs-v11/port/taps.py`.
    - 1 was a `palette.render` 5 s timeout, which passes when run alone (186/186).
  - Feed pytest: 44 passed.

## Owner questions
- An undeclared record reads as discharge, the publisher's default. Default built: yes. The effect is on spots saved
  before T106 that hold both quantities: the like-visit flow now takes their percentile as a flow, where before it
  refused it. A spot holding only a level is still never a flow.
- No envelope for either quantity: the publisher declares the quantity the station reported (`level` for the 9
  level-only stations). Default built: yes.

## Left / follow-ups
- The 9 level-only stations with no envelope need one feed publish (the routine feed run, not a rebuild) before
  their files declare `level`. Until then their Conditions headline shows no figure.
- `ui-native` `ConditionsPanel` still uses `parameter ?? "discharge"` for the map's painted quantity. That is the
  map's chosen quantity, not a station's shown one, so it was left alone and the pin does not match it.
