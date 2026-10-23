# T106 — One shown quantity: the headline infers discharge vs level instead of reading the declared one

Status: OPEN.

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
