# T123 — One donor state: the Conditions lines and the map label read a donor differently

Status: DONE — 7355fc2e, review b78a9bb0 (app only; ships with the next app build, no data rebuild).

Source: owner 2026-10-22 (unify checks that decide the same thing); unification audit 2026-10-22, finding gauges 6
(CONFIRMED in code; mostly masked today).

## Decision being unified
"What state is this donor gauge in: reads, unranked, silent, no forecast?"

## Where it is decided today
- `app/packages/core/src/conditions.ts:174-236`: `donorReads`, `donorUnranked`, `donorSilent`, `donorWeightLine`.
- `app/packages/core/src/routeMap.ts:31`: `donorMapLabel`.

How they differ: with a horizon, one says "no forecast" and the other says "quiet". It is mostly masked because
`donorSilent` hides those pins.

## Work
- `donorState(donor, horizon)` returning one tagged state; every label is a `switch` over it.
- Pin: an app grep test that no module outside the state function tests a donor's reading fields directly.

## Acceptance
For each state a fixture shows the same word in the Conditions line and the map label. Pure refactor otherwise (app
shots unchanged except the horizon case).

Rebuild: none (app). Depends: none. Size: S. Owner: no.

## As built (7355fc2e)
- **Core** `app/packages/core/src/conditions.ts`: `donorState(r, { ref, horizon })` returns one tagged state:
  `reads` (with its percentile), `not-realtime`, `no-forecast`, `unranked` or `quiet`. The order is the ruling. A
  reading wins. "Not real-time" holds at every horizon. A horizon with no forecast comes before anything about today's
  reports. `unranked` means a report within a day of `ref`. This is the only code that tests `percentile`, `realtime`
  or the within-a-day `observedAt` parse. `donorCounted(r)` asks whether the state is `reads`. Types: `DonorState`,
  `DonorReading`, `DonorAsk`.
- **Every label is a `switch` over it**, exhaustive (`never`):
  - `donorReads`, `donorSilent` and `donorRowLabel` in conditions.ts.
  - `donorMapLabel` (and so `donorMapKey`) in `routeMap.ts`.
  - `donorUnranked` is `kind === "unranked"`.
  - `donorShare`, `donorWeightLine`, `gaugeChoiceLabel`, the silent line's member, and the short-record, dam and tide
    notes go through `donorCounted` or the state.
- **The horizon case**: on a day ahead or under Change, a donor with no forecast is drawn on the map as
  **`no forecast`** (new `ROUTE_MAP_WORDS.noForecast`); it used to say `not reporting`. Its screen-reader row label
  now says `no forecast for this gauge, so it counts for nothing`, not `not reporting today, …`. Both are masked
  today, because `donorSilent` hides those rows and pins. No app shot changes.
- **Outside core**: `ui/panel.ts` (`quiet`, `lastReport`, `because` on rows and gauge choices, `lastReportWanted`)
  and `data/standing.ts` `sheetLead` ask `donorCounted`. They no longer test `percentile === null`.
- **Pin** `app/tools/one-donor-state.test.ts`:
  1. One fixture per state (7 rows, both horizon kinds included). Each row is checked as whole values: the row's
     reading line (or its weight line when it counts), the map label, the map key, the silent flag and the
     screen-reader label. The Conditions line and the map label must contain the same word.
  2. The decision itself, checked whole, including its order.
  3. A grep over apps and packages (tests excluded): no module tests `.percentile` against null, `.realtime` against a
     boolean, or `.observedAt ? Date.parse`, except inside `donorState`'s own body. The two declared exceptions are a
     saved spot's own reading (`spots/words.ts`, `SpotScreen.tsx`), which is not a donor. They are counted by file.
  4. Each pattern is shown to catch the spelling it replaced.
- **Mutations** (each was run, failed the named test, and was reverted):
  - `donorMapLabel`'s `no-forecast` case returning `ROUTE_MAP_WORDS.quiet` fails pin 1 and `routeMap.test` "a day
    ahead with no forecast says no forecast".
  - Moving the horizon check above `realtime === false` in `donorState` fails pins 1 and 2.
  - Putting `if (r.percentile !== null) return donorShare(r);` back in `donorMapLabel` fails pin 3.
  - Putting `quiet: r.percentile === null` back in `ui/panel.ts` fails pin 3.
- **Checks**: see T124. One `REGS_STORE_REQUIRED=1 pnpm -C app check` covered both lanes.

## Adversarial review (2026-10-23, b78a9bb0)
- **Call sites re-walked.** Every donor label (`donorReads`, `donorRowLabel`, `donorWeightLine`, `donorSilent`,
  `silentLine`, `donorMapLabel`/`donorMapKey`, ui `panel.ts` rows, pins, key and gauge choices, data `sheetLead`) reads
  `donorState`/`donorCounted`. The callers pass the same `{ ref, horizon }` to the row and to the map label (panel.ts
  pins, key, rows), so the two cannot differ by ask. `donorLastReport`/`readingAgeLine` still parse `observedAt`, but
  only to format a time. They decide no state. `gaugeGeoJSON`'s `percentile !== null` is a station dot, not a donor.
- **Pin strength.** The `percentile` pattern only caught a comparison against `null`.
  `typeof r.percentile !== "number"` is the same test and passed the grep. Added the `percentileType` pattern and its
  spelling proof. Mutation: that spelling in ui `panel.ts` gauge choices fails test 3, then reverted.
- **Mutations re-run.** The builder's list holds: map `no-forecast` → `quiet` fails pin 1 and the routeMap test.
- No behaviour defect found in T123.
- **Checks.** The pins, `packages/core`, `packages/ui/src`, `packages/map` and `NativeAgrees` all pass (144 files,
  1718 tests). `tsc -b` is clean. Both ran under heavy.sh.

## Owner questions
- The map word for a donor with no forecast at the horizon is `no forecast`, for both a day ahead and Change. The
  row line keeps its two sentences: "No forecast for this gauge that far ahead." and "No forecast running for this
  gauge now.". Default built: the one short word on the dot.

## Left / follow-ups
- None. `donorShare` still says `quiet` for any donor that does not count. It is used in pin titles, key labels and
  the gauge switcher, which are not horizon-aware. Silent donors never reach those pins. The gauge switcher lists
  every member and says `quiet`, which is unchanged.
