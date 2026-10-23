# T143 — Conditions +1/+3/+5/+10 say "no forecast" for every gauge (Change works)

Status: DONE — 875ac163 (app only; ships with the next app build. No feed republish, no data rebuild.)

Source: Owner 2026-10-23 (app walk). "when i click on +1, +3, +5, +10 it says no forecast for gauges here even tho there definitely is. this is for
every gauge. but the change option works." Likely since T123 (donorState: a horizon with no percentile → "no-forecast")
or a feed/app format mismatch (T106 declared quantity, T126; the feed republish is still pending on OWNER-TODO).

## Work
Reproduce on the owner's live data path (the feed the app actually reads), find where the horizon percentile goes null
(feed index `ahead`, `reading(row, quantity, horizon)`, the quantity asked, T123's callers), fix at the one function,
and pin with a test that runs on a real feed index (not a fixture that already matches the code). If the app needs a
feed republish, make the app read BOTH shapes until it is published and say so in OWNER-TODO.

## Acceptance
Every horizon chip shows the forecast percentile where the feed has one; a gauge without one says so. Web shot of a
real gauge at +1/+3/+5/+10. Size: S. Priority: high (live regression).

## Reproduction (2026-10-23, on the owner's own data path)
The dev app on :39218 reads `http://localhost:39217/feeds/live/index.json` (`apps/mobile/src/config.ts` ENDPOINTS.feed;
`tools/serve-tiles.mjs` serves `data/generated/gauges/feeds/`) and the release `r-012fb43aa175` from
`/release/manifest.json`. Both were read (GET only; neither server touched) and run through the app's own code
(`makeReleaseSource` + `answerFrom`, every one of the release's 239,037 sections) and through this worktree's own Expo
on :39331 (headless Chromium, not the shared browser).

- **The feed shape is current.** 432 stations; 372 carry `ahead` keyed `"1"/"3"/"5"/"10"`, exactly what `reading` asks
  (`row.ahead[String(day)][quantity]`). No republish is needed and no second shape is read.
- **Under Flow nothing is broken.** At +1 107,339 sections answer, as at Now (107,475); +3/+5/+10 the same order. The
  Chilliwack sheet answered "Very low for the time of year by then" at +1/+3/+5/+10 (shots below).
- **Under Level, the owner's sentence, verbatim.** +1/+3/+5 answered "No forecast for the gauges here reaches this far
  ahead." on 3,859 of a 4,000-section sample (82 answered); +10 answered as Now. Cause: those days are CLEVER's, and
  CLEVER forecasts DISCHARGE only (`publish.forecast_percentiles`; its station series' `level_mid` is all null), so 310 of
  the 372 forecast stations have no level percentile at +1/+3/+5. Meanwhile the chart under the refusal draws CLEVER's
  forecast — "even tho there definitely is". The Layers choice is KEPT (`layerChoice.ts`), so a reader who once picked
  Level (the chart's own Flow/Level control sets it too) sees this at every gauge. "Change works" fits: under Level the
  Change chip is "Flow only", and tapping it moves to Flow (T130), where everything answers.
- I could not read the owner's browser state (not my port, not my browser), so "the owner was on Level" is inferred
  from the sentence, the chip behaviour and the fact that nothing else reproduces it. See the owner question.

## As built (875ac163)
- **One decision, one function**: `silentWhy(panel, index, horizon)` in `app/packages/data/src/panel.ts` — why a panel
  with gauges and an area has nothing to say: `no-record` today; `flow-forecast-only` on a day ahead whose members DO
  forecast that day in flow; `no-forecast` otherwise. `answerFrom`'s no-contribution branch is its only caller. A flow
  still never stands in for a level: the answer stays a refusal.
- **Core**: `NoEstimate` gains `flow-forecast-only` (`core/trust.ts`); `donorReason` says "The forecast for the gauges
  here is of flow, not level, this far ahead. Show Flow to see it."; `refusalIsAboutTheDay(why)` (core/conditions.ts) is
  the one list of refusals that keep the horizon chips.
- **UI** (`ui/src/panel.ts` `donorView`): chips kept for both day refusals; the silent line ("No forecast this far
  ahead: …") is dropped under a flow-only refusal, where it would contradict the reason.
- **Real-feed test**: `app/packages/data/src/liveFeedHorizons.test.ts` over `app/packages/data/src/fixtures/live/` — the
  dev server's `index.json` byte for byte (fetchedAt 2026-10-23T03:50:35Z) and the release's own panel for the Chilliwack
  reach the sheet opened on (section 1235781, r-012fb43aa175, from `panelsFor`). Whole values: the panel at every
  horizon in Flow and Level; every station of the feed as a one-gauge panel, tallied by answer (Level +1/+3/+5: 62 ok,
  310 flow-forecast-only, 60 no-forecast; +10: 359 / 5 / 68); the Fraser at Hope's day-by-day percentiles
  `[0.36, 0.3921, 0.3165, 0.2422, 0.2212]` (each day its own forecast, never today's).
- **Mutation pins** (run 2026-10-23, each restored after):
  - `reading` null for every horizon past 0 → liveFeedHorizons "answers every day ahead in Flow", "reads each day's own
    forecast", the Chilliwack tests, and horizon.test.ts.
  - `reading` reading `row.ahead[q]` instead of by the day's key → the same.
  - `silentWhy` always `no-forecast` (the shipped refusal) → "says the forecast is in flow under Level", "a day nobody
    forecasts is still no forecast; one forecast in flow only says so".
  - `silentWhy` asking the LEVEL contributions → the same two.
  - `silentWhy` dropping its `typeof horizon === "number"` guard → "Change under Level is no forecast" and horizon.test.ts
    "reads the expected change … flow only (T52)".
  - `donorView` keeping chips only for `no-forecast` → DonorPanel.test "under Level at +3 with a flow-only forecast …".
  - `donorView` keeping the silent line under a flow-only refusal → the same DonorPanel test.
- Shots (`../T143-shots/`, this worktree's Expo on :39331 against the owner's :39217 data, desktop 1280×900):
  `chilliwack-flow-1d/3d/5d/10d.png` (Flow: "Very low for the time of year by then" at each), `chilliwack-level-3d.png`
  (Level: the new refusal, chips kept), `chilliwack-level-10d.png` (Level +10: ELF's level answers).

## Owner questions (default built)
1. **Were you on Level when you saw it?** Default built: assumed yes — it is the only state that reproduces the sentence
   on today's feed and code. If you were on Flow, tell me the water and the time; Flow answered at every horizon here.
2. **Should Level borrow nothing at +1/+3/+5, or should the publisher rank a stage forecast for CLEVER stations through
   a rating curve?** Default built: nothing borrowed (a flow never stands in for a level); the refusal says where the
   forecast is.

## Rebuild / republish
None. App code only — ships with the next app build (the owner's :39218 picks it up on reload once merged).

## Left / follow-ups
- The chart's gauge switcher still calls every member "quiet" under a flow-only day (`gaugeChoices` reads
  `donorCounted`); and each row's own words under Level say "No forecast for this gauge that far ahead". Both are the
  per-donor state (T123's `donorState`), which knows no quantity; giving it one is a separate change.
- The Conditions MAP under Level at +1/+3/+5 is grey on most water for the same reason, with no word on the map saying
  the forecast is in flow. A legend line (or offering Flow) is the natural place.
- A "Show Flow" action beside the refusal (the chart's Flow control is further down the sheet).
