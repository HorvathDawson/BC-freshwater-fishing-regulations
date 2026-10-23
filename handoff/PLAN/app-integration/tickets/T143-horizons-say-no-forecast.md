# T143 — Conditions +1/+3/+5/+10 say "no forecast" for every gauge (Change works)

Status: DONE — 8d3de7e0, 92d47b1c (test move), 3d17b1df (docs); adversarial review: see "Adversarial review" (publisher
fix + app wording, 90edf4ae). App part ships with the next app build; the full fix needs a FEED REPUBLISH (owner, `publish.py`).

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

## As built (8d3de7e0)
- **One decision, one function**: `silentWhy(panel, index, horizon)` in `app/packages/data/src/panel.ts` — why a panel
  with gauges and an area has nothing to say: `no-record` today; `flow-forecast-only` on a day ahead whose members DO
  forecast that day in flow; `no-forecast` otherwise. `answerFrom`'s no-contribution branch is its only caller. A flow
  still never stands in for a level: the answer stays a refusal.
- **Core**: `NoEstimate` gains `flow-forecast-only` (`core/trust.ts`); `donorReason` says "The forecast for the gauges
  here is of flow, not level, this far ahead. Show Flow to see it."; `refusalIsAboutTheDay(why)` (core/conditions.ts) is
  the one list of refusals that keep the horizon chips.
- **UI** (`ui/src/panel.ts` `donorView`): chips kept for both day refusals; the silent line ("No forecast this far
  ahead: …") is dropped under a flow-only refusal, where it would contradict the reason.
- **Real-feed test**: `app/conformance/src/liveFeedHorizons.test.ts` over `app/conformance/src/fixtures/live/` (conformance, the package that may read files — `packages/data` may not import `node:fs`) — the
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

## Checks (2026-10-23)
`REGS_STORE_REQUIRED=1 pnpm -C app check` via `scripts/heavy.sh`: every gate through typecheck clean; tests 7,299 passed,
5 failed — all five the regs-store parity files (`conformance/src/regs/{blob,store}.release`, `card/settle.parity`,
`inputs.release`), failing because the main checkout's `data/generated/bundle/regs.sqlite` is store f977e8646d7ddcd8
while the committed oracles were cut from db98ffb0563359a2 ("re-run app/design/regs-v11/port/taps.py"). A data-vintage
mismatch, untouched by this ticket. Mutation pins above run against the four files named there.

## Owner questions (default built)
1. **Were you on Level when you saw it?** Default built: assumed yes — it is the only state that reproduces the sentence
   on today's feed and code. If you were on Flow, tell me the water and the time; Flow answered at every horizon here.
2. **Should Level borrow nothing at +1/+3/+5, or should the publisher rank a stage forecast for CLEVER stations through
   a rating curve?** Default built: nothing borrowed (a flow never stands in for a level); the refusal says where the
   forecast is. (Review: moot for most stations — ELF's own stage forecast is now ranked, see Q3; 13 CLEVER-only remain.)
3. **Level at +1/+3/+5 now ranks ELF's stage while Flow ranks CLEVER's discharge — two runs for one day, each
   attributed (`model`, `levelModel`). OK?** Default built: yes. The alternative is no level on those days, which is
   the behaviour you reported.

## Rebuild / republish
- App: next app build (the owner's :39218 picks it up on reload once merged).
- **Feed republish** (owner; `python -m pipeline.gauges.feed.publish`, then the R2 upload): the per-quantity
  `forecast_percentiles` (review) only reaches the app once a feed is published with it. Until then Level at +1/+3/+5
  keeps saying the flow-only refusal. No re-fetch, no data rebuild, no release cut. The app reads both shapes (the new
  `levelModel` key is additive; `reading` never looked at `model`). Add to OWNER-TODO's feed republish line at merge.

## Adversarial review (2026-10-23)
Reproduced on my own Expo (:39343, own headless Chromium, the owner's :39217 data read with GET only; release now
r-d72230f10993). Flow at +1/+3/+5/+10 answers on the Chilliwack and elsewhere; Level at +1/+3 gives the refusal; Level
+10 answers from 3 gauges. Findings:

1. **The root cause was in the publisher, not only in the words (fixed).** The builder's diagnosis "CLEVER forecasts flow
   only, so there is no level at +1/+3/+5" is true of the FEED but not of the FORECASTS: ELF publishes stage for every day
   of its 30 (08MH001 `level_mid` 1.388 m on 10-23, 1.380 on 10-24…; 08MF005 3.92 m…), and the Level chart draws it.
   `publish.forecast_percentiles` broke out of `MODEL_ORDER` as soon as a model answered ANY quantity, so CLEVER's flow
   ended the search and ELF's level was never ranked — against the function's own docstring ("PER QUANTITY"). So the
   shipped refusal "The forecast … is of flow, not level" was itself false at ~297 stations — the owner's "even tho there
   definitely is", again. Fix: the first model that answers, PER QUANTITY; `model` names the discharge's run (else the
   level's) and `levelModel` appears only when the level came from another run, so a one-model day is byte-identical.
   Measured by re-running the function over the same snapshot (each station file's runs, `feeds/clim.json`, now =
   fetchedAt): stations with a ranked level at +1/+3/+5 go from **62 to 359** of 432; flow tallies unchanged (367);
   +10 unchanged (359). Left flow-only: 13 stations at +1/+3/+5 (CLEVER-only, e.g. 08MH016, which ELF does not cover).
   Pinned: `pipeline/tests/test_forecast_percentiles.py::test_each_quantity_takes_the_first_model_that_answers_IT`
   (mutations: the old per-day `break` → fails; `levelModel` never written → fails).
2. **The refusal's words now hold on BOTH feeds.** Reworded to "The gauges here have a flow forecast this far ahead,
   but no level forecast ranked against their record. Show Flow to see it." — true before the republish (ELF's stage
   exists, unranked) and after (the 13 CLEVER-only stations). Shot: `../T143-shots/review-level-3d-reworded.png`.
3. **`silentWhy` ignored the quantity (fixed).** It was exported and correct only under its one caller's precondition:
   `silentWhy(panel, idx, 3)` said `flow-forecast-only` for a Flow question. It now takes the quantity
   (`silentWhy(panel, index, quantity, horizon)`) and only Level can get that reason. Pinned: liveFeedHorizons "is never
   flow-forecast-only under Flow …" (mutation: drop the `quantity === "level"` guard → fails).
4. **The app on the republished feed** (conformance `the Chilliwack panel on the per-quantity feed`, over
   `fixtures/live/ahead-per-quantity-2026-10-23T0350Z.json` — the new publisher's output for the panel's members and
   08MF005, laid over the real index): Level answers +1/+3/+5/+10 at the 5th percentile from 3 gauges; Flow unchanged;
   Hope's level by day `[0.152, 0.1112, 0.05, 0.05]`. Mutation (`reading` dropping a level beside `levelModel`) fails
   both tests. Browser: the same Chilliwack sheet with the per-quantity index served by request interception (my
   browser only) reads "Very low for the time of year by then" under Level at +1/+3/+5,
   `../T143-shots/review-level-3d-per-quantity-feed.png`.
5. Checked and fine: no second list of refusal reasons (`refusalIsAboutTheDay` is the only one; `donorReason`,
   `routeMap`, saved spots read through them); the owner's :39218 runs the dev branch from src (no stale dist), and the
   30 commits the dev branch is ahead of this lane do not touch horizons; the feed shape the app reads is current.
6. Not fixed (follow-ups below): the switcher's "quiet" under a flow-only day; 2 stations (07FC008, 08FB012) have a
   forecast and an expected Change but no record to rank, so every day ahead says "No forecast … reaches" while Change
   answers — a smaller copy of the owner's sentence.

Owner question added: Q3 (two runs for one day under Flow vs Level).

Checks (review, 90edf4ae): `REGS_STORE_REQUIRED=1 pnpm -C app check` via `scripts/heavy.sh` (with a read-only `.venv`
symlink to the main checkout, removed after): typecheck and every gate clean; tests 7,301 passed, 6 failed — the same
five regs-store vintage files as above, plus `tools/one-speaker.test.ts` hitting ENOENT on `_plat.web.ts`, the temp
file `tools/check-boundaries.test.ts` writes and deletes while the two run in parallel (passes alone, 5/5; untouched
here). Publisher: `pipeline/tests/test_forecast_{percentiles,change,clock,series_carry}.py` 68 passed.

## Left / follow-ups
- 07FC008 / 08FB012 (new roster stations, no HYDAT record): a forecast and a Change exist but no percentile, so days
  ahead say "No forecast for the gauges here reaches this far ahead." A "forecast-unranked" reason (or the same words as
  today's `no-record`) would be honest.
- The chart's gauge switcher still calls every member "quiet" under a flow-only day (`gaugeChoices` reads
  `donorCounted`); and each row's own words under Level say "No forecast for this gauge that far ahead". Both are the
  per-donor state (T123's `donorState`), which knows no quantity; giving it one is a separate change.
- The Conditions MAP under Level at +1/+3/+5 is grey on most water for the same reason, with no word on the map saying
  the forecast is in flow. A legend line (or offering Flow) is the natural place.
- A "Show Flow" action beside the refusal (the chart's Flow control is further down the sheet).
