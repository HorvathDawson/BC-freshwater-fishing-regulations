# T105 — One gauge speaker: the map and the sheet ask different stations

Status: DONE — 4817cb72. App-only: `section_gauge` is unchanged, no deliver needed.

Source: owner 2026-10-22 (unify checks that decide the same thing); unification audit 2026-10-22, findings gauges 1 and
12 (CONFIRMED on `bundle.sqlite`).

## Decision being unified
"Which gauge speaks for this section?"

## Where it is decided today
- Map: `app/packages/data/src/standing.ts:59` `sectionStanding`: the panel, then the lake station.
- Sheet: `app/packages/ui/src/panel.ts:726-736`: panel rows, then `matched?.station` from `section_gauge`, with no lake
  path.
- Pipeline: `section_gauge` comes from `pipeline/gauges/consume/shed.py` `trust_for`, which has no regulation or
  record gate. The panel's gate is `pipeline/atlas/gauges/panel.py:421` `eligible`.

How they differ: 54,401 sections have a `section_gauge` row and no `section_panel` row. The map leaves them uncoloured
while the sheet quotes a station for them. Gauged lakes show a colour on the map but no reading on the sheet.

## Work
- One `speakerFor(section)` in `@app/data` `standing.ts`, used by both the map standing and the sheet.
- Pipeline: derive `section_gauge` from panel member 0, or stop using it as a reading source (it stays a match record).
  Coordinate with T93 (the gauge ↔ section relationship policy) and do not change the drainage gate (memory
  `gauge-representativeness-is-policy`).
- Pin: an app test (one-* style) that `section_gauge` / `matched.station` is read for a reading only inside
  `speakerFor`.

## Acceptance
Proof is a reviewed behaviour change. Report the count of sections whose sheet station changes and the gauged lakes
that start showing a reading. Map and sheet agree on the speaker for a sample over every region (test).

Rebuild: app, plus deliver if `section_gauge` changes. Depends: T93 (policy, default as built). Size: M. Owner: no.

## As built (2026-10-23)

**Choice: stop using `section_gauge` as a reading source** (the ticket's second option). It stays the match record
(the trace). No pipeline change, `section_gauge` byte-identical, T93's default (today's drainage gate) untouched.
**Ships with an app rebuild only; no deliver.**

- `@app/data` `standing.ts`: `speakerFor(panel, lake)` is the one decision — its panel, else the station in its lake,
  else null. `Speaker` is the type (`StandingClass` is now an alias of it). `speaks(speaker, station)` and
  `sheetLead(speaker, rows, picked)` (the sheet's station: a pick the speaker holds, the heaviest reporting donor, any
  donor, the panel's first member when offline, a lake's own station, else null) are built on it.
  `sectionStanding` → `speakerStanding(speakerFor(...))`; `classStanding` and `buildStandingClasses` (panel and lake
  classes, the "panel wins" guard) all go through `speakerFor`.
- `@app/ui` `panel.ts`: `useSpeaker(source, section)` (the same cached `panelsFor` the map's viewport path reads, plus
  `lakeStationsFor`; a lake station's name from `gaugePoints`). `useConditionsView` leads with `sheetLead`, reads the
  reading through `useStationReading` only, shows the trace only when the matched station `speaks`, and `loading` is
  the speaker's. `useConditions` (the matched-station reading) is deleted.
- Removed the third speaker: `RegsSource.stationsFor` (`section_gauge` first, then the lake) and its bundle
  (`gaugesForSections`), release (`SECTION_STATIONS`) and fixture implementations — no caller outside tests.

**Behaviour change, measured on `data/generated/bundle/bundle.sqlite` (current vintage):**
- **54,386 sections** (`section_gauge` row, no `section_panel` row, no lake station) stop quoting a station on the
  sheet: lead null, refusal, no trace — as the map leaves them uncoloured. 368 distinct stations were quoted that way.
  (The ticket's 54,401 was an older vintage.) No such section had a lake station, so none switches station.
- **150 gauged lakes** (150 sections, one per lake; 173 `lake_gauge` rows) start showing their own station's reading
  on the sheet, as the map already coloured them. None of them has a panel.
- Sections with a panel: unchanged online. Offline (no feed index), the sheet now leads with the panel's first member
  instead of the matched station. Where the panel cannot answer, the trace is shown only when the matched station is a
  panel member: 48,125 of the 54,044 panelled sections with a `section_gauge` row; the other 5,919 no longer show a
  trace to a gauge the panel does not use.

**Tests (whole values; mutations each fails, all run):**
- `packages/data/src/standing.test.ts` — `speakerFor`/`speaks`/`sheetLead`/class build. Kills: lake-before-panel in
  `speakerFor`; `sheetLead` keeping any pick; dropping the offline member-0 fallback; the class build skipping the
  "lake only" guard.
- `packages/ui/src/conditions.render.test.tsx` — new: matched-only section refuses with no `gaugeNow` call and no trace
  (kills re-adding the matched fallback and an ungated trace); a gauged lake reads its own station with its name; a
  pick outside the panel is ignored; offline leads with member 0 and traces only a member's match.
- `tools/one-speaker.test.ts` (pin) — only `standing.ts` builds a speaker; no `matched?.station`; `gaugeForSection` is
  called only by `ui/hooks.ts` (trace) and `data/spots/capture.ts` (follow-up below); no `stationsFor`/`useConditions`.
  Each pattern is proven against the spelling it replaced.
- `conformance/src/release.test.ts` — every section of the fixture release, both readers: the map's class equals the
  sheet's `speakerFor`, and the sheet's lead is the speaker's (panel, lake and none all occur).
- `conformance/src/speaker.test.ts` — the province bundle (skips when absent), a sample of 15 sections per region (9
  single-region places) × kind (panel / gauged lake / match only / nothing): map class == sheet speaker, lead is the
  speaker's, and a match alone is no speaker.
- `tools/water-frame-{layout,swipe}.test.ts` — the design fixture has no panel, so its Chilliwack section (match only)
  now refuses with no chart; these T44 layout tests now give the fixture's Chilliwack a one-member panel (Vedder
  Crossing) so the Conditions body still draws the chart they measure.

## Left / follow-ups
- Spot capture (`packages/data/src/spots/capture.ts` `readingFor`, `leadUpOf`) still freezes the `section_gauge`
  station's reading into a saved spot. It is a stored record (format/migration question), so it was left; the pin
  names it as the one allowed reader. Follow-up: freeze the speaker's lead (`sheetLead`) instead.
- The design fixture (`packages/data/src/fixture.ts`) has no panel, so under one speaker its Conditions view is the
  refusal everywhere. If design screens need a chart, give the fixture's Chilliwack a panel (60 test call sites).
