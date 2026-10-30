# T294 — A spot holds several visits, each rated; the coming days are predicted from your rated visits

Status: DONE 2026-10-29, ef431b717 (owner 2026-10-29: "add a way to update a spot with additional visits and you can rank the visit then we can
use that to instead predict how good the day will be").

## Work
1. **Visits:** a spot keeps a list of visits (date/time, conditions captured as today for the first visit: flow, weather,
   lead-up rain; notes; photos). "Add a visit" on the spot page records a new one now (or for a past date, filled in from the
   stored history where available). Existing spots migrate to one visit, losslessly (on-device storage; test the migration).
2. **Rating:** each visit gets a simple rating (e.g. 1–5 or poor/ok/good/great) — owner picks the scale; default 1–5 stars.
3. **Prediction:** "The next few days" scores each day against EVERY rated visit with the T284 similarity (flow + lead-up
   rain, then cloud + pressure, then air + wind) and predicts how good the day will be from the ratings of the most similar
   visits (similarity-weighted; say how many visits it rests on and which visit it is most like). With one unrated visit it
   falls back to today's "how similar to your visit". Plain reasons ("like your 5★ visit on Oct 12: flow within 10%").
   Never claims fish; it's "how your rated visits compare".
4. Spots stay on the device (privacy page unchanged); export/import if one already exists keeps the new fields.

## Acceptance
Tests: migration, prediction math (weights, ties, one visit, no rating), words; phone + desktop screenshots with 3 rated visits.

Depends: T284. Size: M. Owner: rating scale — DECIDED 2026-10-29: 1–5 stars (approved).

## Done
- **Model, lossless by construction** (`data/spots/visits.ts`): a spot's own fields ARE its first visit, `rating?` is that
  visit's rating, `visits?: Visit[]` the later ones (same frozen record + own notes/photos/rating). Nothing is moved on
  disk, so an old spot loads as one visit byte-for-byte and saves back unchanged (test). `visitSpot` reads any visit as a
  spot (all existing renderers/refresh/day-flow reuse), `withVisit` writes it back to that visit alone; `addVisit`,
  `rateVisit`, `removeVisit` (removing the first promotes the next; the last cannot be removed — delete the spot),
  `allPhotos` (the store deletes a removed visit's photos). `fromDisk` cleans later visits like the first (no section ids).
- **Add a visit**: `captureVisit` reads the water through what the spot froze (panel members via `panelOfAnswer`, lead
  station, trace) — live today, that day's record for an earlier day (Q161 path), offline pending → refresh. Page = the
  same day/hour steps as adding a spot (`WhenScreen`, "When was this visit?"). `refreshSpot`/`needsRefresh` cover every visit.
- **Rating**: core `RATING` = 1–5, words Poor/Fair/OK/Good/Great (owner may change; stored as the number). Stars under
  the visit row, one press saves, pressing the lit rating clears it.
- **Prediction** (core `spotVisits.ts`): each coming day × each rated visit through T284 `likeDay` at that visit's hour;
  weight = (likeness)^3, prediction = Σw·rating/Σw ("4.2★ Good"); says how many visits it rests on, the visit it is most
  like ("Most like your 5★ visit on 18 Sep (68% alike)") + that visit's plain reasons; "None of your rated visits looks
  much like this day" below 65%; tie → the more recent visit; unrated visits left out (said). Nothing rated → T284
  similarity to the shown visit + "Rate your visits to see…". Intro ends "not a promise of fish".
- Words: `VISIT_WORDS`, `VISIT_CONFIRM_WORDS`, `PREDICT_WORDS`, `RATING` (core). Existing components (Chip, Button,
  Block/Link, Confirm, LikeVisitStrip). No spot export/import exists, so item 4 is moot.
- Tests: `core/spotVisits.test.ts`, `data/spots/visits.test.ts` (migration, store photos, refresh, captureVisit),
  `ui/visits.test.ts` (3 rated visits whole, one visit, none rated); a11y gate state added (phone + desktop, 44 px).
- Shots (web, headless Chrome via `tools/regs-web/cdp.mjs`, 390 / 1280, 3 rated visits): `T294-shots/`
  (`node tools/spot-visits-shots.mjs <outDir>`).
