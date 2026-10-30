# T294 — A spot holds several visits, each rated; the coming days are predicted from your rated visits

Status: OPEN (owner 2026-10-29: "add a way to update a spot with additional visits and you can rank the visit then we can
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

Depends: T284. Size: M. Owner: rating scale (default 1–5 stars).
