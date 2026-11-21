# T989 — Opened from a Google result, "back" goes to the default map instead of the water

Status: OPEN — joins the 0.8.5 batch (owner 2026-11-21).

Source: owner 2026-11-21: "when a user opens a water body page from Google itself it is on regulation page but if they go
back at top right it goes to default location instead of the location of the water body they were viewing".

## Work
- Reproduce on the website: open a water's page by its own URL in a fresh tab (no history, as from a search result),
  then the top-right back/close. Today the map opens at the default place.
- Fix: leaving a page entered by a deep link lands on the map at that water (its extent / the part linked), as if the
  reader had opened it from the map. One load path (owner 11-03): the same place whatever the way in. Check the phone
  app's link entry the same way.
- Pin with a test: deep link → back → the camera is on the water.

## Acceptance
From a cold deep link, back shows the map at the water, on web and in the app.
