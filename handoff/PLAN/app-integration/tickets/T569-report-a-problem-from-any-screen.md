# T569 — Report a problem from the screen you are on (Conditions has no way in)

Status: IN REVIEW — 774befd64 (A "Report a problem with this water" link on a water's Conditions and Map views opens the existing form prefilled: water item id, place, part, tab, day, gauge charted (`station`), `quantity`, `span`, `feed` (station file fetch stamp); section handle cleared (AGENTS 5). New report/2 context keys: station, quantity, span, feed. Worker needs a DEPLOY before an app build sends them (unknown keys are refused). Review app Reports tab must show those four fields (not touched here). Shots ../T583-shots/T569-*.)

Source: Owner 2026-11-09: "I sent a report after navigating away from the conditions pane to the about tab because there
is no report an issue button on the conditions tab. You need to add that too, it is a bug."

## Work
- A "Report a problem" entry on a water's sheet (Regulations / Conditions / Map), which opens the existing report form
  with the context filled in: the water / section, the tab, the gauge charted, the quantity and span picked, the day,
  the release + feed versions. Today a report from About carries none of where the owner was.
- Check what the report already sends (app/workers/feeds `POST /report`, T237) and add the missing fields.

Acceptance: a report sent from Conditions names the water, the gauge and the quantity in the review app's Reports tab.

Depends: —. Size: S. Owner: no.
