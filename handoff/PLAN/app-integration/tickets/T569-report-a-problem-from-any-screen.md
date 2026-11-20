# T569 — Report a problem from the screen you are on (Conditions has no way in)

Status: IN PROGRESS — 774befd64 (a "Report a problem with this water" link on a water's Conditions and Map views opens the form prefilled: water, place, part, tab, day, `station`, `quantity`, `span`, `feed`). The feeds worker with the new report/2 keys is deployed (staging 2026-11-12, production 2026-11-18). left (Claude): the review app's Reports tab does not show station / quantity / span / feed (ReportsView.tsx context type lacks them), which the acceptance needs.

Source: Owner 2026-11-09: "I sent a report after navigating away from the conditions pane to the about tab because there
is no report an issue button on the conditions tab. You need to add that too, it is a bug."

## Work
- A "Report a problem" entry on a water's sheet (Regulations / Conditions / Map), which opens the existing report form
  with the context filled in: the water / section, the tab, the gauge charted, the quantity and span picked, the day,
  the release + feed versions. Today a report from About carries none of where the owner was.
- Check what the report already sends (app/workers/feeds `POST /report`, T237) and add the missing fields.

Acceptance: a report sent from Conditions names the water, the gauge and the quantity in the review app's Reports tab.

Depends: —. Size: S. Owner: no.
