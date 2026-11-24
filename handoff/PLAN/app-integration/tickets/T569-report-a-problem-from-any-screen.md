# T569 — Report a problem from the screen you are on (Conditions has no way in)

Status: DONE — 774befd64, a6d57826f; report link + Reports tab detail rows; worker deployed staging + production.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T569 | Report a problem from the screen you are on (Conditions has no way in) | DONE | 774befd64, a6d57826f | — |

Source: Owner 2026-11-09: "I sent a report after navigating away from the conditions pane to the about tab because there
is no report an issue button on the conditions tab. You need to add that too, it is a bug."

## Work
- A "Report a problem" entry on a water's sheet (Regulations / Conditions / Map), which opens the existing report form
  with the context filled in: the water / section, the tab, the gauge charted, the quantity and span picked, the day,
  the release + feed versions. Today a report from About carries none of where the owner was.
- Check what the report already sends (app/workers/feeds `POST /report`, T237) and add the missing fields.

Acceptance: a report sent from Conditions names the water, the gauge and the quantity in the review app's Reports tab.

Depends: —. Size: S. Owner: no.

## L3 2026-11-20: the review app shows them (a6d57826f, c6b11bc33 the shot)
`curation-review/frontend/src/components/ReportsView.tsx`: the detail's Context table gains Gauge station, Quantity, Span and Feed rows (shown only when the report carries them), after Platform. The backend needed nothing: `reports.enrich` returns `context` whole; test `test_t569_the_gauge_keys_of_a_conditions_report_reach_the_tab`. `npm run build` clean. Shot `T569-shots/reports-tab-detail-rows-light.png` (a stub API with two sample reports, one from Conditions; the red read-failure banner is the stub having no other routes). Acceptance met in the tab.
