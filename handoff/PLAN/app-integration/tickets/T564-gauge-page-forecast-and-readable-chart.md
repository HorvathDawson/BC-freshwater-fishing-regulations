# T564 — Public gauge page: the forecast is on it, and the chart is large and readable

Status: OPEN.

Source: Owner 2026-11-09 (owner checks page, T487 + T530): "it shares but forecast data isnt on the gauge link. and the chart is very small and hard to read etc. utility is the most important part."

## Work
1. The gauge page (`/gauge/<station>/<slug>`, app/tools/seo/gauges.ts) shows what the app's Conditions gauge shows: current reading with its age, the forecast days, the percentile band.
2. The chart is the main thing on the page: full width, readable axis text on a phone, same presenter as the app (no second chart implementation).
3. Mockup first (one shot, phone + desktop), then build; the share card follows the same data.

## Acceptance
Owner look at the staging page on the phone. A parity test: the page and the app's gauge panel show the same reading, forecast and band for one station fixture.

Depends: —. Size: M. Owner: no.
