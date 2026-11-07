# T487 — Share a gauge's conditions as a public page of its own (future)

Status: OPEN — FUTURE, not for the next chat. Owner 2026-11-07 (scope answered the same day).

Owner: a way to share the conditions for a river that creates a new page — a nice viewer of them, nicely shared, with a
link back to the app.

## Decided (owner 2026-11-07)
- **One page per gauge station** (a gauge can serve several rivers/parts; the page names the waters it serves).
  Shared from the Conditions tab's gauge.
- **Live:** the link always shows current conditions when opened. No per-share snapshot, no storage per share.
- **Shows:** the gauge reading + trend (**last 7 days + the flow forecast** where one exists, as on the Conditions tab)
  and the **weather + forecast** at the gauge. Each sourced and timed.
- **Regulations: a link only** — one line to the water's regulations page (the SEO water page). No open/closed status
  on this page or its preview.
- **Public + indexable**, like the SEO water pages (T269/T304): can bring new users in; sitemap entries.
- **Link preview: a generated card per gauge** — name, the reading, a mini 7-day chart; rendered on our worker
  (self-host rule), cached briefly (it is live).
- **Back to the app:** "Open in the app" to that gauge/water if installed, else the store / web app.

## Open (decide when picked up)
- URL shape (`/gauge/<station-id>/<slug>`?) and how it relates to the SEO water pages' URLs.
- Card image renderer on the worker (satori/resvg-wasm or similar) and its cache time vs gauge refresh (gauges < 3 h).
- A gauge with no forecast / a stale reading: shown as such, never hidden (no-silent-fallbacks).
