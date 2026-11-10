# T487 — Share a gauge's conditions as a public page of its own (future)

Status: IN REVIEW — df77c117f, 1554faf6d, 5f8eece82, on staging; owner: OWNER-TODO "Look at" (a staging /gauge page + a shared link's preview).
Earlier: IN REVIEW — built + on staging (df77c117f, 1554faf6d, 5f8eece82); proof pending: owner look at a page + a shared link's preview. Was OPEN — owner 2026-11-08: do it now (after T497). Owner 2026-11-07 (scope answered the same day).

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

## Owner 2026-11-08
- **Preview cards must not cost much worker compute.** Default: no rendering per request. The cards are drawn when
  the gauge data refresh (the feeds job that already runs), stored as small PNGs on R2, and served as static files.
  The worker only maps the URL to the file. A gauge whose card is missing shows a fixed site card, reported (no silent
  fallback).
- URL default: `/gauge/<station-id>/<slug>`, in the sitemap beside the water pages.

## Built (2026-11-08, df77c117f, 1554faf6d, 5f8eece82)
- **Pages** (`app/tools/seo/gauges.ts`): `/gauge/<station>/<slug>` for every station serving a water with a page (own water,
  lake_gauge, section_gauge good/fair; weak ones in a "rough guide" fold) — 450 on r-25a89ce9f8e6 — plus `/gauges`, all in the
  sitemap with the water pages. One regulations link per water, no open/closed word; "Open in the app" = the web app on the
  first water's Conditions view (`/water/<slug>?view=conditions`; the phone app takes no links, D1). `/gauge/<station>` and an
  old slug 301 to the page.
- **Live half** (`gaugeClient.ts`, one plain script): reads `feeds/cards/<station>.json` (the facts the card was drawn from:
  core's words, the reading's instant, the 7 days, the forecast run + its working-day line) and Open-Meteo at the gauge
  (rounded, `weatherPlace`). Stale reading, no forecast, an out-of-date run: each said in words. A read failure is shown and
  reported as crash/2 (offline: shown only). No new upstream fetch by our servers.
- **Cards** (`app/workers/feeds/src/gauges/card/`): drawn in the gauge run, never per request — a palette rasteriser with
  Archivo glyphs pre-drawn (`tools/card_glyphs.py`), ~8 ms and ~20 KB a card, redrawn only when the reading/standing/forecast/
  stale/day moved. The list is the site's `/gauge/stations.json` (`SITE_ORIGIN` var). The site worker only maps
  `/gauge/<station>/card.png` to R2 (binding CARDS); a missing card → `/share/gauge.png` (`x-card: site`) + a
  `private/cards-missing/` marker the gauge run puts in the digest (kind "card"), as are a run that could not draw and a card
  that failed.
- Staging: site deployed (version cecb1b22, pages built on r-25a89ce9f8e6), feeds worker deployed (2a8376a1) and run: e.g.
  https://staging.canifishthis.ca/gauge/08MH001/chilliwack-river-at-vedder-crossing and its card `/gauge/08MH001/card.png`.
- Production: `deploy/prod/site.wrangler.toml` now binds CARDS (bucket bc-fishing-regulations) and runs the worker first for
  `/gauge/*`; `wrangler.prod.toml` sets SITE_ORIGIN. Owner-run, untouched.
- Left: 5f8eece82 (the chart's unit label moved off the top figure) ships with the next site build; owner look.
