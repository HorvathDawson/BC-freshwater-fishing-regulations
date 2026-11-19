# T910 — Web first load: Lighthouse performance 46 on staging; make the first visit fast

Status: IN PROGRESS (main chat; lane app/t910-web-speed; each change measured before/after, onto staging as checked).

Owner 2026-11-19 ~07:40 (main chat), a Lighthouse run on staging (mobile): Performance 46, Accessibility 95, Best
Practices 81, SEO 58; FCP 7.1 s, LCP 7.9 s, TBT 530 ms, Speed Index 7.1 s, CLS 0.011; verbatim: "staging got this??"
His findings shots: render-blocking CSS (controls-*.css, maplibre-gl-*.css, ~300 ms); font display (8 TTF faces from
@expo-google-fonts — Bricolage 700/800, Archivo 400/500/600/700/800, JetBrains Mono 500 — no font-display, all in the
critical chain, ~40–55 KiB each); LCP is a font file (no fetchpriority); unused JS 621 KiB (web/index-*.js 1.1 MiB, 537
KiB unused at load); total payload 9.3 MiB on first visit (core sqlite 1.5 MiB, regs-sid 1.2 MiB, water-shared 0.75 MiB,
paint 0.7 MiB, …); a Cloudflare Insights beacon (static.cloudflareinsights.com) injected at the edge.

Notes: SEO 58 on STAGING is by design (robots disallow + X-Robots-Tag noindex, deploy/staging/site.sh); measure SEO on
production. The Cloudflare beacon is the zone's Web Analytics setting (owner's Cloudflare; a third-party script against
"self-host everything") — the owner decides.

## Work
1. Measure: Lighthouse mobile + desktop, clean profile, production vs staging, 3 runs each (median).
2. Cheap wins first, each measured: fonts (woff2, font-display swap, only the faces the first screen draws preloaded,
   the rest lazy; drop unused weights), render-blocking CSS (inline or defer), fetchpriority on the LCP resource.
3. Bigger: what the first visit downloads (only what the first screen needs; the rest after first paint or on demand),
   code splitting of web/index.js. Proposal with numbers before anything that changes data loading lands.

## 2026-11-19 ~07:50: the Cloudflare beacon is on production too; the privacy page says "no analytics"
With a browser user agent, both https://canifishthis.ca/ and staging carry
`static.cloudflareinsights.com/beacon.min.js` (Cloudflare Web Analytics, edge-injected) and `/cdn-cgi/challenge-platform`
(Cloudflare's bot check: the source of Lighthouse's two "deprecated API" warnings). The privacy page says "I use no
advertising or analytics services". Owner asked "this seems fine to have?" — answered: fine for speed; but keep it ⇒
reword the privacy page, or turn it off in Cloudflare (his dashboard). Waiting on his pick (To-Do page row). Source maps
stay unpublished on purpose (crash symbolication keeps them private). The extension rows are his browser's React DevTools.
