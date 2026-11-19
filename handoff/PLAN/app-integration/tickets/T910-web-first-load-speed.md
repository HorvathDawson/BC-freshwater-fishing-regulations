# T910 — Web first load: Lighthouse performance 46 on staging; make the first visit fast

Status: IN PROGRESS (lane app/t910-web-speed: cheap wins committed, measured; bigger items proposed below; left (Claude): review, then staging).

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

## 1. Measured (lane, 2026-11-19 ~10:15–10:45)

Lighthouse 12.8.2 via `npx lighthouse@12`, headless Chrome, a fresh temp profile per run (no extensions, no stored
data), default throttling (simulated: mobile 150 ms RTT, 1.6 Mbit/s, 4x CPU; desktop preset). 3 runs each, median, the
perf range across runs in the last column. Other lanes' heavy jobs ran on the machine at the same time (noise, both sides).

| run | Perf | A11y | BP | SEO | FCP | LCP | TBT | SI | CLS | bytes | Perf range |
|---|---|---|---|---|---|---|---|---|---|---|---|
| production mobile | 74 | 95 | 82 | 92 | 1.7 s | 1.7 s | 882 ms | 5.2 s | 0.011 | 9.3 MiB | 50–76 |
| staging mobile | 74 | 95 | 82 | 58 | 1.7 s | 1.7 s | 856 ms | 5.5 s | 0.010 | 10.2 MiB | 73–77 |
| production desktop | 98 | 95 | 81 | 91 | 0.4 s | 0.5 s | 23 ms | 1.7 s | 0.006 | 10.3 MiB | 92–98 |
| staging desktop | 93 | 95 | 81 | 54 | 0.4 s | 1.5 s | 54 ms | 1.7 s | 0.006 | 10.4 MiB | 92–98 |

**Staging is not worse than production: same code path, same numbers (not a regression from tonight).** The owner's 46
(FCP 7.1 s) is the bad side of a race both hosts have, one run in three or four: production mobile run 1 scored 50 with
LCP 7.5 s, the other two 74–76 with LCP 1.7 s. Mechanism (network logs): the first paint is the coho's frame 0, a CSS
background image asked for only once the two map stylesheets are in; when the 1.1 MB app script arrives first, its ~1 s of
main thread (4x) runs before anything is painted, and Lighthouse's simulation then puts FCP/LCP after the whole script
(observed FCP 347 ms = DOMContentLoaded, vs 145 ms in the good runs). Desktop staging LCP 1.5 s in 2 of 3 is the same race.
Mobile's steady loss is TBT (~850 ms, 30 % of the score): see the proposal. "Mobile much worse on a re-run" is this
variance plus the IndexedDB/Cache Storage the owner's profile already held.

## 2. Committed (lane branch, measured locally)

Local A/B: the staging export built from the lane, served by a scratch server with the host's behaviour (brotli, `@` in a
path answered 307 as Cloudflare does, 40 ms to first byte, 6 MB/s), data from data-staging; runs interleaved base/after.

| local, latency-emulated | Perf | FCP | LCP | TBT | SI | Perf range |
|---|---|---|---|---|---|---|
| mobile before (5 runs) | 78 | 0.98 s | 0.98 s | 620 ms | 6.5 s | 76–79 |
| mobile after (5 runs) | **81** | **0.79 s** | **0.79 s** | 612 ms | **5.1 s** | 79–82 |
| desktop before (3) | 96 | 0.32 s | 0.32 s | 27 ms | 2.0 s | 96–96 |
| desktop after (3) | **97** | **0.24 s** | **0.24 s** | 35 ms | **1.7 s** | 97–98 |

Earlier batches (same setup): base hit the race 1 in 11 (score 33, FCP 6.4 s / LCP 7.3 s); frame 0 inlined without the
script change still hit it 1 in 5 (LCP 6.7 s); with the script after the first frame, 0 in 16, observed FCP 55–93 ms in
every run (always before the script). Changes (`app/tools/web-shell.mjs`, run after every web export):
- **The first paint never waits for the app's script** (`paintFirst`): the script is preloaded (downloads at once) and
  run after the first frame (`afterPaint`, the water pages' own hand-over since T269, same text, a test holds them
  equal); the two map stylesheets (maplibre-gl, controls) are still preloaded but applied when in (`media=print` →
  `all`, the water pages' form), so they no longer hold the paint. The map is drawn seconds later, long after both land.
- **Frame 0 is in the page** (data: URI, ~10 KB per scheme; no request, no "fetchpriority" needed: the LCP resource is
  inline). The coho is the LCP element; it is removed from the DOM later, which is why Lighthouse shows no LCP element.
- **Faces: WOFF2 from our own `/assets/fonts/`, font-display swap** (`apps/mobile/src/fonts.web.ts` over the generated
  `webFonts.generated.ts`; the phone keeps the TTF, `fonts.native.ts`). Before: 8 × (307 for the `@` in the path, then a
  41–56 KiB TTF) = 365 KiB + 8 redirects on every first visit; after: 8 WOFF2 = 257 KiB, no redirect (same files T409 made,
  lossless). Water pages already drew these WOFF2s and then fetched all 8 TTFs again when the app took over: now one
  cached file for both (the SEO build names the export's file; it no longer writes its own copy). No face dropped: all 8
  are drawn (`tools/font-imports.test.ts` holds named = loaded). No font preload: the app holds its start screen until all
  8 faces are in (T254), so preloading 1–2 buys nothing and would sit in front of the script.
- **`/_expo/static/*` cached a year, immutable** (`_headers`; every name there is content-hashed). Before: `max-age=0,
  must-revalidate`, so every repeat visit revalidated the script and both stylesheets. (Repeat visits only; cold
  Lighthouse runs do not see it.)
- `deploy/staging/site.sh` and `deploy/prod/site.sh` read the script name once (`sort -u`): index.html now names it
  three times (preload + loader).

No visual change: before/after shots at 390 and 1440, light and dark — pixel-identical but for the search box's random
pun (one per opening). Shots: scratchpad `t910/shots/{before,after}-{390,1440}-{light,dark}.png`.
Tests: `pnpm -C app check:fast` green; vitest web-shell, font-imports, seo/webFonts, seo/site, seo/appLinks,
seo/prerender, one-spinner (79 tests) green; `build-seo --fixture --app` against the new export builds (water pages name the export's WOFF2, no script preload, `_headers` has the new rule).

## 3. Bigger: proposal (numbers; no code)

Where the mobile score goes now: TBT ~600–880 ms (30 % weight) and SI 5–7 s. A CPU profile of a first visit (4x CPU,
local, to the start screen gone + 6 s) puts the main thread at ~3.7 s of the app's own script, of which:
fflate gunzip ~600 ms, MapLibre (style evaluation, worker messages, icon canvas) ~560 ms, `core/statusIndex` 81 ms,
regs blob inflate 81 ms, `map/obstacle-icons` 75 ms, salmon/method building ~100 ms, React ~40 ms.

1. **Gunzip off the main thread on the web (biggest, smallest change).** Every release file is `.gz` decoded by fflate
   (`fetchVerified.ts` `ungzip`, `gunzipSync`) in one synchronous task; on the web `DecompressionStream("gzip")` does the
   same natively and off the main thread. A `fetchVerified` platform pair (web: DecompressionStream; phone: fflate,
   Hermes has none), the sha check unchanged. Expected: ~−500 ms main thread at 4x in the load window → TBT −300 to
   −500 ms, mobile score +8 to +12. Same bytes, same files, same load path (one-load-path rule untouched). Note the
   sha256 is plain JS too (P10, Hermes); `crypto.subtle.digest` on the web is the same kind of pair (smaller win, not
   in the top 3 of the profile).
2. **What the first visit downloads: 9.3–10.4 MiB in Lighthouse's window, ~1 MiB of it for the first screen.** First
   paint needs manifest + mask + paint (0.7 MiB) + the two feeds (the request walk's pin, `WEB_BUDGET.first_paint`);
   everything else is the T186 idle prefetch (core 1.5 MiB + sql-wasm 0.3) and the T253/T737 background tier
   (water-shared 0.75, regs-sid 1.2, down 1.1, geo 0.6, method 0.3, salmon 1.1, regs-<r>, water-<r> …). Lighthouse's
   simulation runs on the real (fast) line, so `prefetchHold` sees "4g" and lets the background tier run during the
   test, gunzipping and hashing every file on the main thread (item 1's cost). Option: start the idle/background tiers
   only after the load event + a quiet spell (e.g. 5 s with no long task) or the first interaction, not on the first
   idle callback. Offline is unchanged (the same files, same order, a few seconds later; More → Offline maps still
   fetches everything); one-load-path unchanged (a tap reads the same cache, or fetches at once as now). Expected:
   bytes in the window −8 MiB, TBT −200–400 ms on top of item 1, TTI 9.6 s → ~5 s. Owner call: how long the first
   visit waits before the background download starts (default 5 s quiet).
3. **Split `web/index.js` (3.8 MiB raw / 1.1 MiB br; 469 KiB unused at load).** By module: maplibre-gl 1028 KiB (27 %),
   react-native-web 259, react-native-gesture-handler 237, react-dom 204, core 329 + core/regs 196, ui-native 222 +
   ui-native/regs 151, ui 160, map/style.meta.json 117, map 110 + style 16. The map is the first screen, so MapLibre
   stays in front; the regulations presenters (core/regs + ui-native/regs + ui/panel ≈ 370 KiB raw) and the rest of the
   water page, Conditions charts, About/changelog, the walkthrough, alerts and notifications move behind the T282 `part()`
   loader (`ui-native/src/later.ts`, already used for six sheets) — fetched when idle, so a later tap is unchanged.
   Expected: index.js −25–30 % (≈ −300 KiB br), parse/compile + evaluate ≈ −300 ms at 4x, SI −0.5 s. Larger and riskier
   than 1–2 (every moved screen needs its first-open path proven); do after them.

## 4. Noted, not changed (owner's Cloudflare / SEO)

- **Edge-injected scripts on both hosts:** Cloudflare Web Analytics (`static.cloudflareinsights.com/beacon.min.js`,
  10 KB, a third-party origin — against "self-host everything"), Bot Fight / JS detections (`/cdn-cgi/challenge-platform/…
  jsd/main.js` + an iframe, ~17 KB), and speculation rules (`/cdn-cgi/speculation`). Main thread cost is small (~20 ms),
  but **Best Practices 81–82 is entirely these**: the challenge script's two deprecation warnings (locally, without the
  edge, Best Practices is 100). The owner decides: turn off Web Analytics' automatic injection and/or JS detections for
  the zone, or keep them.
- **SEO on production 91–92:** the one failing audit is `meta-description` on `/` (the app's index.html has none; the
  water/region pages have theirs). A one-line description in the export's head would make it 100 (staging's 54–58 is the
  intended noindex). Not changed here (brief: report only).
- Accessibility 95 on both: `aria-allowed-attr` — the map's "My location" button carries `aria-selected`, which a button
  may not have (aria-pressed would be the toggle form). Not part of T910; worth a small ticket.
