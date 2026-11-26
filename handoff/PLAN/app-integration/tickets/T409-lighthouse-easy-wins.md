# T409 — Water pages: the next Lighthouse wins (later)

Status: PARTIAL — e843253e7, 011c8a8b8, 4771bb914, eb118fd15, 17791608a, e92983641 (staging 0457d354): fonts + WebCrypto verify kept, water pages 77-83 (were 67-83); the rest of TBT is the warm map behind the page (owner decision, below). OWNER 2026-11-05: build the map only when it is first seen on water pages (web-only), next chat; keep the Cloudflare scripts. Lazy map DONE in code 2026-11-05; MEASURED 2026-11-06 on the local rig: mobile 93·91·93 (was 88·85·78), desktop 1440 99·100·99 (below). left (Claude): the T269 page-vs-tap pairs, the phone-width manual check, then staging.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T409 | Water pages: next Lighthouse wins (fonts, WebCrypto/DecompressionStream verify, code splitting, sql.js after paint, cache headers) | PARTIAL | e843253e7, 011c8a8b8, 4771bb914, eb118fd15, 17791608a, e92983641 | lazy map measured 11-06 (local rig): mobile 93·91·93 (were 88·85·78), desktop 1440 99·100·99; left: T269 pairs, phone check, staging |

Source: T405 outcome on staging (fc215b52): water pages Perf 77–88, LCP 2.1–2.5 s, CLS 0.002, A11y 100; region hubs
97–100. What is left is the app's boot (TBT 350–520 ms) and bytes before first paint. Measure each item alone
(Lighthouse mobile, vedder / cowichan / skeena, 3 runs median) and keep only what moves the score.

Not on the list (owner said no, or it changes behaviour): booting the app only at the first touch.

## Done
Each item measured ALONE on a local rig (staging site's shape served from this machine, data from data-staging,
lighthouse 12.8.2 mobile, 3 runs, median; runs agree within 1-2 points; staging varies ±10), then the kept set on staging.
- 1 fonts: the 8 faces as lossless WOFF2 at `/assets/fonts/` (`tools/emit-web-fonts.mjs`, build refuses stale) — kept
  (011c8a8b8). 2 verify: WebCrypto sha256 + DecompressionStream, web-only (`source.web.ts`); no API = error, reported —
  kept (e843253e7). 3 code splitting: not done (maplibre, 1 MB, RUNS at load for the warm map). 4 sql.js after paint:
  already true (T405 db643ad12). 5 cache headers: `/_expo/static/*` is `max-age=0, must-revalidate`; no score, not done.
  6 injected scripts: Cloudflare JS detections (`/cdn-cgi/challenge-platform/.../jsd/main.js`, ~70 ms CPU) + Web
  Analytics beacon PRESENT on prod and staging: zone setting, owner (T240 §7).
- Staging, kept set (0457d354 vs fc215b52, release r-5a5999c94588): vedder 76 → 83, cowichan 83 → 83, skeena 67 → 77;
  region/2, about 100. Local kept set 88·85·78 (TBT 297·386·608 ms).
- Lazy map (owner 2026-11-05: build the map only when first seen on water pages, web-only; later "desktop waits too"):
  `Map.web.tsx`'s `Map` is a gate drawing an empty box (`data-map-unbuilt`) until seen, then mounts `SeenMap` and stays
  built; "seen" = no ancestor at computed `opacity: 0` (`packages/map/src/web/firstSeen.ts`), at every width. The first
  back builds it (~1-2 s once, accepted). No native file, no Shell change. Tests `firstSeen.render.test.tsx`
  (mutation-checked). Files: `app/packages/map/src/Map.web.tsx`, `firstSeen.ts`, `app/MAP.md`.
- Lazy map measured 2026-11-06 (local rig, HEAD e8ebf21f4, r-5a5999c94588): mobile **93 · 91 · 93** (TBT 149 · 272 ·
  147 ms, LCP 2.48 · 2.25 · 2.48 s), desktop 1440x900 **99 · 100 · 99** — at the no-map bound (93·93·93).

## Work (left)
1. The T269 page-vs-tap pairs (JS off vs landed app, 390x844 / 1440x900) unchanged to the hundredth; before: vedder
   14.70 / 4.85 % (live DFO strip), cowichan 0.03 / 0.16 %, skeena 0.03 / 9.16 %.
2. Phone-width manual check: land on a water page, back → the map appears framed on the water, coloured; tap a water →
   map at once. Then staging.
3. Still between the pages and 90+ / the LCP gate: LCP 2.2-2.8 s (gate 1.8: 6 faces + fish thumbnails before the
   largest paint); the regs store's rules decode (`unpackBlob`, fflate raw inflate with a dictionary) ~0.4 s at 4x —
   a smaller yield slice would cut its long tasks, touches native.
4. **Fonts fetched twice (found by T304's Fast-3G run, 2026-11-06):** after the page's WOFF2 faces, the app bundle loads
  its own TTF copy of every face (expo-font assets, 8 × 36-48 kB, ~315 kB) before the hand-over. Making the app use the
  page's WOFF2 faces on the web (same family names) would save those bytes on every landing. Not done: next item here.

## Merged in (ticket audit 2026-11-05, owner)
This ticket now also carries the leftovers of: T188, T196, T405, T198 — read each file's open items and finish them here; each closes pointing here.

## Acceptance
Each item measured before/after; water pages ≥ 90 if reachable; hubs stay ≥ 95; T269 page-vs-tap pairs unchanged.

Links: T405, T196, T269, T282. Size: S–M per item. Owner: no.
