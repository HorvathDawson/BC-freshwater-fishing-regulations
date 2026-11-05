# T409 — Water pages: the next Lighthouse wins (later)

Status: PARTIAL — e843253e7, 011c8a8b8, 4771bb914 (staging 0457d354): fonts + WebCrypto verify kept, water pages 77-83 (were 67-83); the rest of TBT is the warm map behind the page (owner decision, below).

Source: T405 outcome on staging (fc215b52): water pages Perf 77–88, LCP 2.1–2.5 s, CLS 0.002, A11y 100; region hubs
97–100. What is left is the app's boot (TBT 350–520 ms) and bytes before first paint. Measure each item alone
(Lighthouse mobile, vedder / cowichan / skeena, 3 runs median) and keep only what moves the score.

## Candidates, cheapest first (none changes how the app behaves)
1. **Fonts:** 6 font files load before the first paint. Fewer weights on the prerendered page, subset to Latin,
   `font-display: swap`, preload only the one the largest text uses.
2. **Release verify in the browser's own code:** sha256 + ungzip run in JS today; WebCrypto `digest` and
   `DecompressionStream` do both off the main thread → less TBT. Same checks, same refusals (no silent fallback).
3. **Code splitting:** load the map / Conditions / rule-book code after the pane is interactive, not in the first
   bundle (the `later` chunk exists since T282; move more into it).
4. **sql.js start:** instantiate the WASM after the first paint (it is not needed to show the prerendered pane).
5. **Cache headers:** long immutable cache on hashed JS/CSS/fonts/thumbnails (repeat visits; Lighthouse "efficient
   cache policy").
6. **Third-party / injected scripts:** confirm nothing Cloudflare injects on prod (staging check found none after T196).

Not on the list (owner said no, or it changes behaviour): booting the app only at the first touch.

## Acceptance
Each item measured before/after; water pages ≥ 90 if reachable; hubs stay ≥ 95; T269 page-vs-tap pairs unchanged.

Links: T405, T196, T269, T282. Size: S–M per item. Owner: no.

## Outcome (2026-11-05, staging 0457d354, release r-5a5999c94588, lighthouse 12.8.2 mobile, 3 runs, median)
Each item measured ALONE on a local rig (the staging site's shape served from this machine, data from data-staging; much
less noise than staging: runs agree within 1-2 points), then the kept set on staging.

| item | local before → after (Perf / LCP / TBT, vedder · cowichan · skeena) | kept |
|---|---|---|
| 1 fonts: the 8 faces as lossless WOFF2 at `/assets/fonts/` (`tools/emit-web-fonts.mjs`, build refuses stale) | 81·82·74 → 84·84·77; LCP 2.70·2.40·2.78 → 2.33·2.10·2.40 s; FCP 1.95 → 1.65 s | yes (011c8a8b8) |
| 2 verify: WebCrypto sha256 + DecompressionStream, web-only (`source.web.ts`); no API = error, reported | 81·82·74 → 83·84·76; TBT 426·444·651 → 371·381·604 ms | yes (e843253e7) |
| 3 code splitting | not done: the map code (maplibre, 1 MB) is in the first bundle and RUNS at load for the warm map, so moving it to `later` changes nothing; the split lives in shared ui-native (native bundle would change) | no |
| 4 sql.js after paint | already true: the whole app script is asked for after the first paint (T405 db643ad12); the wasm compiles async (~75 ms at 4x) | no change |
| 5 cache headers | `/_expo/static/*` is `max-age=0, must-revalidate` (a 304 per visit); Lighthouse's cold run does not score it or flag it | no (no score) |
| 6 injected scripts | PRESENT on prod and staging: Cloudflare JS detections (`/cdn-cgi/challenge-platform/.../jsd/main.js`, ~70 ms CPU) and the Web Analytics beacon (cloudflareinsights); Lighthouse counts 0 ms blocking for them | zone setting: owner |

Kept set: local 88·85·78 (TBT 297·386·608 ms, LCP 2.48·2.25·2.55 s). **Staging** (0457d354 vs fc215b52 the same day):
| page | Perf before → after | LCP | TBT | 
|---|---|---|---|
| vedder-river | 76 → 83 | 3.12 → 2.76 s | 425 → 334 ms |
| cowichan-river | 83 → 83 | 2.15 → 2.23 s | 425 → 409 ms |
| skeena-river | 67 → 77 | 3.38 → 2.27 s | 685 → 616 ms |
| region/2, about | 99, 99 → 100, 100 | 1.31, 1.24 s | 0 |
Staging runs vary ±10 points (network); the local rig is the per-item evidence. T269 pairs (JS off vs landed app, 390x844 /
1440x900) unchanged to the hundredth: vedder 14.70 / 4.85 % (live DFO strip), cowichan 0.03 / 0.16 %, skeena 0.03 / 9.16 %.
No native file changed (source.web.ts is web-only; the rest is tools/seo).

## What stops 90+ (owner decision)
- **The warm map behind the page** is most of what is left: a CPU profile (4x) of a water page shows maplibre ~1.5 s of
  main thread (style parse, paint arrays) right after the app's own boot. Measured upper bound, local, no map on
  `/water/*` (experiment only, not committed): **93 · 93 · 93, TBT 147 · 182 · 142 ms** (from 88 · 85 · 78). A middle way
  (web-only, `packages/map` web files): build the map only when it is first seen (phone: on leaving the pane; desktop
  at once) — the phone's "back to map" then waits ~1-2 s for the map once. Owner's call (behaviour).
- The regs store's rules decode (`unpackBlob`, fflate raw inflate with a dictionary) ~0.4 s at 4x: shared data code
  (DecompressionStream has no dictionary); a smaller yield slice there would cut its long tasks — touches native.
- LCP 2.2-2.8 s (gate 1.8): the first view still carries 6 faces + the fish thumbnails before the largest paint.
- Cloudflare's injected JS detections + RUM beacon (item 6): zone settings, owner.
