# T409 — Water pages: the next Lighthouse wins (later)

Status: PARTIAL — e843253e7, 011c8a8b8, 4771bb914, eb118fd15, 17791608a, e92983641 (staging 0457d354): fonts + WebCrypto verify kept, water pages 77-83 (were 67-83); the rest of TBT is the warm map behind the page (owner decision, below). OWNER 2026-11-05: build the map only when it is first seen on water pages (web-only), next chat; keep the Cloudflare scripts. Lazy map DONE in code 2026-11-05 (below); its measurement is owed (heavy lock).

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


## Lazy map 2026-11-05
**What changed (web only):** `Map.web.tsx`'s `Map` is now a gate: until its box is seen it draws only an empty box of the
same size (`data-map-unbuilt`); once seen it mounts the one map (`SeenMap`, the old body, unchanged) with the props of that
moment, and stays built. "Seen" (`packages/map/src/web/firstSeen.ts`): no ancestor at computed `opacity: 0` (the Shell's
`BEHIND`, used only for the warm map behind a landing), re-checked on any ancestor class/style change and on resize; a
window ≥ 1280 px (`LAYOUT_BREAKPOINTS.desktop`, pinned by the test) builds at once, as before. So a phone/tablet landing on
`/water/<slug>` still mounts the warm BrowseHost when idle (the map chunk is still fetched), but maplibre is not built until
back; the first back then builds it (~1-2 s once, accepted). Every visible map (tap, search, step, refresh, Conditions,
MiniMap) is built in the same commit as before (layout effect, before paint). No native file, no Shell change.
**Note for the owner:** on desktop the map is NOT beside the pane today — the water frame is opaque across the whole window
(1280 column centred on `palette.card`), so at 1440 the warm map is also unseen and still costs its build. Dropping the width
rule (one line, `wide()` in `firstSeen.ts`) would make desktop lazy too; kept as decided.
**Files:** `app/packages/map/src/Map.web.tsx`, `app/packages/map/src/web/firstSeen.ts`, `app/MAP.md`.
**Tests:** `app/packages/map/src/firstSeen.render.test.tsx` (7: phone + behind → nothing built, shown → built once and kept;
shown by a class change; desktop → at once; visible → at once; widened to desktop → built; one load path: hidden → props
changed → shown builds the same constructor options + the same call log as a visible mount of those props; the breakpoint
pin). Mutation-checked (gate off → 4 fail; width rule off → 2 fail). Also run: one-engine, adapter-contract, water-copy,
pill-theme, parity, `WarmMap.shell.test.tsx` (116 pass); typecheck, platform, boundaries clean. Not run (heavy lock): `pnpm
check` (knip), full suite.
**Owed (when the heavy lock is free):** export + build-seo, then the local rig exactly as in Outcome (lighthouse 12.8.2 mobile,
3 runs each, median) on vedder-river / cowichan-river / skeena-river: Perf / LCP / TBT vs the kept set 88·85·78 (TBT
297·386·608 ms); expect near the no-map bound 93·93·93. Then the T269 page-vs-tap pairs (390x844 / 1440x900) must be unchanged
to the hundredth, and a phone-width manual check: land on a water page, back → the map appears framed on the water, coloured;
tap a water → map at once. Then staging.

## Merged in (ticket audit 2026-11-05, owner)
This ticket now also carries the leftovers of: T188, T196, T405, T198 — read each file's open items and finish them here; each closes pointing here.
- **Owner 2026-11-05 (later): desktop waits too.** The desktop-at-once rule is gone (`wide()` / `BUILD_AT_ONCE_FROM` removed;
  no resize listener): at every width the warm map is built when first shown. Tests: the desktop case now asserts
  "not built until shown"; the widen and breakpoint tests were dropped with the rule. The measurement owed above now also
  covers 1440x900 (desktop TBT should fall like the phone's).
