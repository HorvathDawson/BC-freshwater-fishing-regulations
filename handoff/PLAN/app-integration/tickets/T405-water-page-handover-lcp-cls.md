# T405 — Water pages fail the Lighthouse gate when the app takes over the prerendered pane

Status: PARTIAL — 5ba3155e5, db643ad12, f32ddb13a; CLS and A11y met; fish thumbnails (owner 11-05) took LCP to 2.1-2.5 s, Performance 77-88; TBT (app boot) still stops 95: owner decides the water-page target (below).

Source: T196 gate on staging 2026-11-05 (release r-5a5999c94588, staging version 965c01af, lighthouse 12.8.2 mobile,
simulated 4G). Hubs and About score Performance 100 / A11y 100 / LCP 1.3-1.4 s / CLS 0. `/water/vedder-river`:
**Performance 45, A11y 97, LCP 38.9 s, CLS 0.154, TBT 750 ms.**
- LCP element is a span the APP renders ("only if you fish for white sturgeon…"), 99 % render delay: the prerendered pane
  (T269) is replaced by the app's own pane once the bundle has run and read the release, and the replacement's text
  becomes the new largest paint. The app bundle costs 3.7 s of main thread under the 4x CPU throttle.
- CLS 0.154: the pane container (0.052) and the `keep-tab` block (most of the rest) move at the hand-over.
- A11y: `aria-dialog-name`, `label-content-name-mismatch` (app components, same on the tapped pane).
## Work
1. Make the hand-over keep the prerendered nodes' boxes: the app's first render of the pane has the same size and place
   as the prerendered one (no shift), and either hydrates the prerendered DOM or takes over without painting a larger
   text node than the prerender already painted.
2. Fix the two A11y findings in the pane components.
3. Re-run the T196 gate on 3 water pages (command in handoff/PLAN/seo/MEASURE.md, against staging).
## Acceptance
Lighthouse mobile on 3 water pages: Performance >= 95 (or the owner lowers the water-page target), LCP <= 1.8 s,
CLS <= 0.05, A11y >= 95; the T269 page-vs-tap pixel pairs still match.

Depends: T269 (water pages are the app). Size: M. Owner: no (unless the target moves). Default: keep the gate's
targets; until fixed the water pages still serve full prerendered content to crawlers (no JS needed), so indexing is
not blocked — only the Core Web Vitals score.

## Outcome (2026-11-05, staging ffb4f71f, release r-5a5999c94588, lighthouse 12.8.2 mobile)
| page | Performance | LCP | CLS | TBT | SI | A11y |
|---|---|---|---|---|---|---|
| vedder-river before | 45 | 38.9 s | 0.154 | 750 ms | 6.2 s | 97 |
| vedder-river | **76** | 3.7 s | **0.002** | 350 ms | 6.1 s | **100** |
| cowichan-river | **82** | 2.2 s | **0.002** | 480 ms | 5.7 s | **100** |
| skeena-river | **78** | 2.6 s | **0.002** | 520 ms | 6.3 s | **100** |
(SEO 61 = staging's noindex.) What changed:
- **The hand-over paints nothing new** (5ba3155e5). Under the prerendered pane the app's `#root` is drawn at 1 % (`transform:scale(.01)`,
  `prerender.ts` PRERENDER_CSS): LCP and CLS are taken from each element's FIRST paint, so the app's pane, painted under the
  prerender at full size, made its first frames count (a larger text block → LCP at the hand-over; its first layout's shifts → CLS).
  At 1 % those paints are a few px; taking the transform off paints nothing new and is not a shift. Measured (CDP, 4x CPU):
  `visibility:hidden` and `opacity:0` do NOT work — an element first painted when shown counts then. The hand-over now waits
  for the app's pane to be complete: every data-testid the prerendered pane shows (the access strip and the salmon row arrive
  0.5-0.8 s after the header) and 3 still frames; else 1 s still (a live feed changed a part: Vedder's DFO salmon strip), else the
  reader's first click/key, else 5 s (console.info). `handoff.test.ts` runs the script in jsdom.
- **The app's script is requested one frame after the pane's first paint** (db643ad12): as a `defer` tag lantern counted its
  download + run in the LCP (24.8 → 2.6-3.7 s locally).
- **A11y 100**: every web Modal (`role=dialog`) named (sheet by its title id, menu page, photo viewer, coach marks). The unscored
  (weight 0) `label-content-name-mismatch` fixed where axe can see it: ‹ "Back to Map", access strip name = its line + sentence;
  date button "Today Nov 5, change date" and glance rows named by their words are right for AT, but axe joins sibling text
  nodes without a space ("TodayNov 5"), so it may still list those two.
- **T269 pairs** (page JS-off vs landed app, staging): skeena phone 0.03 %; vedder phone 13 % and desktop 5 % = the live DFO salmon
  feed (the build's release says Salmon closed with a strip; today's feed folds it into the table) — the known T269 difference;
  skeena desktop 10 %: the prerendered desktop header sits 10 px lower and has no Share button (T385's button is runtime-only) —
  pre-existing, not from this change (follow-up).
## What stops 95 (owner decision)
- **TBT 350-520 ms** (30 % of the score): the app's boot (3.5 MB bundle eval ~200 ms at 4x, release verify sha256 + ungzip in JS
  ~1 s at 4x, sql.js init, the pane's React render). Measured option: boot the app at the reader's first touch/key instead of
  at load → TBT 0, SI 6 → 2 s, ≈ +12 points (local A/B on Vedder: 73 → 85). Cost: the page's controls are inert until the first
  touch boots the app (~1-3 s on a mid phone); the warm map behind the page starts later.
- **LCP 2.2-3.7 s**: the first view fetches ~600 KB before its largest paint — 7 fish thumbnails at 30-72 KB each (full pictures
  drawn at ~40 px) and 6 font files. Thumbnail-size fish images (~3 KB each) would take most of it; that is an app asset change.
- Options: (a) lower the water-page target to Performance ≥ 75 (hubs stay ≥ 95); (b) boot-at-first-touch + thumbnail images
  (expected ≥ 90, 95 not proven); (c) app-boot work (WebCrypto/DecompressionStream for verify, code split) — larger.

## Fish thumbnails (owner decision 2026-11-05; f32ddb13a, 55bd79424; staging fc215b52, release r-5a5999c94588)
- `tools/emit-fish-thumbs.mjs` draws a 240 px WebP thumbnail of every coded fish picture in headless Chrome (deterministic;
  39 files, 254 KB in all, 5-10 KB each vs 25-72 KB), `thumbs.json` records the source and thumbnail sha256; `pnpm fish`
  refuses a missing or stale thumbnail (no fall back to the full picture). `FishImages` is `{ thumb, full }` per picture:
  every row, chip and card (rows, Pictures, salmon table, Runs chips/sheet) draws `thumb`, only the fish sheet's
  `FishImage` draws `full` (loaded when the sheet opens; nothing preloads it). Native uses the same map (decoded bitmaps
  240x120 instead of 800x400). The full picture keeps one size: the sheet draws it at most 360 px wide (`FISH_PICTURE`),
  so 800 px is already the 2x size on every layout; a phone/desktop srcset would buy nothing.
- Fish image bytes on first view (Lighthouse, transfer): vedder 306,592 → 54,251 B (7 pictures), skeena 349,742 → 65,507 B
  (9), cowichan 44,849 → 6,780 B (1). No full picture is requested by any of the three pages.
- Lighthouse mobile (12.8.2), two runs after, one before (before = staging ffb4f71f today; Vedder's LCP varies with the
  live DFO strip):

| page | Perf before | Perf after | LCP before | LCP after | CLS | TBT after | A11y |
|---|---|---|---|---|---|---|---|
| vedder-river | 48 (76 on 11-05 am) | 88 / 86 | 12.4 s (3.7) | 2.4 / 2.3 s | 0.002 | 276 / 383 ms | 100 |
| cowichan-river | 76 | 82 / 85 | 2.7 s | 2.3 / 2.1 s | 0.002 | 468 / 425 ms | 100 |
| skeena-river | 66 | 77 / 78 | 4.1 s | 2.2 / 2.5 s | 0.002 | 686 / 643 ms | 100 |

- T269 pairs (page JS off vs landed app, staging fc215b52, 390x844 and 1440x900): skeena phone 0.03 %, cowichan phone
  0.03 % / desktop 0.16 %, vedder phone 14.7 % / desktop 4.9 % (the live DFO salmon strip, as before), skeena desktop
  9.2 % (the desktop header offset + Share button, as before). None worse; the thumbnails draw identically in both.
- What is left: LCP 2.1-2.5 s (gate 1.8) and TBT 280-690 ms are the app's boot; boot-at-first-touch is NOT done (owner).
