# T914 — Web start: break up the first script's evaluation and the first screen's build (mobile Lighthouse)

Status: OPEN (after 0.8.3; owner 2026-11-19 ~12:00 "yes ticket it"; raised again 2026-11-24 with a production Lighthouse run: 0.9.2).

Owner 2026-11-19 ~12:00 (main chat), asked what else could be done about "most of that delay is the main code starting
up, which these changes don't touch" (T910's result), verbatim: "yes ticket it".

## Where it stands (from T910, f900cd41f)
Mobile Lighthouse on `/` holds at ~78 after T910's speed-ups. The score is lost to TBT (~700 ms) and Speed Index (~5 s).
The long tasks are (1) web/index.js's first evaluation, ~350 ms simulated at 4x CPU, and (2) the first screen's build,
170–260 ms tasks around 9–10 s. index.js is 3.8 MB raw / ~965 KB br. By module: maplibre-gl 1,028 KB, core 329 +
core/regs 196, react-native-web 259, react-native-gesture-handler 237, ui-native 222, react-dom 204, ui 160,
map/style.meta.json 117, map 110. Desktop is 98.

## Owner's Lighthouse run on production 0.9.0, pasted 2026-11-24 (planning chat: "more stuff to add")
Build `web/index-eff5c34….js`, data release r-0aec88ed3edb. Owner 2026-11-24 (answer to "which page and
device"): PageSpeed Insights, opened from Search Console, on `https://canifishthis.ca` (the start page `/`),
"mobile": `https://pagespeed.web.dev/analysis/https-canifishthis-ca/hcg2notentrn1pdxvbma3c?form_factor=mobile`.
So it is Google's hosted Lighthouse with its mobile profile (a slowed mid-range phone on slow 4G; the exact
throttling is PageSpeed's, not ours), not T910's local method: compare like with like, and re-run PageSpeed on the
same URL after each step. The score and the timing metrics (FCP, LCP, TBT, Speed Index) of this run were not pasted:
read them from the link while it lasts.
- Minimize main-thread work: 25.2 s (Other 13,359 ms; Script Evaluation 10,226; Garbage Collection 856; Script
  Parsing & Compilation 575; Style & Layout 179; Rendering 44; Parse HTML & CSS 10).
- Reduce JavaScript execution time: 10.8 s. `web/index-….js` 23,518 ms total CPU (9,314 evaluation, 521 parse);
  `web/later-….js` 70 ms; unattributable 1,461 ms.
- Reduce unused JavaScript: est. 595 KiB of 1,212 KiB transferred (`index` 1,126.6 KiB, 520.5 unused; `later`
  85.3 KiB, 74.3 unused).
- Reduce unused CSS: est. 14 KiB (the inline `#boot-coho …` block, 13.8 KiB).
- Enormous network payloads: 7,141 KiB in all. Largest: `core-….sqlite.gz` 1,492.8 KiB; `index-….js` 1,127.5;
  `water-shared-….sqlite.gz` 765.4; `paint-….bin.gz` 700.3; `salmon-6-….bin.gz` 405.5; `sql-wasm-browser.wasm`
  317.0; `method-….bin.gz` 310.2; `regs-2-….sqlite.gz` 211.1; `coho-light-web….webp` 146.0; `salmon-5-….bin.gz` 141.8.
- Reading (the planning chat's, unverified): 25 s of main-thread work is far above the ~700 ms TBT recorded above,
  so either the page or the throttling differs, or 0.9.0 regressed; "Other" at 13 s is not script and needs a trace
  (map rendering, decompression and SQLite work on the main thread are the candidates). Step 0 below now also
  covers: which first-load data files can wait until after the first paint (`paint`, `salmon-*`, `method`,
  `water-shared`), and whether unpacking and SQLite can move off the main thread.

## Work, in order, each measured (interleaved Lighthouse runs, T910's method, nothing else on the machine)
0. **Measure the pages search visitors land on first:** `/water/<slug>`, `/region/<n>`, `/gauge/<station>` (static
   HTML from tools/seo), mobile and desktop. If they score well, `/`'s score matters less; report before going further.
1. **Split the first evaluation into tasks under 50 ms where possible.** MapLibre as its own chunk loaded in parallel
   with index.js, and yield between start stages. TBT counts only each task's time over 50 ms. Expected −200 to −300 ms
   TBT.
2. **Build the first screen in stages.** Search bar and map first; hidden parts (sheets, legend details, search
   suggestions) a frame later. No visible change and no empty box; pictures at 390/1440, light and dark.
3. **map/style.meta.json as data, not code** (JSON.parse or fetched with the style). Small and cheap.
4. Only if 1–3 are not enough, and only with the owner's go: drop react-native-gesture-handler on the web (−237 KB;
   risk: map gestures and sheet drag), or pre-render `/` as HTML the app starts on top of (largest gain, largest job).

Web and native split by file (AGENTS 34a). Phones must not change: `pnpm platform`, plus the Android testbed
mode-switch and zoom-pinch benches if any shared start code moves.
