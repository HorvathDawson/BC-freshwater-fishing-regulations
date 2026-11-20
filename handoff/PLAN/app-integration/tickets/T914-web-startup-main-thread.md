# T914 — Web start: break up the first script's evaluation and the first screen's build (mobile Lighthouse)

Status: OPEN (after 0.8.3; owner 2026-11-19 ~12:00 "yes ticket it").

Owner 2026-11-19 ~12:00 (main chat), asked what else could be done about "most of that delay is the main code starting
up, which these changes don't touch" (T910's result), verbatim: "yes ticket it".

## Where it stands (from T910, f900cd41f)
Mobile Lighthouse on `/` holds at ~78 after T910's speed-ups. The score is lost to TBT (~700 ms) and Speed Index (~5 s).
The long tasks are (1) web/index.js's first evaluation, ~350 ms simulated at 4x CPU, and (2) the first screen's build,
170–260 ms tasks around 9–10 s. index.js is 3.8 MB raw / ~965 KB br. By module: maplibre-gl 1,028 KB, core 329 +
core/regs 196, react-native-web 259, react-native-gesture-handler 237, ui-native 222, react-dom 204, ui 160,
map/style.meta.json 117, map 110. Desktop is 98.

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
