# T844 — The web app's opening loader is the new coho, as the phone's is (owner)

Status: IN PROGRESS (app chat, built on branch app-fix/t844-web-coho; not landed; 0.8.0).

Source: owner 2026-11-17 ~22:20 (via the main chat), verbatim: "loading on webapp should use the new one too like the
app", then "new loader". Asked whether the small in-app loading spots switch too: "Opening loader only" (recorded in
OWNER-TODO). So the web's first load gets the coho; the small in-app loaders keep the circling fish (T814).

## What to make
Wherever the web app's first load still shows the old circling fish (the pre-JS shell or a web-only first-load path; the
shared BootScreen already draws the coho), show the coho's loader instead. Small in-app loading spots keep the circling
fish (T814). A clip of the web loader to the owner before it ships.

## Cause (measured: a local `expo export --platform web` with staging data, cold, Fast 3G, 390 x 844)
- Staging (built before T814) shows the circling fish on the start screen: blank 0–8 s, circling fish 8–16 s, map.
- This branch already draws the coho on the web start screen (`BootScreen` → `LoadingState coho` → `CohoLoader.web`),
  but the web's opening is still not the coho: the export's index.html is an empty `#root`, so the page is WHITE until
  the app's script (3.8 MB, ~1.1 MB on the wire) has run (~8 s), and then the start screen shows its words with NO
  picture until the coho's 0.9 MB sheet is in (on Fast 3G the map often came first: no coho at all).
- No other web-only first-load loader: water pages (tools/seo) paint their prerendered pane, no loader; the circling
  fish elsewhere is the in-app "all other loading" the owner kept.

## Fix
- `app/tools/web-shell.mjs` (new), chained into both web exports (`apps/mobile/package.json` `export:web`,
  `export:web:staging`, after `expo export`): puts the coho's frame 0 into the export's index.html at the start screen's
  size and place (`COHO_WIDTH`, `COHO_GROUND_PAD`, the window's centre; card colour; light/dark by the system scheme).
  It is the first paint (~1.5–2.5 s on Fast 3G after the map stylesheet, ~1 s on a fast line). A small module script
  watches the app: frame 0 covers the window until the app draws something of its own (its first commit is the
  safe-area provider's empty boxes: an empty-root CSS rule alone gave a white frame), stays over the start screen's coho
  box until that coho's sheet is in (the app's coho draws nothing until then), and then removes itself; anything else
  the app draws (map, failed start) removes it too. Never asks for the sheet itself: measured, a shell that fetched the
  sheet to animate early held the map back ~0.7 s on Fast 3G (the sheet took the line from the release files).
- `app/tools/build-splash.mjs`: also writes `apps/mobile/assets/splash/web-frame0-{light,dark}.png` (the coho sheet's
  frame 0 at its own 288 x 120 px, 21/25 KB); `pnpm splash` checks them like the phone's launch pictures.
- `app/tools/seo/prerender.ts` `appDocument`: the water pages strip the shell (their pane is the first paint).
- Cost: frame 0 is 21 KB; the map came ~0.3 s later on Fast 3G (18.4 → 18.7 s, same app bundle, 2 runs each).

## Tests
- `app/tools/web-shell.test.ts` (new): facts equal the app's (`COHO.frame`, `COHO_WIDTH`, `COHO_GROUND_PAD`, each
  theme's card + coho ground); size/place; each scheme's frame 0; never the sheet; the states run in jsdom (providers'
  boxes keep it, start screen → over, its sheet's `load` → gone, a sheet already in → gone at once, anything else →
  gone, once gone gone); a module script; idempotent; water pages without it; an export put through it (hashed frame 0,
  the bytes `pnpm splash` wrote); frame 0's size.
- `app/tools/one-spinner.test.ts`: the web's opening is a coho place (owner's words in the header); both export scripts
  must chain web-shell.mjs; no fish sprite in it.
- Mutation-checked: dropping the "over" wait, the providers rule, the load listener, the boot-failed end, the module
  type, the staging chain, the water-page strip, the ground pad, or `pnpm splash`'s frame 0 bytes each fails.

## Pictures (lane scratch, app chat sends them)
Frame strip, Fast 3G (staging today / branch before / with T844): `t844-frame-strip-fast3g.png`; fast line with T844:
`t844-frame-strip-fastline.png`; clips: `t844-before-staging-fast3g.mp4`, `t844-before-branch-fast3g.mp4`,
`t844-after-fast3g.mp4`, `t844-after-fastline.mp4`, `t844-after-dark-fast3g.mp4`.

## Open for the owner
(Answered 23:02, below.) On a slow line the coho stayed still (frame 0) until its 0.9 MB sheet was in, after the map: a
lighter web sheet (lossy WebP or fewer frames) would let it swim sooner. Not built; his call.

2026-11-17 23:02, owner (app chat dialog, after the Fast 3G frame strip and the fast-line clip), verbatim: "make way
smaller for web". So the web gets its own much smaller coho sheet (lossy and/or fewer frames, web only; the phones keep
theirs), so it swims on a slow line before the map.

## The web's own sheet (built after the owner's "make way smaller for web")
- `pnpm coho:build` (`app/tools/build-coho.mjs`) also bakes `coho-{light,dark}-web.webp` from the owner's generator: the
  same drawing and frame size (288 x 120, drawn 192 x 80), 40 frames over the same 4 s loop (10 fps, not 20), and each
  pixel put back on the line between its two inks at 4 steps and its alpha at 16 (`app/tools/fish-loader/web_sheet.py`),
  so the lossless WebP packs it as a palette: 105 KB / 106 KB, from 887 KB / 1,088 KB. The inks stay exact. Lossy WebP
  was tried and is bigger at any quality that keeps the edges (q40 293 KB at 40 frames); 2 ink steps (146 KB at 80
  frames) left a jagged ink/red seam; 4 steps x 16 alpha matches the phones' sheet at the start screen's size (side by
  side: `t844-web-sheet-side-by-side.png`). `pnpm coho` checks the web sheets (size, grid, the two inks) and
  `cohoWeb.generated.ts` like the phones'.
- `cohoWeb.generated.ts` (`COHO_WEB`) is imported by `CohoLoader.web.tsx` only; `cohoSheet(palette, sheets?)` takes the
  web's sheets there; the phones' loader and sheets are untouched (a phone never bundles the web's). The shell's frame 0
  (`pnpm splash`, `web-frame0-*.png`, now 5.5 KB) is the web sheet's own frame 0, so the hand-over is the same picture.
- Measured (same harness, cold, staging data): Fast 3G, the coho swims from ~13.1 s (sheet in) and the map comes at
  16.3 s (two runs; was 18.4-18.7 s with the phones' sheet, which came in after the map); fast line, it swims from
  ~1.2 s and the map comes at 1.75-1.86 s (as before).
- Tests: `CohoLoader.test.tsx` (the web loader draws `COHO_WEB`'s sheet per theme, never the phones'; its grid and
  steps are the web's); `app/tools/web-shell.test.ts` (only `CohoLoader.web.tsx` imports `cohoWeb.generated`, the
  native loader draws the phones' sheet; each web sheet at most 150 KB, same frame and loop). Mutation-checked: the web
  loader on the phones' sheet, the native loader on (or importing) the web's, a 0.9 MB file as the web sheet each fail
  (the last also fails `pnpm coho`).
- Pictures: `t844-web-sheet-side-by-side.png` (a frame, phones' vs web's, both themes, 2x and 4x);
  `t844-web-sheet-frame-strip-fast3g.png` (staging today / T844 before this change / with the web sheet);
  `t844-web-sheet-frame-strip-fastline.png`; clips `t844-web-sheet-after-fast3g.mp4`, `t844-web-sheet-after-fastline.mp4`.
