# T852 — The coho loader is drawn sharp on the phone (and the web)

Status: OPEN, 0.8.1 (owner 2026-11-18 ~13:50: "also on the app the loader fish icon should be higher resolution please"; ~13:55 the line under it too).

Today (T814/T844): the sheet is baked at 1.5 px per dp (`tools/build-coho.mjs` FRAME_PX 288 for a 192 dp loader;
80 frames of 288x120 in an 8x10 sheet, 2304x1200, lossless WebP ~0.9-1.1 MB per theme). A Pixel 9 draws ~2.6 px per dp,
so the fish is upscaled ~1.75x and looks soft. The web sheet (cohoWeb.generated.ts, 2304x600) is drawn at 192 css px on
2-3x screens: softer still.

Fix: bake at the screen's density (about 3 px per dp on phones; 2-3x on the web) from the owner's generator
(`tools/fish-loader/fish_loader.py`, vector), keeping each sheet inside the 4096 px texture limit (re-grid the frames)
and the decoded memory and download size reasonable (or pick per-density sheets). Before/after pictures at phone size.

## 2. The line under the fish, set nicer (owner 2026-11-18 ~13:55, verbatim: "the quote text should be formated in a nicer way too on loader for 0.8.1")

The sayings under the loader (core `DOWNLOAD_SAYINGS`, first install; `APPLY_WORDS.lines` on the apply curtain and the
boot screen, `ApplyCurtain.tsx` `apply-line`, 15 pt body, centred) read as a plain caption. Draw 2-3 settings (e.g. italic
in quotes with hanging punctuation, a smaller muted line with a short rule, two-line balanced wrap) at phone and web size,
light and dark, and let the owner pick from pictures before any lands. Target 0.8.1.
