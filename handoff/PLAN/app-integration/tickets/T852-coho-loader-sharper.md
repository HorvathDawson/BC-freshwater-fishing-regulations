# T852 — The coho loader is drawn sharp on the phone (and the web)

Status: DONE — dc6a19b13, 0f3ada780. coho loader sheets baked at 2.5 px/dp (phones) and 2 px/css px (web); the line under it set as the owner's pick B.

Closed by the 2026-11-20 audit: owner picked B 2026-11-18 14:04 (2ef385a37); both commits are on the dev branch and shipped from 0.8.1 (production 0.8.3).

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

## 2026-11-18 14:04: owner picks B for the line under the fish
Pictures sent (today / A italic red quotes / B red rule + muted line / C two balanced lines, light and dark). Owner: "B: red
rule, muted line". Part 1 (sharper sheet) on branch worktree-agent-aa579d9bb6fd2ca09 dc6a19b13: phone 2.5 px/dp (480x200
frames, 8x10 = 3840x2000, 30.7 MB decoded), web 2 px/css px (384x160, 145 KB, under the 150 KB web limit).

## Done (2026-11-18, branch worktree-agent-aa579d9bb6fd2ca09, not yet merged)

1. **Sheets** (dc6a19b13). Phones: 80 frames of 480x200 (2.5 px per dp), full 8x10 grid, 3840x2000 lossless WebP,
   1.48 / 1.82 MB (light / dark; was 0.89 / 1.1), 30.7 MB decoded (was 11.1). A Pixel 9 (2.625 px/dp) draws it at
   1.05x. 3 px per dp (576x240) was measured and not taken: 44.2 MB decoded, 1.8 / 2.3 MB, and no 8-column grid fits
   4096 px. Web: its own 384x160 frame (2 px per CSS px), 8x5, 3072x800, ~146 KB, inside the 150 KB web budget (the
   phones' 480 frame would be ~188 KB). `build-coho` refuses a sheet past 4096 px or a part-filled last row (the web's
   CSS steps would show empty cells). Splash and web frame 0 re-cut.
2. **The line, owner's pick B** (0f3ada780; picked from the three settings drawn 2026-11-18): `LoaderLine`
   (ui-native) — a 24x2 rule in `palette.coho.red`, then the line in `TYPE.small`, Archivo Medium, tracked 0.4,
   `palette.sub`, centred — on the curtain's `apply-line`, the boot screen's `boot-line` and the first install's
   `SayingLine`; the step words beneath (`boot-words`) go to `TYPE.small` in `sub`. Pinned by `LoaderLine.test.tsx`
   and `tools/one-spinner.test.ts`.

Checks: `pnpm check:fast` (with knip) passes; the coho, curtain, loader-line, web-shell and one-spinner tests pass.
