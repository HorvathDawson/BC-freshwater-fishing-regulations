# T44 water frame: web check (2026-10-22)

Expo web from this worktree on its own port (39452), reading its own data server (39451: `app/tools/serve-tiles.mjs`
over the main checkout's data, read only), driven by Playwright's Chromium at deviceScaleFactor 1. The water: Vedder
River, opened from Search (eye, then a tap on the lit stream); the day is the real today, Oct 22. Light theme only.
`measure.js` and `swipe.js` are the Playwright scripts that took the shots (`PHASE` in measure.js names the folder).

`before/` is the code before T44 (redesign/stream-sections at 39f3a2dd), `after/` this branch. Each folder holds
`<size>-<view>.png` for 390x844, 768x1024 and 1440x900 and `measurements.json` (x/y of the back button, the view strip,
the water's name text, and the content's left edge = the leftmost line of text under the name).

## Measured (px)

| size | view | back x,y | strip x,y | name x,y (font) | content left |
|---|---|---|---|---|---|
| **before** 390 | Regulations | 16,12 | 16,48 | 16,108.23 (17 px) | 16 |
| | Conditions | 16,12 | 16,48 | 16,94 (30 px) | 16 |
| | Map | 16,12 | 16,48 | 16,94 (30 px) | **12** |
| **before** 768 | Regulations / Conditions / Map | 16,12 | 16,48 | as at 390 | 16 / 16 / **12** |
| **before** 1440 | Regulations | 16,12 | 16,48 | 16,108.23 | 16 |
| | Conditions | **96**,12 | **96**,48 | **96**,94 | **96** |
| | Map | 16,12 | 16,48 | 16,94 | **12** |
| **after** 390 | all three | 16,12 | 16,48 | 16,108.23 (17 px) | 16 |
| **after** 768 | all three | 16,12 | 16,48 | 16,108.23 | 16 |
| **after** 1440 | all three | 96,12 | 96,48 | 96,108.23 | 96 |

What moved before: on a desktop, Conditions alone was held to a 1,280 px centred column, so Regulations ↔ Conditions
jumped the back, strip, name and content 80 px sideways (the owner's report); at every size the name changed face and
place (Regulations' 17 px name row vs the others' 30 px title, 14 px apart), and the Map view's bar started at 12 px.

After: one column (1,280, centred) for every view, one header (drawn by `WaterFrame`), one name row (`WaterTitle`, the
regs header's), one gutter. The Map canvas goes full-bleed under the header (`after/1440x900-map.png`); its bar's words
stay on the column. Phone: the back and the strip did not move; the Conditions and Map titles now use the regs name row
(17 px, was 30 px) and the Map bar's words moved from 12 to 16 px — the places the views disagreed.

## Swipe recording (`after/swipe/`)

A mouse drag right→left across the Regulations body (x 70% → −180 px, y 420), a frame at 20/60/100/140/180 px, on
release, and settled (3 s), at 390x844 and 1440x900 — 8 frames each. In every frame the back sits at 16,12 (96,12 on the
desktop), the strip at 16,48 (96,48) and the name row at 0,94 (80,94); the body alone follows the finger
(`390x844-04-drag140.png`), and on release the view is Conditions with nothing shared having moved (`-06-released`,
`-07-settled`). The blue word in the drag frames is the browser's text selection under a mouse drag, not the app.

**A drag on the header (review of T44, fixed after).** The recording above drags the body only. A drag on a header used
to carry that header with the finger: on Regulations the frame's back and strip and the regs header's name row slid
separately, and on Conditions and Map the whole header slid. Now the headers listen without moving (`still`), and every
detector inside the frame drives the frame's one offset, which only the view's body follows (on Map, the canvas via
`SwipeFollow`). Pinned by `app/tools/water-frame-swipe.test.ts`: the real Shell, each detector (frame header, regs
header, body) dragged on each view, back / strip / name / place carried 0 px and the content carried left, with a
mutation pin (headers that follow the finger fail with exact rows). Not re-recorded in a browser.

Not checked here: native (no emulator), dark theme, a real finger.
