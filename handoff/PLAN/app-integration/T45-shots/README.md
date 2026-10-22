# T45 the Regulations view on a wide screen: web check (2026-10-22)

Expo web from this worktree on its own port (39462), reading its own data server (39461: the main checkout's
`app/tools/serve-tiles.mjs`, read only, live release), driven by Playwright's Chromium at deviceScaleFactor 1. Light
theme. Each water opened from Search (View, then a tap on the lit water; Bella Coola's side channels through the
water's Map view, "▼ Downstream", "Show regulations for this part"), the day set with the date pill ("Which day?").
`shoot.js` is the Playwright script (its `STATE` set per water).

| file | what it shows |
|---|---|
| `<state>-1440x900-1..4-*.png` | desktop (layout class `desktop`, ≥ 1280): each section reached by pressing it in the side column's section list; the page in the reading column, the side column (date pill, sections, pinned lines) at the frame's left |
| `<state>-1024x768-1..4-*.png` | tablet (`tablet`, 900–1279): the header as on a phone (tab row, date pill), the page and its pin bar in the reading column, centred |
| `<state>-<size>-5-sheet.png` | the first ⓘ sheet on the page as a centred dialog, 640 px wide, sized to its content (vedder / bc1: "Steelhead", 640 × 221; klwa: "Anglers under 16", 640 × 242). Escape closed it and the water stayed (checked by the script at both sizes, all three waters) |
| `vedder_1010-1440x900-6-pinned-lines.png` | the pinned lines in the side column after a scroll ("Trout and char · 2 a day from streams / (of 4 across Region 2) / ↳ of those, 1 over 50 cm …") |
| `vedder_1010-1024x768-6-pinned-lines.png` | the same lines over the reading column on a tablet |
| `vedder_1010-390x844-phone-unchanged.png` | a phone: no reading column, no side column (the DOM has neither) |

States: `vedder_1010` Vedder River, Oct 10; `klwa_1010` Kootenay Lake — Lower West Arm, Oct 10 (no pinned lines on
this page at any width — none on a phone either); `bc1_501` Bella Coola River, 4 side channels, May 1.

## Measured (tools/regs-wide-layout.test.ts, headless Chrome with the app's fonts)

Longest line of body text (15 px faces) in the reading column: vedder 77, klwa 61, bc1 68 characters at every tablet and
desktop size (the measure is 560 px with its gutters). The 12–13 px notes run to 103. No sideways overflow; the content's
left edge under the name row is the frame's gutter (16 at 900/1024, 16 at 1280, 96 at 1440 — the same as Conditions,
T44); the page's centre is within 1 px of the centre of the room beside the side column. A reading column with no
measure fails it (lines of 93–114).

## Which sheets become a dialog (review fix)

Only the regs sheets (the ⓘ / rule / source / fish sheets drawn by `SheetHost`) are centred dialogs on a wide screen:
they opt in with `<Sheet dialog>`. Every other sheet — Layers, Spots, SpotDrop and the Day sheet — changes what the map
shows, so it stays a bottom sheet at every width and the map stays visible above it (`Sheet`'s rule: "not a modal
dialog"). Pinned by `SheetWide.test.tsx` ("the map's sheets stay bottom sheets on a wide screen"). Owner question: the
map's bottom sheets still run the full window width on desktop; capping them (e.g. a left-anchored panel) is open.

Not checked here: native (no emulator), dark theme, a real tablet's touch. Bella Coola part 1 was reached through the
part stepper, not a tap.
