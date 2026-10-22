# T46 fish identification on wide screens: shots (2026-10-22)

Two sets.

**`live-sp-<code>-<width>.png` — the live app.** Expo web from this worktree (port 39462, its own data server on 39461,
the main checkout's data read only), Playwright Chromium, light theme. Vedder River (today, Oct 22), the Keep tab's
picture buttons "What a Steelhead looks like" (sp:ST) and "What a Rainbow trout looks like" (sp:RB), at 1440x900,
1024x768 and 390x844. Each picture's box measured in the page:

| sheet | 1440 | 1024 | 390 |
|---|---|---|---|
| sp:ST (ocean, spawning) | one row, 297 + 297 px | one row, 297 + 297 px | stacked, 358 px each |
| sp:RB (one look) | 360 px (the token) | 360 px | 358 px |

**`sp-<code>-<width>[-dark].png` — the sheet harness** (`app/tools/fish-looks-shots.test.ts`, run with
`T46_SHOTS=1`): core's own sp: sheet (`fishSheet`: the fish manifest's pictures and captions) drawn by the real
SheetHost with the app's picture files, laid out by headless Chrome at 390 / 1024 / 1440. Steelhead (ST), rainbow
(RB), sockeye (SK), kokanee (KO), coho (CO), chinook (CH), and steelhead in the dark theme. A harness because no page I
could reach offers sockeye, coho or chinook pictures (they are on no fixture page either); the steelhead and rainbow
sheets match the live ones.

What the shots show: on a tablet and a desktop the sheet is a centred dialog (T45) and a two-look fish draws its looks
side by side, each captioned under its picture (caption · phase · credit, from the manifest); a one-look fish stops at
360 px instead of filling the dialog; a phone is as before (stacked, full width — 358 px, under the token). No ground
behind any fish in light or dark (`sp-ST-*-dark.png`; pinned in jsdom by FishLooks.test.tsx).

Not checked: native, phone landscape (844 px is the phone class: stacked — see the T46 report), a real tablet.
