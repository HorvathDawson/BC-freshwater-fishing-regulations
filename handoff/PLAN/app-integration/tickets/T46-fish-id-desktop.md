# T46 — fish identification on desktop: phases side by side, sensible sizes
Phase 4 · Depends on T20 (fish manifest/sprites), T36/T37 (sp: sheets, SheetHost), T45 (layout class) · Size 0.5–1 day

## Owner (2026-10-21)
"especially items like the fish identification etc. for stuff that has ocean and spawning look they should be side by
side in desktop right now they become hugeeee."

## Goal
In the species sheet (sp:) and anywhere a fish picture is drawn (Keep tab rows, other: sheets, the hatchery/adipose
sheet), a fish with more than one look (ocean / spawning, sea-run / resident, male / female …) shows the looks SIDE BY
SIDE when there is room (tablet and desktop, and phone landscape where they fit), each one captioned. Every picture has
a max rendered width (a token, about 320–360 px), so a picture never scales with the window. The phone portrait layout
stays as it is (stacked).

## Work
- Fish image component: `maxWidth` from tokens, aspect ratio preserved, a caption per look from the fish manifest
  (never hand-written). A row of looks wraps when it doesn't fit.
- Uses T45's layout class. Never reads the window directly.
- Shots: the sp: sheets for steelhead/rainbow, sockeye/kokanee, coho and chinook at 390, 1024 and 1440 px, saved to
  `T46-shots/`.

## Acceptance
- A render test: at desktop width, a two-look fish draws its looks in one row, each at or below the max width. At phone
  width it draws them stacked. A mutation (dropping maxWidth) fails it.
- The T39 picture rule still holds (no ground behind a fish), in light and dark.
