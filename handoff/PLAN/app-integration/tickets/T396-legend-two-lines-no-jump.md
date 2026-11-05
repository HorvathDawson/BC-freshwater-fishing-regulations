# T396 — Trout & more legend takes three lines; switching modes jumps

Status: DONE in code 2026-11-05 (8d1500036; owner report 2026-11-05, 0.5.0 build 8, map). Phone check with the next APK.

The Trout & more legend wraps to 3 lines where 2 should fit, and because the Salmon legend is a different height the
bottom bar jumps when switching between Salmon and Trout & more. Fit it in 2 lines (phone width ~390 dp, the T283 text
size) and give both modes' legends the same height. Ties to T377 (tappable/collapsible legend).

## Done in code 2026-11-05 — phone check with the next APK
Measured, not guessed: in Archivo at 15 px (T283's size, unchanged) the seven Trout & more entries with the old
swatches (20 px lines, 22 px edges, 7 px to the label, 16 px apart) need ~407 px for any four in a row; a 390 dp phone
has 362. So the key is laid in TWO FIXED ROWS (`LegendRows`, ui-native Chrome.tsx) with tight entries (`LEGEND_ROWS`:
14 px lines, 16 px edges, 4 px to the label, 8 px apart, 12 px side padding):
- Trout & more: Closed · Own rules · Base rules / No access · Fly only · Bait ban · Not fishable (353 of 366 px; the
  dated "Closed Sep 30" row 1 fits too).
- Salmon: Keep salmon · Release only · Closed / Check DFO · Fly only · Bait ban.
- The switch row is one row in both modes: Salmon's "DFO rules checked …" sits beside the switch (up to two lines, no
  taller than it) and no longer wraps under it, so the bar is the same height in both.
A row too wide for a narrower phone (360 dp) still wraps rather than cutting a word — there Trout & more's second row
can take a third line. Test: MapLegend.rows.test.tsx (two rows per mode; every row's width from Archivo's own advance
widths ≤ 390 dp; fails on the old measures).
Phone: switch Trout & more ↔ Salmon on the map; the bar must not move, and both keys stand two lines.
