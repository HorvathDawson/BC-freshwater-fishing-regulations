# T568 — The gauge map moves when Level / Flow is tapped on the chart

Status: OPEN.

Source: Owner 2026-11-09: "when you click level vs flow on chart the location of gauge map changes in an obnoxious way
this is important to fix too".

## Known from the code (not reproduced on a device)
- The map's camera fits only the members that speak in the asked quantity (`app/packages/ui/src/panel/donor.ts`
  `donorView`: `shown` → `placed` → `panelCamera`), and the map remounts when the camera changes (`key`). A member
  silent in one quantity leaves the frame, so the view re-zooms on the toggle.
- When no member reads the asked quantity the sheet is a refusal with NO map at all, so the map appears / vanishes and
  everything under it shifts (Fraser at Mission under Flow before T567).
- After T567 a level-only gauge stays a row and a pin under Flow, so today's frame is the same in both quantities where
  the estimate answers; the refusal case and days ahead are not covered.

## Work
1. Ask the owner which it is (the view inside the map re-zooms, or the map jumps on the page), then reproduce on the
   emulator at Fraser at Mission and one water with mixed gauges.
2. One frame per water: the camera fits every located member that reports either quantity, whatever is asked; the
   toggle never remounts the map. A refusal keeps the map (pins say why they do not count).
3. Test: the camera and key are equal for Flow and Level on a mixed panel.

Depends: T567. Size: S. Owner: one question.
