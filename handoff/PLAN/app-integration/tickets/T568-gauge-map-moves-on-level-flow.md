# T568 — The gauge map moves when Level / Flow is tapped on the chart

Status: IN REVIEW — 1c57e7c07 (map over the chart whatever the quantity; one frame per water; refusal keeps the map; reaches the phone with the next build; owner confirms there)

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

## Owner's answer 2026-11-09
"the whole map jumps position from above the chart to below it." So it is the ORDER of the sheet's blocks that changes
with the quantity, not the camera. Work item 2 becomes: the map keeps one place in the sheet (above the chart) whatever
is asked and whether or not the estimate answers; find what orders the blocks (the refusal branch of `donorView` returns
no map, and the Conditions sheet lays the chart and the map out by what it has). The camera point stays as a second check.
Owner also: a forecast showed under Level for Mission in the phone app only, not the web app (picture to come): maybe a
stale update on the phone. Look at it with this.

## Done 2026-11-10 (1c57e7c07)
- Cause: two things. (1) The block order: the route map sat UNDER the chart when the estimate answered, and a refusal
  (Flow on a level-only water) drew no map at all, so the toggle moved or removed the whole map. (2) The camera fitted
  only the members that speak in the asked quantity, so the frame (and the remount key) moved on the toggle.
- Fix: `donorView` frames every located member that is silent only for the quantity or day asked (not `quiet` /
  `not-realtime`), and a refusal now carries the map (`DonorVM.map`; pins are the members, each with its state). The map
  is one block in `DonorPanel`, under the tile and over the chart, refusal or not. `DonorMap`'s two buttons gained
  `hitProps` (a latent 44 px gap the a11y test found once the map showed on a refusal).
- Tests: `panel.frame.test.ts` (camera, key, bbox equal for Flow and Level, today and +3; a refusal keeps the map),
  `DonorPanel.map.test.tsx` (map before chart on an answer and on a refusal).
- Not run on the emulator or the web build (no new APK may be built this session).

## Seen in the web shots 2026-11-10 (staging data, web build on :39240)
- Four shots in `../T568-shots/` (web-now-flow, web-now-level, web-day-flow, web-day-level; day = Fri 13). The map sits above
  the chart and its frame is the same on the toggle; the lead (Hope 08MF005 on today's feed) is the same in both.
- DEFECT found by the shot: on a forecast day under Level the refusal drew `GaugeTrace` ("How this spot reaches the gauge",
  with its own map and a table) above the donor map, pushing the map down by ~400 px. Fixed: the trace is drawn only where the
  panel has no gauges (`no-station`, `offline`). The headline above the map still differs by quantity (estimate vs "No
  estimate"), so the map's top moves ~60 px between Flow and Level on a forecast day.
- Not run on the emulator.
