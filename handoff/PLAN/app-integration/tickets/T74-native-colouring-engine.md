# T74 — Native colouring engine, plus the native map gaps (audit N4, APP-PREP N2-F1)

Status: OPEN.

Source: `map/colouring.ts` has `coloursEachTile = platform === "web"`; `ui/lakeColour.ts`: native "has no per-tile event
yet … keeps its lakes plain"; native still uses the pushed "status" mode (wave item 2.19). T50 refers to "the native
engine ticket" — this is it. N2-F1: hatches (`no_access` and the others) and the gauge pill are plain on native (RN has
no addImage). expo-location is not in apps/mobile, so "At my location" does not work on native.

## Work
- A per-tile source event through the MapLibre Native binding; `coloursEachTile` true on native; delete the pushed
  "status" mode and the documented split.
- Hatch/pill PNGs into the release map-assets sprite, registered through `Images`.
- Add expo-location (pinned; deps.md line).

## Acceptance
Emulator pans: 0 grey frames, numbers recorded (phone numbers come from T77). Lake modes coloured on native; the five
hatch layers draw; native "At my location" works.

Depends: T50 (emulator setup). Heavy (emulator), scripts/heavy.sh. Size: L. Owner: no.


## Owner 2026-10-27
Native gestures and navigation must MATCH the web app (see T50 note, T214).
