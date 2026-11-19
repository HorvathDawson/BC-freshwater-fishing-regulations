# T864 — A tester far from B.C.: location must not glitch; say plainly it is not useful there

Status: OPEN (app chat; before T863's closed/external testing).

Owner 2026-11-18 ~18:02, verbatim: "we need to make sure a tester outside of british columbia doesnt have it glitch out too
with location. we could give a little note saying you are far away from bc location will not be usefull etc etc"

Today: the camera is held to `CAMERA_BOUNDS` (core/src/ladder.ts, B.C. plus 8 degrees of slack), the basemap and atlas are
clipped to B.C. Nothing known says what "locate me", the you-are-here pin, "near you" lists (Conditions, search's near
rows), spots' weather and distances do when the position is far outside (Toronto, Seattle, London, the far side of the
date line).

Work:
1. Audit every reader of the device position (web and phones) with positions far outside B.C.: no crash, no fly to a
   blank screen, no camera fight with the bounds, no nonsense distances ("3,400 km").
2. When the position is outside B.C. (beyond a small margin, so a border town in Alberta/Washington still works), show
   one short, calm note in the owner's voice, e.g. "You're outside British Columbia. This app only covers B.C. waters, so
   your location won't be much use here — the map and regulations still work." and keep the map on B.C. Proposal; the
   owner picks the words.
3. Tests with fake positions; emulator / simulator / web shots for the owner.

## 2026-11-18 ~20:24: wording kept (owner)
Owner, in the app chat's dialog, picked "Yes, keep it" for: "You're outside British Columbia. This app only covers B.C.
waters, so your location won't help here — the map and regulations still work." Built on app/t864-far-location
(66901c3e1, 7f929431c, 44075496d), reviewed (no defects), in app/integrate-081-web.
