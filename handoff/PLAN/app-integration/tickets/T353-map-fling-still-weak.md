# T353 — Map fling still too weak on 0.4.0 (Android)

Status: DROPPED — folded into T312, the one map-feel ticket (owner 2026-11-03). Was: OPEN (owner 2026-11-02: "the flick gesture movement on map still isn't enough on 0.4.0 mobile").

After T312's fling tuning, a quick flick on the map still travels too little and stops too soon compared with Google
Maps or Apple Maps.
- Measure the fling distance per flick velocity on the emulator (test bed pan scenario) and on the owner's phone.
- Tune MapLibre RN's deceleration, velocity multiplier and max fling to feel natural (1:1 finger tracking, then a long
  glide).
- Check iOS too.
- Make sure no JS work runs per camera tick.

See T312. Size: S.
