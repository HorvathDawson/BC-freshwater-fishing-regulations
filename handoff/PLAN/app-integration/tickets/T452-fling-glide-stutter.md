# T452 — A fling's glide stutters as it goes (Pixel 9, 0.6.6)

Status: IN REVIEW — e63c921c3 (the glide's shape changed: owner feel check on the next APK)

Owner: "the panning is a bit better, zoomed out and in are similar now; still feels a little stuttery — if you flick
the screen the way it moves stutters a bit as it goes."

## Work
1. Measure the glide's frame pacing on the NORMAL emulator (not only the stressed one): on a fast phone a stutter can be
   uneven camera steps rather than slow frames — per-frame camera deltas and timing (MapLibre's fling animator vs the
   frame callbacks), work on the UI/JS threads during the glide (the binding's per-frame events, tile events, renders).
2. Fix what the measurement names; before/after numbers on the same script.
Related: T312 (fling length: the binding's `tuneFling`), T420 (pan lag). Size: M. Owner: a phone feel check after.

## Found and fixed (2026-11-06, e63c921c3)
**Measured** with a camera probe in the binding patch (`adb shell setprop log.tag.CifCam D`: each camera sample, frame,
vsync, lift, settle) and test bed `glide` (8 flings, 300 px in 120 ms ≈ 990 dp/s, NORMAL emulator, Fraser Valley z11).
**Cause:** MapLibre's fling (`moveBy` with a duration, its bezier ease) places the camera when the map thread happens to
run, not at the frame's vsync: samples landed 2–16 ms after their vsync, so the step per frame jittered even when frames
were on time; 1 in 7 sample gaps was over 20 ms; and the glide's first frame moved at ~half the release speed (a hitch at
lift). The binding's per-frame events to JS (onRegionIsChanging / frame events, sent even with no JS listener) cost some
too (experiment: gaps >20 ms 51 → 35 with them off) but the sampling was the bigger part and is gone with the fix below.
**Fix (binding patch, Android):** MapLibre's fling animation off; the binding glides itself on Choreographer: each
frame's camera from that frame's vsync time (`scrollBy`, no animation), starting at the release velocity and decaying
exponentially over Android's own fling distance (OverScroller spline, T312's length); a finger or an app camera move
stops it; one `onRegionDidChange` at the end (MapLibre's cached camera refreshed via `MLRNCameraCache`, a class in
MapLibre's package — pinned to 13.6.1).
| 8 flings, same script | before (MapLibre fling) | after (own glide) |
|---|---|---|
| sample gaps > 20 ms | 51 / 434 | 0 / 584 |
| steps > 25 % off their neighbours | 39 / 255 | 0 / 240 |
| sample phase after vsync, p5–p95 | 2.3–16.1 ms | 0.4–1.6 ms |
| first-frame speed (release 990 dp/s) | 432 dp/s | 886 dp/s |
| map surface presents > 20 ms | 30 / 301 | 46 / 551 |
| glide duration / travel | 671 ms / 191 dp | 1248 ms / 176 dp |
| settles per fling | 1 | 1 |
Runs: `out/runs/t452-a` (before, experiment build), `t452-c` (after, committed patch).
**Proposal (owner's call):** the glide's SHAPE is different: it starts at the finger's speed and eases out longer and
softer (≈1.25 s vs 0.67 s at this speed, similar distance) — more like a list or Google Maps. If it feels too floaty,
the length is one number (`platformFlingDp`) and the decay curve can be shortened.
**Settle camera checked (2026-11-06, test bed `feel`, `out/runs/t452-feel`):** the scenario now also reads the binding
probe's last `m` sample (the map's real centre off its projection) per gesture. Flings at 1429 / 952 / 571 dp/s: the
settle's reported travel 459 / 258 / 182 dp = the real travel to the dp (err 0), the reported centre 0.3 dp from the
real one each time (constant: no stale settle). The 2286 dp/s fling had no baseline (the slow drags log no probe `m`
samples, a probe gap, not the settle's); drags ×0.97 of the finger as before; arrivals settle as before. No fix needed.
Not verified: iOS (no change there); a real Pixel 9 (owner's feel check on 0.6.7).
