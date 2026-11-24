# T906 — The map buttery smooth: measure more speed-ups beside T903

Status: IN PROGRESS (main chat; emulator lane with the T903 rerun; each candidate lands only on a measured win).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T906 | The map buttery smooth: measure more speed-ups beside T903 (owner) | IN PROGRESS | main chat | owner 2026-11-19 ~06:50 |

Owner 2026-11-19 ~06:50 (main chat), verbatim: "could we test oother speed ups at same time? we want the map buttery
smooth ideally".

## Work
In the same emulator session as T903's clean rerun (one emulator, interleaved arms, ≥ 6 rounds, stressed + unstressed,
patched MapLibre): profile pans, pinches and mode switches on Salmon / Trout / Conditions from the traces, list the
costs (tile parse, layout, symbol placement/collision, expression evaluation, feature-state writes, JS thread), then
try the cheap candidates the traces point at behind map-lab switches (e.g. fewer symbol layers or labels at low zoom,
label fade off, no cross-source collisions, simpler casing at low zoom, line layers merged), each measured against base.
A candidate lands only if it wins without a regression and the look is unchanged (or the owner approves the look).
Related: T903, T876, T861, T855, T848, T420, T699.

## 2026-11-19 ~10:45: where the time goes (from the T903 rerun's traces) — no candidate measured yet

Not done in the T903 session: every candidate needs a new lab switch, so a new APK, and the T903 A/B took the slot.
What the traces already say (stressed emulator, Salmon, Fraser Valley; `scratchpad/t903/t906/perf-sub.txt`,
`scratchpad/t903/rerun/`):
- **Quick pinch** (perfetto, one 9.6 s window, base): MapLibre's 4 tile workers 7.4 s CPU (tile parse + layout,
  including data-driven paint evaluation) > its render thread 4.2 s > the Android UI thread 0.6 s > our JS 0.5 s. About
  45 % of the render thread's samples are the emulator's GPU pipe (`write`/`read` to gfxstream), which a phone does not
  pay; the rest is in libmaplibre, which is stripped, so layout vs symbol placement vs expression evaluation cannot be
  split from these traces.
- **Pans** (24–48 flings per mode): ~600 ms of slow frames (> 50 ms) per fling against ~30 ms of JS stalls, no style
  swaps, ~13 % of frames drawn with a tile missing. Pan jank here is native (render + workers), not our JS or
  feature-state writes.
- **Mode switch** stays ~1–1.5 s stressed (~400 ms unstressed) per leg: the style swap and relabel.
Next: (1) a symbolised trace (the patched AAR's unstripped `libmaplibre.so` for build 7bf3dc29…, fed to trace_processor)
to rank layout / placement / evaluation; (2) then the cheapest candidates as lab switches in ONE new APK, interleaved
like T903: symbol layers off below a zoom, label fade 0, stream line layers in one bucket (`streamsOneBucket` already
exists), simpler casing at low zoom. Measure with the fixed `mode-switch` pan reading (raw frames inside each fling's
window) — the old capped summary counted the switch's own frames as pan frames.
