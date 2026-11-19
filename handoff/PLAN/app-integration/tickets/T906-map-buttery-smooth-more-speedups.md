# T906 — The map buttery smooth: measure more speed-ups beside T903

Status: IN PROGRESS (main chat; emulator lane with the T903 rerun; each candidate lands only on a measured win).

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
