# T823 — "Small next to its gauge": Python decides when a section's lead gauge reading needs a warning; the release ships it

Status: OPEN (owner 2026-11-16; data half of the app chat's spots cleanup).

Source: owner 2026-11-16 (relayed by the app chat, spots cleanup), on a 1.4 km² creek shown "Below normal" from a
gauge 676× bigger: "Could we just use our gauge network that colors stuff wtc to determine if it is significant
enough? It would drop that in theory. Then ofr small ones still give a warning if used."

## What
- A spot (and any card) takes its gauge from the section's lead in `section_gauge` (the network the Conditions map
  colours with; the drainage gate already applied). No lead = no estimate.
- Python decides, per section with a lead, whether the water is "small next to its gauge" and ships it in the
  conditions file (a field on `section_gauge`, e.g. `small` or the ratio band), with the sentence the app shows as
  the warning written by Python. The app computes no ratio and no threshold (owner 16:50: Python decides every word
  and number).
- The threshold is policy (memory: gauge representativeness is policy): propose it from the drainage areas
  (`gauge.area_km2`, the section's own drainage) with examples, for the owner's word before it ships.

## Built 2026-11-16 (lane/t823-small-water) — threshold PENDING OWNER
- `pipeline/gauges/consume/small_water.py`: a led section is small when its lead gauge drains at least N times its own
  area (both modelled by the panels' model: the section's panel area first, so side channels count as their river; the
  gauge at the node it reads). Only the small direction. Switch `SMALL_WATER_POLICY`: `x50` (default, recommended: the
  network's own `weak` band in area), `x100` (trust ladder's `distant`), `x200`. A change is a bundle rebuild.
- Ships as `section_gauge.small` (conditions/1, additive, last, default 0) + conditions meta `small_water_text` /
  `small_water_ratio`; keys emitted to `gauge-policy.generated.ts`. The release refuses a bundle built before it.
- Live bundle 2026-11-16, 114,428 led sections (24,667 named, 5,216 waters): x50 ~93k (12.3k named, 3,750 waters),
  x100 ~76k (8.5k named, 2,938), x200 ~44k (4.1k named, 1,690). Every lead is under ~357x (the network's floor).
- The owner's creek: unnamed Chilliwack-valley sections of 1.4 km² (sid 63623 …) are NOT led by 08MH001 (676x, below
  the floor): no estimate. Its neighbour 63923 is led by Slesse Creek 08MH056 at 131x: warned at x50 and x100.
- Owner to pick: the threshold and the sentence ("This stream is much smaller than the river its gauge measures: the
  reading may not match it."). App half: show the meta text where `small` = 1.
- Seen, not this ticket: 251 led sections are 10x+ BIGGER than their lead (lower Fraser side channels led by Red Pass
  08KA007 750-1,300 km up; Homathko, Similkameen, Kootenay, Bridge) — a line magnitude of 3 passes the trust floor.
