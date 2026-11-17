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
