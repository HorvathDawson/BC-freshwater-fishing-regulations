# T497 — Lower Fraser braids carry the Fraser's name (Nicomen Slough, Maria Slough)

Status: OPEN — owner 2026-11-08 (phone, r-25a89ce9f8e6). Part of T495 gate 1 (no wrong regs).

A section shown as "Fraser River" is a braid of Nicomen Slough; same for Maria Slough. The lowest stretch reads
"Fraser River — stretch 1 of 2", as if a second 2-stretch Fraser water exists. Framing is right.

## Do
- Find the affected sections, the separate "Fraser River" item, the root cause, and how many other rivers share it.
- Say whether the rules are wrong too (slough getting the Fraser's rules, or the reverse).
- Fix at the cause; the slough keeps its own name; diff rule bindings before promoting (split fixes move the trib walk).

## Found (2026-11-08, r-25a89ce9f8e6) — the rules are wrong too
- No separate Fraser water: "stretch 1 of 2" is the Fraser's part 2 (above the CPR Bridge at Mission), split in two by
  the 138 m Landstrom Bar sign zone (part 6). The label is right; the tap landed on a slough channel inside it.
- Two unnamed FWA channels, both ends on their slough, sit in the Fraser item `gnis:39325`: blk 355991891 (3.1 km, Nicomen
  Slough 355991886) and 355992357 (2.3 km, Maria Slough 355992361). Both sloughs carry the Fraser mainstem's watershed
  code (100-000000…), so every unnamed channel there shares it.
- Rules: each channel gets Fraser r1/r3/r5/r6 — r3 (closed one hour after sunset to one hour before sunrise) and r5
  (steelhead closed Aug 15–Dec 31) — which its own slough does not have; the Nicomen channel misses the slough's 8 km/h
  speed rule.
- Cause: `pipeline/atlas/graph/names.py:59-69` — an unnamed blk inherits the "side channel" name + gnis of the
  highest-magnitude named blk on its watershed code (magnitude only, never what it connects to); the registry
  (`registry/build.py:45-62`, grouped `:313`) then files it under that gnis. `split_distinct_names` only moves channels
  with their own name. (Side note: `_wsc_gnis_map` `:35-42` keeps the first gnis seen — order-dependent, rule 46.)
- Province-wide: 6 channels (14.0 km) certain — both ends on another named sibling (Fraser: Nicomen, Maria, Morey
  Channel; Columbia: Baldy Channel ×2, Hotsprings Channel). 16 more (16.5 km) probable — one end on a named sibling,
  the other on unnamed water (Williamson Slough, Morey, Back Channel, Baldy, Hotsprings, Old Kootenay River Channel,
  Widgeon Slough). Direct end contact only, so a lower bound.

## Fix (proposed)
- An unnamed channel takes its name + gnis from the named water its ends reach, walking along connected unnamed pieces
  (a chain whose ends land on the slough goes to the slough); magnitude stays only when an end reaches the main river.
- Pin test: 355991891 → gnis:21105, 355992357 → gnis:13499. Diff rule bindings + braid_sweep before promoting.
- Needs a full atlas rebuild (item membership is in registry.json), then reaches, tiles, bundle. Section ids unchanged.
- Every moved channel listed for the owner (a proposal), with its before/after rules.
