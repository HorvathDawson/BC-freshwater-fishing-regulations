"""steelhead — the KNOWN-STEELHEAD LIST, built from evidence (`known_waters`).

A separate step, NOT part of the atlas build or the reach run: `python -m
pipeline.regs.steelhead.known_waters` fetches (once, cached under `data/source/steelhead/`) the
province's steelhead observations and fish-accessibility model, applies the hand review kept in the
module (REVIEWED data, AGENTS 35), and writes `data/generated/steelhead/`. The owner accepts its
`steelhead_waters.json` with `python -m pipeline.regs.steelhead.accept` (prints the waters added / dropped; `--write`
writes `data/curated/waters/steelhead_waters.json`, T324); the reach builder reads
only the curated copy (`pipeline.atlas.reach.steelhead.load_list`). The list is a presence indicator
for display — it binds no regulation.

Submodules are imported directly; this file stays empty so `python -m` does not double-import.
"""
