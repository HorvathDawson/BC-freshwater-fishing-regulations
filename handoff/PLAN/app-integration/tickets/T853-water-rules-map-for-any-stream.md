# T853 — One command draws the Nass-style rules map for any water (salmon or freshwater)

Status: OPEN (owner 2026-11-18 14:12, verbatim: "could we make a script that generates the nass style images for any stream
we want? for both salmon or freshwater regs?").

The Nass maps (2026-11-18; app chat scratchpad nass-map/py: extract, render, zoom) were drawn from a release's bundle,
answers file, salmon files and the tiles' stream layer: each part coloured by today's answer, breaks named with km, and a
numbered cascade panel per part (region base -> the water's own rows -> DFO -> today's answer). Make it a repo tool: give it
a water (name or item id), a release (default LATEST), a date (default today) and a mode (salmon | freshwater | both); it
finds the system (mainstem + named tributaries with their own rows), lays out callouts without overlap, adds a zoom inset
when parts are small, and writes PNGs. Read-only on data; no network needed beyond the release on disk.

## Done (branch worktree-agent-a619c74a4ba892f13)

    .venv/bin/python -m pipeline.tools.water_map "<water name or item id>" [--release DIR] [--date YYYY-MM-DD]
        [--mode salmon|freshwater|both] [--out DIR] [--focus KM-KM] [--max-tribs N] [--layers DIR]

`pipeline/tools/water_map/` (doc in its `__init__`; MAP.md row). Reads only the release (files proved against the
manifest sha256; answers via `province_store` + `encode.tap`, DFO via `salmon.decode_files`) and the tile layers (refused
when their section handles differ). System = the watercourse (`water_continue`) + tributaries by `water_exit` carrying a
BC row or a DFO place of their own (14 numbered, the rest named in a note); a lake brings its river's near parts. A water's
parts that read the same in a mode join one panel. A main water with >16 stretches in a mode refuses and lists the parts
for `--focus`. Zoom insets go to whichever column keeps the image shorter. Tests: `pipeline/tests/test_water_map.py`
(mini release; one `needs_bundle` test on the live Nass). Proofs (Nass, Vedder/Chilliwack, Kamloops Lake, 2026-11-18,
r-f1f2e8da9666) in the session scratchpad `t853/`.
