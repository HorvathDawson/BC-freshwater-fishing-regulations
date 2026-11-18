# T853 — One command draws the Nass-style rules map for any water (salmon or freshwater)

Status: OPEN (owner 2026-11-18 14:12, verbatim: "could we make a script that generates the nass style images for any stream
we want? for both salmon or freshwater regs?").

The Nass maps (2026-11-18; app chat scratchpad nass-map/py: extract, render, zoom) were drawn from a release's bundle,
answers file, salmon files and the tiles' stream layer: each part coloured by today's answer, breaks named with km, and a
numbered cascade panel per part (region base -> the water's own rows -> DFO -> today's answer). Make it a repo tool: give it
a water (name or item id), a release (default LATEST), a date (default today) and a mode (salmon | freshwater | both); it
finds the system (mainstem + named tributaries with their own rows), lays out callouts without overlap, adds a zoom inset
when parts are small, and writes PNGs. Read-only on data; no network needed beyond the release on disk.

2026-11-18: built on branch worktree-agent-a619c74a4ba892f13 (203ecf76d), not merged. `python -m pipeline.tools.water_map
"<name or item id>" [--release DIR] [--date] [--mode salmon|freshwater|both] [--out DIR] [--focus KM-KM] [--max-tribs N]`;
reads only the release files (manifest-checked), answers through the existing store reader and salmon decoder; 11 tests in
pipeline/tests/test_water_map.py. Proof images (Nass, Vedder/Chilliwack, Kamloops Lake; both modes) sent to the owner. On
the current release the Nass gauge break still reads "Canyon City bridge (gauge 08DB001)" (T850) and the Nass trout lines
still keep lake trout (T854) until the new data release; the tool shows whatever the release holds. Merges with 0.8.1.
