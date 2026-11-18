# T853 — One command draws the Nass-style rules map for any water (salmon or freshwater)

Status: OPEN (owner 2026-11-18 14:12, verbatim: "could we make a script that generates the nass style images for any stream
we want? for both salmon or freshwater regs?").

The Nass maps (2026-11-18; app chat scratchpad nass-map/py: extract, render, zoom) were drawn from a release's bundle,
answers file, salmon files and the tiles' stream layer: each part coloured by today's answer, breaks named with km, and a
numbered cascade panel per part (region base -> the water's own rows -> DFO -> today's answer). Make it a repo tool: give it
a water (name or item id), a release (default LATEST), a date (default today) and a mode (salmon | freshwater | both); it
finds the system (mainstem + named tributaries with their own rows), lays out callouts without overlap, adds a zoom inset
when parts are small, and writes PNGs. Read-only on data; no network needed beyond the release on disk.
