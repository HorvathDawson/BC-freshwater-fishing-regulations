# T370 — Web: the `water-{region}` slices repeat ~0.7 MB each

Status: OPEN (found 2026-11-03; after 0.5.0 unless a web gate needs it).

Every region's `water-*` slice is 0.74–0.86 MB gz, small regions included: per contract.py (T256) "its rules, statics and
keys are whole in every slice". So a web visitor's first tap pays ~0.7 MB that is the same in every region, and a second
region pays it again. Idea: put the shared part in ONE web file (fetched once, cached) and leave only the region's own
rows in the slice. Measure first (the shared vs per-region split on the live release), then decide with the owner.
Web only: a phone installs the whole `water` file.
