# T370 — Web: the `water-{region}` slices repeat ~0.7 MB each

Status: OPEN (found 2026-11-03; after 0.5.0 unless a web gate needs it).

Every region's `water-*` slice is 0.74–0.86 MB gz, small regions included: per contract.py (T256) "its rules, statics and
keys are whole in every slice". So a web visitor's first tap pays ~0.7 MB that is the same in every region, and a second
region pays it again. Idea: put the shared part in ONE web file (fetched once, cached) and leave only the region's own
rows in the slice. Measure first (the shared vs per-region split on the live release), then decide with the owner.
Web only: a phone installs the whole `water` file.

## Measured 2026-11-04 (live release r-ac5a4e4f734c)
Eleven of the slice's 21 tables are byte-identical in all nine slices: `rule` (~331 kB of the repr), `display_rule`,
`zdict`, `static`, `ruleset`, `lic`, `akey`, `akey_region`, `place`, `seglist`, `segmom`. Region 6 split as a test:
shared part 623 kB gz, own part 217 kB gz (slices are 736–861 kB gz today).
- First region a visitor opens: no saving (~0.62 + ~0.15–0.22 MB, two fetches instead of one).
- Each further region: ~0.15–0.22 MB instead of ~0.78 MB.
- All nine: ~7.1 MB today → ~2.1 MB.
Recommendation: after 0.5.0 — it does not make the first tap faster, only a second region. Owner decides.
