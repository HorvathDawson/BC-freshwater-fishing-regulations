# T370 — Web: the `water-{region}` slices repeat ~0.7 MB each

Status: DONE in code (owner 2026-11-04: "we should fix this here", in 0.5.0); live with the release rebuild.

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

## Done 2026-11-04
Pipeline `slices.py`: `water-shared` (water-shared/1: akey, akey_region, ruleset, seglist, segmom, rule, display_rule,
lic, static, place, zdict + the str rows they name) beside the slices; a water slice (water-slice/2) holds only its
region's own rows. Contract + pack regenerated; gates water-<r> 1.1 MB → 400 kB, water-shared 850 kB; the tap moments
list `water-shared`. App `focus.ts`: a read of only region-wide tables → `{shared}`; mixed with a keyed table → the
whole file; `loader.dbFor` sends `{shared}` to `water-shared` (a release cut before it: any slice, as before) and tries
it for strings. Cut from the live release (scratch): water-<r> 97–222 kB gz, water-shared 640 kB; nine regions 1.96 MB
(was 7.1). `conformance/src/regs/slices.release.test.ts` passes on that cut (whole core/water refused) and on the live one.
LEFT: `app/tools/request-walk.pin.json` gained `water-shared` BY HAND beside each `water-<r>` — re-pin with the real
walk (`node app/tools/request-walk.mjs --pin`) on the rebuilt release at the batch 7 cut.
