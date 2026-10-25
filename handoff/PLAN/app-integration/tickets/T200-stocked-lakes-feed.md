# T200 — stocked lakes: feed the prepared Stocked mode

Status: OPEN

Source: owner 2026-10-25 ("at the end, alongside the stocked and bathy maps"); decisions.md "OWNER 2026-10-21 — lake
Stocked / Depth modes: prepared, not fed yet".

## State
The release's optional per-lake values slot (contract + reader) and the colouring engine's stocked ramp exist; the
Stocked toggle is disabled with "no data yet"; the bundle's stocking tables are empty (stock_water, stock_code, release).
T158: the cvd stocking ramp (navy → pale yellow) does not hold against the no-access line colour.

## Work
Pick the source (FFSBC stocking reports / BC open data), its licence and refresh cadence (owner); a fetch + parse into
curated data; bind stocked waters to lake items at delivery; fill the slot; enable the toggle; fix the cvd ramp
conflict (T158); tests + size diff; owner look.
