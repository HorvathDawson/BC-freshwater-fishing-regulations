# T201 — bathymetry: feed the prepared Depth mode and draw contours

Status: OPEN

Source: owner 2026-10-25 ("at the end, alongside the stocked and bathy maps"); decisions.md "OWNER 2026-10-21 — lake
Stocked / Depth modes: prepared, not fed yet"; T158 (tiles carry no contour layer — a follow-up of its own).

## Work
Pick the source (BC lake bathymetric maps / FFSBC surveys), licence and coverage (owner); fetch + parse per-lake depth
values into the release slot and enable the Depth toggle; a contour layer in the tiles (tile-size budget, T64/T85
numbers) drawn per T158's no-access-vs-contour rules; tests + size diff; owner look.
