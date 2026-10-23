# T140 — The bundle's regions of a water ask the one region membership (G3), and area families too

Status: OPEN

Source: T115 follow-ups (adversarial review 2026-10-23). Depends: T115 (done). Size: S. Rebuild: deliver (measured diff).

## Problem
T115 made `registry.regions` the one answer to "which region's members is this piece" for the reach. Three readers
still answer it themselves:
- `deliver.bundle.rules.regions_of_waters` calls `outside.water_regions(…, registry)` with NO graph, so a water's
  regions are plain polygon membership, not home membership. That decides which neighbouring region a printed
  exemption carries to (G3).
- `reach.licensing` reads the `national_parks` family itself (`f"area:{NATIONAL_PARKS}:"`).
- `deliver.bundle.licensing` (`province_except`) reads an area family itself (`f"area:{kind}:"`).

## Work
1. Pass the attached graph to `water_regions` in `regions_of_waters`; measure the bundle diff (which waters' region
   sets change, which exemptions move) BEFORE promoting; each moved exemption named in this ticket.
2. `reach.licensing` and `deliver.bundle.licensing` call `regions.area_family_sections`.
3. Extend T115's no-second-copy AST scan to `pipeline/deliver/bundle/*.py` and `pipeline/atlas/reach/licensing.py`.

## Acceptance
Measured deliver diff written into this ticket; every change explained; mutation pin (graph=None in
`regions_of_waters` fails a test).

Owner question: Q99 (default: do it, measured; no change ships until the diff is read).
