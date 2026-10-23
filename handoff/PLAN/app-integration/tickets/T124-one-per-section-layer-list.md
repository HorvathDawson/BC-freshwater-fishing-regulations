# T124 — One per-section layer list: three hand-typed lists beside `SECTION_SOURCE_LAYERS`

Status: OPEN.

Source: owner 2026-10-22 (unify checks that decide the same thing); unification audit 2026-10-22, findings app 3 and 4
(CONFIRMED, latent).

## Decision being unified
"Which tile source layers carry per-section features?"

## Where it is decided today
- `pushedRoute`: `{stream, lake}`.
- `pushedStandings`: `{stream, lake, wetland_flowing, basin}`.
- `statusData`: `{stream, lake, wetland}`.
- Three more hand-typed lists beside `SECTION_SOURCE_LAYERS` in `app/packages/map/src/`.

How they differ: a wetland is pushed for standings but not for the route; `wetland` vs `wetland_flowing` differ by
list. A new section layer silently drops out of whichever list is not updated.

## Work
- `perSectionLayers()` in `app/packages/map/src/sections.ts`; the lists become aliases of it, or subsets with a named
  reason and a set-equality test.
- Pin: extend `check-boundaries` or a one-* test so no module spells layer-name arrays outside `sections.ts`.

## Acceptance
A test that every list equals `perSectionLayers()` or its declared subset. App shots unchanged.

Rebuild: none (app). Depends: none. Size: S. Owner: no.
