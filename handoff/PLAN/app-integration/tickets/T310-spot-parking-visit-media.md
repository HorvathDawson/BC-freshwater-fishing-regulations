# T310 — Spots: a parking spot; notes and photos belong to visits

Status: OPEN (owner 2026-10-31: "for spots it would be nice to have the option to add a parking spot to it. Also the pictures
and text should instead be linked to the visits not to the spot overall").

## Work
1. **Parking:** a spot can hold an optional parking location (drop a pin on the map or "use my location"), shown on the spot
   page and the map (P marker), with "Directions" (opens the phone's maps app at the parking point).
2. **Notes and photos per visit:** each visit (T294) owns its notes and photos; the spot itself keeps only name, location,
   parking. Migration: existing spot-level notes/photos move to the spot's first visit, losslessly (test). Spot page shows
   each visit with its own notes/photos; add/edit per visit.
3. Words in core tables; web shows the T302 notice (unchanged).

## Acceptance
Migration test; phone shots: spot with parking + 2 visits each with notes/photos; directions opens maps.

Depends: T294. Size: M. Owner: no.
