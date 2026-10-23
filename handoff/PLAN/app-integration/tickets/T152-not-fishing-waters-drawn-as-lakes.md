# T152 — Sewage basins, tailing ponds and dumps are drawn as fishable lakes

Status: OPEN

Source: T148 census (handoff/PLAN/sloughs/v2/census.md table B), 2026-10-23.

## Problem
All 275 FWA manmade polygons coded EA26700110 (sewage settling basin, 214), AP90300100 (mine tailing pond, 59) and
AP09200000 (dump, 2) are in the lake tile layer (minzoom 8–14) and draw as lakes. None is an item, named, gauged or
bound by a named-water rule, but 12 Fort Nelson sewage basins carry the "Liard River watershed" area rule and 4 tailing
ponds sit on named creeks (Alkali, Pukaist, MacDonald, Wilfred; 93–231 ha, visible from z8). The app tells an angler
these are lakes with the region's rules.

## Work
One decision (registry: a water's fishing status from its FWA feature code): these codes are NOT_FISHING_WATER — not an
item, never bound by an area/zone rule, drawn as a non-water (or a distinct "not fishing water" fill) in the tiles, never
searchable. A pinned list of the codes. Measure: bindings removed (area rules), tiles change (rides the tiles window).

Rebuild: atlas registry + reach + deliver; tiles (tiles window). Owner question: draw them at all (default: a muted
non-water fill with no tap)?
