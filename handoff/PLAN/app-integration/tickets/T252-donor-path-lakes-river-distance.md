# T252 — donor paths through lakes, distance along the river, station names cased

Status: PARTIAL — code done 2026-10-28; rides the next bundle + release (not run here); the Pitt check on :39218 waits
for that release.

Source: follow-ups to the Conditions donor-path work (9ea333d1), 2026-10-28: (1) a flow path stopped at a lake — the
upper Pitt (gnis:7551) above Pitt Lake showed "no mapped path" to 08MH006 on the North Alouette below the lake;
(2) "joins at <confluence> · <N km along the river>" on the map and in the relation line, not the straight line;
(3) "Mcgregor": ECCC station names re-cased by hand instead of the shared caser (T113).

## Built

**1. Pointers closed downstream** (`pipeline/gauges/consume/shed.py` `downstream_map`). `section_down` was seeded with
shed members only, and a pointer's target had no pointer of its own unless it was a shed member too — a LAKE never is
(`lake_gauge` reads lakes), so every walk into a lake ended there. Now each target gets its own pointer, to the sea or
the edge of the atlas; a lake's is its `lake_out` edge. `_down_one` also ranks edges for a walk: never a channel that
flows straight back (the upper Pitt's braids leave and rejoin piece `356360869:46555`, and the walk went round them
and stopped), then the river going on or into its lake (`continuation` / `lake_out` / `lake_in`), then the bigger
water (stream magnitude: Pitt Lake's outlet is the Pitt, not the side channel beside it), then the lowest id.
Paths only: nothing in the build reads `section_down`; it is written after `gauge`, `section_gauge`, `lake_gauge`,
the panels and the basins, from the same graph, so none of them can change (gauge choice is policy, untouched).

MEASURED_POINTERS

**2. Distance along the river.** `section_down.len` (ADDITIVE, nullable, still down/1): each reach's length along the
flow path in units of `DOWN_LEN_UNIT_M` (shed.py ↔ app `sql/shared.ts`) — a stream piece's own length, a lake's
through-line measure window (`windows.polygon_window`), else the straight line from its inflow to its outlet (Pitt
Lake: 25.8 km). The app sums a route (`alongMetres`: the spot at the middle of its reach, a station at the downstream
end of its own) into `PanelRoute.alongM`, and names the tributary whose mouth the confluence is (`joinWater`,
`joinSide`). The relation line says "… which joins this water at the mouth of Alouette River, about 34 km along the
river"; a gauge on this water's line "about 7.0 km downstream on this water's line"; the straight line only where no
path joins them. The map marks the confluence (`trace-join` + `trace-join-label`: "joins at the mouth of … · about N km
along the river") at the tributary's end against the confluence reach, found in the TILES' geometry
(`trace-geometry.junction`; no second geometry store). A release cut before `len` reads as before (no distance).

SIZE_NOTE

**3. Station names cased once.** `gauge.name` is written through `place_names.display_case` (T113's one caser:
"McGregor River at Lower Canyon", "North Alouette River at 232nd Street, Maple Ridge"); core `stationWater` keeps a cased
name as it comes and title-cases only an older release's capitals.

## Tests
`pipeline/tests/test_gauge_shed.py` (`TestDownstreamThroughLakes`: a walk passes the lake; seeds unchanged; lake
through-line and straight-line lengths); `app/packages/data/src/sql/tidal.test.ts` (T252: the sums, the tributary's
side, no lengths → no distance); `core/routeMap.test.ts` (relation words, `joinMapLabel`, cased names);
`map/trace-geometry.test.ts` (`junction`, the join point); `ui/panel.sides.test.ts` (mark + relation from the VM).

## Left
- The next bundle + release (rides the combined build): then `test_a_built_release_is_exactly_the_contract` sees `len`,
  and the `down` gate is set from the measured size (provisional now).
- Verify on :39218 after that release: Pitt above the lake (e.g. `356360869:46555`) and below it (`356360869:3342`) —
  08MH006 drawn through Pitt Lake and the Alouette mouth, with the confluence label and the along-river km.
