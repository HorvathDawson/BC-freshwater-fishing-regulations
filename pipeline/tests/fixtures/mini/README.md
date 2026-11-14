# The mini world (T667 slice 8)

Every water here is invented. The ids are in ranges of their own: blue lines from 900000001, waterbody keys from
800000001, GNIS ids from 990001, obstacles from 950000001, OSM-style ids from 970000001, the gauge `99MA001`, and the
code root is `920-123456`.

- `world.json`: the waters as lines and boxes, plus everything a fetch would bring: the access land (`land_access`,
  `aboriginal_lands`, `parcels`), the watershed groups, one gauge with its record and envelope, two places, the
  synopsis's period line, the HYDAT release and one obstacle. `python -m pipeline.tools.mini_release` writes from it,
  into scratch: an FWA-shaped GeoPackage, the coast pair and the obstacles file (`write_inputs`), and the fetched and
  fed tree the deliver steps read (`write_source`: roster, places, outline, a one-page synopsis PDF, HYDAT, clim.json,
  an empty typed DFO feed).
- `curated/`: a whole curated tree in the layout of `data/curated`, holding only invented rows. The build reads it
  through `CURATED_ROOT`. Copied as they are, because they are the book's words and name no water: the book's
  `definitions.md`, `licensing.md` and `water-specific-tables.md`, the DFO limits wording, and the provincial `bait.r5`
  text. `zp:steelhead` is the province's entry, with its regions narrowed to the mini world's two.

What it holds (pinned by `test_mini_release.py::test_the_mini_world_holds_the_plans_inventory`): Fixture River (a
tidal mouth 1 km into the sea, Fixture Lake on it, Twofold Lake across the region line, a braid, a gauge, Sample
Reserve 1 over it), Mock Creek (its falls a cut, closed all year above them) with an unnamed trib, Stub Creek (its own
release beside the zone's 50 cm clause, its row lifting the zone's summer closure; Mock Range no-access land over it)
with the added Mockup Creek (closed whole), Sample Lake, Mockup Marsh, one unnamed wetland, one reservoir; 15 entries.

Build it into scratch only. `--twice` builds it two times and checks that the digests agree:

    scripts/heavy.sh mini-release 30 -- .venv/bin/python -m pipeline.tools.mini_release --build <scratch> --twice

The chain stops after the status index: the export is the book's (ticket T667 "Slice 8, step 2b/3 built").

To change the world, edit these files. Never copy live data in.
