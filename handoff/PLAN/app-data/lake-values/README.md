# Lake modes' values: Stocked and Depth — prepared, not fed (2026-10-21)

Owner rule: stocked-lake colouring and the depth (surveyed) colouring go through the ONE colouring pathway, and their
colours update from data alone — no tile change.

Owner decision, same day (`handoff/PLAN/app-integration/decisions.md`, "lake Stocked / Depth modes: prepared, not fed
yet"): "we dont have the stocking and bathy map data in a feed yet. but we just want it prepared for it when we do have
the feed. we even might want the toggles grey/disabled for now." The first cut of this lane fetched FFSBC's stocking
report (gofishbc.com wp-json, no open licence) and bound the BC Data Catalogue's survey-sheet rosters ("Access Only").
The correction pass removed the feed and kept the preparation.

## What is kept (the preparation)

- **The release slot.** An optional file `lakes` (format `lakes/1`, kind bin, size gate 100 KB) in the release contract
  (`pipeline/deliver/release/contract.py`: `FILES["lakes"]`, `OPTIONAL = {"lakes"}`) and the app's generated copy.
  `release.build(lakes=...)` ships it only when handed a file, refusing one built against another atlas;
  `python -m pipeline.deliver.release --lake-facts FILE` is the only way in. `deliver all` passes none.
- **The format and the binding rule** (`pipeline/deliver/lake_facts.py`): `encode` / `decode` / `handles_of`, `bind`,
  `core` and `facts` — source-agnostic, tested on synthetic input (`pipeline/tests/test_lake_facts.py`). Its docstring
  says what a future feed must provide (below).
- **The app's reader**: `@app/core` `decodeLakeFacts` (now also counting `surveyed` / `stocked` sections),
  `@app/data` `ReleaseLoader.lakes()` / `WaterSource.lakeFacts()` (null for a release without the file, the bundle and
  the fixture), `@app/ui` `useLakeColour`.
- **The engine's two channels**, `survey` and `stocked` (`@app/map` `colouring.ts`; `MapProps.lakes`), and the tests
  that a release carrying values recolours lakes with no tile change: `colouring.test.ts` (the lake channels),
  `one-engine.render.test.tsx` "A NEW RELEASE ALONE recolours the lakes", `tools/one-colouring-engine.test.ts`
  (pushed `stocked` / `survey` refused). All on synthetic facts.

## What is removed (the feed)

- `pipeline/stocking/fetch.py` (the FFSBC download) and its `data/.gitignore` entry.
- `python -m pipeline.deliver lake_facts`, its step in `deliver all`, its live home `lake_facts.bin`, and the release
  CLI's default of reading that file. `deliver all` neither builds nor needs one; the release ships no `lakes`, so its
  size is unchanged.
- The source adapters (`surveys`, `last_stocked`, `fwa_lakes`, `build`, `main`) and the live test that read the FFSBC
  roster; the screenshots and JSON numbers taken from a release built with FFSBC data.

## The toggles

While the loaded release carries no lake values, the layers menu greys Depth and Stocked: not pressable,
`aria-disabled` (AGENTS 34 — `accessibilityState` is dead on web), and one line under the choices, "No data yet for
Depth and Stocked". The gate is per mode (`@app/ui` `lakeModesReady`: Depth needs a surveyed section, Stocked a stocked
one) and follows what the map is handed, so a release that carries values enables them with no app change. A choice made
while values existed draws plain if they go away (`lakeModeOn`). Off a proven pair, and on the native map (no per-tile
labelling yet, N2), the map is handed nothing, so the modes stay grey there too: a mode the map cannot colour is not
offered. Tests: `@app/ui` `lakeModes.test.ts`, `@app/ui-native` `LayersSheet.test.tsx` "the lake modes' gate".

## What a future feed must provide

- A licence that permits redistributing a derived per-lake value.
- Per water: an identifier the FWA lakes layer carries — the 50K waterbody identifier is
  `WATERBODY_KEY_GROUP_CODE_50K` in the lakes layer of `bc_fisheries_data.gpkg`, and the lake's section is
  `lake:<WATERBODY_KEY>` in the atlas's handle table — plus the water's name(s).
- Survey: none / sheet / digitised. Stocking: release dates; the file keeps the latest on or before the feed's as-of
  day, and that day.
- The output is keyed by section_id (integer handle) and pinned to the bundle's `section_handles`.
- **The binding rule learned**: a name binds only the lakes it names under a shared 50K code, plus that code's unnamed
  parts — never a differently-named neighbour (the first cut coloured McCombe Lake from Buntzen's record, Rosemond from
  Mara's, Pigeon from Meadow's). When the FWA names none of the code's lakes, all bind; a code whose names all disagree
  is refused. Every verdict is counted.

## Not done

- No feed. When one exists: a producer writing a `lakes/1` file, then `--lake-facts` in `deliver all`'s release step
  (and its `test_deliver_atomic` pin flipped).
- Native (N2): the phone map takes pushed data only; its lake modes stay grey.
- Search's `stocked` signal and the lake sheet's `lakeInfo` / `stockingHistory` are still empty in the release reader.
- The Layers sheet's "Live data" list still names "Stocking releases" as a source; it predates this lane.
