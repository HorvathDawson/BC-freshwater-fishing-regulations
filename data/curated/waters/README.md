# `waters/` — the shape of the atlas, and lists of waters

Read by the atlas build (cuts, names, areas, added waters, tidal limits, sloughs) unless noted. Atlas inputs change
the map only after the next atlas build (`pipeline build --atlas --promote`).

| file | kind | written by | read by |
|---|---|---|---|
| `cuts.json` | authored | hand; review app write-through (split / entry / DFO marks) | atlas sectionizer; review app |
| `name_variants.json` | authored | hand | atlas registry (names on FWA waters) |
| `found_names.json` | promoted | `pipeline.tools.mined_names` / write-through (Names marks) | atlas registry |
| `found_name_calls.json` | authored | hand (owner calls on single found-name candidates) | `pipeline.tools.mined_names` |
| `area_closures.json` | authored | hand | atlas (cuts streams at area edges); tiles |
| `added_areas.geojson` | authored | hand-drawn polygons | an `area_closures.json` def with `file` |
| `added_lakes.geojson` | authored | hand-drawn polygons | atlas |
| `added_streams.json` | promoted | the added-streams resolver (never rerun casually: negative `blk` ids are an ABI) | atlas |
| `continuations.json` | authored | hand | bundle (`water_continue` gate) |
| `tidal_limits.json` | applied | `pipeline.tools.tidal_limits` / write-through (Tidal marks) | atlas (`tidal.resolved.json`) |
| `tidal_split_authority.json` | classified (Claude, cited; owner may correct) | hand, T678 2026-11-11 | the atlas tidal cut (T678 step 10, not wired yet) |
| `shore_held.json` | the owner's rulings on single lines for the coast cut (every row `decided`) | by hand, from a recorded owner answer | atlas (`build.cut_shores`, `coast_owner.json`) |
| `slough_calls.json` | applied | `pipeline.tools.slough_calls` / write-through (slough marks) | atlas registry |
| `slough_page_calls.json` | frozen | the retired slough page (2026-10-28) | review app (old notes) |
| `steelhead_waters.json` | accepted | `python -m pipeline.regs.steelhead.accept --write` | reach run |
| `unmapped_waters.json` | authored | hand (worklist for added lakes) | `check_curated --waters` only |
| `unplaced_piece_calls.json` | authored | the owner (`accepted`); Claude adds `proposed` rows, read by nothing | reach run (`build.unplaced_gate`, T632: any other unplaced piece stops it) |

Hand-edited files are checked by `python -m pipeline.tools.audit.check_curated --waters` (the build's first step).
Config keys: `CURATED.waters.<file stem>`, except `tidal` (tidal_limits.json) and `areas` (area_closures.json), which
keep their old attribute names until the tile code next changes (renaming them would rebuild and re-ship the tiles).
