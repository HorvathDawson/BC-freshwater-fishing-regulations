# T632 item 4: the six entries whose saved match and the matcher's pick disagree

Owner answer 2026-11-11: **the curator's saved match wins.** Nothing below has been changed. The build reads the saved
`matched` (column 2). `backfill_matched` no longer overwrites a saved match (T546, 1036ccd51). It lists these six
every time it runs. Measured 2026-11-10 with `backfill_matched --dry-run` against `data/generated/atlas/full`.

Tick each row: **keep saved** (the default; nothing to do) or **take the matcher's** (then run
`python -m pipeline.regs.parsing.backfill_matched --restamp <entry>`).

| # | entry | saved match (what the build uses) | matcher's pick | who added what |
|---|---|---|---|---|
| 1 | `r2:nelson_creek@2-8` NELSON CREEK | Nelson Creek `gnis:20408` (West Vancouver, wsc 900-088087) | the same **plus** Nelson Creek `gnis:37595` (a ditch near Maillardville, Coquitlam, wsc 100-019698-194371) | The saved match has stood since the first ingest (295c6fa75, 2026-09-10). The matcher adds the second creek because the curated override `NELSON CREEK` (book_name_links.json, 5c3319c01) links both, "linked to both for completeness", and calls West Vancouver "MOST LIKELY". |
| 2 | `r2:stave_river@2-8` STAVE RIVER | Stave River `gnis:14589` + Ruskin Spawning Channel `blk:355993381` + Northrop Spawning Channel `wsc:100-046202-028692-080600` | Stave River only | The owner approved both channels on 2026-10-28 (T222: e8ff6db17, 0f40f008b, c47572afc). No override names them, so the matcher drops them. |
| 3 | `r2:sumallo_river_includes_cedar_lake_at_sunshine_valley@2-2` SUMALLO RIVER (includes "Cedar" Lake) | Sumallo River `gnis:2481` + Cedar Lake `wbk:329522925` | Sumallo River only | Cedar Lake has been in the entry since 2026-09-10 (295c6fa75) and was kept in the 2026-09-25 "Sumallo/Cedar Lake" fix (b460859a0). The book row names the lake; the matcher does not. |
| 4 | `r3:seton_river_includes_bc_hydro_power_canal_upstream_of_the_da@3-16` SETON RIVER (includes BC Hydro Power Canal) | Seton River `gnis:6291` + Seton Canal `wbk:328988335` | Seton River only | The canal was added in the rules-round fix batch, 2026-10-07 ("Seton canal item", bd0de3925). The book row names the canal. |
| 5 | `r3:shuswap_lake_see_maps_on_page_28_includes_little_shuswap_lak@3-26` SHUSWAP LAKE (includes Little Shuswap Lake, that part of South Thompson River between …) | Shuswap, Little Shuswap and Mara lakes + Little River `gnis:33426` | the three lakes only | Little River (the stretch between Shuswap and Little Shuswap lakes) was added 2026-09-29 ("Little River in Shuswap", 8edb4ae89). |
| 6 | `r8:okanagan_river_oxbows@8-1` OKANAGAN RIVER OXBOWS | the 40 oxbow polygons (`wbk:…`) | the same **plus** Park Rill `gnis:22618` | Park Rill was in the entry until 2026-10-07 and was dropped in bd0de3925. The matcher adds it back because the override's watershed code `300-432687-516449` is Park Rill's own code. On 2026-11-03 the owner ruled that the oxbow touching Park Rill stays its own water (T322, efe94fb), which agrees with the saved match. The Oxbows fix proposal removes that code from the override (`T632` ticket, item 3). After that change the matcher agrees and this row disappears. |

The saved match wins in all six. Rows 2 to 5 are additions the book row names. Row 1 is a second candidate that the
override itself calls less likely. Row 6 is a code that names the wrong water.
