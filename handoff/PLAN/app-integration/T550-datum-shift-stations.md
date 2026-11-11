# T550: stations whose water-level record shifts datum

Built 2026-11-10 from HYDAT 20261019 with the current code (T550 item 2, commit 61a97ca16). The new clim.json went to
STAGING only (`feeds/live/clim.json` in bc-fishing-regulations-staging) and one staging gauge run read it straight away.
Production is unchanged.

## What this list is
A gauge's water level is measured against a fixed mark (its datum). When the mark is moved, the level jumps by a set
amount and the old years are no longer comparable with the new ones. The build now finds those jumps and ranks today's
level only against the years since the last one. Flow is never affected.

- 45 stations have at least one shift. 21 of them report live on staging today; the other 24 are old or closed gauges.
- 8 old gauges (Cariboo, Pavilion and Allison lakes, five on the Kootenay) have under 3 years left after their shift, so
  they no longer have a level record at all. None of them reports live.
- "Years of level kept" counts the years since the last shift; "Years dropped" counts the years before it.

## Read me first
- **Map shows** is the figure the map and the gauge card lead with. For 19 of the 21 live stations that is flow, so the
  level standing is the second line. Only Harrison River (08MG014) and Kalamalka Lake (08NM143) lead with level.
- **Before** is the staging feed at 03:35 UTC, **after** is the run at 03:39 UTC. Most stations had a reading up to an
  hour newer in the second run, so part of a small move can be the newer reading. A large swing comes from the cut: the
  level record changed for these 45 stations only, and for every other station it is byte for byte the same.
- **No level standing** means the live reading does not match the station's own record at the reading's flow, so it is
  probably on another datum and is not ranked (08HD006 reads 2.3 m where its record says about 7 m). Four stations without
  a shift lost their level standing for the same reason: 07FD001, 08EF001, 08KH019 and 08MH153.

## Live on staging (21)
| Station | Name | Water | Last shift (year the new datum starts) | Years of level kept | Years dropped | Live on staging | Level standing before → after | Map shows |
|---|---|---|---|---|---|---|---|---|
| 07FA006 | Halfway River Near Farrell Creek | Halfway River | 2022 | 4 | 14 | yes | lower than 9 days in 10 → higher than 8 days in 10 | flow |
| 08DB001 | Nass River Above Shumal Creek | Nass River | 2018 | 9 | 7 | yes | higher than 8 days in 10 → higher than 6 days in 10 | flow |
| 08FB004 | Salloomt River Near Hagensborg | Salloomt River | 2018 | 8 | 7 | yes | lower than 7 days in 10 → higher than 5 days in 10 | flow |
| 08FE003 | Kemano River Above Powerhouse Tailrace | Kemano River | 2018 | 9 | 7 | yes | higher than almost every day on record → higher than almost every day on record | flow |
| 08GA030 | Seymour River Near North Vancouver | Seymour River | 2015, 2018 | 9 | 7 | yes | higher than 8 days in 10 → higher than almost every day on record | flow |
| 08GC007 | Theodosia River Below Olsen Lake Diversion | Theodosia River | 2014 | 11 | 3 | yes | higher than 7 days in 10 → higher than 7 days in 10 | flow |
| 08HD006 | Salmon River Near Sayward | Salmon River | 2016 | 9 | 5 | yes | lower than almost every day on record → no level standing | flow |
| 08KG001 | West Road River Near Cinema | West Road (Blackwater) River | 2021 | 4 | 10 | yes | lower than 8 days in 10 → higher than 9 days in 10 | flow |
| 08LB020 | Barriere River At The Mouth | Barrière River | 2017 | 9 | 6 | yes | lower than 5 days in 10 → lower than almost every day on record | flow |
| 08LB024 | Fishtrap Creek Near Mclure | Fishtrap Creek | 2018 | 9 | 7 | yes | higher than 8 days in 10 → higher than 7 days in 10 | flow |
| 08LB078 | Lemieux Creek Near The Mouth | Lemieux Creek | 2014 | 12 | 3 | yes | lower than 6 days in 10 → lower than 8 days in 10 | flow |
| 08LF007 | Criss Creek Near Savona | Criss Creek | 2024 | 3 | 13 | yes | lower than almost every day on record → lower than almost every day on record | flow |
| 08LF099 | Arrowstone Creek Near The Mouth | Arrowstone Creek | 2017 | 8 | 6 | yes | higher than 6 days in 10 → lower than 8 days in 10 | flow |
| 08LG008 | Spius Creek Near Canford | Spius Creek | 2022 | 4 | 10 | yes | lower than almost every day on record → lower than 7 days in 10 | flow |
| 08LG048 | Coldwater River Near Brookmere | Coldwater River | 2022 | 3 | 11 | yes | higher than 9 days in 10 → lower than 6 days in 10 | flow |
| 08MG014 | Harrison River At Harrison Mills | Harrison River | 2013, 2022 | 5 | 56 | yes | higher than 8 days in 10 → lower than 8 days in 10 | level |
| 08MG027 | Lillooet River At Tenas Narrows | Lillooet River | 2017 | 9 | 5 | yes | higher than 6 days in 10 → higher than 6 days in 10 | flow |
| 08NG076 | Mather Creek Below Houle Creek | Mather Creek | 2015 | 12 | 4 | yes | lower than 7 days in 10 → lower than 9 days in 10 | flow |
| 08NJ061 | Redfish Creek Near Harrop | Redfish Creek | 2020 | 7 | 9 | yes | higher than 8 days in 10 → higher than 5 days in 10 | flow |
| 08NL050 | Hedley Creek Near The Mouth | Hedley Creek | 2018 | 9 | 7 | yes | lower than 5 days in 10 → higher than 7 days in 10 | flow |
| 08NM143 | Kalamalka Lake At Vernon Pumphouse | Kalamalka Lake | 1972 | 55 | 5 | yes | lower than almost every day on record → lower than almost every day on record | level |

## Not live (24)
| Station | Name | Water | Last shift (year the new datum starts) | Years of level kept | Years dropped | Live on staging | Level standing before → after | Map shows |
|---|---|---|---|---|---|---|---|---|
| 07FC005 | Charlie Lake Near Fort St. John | Charlie Lake | 1988 | 3 | 13 | no | — → — | — |
| 08EC007 | Fulton Lake Near Smithers | Fulton Lake | 1969 | 5 | 5 | no | — → — | — |
| 08EG018 | Clarence Creek Near Rosswood | Clarence Creek | 2015 | 10 | 4 | no | — → — | — |
| 08KH017 | Cariboo Lake Near Keithley Creek | Cariboo Lake | 1976 | too few to rank | 17 | no | — → — | — |
| 08LE070 | Shuswap Lake At Salmon Arm | Shuswap Lake | 1961 | 40 | 10 | no | — → — | — |
| 08MD014 | Pavilion Lake Near Pavilion | Pavilion Lake | 1942 | too few to rank | 11 | no | — → — | — |
| 08ME019 | Fraser River At Lillooet | Fraser River | 1965 | 5 | 22 | no | — → — | — |
| 08NE128 | Coursier Lake At The Dam | Coursier Lake | 1999 | 5 | 3 | no | — → — | — |
| 08NH018 | Kootenay Lake At Procter (Gauge No. 24) | Kootenay Lake | 1933 | 5 | 10 | no | — → — | — |
| 08NH060 | Kootenay River At Six Mile Lake | Kootenay River | 1946, 1949 | too few to rank | 7 | no | — → — | — |
| 08NH094 | Kootenay River Near Creston (Well No. 436) | Kootenay River | 1949 | too few to rank | 5 | no | — → — | — |
| 08NH096 | Kootenay River At Goat Slough (Outside Dyke) | Kootenay River | 1949 | too few to rank | 5 | no | — → — | — |
| 08NH098 | Kootenay River (East Branch) Near Sirdar | Kootenay River | 1954 | too few to rank | 8 | no | — → — | — |
| 08NJ074 | Kootenay River Near Nelson (Gauge No. 8) | — | 1932, 1945 | 4 | 5 | no | — → — | — |
| 08NJ075 | Kootenay River Near Taghum (Gauge No. 9) | — | 1932 | too few to rank | 5 | no | — → — | — |
| 08NJ076 | Kootenay Lake Near Nelson (Gauge No. 11) | Kootenay Lake | 1945 | 4 | 4 | no | — → — | — |
| 08NJ077 | Kootenay Lake Near Nelson (Gauge No. 12) | Kootenay Lake | 1945 | 4 | 3 | no | — → — | — |
| 08NJ087 | Kootenay Lake Near Harrop (Gauge No. 18) | Kootenay Lake | 1945 | 4 | 4 | no | — → — | — |
| 08NJ092 | Kootenay River Near Taghum (Gauge No. 4) | — | 1932, 1935 | 11 | 6 | no | — → — | — |
| 08NJ113 | Kootenay River Above Corra Linn | Kootenay River | 1937 | 67 | 5 | no | — → — | — |
| 08NL058 | Allison Lake Near Princeton | Allison Lake | 1980 | too few to rank | 6 | no | — → — | — |
| 08NM148 | Twin Lakes Near Olalla | Twin Lakes | 1970, 1972 | 6 | 4 | no | — → — | — |
| 08NM231 | Ideal Lake Near The Outlet | Ideal Lake | 1976 | 5 | 10 | no | — → — | — |
| 08PA004 | Skagit River At International Boundary | Skagit River | 1981 | 41 | 28 | no | — → — | — |

## Check for the owner
- Is any shift year wrong for a gauge you know? Big swings worth a look: 07FA006 Halfway (lower than 9 days in 10 → higher
  than 8), 08MG014 Harrison (higher than 8 → lower than 8, and the map shows its level), 08LG048 Coldwater (higher than 9 →
  lower than 6), 08KG001 West Road (lower than 8 → higher than 9).
- 08PA004 Skagit at the boundary keeps 41 years from 1981 and drops 28. 08NJ113 Kootenay above Corra Linn keeps 67.
