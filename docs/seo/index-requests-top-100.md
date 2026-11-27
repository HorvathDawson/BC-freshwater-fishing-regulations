# Request indexing: the top 100 pages to submit by hand

The order to paste URLs into Google Search Console's URL Inspection box and press **Request indexing** (still to be made:
OWNER.md lists it). Ranked by relative BC search interest from Google Trends (BC, past 12 months, `"<water>" fishing`, or
`<river> level` for gauge pages), scale Vedder River fishing = 100, plus how many of 18 "best fishing in BC" guide lists
name the water. Below a score of about 10 the ranks are roughly tied; a name several waters share (Trout Lake, Deer Lake,
Elk River) collects all their searches. Once Search Console has impressions, its Performance report is a better source and
the list should be redone from it (open ticket T1011 part 3).

**Use.** The quota is about 10 requests a day per property (Google publishes no number): work down the list over about 10
days. Bulk discovery is the sitemap (Search Console, Sitemaps) and, for Bing and Yandex, IndexNow. A request does not
guarantee indexing and repeating it does not speed it up. Ranks 1 to 3 are hubs: `/gauges` links all 452 gauge pages and each
region page links its A to Z lists (the home page `/` is the JavaScript map, with no plain links). Every URL is in the sitemap
and answered 200 with a full body when checked on 2026-11-22.

| # | URL | Name (as anglers say it) | Type | Demand signal |
|---|---|---|---|---|
| 1 | https://canifishthis.ca/gauges | River gauges index (all 452 gauge pages) | hub | hub: the only plain-link page to every gauge page |
| 2 | https://canifishthis.ca/region/2 | Region 2 Lower Mainland index | hub | hub: region with the top waters by DFO angler days (Vedder, Chilliwack, Fraser) and most of the top searches |
| 3 | https://canifishthis.ca/region/3 | Region 3 Thompson-Nicola index | hub | hub: region with the most freshwater fishing days in BC (DFO 2022: 20%) |
| 4 | https://canifishthis.ca/water/fraser-river | Fraser River | water | Trends 210; in 7 guide lists (Bead n Float salmon, Outdoor Canada fly, DFO angler survey top water, BC Outdoors top-10 steelhead, Backroad LM, BC Outdoors urban, Outdoors Intl); a Trends top query; includes lower-Fraser sturgeon/salmon searches |
| 5 | https://canifishthis.ca/water/vedder-river | Vedder River | water | Trends 100; in 7 guide lists (S&S Journal 20 steelhead, Bead n Float salmon, Outdoor Canada fly, DFO angler survey top water, BC Outdoors top-10 steelhead, Backroad LM, BC Outdoors urban); a Trends top query |
| 6 | https://canifishthis.ca/gauge/08MH001/chilliwack-river-at-vedder-crossing | Vedder River — water level | gauge | Trends 63 for "vedder river level(s)" |
| 7 | https://canifishthis.ca/gauge/08NM083/okanagan-lake-at-kelowna | Okanagan Lake at Kelowna — water level | gauge | Trends 63 for "okanagan lake level(s)"; lake level is a big local search (flooding), not only anglers |
| 8 | https://canifishthis.ca/gauge/08HD022/campbell-river-at-campbell-river-lodge | Campbell River at Campbell River Lodge — water level | gauge | Trends 44 for "campbell river level(s)"; lower river; some "Campbell River level" searches may be about the town |
| 9 | https://canifishthis.ca/water/okanagan-lake | Okanagan Lake | water | Trends 40; in 5 guide lists (Destination BC, Backroad Thompson-Okanagan, BC Parks, DFO angler survey top water, BC Outdoors urban); a Trends top query |
| 10 | https://canifishthis.ca/water/capilano-river | Capilano River | water | Trends 40; in 2 guide lists (Bead n Float salmon, Backroad LM); a Trends top query |
| 11 | https://canifishthis.ca/water/rice-lake-r2 | Rice Lake | water | Trends 38; in 1 guide list (Backroad LM); a Trends top query; North Vancouver (Seymour reserve) |
| 12 | https://canifishthis.ca/gauge/08GA010/capilano-river-above-intake | Capilano River above Intake — water level | gauge | Trends 38 for "capilano river level(s)"; the fished lower river has no WSC gauge; this is the closest station page |
| 13 | https://canifishthis.ca/water/skeena-river | Skeena River | water | Trends 37; in 6 guide lists (S&S Journal 20 steelhead, Bead n Float salmon, DFO angler survey top water, BC Outdoors top-10 steelhead, Backroad North, Outdoors Intl); a Trends top query |
| 14 | https://canifishthis.ca/water/cultus-lake-r2 | Cultus Lake | water | Trends 35; in 1 guide list (BC Outdoors urban); a Trends top query; near Chilliwack |
| 15 | https://canifishthis.ca/water/deer-lake-r2-2-8 | Deer Lake | water | Trends 35; a Trends top query; Burnaby; name shared with other Deer Lakes |
| 16 | https://canifishthis.ca/gauge/08HA011/cowichan-river-near-duncan | Cowichan River near Duncan — water level | gauge | Trends 31 for "cowichan river level(s)" |
| 17 | https://canifishthis.ca/gauge/08LE111/shuswap-lake-at-blind-bay | Shuswap Lake at Blind Bay — water level | gauge | Trends 31 for "shuswap lake level(s)" |
| 18 | https://canifishthis.ca/water/kootenay-lake-main-body | Kootenay Lake | water | Trends 31; in 5 guide lists (Outdoor Canada fly, Backroad Kootenay, BC Parks, DFO angler survey top water, GoFishBC May long); a Trends top query; main body page; the West Arm (Nelson) has its own pages |
| 19 | https://canifishthis.ca/water/cowichan-river | Cowichan River | water | Trends 30; in 5 guide lists (S&S Journal 20 steelhead, Bead n Float salmon, DFO angler survey top water, BC Outdoors top-10 steelhead, Backroad Van Isle); a Trends top query |
| 20 | https://canifishthis.ca/water/tunkwa-lake | Tunkwa Lake | water | Trends 29; in 1 guide list (Backroad Kamloops); a Trends top query |
| 21 | https://canifishthis.ca/water/squamish-river | Squamish River | water | Trends 28; in 3 guide lists (S&S Journal 20 steelhead, Bead n Float salmon, BC Outdoors top-10 steelhead); a Trends top query |
| 22 | https://canifishthis.ca/water/buntzen-lake | Buntzen Lake | water | Trends 28; in 1 guide list (GoFishBC May long); a Trends top query |
| 23 | https://canifishthis.ca/gauge/08GA022/squamish-river-near-brackendale | Squamish River near Brackendale — water level | gauge | Trends 28 for "squamish river level(s)" |
| 24 | https://canifishthis.ca/water/thompson-river | Thompson River | water | Trends 27; in 2 guide lists (BC Outdoors top-10 steelhead, BC Outdoors urban) |
| 25 | https://canifishthis.ca/water/chilliwack-river | Chilliwack River | water | Trends 27; in 4 guide lists (Bead n Float salmon, DFO angler survey top water, BC Outdoors top-10 steelhead, Backroad LM); a Trends top query |
| 26 | https://canifishthis.ca/water/elk-river-r4 | Elk River | water | Trends 25; in 3 guide lists (Backroad Kootenay, Backroad Van Isle, Outdoors Intl); near Fernie; name shared with the Elk River on Vancouver Island |
| 27 | https://canifishthis.ca/water/campbell-river-r1 | Campbell River | water | Trends 25; in 4 guide lists (Bead n Float salmon, DFO angler survey top water, Backroad Van Isle, Outdoors Intl); a Trends top query; raw Trends 233, but most "Campbell River fishing" searches are ocean fishing off the town; scored 25 from DFO angler days (Campbell River 21k vs Vedder 85k, 2024) |
| 28 | https://canifishthis.ca/water/sheridan-lake | Sheridan Lake | water | Trends 25; in 3 guide lists (Backroad top-10 lakes, BC Outdoors Cariboo, DFO angler survey top water) |
| 29 | https://canifishthis.ca/gauge/08MF005/fraser-river-at-hope | Fraser River at Hope — water level | gauge | Trends 25 for "fraser river level(s)" |
| 30 | https://canifishthis.ca/water/shuswap-lake | Shuswap Lake | water | Trends 24; in 2 guide lists (Destination BC, Backroad top-10 lakes) |
| 31 | https://canifishthis.ca/water/loon-lake-r3 | Loon Lake | water | Trends 23; in 1 guide list (BC Outdoors Cariboo); a Trends top query; near Cache Creek |
| 32 | https://canifishthis.ca/water/como-lake-r2 | Como Lake | water | Trends 23; in 1 guide list (Backroad LM); a Trends top query; Coquitlam |
| 33 | https://canifishthis.ca/gauge/08NJ158/kootenay-lake-outflow-near-corra-linn | Kootenay Lake — water level | gauge | Trends 23 for "kootenay lake level(s)" |
| 34 | https://canifishthis.ca/water/coquitlam-river | Coquitlam River | water | Trends 21; in no guide list (added as a well-known water) |
| 35 | https://canifishthis.ca/water/trout-lake-r2 | Trout Lake | water | Trends 21; in 2 guide lists (Backroad Kootenay, BC Outdoors urban); a Trends top query; Vancouver (John Hendry Park); name shared with Trout Lake in the Kootenays |
| 36 | https://canifishthis.ca/water/elk-lake-r1 | Elk Lake | water | Trends 20; in 2 guide lists (Backroad top-10 lakes, BC Outdoors urban); a Trends top query; Saanich |
| 37 | https://canifishthis.ca/water/green-lake-r2 | Green Lake | water | Trends 19; a Trends top query; Whistler; name shared (no page found for Green Lake at 70 Mile House) |
| 38 | https://canifishthis.ca/water/rolley-lake | Rolley Lake | water | Trends 19; in no guide list (added as a well-known water) |
| 39 | https://canifishthis.ca/water/alouette-river | Alouette River | water | Trends 18; in no guide list (added as a well-known water) |
| 40 | https://canifishthis.ca/water/gold-river-r1 | Gold River | water | Trends 18; in 2 guide lists (S&S Journal 20 steelhead, BC Outdoors top-10 steelhead); also a town |
| 41 | https://canifishthis.ca/water/stamp-river | Stamp River | water | Trends 17; in 3 guide lists (S&S Journal 20 steelhead, Bead n Float salmon, Backroad Van Isle) |
| 42 | https://canifishthis.ca/water/stave-river | Stave River | water | Trends 17; in 1 guide list (S&S Journal 20 steelhead); a Trends top query |
| 43 | https://canifishthis.ca/water/babine-lake-main-body | Babine Lake | water | Trends 17; in 3 guide lists (Backroad top-10 lakes, BC Parks, Backroad North); main body page |
| 44 | https://canifishthis.ca/water/roche-lake | Roche Lake | water | Trends 17; in 1 guide list (Backroad Kamloops) |
| 45 | https://canifishthis.ca/water/kitimat-river | Kitimat River | water | Trends 17; in 3 guide lists (S&S Journal 20 steelhead, Bead n Float salmon, DFO angler survey top water) |
| 46 | https://canifishthis.ca/water/beaver-lake-r1-1-1 | Beaver Lake | water | Trends 17; in 3 guide lists (Backroad top-10 lakes, Backroad Thompson-Okanagan, BC Outdoors urban); Saanich (Elk/Beaver Lake) |
| 47 | https://canifishthis.ca/water/bridge-lake | Bridge Lake | water | Trends 17; in no guide list (added as a well-known water); also a community |
| 48 | https://canifishthis.ca/water/harrison-river | Harrison River | water | Trends 16; in 2 guide lists (Bead n Float salmon, Outdoor Canada fly) |
| 49 | https://canifishthis.ca/water/harrison-lake-r2 | Harrison Lake | water | Trends 15; in 1 guide list (Backroad LM); Harrison Hot Springs |
| 50 | https://canifishthis.ca/water/sasamat-lake | Sasamat Lake | water | Trends 15; in 1 guide list (Backroad LM); a Trends top query |
| 51 | https://canifishthis.ca/water/nanaimo-river | Nanaimo River | water | Trends 15; in 1 guide list (BC Outdoors urban) |
| 52 | https://canifishthis.ca/water/oyster-river | Oyster River | water | Trends 14; in no guide list (added as a well-known water) |
| 53 | https://canifishthis.ca/gauge/08HA009/cowichan-lake-near-lake-cowichan | Cowichan Lake — water level | gauge | Trends 14 for "cowichan lake level(s)" |
| 54 | https://canifishthis.ca/water/skaha-lake | Skaha Lake | water | Trends 14; in no guide list (added as a well-known water) |
| 55 | https://canifishthis.ca/water/pitt-river | Pitt River | water | Trends 14; in no guide list (added as a well-known water) |
| 56 | https://canifishthis.ca/water/christina-lake | Christina Lake | water | Trends 14; in 2 guide lists (Destination BC, Backroad Thompson-Okanagan) |
| 57 | https://canifishthis.ca/water/stump-lake-r3 | Stump Lake | water | Trends 14; in 1 guide list (Outdoor Canada fly); a Trends top query; near Merritt |
| 58 | https://canifishthis.ca/gauge/08HC001/gold-river-below-ucona-river | Gold River — water level | gauge | Trends 14 for "gold river level(s)"; also a town |
| 59 | https://canifishthis.ca/gauge/08HB034/nanaimo-river-near-cassidy | Nanaimo River near Cassidy — water level | gauge | Trends 14 for "nanaimo river level(s)" |
| 60 | https://canifishthis.ca/water/alouette-lake | Alouette Lake | water | Trends 13; in 1 guide list (BC Parks); a Trends top query |
| 61 | https://canifishthis.ca/water/lafarge-lake | Lafarge Lake | water | Trends 13; in 1 guide list (Backroad LM) |
| 62 | https://canifishthis.ca/water/wood-lake-r8 | Wood Lake | water | Trends 13; in no guide list (added as a well-known water); Lake Country |
| 63 | https://canifishthis.ca/water/chilliwack-lake | Chilliwack Lake | water | Trends 13; a Trends top query |
| 64 | https://canifishthis.ca/water/englishman-river | Englishman River | water | Trends 13; in 1 guide list (Backroad Van Isle) |
| 65 | https://canifishthis.ca/water/hicks-lake | Hicks Lake | water | Trends 13; in 1 guide list (BC Parks) |
| 66 | https://canifishthis.ca/water/kalamalka-lake | Kalamalka Lake | water | Trends 13; in 1 guide list (BC Outdoors urban) |
| 67 | https://canifishthis.ca/water/shannon-lake-proper | Shannon Lake | water | Trends 12; in 2 guide lists (BC Outdoors urban, GoFishBC May long); West Kelowna |
| 68 | https://canifishthis.ca/water/echo-lake-r8-near-cherryville | Echo Lake | water | Trends 12; in 2 guide lists (Backroad Thompson-Okanagan, GoFishBC May long); near Lumby/Cherryville |
| 69 | https://canifishthis.ca/water/sproat-lake | Sproat Lake | water | Trends 12; in no guide list (added as a well-known water) |
| 70 | https://canifishthis.ca/water/cowichan-lake | Cowichan Lake | water | Trends 11; in 2 guide lists (BC Outdoors top-10 steelhead, Backroad Van Isle) |
| 71 | https://canifishthis.ca/water/lac-la-hache | Lac La Hache | water | Trends 11; in no guide list (added as a well-known water) |
| 72 | https://canifishthis.ca/water/bulkley-river | Bulkley River | water | Trends 11; in 5 guide lists (S&S Journal 20 steelhead, Bead n Float salmon, DFO angler survey top water, BC Outdoors top-10 steelhead, Outdoors Intl) |
| 73 | https://canifishthis.ca/water/monte-lake | Monte Lake | water | Trends 11; in no guide list (added as a well-known water) |
| 74 | https://canifishthis.ca/water/sooke-river | Sooke River | water | Trends 11; in 1 guide list (Backroad Van Isle) |
| 75 | https://canifishthis.ca/water/quinsam-river | Quinsam River | water | Trends 11; in 1 guide list (Bead n Float salmon) |
| 76 | https://canifishthis.ca/gauge/08MH002/coquitlam-river-at-port-coquitlam | Coquitlam River at Port Coquitlam — water level | gauge | Trends 11 for "coquitlam river level(s)" |
| 77 | https://canifishthis.ca/water/lac-le-jeune | Lac Le Jeune | water | Trends 11; in 3 guide lists (BC Parks, Backroad Kamloops, GoFishBC May long) |
| 78 | https://canifishthis.ca/water/qualicum-river | Qualicum River | water | Trends 11; in 1 guide list (Backroad Van Isle) |
| 79 | https://canifishthis.ca/water/mill-lake-r2 | Mill Lake | water | Trends 10; in 1 guide list (BC Outdoors urban); Abbotsford |
| 80 | https://canifishthis.ca/water/mabel-lake | Mabel Lake | water | Trends 10; in 1 guide list (Backroad Thompson-Okanagan) |
| 81 | https://canifishthis.ca/water/stave-lake | Stave Lake | water | Trends 10; in no guide list (added as a well-known water) |
| 82 | https://canifishthis.ca/water/edith-lake-r3 | Edith Lake | water | Trends 10; in 1 guide list (Backroad Kamloops); near Kamloops |
| 83 | https://canifishthis.ca/water/puntledge-river | Puntledge River | water | Trends 10; in no guide list (added as a well-known water) |
| 84 | https://canifishthis.ca/water/chehalis-river | Chehalis River | water | Trends 10; a Trends top query |
| 85 | https://canifishthis.ca/water/shuswap-river | Shuswap River | water | Trends 9; in 1 guide list (BC Outdoors urban) |
| 86 | https://canifishthis.ca/water/swan-lake-r8 | Swan Lake | water | Trends 9; in 1 guide list (BC Outdoors urban); near Vernon |
| 87 | https://canifishthis.ca/water/pitt-lake | Pitt Lake | water | Trends 9; in no guide list (added as a well-known water) |
| 88 | https://canifishthis.ca/gauge/08FF001/kitimat-river-below-hirsch-creek | Kitimat River — water level | gauge | Trends 9 for "kitimat river level(s)" |
| 89 | https://canifishthis.ca/water/nicomen-slough | Nicomen Slough | water | Trends 9; in 1 guide list (Bead n Float salmon) |
| 90 | https://canifishthis.ca/water/horse-lake-r5-near-100-mile-house | Horse Lake | water | Trends 9; in no guide list (added as a well-known water); near 100 Mile House |
| 91 | https://canifishthis.ca/water/alice-lake-r2 | Alice Lake | water | Trends 9; in 3 guide lists (BC Parks, Backroad LM, Backroad Van Isle); near Squamish |
| 92 | https://canifishthis.ca/water/fraser-lake-r7 | Fraser Lake | water | Trends 9; in 1 guide list (Backroad North); also a town |
| 93 | https://canifishthis.ca/water/hayward-lake-r2 | Hayward Lake | water | Trends 9; in no guide list (added as a well-known water); Mission |
| 94 | https://canifishthis.ca/water/shawnigan-lake | Shawnigan Lake | water | Trends 9; in 1 guide list (Backroad Van Isle) |
| 95 | https://canifishthis.ca/water/kamloops-lake | Kamloops Lake | water | Trends 9; in 1 guide list (Destination BC) |
| 96 | https://canifishthis.ca/water/eagle-river-r3 | Eagle River | water | Trends 9; in no guide list (added as a well-known water); near Sicamous; name shared with US Eagle Rivers |
| 97 | https://canifishthis.ca/water/cheakamus-river | Cheakamus River | water | Trends 9; in no guide list (added as a well-known water) |
| 98 | https://canifishthis.ca/water/pass-lake-r3 | Pass Lake | water | Trends 9; in no guide list (added as a well-known water); near Kamloops |
| 99 | https://canifishthis.ca/water/vaseux-lake | Vaseux Lake | water | Trends 9; in no guide list (added as a well-known water) |
| 100 | https://canifishthis.ca/water/weaver-lake | Weaver Lake | water | Trends 9; in no guide list (added as a well-known water) |

## Left out

- No water page on the site: Columbia River (Trends 33, mostly the US river; gauge `08NE049`), Kettle (16), Kootenay (8) and
  Similkameen (8) rivers (gauges 08NN026, 08NG065, 08NL038), Peace, Blackwater, Wigwam and Sumas rivers, Chilko, Alces,
  Puntzi and Quartz lakes. Green Lake at 70 Mile House: none of the seven `green-lake-*` pages is near it; check it is not
  missing.
- Excluded as ambiguous: Fish Lake, Salmon River, Williams Lake, Columbia Lake. Not tidal: Campbell River's raw score (233)
  is mostly ocean fishing off the town.
- Famous but little BC search volume (0 to 6): Babine, Kispiox, Dean, Copper (Zymoetz), Slocan and Whiteswan lakes.
