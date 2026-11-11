# T635 findings: where are the fishways and government fish facilities?

Research only, 2026-11-10. Nothing was added to the repo except this page. Every number below was **fetched** unless it is
marked **(unverified)**.

The rules (book p.9, shipped as `zp:no_fishing_buffers` r1/r2):
- "Within 23 m downstream of the lower entrance to any fishway, canal, obstacle or leap."
- "Within a 100 m radius of any government facility operated for counting, passing or rearing fish (e.g., fishway, fish hatchery)."

## Short answer

Yes, most of it can be found, but no single source covers it, and none of them marks a "lower entrance".

- **Hatcheries: there is a good list.** DFO publishes every licensed enhancement and stocking facility in B.C. with
  coordinates and a licence type. That covers all 18 DFO-run major facilities, all FFSBC hatcheries, and 100 community
  sites. It is under the Open Government Licence – Canada. But the points sit 0.03–2.2 km from the actual site, so each
  needs hand-placing before a 100 m circle means anything.
- **Fishways: one usable list, with a licence problem.** The Canadian Aquatic Barriers Database (CABD) has 74 B.C. fishways
  with their type, and sometimes the operator. Its licence is **CC BY-NC-SA 4.0** (non-commercial, share-alike).
- **Spawning channels and counting fences: no clean dataset.** OpenStreetMap names most spawning channels. Counting
  fences exist only as DFO web pages and reports.
- **"Obstacle or leap": already in the app.** The T173 obstacle layer shows FISS falls, cascades, canyons and velocity
  barriers, plus BC Dams. The 23 m rule applies at every one of them, but no data says where the "lower entrance" is.

## Sources

### 1. DFO: Current valid British Columbia aquaculture licence holders, Enhancement list (the best source)
- URL: https://open.canada.ca/data/en/dataset/522d1b67-30d8-4a34-9b62-5da99b1035e6. The CSV is
  `licence-permis-enhancement-renforcement-rpt-pac-dfo-mpo-aquaculture-eng.csv` (35 KB, fetched).
- Licence: "Open Government Licence - Canada" (https://open.canada.ca/en/open-government-licence-canada). Apps may
  redistribute it with attribution.
- Currency: record modified 2026-10-29. Licences expire 2027-06-18 to 2027-06-30. Land-based licences run nine years
  (from the terminology file).
- Count: **128 points**, all with lat/long.
  - 18 `SEP Major Facility`, which the terminology file says are "operated and managed by DFO staff".
  - 100 `SEP Community Involvement`, "operated under direction and support of a DFO Community Advisor".
  - 10 `Recreational Stocking`: 8 FFSBC sites (5 trout hatcheries, the Nechako sturgeon centre, Skaha ponds, the old Vancouver Island Trout Hatchery
    site) and 2 Okanagan Nation Aquatic Enterprises.
- Fields: Facility Reference Number, Licence Number, Expiry Date, Licence Type, Licence Holder, Operating Group, Site Common
  Name, Latitude, Longitude, Licensed Species, Site Specific Conditions.
  - `Licence Type` plus `Operating Group` tells DFO-run from community-run. Privately owned freshwater farms are on separate lists and are not in this one.
- Position quality is the catch. I measured DFO points against OSM/TRIM outlines of the same sites:
  - Under 130 m: Kootenay TH 27 m, Snootli 78 m, Quinsam 122 m.
  - 300–600 m: Capilano 370 m (462 m from the CABD fishway), FVTH 320 m, Tenderfoot 338 m, Inch 327 m, Robertson 424 m,
    Nitinat 439 m, Kitimat 447 m, Puntledge 590 m.
  - Worse: Chilliwack 743 m, Chehalis 976 m, Big Qualicum about 2.2 km.
  - "Weaver Creek" carries Inch Creek's coordinates. Rosewall shares Fanny Bay's.
  - 7 of 128 have only 2 decimals (about 1 km). 21 look snapped to a grid (…983 / …982).
- Not in it: spawning channels other than the mislocated Weaver row, and counting fences.

### 2. CABD Fishways (Canadian Wildlife Federation)
- API: `https://cabd-web.azurewebsites.net/cabd-api/features/fishways?filter=province_territory_code:eq:bc` (fetched,
  143 KB). Docs: https://cabd-docs.netlify.app/docs_tech/docs_tech_arch_api
- Licence, from the API response metadata: `"data_licence": "https://creativecommons.org/licenses/by-nc-sa/4.0/"`.
  - **NonCommercial**: a paid app, or any app that earns money, cannot use it without CWF's permission.
  - **ShareAlike**: a derived layer must carry the same licence.
  - I found no separate licence page on the docs site, so ask CWF before any use.
- Count: **74 B.C. fishway points**. Data version 1.1, fetched 2026-11-11 UTC.
  - Types: Pool and weir 21, Vertical slot 17, Pool and weir with hole 14, Nature-like 8, Denil 8, Unknown 6.
- Fields include:
  - `structure_name_en`, `river_name_en`, `fishpass_type`, `year_constructed` (47 of 74), `dam_id`.
  - `entrance_location` / `entrance_position`: mid-stream vs bank and position in the water column. These are not coordinates.
  - `operated_by`, which is **empty on 58 of 74**. The filled ones say BC Hydro 5, City of Langford 4, DFO 5 (three spellings), Ducks Unlimited 1, Bowen Island club 1.
- Errors seen:
  - "Kokish River Dam" is on "Kiskatinaw River" and "Toosey Dam" is on "Kokish River"; the river names look swapped.
  - One point has no name at all.
  - The point named "Cache Creek Falls" (1988, vertical slot) is probably the Bonaparte River fishway. That is my reading; I did not check it.
- What it misses: no Big Qualicum fishway, and no Moricetown/Witset (Bulkley) fishway in the B.C. list.

### 3. OpenStreetMap (Overpass, fetched; base 2026-11-11T04:39Z)
- Licence: ODbL 1.0. Apps may redistribute it with attribution "© OpenStreetMap contributors". A derived database that is
  made public must stay ODbL. The app already uses OSM layers (waterfalls, access points).
- Counts in B.C.:
  - `waterway=fish_pass`: 16 (1 node, 15 ways).
  - `man_made=fish_ladder`: 1.
  - `amenity=fish_hatchery`: 9.
  - Name matches for hatchery / fishway / fish ladder / counting fence / spawning channel: 209, mostly roads, trails and creeks named "Hatchery".
  - `landuse=aquaculture`: 122, mostly marine salmon farms.
- Useful for spawning channels and some site outlines:
  - Weaver Creek Spawning Channel and Pinkut Creek spawning channel are tagged with a DFO operator.
  - Also named: Nadina, Fulton River, Gates (N'Quatqua Fisheries), Seton River Upper, Upper Kawkawa.
  - Site outlines for Chehalis, Inch Creek, Capilano, Robertson Creek, Tenderfoot, Kitimat, FFSBC Abbotsford.
  - Also named: Hell's Gate Fishways, Stamp Falls fish ladder, Meziadin Fishway, Kanaka Creek fish fence.
- Not found by name: Chilliwack River hatchery, Big Qualicum, Babine fence. Tagging is inconsistent, and the data is not usable alone.

### 4. TRIM (provincial 1:20,000 base map)
- `WHSE_BASEMAPPING.TRIM_CULTURAL_LINES` FCODE `AF11150000` ("fish hatchery"): 261 lines.
  `TRIM_EBM_DESIGNATEDAREAS` `fishHatchery`: 31 polygons.
  - Together they form about **51 sites**, and TRIM's "fish hatchery" includes spawning channels: Nadina, Pinkut,
    Fulton, Weaver, Gates, Seton, and a group near Lakelse.
  - 11 of the 51 lie within 1.5 km of a DFO list point.
- Licence: catalogue licence "Access Only". The Province's wording: "'access only' and reproduction is not permitted
  without written permission." **It cannot be shipped.** It is fine to look at while hand-placing points.
- Currency: legacy data, "not actively maintained since the last update in 2008".
- I found no TRIM code for fishways or fish ladders. The code dictionary was not reachable, so this is (unverified).

### 5. Provincial Obstacles to Fish Passage (FISS), already in the repo
- `data/source/fish_obstacles/fish_obstacles.gpkg` table `fiss`: 24,832 rows. Licence OGL-BC (catalogue). Catalogue
  record modified 2026-11-11.
- Covers falls 6,165, cascades about 4,560 across three spellings, canyon 556, velocity barrier about 249, dams of various kinds, and
  "Weir" 22.
  - **No fishway type exists.** No obstacle name contains fishway, ladder, fence or hatchery.
- This is the "obstacle or leap" half, and it already ships as the T173 obstacle layer, with deduping and pruning.
  It is surveyed 1:50k data, often from the 1990s.

### 6. BC Dams (WRIS), already in the repo
- `fish_obstacles.gpkg` table `dams`: 2,490 crests. OGL-BC. Has `DAM_OWNER_NAME`.
- 37 are owned by DFO, for example Nicola Lake, Sakinaw, Fulton Lake, Nadina Lake, Horne Lake, Cameron Lake, Glendale
  Lake weir and Tahltan Lake Creek. About 18 more belong to Fish & Wildlife Branch (Kamloops Fisheries Branch).
- The register has no fishway field. It says which dams a fisheries agency owns, not where fish pass.
  Several of those dams (Nicola, Sakinaw, Bonaparte Lake) do appear as CABD fishways.

### 7. PSCIS, already in the repo
- Road-crossing culverts only. Not relevant to fishways or facilities.

### 8. NuSEDS (DFO escapement database)
- https://open.canada.ca/data/en/dataset/c48669a3-045b-400d-b730-48aafe8c5ee6. OGL-Canada. Modified 2026-09-14.
- Metadata: "Each population is referenced to the location of the stream mouth."
  - So it can tell which streams have fence counts (estimate method, **unverified**: I did not download the 2026-06 file). It does not say where any fence stands.

### 9. Pacific Salmon Explorer (Pacific Salmon Foundation)
- salmonexplorer.ca map layers include "Spawning Channels", "Hatchery Facilities" and "Hatchery Release Sites". That comes from search
  snippets only and is **(unverified)**.
- I could not reach a download or a licence page. The site's data portal returned an empty page.

### 10. Others checked
- **DFO SEP pages** (pac.dfo-mpo.gc.ca hatchery list and hatchery map): **unreachable from this sandbox** (the fetch failed, and a run outside the sandbox was not allowed).
  - Search snippets give 23 facilities (16 hatcheries and 7 spawning channels) **(unverified)**.
  - An HTML map exists, dated June 2021 **(unverified)**.
  - Counting-fence pages exist for Babine, Kitwanga and Meziadin (search snippets, **unverified**).
- **SEP Production Plan / Post-Season archives** (open.canada.ca): production tables by facility, with no coordinates.
- **"Public Fish Hatcheries"** (NRCan): a 1980 atlas JPG/PDF. Useless.
- **Salmon CWT Location Data** (LC042, OGL-Canada): release and recovery sites. The file showed a content-length of 0 on HEAD, so I did not open it **(unverified)**.
- **BC Data Catalogue**: searches for "fishway", "fish fence" and "counting fence" return 0 datasets. "Hatchery" returns only shellfish and marine (CRIMS) layers.
- **bcfishpass** (github smnorris/bcfishpass): models barriers for access. I did not check whether it holds fishway records **(unverified)**.

## Check against facilities named independently

| Facility | DFO licence list | CABD fishways | OSM | TRIM |
|---|---|---|---|---|
| Hells Gate fishways | n/a | yes (east + west bank) | yes (name) | no code |
| Bonaparte fishway | n/a | probably ("Cache Creek Falls", 1988) and "Bonaparte Dam" at the lake | no | n/a |
| Capilano hatchery | yes (370 m off) | yes (hatchery fishway) | yes | yes |
| Chilliwack River hatchery | yes (743 m off) | no | **no** | yes |
| Babine counting fence | no | no | no | no: **found nowhere as data** |
| Stamp Falls fishway | n/a | yes | yes | n/a |
| Seton Dam fishway | n/a | yes (BC Hydro) | spawning channel only | channel yes |
| Big Qualicum hatchery / fishway | hatchery yes (2.2 km off) | **no** | no | yes |
| Puntledge hatchery / fence | yes (590 m off) | yes (several, incl. "lower hatchery fence") | no | yes |
| Fraser Valley Trout Hatchery | yes | n/a | yes | no |
| Meziadin fishway | n/a | yes ("Victoria Falls") | yes | n/a |
| Weaver / Pinkut / Fulton / Nadina channels | Weaver only, wrong position | Weaver fishway | yes | yes |
| Moricetown (Witset) fishway | n/a | **no** | no | n/a |

## What can be drawn

- **100 m circles around about 26 government-run hatcheries**: 18 DFO major facilities and 8 FFSBC sites, plus about 7
  DFO spawning channels.
  - Each point placed by hand on imagery. The OSM outline helps; TRIM may be used to look, not to ship.
  - **About 33 sites. These could be close to complete for DFO and FFSBC.** Counts from the DFO page are unverified, so cross-check them once.
- **Fishways**: about 74 CABD points with a 23 m downstream wedge and a 100 m circle where a government body runs it. But:
  - Only if the licence allows it (needs CWF's yes, or the owner confirming the app is non-commercial and accepting share-alike).
  - Or hand-placed again from imagery, with CABD used only as a checklist. A checklist is not a copy; that is my reading, and a lawyer should confirm it.
- **Counting fences**: a short hand-made list from DFO pages and reports (Babine, Kitwanga, Puntledge, Big Qualicum and
  others). Positions come from imagery and fences are seasonal, so mark them "seasonal, when installed".
- **Obstacles and leaps**: already marked. Add the 23 m rule to their sheet as a sentence, not a drawn wedge, because the
  data gives a point, not the downstream foot of the falls.

## What data cannot tell

- **Where the "lower entrance" is.** CABD gives a single point per fishway, often the dam. The 23 m is measured from the
  downstream entrance, which can be tens of metres away. Drawing it means placing each entrance by hand.
- **Which facilities count as "government".**
  - DFO major facilities clearly do.
  - FFSBC is a non-profit society operating provincial hatcheries.
  - The 100 community sites are run by societies and First Nations under DFO direction.
  - BC Hydro fishways are a Crown corporation's.
  - The owner (or the Ministry) has to rule on these. The data cannot.
- **When a fence is in the river.** Fences go in and come out each season. No dataset has the dates.
- **Facility extent.** "A 100 m radius of any facility": from a building, the intake, the fence, the whole fenced lot?
  A point plus 100 m will be wrong at large sites like Chehalis, Big Qualicum and Capilano.
- **Private and unlisted structures.** Municipal and volunteer fishways (Langford's Millstream fishways, Bowen Island),
  small weirs, and new works are missed or out of date.

## Risk

The rule applies at every fishway, obstacle and government facility, whether the app marks it or not. An incomplete
layer has two failure modes:

1. **Silence read as permission.** If an angler sees no ring, they assume they can fish. This is the serious one. The
   map must never imply "nothing drawn here, so it's clear".
2. **A wrong ring.** A circle 400 m off the real hatchery (the DFO points as published) tells someone they are clear
   when they are 50 m from the intake. This is why the DFO coordinates must not be drawn as published.

## Recommendation

1. **Hand-place a small, high-confidence "fish facilities" layer** of about 35–45 points:
   - The 18 DFO major hatcheries and the DFO spawning channels.
   - The FFSBC hatcheries and ponds.
   - The fishways run by DFO, BC Hydro and the Province.
   - Each from imagery, with the DFO licence row as its id and source (OGL-Canada, attributed), and OSM as a cross-check (ODbL attribution if any OSM geometry is used).
   - Draw a **marker** and a 100 m circle. Add a 23 m downstream mark only where the entrance was placed by hand.
   - Community sites: ask the owner first. Default to a marker without a circle and the sentence below.
2. **Ask CWF about CABD** before using it beyond a checklist.
3. **Put the rule in words everywhere it applies**: the obstacle sheet, the facility sheet, and the water's page. It is
   already shipped as a `where` rule.
4. **Wording**:
   - Facility sheet: "Fish facility. No fishing within 100 m of a government facility that counts, passes or rears fish (p.9). Shown where we know of one. Many are not on this map."
   - Fishway / falls sheet: "No fishing within 23 m downstream of the lower entrance to a fishway, canal, obstacle or falls (p.9). Where that entrance is depends on the site."
   - Layers sheet key, always visible: "Not every fishway, falls or fish facility is shown. The 23 m and 100 m rules apply at all of them."
   - Never: "No restrictions here", or a green/clear state near water without a mark.
5. **Effort, rough**:
   - About 1 day to hand-place and verify 35–45 points from imagery.
   - Half a day for the pipeline: a curated file, then tiles, the same way as obstacles.
   - Half a day for the app (marker, circle, sheet, key) plus tests.
   - Adding the 100 community sites: another 1–2 days of placement, after the owner decides.
   - Using CABD after a yes: half a day to map fields, plus about 1 day to place lower entrances.

Nothing here enters a release without the owner's yes.

## Unverified (did not fetch the source)
- The DFO SEP facility count (23, or 16 hatcheries + 7 spawning channels) and the 2021 hatchery map: the DFO Pacific site was not reachable.
- Counting-fence details (Babine, Kitwanga, Meziadin): from search snippets.
- Pacific Salmon Explorer layers and licence.
- NuSEDS fence estimate methods.
- The CWT location file.
- The bcfishpass fishway content.
- That no TRIM fish-ladder code exists.
- That "Cache Creek Falls" in CABD is the Bonaparte fishway (my inference).
- That FFSBC or community sites count as "government": a legal reading, not data.

Samples fetched for this page are in the session scratchpad only (cabd/bc.json, dfo/enh.csv, osm/bc.json, trim_h.json).
