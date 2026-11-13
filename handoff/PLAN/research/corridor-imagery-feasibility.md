# Offline imagery along river corridors: feasibility (research only)

2026-11-13. Research note. **No product code was changed and no ticket was opened.** It builds on two earlier notes:

- `handoff/PLAN/app-data/stream-imagery/SPEC.md` (2026-10-21): sources, user flow, packaging options, 8 owner questions.
- Ticket **T79** (POSTPONED 2026-10-28) and ledger decision **Q10**. The defaults still stand: no offline imagery
  before the first native release, no 10 m pack, a 250 m buffer, online-only on web.

What this note adds: the owner's quality bar ("high enough quality to actually tell what a river is doing"),
**measured stream lengths from the live release**, a size model per region and per river, a re-check of the
licences (Esri's offline terms, BC orthos and lidar, commercial vendors), what the app's code already does, a
look at real sample tiles, and a cheaper delivery design than the SPEC's option D.

Labels used below: **Fact** means checked, with a source. **Measured** means computed here from repo data or
sample files. **Estimate** means a model with a stated error. **Opinion** means my judgement. **Unverified**
means I could not confirm it.

---

## 1. Short answer

- **There is still no open, recent, sub-metre aerial photo of rural BC that we may store and serve.** That was
  re-checked today. The BC Data Catalogue lists every provincial orthophoto product as "Access Only". The Province's
  open answer is **lidar**: a $38M programme to fly the whole province (2023 on), released under OGL-BC.
- **Sentinel-2 (10 m) is not good enough. Plainly no.** A 20 m river is 2 pixels wide, and a 5 m creek is less
  than one. In the sample tile below, the river cannot be seen at all.
- **Esri World Imagery cannot be taken offline in our app.** Its export layer is for "ArcGIS applications and
  other applications built with an ArcGIS Runtime SDK", and Esri's terms forbid redistributing tiles. We use
  MapLibre.
- **What *can* be built legally:** an offline **1 m lidar relief image** (hillshade, or a "relative elevation"
  view) of corridors wherever lidar exists, plus **10 cm municipal photos** in the few towns that publish them
  under an open licence (North Cowichan, Surrey). Relief shows channels, side channels, bars, terraces, cut banks,
  old meanders under the trees, roads and tracks. It **does not show water**: no pools, riffles or colour,
  because lidar flattens the water surface.
- **Real photos of rural rivers need a licence**, either from the Province (GeoBC) or from a commercial vendor.
  At the 250 m buffer that is roughly 16,000 km² for the waters with their own entry, and about 100,000 km² for all
  named streams. **Unverified**: at ballpark archive prices of about $4–30/km² for a single-organisation licence, that
  comes to six to seven figures before any public-redistribution uplift.

---

## 2. What resolution "tells what a river is doing"

**Fact (Web-Mercator maths):** metres per pixel for a 256 px tile is 156,543 × cos(latitude) / 2^z.

| Zoom | 49°N | 52°N | 55°N | 60°N | Tile edge at 52°N |
|---|---|---|---|---|---|
| z14 | 6.3 m | 5.9 m | 5.5 m | 4.8 m | 1.5 km |
| z15 | 3.1 m | 2.9 m | 2.7 m | 2.4 m | 753 m |
| z16 | 1.57 m | 1.47 m | 1.37 m | 1.19 m | 376 m |
| z17 | 0.78 m | 0.74 m | 0.69 m | 0.60 m | 188 m |
| z18 | 0.39 m | 0.37 m | 0.34 m | 0.30 m | 94 m |
| z19 | 0.20 m | 0.18 m | 0.17 m | 0.15 m | 47 m |

**Opinion (rule of thumb from photo interpretation):** a feature needs about 3–5 pixels across to be recognised,
and about 10 pixels before its texture can be read (white water against glide, gravel against sand).

| What the angler wants to see | Size on the ground | Pixel needed | Zoom needed | Source resolution needed |
|---|---|---|---|---|
| Where a big river (50–300 m) runs, its braids, big bars, islands | 20–300 m | ≤ 3 m | z15–z16 | ≤ 2 m. Sentinel-2 is marginal for the largest channels only |
| Bars, riffle and pool sequence by colour on a big river, access tracks | 5–30 m | ≤ 1 m | z17 | 0.5–1 m |
| A small stream (5–20 m) as a channel, its bars and side channels | 5–20 m | ≤ 0.5 m | z18 | 0.3–0.5 m |
| Riffle against pool on a small stream, log jams, single big logs, a trail to the bank | 0.5–5 m | ≤ 0.2 m | z19 | 0.1–0.2 m (air photo) |

**Opinion:** "Tell what a river is doing" means **z18 at about 0.3–0.4 m for small streams** and **z17 at about 0.7 m for
big rivers**. That rules out every satellite source we may store (10 m and coarser). One caveat that no
resolution fixes: many 5–20 m creeks are **under canopy**, so no overhead photo shows them. Only lidar's bare-earth
model sees the channel under the trees.

---

## 3. Sources and licences for offline storage and serving from R2

Decision rule (unchanged from the SPEC): **if the licence does not clearly allow storing the imagery AND
redistributing it from our server, the answer is no.**

| Source | Resolution | BC coverage and age | Licence: the deciding clause | Rehost and bundle? |
|---|---|---|---|---|
| **Esri World Imagery** (online today) | Vantor "Vivid Standard 1.2 m to 0.6 m" over most of the world, plus community photos | All BC; mixed dates | Item page: *"This layer is not intended to be used to export tiles for offline."* The export layer is *"intended to support export of basemap tiles for offline use in ArcGIS applications and other applications built with an ArcGIS Runtime SDK"*, up to 150,000 tiles per request, and needs an ArcGIS account. Esri's terms summary prohibits *"redistributing basemap tiles"* and harvesting tiles outside Esri Content Packages ([item](https://www.arcgis.com/home/item.html?id=10df2279f9684e4a9f6a7f08febac2a9), [export item](https://www.arcgis.com/home/item.html?id=226d23f076da478bba4589e7eae95952), [TOU summary](https://downloads2.esri.com/arcgisonline/docs/tou_summary.pdf), [e300](https://www.esri.com/content/dam/esrisites/en-us/media/legal/product-specific-terms-of-use/e300.pdf)). A third party (Gaia GPS) reported losing Esri offline downloads in Jan 2025 ([blog](https://blog.gaiagps.com/esriupdate), **unverified** on Esri's side) | **NO** |
| **BC provincial orthophotos** (GeoBC: 1 m mosaic, 0.5 m regional, per-sheet orthos) | 0.3–1 m | Whole province for the 1 m mosaic, but it dates from **1995–2004**; newer per-sheet orthos are partial and sold | The catalogue licence is **"Access Only"** for `orthophoto-coverage`, `orthophoto-tile-*`, `imagery-web-map-service` and `base-map-online-store` (re-checked today with the CKAN API). The WMS: *"cannot be cascaded through another Web Map Service without the express written permission of the Province"*. The Orthophoto Viewer offers free low-resolution images only ([orthophotos](https://www2.gov.bc.ca/gov/content/data/geographic-data-services/digital-imagery/orthophotos), [viewer](https://www2.gov.bc.ca/gov/content/data/geographic-data-services/digital-imagery/orthophotos/orthophoto-viewer)) | **NO** without written permission |
| **BC LiDAR (LidarBC)** | 1 m bare-earth DEM, plus point clouds | **14% of BC in Oct 2023**. The $38M programme to "acquire LiDAR data for the entire Province" began in 2023 and concentrates on rural areas first. **Today's % is unverified** ([release](https://news.gov.bc.ca/releases/2023WLRS0054-001554)) | **OGL-BC.** The portal record says *"The data in the portal is released as Open Data under the Open Government Licence – British Columbia"*. OGL-BC: *"Copy, modify, publish, translate, adapt, distribute or otherwise use the Information in any medium, mode or format for any lawful purpose"*, with attribution (default: *"Contains information licensed under the Open Government Licence – British Columbia."*) ([catalogue](https://catalogue.data.gov.bc.ca/dataset/lidar), [OGL-BC](https://www2.gov.bc.ca/gov/content/data/open-data/open-government-licence-bc)) | **YES** (as a derived relief image) |
| **NRCan HRDEM mosaic** (CanElevation) | 1–2 m DTM/DSM, served as hillshade and slope over WMS/STAC | Wherever lidar has been flown in Canada; includes provincial lidar | **OGL-Canada** ([dataset](https://open.canada.ca/data/dataset/0fe65119-e96e-4a57-8bfe-9d9245fba06b)) | **YES** |
| **Municipal orthos: North Cowichan** | 10 cm; 2017, 2019, 2022, 2024 (flown May–July) | Inside the municipality: the lower **Chemainus River** and part of the Cowichan system | *"This data may only be used under the terms of the Open Government Licence - Municipality of North Cowichan"* ([catalogue](https://catalogue.data.gov.bc.ca/dataset/north-cowichan-ortho-imagery-lidar-contours)). The 2022 set is about 290 GeoTIFF zips of about 36 MB each (measured: one HEAD request) | **YES** |
| **Municipal: Surrey** | high-res | Lower Serpentine, Nicomekl, Campbell | OGL-Surrey (SPEC §3) | **YES** |
| Municipal: Kelowna, Maple Ridge (2025), Langley | high-res | Inside those towns | Maple Ridge 2025 is reported under OGL-Metro Vancouver ([OSM thread](https://community.openstreetmap.org/t/new-imagery-for-maple-ridge-bc/145506)); Kelowna is **unverified** | Treat as NO until a person reads each licence |
| **Sentinel-2** (Copernicus, ESA WorldCover composites) | **10 m** | All BC; any year | Free, full and open, redistribution allowed with *"Contains modified Copernicus Sentinel data [year]"* (SPEC §3) | YES, **but useless for this purpose** (§2, §6) |
| EOX s2cloudless | 10 m | All | 2016 edition CC BY 4.0; 2018+ editions CC BY-NC-SA | 2016 only; useless |
| Landsat 8/9 | 30 m (15 m pan) | All | Public domain | YES; useless |
| NRCan Aerial Imagery of Canada (AWS) | 15 cm | **Measured today: the bucket root still holds only `national-parks/banff-national-park/`** | OGL-Canada | Would be YES; nothing in BC |
| NAIP | 0.6 m | **US only; stops at 49°N** | Public domain | Not applicable |
| Mapbox Satellite | high-res | All | Offline only inside the Mapbox SDK; *"may not be preloaded, bundled or otherwise redistributed"* (SPEC §3) | **NO** |
| MapTiler Satellite | high-res | All | Self-hosting needs a paid data plan plus MapTiler Server; the price is a sales quote ([pricing](https://maptiler.com/data/pricing), [self-host satellite](https://maptiler.com/server/self-host-satellite-maps), [terms](https://www.maptiler.com/terms/server-data/)) | **NO** for MapLibre on R2 without a negotiated licence (**unverified** whether one is offered) |
| Google Map Tiles API | high-res | All | The policies list *"prefetching, caching, and storage of content"* and offline use as restricted ([policies](https://developers.google.com/maps/documentation/tile/policies)) | **NO** |
| Vantor (ex-Maxar), Airbus, Planet | 0.3–0.5 m (Vantor, Airbus); 3 m (Planet) | Archive coverage of rural BC; dates vary | Commercial EULA. Ballpark archive prices: Airbus 50 cm from €3.80/km², 30 cm €18/km² (minimum 25 km²) ([guide](https://geoawesome.com/demystifying-satellite-data-pricing-a-comprehensive-guide/)). A licence that lets the public download the tiles is **unverified** and normally costs extra. Planet sells per km² per year ([Planet](https://docs.planet.com/data/imagery/basemaps)) | Only with a negotiated licence and a budget |
| Maxar Open Data | 30–50 cm | Disaster events only (Kelowna 2023) | CC BY-NC 4.0 | NO |

**Opinion:** the only open route to a sharp image of a rural river is **lidar relief**. The only route to a
*photo* is a **licence from GeoBC** (ask about recent per-sheet orthos, not the 1995–2004 mosaic) or a vendor.

---

## 4. Size

### 4.1 Stream lengths (measured from the live release)

Source: release `r-0a9930fb8826`, `water-*.sqlite`, table `part_section.m` (the section's length along the
water, metres, T387). Each section is counted once, kind `stream`, and assigned to the first region of its item
(`core.item_area`). Lakes are excluded: their sections add 24,389 km of "length" that is not a corridor.

| Set | Items | Sections | Length |
|---|---|---|---|
| **All named streams** in the release | 12,113 | 73,686 | **204,693 km** |
| **Streams with their own synopsis entry** (`entries` not empty) | — | — | **31,337 km** |
| Median item length: all / with an entry | | | 8.8 km / 30.6 km |
| 90th-percentile item length: all / with an entry | | | 33.9 km / 137 km |

Rivers used as examples (km in the release, including side channels): Vedder 13 · Stamp 22 · Englishman 42 ·
Cowichan 55 · Chilliwack 59 · Coquihalla 65 · Cheakamus 81 · Thompson 147 · Squamish 181 · Elk 263 · Dean 299 ·
Bulkley 312 · Skeena 883 · Fraser 1,502.

### 4.2 Tiles and bytes

**Estimate (model):** tiles per zoom = L × (2·buffer + tile edge) / tile edge², at 52°N, summed from z12 up to
the top zoom. Accuracy is about ±30%. Wide rivers add their own width, which is ignored here. Overlap between
neighbouring corridors (confluences, dense networks) is **not** removed, so the province totals are upper
bounds, and for "all named" probably 20–40% high (**unverified**).

Bytes per 256 px tile:

| Kind | JPEG | WebP | AVIF | How |
|---|---|---|---|---|
| Lidar hillshade at 1.2 m/px (grey) | 16.6 KB | 15.2 KB | 8.3 KB | **Measured**: 16 tiles cut from the NRCan sample, PIL q75, avifenc q55 |
| Air photo at z16–z19 | 18 KB | 13 KB | 9 KB | **Estimate**. Esri's own JPEG tiles at this spot measured 15.8 / 13.8 / 12.3 / 10.6 KB at z16 / z17 / z18 / z19 (byte counts only, not stored), but Esri compresses hard and upsamples rural z19 |

Tiles per km of river (cumulative from z12):

| Buffer each side | to z16 | to z17 | to z18 | to z19 |
|---|---|---|---|---|
| 100 m | 7 | 18 | 51 | 163 |
| **250 m** | 10 | 29 | **96** | 343 |
| 500 m | 15 | 48 | 172 | 644 |

**Province-wide archive on R2**, photo layer, WebP (JPEG ≈ ×1.4, AVIF ≈ ×0.7):

| Set | Buffer | to z17 | to z18 | to z19 |
|---|---|---|---|---|
| Own-entry streams (31,337 km) | 100 m | 7 GB | 21 GB | 66 GB |
| | **250 m** | 12 GB | **39 GB** | 140 GB |
| | 500 m | 20 GB | 70 GB | 263 GB |
| All named (204,693 km) | 100 m | 48 GB | 136 GB | 433 GB |
| | **250 m** | 78 GB | **256 GB** | 914 GB |
| | 500 m | 128 GB | 457 GB | 1.7 TB |

A lidar hillshade layer to z17 (its native resolution) at 250 m is about **0.45 MB/km** in WebP (0.25 MB/km in
AVIF). That gives **14 GB** for own-entry streams and **92 GB** for all named streams.

**Per region** at the 250 m buffer (WebP; hillshade to z17, photo to z18):

| Region | All named km | Own-entry km | Hillshade, all named (GB) | Hillshade, own entry (GB) | Photo z18, all named (GB) | Photo z18, own entry (GB) |
|---|---|---|---|---|---|---|
| 1 | 13,819 | 3,156 | 6 | 1.4 | 17 | 4.0 |
| 2 | 12,589 | 4,277 | 6 | 1.9 | 16 | 5.4 |
| 3 | 16,760 | 3,688 | 7 | 1.6 | 21 | 4.6 |
| 4 | 23,188 | 4,893 | 10 | 2.2 | 29 | 6.1 |
| 5 | 21,651 | 3,007 | 10 | 1.3 | 27 | 3.8 |
| 6 | 46,485 | 6,392 | 21 | 2.8 | 58 | 8.0 |
| 7a | 23,590 | 2,509 | 11 | 1.1 | 30 | 3.1 |
| 7b | 36,615 | 2,212 | 16 | 1.0 | 46 | 2.8 |
| 8 | 9,995 | 1,204 | 4 | 0.5 | 13 | 1.5 |

A whole region is **too big for a phone download**: even own-entry waters only are 1–8 GB of photo per region.
**Per-water packs are the unit.**

**Per river** (photo, WebP, MB, measured length):

| Water | km | 250 m to z17 | 250 m to z18 | 250 m to z19 | 100 m to z18 | 500 m to z18 |
|---|---|---|---|---|---|---|
| A 10 km stretch | 10 | 4 | 13 | 45 | 7 | 22 |
| Median own-entry water | 31 | 12 | 38 | 137 | 20 | 68 |
| Cowichan River | 55 | 21 | 69 | 247 | 37 | 123 |
| Chilliwack + Vedder | 73 | 28 | 91 | 325 | 48 | 162 |
| Bulkley River | 312 | 119 | 390 | 1,392 | 207 | 696 |
| Skeena River | 883 | 336 | 1,106 | 3,941 | 587 | 1,969 |

Hillshade adds about 35% of the z17 photo figure (0.45 MB/km).

### 4.3 What a phone and R2 can bear

- **Phone (measured from code):** the app already installs about **531 MB of stream tiles plus the basemap**
  (`app/packages/data/src/release/install.ts` cites 274 MB, and 918 MB for an older build). The SPEC's caps are
  300 MB per pack and 1 GB in total. Under those caps, a **z18 pack at 250 m covers about 240 km per 300 MB**: a
  median water or a Cowichan fits easily, a Bulkley needs per-part packs, and a Skeena needs per-part packs or a
  z17 cap.
- **R2 (Fact, [pricing](https://developers.cloudflare.com/r2/pricing/)):** storage costs $0.015/GB-month, egress is
  free, Class B reads cost $0.36 per million, and the free tier covers 10 GB plus 10M reads.
  - Own-entry, 250 m, photo to z18 plus hillshade: about 53 GB, roughly **$0.65/month**.
  - All named, the same layers: about 350 GB, roughly **$5/month**.
  - Per-water pre-cut packs double this at most.
  - A 300 MB pack fetched as one file is one read. Fetched tile by tile it is about 25,000 reads, which is why
    whole-file packs are preferred (§5).

---

## 5. Delivery design

### 5.1 What the app already does (checked in code)

- **Native:** `@maplibre/maplibre-react-native` 11.5.0 (patched, T74/T217) on MapLibre Native Android 13.6.1 /
  iOS 6.31.0. It reads the local `atlas.pmtiles` and `basemap.pmtiles` from `pmtiles://file:///` paths
  (`app/deps.md`, `Map.native.tsx`). Both are **vector** archives. **There is no raster PMTiles on the device
  today.**
- **Satellite** is one online raster source: Esri `World_Imagery/MapServer/tile/{z}/{y}/{x}`, from
  `packages/map/style/layers.source.json`. When offline it falls back to the plain map with the line
  *"Satellite needs a connection…"* (`packages/core/src/imagery.ts`).
- **Big-file install is solved:** `install.ts` streams large files to `*.part`, verifies the sha256 natively, and
  renames. `bgDownload.ts` runs the first install with the app closed. A per-water imagery pack could reuse this
  path unchanged.
- **Expected but not tested:** MapLibre Native's PMTiles support is the same file source for raster and vector, so a
  `{type:"raster", url:"pmtiles://file:///…/pack.pmtiles", tileSize:256}` source should render. Prove it on the
  emulator before building anything.
- MapLibre Native offline packs (`OfflineManager`) are not involved. The file *is* the offline copy.

### 5.2 Options

| Option | How | Fit |
|---|---|---|
| A. Pre-bundled with the release | Imagery inside the install | **No.** Tens to hundreds of GB |
| B. Per-region pack | One file per region | **No.** 1–58 GB per region (§4.2) |
| C. Device extracts a corridor from a province-wide archive (SPEC option D) | Range-reads tiles from R2 and writes a local pmtiles | Works, but needs a **new pmtiles writer in JS**, makes thousands of range requests, and fails silently if the header is wrong (SPEC risk 7) |
| **D. Server pre-cuts one pack per water (per part for long rivers)** | Pipeline builds the province-wide layer archive, then runs `pmtiles extract <archive> --region=<water corridor>.geojson` per water, writing `imagery/<layer>/<item_id>[/<part>].pmtiles` plus an index with bytes, vintage and attribution | **Recommended (opinion).** The phone downloads **one file** through the existing streamed install (`.part`, sha, rename): no writer, one R2 read. Keyed by **`item_id`**, which is durable (AGENTS rule 5; `section_id` must never leave the bundle). Cost: storage roughly doubles (still a few dollars a month) and a rebuild when corridors change |

### 5.3 Build pipeline (none of it exists; GDAL is not installed on this Mac)

1. **Corridors:** buffer each stream item's lines from the atlas (BC Albers, round caps, dissolve), at 250 m,
   and keep a 500 m union as the build mask. Run it as a heavy job via `scripts/heavy.sh`.
2. **Layers:**
   - *Hillshade:* fetch the 1 m DTM COGs (LidarBC or HRDEM STAC) that intersect the mask, then
     `gdaldem hillshade -multidirectional`. Optionally add a relative-elevation model (height above the channel),
     which reads river form far better than hillshade (opinion).
   - *Municipal photo:* mosaic and clip to the town boundary.
3. **Clip and tile:** `gdalwarp -cutline mask -dstalpha -t_srs EPSG:3857`, then
   `gdal_translate -of MBTILES -co TILE_FORMAT=WEBP`, then `gdaladdo`, then `pmtiles convert`. GDAL's PMTiles
   driver writes vector only, so the route goes through MBTiles (SPEC §4).
4. **Extract** per-water packs (`pmtiles extract --region`), write `imagery/index.json`, and upload to R2.
5. **Cadence:** the ground does not go stale like regulations. Rebuild **once a year** when LidarBC adds coverage,
   or when a new licensed source arrives. Packs are **not** tied to the tiles/bundle vintage (SPEC §6): they are
   matched to waters by `item_id` and the corridor polygon.

---

## 6. Sample tiles looked at today (scratch only, nothing in the repo)

Place: lower **Chemainus River** (48.899°N, −123.727°W), inside North Cowichan.

- **Sentinel-2 (EOX 2016, CC BY) at z14, 6 m/px (7 KB):** the river **cannot be found**. The highway and a field
  are visible, and everything else is a green smear. The z15 tile is only 4.8 KB, a sign of no added detail (not inspected). This
  confirms that 10 m is useless for this purpose.
- **NRCan HRDEM DTM hillshade at 1.2 m/px** (one 1024 px WMS image over a z15 tile, 805 KB PNG): the **channel is
  obvious** as a smooth flat ribbon about 30 m wide. Also clear: both cut banks and terrace edges, a tributary
  ravine, gullies, roads, the highway and building pads. **Inside the channel there is nothing to read**: lidar
  flattens the water surface (triangulation facets are visible), so no pools, riffles or wood. Bars above the water
  line would show; this reach had none in frame.
- **North Cowichan 10 cm ortho:** not downloaded (each GeoTIFF zip is about 36 MB, and the grid-to-river lookup
  needs the town's grid map). At 10 cm it would clearly meet the bar. It covers only this town.

**Opinion:** hillshade answers "where does the river go, where are the side channels, banks and access". It does
not answer "where is the pool". For that, only a photo will do.

---

## 7. A cheap proof the owner could look at

Goal: one 10 km stretch, three layers side by side, at z16–z19. **Cost: about half a day of agent time, no money,
about 0.5–1 GB of downloads.** Not done now.

1. `brew install gdal` (`pmtiles`, `cwebp` and `avifenc` are already installed).
2. Pick the lower Chemainus River (the only river with all three layers). Optionally add a 10 km rural stretch
   with lidar (for example the Bulkley), because that is the realistic case.
3. Corridor: take the item's lines from the atlas (or FWA) for that stretch and buffer 250 m (shapely/ogr2ogr).
4. Photo: from the North Cowichan grid map, list the 2024 ortho tiles the corridor crosses (about 10–20 zips),
   download them, run `gdalbuildvrt`, then `gdalwarp -cutline` to EPSG:3857, then MBTiles WebP z12–z19,
   `gdaladdo`, and `pmtiles convert`.
5. Relief: fetch the HRDEM 1 m DTM COG over the corridor (STAC at datacube.services.geo.ca), run `gdaldem hillshade
   -multidirectional`, and tile it the same way to z17. Optionally add the Sentinel-2 z14 tiles for contrast.
6. Look: open the files in the local pmtiles viewer, or load them as raster sources in the web app's dev style.
   Take screenshots at z16, z17, z18 and z19, and send the images to the owner (the owner cannot open repo paths).
7. Optional, the native proof: copy one pack to the emulator and add it as a `pmtiles://file:///` raster source.
   This confirms §5.1's "expected" with no new package.

---

## 8. Recommendation, size and risks

**Recommendation (opinion):**

1. **Drop the idea of offline Esri or any satellite pack.** Esri is not allowed, and Sentinel or Landsat do not
   meet the bar.
2. **The legal, useful layer is offline lidar relief along corridors**, labelled for what it is ("Terrain relief
   1 m, lidar 20xx"), plus **10 cm photo where a town publishes it under an open licence**. Ship it as per-water
   packs (option D, §5.2) after Play closed testing.
3. **In parallel, ask GeoBC** (Copyright Permission Request, or LiDAR@gov.bc.ca and GeoBC imagery) whether recent
   orthos can be licensed for corridor tiles in a free fisheries app. That is the only route to photos without a
   vendor bill.
4. Do the **cheap measurements first**: the lidar coverage % of our 31k km of own-entry streams (overlay the
   LidarBC index on the corridors), and the §7 proof.

**Size:**
- P0 (lidar coverage overlay, §7 proof): **S**.
- Pipeline (corridors, hillshade, extract, R2): **M**.
- App (pack download, manager, raster source, attribution, caps): **M**.
- In total, **L**.
- With a licensed photo source added: **XL**, and gated on the licence.

**Risks:**
1. **Expectation gap.** Relief is not a photo. If it is called "high-res imagery", users will feel misled.
2. **Lidar coverage** may still be thin in the north (14% in 2023, current figure unverified).
3. **Canopy.** Even a perfect photo hides small creeks. Only lidar sees under the trees.
4. **Licence drift.** EOX already went NC. Pin the licence URL and the check date for each layer.
5. **Old imagery misleads.** Channels move. Always show the vintage on the map.
6. **Native raster PMTiles is untested** on our patched build (§5.1).
7. **A sharper view tempts trespass.** Keep the access disclaimers visible.
8. **Province totals are upper bounds** (corridor overlap is not removed, ±30% model).

## 9. Questions only the owner can answer

1. Is a **1 m lidar relief image** (channels, banks, bars and access, but no water detail) acceptable as the
   offline layer, or is the feature only worth building with real photos?
2. May we **ask GeoBC** for permission or a licence for recent orthos? Is there **any budget** for imagery (GeoBC
   per-sheet fees, or a vendor at roughly $4–30/km² for about 16,000 km² plus a public-use licence)?
3. **Scope:** only waters with their own synopsis entry (31k km), or every named stream (205k km)?
4. Default **buffer** (100 / 250 / 500 m) and **top zoom** (z17 or z18), given that 250 m to z18 is about 1.25 MB/km
   for photos.
5. Is **option D** (server pre-cut per-water packs, downloaded as one file) OK, in place of the SPEC's on-device
   extraction?
6. Should the **web** stay online-only (Q10 default)?
