# Stream imagery corridor: SPEC (not to be implemented yet)

Owner, 2026-10-21: *"a way to get high resolution imagery in a buffer around a stream (the user can download a
stream imagery) ... or even within a section on the river (buffered around stream). is there anywhere we could get
images free?"*

Status: **spec only. Nothing here is built.** This records what is possible, what it costs, and what the
owner needs to decide. Licence claims were checked on 2026-10-21 against the URLs given. Where a check failed, the
text says so, and those sources are treated as NOT allowed.

---

## 0. Short answer

**There is no free, current, sub-metre aerial photo of rural BC rivers that we may store on a phone or serve
ourselves.** Every sub-metre source that covers rural BC is either sold, licensed "Access Only", or restricted to
offline use inside the vendor's own SDK. Most fishing water is rural.

What we *can* legally download, keep offline and serve from R2:

| Tier | What | Pixel | Where in BC | Vintage | Verdict |
|---|---|---|---|---|---|
| **T1** | Sentinel-2 colour (ESA WorldCover composites, or our own summer median) | 10 m | all | 2020-21 (WorldCover) / any year (own composite) | allowed. **Same sharpness as the online satellite layer we already have** |
| **T1.5** | **Lidar bare-earth hillshade** (BC LiDAR OGL-BC; NRCan HRDEM OGL-Canada) | 1-2 m | wherever lidar has been flown (coverage % NOT verified) | varies | allowed. **The only open ~1 m view of rural channels.** It is a relief image, not a photo |
| **T2** | Municipal open orthophotos (North Cowichan, Surrey; Kelowna, Maple Ridge and Langley still to verify) | 7.5-15 cm | inside those boundaries only | 2017-2025 | allowed where the OGL is confirmed |
| **T3** | Provincial 1 m / 0.5 m orthos (GeoBC) | 0.5-1 m | whole province (1 m mosaic) | **1995-2004** for the mosaic | **NOT allowed** without the Province's written permission |

Honest consequence: a T1 pack only makes the existing 10 m satellite look work offline. It adds no detail.
The thing that would actually show runs, side channels, bars and old meanders under the trees in rural BC is
**T1.5 lidar hillshade**. The thing that would give real photos is **T3, and only if the Province grants a
licence**. Even then the mosaic is 20-30 years old, and rivers have moved since.

---

## 1. User flow

1. **Pick.** From a water screen, choose *Download imagery*. The scope is either the **whole stream** (every
   part/section of the item) or **this section** (the one open now). On the map, a long-press on a section offers
   the same.
2. **Preview.** Before any download, draw the corridor outline on the map and show:
   - the buffer chooser: 100 / **250** / 500 m each side (default: an owner question, §11);
   - the layers that exist for this corridor, each with pixel size, vintage and size in MB, for example:
     *"Satellite 10 m (2021), 0.2 MB"*, *"Terrain relief 1 m (lidar 2019), 9 MB, covers 70% of the
     corridor"*, *"Air photo 10 cm (North Cowichan 2024), 38 MB, covers 2.1 of 14 km"*;
   - a plain warning where nothing better than 10 m exists: *"Only 10 m satellite is available here. Small
     creeks are narrower than one pixel."*
   - free space on the device and the total after this download.
3. **Download.** The download is resumable and runs in the background where the platform allows. Show progress
   in MB. On failure, keep the partial file and offer *Resume*. Never write a half-built pack where the map
   would read it.
4. **Use offline.** When the user picks the satellite ground, a downloaded pack draws inside its corridor and
   takes precedence over the online imagery there. With no network, outside every pack, the satellite ground shows
   the plain map, as it does today. A small chip on the water screen says *"Imagery saved, 250 m, 2021 / 2019"*.
5. **Manage.** Settings has a *Downloaded imagery* list with one row per pack: the water name, section, buffer,
   layers, vintage of each layer, size and date downloaded. Each row offers *Delete* and *Update* (shown only when
   the server has a newer vintage). The list shows the total used against the cap.
6. **Expiry and vintage.** Packs never expire on a timer: imagery does not go stale the way regulations do. The
   **vintage is always shown** in the pack row, in the attribution, and as a one-line note on the water screen when
   the newest layer is more than 10 years old. When the server publishes a newer vintage for a layer, the row
   offers *Update*.

---

## 2. The corridor

**Geometry source: our own atlas, nowhere else.** This follows the owner rule that there is no second geometry
store. The section handle *is* the MVT feature id (P18), and `trace-geometry.ts` already copies a section's lines
out of `atlas.pmtiles` by id at any zoom. The corridor is built from those same lines:

1. Collect the line(s): one section, or every section of the item. Use z14 (the atlas maxzoom, where every
   reach exists).
2. Project to BC Albers (EPSG:3005), buffer by D metres with round caps, dissolve, and simplify by about 5 m.
3. Reproject to EPSG:4326. The result is the **corridor polygon**: it is drawn in the preview and used to choose
   tiles.
4. Lakes on the route: a section that passes through a lake (the `river__lake` split) buffers its lines only, so
   the corridor covers a strip through the lake and not the whole lake. Lake imagery is out of scope; it could
   be a later "lake pack".
5. **Whole stream vs one section.** A whole stream is the union of its sections' corridors. A pack for a section
   that is already inside a downloaded whole-stream pack is offered as *already saved*. Overlapping packs share
   nothing on disk in v1, because simple beats deduplication here; the manager shows the overlap.
6. `part_geo` boxes from the release (integer microdegrees per part) are used **only** for the size estimate,
   before any tile is read. They are not used as the corridor: a box around a winding river is mostly land that
   nobody asked for.

**Zoom range per layer.** Each layer stops at its own native maxzoom, and MapLibre overzooms above it. This
matches the overzoom rule the owner already set: every source keeps its own maxzoom.

| Layer | Stored zooms | Why |
|---|---|---|
| Sentinel-2 10 m | z10-z14 | z14 is about 6 m/px at 50N. Anything above z14 is only upsampling |
| Lidar hillshade 1-2 m | z12-z17 | z17 is about 0.77 m/px |
| Municipal 7.5-15 cm | z14-z19 (cap) | z20-21 grow to hundreds of MB per corridor. Cap at z19 (about 0.19 m/px) |
| Provincial 0.5-1 m (if ever licensed) | z12-z17 (z18 for 0.5 m sheets) | |

---

## 3. Sources, ranked

Default rule: **if the licence does not clearly allow offline storage AND redistribution from our server, the
answer is NO.**

### Allowed

1. **ESA WorldCover Sentinel-2 composites (T1, recommended input).** These are ready-made, cloud-free yearly median
   composites for 2020 and 2021. They are global, about 10 m, carry bands B02/B03/B04/B08, and come as COGs in
   1x1 degree tiles on `s3://esa-worldcover-s2` (no request signing needed). The licence is **CC BY 4.0**. This is
   better than EOX 2016: newer, open, and already composited, so we need no per-corridor cloud masking.
   https://registry.opendata.aws/esa-worldcover-vito-composites/
   *(Check the exact attribution string on the registry or product page before shipping.)*
2. **Copernicus Sentinel-2 L2A scenes (T1, if we want a newer year than 2021).** Access is free, full and open.
   Reproduction, distribution, communication to the public, and adaptation/combination are all allowed.
   Attribution: *"Contains modified Copernicus Sentinel data [year]"*. The COGs on AWS `s3://sentinel-cogs` need
   no account; STAC is at https://earth-search.aws.element84.com/v1 (no SLA). Coastal cloud means we would have to
   build a summer median composite ourselves.
   Licence: https://ads.atmosphere.copernicus.eu/licences/ec-sentinel (sentinel.esa.int no longer resolves) ·
   https://registry.opendata.aws/sentinel-2-l2a-cogs/
3. **EOX Sentinel-2 cloudless, 2016 edition only (T1 fallback).** Licence: CC BY 4.0. This is what the online
   satellite layer shows today. The 2018-2025 editions are **CC BY-NC-SA 4.0**, and commercial use needs a paid
   licence, so the answer for them is NO. Current attribution: *"EOxCloudless https://cloudless.eox.at by EOX IT
   Services GmbH (Contains modified Copernicus Sentinel data 2016)"*. The licence page says nothing about caching
   or offline use; only the pricing page states that 2016 is CC BY 4.0.
   https://cloudless.eox.at/pricing · https://cloudless.eox.at/documentation/license
4. **BC LiDAR (T1.5).** The BC Data Catalogue's "LiDAR" record is under the **Open Government Licence - British
   Columbia**. The LidarBC portal record itself is "Access Only", but the data carries OGL-BC. Bare-earth DEM at
   about 1 m. We would render a hillshade ourselves. BC coverage % is **not verified**.
   https://catalogue.data.gov.bc.ca/dataset/lidar
5. **NRCan HRDEM Mosaic, CanElevation (T1.5).** **OGL-Canada.** 1 m and 2 m DTM/DSM, a DSM hillshade, and STAC/WCS
   at datacube.services.geo.ca. Attribution: *"Contains information licensed under the Open Government Licence -
   Canada"*.
   https://open.canada.ca/data/dataset/0fe65119-e96e-4a57-8bfe-9d9245fba06b ·
   https://open.canada.ca/en/open-government-licence-canada
6. **North Cowichan orthoimagery (T2).** 10 cm; 2017, 2019, 2022 and 2024 editions; **Open Government Licence -
   Municipality of North Cowichan**. It is listed in the BC Data Catalogue with S3 tile lists at
   openfiles.northcowichan.ca, and it covers part of the **Cowichan River** system: real fishing water. It was the
   only open-licence ortho found in a catalogue-wide search.
   https://catalogue.data.gov.bc.ca/dataset/north-cowichan-ortho-imagery-lidar-contours
7. **City of Surrey orthophotos (T2).** Under the **Open Government Licence - Surrey**; downloaded from
   data.surrey.ca. Covers the lower Serpentine, Nicomekl and Campbell and the Fraser edge. (The "750 GB requests"
   note on that page is about LiDAR, not orthos.)
   https://www.surrey.ca/services-payments/online-services/open-data
8. **Landsat 8/9 (fallback only).** Public domain. 30 m (15 m pan), coarser than Sentinel, so it is only useful if
   Sentinel were ever unavailable. On AWS it is **requester-pays**.
   https://www.usgs.gov/faqs/are-there-any-restrictions-use-or-redistribution-landsat-data ·
   https://registry.opendata.aws/usgs-landsat/
9. **NRCan GeoBase Orthoimage 2005-2010 (SPOT 4/5).** OGL-Canada. 10 m pan and 20 m multispectral (about z13-z14),
   from 2005-2010. A "legacy product that is no longer supported", although the FTP still answers. Coarser and
   older than Sentinel, so no use to us.
   https://open.canada.ca/data/en/dataset/d799c202-603d-4e5c-b1eb-d058803f80f9
10. **OpenAerialMap.** Images are CC BY 4.0 by default. BC coverage is a handful of ad-hoc drone uploads, so it is
    not a dependable source. If we ever use it, check each image's metadata.
    https://openaerialmap.org/legal
11. **NRCan Aerial Imagery of Canada (AWS).** OGL-Canada, 15 cm. It would be ideal, but on 2026-10-21 the bucket
    held **only `national-parks/banff-national-park/`**. The `napl/` and `parks-canada/` prefixes that the registry
    documents are empty. **Watch it**: any watcher must list the bucket root, not the documented prefixes.
    Yoho, Kootenay, Glacier, Mt Revelstoke and Pacific Rim would be the BC parks to look for.
    https://registry.opendata.aws/aerial-imagery-canada/

### Still to verify. Treat as NO until a person reads the licence

- **City of Kelowna orthophotos** (Mission Creek lower reach). The page says downloads follow "the Open Government
  License", but our fetch was blocked (Cloudflare 403), so this is unconfirmed.
  https://kelowna.ca/city-services/maps-open-data/orthophotos
- **Maple Ridge (2025) and Langley** under OGL - Metro Vancouver. Check each dataset.
  https://community.openstreetmap.org/t/new-imagery-for-maple-ridge-bc/145506 ·
  https://community.openstreetmap.org/t/new-imagery-for-langley-city-township-imagery/147098
- **Copernicus Data Space quarterly cloudless mosaics** (global, 20 m resampled). No licence was confirmed.
  https://dataspace.copernicus.eu/node/1290

### Not allowed

- **BC Imagery WMS** (openmaps.gov.bc.ca/imagex: the 1 m provincial mosaic from 1995-2004, 0.5 m regional
  mosaics, SPOT 15 m, Landsat). Catalogue licence **"Access Only"** (id 22, `isopen: false`). GetCapabilities
  says: *"cannot be cascaded through another Web Map Service without the express written permission of the
  Province"*. This rules out both device-side cutting and server-side cutting.
  https://catalogue.data.gov.bc.ca/dataset/73b2a989-df6b-4298-bd38-c60f0254a453 ·
  https://openmaps.gov.bc.ca/imagex/ecw_wms.dll?service=wms&request=getcapabilities&version=1.3.0 ·
  https://catalogue.data.gov.bc.ca/dataset/bcad8f9e-274b-4a29-9e06-d05def3a50c2
- **GeoBC orthophotos per 1:20k sheet.** Sold at $100-200 per sheet and copyright of the Province. A buyer may
  keep a copy, but redistributing it to users needs a negotiated licence.
  https://www2.gov.bc.ca/gov/content/data/geographic-data-services/digital-imagery/orthophotos
- **City of Vancouver, CRD/Saanich, and the Data for Canada aggregator.** No clear open licence was found.
  Vancouver also has essentially no freshwater fishing water.
  https://opendata.vancouver.ca/explore/dataset/orthophoto-imagery-2008/ ·
  https://mapservices.crd.bc.ca/arcgis/rest/services/Ortho/MapServer/layers ·
  https://github.com/dataforcanada/d4c-datapkg-orthoimagery
- **Maxar Open Data.** CC BY-NC 4.0 and event-only (Kelowna 2023 fires).
  https://registry.opendata.aws/maxar-open-data/
- **Mapbox Satellite.** Offline only through the Mapbox SDK, and "the data may not be preloaded, bundled or
  otherwise redistributed". We use MapLibre, so this is NO.
  https://docs.mapbox.com/ios/maps/guides/offline/concepts/
- **Esri World Imagery, Google, Bing/Azure, MapTiler.** Proprietary. Offline use is only inside the vendor's own
  SDK, if at all, with no bulk export. (We could not read the Esri terms PDF, so the default NO stands.)
  https://www.esri.com/content/dam/arcgisonline/docs/tou_summary.pdf
- **Not applicable.** USDA NAIP covers the US only and stops at 49N (https://registry.opendata.aws/naip/).
  Planet NICFI covers the tropics only. Copernicus VHR is limited by eligibility and focused on Europe.
  Mapillary is street-level imagery, not overhead.

### The one real path to high-res rural photos

Ask the Province. File the BC **Copyright Permission Request Form**, or approach GeoBC, for permission to cut
corridor tiles from the provincial ortho mosaic. Explain that this is a free app for fisheries information,
with corridors of 1 km or less around regulated water, and that the attribution would be displayed.
https://www2.gov.bc.ca/gov/content/data/bc-data-catalogue/using-the-catalogue ·
https://www2.gov.bc.ca/gov/content/data/geographic-data-services/web-based-mapping/map-services
Caveat: the open mosaic dates from 1995-2004. If GeoBC would license **recent** per-sheet orthos instead, that
would be the prize, but it means a negotiated licence and possibly a fee (owner question).

---

## 4. Packaging: where the cutting happens

| Option | How | Licence fit | Cost / complexity |
|---|---|---|---|
| **A. Device cuts from a WMS** | The phone requests WMS GetMap tiles over the corridor and stores them | **Ruled out.** The only sub-metre WMS (BC imagex) forbids it, and the open sources do not publish WMS anyway | — |
| **B. Device cuts from a raw COG** | The phone reads the Sentinel/lidar COGs by range and renders tiles | Legal for the open sources | Needs a raster/reprojection engine on the phone, which goes against the "minimal prod packages" rule. **No.** |
| **C. Pre-cut pmtiles per corridor on R2** | A server job builds `<item>/<section>/<buffer>/<layer>.pmtiles` for every section | Legal only for redistributable sources (all of T1, T1.5 and T2 qualify) | Every section x every buffer x every layer has to be built and stored ahead of time, and rebuilt when sections change |
| **D. Province-wide raster pmtiles per layer on R2, extracted on the device (RECOMMENDED)** | One archive per layer (for example `imagery/s2-2021.pmtiles`, `imagery/hillshade.pmtiles`, `imagery/ortho-north-cowichan-2024.pmtiles`). The phone computes the corridor's tile list (z/x/y that intersect the polygon), fetches those tiles by HTTP range through the `pmtiles` JS reader the app already ships, and writes a small local pmtiles | Legal for redistributable sources | One build per layer per vintage, not per section. Any buffer and any section work, and the corridor never goes stale when sections are re-cut. The cost is a small pmtiles **writer** in JS (directory plus tile data; the format is simple), and corridors are clipped to whole tiles rather than to pixels |
| E. On-demand cutting worker with a cache | A small worker runs `pmtiles extract --region=corridor.geojson` against the layer archive and returns a file | Same as D | Adds a server that has to be running. Only worth it if the JS writer turns out to be a problem |

**Recommendation: D.** It needs no per-section build and no running service. It reuses the `pmtiles` reader the
app already ships and R2, which already serves the atlas. Whole-tile clipping is fine, and arguably better: at z14 a
tile is about 1.57 km across at 50N, so a 250 m corridor simply comes with some extra context.

### Server build per layer (pipeline side, offline job, nothing run yet)

1. **Choose the area.** Do not build province-wide at high zooms. Build only inside a **500 m buffer around every
   regulated stream in the atlas**. That one dissolved polygon covers every corridor a user can ask for, at any
   buffer up to 500 m.
2. **Source raster to EPSG:3857 COG.**
   - *T1*: mosaic the WorldCover composites (B04/B03/B02) and apply one fixed reflectance stretch to 8-bit, so
     corridors match across the province.
   - *T1.5*: hillshade the 1 m DTM in about 10 km chunks, with a multi-directional, z-factor-consistent
     greyscale style.
   - *T2*: mosaic the municipal tiles and clip them to the municipal boundary.
3. **Clip** to the stream-buffer polygon:
   `gdalwarp -cutline streams500.geojson -crop_to_cutline -dstalpha -t_srs EPSG:3857`.
4. **Tile**: `gdal_translate -of MBTiles -co TILE_FORMAT=WEBP`, then `gdaladdo -r average`, then
   `pmtiles convert x.mbtiles x.pmtiles`. **The GDAL PMTiles driver writes vector tiles only**, so the route has to
   go through MBTiles (https://gdal.org/en/stable/drivers/vector/pmtiles.html, https://docs.protomaps.com/pmtiles/cli).
   The archive must be clustered so that a corridor's tiles sit close together for range reads.
5. **Publish** to R2 under `imagery/` with a `imagery/index.json`. Each layer's entry holds its id, title, pixel
   size, vintage, licence name and URL, the exact attribution string, the coverage polygon (simplified), minzoom
   and maxzoom, the archive URL, and bytes per tile at maxzoom (used for the estimate in step 2 of §1).

Heavy jobs go through `handoff/tools/heavy.sh` with no more than 4 workers (standing rule). Hillshading lidar for
the whole province is the one heavy step.

---

## 5. Size and time

Tile counts below are computed for a sinuous 20 km river at 50N. z16 = 393 m tile, 1.54 m/px · z17 = 197 m,
0.77 m/px · z18 = 98 m, 0.38 m/px.

**Tiles per km of river, by zoom** (cumulative is z10 up to the zoom shown):

| Corridor | z14 | z15 | z16 | z17 | z18 | cum. to z17 | cum. to z18 |
|---|---|---|---|---|---|---|---|
| 100-150 m each side (about 300 m wide) | 0.9 | 2.4 | 6.5 | 18 | 58 | ~29 | ~87 |
| 250 m each side | 1.1 | 2.9 | 8.4 | 26 | 86 | ~39 | ~125 |
| 500 m each side | 1.7 | 4.3 | 12.6 | 43 | 155 | ~62 | ~217 |

**MB per km** (about 20 KB per 256 px WebP photo tile; hillshade is greyscale and smoother, about 10-15 KB, an
estimate not yet measured):

| Layer (maxzoom) | 300 m corridor | 500 m each side | 50 km river, 300 m |
|---|---|---|---|
| Sentinel-2 (z14) | < 0.01 MB/km | < 0.01 MB/km | **< 0.2 MB** (only about 30 pixels across) |
| Lidar hillshade (z17) | ~0.3-0.45 MB/km | ~0.6-0.9 MB/km | ~15-22 MB |
| Photo 0.5-1 m (z17) | ~0.6 MB/km | ~1.2 MB/km | ~30 MB |
| Photo 0.5 m (z18) | ~1.7 MB/km | ~4.3 MB/km | ~87 MB |
| Photo 10 cm, capped (z19) | ~5-6 MB/km (estimate, 4x z18) | ~15 MB/km | ~280 MB: **per-section only** |

**Download time** (whole tiles, many small range requests batched into contiguous runs): a 10 km section with
hillshade and Sentinel at 250 m is about 6 MB, which takes about 5 s on LTE at 10 Mbps and about 25 s on a weak
2 Mbps signal. A 50 km river with z18 photos is about 87 MB: about 70 s at 10 Mbps, about 6 min at 2 Mbps. Range
reads are only efficient if the archive is clustered and the client merges neighbouring tile offsets into one
request.

**Server-side storage** (rough estimate, for the owner's budget question): BC has very roughly 100,000+ km of
regulated stream. That figure is **not measured; get it from the atlas before deciding.** A 1 km-wide band
(500 m each side) at z17 hillshade is about 62 tiles/km x 12 KB, so on the order of **75 GB**. The same band at
z14 Sentinel is about 1-2 GB. R2 charges roughly $0.015/GB-month with no egress fee, so about **$1-2/month**.
The real cost is the **one-off compute** to hillshade the lidar.

---

## 6. Storage limits and cleanup (device)

- Packs live in the app's documents directory under `imagery/<pack-id>/` (written with `expo-file-system`, which
  is already a dependency). Each pack holds one `.pmtiles` per layer plus `manifest.json`.
- **Manifest**: pack id, item id, section ids, buffer, the corridor polygon, and per layer: source id, vintage,
  licence and URL, attribution string, zooms, bytes, and the server index version it was cut from. It also records
  the atlas vintage it was traced against.
- **Caps**: no more than **300 MB per pack** (larger is refused, with *"try one section"*), and **1 GB total** by
  default, which the user can raise. Before downloading, check free disk space and refuse if the result would
  leave less than 1 GB free.
- Write to `*.part`, verify size and header, then rename. The map only ever reads complete files.
- **No automatic deletion.** Users download a pack for a trip into no signal, and deleting it on our own would
  break exactly that trip. Deletion is manual from the manager. At the cap, the manager lists packs oldest-first
  to help the user choose.
- **Atlas rebuilds re-cut sections** (registry change leads to entry resync, and handles can remap). The imagery
  itself stays valid because the ground did not move, so a pack is **never invalidated by an atlas vintage
  change**. Only its link to "section X" can go stale. On a vintage change, re-match each pack to sections by its
  stored corridor polygon. If none match, show it as "saved area near <name>" and keep drawing it.

---

## 7. Attribution

- While any pack layer is visible, the map attribution control shows each visible layer's string from the
  manifest, exactly as stored. The manager and the preview show them too.
- Strings:
  - Sentinel (own composite): *"Contains modified Copernicus Sentinel data <year>"*
  - WorldCover: per the product page (confirm before shipping)
  - EOX 2016: *"EOxCloudless https://cloudless.eox.at by EOX IT Services GmbH (Contains modified Copernicus Sentinel
    data 2016)"*
  - NRCan HRDEM: *"Contains information licensed under the Open Government Licence - Canada"*
  - BC LiDAR: *"Contains information licensed under the Open Government Licence - British Columbia"*
  - Municipal: *"Contains information licensed under the Open Government Licence - <Municipality>"*
- Native: MapLibre RN's attribution is set from the source's `attribution` field. Check that 11.5.0 renders it,
  and if not, draw our own line.

---

## 8. How it sits under the existing map

- **One raster source per pack layer**, `{type: "raster", url: "pmtiles://file:///…/imagery/<pack>/<layer>.pmtiles",
  tileSize: 256}`, plus one raster layer placed **in the slot the online `imagery` layer already uses** (the
  `basemap: "map" | "satellite"` ground). It sits under every water layer, so regulation colouring always draws on
  top.
- **Native**: MapLibre RN 11.5.0 already reads `atlas.pmtiles` and `basemap.pmtiles` from local
  `pmtiles://file:///` paths (`app/deps.md`), so **no new dependency** is needed. MapLibre Native's offline packs
  ignore pmtiles sources, which is fine: the file *is* the offline copy.
  https://maplibre.org/maplibre-native/android/examples/data/PMTiles/
- **Layer order inside the ground**: online satellite (when the network is up) goes at the bottom, then pack
  layers, coarsest first: Sentinel, then hillshade (multiply blend at about 50% where colour exists, plain grey
  otherwise), then municipal photo. Each layer keeps its own maxzoom, and MapLibre overzooms above it.
- **Web** (`maplibre-gl` v5 plus the `pmtiles@4` protocol): the web app reads R2 live, so a download there gains
  little. Recommendation: on web, show the **same province-wide layer archives online** inside the corridor, with
  no download. Offline storage on web (OPFS plus a custom protocol) is deferred. Owner question.
- **Style generation**: pack sources are added at run time by the map adapter (the same path as `setTrace`), not
  baked into `runtimeStyle()`. The native/web parity test grows one case: a pack layer is below water on both.

---

## 9. Interaction with the owner rules

| Rule | How this fits |
|---|---|
| Tiles are geometry-only | Imagery goes in **separate raster archives**. Nothing is added to `atlas.pmtiles`, and no regulation data touches the imagery |
| Geometry only from the tiles (no second geometry store) | The corridor is buffered from the atlas lines by section id (`trace-geometry.ts`). The server's 500 m stream band is built from the same atlas. `part_geo` is used only for the size estimate |
| Satellite is online-only | This is **the one stated exception**, scoped to corridors the user chose to download. The general satellite ground stays online-only, and no area-wide or "cache what I looked at" behaviour is added |
| Always-offline core | Packs are optional extras. Nothing in regulations, status or conditions reads them, and the app is identical with zero packs |
| Bundle is the only source (for regs) | Unaffected: imagery is never a source of rules |
| Tiles and bundle share a vintage | Packs are deliberately **outside** this coupling (§6). They carry their own imagery vintage and are re-matched, not invalidated |
| Minimal prod packages | Option D needs no new package: the `pmtiles` reader, `expo-file-system` and MapLibre are already there. The only new code is a small pmtiles writer |
| Heavy jobs one at a time | The lidar hillshade build runs through `heavy.sh` with 4 workers or fewer |

---

## 10. Risks

1. **Expectation gap (biggest risk).** The owner asked for *high resolution*. For most rural water, the open answer
   is 10 m colour (the same as today's online layer) plus a 1 m relief image. If the UI calls it "high-res imagery",
   users will feel misled. Name the layers for what they are: *Satellite 10 m*, *Terrain relief 1 m*, *Air photo*.
2. **Lidar coverage unknown.** The T1.5 value depends on how much regulated water has lidar. Measure this before
   building (it is a cheap overlay of the lidar index with the atlas).
3. **Licence drift.** EOX already moved from CC BY to NC-SA after 2016. Each layer entry pins its licence URL and
   the date it was checked. Re-check licences before each new vintage.
4. **Old imagery misleads.** Channels move, and a 1995-2004 photo (if T3 is ever licensed) can show a river where
   there is now forest. Always print the vintage on the map, not only in Settings.
5. **Unverified municipal licences** (Kelowna, Maple Ridge, Langley). These are excluded until a person reads them.
6. **Range-read efficiency** on poor signal: many small tiles. Mitigate with a clustered archive and a client
   that merges contiguous offsets.
7. **pmtiles writer correctness.** A bad header makes MapLibre render nothing, silently; maplibre-gl 6 already
   failed like this once. Test it with a round-trip: write, read back with the `pmtiles` reader, and render.
8. **Storage pressure** on small phones. Mitigated by the caps and the free-space check.
9. **Atlas section remap** detaches packs from their sections. Mitigated by matching on the corridor polygon (§6).
10. **Safety and trespass.** A sharper photo can tempt access across private land. Keep the existing access and
    land-use disclaimers visible on any pack view.

---

## 11. Phased plan (when the owner says go)

- **P0. Decisions and measurements (no build).** Owner answers §12. Measure regulated stream km and lidar
  coverage % from the atlas. Read the Kelowna, Maple Ridge, Langley and WorldCover attribution terms. Optionally
  file the BC Copyright Permission Request.
- **P1. Layer archives (pipeline).** Build the 500 m stream band. Build the T1 Sentinel archive (z10-z14) from
  WorldCover 2021. Publish `imagery/index.json` and the archive to R2.
- **P2. Device download (native).** Corridor from `trace-geometry`, tile list, range fetch with offset merging,
  the pmtiles writer, the manifest, and the caps. Manager screen. Preview with size.
- **P3. Map integration.** Pack sources in the ground slot, attribution, parity test, and the vintage chip.
- **P4. T1.5 hillshade.** Hillshade the lidar inside the band (heavy job), z12-z17 archive, and blend rule.
- **P5. T2 municipal.** North Cowichan and Surrey first, then the others once their licences are confirmed.
  Coverage polygons in the index, so the preview reports "covers x of y km".
- **P6. T3, only if the Province grants permission.** Provincial ortho archive under the granted terms, with the
  attribution and vintage those terms require.
- **Web**: online view of the layer archives inside a corridor. Can ship any time after P1.

---

## 12. OWNER QUESTIONS

1. **Is a 10 m offline pack worth shipping on its own?** It looks the same as today's online satellite. Or should
   the feature wait until the 1 m lidar hillshade (P4) is ready?
2. **Buffer default**: 100, **250** (suggested) or 500 m each side? And should the user be able to choose?
3. **Sources to accept**: WorldCover/Sentinel (T1), lidar hillshade (T1.5), and municipal OGL (T2), yes or no
   for each? Is a relief image (not a photo) acceptable as the "high-res" layer?
4. **Ask the Province?** Should we file the Copyright Permission Request for the provincial ortho mosaic (old,
   1995-2004), and/or ask GeoBC about licensing **recent** orthos? Is there any **budget** for imagery licences or
   per-sheet fees ($100-200 per 1:20k sheet)?
5. **Server side**: is Option D OK? That means province-wide layer archives on R2, extracted on the phone, at
   roughly 75 GB and $1-2/month, plus a one-off hillshade compute. Or would you prefer per-corridor pre-cut files
   (C), or an on-demand cutting worker (E)?
6. **Caps**: 300 MB per pack and 1 GB total by default. OK?
7. **Web**: online-only corridor view (suggested), or real offline download on web too?
8. **Whole-stream packs on long rivers** (for example a 300 km mainstem): allow them at reduced maxzoom, or
   require a per-section download?
