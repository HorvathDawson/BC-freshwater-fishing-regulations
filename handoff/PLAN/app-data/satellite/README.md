# Satellite imagery for BC: what is sharper than EOX 2016, and what we may legally use

Research only, 2026-10-21. No app code, keys or config changed. Owner request: "the satellite imagery in the
app is very very low res. we should find one that can zoom more."

## Where we are today

- Source: EOxCloudless (Sentinel-2 cloudless) **2016**, 10 m/px, CC BY 4.0, served by EOX's free WMTS
  (`app/packages/map/style/layers.source.json`, source `imagery`, `maxzoom: 13`, `external: true`). The attribution
  is also hard-coded in `app/apps/mobile/src/App.tsx`.
- It is **online-only** even on native: the source is external, so an offline phone shows no imagery now.
- At BC latitudes (48–60° N) a z13 tile pixel is about 10–13 m, so z13 is the true limit of a 10 m source.
  Past z13 MapLibre can only enlarge the z13 pixels (that is the "overzoom" half of the request; see the end).
- **No free, open, province-wide source is sharper than 10 m.** Anything sharper over rural BC is
  commercial (Vantor/Maxar, Airbus) and every commercial ToS read below forbids bundling it into an app
  package. That limit drives the recommendation.

## Size check: why province-wide offline high-res is impossible whatever the licence

BC is about 945,000 km². At a mid-BC latitude (54° N) one web-mercator tile covers about:

| zoom | m/px (256 px tile) | tiles over BC | size at ~20 KB/tile |
|---|---|---|---|
| 13 | ~11 m | ~115 k | ~2–3 GB (with z0–12 added) |
| 14 | ~5.6 m | ~460 k | ~9 GB |
| 16 | ~1.4 m | ~7.3 M | ~150 GB |
| 18 | ~0.35 m | ~117 M | ~2 TB |

These are back-of-envelope figures (tile area × BC area; real JPEG/WebP sizes vary with terrain). Even a free
sub-metre source could only go offline for a small region the user picks, never as the full-BC pmtiles
the app ships for its own layers.

## Candidates

"Max zoom over BC" means the deepest zoom with real new detail, not a provider's upsampled tiles.

### 1. Esri World Imagery (ArcGIS Location Platform)
- **Resolution/zoom over BC: the best found, and checked directly.** Esri's imagery metadata service
  (`World_Imagery/MapServer/identify`), queried today at remote BC points:
  - 56.26 N −126.98 (north-central bush): Vantor Vivid, **0.31 m, 2023-05-25, max level 18**
  - 52.11 N −122.18 (Cariboo, rural): Vivid, **0.31 m, 2025-04-16, max level 18**
  - 59.5 N −129.0 (far north, Liard/Dease): Vivid, **0.31 m, 2025-07-10, max level 17**
  - 50.9 N −127.5 (north Vancouver Island coast): Vivid, **0.5 m, 2022-04-17, max level 18**
  - Below z12 the source is 15 m Earthstar TerraColor.
  Item description: "one meter or better satellite and aerial imagery for most of the world's landmass".
  Urban BC is z18–19+.
- **Licence:** Esri Master License Agreement plus Product-Specific Terms (E300, 2025-11-13).
  https://www.arcgis.com/home/item.html?id=10df2279f9684e4a9f6a7f08febac2a9
- **Cost:** Location Platform: "2M free then $0.15 per 1,000" basemap tiles a month (static basemap tiles
  count against the same allowance); sessions: 1K free, then $4 per 1,000. https://location.arcgis.com/pricing/
- **Key:** yes (ArcGIS access token or session token). Restrict it by referrer or app.
- **MapLibre:** officially supported. Esri publishes MapLibre guides for the Basemap Styles service
  (`arcgis/imagery`) and the Static Basemap Tiles service (512 px raster):
  https://developers.arcgis.com/maplibre-gl-js/ ,
  https://developers.arcgis.com/documentation/mapping-and-location-services/mapping/basemaps/introduction-static-basemap-tiles-service/
  Calling the old token-free `services.arcgisonline.com/.../World_Imagery/MapServer/tile` URL from a
  non-Esri app is not a licensed path, so use the Location Platform route.
- **Offline / bulk / redistribution: NO** outside Esri software.
  - World Imagery item: "This layer is not intended to be used to export tiles for offline. If you would
    like to export imagery for offline use in ArcGIS applications, you may use the World Imagery (for
    Export) layer."
  - Master agreement (as quoted in search results): basemaps go offline only "through Esri Content
    Packages ... for use with licensed ArcGIS Runtime applications and ArcGIS Desktop. Customer may not
    otherwise scrape, download, or store Data."
  - E300: "Programmatic use of session tokens (e.g., exporting volumes of basemap tiles) is not permitted."
  - ArcGIS Online help: systematically requesting ArcGIS tiles for offline use through other apps is
    prohibited.
- **Attribution:** "Powered by Esri" plus the item's credit line: "Esri, Vantor, Earthstar Geographics,
  and the GIS User Community" (the credit was "Maxar" until recently; take it from the service metadata).
- **Self-hosted pmtiles:** not permitted.

### 2. Mapbox Satellite
- **Resolution/zoom:** "global coverage to zoom 16 (1–2 m)", "regional coverage to zoom 18 (0.6–0.3 m)",
  and Canada is named among the high-res regions. z9–12 is mostly Maxar, z13–16 is Maxar Vivid, and z16+
  is Vexcel aerial over North American cities.
  https://docs.mapbox.com/data/tilesets/reference/mapbox-satellite/
- **Cost:** Raster Tiles API: 750,000 free a month, then $0.25 per 1,000; Static Tiles 200k free.
  https://www.mapbox.com/pricing
- **Key:** yes (public token).
- **MapLibre:** Mapbox documents it ("Use Mapbox APIs in MapLibre GL JS",
  https://docs.mapbox.com/help/dive-deeper/mapbox-in-maplibre/), and raster XYZ satellite works.
- **Offline:** only via the Mapbox Maps SDK offline regions (6,000-tile default ceiling per user,
  adjustable, billed as tile requests). A reseller copy of Mapbox tile terms allows on-device caching
  only when "each device fills its cache through direct requests", for a single user, with no
  redistribution or proxying (https://www.precisely.com/legal/licensing/mapbox-tiles-06-2021/). The
  current Product Terms are a PDF I could not extract, so read it before building on this. Bundling into
  our pmtiles: **no**.
- **Attribution:** © Mapbox, © Maxar (and the logo per the product terms).

### 3. MapTiler Satellite
- **Resolution:** Cloud "Satellite" is 1–2 m globally (Maxar partnership) with aerial photography in
  populated areas. BC-specific zoom limits are not published.
- **Cost:** Free plan is "limited to non-commercial use and research & development" (Cloud terms §1.2).
  Flex is $30/month (25k sessions, 500k requests). https://www.maptiler.com/cloud/pricing/
- **Offline / redistribution:** Cloud terms §5.7: results may sit in "a temporary personal cache (browser
  cache, mobile app cache, etc.)". §6.2: "prohibited to export map content for usage outside the
  Service". §6.3: "prohibited to batch or excessive bulk download of map tiles". §7.2 bans server-side
  caching. https://www.maptiler.com/cloud/terms/
- **Self-hosted datasets** (https://www.maptiler.com/data/pricing/): Low-Res 2016 (free, non-commercial,
  ≤100 MAU), Medium-Res ($2,500/yr, "internal app only (no B2B or B2C)", ≤500 MAU), High-Res (custom
  quote). None of these fits a public consumer app without a negotiated deal.
- **MapLibre:** yes (MapTiler maintains a MapLibre-based SDK). **Attribution:** MapTiler + data credits,
  shown "while the map is displayed" (§5.4).

### 4. Stadia Maps "Alidade Satellite"
- **Resolution:** "1.5m or better everywhere", 37M km² at 30 cm, 7M km² at 50 cm "concentrated across
  North America, Europe, and urban hubs". Global coverage reaches z18, and tiles are served to z20.
  Imagery is from Airbus/PlanetObserver: "© CNES, Distribution Airbus DS, © Airbus DS, © PlanetObserver
  (Contains Copernicus Data)". https://docs.stadiamaps.com/map-styles/alidade-satellite/ ,
  https://stadiamaps.com/learn/satellite-imagery-resolution/
- **Cost:** satellite is included from **Standard, $80/month** (7.5M credits); a 512 px satellite tile
  costs 4 credits, so about 1.9M satellite tiles a month. Free and Starter plans do not list it, and
  free is "non-commercial or evaluation" only. https://stadiamaps.com/pricing/
- **Offline:** the only vendor whose ToS explicitly allows an app offline cache: §8.4 "caching small
  amounts of data for offline use in a mobile application ... not to exceed 100MB cached at a time per
  device". §8.5 limits other client caching to the HTTP cache headers (7 days if none), and server-side
  caching is banned. §30.1 forbids "process or bulk download Imagery". https://stadiamaps.com/terms-of-service/
  100 MB is roughly 5–7k tiles, enough for one or two fishing areas at z16–17. Bundling into our pmtiles:
  **no**.
- **MapLibre:** first-class (Stadia's own examples use MapLibre). **Key:** API key or domain auth.
- **Caveat:** BC-specific coverage is not published. Spot-check a few rural BC areas before committing.

### 5. Google Maps Platform (Map Tiles API, 2D satellite)
- **Resolution:** very high (sub-metre almost everywhere in southern BC, z19–21 urban).
- **Cost:** "Map Tiles API: 2D Map Tiles": 100,000 free a month, then $0.60 per 1,000.
  https://developers.google.com/maps/billing-and-pricing/pricing
- **MapLibre:** technically usable. Third-party renderers are allowed if the Google logo stays separate
  and unobscured, and every client must first POST `createSession` with an API key and send the session
  token on each tile request. The tile URL is not a static XYZ template, so it needs a small runtime
  shim, and the key ships to the client.
- **Offline: NO.** Policies: "you must not pre-fetch, index, store, or cache any Content except under the
  limited conditions stated in the terms"; non-visualization uses are prohibited, including "Offline
  uses". The client must honour `Cache-Control`. https://developers.google.com/maps/documentation/tile/policies
- **Attribution:** Google Maps logo plus data attributions, logo unmodified. Usable online-only; offline
  and bundling are ruled out.

### 6. Bing Maps / Azure Maps imagery
- **Bing:** free (Basic) keys lost Web Control access after 2025-06-30, and enterprise accounts end
  2028-06-30. Bing is being retired in favour of Azure Maps, so it is not a candidate.
- **Azure Maps** (`microsoft.imagery`, roughly z1–19): Gen2 includes **1,000 free imagery transactions
  a month (1 transaction = 15 tile requests)**, and the price beyond that is "Contact Us".
  https://azure.microsoft.com/en-us/pricing/details/azure-maps/
- **Caching:** terms as quoted by Microsoft staff: "Customer may not cache or store information delivered
  by the Azure Maps API including ... map data tiles", except to reduce latency, and for no longer than
  the header validity or 6 months. Offline bundling: **no**.
- **MapLibre:** Azure's own web SDK is MapLibre-based, and the tile URL plus key works in plain MapLibre.
- Verdict: expensive at volume, opaque pricing, no offline.

### 7. EOxCloudless (Sentinel-2) newer years
- **Resolution:** still **10 m** (`gsd 10.0`, max_zoom 13 in WGS84 / 14 in Web Mercator in EOX's STAC).
  Newer years add currency and better cloud and colour work, not detail. It does not answer "zoom more".
- **Licence** (https://cloudless.eox.at/pricing , /documentation/license , /license-deed): 2016 is CC BY
  4.0. **2018–2025 is CC BY-NC-SA 4.0**, so commercial use needs the "EOX Commercial
  Attribution-RestrictedUse 1.2" licence, which "allows use but not direct sharing or redistribution".
- **Prices** from EOX's public STAC catalogue (`staging.stac-catalog.eox.at/eoxcloudless`, 2025 release,
  excl. VAT):
  - Viewing Basic: **€6,000**
  - Viewing Ready (pre-rendered, "for self-hosting or offline usage", GeoPackage/SQLite/GeoTIFF):
    **€6,000**
  - Exploitation Starter: €10,000
  - Exploitation Ready: €18,000
  - Regional subsets from €1,500 to €2,000 for some products
  - Earlier years "at a discounted rate"
  - Whether an in-app offline copy counts as "redistribution" under the RestrictedUse licence needs EOX's
    written answer (cloudless@eox.at).
- **2016 itself:** CC BY 4.0 allows redistribution, so a BC pmtiles cut of 2016 is licence-legal. Building
  it by harvesting ~150k tiles from EOX's free WMTS is not something EOX invites (bulk and MBTiles
  delivery is their paid product). Ask first, or build from Copernicus data instead (item 8).

### 8. Copernicus Sentinel-2 raw / Copernicus quarterly mosaics (open)
- **Resolution:** 10 m (B02/B03/B04); about 5-day revisit, 2015–present.
- **Licence:** Copernicus Sentinel data legal notice (Reg. (EU) 1159/2013): "free, full and open" access
  for "reproduction; distribution; communication to the public; adaptation, modification and combination".
  The only duty is the notice "Contains modified Copernicus Sentinel data [Year]".
  https://sentinel.esa.int/documents/247904/690755/Sentinel_Data_Legal_Notice
- **Offline / pmtiles / redistribution: YES.** This is the one open source with both an open licence and
  10 m province-wide coverage.
- **Routes:** Element84 Earth Search (AWS, `sentinel-2-c1-l2a` COGs, no key), or the Copernicus Data
  Space quarterly cloud-free mosaics (B02/B03/B04/B08, first-quartile composite; the public global view
  is resampled to 20 m, and full-res access is via STAC/OData/Sentinel Hub, so confirm 10 m before
  relying on it). https://dataspace.copernicus.eu/node/883
- **Cost:** free data. Compute for a BC summer composite is a heavy pipeline job (one at a time, through
  `handoff/tools/heavy.sh`).

### 9. BC Government imagery (DataBC / openmaps.gov.bc.ca)
- **What exists:** "British Columbia Imagery WMS" (`https://openmaps.gov.bc.ca/imagex/ecw_wms.dll`). Its
  capabilities, read today, show a **province-wide 1 m orthophoto mosaic `bc_bc_bc_xc1m_bcalb_1995_2004`**
  (sampled: greyscale, 1995–2004 vintage), many 0.1–0.5 m regional mosaics (Lower Mainland 1999,
  Okanagan Lake 2013, Whistler/Squamish/Sea-to-Sky 2009, fire areas 2011), SPOT/Landsat 15–30 m (2004–06),
  and hillshades. The `xc2m_20070115` layer is a scanned TRIM map, not imagery. The provincial 1:20k
  orthophoto programme is 1 m (some 0.5 m colour), selected areas only, sold at $200/sheet:
  https://www2.gov.bc.ca/gov/content/data/geographic-data-services/digital-imagery/orthophotos
- **Licence: "Access Only"** (BC Data Catalogue record `imagery-web-map-service`, licence id 22). This is
  not the Open Government Licence – BC. Gov.bc.ca: "the materials are 'access only' and reproduction is
  not permitted without written permission." The WMS AccessConstraints: "cannot be cascaded through
  another Web Map Service without the express written permission of the Province".
- **Offline / pmtiles: NO** without written permission. Online use from the client is "access", but the
  only usable layer is 20–30-year-old greyscale, a worse look than Sentinel colour for most users.
- Worth one email to GeoBC if a 1 m layer is ever wanted. Not recommended.

### 10. Canada (NRCan / GeoBase / CanVec)
- "Orthoimages of Canada, 2005-2010" (SPOT 4/5): **10 m pan / 20 m multispectral**, Open Government
  Licence – Canada, GeoTIFF on `ftp.maps.canada.ca`, labelled "legacy product ... no longer supported".
  Open and bundleable, but older and no sharper than Sentinel-2.
  https://open.canada.ca/data/en/dataset/d799c202-603d-4e5c-b1eb-d058803f80f9
- Landsat-7 orthoimages are 15/30 m. CBMT/CanVec are vector or road basemaps, not imagery
  (https://natural-resources.canada.ca/science-data/data-analysis/geospatial-data-tools-services/basemaps-canada).
  Nothing here beats item 8.

### 11. USGS / NAIP / Landsat
- NAIP (0.6–1 m, public domain) covers the **conterminous US only**, so it stops at the 49th parallel
  with no BC coverage.
- Landsat 8/9 (public domain) is 30 m multispectral and 15 m pan, which is coarser than Sentinel-2.
- USGS "Imagery Only" basemaps are US-only. Not useful for BC.

### 12. Planet
- PlanetScope basemaps are about 4.77 m (marketed as "4 m"), monthly or quarterly, commercial licence
  only. Reseller pricing seen: **$1,999/month or $19,999/year** (Upstream Lens, which is not Planet's list
  price).
- NICFI free basemaps covered the **tropics only**, never BC.
- Redistribution in an app is contract-specific. Sharper than Sentinel but far short of sub-metre, and
  expensive. Not recommended.

### 13. OSM-community imagery
- **OpenAerialMap:** CC BY 4.0 user uploads (https://openaerialmap.org/legal). Licence-friendly, but BC
  coverage is a handful of drone and UAV patches, not a basemap.
- The Esri, Bing and Mapbox imagery in the OSM editor carries an **OSM-tracing permission only**, which
  does not extend to display in third-party apps.
- BC municipal orthophotos (Vancouver, Kelowna, Surrey, Maple Ridge…) are often open-licensed and
  0.1–0.3 m, but urban only, which is the wrong place for a fishing app.

## Comparison

| Source | Best detail over rural BC | Licence | Free tier / cost | Key | MapLibre OK | Offline cache | Bundle in our pmtiles |
|---|---|---|---|---|---|---|---|
| **EOX 2016 (today)** | 10 m, z13 | CC BY 4.0 | free | no | yes | — | licence yes; harvesting the WMTS: ask EOX |
| EOX 2018–2025 | 10 m, z13–14 | CC BY-NC-SA / commercial | €6,000 (Viewing Ready 2025) | no | yes | with licence | needs EOX's written OK |
| **Copernicus Sentinel-2 (self-built)** | 10 m, z13 | open, attribution only | free + compute | no | yes | yes | **yes** |
| NRCan SPOT 2005–10 | 10/20 m | OGL-Canada | free | no | yes | yes | yes (worse than S2) |
| BC Imagery WMS 1 m | 1 m grey, 1995–2004 | Access Only | free | no | WMS | no | **no** (written permission) |
| **Esri World Imagery** | **0.3–0.5 m, z17–18 (measured)** | Esri MLA / E300 | 2M tiles/mo free, then $0.15/1k | yes | yes (official) | **no** | no |
| Mapbox Satellite | ~0.3–0.6 m regional, z18 | Mapbox ToS | 750k tiles/mo free, then $0.25/1k | yes | yes (documented) | Mapbox SDK only | no |
| Stadia Alidade Satellite | ≤1.5 m, 50 cm in N.A., z18 | Stadia ToS | $80/mo (Standard) | yes | yes | **≤100 MB/device** | no |
| MapTiler Satellite | 1–2 m+ | MapTiler ToS | free = non-commercial; Flex $30/mo | yes | yes | temp personal cache | no |
| Google 2D tiles | sub-metre | GMP ToS | 100k/mo free, then $0.60/1k | yes + session | via shim, logo rules | **no** | no |
| Azure Maps imagery | sub-metre, z19 | MS Product Terms | 1k tx (15k tiles)/mo, then contact | yes | yes | latency only | no |
| Planet basemaps | ~4.8 m | commercial | ~$20k/yr (reseller) | yes | yes | contract | contract |
| NAIP / USGS | none in BC | public domain | — | — | — | — | — |

## Recommendation

**(a) Best free/open option that is legal offline: Copernicus Sentinel-2, built ourselves.** Make a recent
(e.g. 2024–2025 summer) cloud-free BC mosaic from Sentinel-2 L2A, publish it as our own raster pmtiles
(z0–13, WebP), and credit "Contains modified Copernicus Sentinel data 2025".
- It is the only source that is open, province-wide, current, and allowed into an offline package.
- It gives the same 10 m detail as today, so it fixes currency, colour, cloud and offline, but **not
  "zoom more"**.
- At about 2–3 GB it should be an optional download, not part of the default bundle.
- A cheaper stop-gap is to ask EOX in writing whether a BC pmtiles cut of the CC BY 2016 layer is welcome.

**(b) Best quality: Esri World Imagery through ArcGIS Location Platform.**
- **Detail:** measured 0.31–0.5 m Vantor Vivid down to z17–18 even in the far north (2022–2025 dates).
- **Support:** MapLibre support is official.
- **Cost:** about 2M tiles a month free. At roughly 100 tiles per imagery session that covers about
  20k sessions a month at $0; beyond that, $0.15 per 1,000.
- **Catch:** online-only. Offline export is limited to Esri SDKs, and it needs a client-side key, which
  is a paid dependency once usage passes the free tier.
- **Runner-up: Stadia Alidade Satellite at $80/month.** Less proven over rural BC (spot-check first), but
  it is the only commercial ToS that allows a real offline cache (≤100 MB per device), which suits
  "download this lake". If the owner wants a per-water offline imagery feature, Stadia is the vendor.
- **Mapbox:** quality is close to Esri, with a 750k tiles/month free tier, but its offline story is tied
  to the Mapbox SDK.
- **Not recommended:** Google (offline banned, session shim, logo rules) and Azure (pricing opaque past
  1k transactions).

**(c) Hybrid (recommended shape): offline low-res base + online high-res detail.**
1. Base: our Sentinel-2 BC raster pmtiles (or the current EOX 2016 online source until it exists), with
   `maxzoom: 13`.
2. Detail: a second external raster source over the same layer slot, e.g. Esri `arcgis/imagery` static
   tiles with `minzoom: 12`, `maxzoom: 18`. It is used only when the device is online. Offline, its tiles
   fail quietly and the z13 base shows through, so no explicit online/offline switch is needed beyond not
   adding the detail source when offline (or letting it 404).
3. Overzoom (the second half of the owner's request): MapLibre already enlarges a raster source past its
   own `maxzoom`; the map's camera `maxZoom` is what stops the user. Raise the camera max (e.g. 18–20) and
   keep each source's true `maxzoom` (13 for 10 m, 18 for Esri) so nothing requests tiles that do not exist
   and the last real zoom keeps showing, just enlarged.
4. Attribution must switch with the visible source. Esri needs "Powered by Esri" and "Esri, Vantor,
   Earthstar Geographics, and the GIS User Community" while its tiles are on screen.

**Owner decisions needed**
1. Is a client-side API key and a usage-billed dependency (free up to a limit) acceptable for online
   imagery? If not, we stay at 10 m and only gain currency and offline.
2. Esri (best measured quality, free 2M tiles) or Stadia ($80/month, allows a 100 MB per-device offline
   cache)?
3. Is a ~2–3 GB optional offline Sentinel-2 BC download worth building, or should offline stay
   imagery-free?

## Caveats

- Mapbox's current Product Terms (PDF) and Esri's E300 PDF could not be text-extracted here. The clauses
  above come from Esri's item metadata, Esri help pages and search-result quotes of the master agreement.
  Read both PDFs before signing up.
- The Azure caching clause is quoted from a Microsoft staff Q&A answer (2020), not today's Product Terms.
- Stadia's ToS read is the 2026-03-18 version; a 2026-11-09 revision is published and was not read.
- Esri resolution was measured at only four points. Stadia and MapTiler BC coverage was not measured.
- Tile counts and GB figures are estimates.


## Independent verification (2026-10-21) — corrections win over the text above

The comparison is mostly accurate and its recommendation stands: no free open imagery of BC is sharper than 10 m, the offline base should be a 10 m Sentinel-2-class layer with source maxzoom 13 and a higher camera maxZoom for overzoom, and online-only sharp imagery should be layered on top.

**Confirmed against current official pages:**
- Esri: 2M free tiles, then $0.15/1k. The World Imagery item licence says it is 'not intended to be used to export tiles for offline', and offline export is only via the 'for Export' layer in ArcGIS apps.
- Mapbox: Raster Tiles 750k free, then $0.25/1k. Mapbox documents using its tiles in MapLibre.
- MapTiler: Flex is $30/mo, and the Free plan is non-commercial or R&D only.
- Stadia: Standard is $80/mo and includes satellite. The ToS allows ≤100 MB of offline cache per device.
- Google: 2D Map Tiles are 100k free, then $0.60/1k. Offline use is prohibited, and the logo rules are as stated.
- Azure: imagery is 1k free transactions of 15 tiles each, then contact sales.
- EOX: the 2016 layer is CC BY 4.0 and the 2018–2025 layers are CC BY-NC-SA. The 2025 Viewing Ready price is 6000 (currency not shown on the STAC record).
- NRCan's 2005–10 orthoimages are 10/20 m under OGL-Canada.
- The BC WMS is 'Access Only' and cannot be cascaded.
- Size estimates hold: z13 ≈ 11 m per pixel at 54°N, so it matches 10 m data. BC at z13 is ~114k tiles, roughly 2–4 GB. z16 at ~150 GB and z18 at ~2 TB are in the right range.

**Material corrections:**
- EOX paid licence: the 2025 Viewing Ready product is sold for 'self-hosting or offline usage', and the licence covers apps installed on users' devices. So bundling looks allowed with a paid licence, not 'needs written OK'. That makes buying EOX 2025 a real alternative to building our own Sentinel-2 mosaic.
- BC's 1 m mosaic also exists in colour. The licence conclusion is unchanged.
- Stadia and Mapbox detail over rural BC is likely 1–2 m (z16), not 0.3–0.6 m. That weakens Stadia as the runner-up and strengthens Esri, whose resolution was actually measured.
- Esri basemap sessions ($4 per 1k sessions, 1k free, unlimited tiles per 12 hours) are missing from the comparison and may be the cheaper model for deep zooming.
- Two missed ready-made open 10 m options: ESA WorldCover 2021 S2 composite (CC BY 4.0) and the Copernicus quarterly mosaics (possibly 20 m, licence unstated).

**Still unverified:**
- Mapbox Product Terms and Esri E300 PDFs (offline clauses).
- The current Azure Product Terms caching text.
- Stadia's ToS revision taking effect 2026-11-09.
- Whether EOX's free WMTS may be harvested.

- **EOX 2018–2025 bundled into our pmtiles 'needs EOX's written OK'; whether the commercial RestrictedUse licence allows an offline copy inside an app 'needs EOX's written answer'.** → EOX's own STAC record says the 2025 'Viewing Ready' product (price 6000, gsd 10 m) is delivered as GeoPackage, MapCache SQLite or GeoTIFF tiles 'for self-hosting or offline usage'. The EOX Commercial Attribution-RestrictedUse licence §1(i) defines permitted Use as including 'Your website or app integrating the Licensed Material' and software 'shipped by You for installation on hardware of Your customers/users'. What it forbids is Direct Redistribution 'as-is (without integration into Your Product)', and the licence binds one named entity. So a paid licence looks designed to cover tiles bundled inside an app. A confirming email is still prudent, because the licence page shows v1.1 while the docs cite v1.2. The STAC record gives no currency or VAT for the 6000 figure; 'EUR excl. VAT' is unverified. (https://staging.stac-catalog.eox.at/eoxcloudless/eoxcloudless2025/2025-eoxcloudless-viewing-ready/2025-eoxcloudless-viewing-ready.json ; https://cloudless.eox.at/license-legal ; https://cloudless.eox.at/documentation/license)
- **BC Imagery WMS 1 m is greyscale, 1995–2004.** → The WMS has both a greyscale layer (bc_bc_bc_xb1m_bcalb_1995_2003) and a COLOUR 1 m layer (bc_bc_bc_xc1m_bcalb_1995_2004). A GetMap sample over Kamloops returned an RGB image with real colour. It also carries a 2004–06 SPOT5/Landsat pan-sharpened 15 m colour mosaic. The licence conclusion is unchanged: AccessConstraints still says it 'cannot be cascaded through another Web Map Service without the express written permission of the Province', and provincial orthophotos are sold at $200 a sheet rather than released under OGL-BC. (https://openmaps.gov.bc.ca/imagex/ecw_wms.dll?service=WMS&request=GetCapabilities&version=1.3.0 (layers listed and both 1 m layers sampled) ; https://www2.gov.bc.ca/gov/content/data/geographic-data-services/digital-imagery/orthophotos)
- **Stadia Alidade Satellite: 50 cm in North America, so a candidate for sharp rural BC imagery at $80/mo.** → Stadia's resolution page says only that 50 cm is 'concentrated across North America, Europe, and urban hubs', with '1.5m or better everywhere else'. Its imagery is credited to CNES/Airbus/PlanetObserver, which suggests SPOT-class 1.5 m over much of rural BC. Satellite tiles cost 4 credits each, so Standard's 7.5M credits is about 1.9M satellite tiles a month; Free and Starter do not include satellite. The 100 MB per-device offline allowance is confirmed in ToS §8 (effective 2026-03-18), but a revised ToS takes effect 2026-11-09, which is weeks away. §30.1 also bans bulk download and processing of the imagery. (https://stadiamaps.com/learn/satellite-imagery-resolution/ ; https://docs.stadiamaps.com/map-styles/alidade-satellite/ ; https://stadiamaps.com/pricing/ ; https://stadiamaps.com/terms-of-service/)
- **Mapbox Satellite gives ~0.3–0.6 m at z18 regionally, with Canada named.** → Mapbox's own reference lists 'global coverage to zoom 16 (1–2 m resolution)' and 'regional coverage to zoom 18 (0.6–0.3 m)'. Canada is named only as having higher-resolution imagery 'in certain areas', and above z16 the sources are Vexcel aerials over North American cities. Rural BC should therefore be assumed to stop at z16 / 1–2 m unless sampled. The pricing (Raster Tiles 750k free, then $0.25/1k) is confirmed. (https://docs.mapbox.com/data/tilesets/reference/mapbox-satellite/ ; https://www.mapbox.com/pricing)
- **Esri pricing is 2M tiles/mo free, then $0.15/1k (about 20k imagery sessions).** → The tile price is correct, but it leaves out a second billing model. Since Oct 2025 ArcGIS Location Platform also sells basemap SESSIONS: '1K free then $4 per 1,000 sessions', each giving one user unlimited tiles for up to 12 hours. That is the better fit for users who pan and zoom deep imagery a lot, and the comparison never mentions it. Also, the Static Basemap Tiles service offers imagery only as 'arcgis/imagery-labels' (labels baked in). Plain imagery comes through the Basemap Styles service or the MapLibre plugin. (https://location.arcgis.com/pricing/ ; https://www.esri.com/arcgis-blog/products/platform/announcements/basemap-sessions ; https://developers.arcgis.com/documentation/mapping-and-location-services/mapping/basemaps/introduction-static-basemap-tiles-service/)
- **MapTiler: only a temporary personal cache is possible; nothing can be bundled.** → The Cloud terms are as stated: §5.7 allows a temporary personal cache for a single end user, §7.2 bans server-side caching and §6.3 bans bulk download. But MapTiler also sells satellite imagery for self-hosting (MapTiler Data on-prem). Standard costs $2,500/yr and includes Medium-Res satellite, but it is limited to one internal app, 500 MAU and no B2C use. B2C or High-Res needs a Custom contract. So bundling is possible 'by contract', the same as Planet, rather than impossible. (https://www.maptiler.com/cloud/terms/ ; https://www.maptiler.com/data/pricing/)
- **Azure's 'no caching' rule (for latency only) is quoted from a 2020 staff answer.** → The pricing is confirmed: imagery tiles 1,000 free transactions, 15 tiles per transaction, then 'Contact Us'. The caching wording found is still community Q&A quoting the Online Service Terms: caching is permitted to reduce latency for up to the header validity or 6 months, and caching to serve multiple users is barred. The current Product Terms were not located, so this remains unverified, as the comparison itself admitted. (https://azure.microsoft.com/en-us/pricing/details/azure-maps/ ; https://learn.microsoft.com/en-us/answers/questions/214472/azure-maps-caching-policy)

**Missed:** 1) ESA WorldCover 2021 Sentinel-2 RGBNIR yearly median composite. It is a ready-made, global, 10 m open composite (CC BY 4.0 per the WorldCover site and the AWS registry), stored as COGs in the public S3 bucket esa-worldcover-s2. Cutting a BC pmtiles from it avoids building our own Sentinel-2 mosaic pipeline. Its drawbacks are that it is from 2021 and is not colour-balanced for viewing. One catalogue record (FEDEO) lists its licence as 'other', so check the RGB product's licence specifically (https://registry.opendata.aws/esa-worldcover-vito-composites, https://worldcover2021.esa.int/download). 2) Copernicus Data Space quarterly cloudless mosaics now cover the globe from Q1 2020 onward. However, Sinergise's announcement says they were resampled to 20 m, and no explicit licence text was found, so they are a weaker basis than raw Sentinel-2 L2A (https://sinergise.com/en/news/copernicus-data-space-ecosystem-grows-new-data-collection-enter-sentinel-2-quarterly-cloudless). 3) BC WMS colour 1 m layer and SPOT5 15 m colour mosaic (see corrections). Neither is usable offline without GeoBC permission. LidarBC is OGL-BC but supplies lidar, not orthophotos. 4) Esri basemap sessions pricing. 5) MapTiler on-prem satellite licensing. 6) EOX Viewing Ready is explicitly an offline/self-hosting product. This makes a paid EOX 2025 licence the simplest legal 10 m offline layer, with no Sentinel-2 build: buying it versus building our own Sentinel-2 mosaic for 'free + compute' should be weighed. No Canadian federal open optical basemap sharper than 10 m exists. NRCan's 2005–10 SPOT orthoimages are confirmed as 10 m pan / 20 m MS under OGL-Canada and a legacy product. Quebec publishes 10 m Sentinel-2 mosaics under CC BY, but they cover Quebec only, and NRCan's 2024 RCM mosaic is 30 m radar, not optical. Overzoom note: EOX's WMTS advertises GoogleMapsCompatible to z21, so the online service upsamples on the server. For our pmtiles, setting source maxzoom 13 with a higher camera maxZoom is the right approach, as the comparison says.

**Owner decision recorded:** satellite is ONLINE-ONLY (decisions.md), so offline-caching terms no longer rule a provider out.
