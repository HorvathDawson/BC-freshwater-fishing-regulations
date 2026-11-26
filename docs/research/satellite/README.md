# Satellite imagery: what is built, what the research found

The app's Satellite basemap is **Esri World Imagery, online-only**. Offline corridor imagery (open tickets T1108 and
T79) is not built; this page holds the facts that decide it.

## What is built

- **Source:** `app/packages/map/style/layers.source.json`, source `imagery`: Esri's World_Imagery MapServer,
  `https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}?blankTile=false`.
  256 px tiles, source maxzoom 19, camera stops at z18. `blankTile=false` makes a place with no imagery answer 404 so
  MapLibre overzooms the parent tile (otherwise Esri serves a grey "not yet available" tile).
- **No key by default** (owner: the original app used Esri keyless, "this is a non profit app").
  `EXPO_PUBLIC_ARCGIS_API_KEY`, when set, switches the tiles to `ibasemaps-api.arcgis.com` with a token
  (`app/apps/mobile/src/config.ts` `IMAGERY_TILES`, `imageryTiles` in `@app/map`); that endpoint was never exercised.
  Esri's licensed route is the ArcGIS Location Platform (2M basemap tiles a month free, then $0.15 per 1,000; sessions
  $4 per 1,000 after 1K free); calling the token-free URL from a non-Esri app is not a licensed path by their terms.
- **One toggle, one colouring path:** Layers sheet Basemap, `mapModes` (`basemap`, `imageryUp`; pinned in
  `mapModes.test.ts`), every map's `basemap` prop, `applyLook` / `setBasemap`. Regulation colours draw over imagery as
  over the drawn map.
- **Online-only** (`app/packages/core/src/imagery.ts`, `app/packages/ui/src/imagery.ts`, `useImageryReach`): satellite
  shows only while reachable; any one signal failing counts as unreachable: web `navigator.onLine` events
  (`connectivity.web.ts`), a failed web imagery tile (`MapProps.onImageryError`), a z0 tile probe (every 30 s while
  unreachable, every 120 s while reachable on a phone), and on native `@react-native-community/netinfo`
  (`netInfoConnectivity.ts`; the probe is the fallback where its native module is not linked). The reader's choice
  stays Satellite; Layers says "Satellite needs a connection. Showing the map until you are back online."
- **Attribution switches with the ground:** "Powered by Esri" and the service's `copyrightText` ("Esri, Vantor,
  Earthstar Geographics, and the GIS User Community") show only while imagery is drawn (the map control, `nativeStyle`,
  `withImageryCredit`); both copies of the words are held equal by `app/tools/imagery.test.ts`.
- **Offline is not allowed:** the item says the layer "is not intended to be used to export tiles for offline"; Esri's
  agreement allows offline only through Esri Content Packages in Esri software; E300 bans exporting tile volumes with
  session tokens. Nothing is cached or packaged (`no-store`, external source).
- **Checked on the web** with `esri/satellite_check.py` (results in `esri/results.json`): imagery at z12, 16 and 18
  under the Vedder, the offline path (drawn map, no Esri credit) and the return online. **Not run on a device:**
  MapLibre Native's handling of a 404 raster tile (parent overzoom or a hole) is unmeasured.

## Resolution over rural BC

No free, open, province-wide source is sharper than 10 m. Esri World Imagery measured at four rural points: Vantor
Vivid 0.31 to 0.5 m, 2022 to 2025, max level 17 or 18; below z12 it is 15 m TerraColor. The earlier ground, EOX
Sentinel-2 cloudless 2016, is 10 m (true limit z13; CC BY 4.0; the 2018 to 2025 years are CC BY-NC-SA, and the 2025
"Viewing Ready" product is sold at EUR 6,000 for self-hosting or offline use).

Size of a province-wide raster pmtiles: z13 about 115 k tiles (2 to 4 GB), z14 about 9 GB, z16 about 150 GB, z18
about 2 TB. Sub-metre offline is possible only for a small area the user picks.

## Other providers (none built)

| Source | Detail over rural BC | Offline / redistribution | Cost |
|---|---|---|---|
| Mapbox Satellite | 1 to 2 m to z16, 0.3 to 0.6 m regional z18, mostly cities above z16 | Mapbox SDK offline regions only; no bundling | 750 k tiles a month free, then $0.25 per 1,000 |
| Stadia Alidade Satellite | 1.5 m or better; 50 cm in North America, rural BC likely 1 to 2 m | only vendor whose ToS allows a mobile offline cache (100 MB per device) | $80 a month (Standard) |
| MapTiler | 1 to 2 m | temporary personal cache only; free plan non-commercial | Flex $30 a month |
| Google 2D tiles | sub-metre | banned | 100 k a month free, then $0.60 per 1,000 |
| Azure Maps | sub-metre | latency cache only | 1,000 transactions free, then contact sales |
| Copernicus Sentinel-2 (self-built) | 10 m | yes (open; "Contains modified Copernicus Sentinel data") | free plus a heavy compute job |
| ESA WorldCover 2021 S2 composite | 10 m, 2021, not colour balanced | yes (CC BY 4.0, public S3 `esa-worldcover-s2`) | free |
| BC Imagery WMS (openmaps.gov.bc.ca) | 1 m mosaic 1995 to 2004 (grey and colour), some 0.1 to 0.5 m regional | "Access Only": no cascading or copying without the Province's written permission | free |
| NRCan SPOT 2005 to 2010 | 10 m pan, 20 m colour | yes (OGL-Canada), legacy | free |
| BC municipal orthophotos (North Cowichan, Surrey; others unverified) | 7.5 to 15 cm | yes where the OGL is confirmed | free; inside those boundaries only |
| Planet | about 4.8 m | contract | about $20,000 a year (reseller) |

NAIP and USGS have no BC coverage. Bing basemaps are retired for new keys. OpenAerialMap is a handful of drone patches.
The OSM editor's Esri, Bing and Mapbox layers carry an OSM-tracing permission only.

## Conclusions for offline corridor imagery (T1108, T79)

- There is no free, current, sub-metre photo of rural BC rivers that may be stored on a phone or served from R2. A
  corridor pack from an open source (Sentinel-2, WorldCover) only makes the 10 m look work offline and adds no detail.
- **Lidar bare-earth hillshade** (BC LiDAR OGL-BC, NRCan HRDEM OGL-Canada; 1 to 2 m, only where flown, coverage fraction
  unverified) is the only open view of rural channels, side channels and old meanders. It is relief, not a photo.
- **The one path to real photos** is the Province: file the BC Copyright Permission Request Form or ask GeoBC for
  corridor tiles (1 km or less around regulated water) from the ortho mosaic. The mosaic is 20 to 30 years old, so recent
  per-sheet orthos would be the prize, possibly with a fee.
- The earlier design: one corridor per water (a buffer of the stream line, about 1 km), packs served from R2 like the
  data packs, a download button on the water, a storage cap and cleanup on the device, attribution switching with the
  visible source, imagery under the regulation colours. The camera's maxZoom can exceed a source's true maxzoom
  (MapLibre enlarges the last real zoom), so overzoom needs no new data.
- Mapbox's SDK is the one vendor path to offline satellite on their terms; it brings a second map engine or a
  rewrite, SDK telemetry (privacy answers), an app token, and breaks the self-host rule (T1108 asks these questions).
- Unread or unverified: Mapbox Product Terms and Esri E300 (PDFs); Azure's current caching text; Stadia's 2026-11-09
  terms revision; whether EOX's free WMTS may be harvested; Stadia and MapTiler BC coverage.
