# Handoff: offline satellite imagery + a reversible Mapbox engine for Can I Fish This?

Starting point for an agent picking this up. Research done 2026-11-25. Prices and terms change, so re-check anything you act on.

Repo: https://github.com/canifishthis/canifishthis (the app is in `app/`). Read `AGENTS.md` (binding rules: A1–A8, W47 PR-only to `main`), `app/MAP.md`, `app/packages/map/README.md` and `app/deps.md` before touching code.

---

## 1. The goal

- High-res satellite imagery of **BC river corridors** (buffered river areas), **downloadable for offline use** in the app.
- Fewer than **10k users a month**. Free, open-source public app, so the cheapest licensing wins.
- Keep the current stack: OSM basemap as PMTiles plus our own river and regulation layers (`atlas.pmtiles`).
- Owner's direction (2026-11-25):
  - **Web:** stays on MapLibre + PMTiles.
  - **Native app:** may move to the Mapbox SDK, mainly for Mapbox Satellite offline.
  - Mapbox must only replace the basemap/imagery. Our PMTiles layers stay.
  - Switching back to MapLibre has to stay easy, in case Mapbox changes its terms.
  - Later, if GeoBC grants a licence, swap in provincial orthophotos.

## 2. The current stack (read from the repo; verify, the code wins)

- **Not Capacitor.** One Expo / React Native shell (`app/apps/mobile`) serves both native and web (react-native-web).
- **Map packages:**
  - `app/packages/map` is the only package that knows the map SDKs.
  - Web: `maplibre-gl` v5, pinned together with `pmtiles@4`.
  - Native: `@maplibre/maplibre-react-native` 11.5.0, **heavily patched** (`app/patches/@maplibre__maplibre-react-native@11.5.0.patch`, ~2,700 lines). Android uses a local MapLibre Native build (`app/tools/maplibre-android/`).
- **An abstraction already exists:**
  - `packages/map/src/adapters/contract.ts` has `MapAdapter` (shared logic), `MapRenderer` and `MapHandle` (thin platform halves).
  - `adapters/web.ts` and `adapters/native.ts` implement them.
  - `parity.test.ts` runs one op script against both.
  - Layer and package boundaries are enforced by `app/layers.json` and `tools/check-boundaries.mjs`.
  - `Map.native.tsx` (~1,160 lines) uses the MapLibre RN components (`Map`, `Camera`, `VectorSource`, `GeoJSONSource`, `Images`, `Layer`) directly. That's where engine-specific code lives.
- **Style:** generated from `style/layers.source.json` + `tokens.json` + `themes/` by `pnpm style:build`. The app addresses layer groups, never individual layers. On native, a theme or mode change is a whole-style swap.
- **Tiles:** `atlas.pmtiles` + `basemap.pmtiles`, read by byte range (`pmtiles://`). They come from R2 on the web and are installed to disk on a phone (`pmtiles://file://…`). Offline maps page: `core/src/offline.ts`, `data/src/release/offline.ts`. There's one province-wide file today, with no per-region split yet (T85/T202).
- **Satellite today:**
  - Esri World Imagery, **online only, never stored** (`core/src/imagery.ts`, `map/src/endpoints.ts`, `setBasemap("map"|"satellite")` on the adapter contract).
  - It uses the token-free `server.arcgisonline.com` path. The code comments note this isn't Esri's licensed path for non-Esri apps; the licensed route is ArcGIS Location Platform, optional via `EXPO_PUBLIC_ARCGIS_API_KEY`.
  - When satellite can't be reached, the app falls back to the drawn map with a message (`basemapInUse`, `SATELLITE_OFFLINE`).
- **Colouring engine:** relies on per-tile feature ids plus feature-state (`colouring.ts`, `native/useColouring.ts`). The native side depends on the patch.

## 3. Imagery market survey (what we found)

### Commercial basemaps

| Provider | Offline? | Cost at our scale | Notes |
|---|---|---|---|
| **Mapbox Satellite** | **Yes, via Mapbox native SDK offline tile regions** | **Likely $0**: offline downloads count toward Maps SDK MAU billing, and the free tier is 25,000 MAU/month | Older docs billed offline per tile; the current help docs say offline is included in MAU billing. **Confirm with Mapbox before relying on it.** Native SDK only (GL JS has no offline). 750 tile packs per device. Offline maps can't be redistributed; each device downloads from Mapbox. Reinstalling the app counts as a new MAU. Imagery quality in remote BC is unverified, so check sample rivers. Has a nonprofit program (applications reviewed May and November). |
| Esri World Imagery | No | ArcGIS Location Platform: 2M basemap tiles/month free, then $0.15 per 1,000 | Offline only through Esri Content Packages used with Esri software. Distributing apps with offline support means contacting Esri Sales. Gaia GPS lost Esri offline in Dec 2024. Esri has a Nonprofit Program. |
| Google | No | — | Terms forbid caching and offline use. |
| Stadia Alidade Satellite | No | Standard plan $80/month for 7.5M credits; satellite costs 4 credits per 512 px tile | No caching on satellite; bulk download prohibited. 1.5 m or better everywhere, 30 cm in places (Airbus). |
| MapTiler Cloud | No (only a temporary single-user cache) | Free plan is non-commercial; Flex ~$25–30/month | Bulk download and export need a written agreement. |
| MapTiler Data (self-host) | Yes | Free tier is non-commercial with max 100 MAU; Standard $2,500/year for max 500 MAU; B2C needs a custom contract | The Canada satellite you can download is only z0–13 (~20 m). |

### Open or cheap imagery we can tile ourselves into PMTiles

- **Copernicus Sentinel-2 (10 m):** Free for any use, including reproduction, distribution and modification. Credit: "Contains modified Copernicus Sentinel data [Year]". Shows large rivers; small streams don't show up.
- **EOX Sentinel-2 cloudless:** The 2016 version is CC BY 4.0; 2018–2025 are CC BY-NC-SA 4.0 (non-commercial plus share-alike). The commercial file is ~€6,000. If the app ever has ads or charges, the NC versions stop applying.
- **LidarBC:** Open (OGL-BC) 1 m lidar. A hillshade from it shows channels and bars very well. Coverage was 14% of BC in 2023, and a $38M province-wide acquisition program is underway.
- **GeoBase SPOT 2005–2010 (OGL-Canada):** 10 m and old. Low value.
- **Municipal orthophotos (open licences):** Vancouver, Surrey, Nanaimo, Metro Vancouver (Langley and Maple Ridge 2025). Urban only.
- **Drone imagery (CC BY 4.0):** Hakai Institute (coastal BC) and OpenAerialMap. Patchy.

### BC government orthophotos (GeoBC)

- 0.5 m and 1 m. **$200 per BCGS 1:20K map sheet** ($100 for a partial sheet). "Selected areas"; some areas have no coverage. 15,000+ orthophotos since 1995. Much of it is flown for forest inventory by timber supply area.
- **No published coverage percentage.** To measure it, overlay our corridors on the WIMSI / Orthophoto Viewer coverage KML.
- The images are copyrighted by the Province. **Commercial use or publication needs permission.** The openmaps WMS (`openmaps.gov.bc.ca/ecwp/ecw_wms.dll`) serves mosaics from 30 m down to 0.5 m, with no stated licence.
- **iMapBC's imagery basemap is Esri World Imagery, not provincial imagery.** The province can't licence it to us. Ask GeoBC only about *their* orthophotos and WMS mosaics.
- Contact: GeoBCinfo@gov.bc.ca. Pitch: a free public-good BC angling app wants a redistribution licence for its river corridors only.

### Buying archive imagery

- SkyFi 31–50 cm archive: ~$8/km², 5 km² minimum. Airbus 50 cm archive: from ~€3.80/km². SkyWatch 50 cm: from ~$10/km².
- Affordable for the top ~50–100 rivers (~$10–20k), not the whole province.
- The standard licences rarely allow public redistribution, so a **redistribution licence must be negotiated**.
- Planet nonprofit program: discounted PlanetScope, SkySat and basemaps. NICFI is tropics only.

### Self-hosting cost (rough estimate)

- ~25,000 km² of buffered corridors at z17 (~0.8 m/px at BC's latitude) is ~1–2M tiles, ~20–40 GB of WebP. Sentinel-2 only needs ≤z14, a few GB.
- Cloudflare R2: storage $0.015/GB-month, **egress free**, reads $0.36 per million after 10M free. About **$1–5/month** for 10k users.
- `pmtiles extract IN OUT --region=corridor.geojson` cuts per-corridor archives.
- MapLibre Native reads `pmtiles://file://` but has **no offline pack** support for PMTiles. The app downloads the file itself (the existing `modules/bg-download`).

## 4. Recommended direction

Two tracks. The first is useful whatever happens with Mapbox.

**Track A (engine-neutral, do first):**
1. Generalize imagery into a provider interface. Today it's hard-wired to Esri.
2. Add an offline-imagery service interface.
3. Ship our own corridor PMTiles: Sentinel-2 plus a LidarBC hillshade on R2, offline. Ask GeoBC for an orthophoto licence; if granted, the same pipeline tiles those.

**Track B (Mapbox native engine, reversible):**
1. Spike first.
2. Then split the native engine behind the existing contract with a build-time flag.
3. Keep MapLibre building and tested in CI so switching back is one flag.

### Suggested interfaces (sketch)

```ts
interface ImageryProvider {
  id: "esri" | "mapbox" | "own-pmtiles";
  engines: ("maplibre" | "mapbox")[];      // mapbox satellite only works on the Mapbox engine
  online: { tiles: string } | null;
  offline: "none" | "engine-pack" | "file";
  attribution: string;
}

interface OfflineImagery {                   // used by the Offline maps page; it never learns the method
  download(corridor: GeoJSON.Geometry, zooms: [number, number]): Progress;
  remove(id: string): Promise<void>;
  size(id: string): Promise<number>;
  list(): Promise<OfflineRegion[]>;
}
// implementations: engine-pack (Mapbox TileStore with corridor geometry) | file (corridor .pmtiles via bg-download)
```

- `basemapInUse` / `SATELLITE_OFFLINE` / `withImageryCredit` in `core/src/imagery.ts` should read from the provider instead of assuming Esri. The existing fallback to the drawn map covers a provider going away.

### Engine split (Track B)

- Split `Map.native.tsx` into an engine-neutral shell plus `native/maplibre/*` and `native/mapbox/*`. Each provides its own `MapRenderer`, `MapHandle` and component tree.
- `EXPO_PUBLIC_MAP_ENGINE` is read by an Expo config plugin and a Metro alias. **Only one native SDK is linked per build.** Don't ship both.
- Add a `layers.json` rule: only `native/mapbox/**` may import `@rnmapbox/maps`.
- `pnpm style:build` emits both a MapLibre and a Mapbox style from the same source. `style-valid` checks both specs.
- Extend `parity.test.ts` so the same op script runs against web, MapLibre-native and Mapbox-native.
- CI builds both native engines on every PR (`app-ci.yml` already splits `bundles`).
- Mapbox offline packs are marked removable. Switching back tells users their offline satellite is gone; it never shows a blank map.

### The PMTiles problem on Mapbox native

- No native PMTiles support was found in the Mapbox mobile SDK. (Mapbox **GL JS** v3.21+ supports PMTiles vector sources, but that's web only.)
- Preferred fix: **a local tile server inside the app** (a local Expo module beside `bg-download`). It serves the installed `atlas.pmtiles` / `basemap.pmtiles` as `http://127.0.0.1:<port>/{z}/{x}/{y}`, and both engines read the same URLs.
- Move MapLibre onto it first, with no change in behaviour. That de-risks the plan.
- Risks to measure: startup time, iOS app lifecycle and backgrounding, battery.
- Bonus: because the server has each tile in hand, it can **report feature ids per tile**, which replaces the patch's PMTiles id readers for either engine.

## 5. The MapLibre patch vs what Mapbox / rnmapbox already has

The size of the patch was the main worry. Here's how much of it Mapbox may make unnecessary:

| What the patch adds | Mapbox / `@rnmapbox/maps` |
|---|---|
| Stretchable images (`stretchX`/`stretchY`, T74) | **Built in** (`stretchX`, `stretchY`, `content` on images). Can drop. |
| Feature-state, batched `setFeatureStates` | Single `setFeatureState` / `removeFeatureState` **built in**. **No batching.** Time a full province colouring pass. |
| Per-tile feature ids (`TileIds.kt`, `MLRNPmTilesIds.m`; MapLibre only reports tile *requests*) | Native SDK emits `SourceDataLoaded` with a `tileId` (tile **coordinate**, not the ids inside it). Ids still need reading (local tile server or our reader). rnmapbox may not pass this event to JS, so a small patch may be needed. |
| Fling and pinch-carry tuning (T312, T452, T707) | Different gesture engine. Unknown until tested on a device. |
| Still-tap `onTapUp` | Probably missing (rnmapbox has `onPress` / `onLongPress`). Small patch or workaround. |
| Style-swap feature-state bug (new layer drawn from old bucket) | Likely MapLibre-specific. Re-test theme and mode swaps. |
| Safe calls before ready, camera cache | Small. Re-check. |

Rough conclusion: about a third of the patch would still be needed (id reading, batching, tap-up, maybe gestures), not all of it.

## 6. Suggested order of work

1. **rnmapbox spike** (go/no-go), in a throwaway branch. Test:
   - feature-state at full scale (timed colouring pass)
   - `SourceDataLoaded` tile ids reaching JS
   - tap-up
   - pan and pinch feel
   - theme/style swap with feature-state
   - one corridor offline tile region
   - Mapbox Satellite quality on a few remote BC rivers
   - whether our river layer renders from local tiles
2. `ImageryProvider` + `OfflineImagery` interfaces, with Esri (online) and the file method only.
3. Local PMTiles tile server; move MapLibre onto it.
4. Corridor pipeline: buffer the river layer, then `pmtiles extract` Sentinel-2 / LidarBC hillshade per corridor, then R2.
5. If the spike passed: engine split, build flag, boundary rule, dual style emit, parity and CI for both engines.
6. In parallel: email GeoBC about orthophoto redistribution for corridors; apply to Mapbox's nonprofit program (May/Nov cycle).

## 7. Downsides of the hybrid plan (for the decision record)

- Two map stacks: features and bugs are duplicated, and the styles drift apart.
- Native glue code; harder to test in a browser.
- Lock-in on the most important feature (offline imagery) to Mapbox's terms. Mapbox has changed offline billing before, and Esri's 2024 change killed Gaia's offline imagery.
- No shared or pre-packaged downloads. MAU inflates on reinstall.
- Mapbox SDK token in an open-source app: needs URL or app restrictions, and contributors need their own token. Telemetry is on by default.
- Web users get no offline satellite. Large per-river downloads on rural data plans.
- The alternative that avoids all of this: MapLibre everywhere, with our own corridor PMTiles (open imagery now, GeoBC or purchased imagery later).

## 8. Open questions

- Confirm with Mapbox (support or sales) that offline tile-region downloads are covered by Maps SDK MAU pricing in v11, and what "redistribution" means for an open-source app.
- Mapbox Satellite resolution and date on remote BC rivers.
- Total buffered corridor area (km²) and the buffer width wanted. These drive every storage and price estimate here.
- GeoBC's answer on redistribution and any fee.
- Whether the app will ever be "commercial" (ads, donations-for-features): this affects EOX 2018+ and MapTiler's free tiers.
- Keep using token-free Esri for online satellite, or move to ArcGIS Location Platform (the licensed route)?

## 9. Sources

- Mapbox offline + MAU: https://docs.mapbox.com/help/dive-deeper/mobile-offline/ · pricing: https://www.mapbox.com/pricing · legacy per-tile offline billing: https://docs.mapbox.com/android/legacy/maps/guides/pricing/ · offline concepts / 750 packs / no redistribution: https://docs.mapbox.com/android/maps/guides/offline/concepts/ · nonprofit: https://www.mapbox.com/nonprofit
- `SourceDataLoaded` tileId: https://docs.mapbox.com/android/maps/api/11.26.0/mapbox-maps-android/com.mapbox.maps/-source-data-loaded/ · Mapbox GL JS PMTiles: https://docs.mapbox.com/mapbox-gl-js/example/pmtiles-vector-source/
- rnmapbox Image stretch: https://rnmapbox.github.io/docs/components/Image · Feature state example: https://rnmapbox.github.io/docs/examples/V10/FeatureState
- MapLibre Native PMTiles (no offline packs): https://maplibre.org/maplibre-native/android/examples/data/PMTiles/ · pmtiles extract: https://docs.protomaps.com/pmtiles/cli
- Esri offline terms summary: https://www.esri.com/content/dam/arcgisonline/docs/tou_summary.pdf · Gaia GPS change: https://blog.gaiagps.com/esriupdate · ArcGIS Location Platform: https://location.arcgis.com/pricing/ · Esri nonprofit: https://www.esri.com/en-us/industries/nonprofit/nonprofit-program/eligibility
- Stadia pricing: https://stadiamaps.com/products/maps/ · Stadia satellite no caching: https://stadiamaps.com/blog/announcing-cacheable-static-map-images/
- MapTiler Cloud terms: https://www.maptiler.com/terms/cloud/ · on-prem pricing: https://www.maptiler.com/data/pricing/ · Canada satellite download: https://www.maptiler.com/on-prem-datasets/dataset/satellite-2021/north-america/canada/
- Google tiles policy: https://developers.google.com/maps/documentation/tile/policies
- EOX licence: https://cloudless.eox.at/license-non-commercial · EOX 2024 release: https://eox.at/2025/03/sentinel-2-cloudless-2024/
- Copernicus legal notice: https://sentinel.esa.int/documents/247904/690755/Sentinel_Data_Legal_Notice
- GeoBC orthophotos: https://www2.gov.bc.ca/gov/content/data/geographic-data-services/digital-imagery/orthophotos · Orthophoto Viewer (copyright terms): https://www2.gov.bc.ca/gov/content/data/geographic-data-services/digital-imagery/orthophotos/orthophoto-viewer · WIMSI: https://a100.gov.bc.ca/pub/wimsi/ · BC imagery WMS: https://www2.gov.bc.ca/gov/content/data/geographic-data-services/web-based-mapping/map-services · iMapBC services (Esri imagery): https://delivery.maps.gov.bc.ca/ess/rest/sites/imap4m/map
- LidarBC: https://www2.gov.bc.ca/gov/content/data/geographic-data-services/topographic-data/lidarbc · province-wide lidar program: https://news.gov.bc.ca/releases/2023WLRS0054-001554
- GeoBase SPOT: https://open.canada.ca/data/en/dataset/d799c202-603d-4e5c-b1eb-d058803f80f9
- OpenAerialMap licence: https://openaerialmap.org/legal · Hakai drone index: https://catalogue.hakai.org/dataset/ca-cioos_cfa4ad65-ee12-47c0-9527-206a0f8d92ae
- SkyFi pricing: https://skyfi.com/files/SkyFi_Pricing_2025.pdf · archive price overview: https://geoawesome.com/demystifying-satellite-data-pricing-a-comprehensive-guide/ · Planet nonprofit: https://planet.com/nonprofit
- Cloudflare R2 pricing: https://developers.cloudflare.com/r2/pricing
