# Satellite = Esri World Imagery, online-only (built 2026-10-21)

Owner decision (`handoff/PLAN/app-integration/decisions.md`, "satellite imagery is ONLINE-ONLY, from Esri World
Imagery"). This replaces EOX's Sentinel-2 2016 10 m mosaic (maxzoom 13). The BC 1 m "Access Only" imagery is not
used anywhere.

## What changed

- **Source** (`app/packages/map/style/layers.source.json`, source `imagery`): this is Esri's ArcGIS Online
  World_Imagery MapServer, the same endpoint the original app used (commit 03c81f38, 2026-03):
  `https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}?blankTile=false`.
  - Tiles are 256 px and the source's maxzoom is 19. The camera stops at z18, and with 256 px tiles that means it
    requests z19 tiles (measured: requested zooms were 0, 10, 13, 15, 17 and 19).
  - `blankTile=false` makes a place with no imagery at a zoom answer 404, so MapLibre overzooms the parent tile. Without
    it, Esri serves its grey "Map data not yet available" tile (2,521 bytes). Probes: the far north (59.5 N) has a hole
    at z18, and north-central BC (56.26 N) has one at z19.
- **No key by default — the owner's decision** (2026-10-21 late, decisions.md "layers menu, water map, imagery": "we used
  esri free without a key in original app lets do that again. this is a non profit app"). The app ships the keyless
  `server.arcgisonline.com` URL above, as the original app did.
- **Key, as OPTIONAL config** (`app/apps/mobile/src/config.ts` → `IMAGERY_TILES`): `EXPO_PUBLIC_ARCGIS_API_KEY`, when set,
  switches the tiles to `https://ibasemaps-api.arcgis.com/arcgis/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}?blankTile=false&token=<key>`
  (`imageryTiles` in `@app/map`). No key appears anywhere in the code.
- **One toggle, one colouring pathway:** the Layers sheet's Basemap → `mapModes` (which now takes `basemap` and
  `imageryUp`, and returns the ground actually drawn plus `basemapNote`) → every map's `basemap` prop →
  `applyLook` / `setBasemap`. `mapModes` changes no mode and no group on satellite or offline (pinned in
  `mapModes.test.ts`). The screenshots show regulation colours drawn over the imagery exactly as over the drawn map.
- **Online-only** (`@app/core` `imagery.ts`, `@app/ui` `useImageryReach`): satellite shows only while it can be reached.
  Three signals feed that, and any one of them failing counts as unreachable:
  - **Web:** `navigator.onLine` and the `online`/`offline` events (`ui-native/src/connectivity.web.ts`).
  - **Web map:** a failed imagery tile (`MapProps.onImageryError`). GL JS never reports a 404, and an imagery error goes
    there instead of `onError`.
  - **Both platforms:** a probe for one z0 tile when Satellite is chosen, repeated every 30 s while unreachable. On a
    phone it also repeats every 120 s while reachable.
  - **Native:** ~~NetInfo is NOT a dependency~~ — 2026-10-21 (late), owner: "Add network-status detection on phones
    (netinfo)". `@react-native-community/netinfo` 12.0.1 (Expo SDK 57's version) now feeds `connectivity.native.ts`
    (read in `netInfoConnectivity.ts`): offline and back online are instant. The probe stays as the fallback — on
    choosing satellite, on coming back, and every 120 s while up (the phone's map reports no failed tile) — and is the
    only signal where NetInfo's native module is not linked (a development client built before it; rebuild it).

  The reader's choice stays Satellite. Layers says: "Satellite needs a connection. Showing the map until you are back
  online; satellite returns by itself."
- **Attribution switches with the ground:**
  - The map's control lists `Powered by Esri · Source: Esri, Vantor, Earthstar Geographics, and the GIS User Community`
    only while the imagery layer is visible. GL JS lists only sources used by a visible layer (measured, below).
  - The phone's style drops the imagery source's attribution unless satellite is drawn (`nativeStyle`).
  - The app's credit list gets `IMAGERY_CREDIT` from the Shell only while satellite is drawn (`withImageryCredit`).
  - The EOX line is gone from `App.tsx`.

## Esri terms: what the owner must know

- **Credit wording:** "Powered by Esri" plus the service's `copyrightText`, which I read on 2026-10-21 from
  `services.arcgisonline.com/.../World_Imagery/MapServer?f=json`: "Source: Esri, Vantor, Earthstar Geographics, and
  the GIS User Community". It said "Maxar" until Vantor's rebrand; the item's `accessInformation` says the same. Both
  copies of the words are held equal by `tools/imagery.test.ts`.
- **Licence:** the Esri Master License Agreement and Product-Specific Terms (item 10df2279f9684e4a9f6a7f08febac2a9).
  The item says the layer "is not intended to be used to export tiles for offline", which is why nothing is cached or
  packaged (`no-store` probe, external source).
- **The keyless URL ships (owner's decision, 2026-10-21 late).** The facts stay recorded: the research (`../README.md`)
  found that calling the token-free `server.arcgisonline.com` tiles from a non-Esri app is not, by Esri's terms, a
  licensed path; Esri's licensed route is ArcGIS Location Platform. The owner weighed that and chose keyless, as the
  original app ran ("this is a non profit app"). The key stays optional config, for if the owner ever wants it:
  - Create an API key, restrict it by referrer and app, and set `EXPO_PUBLIC_ARCGIS_API_KEY` for builds.
  - Billing is 2M basemap tiles a month free, then $0.15 per 1,000; basemap sessions are an alternative.
- The keyed endpoint (`ibasemaps-api.arcgis.com`) was **not exercised** because no key is available here. It is
  Esri's documented Location Platform path for this MapServer, but `blankTile=false` on it is assumed, not measured.

## Evidence (web check: `satellite_check.py`, own ports 39317/39318, results in `results.json`)

| shot | what it shows |
|---|---|
| `vedder-satellite-z12.png` / `-z16.png` / `-z18.png` | Esri imagery under the Vedder River, regulation colour over it; attribution includes Esri |
| `vedder-offline-map.png` | browser offline (`navigator.onLine` false): drawn map, imagery layer `none`, no Esri credit |
| `vedder-offline-layers.png` | Layers: Satellite still chosen (`aria-checked=true`), the reason line under Basemap |
| `vedder-online-again.png` | back online: imagery `visible` again, Esri credit back, no reason line (no reload, no tap) |

There were 51 Esri requests, all 200, and no console errors.

## Not done / caveats

- **Native was not run on a device.** MapLibre Native's handling of a 404 raster tile (parent overzoom or a
  transparent hole) is unmeasured; the worst case is the drawn map showing through at z18 in rural BC. The probe path
  is covered in jsdom only.
- `EXPO_PUBLIC_ARCGIS_API_KEY` is optional; unset (the owner's choice) the keyless URL ships.
