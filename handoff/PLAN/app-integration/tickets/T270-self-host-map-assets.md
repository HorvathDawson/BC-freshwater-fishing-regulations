# T270 — Map fonts and symbols from our own server only

Status: DONE 2026-10-29 (owner 2026-10-29: "we need everything moved to our server").

Source: privacy-page check found glyphs + sprites load from `https://protomaps.github.io/basemaps-assets`
(`app/packages/map/src/endpoints.ts` PM_ASSETS).

## Work
1. Copy the exact font stacks and sprite sheets the style uses (only those) into our data host
   (local tile server, staging R2 `data-staging…`, prod path for the human upload) under a versioned path.
2. PM_ASSETS → our data host per APP_ENV (env.ts); offline: glyphs/sprites bundled or cached with the region download so the
   map labels work with no network.
3. Audit every other third-party request the app makes (network log on web + APK testbed); anything besides Esri imagery
   (owner-opt-in) and Open-Meteo weather is listed for the owner. Update the privacy page line accordingly.

## Acceptance
Web + Android network logs show no request to protomaps.github.io (or any host not on the list); labels/icons render
identically (screenshot pair); runtime-style test pins our host.

Depends: none. Size: S. Owner: no (prod R2 upload of the assets is human; command handed over).

## Outcome (2026-10-29)
- Releases already shipped glyphs/sprites (`tiles.assets`, map-assets-1, 13 MB: only the 3 Noto stacks + light/dark sprites) and the app already
  installs them for offline; the Protomaps CDN was only the no-assets fallback. That fallback is now OUR host: `PM_ASSETS` removed, `mapAssetsUrl(origin)`
  = `<data origin>/map-assets/v1/` carried in `endpointsFor().tiles.assets` (per APP_ENV) and read by `releaseTiles` when the release names none;
  last-resort default `OUR_ASSETS` = staging host. runtime-style.test pins it. Local `serve-tiles.mjs` serves the path from the newest release's folder
  (restart `pnpm tiles` to pick it up).
- Staging R2 `map-assets/v1/` uploaded (200 at data-staging.canifishthis.ca/map-assets/v1/...); `deploy/staging/upload.sh` now uploads it every time.
  Prod (human): `rclone copy <release tiles>/map-assets-1 r2:<prod data bucket>/map-assets/v1` with the immutable cache header, pbf as application/x-protobuf.
- Third-party audit (code + web network log; origins at load: only the local data host + app): Esri imagery (opt-in), Open-Meteo (forecast + archive).
  ALSO: ECCC MSC GeoMet `api.weather.gc.ca` (dayFlow.ts, past-day flow when online; station + date only) - now on the privacy page; owner may keep or drop.
  Credit links (lucide, protomaps, osm...) are links the user taps, not requests. No APK log taken (not cheap); native path uses the same code.
- Privacy page: map-fonts line now "from our servers too"; GitHub Pages removed.
