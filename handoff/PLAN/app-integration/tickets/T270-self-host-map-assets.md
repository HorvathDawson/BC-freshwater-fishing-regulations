# T270 — Map fonts and symbols from our own server only

Status: OPEN (owner 2026-10-29: "we need everything moved to our server").

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
