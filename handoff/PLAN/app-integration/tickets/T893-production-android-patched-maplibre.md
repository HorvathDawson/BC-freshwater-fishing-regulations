# T893 — Production Android builds carry the patched MapLibre too

Status: IN PROGRESS (app chat, 2026-11-19 ~00:23, on app/integrate-082 for 0.8.2).

Owner 2026-11-19 ~00:21 (app chat), verbatim: "Can't we make it use the patched one?" — after the app chat noted that
`--production` stayed on the stock MapLibre SDK (app.config.js refused MLN_LOCAL_MAVEN in production; build-apk.sh only
defaulted the lean repo for non-production builds), so the production AAB would not carry the T420 lean feature-state
patch every Play internal build has (T512: the stock SDK cost the owner's Pixel ~6 fps; the 0.8.2 bench, 51–55 fps pinch,
was the patched SDK).

Done:
- deploy/staging/build-apk.sh: MLN_LOCAL_MAVEN defaults to cache/maplibre/fslean-m2 for EVERY build, --production too;
  the AAB build-id check (the patched AAR's libmaplibre.so) now runs for production as well; `--stock-maplibre="<reason>"`
  stays the only way to the stock SDK. The prod build needs cache/maplibre/fslean-m2 in the checkout it runs from (the
  main checkout has it; app/tools/maplibre-patch/build.sh makes it).
- app/apps/mobile/app.config.js: the production refusal of MLN_LOCAL_MAVEN removed (the map lab and R8 refusals stay).
- app/tools/r8-config.test.ts: a production config with MLN_LOCAL_MAVEN is made and names the local SDK version.
- iPhone: unchanged (the patch is Android's; iOS uses MapLibre's iOS SDK).
