# T448 — Native map: cap the drawing density like the web (pixel ratio ≤ 2)

Status: DONE — 26c59849f; owner 2026-11-09 on build 24: keep cap 2 ("I notice no different"); iOS half stays with T88

Was: PARTIAL — Android built and measured 2026-11-06 (26c59849f): the map's surface is 823×1567 instead of 1080×2057 (−42 % pixels), same scale, same tap. Left: the owner's phone check (the emulator cannot show fill savings), iOS (not done).

Source: T420 (2026-11-06, 2b891823f): the web map now draws at `min(devicePixelRatio, 2)` (`packages/map/src/web/
pixelRatio.ts`) — 28–32 % less frame time at z10–z12 on a Pixel 9 Chrome proxy (412×915 @ 2.625). MapLibre Native on
the same phone also draws at 2.625; the binding exposes no pixel ratio.

## Work
Expose `MapLibreMapOptions.pixelRatio` (Android) / the iOS equivalent in the binding patch, set the same rule in the
native half (`Map.native.tsx`; rule 34a: the native file, not a shared branch), measure on the stressed emulator (pan
scripts, T420 tables) and say what it costs in sharpness (shots at 2.625 vs 2).
Size: S–M.

## Done (2026-11-06, 26c59849f)
**Mechanism.** `MapLibreMapOptions.pixelRatio` alone is wrong (MapLibre Native android-v13.6.1 source): `NativeMapView.resizeView`
sizes the map as view px ÷ that ratio (ratio 2 on a 2.625 phone = a 540-dp map, everything 24 % smaller), and the renderer
reads its own ratio from the display density anyway (`MapRenderer` ctor). Instead the binding patch's new `maxPixelRatio` prop
(Android, `MLRNMapView.capSurface`) gives the map's SurfaceView a fixed buffer (`holder.setFixedSize(view × cap/density)`):
the map stays sized in dp from the VIEW (gestures, taps, projections all view px ÷ density: unchanged), and MapLibre draws
the same map into the smaller buffer (GL viewport = `onSurfaceChanged` size; Vulkan swapchain = the surface's currentExtent);
the hardware composer scales it up. `Map.native.tsx` passes `MAX_PIXEL_RATIO` (`pixel-ratio.ts`, the web's cap too). Texture
mode is left alone; iOS ignores the prop (MLNMapView's scale not checked, no iOS build).

**Proof (stressed emulator, n1_pixel 1080×2400 @ 2.625, Vulkan, `out/t448/sf-*.txt`).** SurfaceFlinger, the map layer: source
crop 1080×2057 → **823×1567** into the same display frame 1080×2057, composition DEVICE (no GPU pass to scale). Same-place
shots (`app/tools/testbed/out/t448/z7-*.png`, `z12-*.png`): mean pixel difference 1.1 (z7) / 0.5 (z12) of 255 over the map —
same framing, same scale bar; a tap at the same screen point opens the same water ("Unnamed stream · Region 2, Tributary of
Harrison River") in both. Sharpness (`z7-crop-base-vs-cap.png`, 4× zoom): slightly softer text and line edges, the closed
red/white hatch loses a little detail; renderer anti-aliasing stays at 2.625 (a touch narrower than a matching ratio).

**Frame times (same tree, cap 0 vs 2, one run each, `out/runs/t448-{base,cap}-{coq13,fv11}`)** — map surface, 16 flings/round:
| run | base frames · >50 ms · p50 / p95 ms · missing tiles | capped |
|---|---|---|
| Coquitlam z13 circling | 292 · 213 · 66.3 / 118.1 · 21.1 % | 362 · 139 · 41.4 / 111.5 · 11.1 % |
| Coquitlam z13 travelling | 387 · 271 · 63.2 / 112.7 · 18.4 % | 392 · 198 · 50.1 / 114.4 · 16.3 % |
| Fraser Valley z11 circling | 456 · 135 · 38.9 / 84.2 · 7.0 % | 423 · 167 · 46.6 / 100.0 · 12.5 % |
| Fraser Valley z11 travelling | 417 · 176 · 48.5 / 110.3 · 20.5 % | 402 · 218 · 50.4 / 117.4 · 19.6 % |
Mixed, within this emulator's run-to-run noise: its GPU is the Mac's passed through (fill is ~free there) and the stress is
CPU (tile parse, JS), which the cap does not touch. The fill saving (42 % fewer pixels per frame, as the web proxy's −28..−32 %)
can only show on the Pixel 9's own GPU: owner phone check.

## Build 22 check (2026-11-08, read only)
`MLN_LOCAL_MAVEN=$PWD/cache/maplibre/fslean-m2 deploy/staging/build-apk.sh --play` carries it: build-apk refuses a `--play`
build without the lean AAR (`android-sdk-vulkan-13.6.1-fslean.aar`, present); the binding patch
(`app/patches/@maplibre__maplibre-react-native@11.5.0.patch`) has `maxPixelRatio` + `prefetchZoomDelta`; `Map.native.tsx`
passes the cap 2 (lab default `pixelCap: true`) and prefetch −1 / lab 4 (MapLibre's default). Left: the owner's phone check
(OWNER-TODO "Phone check with build 22").
