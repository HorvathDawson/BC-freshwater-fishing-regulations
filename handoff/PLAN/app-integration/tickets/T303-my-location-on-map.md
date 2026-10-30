# T303 — Show your current location on the map

Status: DONE on web 2026-10-30 (commit 1b74740); the emulator pass (mock location + T300's map-gesture item 5) is pending the heavy lock (held by t301-build). Owner 2026-10-30: "add location onto map. So it shows your current location".

## Work
1. A "my location" button with the map's zoom controls; tapping it asks for location the first time (never silently —
   same rule as locate.native.ts / locate.web.ts, foreground only), then shows the standard blue dot (with accuracy circle)
   and centres the map on you. Tapping again follows / stops following. Native: MapLibre's user-location layer; web: the
   browser geolocation with a drawn dot.
2. Denied or location off → a short line saying how to turn it on; the button stays usable to retry.
3. Location never leaves the device (privacy page already says on-device only; check the wording covers the map dot).
4. Walkthrough (T293): one optional hint pointing at the button on first use.

## Acceptance
Emulator (mock location) + web screenshots: dot shown, centre, follow, denied state.

Depends: none. Size: S. Owner: no.

## Done
- Button: `MapRail` (the rail under MapLibre's zoom on the web, at the top on a phone), icon `locate`, named by state (`MY_LOCATION`: My location / Follow my location / Stop following my location). One state machine for both platforms, `useMyLocation` (`@app/ui`): tap 1 asks (`locate.here()`, never before) and shows the dot, centred (z15 at least); tap 2 follows; tap 3 stops. A hand drag stops following; the dot stays.
- Web: `packages/map/src/web/useMe.ts`: a DOM marker (blue dot, white ring) with an accuracy circle sized from metres at the zoom; follow = browser `watchPosition` (`Locate.watch`, new, optional). Phone (d51ca02c8, 2026-10-31): our own dot + accuracy circle drawn from `me.fix` (a GeoJSON source, two circle layers, `color.ui.me` tokens); follow eases the camera to each new fix (expo-location `watchPositionAsync`, foreground only, paused in the background); a hand pan stops following (`onRegionWillChange` userInteraction). MapLibre's `UserLocation` puck and `Camera trackUserLocation` are gone — they drew nothing on Android (MLRN 11.5.0). `Locate.here()` now also returns `accuracy`.
- Denied / no fix: a line under the button (denied: "Location is off for this app. Turn it on in your device settings, then tap again."; no fix: "...could not be found..."), tap it to dismiss; the button stays usable to retry. A missed reading while following is silent.
- Walkthrough: one optional hint at the button on the Map tab (`TAB_TOURS.locate`, target `locate`), once per device; not in More > Help's replay list.
- Privacy: the page already says the app uses location "to show where you are on the map ... used on your device and is not sent to us" (`tools/seo/html.ts`); no change.
- T275 (map rect) and T300's legend untouched; the rail only gained a button.
- Tests: `myLocation.render.test.tsx` (5), `MapRail.shell.test.tsx` (+1). Web shots (headless Chromium, mocked geolocation, phone 390x844 and desktop 1280x800): `T303-shots/` (`shots.mjs`): hint, button, tap 1 dot + centre + accuracy circle, tap 2 following (moved fix), tap 3 stopped, denied line.

## Emulator pass (pending)
One batch via `scripts/heavy.sh` when the lock is free: install the APK, set a mock location (`adb emu geo fix -121.95 49.1`), then check (1) tap 1 prompts and shows the blue dot + accuracy circle, centred; (2) tap 2 follows a moved fix; a pan stops it; (3) refuse the prompt: the line shows; (4) T300 item 5: one-finger pan and two-finger pinch on the water Map view and the Conditions route map.
