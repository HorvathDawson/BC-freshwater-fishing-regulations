# T734 — The app closed when a water was opened before its map was ready

Status: IN PROGRESS (before closed testing; code + tests done; left (Claude): the emulator loops on the fixed build, build 32)

Source: build 32's verify lane, 2026-11-14 (HEAD 59aaa4de8). The app closed twice with
`java.lang.NullPointerException at org.maplibre.reactnative.components.mapview.MLRNMapView.project(MLRNMapView.kt:1653)`
via `MLRNMapViewModule.project`: once in the test bed's `pane-open` hold (search → Open on the Fraser, 6th open), once by
hand seconds after a cold start (search "Chilliwack River" → preview → Open). Logs:
`~/bcfish-session-backup-2026-11-12/build-32/crash-1-pane-open-hold-logcat.txt`, `crash-2-chilliwack-open-logcat.txt`.
The same NPE is in `app/tools/testbed/out/runs/t512-lean-a` (2026-11-08): the race is older than today's tickets.

## Cause (read in the code)
- Line 1653 of the installed (patched) `MLRNMapView.kt` is `project`: `mapLibreMap!!.projection.toScreenLocation(…)`.
  `mapLibreMap` is null until MapLibre calls `onMapReady` (it is never set back to null). The module runs the body on
  the UI thread through `ReactTagResolver.withViewResolved`, which caught only `IllegalViewOperationException`: any other
  throw (this NPE) is uncaught on the main thread and closes the app. The JS `.then(…, () => null)` never sees it.
- The app's only `project` / `unproject` calls are `shiftMarks` (`packages/map/src/Map.native.tsx`, T317: a boundary
  mark's label kept inside the map). It ran on every change of `marks` and every settle, gated only on the map's width
  and `mapRef` being set — not on the map existing. A water's freshly mounted map (search → Open builds one; T717 keeps
  the browse map mounted under the page, so the water's own map is the new one) can have its width and its marks before
  MapLibre has handed it its map.
- Every other JS-callable method with `mapLibreMap!!` (`getCenter`, `getZoom`, `getBearing`, `getPitch`, `getBounds`,
  `getViewState`, `queryRenderedFeaturesWith{Point,Rect}`, `showAttribution`, `takeSnap`) and the sources' own `!!`
  (`MLRNGeoJSONSource`, `MLRNSource.mMap!!`) run through the same resolver. What the app calls from JS on the phone:
  `project` / `unproject` (shiftMarks), `queryRenderedFeatures` (taps, the visible set, the probe; all after the first
  draw, T418/N2), the sources' `setFeatureState` / `removeFeatureState` / `setFeatureStates` / `watchTiles` (gated on
  `styled && drawn` already). `ui-native` calls none directly.

## What changed
1. **Native** (the binding's pnpm patch, `app/patches/@maplibre__maplibre-react-native@11.5.0.patch`):
   `ReactTagResolver.runOrReject` — a module method's body that throws REJECTS its promise (all module methods of the
   map view, camera, vector and GeoJSON sources); `readyMap(call)` makes `project` / `unproject` /
   `queryRenderedFeaturesWith{Point,Rect}` say "<call>: the map is not ready". Lean and upstreamable. Noted in
   `app/deps.md` and `app/tools/maplibre-patch/README.md`.
2. **JS** (`packages/map/src/native/labelShift.ts`, used by `shiftMarks`): no call is issued unless the map is mounted,
   styled and has drawn once (checked before each call); the shift runs again when the map first draws. A skipped call
   is not reported; a call that rejects goes to the map's `onError` (`reportFailure`), not `() => null`.
   The web half (`web/markers.ts`) needs nothing: maplibre-gl's `project` is synchronous on a map that exists once
   the component has it (rule 34a pair checked).
3. **Test bed**: `open-race` (cold start → search → Open at once, and warm opens, Chilliwack + Fraser, crash buffer and
   pid read every cycle); `lib/maestro.mjs` runs a flow again once when Maestro's own driver did not start (the hold ×40
   failure on build 32 was "Maestro Android driver did not start up in time", not the app).

## Tests
- `packages/map/src/native/labelShift.test.ts` (not ready → no call; no map → no call; unmounted between two calls →
  the second not sent; a rejection reported). Mutations: each of the three guards removed fails a test.
- `adapter-contract.render.test.tsx` "(T734)": the native Map styled but not drawn sends no `project` / `unproject`,
  and does once drawn (mutation: drop `rendered` from the gate, or `drawn` from the effect: fails); unmounted, a late
  settle sends nothing (the binding's ref is null then too, so this one does not isolate the mount flag; the unit test does).
- `lib/maestro.test.mjs`.

## Acceptance
- On the fixed build: `open-race` ≥ 60 cycles cold + warm and the `pane-open` hold × 40, no FATAL EXCEPTION in logcat.
- `regs-open`, `tap-stalls`, `pane-open`, `back-flash` pass on build 32.
