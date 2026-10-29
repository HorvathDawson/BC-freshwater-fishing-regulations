# T277 — Satellite imagery works in the Android app whenever online

Status: DONE 2026-10-29 — COMMIT (not reproduced on the emulator; probe hardened + logged) (owner 2026-10-29: "satellite should work in app if they have wifi").

## Work
Find why the Esri imagery layer doesn't show in the APK (style source on native, offline/bad-connection mode suppressing it,
cleartext/UA/referrer on native requests, layer toggle hidden on native). Make it draw whenever the device is online (wifi or
data), never stored (Esri terms, style.json comment), with Esri's attribution visible. Offline: the toggle says it needs a
connection rather than failing silently.

## Acceptance
Emulator screenshot with imagery on (online) and the offline message (airplane mode); web unchanged.

Depends: none. Size: S. Owner: no.

## Outcome (2026-10-29)
NOT REPRODUCED. New test-bed scenario `run.mjs satellite` (Wi-Fi: Layers → Satellite → screenshot; airplane mode: the
sheet's words). Run on the emulator with (a) this branch's build and (b) the owner's own staging APK
(`app/dist/apk/latest.apk`, 4bb1d4582): both draw Esri's imagery on Wi-Fi, and offline the sheet says "Satellite needs a
connection. Showing the map until you are back online; satellite returns by itself." The style source, the native
request path (no key, no referrer needed) and the toggle are fine. On the owner's APK the "Powered by Esri" badge sat
under the legend — already fixed by T275 (c86b23d07).

What can still turn it off on a real phone is the one-tile PROBE, the phone's only signal (`useImageryReach`): a single
slow/dropped request (8 s timeout) said "down" for 30 s, or NetInfo saying `isInternetReachable === false`. Hardened:
`imageryProbe` asks a failed tile once more (1.5 s later) before saying down, and logs WHY it failed
(`[imagery] satellite tile probe failed twice: HTTP …/no answer in 8000 ms/…`, console.error → logcat and the Updates
log). Web: the same probe, one extra request only when one fails.

Owner: on the next APK, if satellite still says "needs a connection" on Wi-Fi, the Updates log line names the cause.
Shots: `../T277-shots/satellite-online-head.png`, `../T277-shots/satellite-online-owner-apk-4bb1d4582.png`,
`../T277-shots/layers-offline.png`.
