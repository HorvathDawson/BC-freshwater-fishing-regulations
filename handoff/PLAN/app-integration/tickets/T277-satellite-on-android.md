# T277 — Satellite imagery works in the Android app whenever online

Status: OPEN (owner 2026-10-29: "satellite should work in app if they have wifi").

## Work
Find why the Esri imagery layer doesn't show in the APK (style source on native, offline/bad-connection mode suppressing it,
cleartext/UA/referrer on native requests, layer toggle hidden on native). Make it draw whenever the device is online (wifi or
data), never stored (Esri terms, style.json comment), with Esri's attribution visible. Offline: the toggle says it needs a
connection rather than failing silently.

## Acceptance
Emulator screenshot with imagery on (online) and the offline message (airplane mode); web unchanged.

Depends: none. Size: S. Owner: no.
