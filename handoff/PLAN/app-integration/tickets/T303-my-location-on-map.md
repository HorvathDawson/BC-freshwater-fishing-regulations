# T303 — Show your current location on the map

Status: OPEN (owner 2026-10-30: "add location onto map. So it shows your current location").

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
