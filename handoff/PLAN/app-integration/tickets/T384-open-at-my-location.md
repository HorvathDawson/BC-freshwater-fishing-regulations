# T384 — The app opens on the reader's current location

Status: OPEN (owner 2026-11-04: "also when we open the app default should be the persons current location", in 0.5.0).

## Work
1. Read today's opening camera (Shell / MapScreen `initial`, a start link, the last camera kept?) and the location
   code (`useMe`, "At my location", the permission prompt on the tap).
2. On open with no start link: if location permission is already granted, centre on the reader (a sensible zoom);
   if not yet asked, ask once on the first open (or show the "My location" control's prompt) — never block the map; denied
   or no fix: the current default, and the reason said where it matters (T368: a refused permission is not reported).
3. A start link (a water, a place) still wins; web the same through the browser's geolocation.
4. Tests (render: granted → camera on the fix; denied → default; a link → the link), emulator check (`geo`).
Size: S–M.
