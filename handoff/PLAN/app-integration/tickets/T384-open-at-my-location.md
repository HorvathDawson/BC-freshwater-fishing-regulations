# T384 — The app opens on the reader's current location

Status: DONE (owner 2026-11-04: "also when we open the app default should be the persons current location", in 0.5.0) — a4873c54e.

## Done (a4873c54e)
- One path: `@app/ui` `useMyLocation` gained `open()` (the press's off → on, centred at `OPEN_ZOOM` 12 — a town; a press
  stays street scale 15); the map's `me.zoom` carries it (Map.native, web `useMe`). `useOpenAtMe` + `opensAtMe` decide:
  a plain open only (no start link route/arrive/regsStart, no held camera — a recreated activity keeps its place).
- Granted → centred at once; `prompt` → asked once, `openLocationAskStore` written "asked" before the ask (web key /
  phone file); asked before, denied, unavailable, no store → today's HOME, nothing read. Waits for the first-run notice.
- A refusal / no fix: the rail's existing line (`MY_LOCATION`); never reported (native refusal now `environmentError`).
- Web: the same path through `locate.web.ts` (browser geolocation). Tests: `OpenAtMe.shell.test.tsx` (granted, asked
  once, the default cases, refusal, a start link wins). Not done: the emulator `geo` check (no emulator this lane).

## Work
1. Read today's opening camera (Shell / MapScreen `initial`, a start link, the last camera kept?) and the location
   code (`useMe`, "At my location", the permission prompt on the tap).
2. On open with no start link: if location permission is already granted, centre on the reader (a sensible zoom);
   if not yet asked, ask once on the first open (or show the "My location" control's prompt) — never block the map; denied
   or no fix: the current default, and the reason said where it matters (T368: a refused permission is not reported).
3. A start link (a water, a place) still wins; web the same through the browser's geolocation.
4. Tests (render: granted → camera on the fix; denied → default; a link → the link), emulator check (`geo`).
Size: S–M.
