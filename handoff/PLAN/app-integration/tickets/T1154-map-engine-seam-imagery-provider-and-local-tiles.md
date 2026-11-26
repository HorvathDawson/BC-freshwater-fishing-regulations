# T1154 — Under the hood: an engine seam for the phone map, an imagery provider, and one way to read our tiles (no change the user sees)

Status: OPEN — the first step of the satellite work (owner 2026-11-25); scoping then build; no behaviour change

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1154 | Under the hood: an engine seam for the phone map, an imagery provider, and one way to read our tiles (no change the user sees) | OPEN — the first step of the satellite work (owner 2026-11-25); scoping then build; no behaviour change | — | planning chat 2026-11-25: a read-only scoping pass (what in Map.native.tsx and the patch is engine-specific), then lanes; T1155 (the Mapbox trial) follows it |

Source: owner 2026-11-25 evening (planning chat), with a research handoff he pasted, kept whole at
`handoff/PLAN/app-integration/T1108-files/handoff-2026-11-25-offline-satellite-and-reversible-engine.md`. His words,
verbatim: "i feel like this is an important thing to nail down and the first step could be figuring out how we can
restructure things under the hood to make it so a engine switch like this would be easy. then we could have. the
next ticket after that be a testing it out for the apps." This ticket is that first step. It is useful whatever
happens with Mapbox (the handoff's "Track A" and the engine split of "Track B", without Mapbox itself).
Related: T1108 (the idea and four scoping passes), T1155 (the trial), T1148 (tile cache), T79 (imagery corridor),
T85 / T202 (per-region offline), `app/MAP.md`, `app/packages/map/README.md`, `app/deps.md`.

## Work (the handoff's sections 4 to 6 give the sketch; the code wins where they differ)
1. Read-only scoping first: a list of every place that knows the map engine (`Map.native.tsx`, `adapters/native.ts`,
   the 2,700-line MapLibre patch, the style build, `core/src/imagery.ts`, `map/src/endpoints.ts`, the offline maps
   page), each marked engine-neutral or MapLibre-only, and the order of the moves below with their risk.
2. Imagery as a provider: `basemapInUse`, `SATELLITE_OFFLINE` and the credit read from a provider record (id,
   engines it works on, online tiles, offline method, attribution) instead of assuming Esri. An offline-imagery
   service interface (download a corridor, remove, size, list) that the Offline maps page uses without knowing
   the method. Implementations now: Esri online, and "file" (nothing offered yet).
3. The native map split into an engine-neutral shell and `native/maplibre/*` behind the existing contract
   (`MapAdapter`, `MapRenderer`, `MapHandle`), chosen by one build-time setting; a boundary rule in
   `app/layers.json` so only the engine folder imports the engine's package; `parity.test.ts` runs its op script
   per engine. Only MapLibre exists after this ticket; a second engine is a new folder, not a rewrite.
4. One way to read our tiles on a phone that any engine can use: a local tile server module (beside
   `bg-download`) serving the installed `atlas.pmtiles` and `basemap.pmtiles` as `http://127.0.0.1:<port>/{z}/{x}/{y}`,
   with MapLibre moved onto it FIRST and no change in behaviour. Measure before and after on a mid phone: cold
   start, first paint, pan frame times, memory, battery over 10 minutes, iOS backgrounding. If it is slower by
   more than an agreed margin, stop and report (the `pmtiles://file://` path stays). Whether the server can report
   feature ids per tile (replacing the patch's id readers) is measured, not assumed.
5. The style build emits per engine from the one source (`pnpm style:build`); the style check validates each.

## Acceptance
The app looks and behaves the same (the existing device bench and the parity test pass; pictures before / after
at the same places); the numbers of step 4 are in the ticket; a grep-style boundary check fails when engine code
leaks out of its folder (mutation); the MapLibre patch is unchanged or smaller. Severity: enabling work.
Release: its own minor after the hardening release (planning chat's proposal). Model: Opus (the native map and the
patch). Size: L (three to four lanes after the scoping). Owner: the margin in step 4 and a look at the bench.

## Scoped 2026-11-25 night (read-only; nothing ran on a phone): the plan changes
Notes: `tickets/archive/night-notes-2026-11-25.tar.gz` `T1154-scoping.md` (the inventory with file:line, the interfaces as they fit this code, the lanes).
- **MapLibre must NOT move onto a 127.0.0.1 tile server** (this replaces Work step 4 above): on Android, MapLibre
  holds every http request while the phone is offline, so airplane mode would show a blank map
  (`online_file_source.cpp:497-504`); it writes every http tile into its 50 MB cache, pushing the Esri tiles out;
  and our patch reads tile ids only from a `pmtiles://file` address, so the map would lose all its colours with no
  error. MapLibre stays on `pmtiles://file://`; the server is built only for the Mapbox trial (T1155).
- The owner's handoff is wrong about the code in about 14 places (it says "verify, the code wins"): engine code
  is not only in `Map.native.tsx` (`labelHold.tsx`, `useColouring.ts`, `tile-hold.ts`, `styleAssets.ts` and four
  shell plugins / modules also know MapLibre); the Android MapLibre build carries no patch of ours; the style is
  assembled per engine at run time, not by `pnpm style:build`; the real per-engine parity test is
  `adapter-contract.render.test.tsx`, not `parity.test.ts`.
- The seam is four lanes, all invisible to the user: the style dialect, then the imagery provider beside the
  native split, then the build switch. Before the split moves any code, two tests are added: none exists today
  for the still-tap or for the per-tile event, so a move could drop either silently.

## Owner

- [ ] 2026-11-25 — After the scoping pass: agree the order and the "not slower than" margin for the local tile server (default: no more than 10% slower first paint and pan frame time on a mid Android phone, else it stays as it is).
- [ ] 2026-11-25 — Picks from the scoping (defaults first): the local tile server only in the Mapbox trial, MapLibre stays as it is / a test-only switch / move MapLibre first; the margin: refactors within noise (5% cold start, 2 fps, 10 MB) / 10% everywhere / identical numbers only; lane order as scoped / the native split first; offline imagery: the interface only for now / real downloads now; T1148 lands first / its wording folded into the imagery lane.
