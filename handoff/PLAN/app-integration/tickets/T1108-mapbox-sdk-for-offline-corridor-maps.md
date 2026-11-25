# T1108 — Idea: the Mapbox mobile SDK (no navigation) for free offline corridor maps. Research first.

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1108 | Idea: the Mapbox mobile SDK (no navigation) for free offline corridor maps; research the terms, cost and the two-engine question first | OPEN — 0.10, chat 4c (research first) | | owner 2026-11-25 |

Source: owner 2026-11-25 ~00:40 (planning chat), verbatim: "i got an idea for free offline maps for corridors we could
integrate mapbox mobile sdk with no navigation and it will be free until 25k users which should be more then good. we
could implement that in 0.10 ?" Belongs with T79 (imagery corridor + pools), roadmap chat 4c.

## Research, before anything is built (the planning chat's questions; none is answered yet)
1. What is free and for how long: the Maps SDK's free monthly-active-user allowance today, what counts as a user,
   and the price past it. Read Mapbox's current pricing page; do not rely on the figure above or on memory.
2. Offline terms: may satellite imagery be downloaded for offline use, how many tiles / tile packs per device, do
   packs expire, and does offline use need the device to call home. This decides whether "corridor imagery
   offline" is possible at all on their terms.
3. Two map engines: the app draws everything with MapLibre (one pinned upstream commit since 0.9.0) from our own
   tiles. Mapbox's terms tie their tiles to their SDK, so this means either a second engine in the app for one
   screen (size, memory, two sets of map code) or moving the whole map to Mapbox (a rewrite of the styles and
   layers work). Size both.
4. Rules this crosses, each needs the owner's say: `self-host-everything` (today only Esri imagery and Open-Meteo
   are outside servers); the SDK sends its own telemetry, which changes the privacy page and both stores' privacy
   answers (T1098, T1099); an access token ships inside the app; the web app is a separate product with its own
   allowance.
5. The alternatives on the same table: our own corridor imagery tiles from an open source (the T79 line) served
   from R2 and downloaded like the data packs; Esri's offline terms; what each costs in storage and egress.
6. One page with a recommendation and the numbers, to the owner, before chat 4c freezes its scope.

## Owner's addition 2026-11-25 00:38 (planning chat)
Verbatim: "also in 0.10 when we add offline corridors we will switch mobile to use mapbox online satellite too and
remove esri from mobile." So if the research says go, the scope is both: Mapbox for the offline corridor imagery and
for the online Satellite map on Android and iPhone, with Esri World Imagery removed from the phone apps. The web is
not named, so it keeps Esri until he says otherwise (planning chat's reading). What changes with it, to be listed by
the research: the map credits and Sources and licences (T235), the privacy page and the stores' data answers (Mapbox
is a new third party that sees map requests; T1098, T1099), the App Store "content rights" answer that names Esri
(T1071), the `self-host-everything` exception list (Mapbox in, Esri out on mobile), whether Mapbox's terms allow its
satellite tiles inside MapLibre or require its own SDK for the whole map, and what the monthly-user count is counted
on (the 25k free figure is the owner's; confirm it against the price page).

## Acceptance
The owner has the comparison (cost, terms, app size, privacy, work) and has picked; a build ticket follows his pick.

Severity: feature idea. Release: 0.10, chat 4c (research first). Model: Sonnet for the terms and pricing read, Opus
for sizing the engine question. Size: S (research). Owner: yes (the pick).
