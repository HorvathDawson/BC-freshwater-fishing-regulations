# T1108 — Idea: the Mapbox mobile SDK (no navigation) for free offline corridor maps. Research first.

Status: OPEN — 0.11 shrinks to nothing built: phones stay on MapLibre with keyless Esri, no Mapbox, no offline imagery (owner 2026-11-25, after four scoping passes)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1108 | Idea: the Mapbox mobile SDK (no navigation) for free offline corridor maps; research the terms, cost and the two-engine question first | OPEN — 0.11 shrinks to nothing built: phones stay on MapLibre with keyless Esri, no Mapbox, no offline imagery (owner 2026-11-25, after four scoping passes) | | the research is recorded below; reopen when a provider allows offline corridors through MapLibre |

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

Added 2026-11-25 13:23 (iOS chat): during the App Store walk-through the owner said of the content-rights answer "we
will be changing it to mapbox soon". The 0.10.0 submission answers for Esri, as the app ships. When this ticket
ships, two more places name the new provider: the App Privacy coarse-location row and the App Review notes.

## Acceptance
The owner has the comparison (cost, terms, app size, privacy, work) and has picked; a build ticket follows his pick.

Severity: feature idea. Release: 0.10, chat 4c (research first). Model: Sonnet for the terms and pricing read, Opus
for sizing the engine question. Size: S (research). Owner: yes (the pick).

## Scoped 2026-11-25 (four read-only passes, Opus; the owner's answers, verbatim)

**What was found** (vendor pages read 2026-11-25; sources in the passes' notes, summarised here):
- The app has no Mapbox today. Web and phone share one Esri World Imagery definition
  (`app/packages/map/style/layers.source.json:10-20`, `endpoints.ts:56-68`, mobile `config.ts:97-104`); an optional
  ArcGIS key is already supported. Our own vector data is already fully offline on phones.
- **Mapbox Product Terms (2026-10-20) s2.9.1:** "Customer shall use the Mobile SDKs as Customer's exclusive means of
  accessing the Service Offerings in mobile applications." So Mapbox tiles in a phone app through MapLibre are not
  allowed, online or cached, without a signed Order from Mapbox sales. s1.9 and s2.8.1 forbid downloading or storing
  outside the SDK limits; s2.9.2 gives monthly-user billing (25,000 free) only with a "Qualified SDK", otherwise per
  request (Raster Tiles API: 750,000 a month free, then $0.25 per 1,000).
- **The Mapbox SDK** reads no PMTiles (an in-app tile server would be needed for our 225 MB basemap and atlas),
  sends location and usage telemetry that may only be opted out of (on by default), is closed source (our pinned
  feature-state fix, upstream PR #4919, could not be patched there), counts every map view as a monthly user, and
  caps offline at 750 tile packs. A full move: about 8-12 lane-weeks, 7 lanes, every patch feature re-done in a
  fork of the wrapper, two style dialects (web stays MapLibre), high release risk. A hybrid (one Mapbox screen for
  an offline satellite corridor): large, telemetry on that screen, a second native engine (clash unproven).
- **Esri (E300 of 2026-11-10, E204 of 2026-08-14):** the KEYLESS `services/server.arcgisonline.com` address the
  app ships has no licence grant anywhere in the terms (E204 s3.2(a)); nothing names or forbids it either. It is
  unlicensed and Esri can block it at will. With an ArcGIS Location Platform key, MapLibre is allowed (E300 fn 64,
  fn 109): 2,000,000 basemap tiles a month free, then $0.15 per 1,000. Offline only inside Esri's own SDKs (E204
  s3.2(c)); the device cache is not expressly allowed.
- **Others** (assuming 5 satellite sessions a user-month at 100 tiles): Stadia Alidade Satellite $80 a month, its
  terms allow "caching small amounts of data for offline use in a mobile application ... not to exceed 100MB ...
  per device" (s8.4) but forbid bulk download (s30.1), rural BC detail probably 1-1.5 m; MapTiler $30 then
  $0.15 per 1,000, no export; Google 2D tiles no cache or offline, about $240 a month at 1,000 users; provincial
  orthophotos are "Access Only" (redistribution needs the Province's written permission); open 10 m imagery
  (Sentinel-2) is redistributable and nearly free to host, and the owner called 10 m "useless".
- No provider allows offline corridor imagery freely through MapLibre.

**Owner's answers (2026-11-25, planning chat dialogs):**
- Web: "Web keeps Esri (Recommended)". Spend: "$0 overage + kill switch (Recommended)" (asked for Mapbox; it
  stands for any keyed provider).
- "For the map libre option could we look into how offline works? And what not? Also there is no way to make ask
  work here?" and "We should scope out the mapbox sdk more maybe could mapbox sdk display the protomaps tiles?"
  (answered by passes two and three above).
- "Could we look into what a rewrite would do negative for us etc. also look for other cheap satellite options";
  "Opt out is okay if we decide to do this unless it is full mapbox. But we need more market research".
- After the rewrite negatives and the market research: Esri key = **"Stay keyless for now"**; engine = **"Stay on
  MapLibre (Recommended)"**; offline imagery = **"Nothing now"**; tile cache = "Keep cache, reword the page" and
  "Only the cache rewording" (T1148).

**What this ticket is now:** nothing is built for 0.11. The keyless Esri risk is the owner's informed choice; the
fix when wanted is small (he creates an ArcGIS Location Platform account and key; a usage alert and a kill switch
to the drawn map with a shown reason). Offline imagery reopens when a provider's terms allow it through MapLibre
(candidates to ask in writing, drafts on request: Stadia s8.4, the Province for corridor rights, Esri on the
device cache). The earlier research: `handoff/PLAN/research/satellite/README.md` and
`handoff/PLAN/research/stream-imagery/SPEC.md`.

## Owner

- [ ] 2026-11-25 — Pick after the research page compares the Mapbox mobile SDK (free offline corridors, free until 25k users) with the alternatives; mobile would also switch to Mapbox online satellite and drop Esri (the web keeps Esri unless you say otherwise). The scoping above (planning chat dialogs, same day) already records: web "Web keeps Esri (Recommended)"; engine "Stay on MapLibre (Recommended)"; Esri key "Stay keyless for now"; offline imagery "Nothing now"; this line stays open until you confirm the Mapbox switch is off for now.
- [x] 2026-11-25 — Spend if a keyed provider is used → "$0 overage + kill switch (Recommended)"; tile cache → "Keep cache, reword the page" and "Only the cache rewording" (T1148) (2026-11-25).
