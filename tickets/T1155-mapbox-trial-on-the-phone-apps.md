# T1155 — Trial: the Mapbox engine on the phone apps (online satellite, corridor downloads, our own layers and painting), measured against today

Status: OPEN — after T1154 (owner 2026-11-25); a trial on test builds, not a release

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1155 | Trial: the Mapbox engine on the phone apps (online satellite, corridor downloads, our own layers and painting), measured against today | OPEN — after T1154 (owner 2026-11-25); a trial on test builds, not a release | — | waits for T1154; the owner creates the free Mapbox account and token; then the spike list below, numbers to him, he decides go / no-go |

Source: owner 2026-11-25 evening (planning chat), verbatim: "then we could have. the next ticket after that be a
testing it out for the apps. basically we can get a free token figure out how to properly have it in the map
being public repo and then test it with corridor downloading and mobile satellite online using it since it is mau
based. then we can gauge how similar it is to what we had (space requirements and performance) and also we can
make sure our pmtile thing we get working well and the painting etc still happens well and is performant." The
research handoff is `docs/app-integration/T1108-files/handoff-2026-11-25-offline-satellite-and-reversible-engine.md`
(its direction: the web stays on MapLibre + PMTiles; the phone app may move to the Mapbox SDK for the basemap and
imagery only, our PMTiles layers stay; switching back must stay easy; provincial orthophotos later if GeoBC grants
a licence). Related: T1108, T1154, T1148.

## Work
1. The token in a public repo: a public (pk) token restricted by app id / bundle id and URL, never in git (build
   secret, `EXPO_PUBLIC_…` at build time), contributors use their own; a secret download token only if the SDK
   install needs one (kept out of the repo and CI logs); what an attacker can do with the shipped token and the
   $0 overage + kill switch the owner asked for ("$0 overage + kill switch (Recommended)").
2. A Mapbox engine folder behind T1154's seam (`@rnmapbox/maps`), one native engine linked per build.
3. Test on real phones (a mid Android and an iPhone), each with a number and against today's MapLibre build:
   - our layers from the local tile server: do they draw, and is the painting (feature-state colouring of the whole
     province, a full pass timed; there is no batched call) as fast; theme and mode swaps; still-tap; pan and
     pinch feel; tile ids reaching the app.
   - online Mapbox Satellite: look and date on ten remote BC rivers beside Esri at the same places (pictures).
   - corridor download: one river corridor as an offline tile region: size on disk per km of river by zoom, time
     to download, what it costs in monthly users, the 750-pack limit, what happens offline.
   - app size, cold start, memory, battery; telemetry: what is sent and that it can be switched off.
4. Confirm in writing with Mapbox (the owner sends; a draft is provided): offline tile regions are covered by
   monthly-user pricing in SDK v11; what "no redistribution" means for an open-source app; the nonprofit program.
5. What changes outside the code if it goes ahead: the privacy page and the stores' data answers (a new third
   party sees map requests), the App Store content-rights answer, the credits, the `self-host-everything` list.
6. Report: a table (today against Mapbox), the pictures, how much of the MapLibre patch would still be needed,
   and a recommendation. He decides go / no-go; a build ticket follows a go.

## Acceptance
The owner has the numbers and pictures and has decided. Nothing ships to testers or the stores from this ticket;
the MapLibre build still builds and passes its tests at the end (switching back is one setting). Severity:
feature trial. Model: Opus. Size: M–L. Owner: the Mapbox account and token, the letter to Mapbox, the decision.

## 2026-11-25 night: from T1154's scoping
The local tile server for our PMTiles is built HERE, for the Mapbox engine only (MapLibre stays on
`pmtiles://file://`: moving it would blank the map offline on Android and drop the colours; see T1154). So the
trial also has to prove on Mapbox what MapLibre gets from the file path: the map offline in airplane mode, the
per-tile feature ids for colouring, and that the server's tiles do not evict the satellite cache.

## Owner

- [ ] 2026-11-25 — When T1154 is done: create a free Mapbox account and a restricted public token for the trial builds (steps will be handed to you), and send the confirmation questions to Mapbox (a draft comes with them).
- [ ] 2026-11-25 — After the trial: go / no-go on the Mapbox engine for the phone apps, from the numbers and pictures.
