# T1119 — Boat launches, docks and the like on the basemap, always drawn, from OpenStreetMap

Status: OPEN — 0.11; scoped 2026-11-25, the owner picked the kinds, the filter, the coast and no tap; a spike first

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1119 | Boat launches, docks and the like on the basemap, always drawn, from OpenStreetMap, through every stage | OPEN — 0.11; scoped 2026-11-25, the owner picked the kinds, the filter, the coast and no tap; a spike first | | spike, then 4 lanes (2 Opus, 2 Sonnet); pictures to the owner |

Source: owner 2026-11-25 (planning chat), verbatim: "add a ticket to add boat luanch and docks etc etc to the basemap
(will always be displayed) we will get this data from osm and will want it added all the way up properly will go in
0.10 at min". Related: T280 (the basemap's two stages: raw source, then the pruned ship copy), T235 (attribution and
licences: the basemap already comes from OpenStreetMap through Protomaps, credited on the map), T299 (map labels and
place density), T270 (`self-host-everything`: served from our servers only).

Planning chat's reading of "all the way up properly" (not the owner's words; he corrects it if wrong): the features
go through every stage like any other basemap content, not as a side file: source fetch, the pruned ship copy, the
release and its tiles, the map style on Android, iPhone and web, the legend, the credits, and the checks.

## Open, for the owner (pictures first, one batch)
1. What "etc" covers. Proposed: boat launches (`leisure=slipway`), docks and piers (`man_made=pier`, `floating=yes`
   among them), marinas (`leisure=marina`). Candidates to show and ask about: boat rentals, fishing piers and
   platforms (`leisure=fishing`), fuel docks, public wharves. Private docks: OSM tags `access=private` on some; show
   how many there are and ask whether they are drawn.
2. The look: an icon per kind, from which zoom, with or without a name label, on the Standard and the Satellite map.
   Two or three options as pictures on a busy lake (Okanagan, Shuswap) and a small one.
3. Whether a tap does anything (name and kind) or they are drawn only. Proposed: drawn only in the first cut.

## Work
1. Research, no build: does the raw Protomaps file we already fetch carry these (which layer, which kinds, from which
   zoom), or do they need a second OSM source (a BC extract)? Counts per kind in BC, how many sit on or beside a
   water in our atlas, and how complete OSM is against a known list (BC Parks and Recreation Sites launches). One
   page with the counts and the pictures of item 2; the owner answers the three questions.
2. Build the stage: the features are kept through the prune (T280) or added as their own layer in the basemap file,
   by one command that rebuilds from the fetched source with nothing kept by hand. Size cost of the ship copy stated
   before and after. No silent drop (`no-silent-fallbacks`): a kind that is asked for and comes back empty stops the
   build.
3. Style: the layer in the one style source all three platforms read, always on (no toggle), offline with the
   basemap, icons self-hosted in the sprite. Legend entry. Rule 28 (map layers) checked.
4. Credits: OpenStreetMap is already credited; check the wording on Sources and licences still covers it (T235).
5. Checks: a build check on the count per kind against the last build (a large fall stops it); a render test on one
   known launch and one known dock; device check on Android and iPhone, offline.

## Acceptance
On all three platforms, online and offline, a known boat launch and a known dock show at the agreed zoom on both map
kinds, whatever mode is on; the legend names them; a rebuild from the source reproduces the layer; the checks above
fail when the layer is emptied (mutation).

Depends: T280. Severity: feature. Release: 0.10 at the earliest (owner). Model: Opus for the source and prune stage,
Sonnet for the style, legend and pictures. Size: M. Owner: yes (the three questions above, with pictures).

## Scoped 2026-11-25 (read-only pass, Opus; the owner's picks, verbatim)

**What exists:** the basemap is Protomaps' build of OSM (`data/source/bc.pmtiles`, build 20260305, pinned by
sha256); we cut it and never re-tile it, and a changed basemap makes phones re-download about 225 MB. Piers are
already drawn grey with no icon; slipways and marinas are only in the `pois` layer we drop. We ALREADY fetch this
data from Overpass (`data/fetch_data.py:803-931`; pin `data/source/pins.json:92`, 2026-07-20, 16,704 rows) and
nothing ships it (T235). The snapshot: piers 3,858 nodes and 11,854 ways (521 named), slipways 426 and 316 (98
named), marinas 58 and 150 (145 named), fishing 39 and 3; 934 rows tagged private. Piers are 94% of rows, almost
all unnamed residential docks. The snapshot is out of step with the code (a fresh fetch gives about 15,800).

**Design (recommended and taken as the plan):** our own Overpass layer in the ATLAS tiles, built like the
`obstacle` layer (`pipeline/deliver/tiles/obstacles.py`, `layers.py:415-434`, `tile-contract.json:268`,
`tiles/__main__.py:87-123`, `layers.source.json` then `pnpm style:build`), always on (`toggleable: false`), icons
drawn in code like the obstacle icons, icon from z11 and name from about z14 (proposals), both map kinds.

**Owner's picks (2026-11-25, planning chat dialog):** kinds = "Launches, marinas, named piers (Recommended)"; private
docks = "Hide, drop unnamed piers (Recommended)" (hide anything tagged private; drop unnamed piers unless next to
a launch or marina); coast = **"All of BC incl. the coast"** (so no fresh-water-only filter: ocean launches and
marinas show); tap = "Drawn only (Recommended)".

**ODbL (not legal advice):** attribution is already shown; every feature of this layer is OSM, so it stays a
separate layer (the Horizontal Map Layers guideline); the filtered snapshot is committed with a `_licence` record
and listed in `data/LICENSE-ODbL.md`. Mixing in non-OSM launches (Rec Sites) would bring them under share-alike:
do not, without asking.

**Lanes (spike first):** 1 source and snapshot (Opus, M: `data/fetch_data.py` kinds, keep the osm3s base,
`pins.json`, a committed pinned GeoJSON, stop if a kind comes back empty); 2 tile layer (Opus, M: new
`pipeline/deliver/tiles/water_access.py` with dedupe and way-to-point, contract, a count-against-last-build
check; one full tiles build through `heavy.sh`); 3 style, icons, legend, credits (Sonnet, M, beside lane 2 on a
fixture); 4 device and offline check (Sonnet, S, with the APK batch).
**The spike proves:** recall against BC Parks and Recreation Sites launch lists, the atlas byte cost (expected
1-2 MB), piers not drawn twice (Protomaps' grey shape plus our icon), pictures on Okanagan, Shuswap, one small
lake and one coastal harbour for the owner. T1129 (give-back to OSM) stays after launch.

## Owner

- [x] 2026-11-25 — The three picks for boat launches, docks and the like → kinds "Launches, marinas, named piers (Recommended)"; private docks "Hide, drop unnamed piers (Recommended)"; coast "All of BC incl. the coast"; tap "Drawn only (Recommended)" (2026-11-25, planning chat dialog).
- [ ] 2026-11-25 — Look at the spike's pictures (Okanagan, Shuswap, one small lake, one coastal harbour): the look, from which zoom, with or without names.
