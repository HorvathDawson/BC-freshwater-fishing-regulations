# T1119 — Boat launches, docks and the like on the basemap, always drawn, from OpenStreetMap

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1119 | Boat launches, docks and the like on the basemap, always drawn, from OpenStreetMap, through every stage | OPEN — 0.11 at the earliest (owner 2026-11-25); research and pictures first | | owner picks what "etc" covers, the look, tap or not |

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
