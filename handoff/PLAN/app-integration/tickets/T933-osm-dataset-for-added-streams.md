# T933 — A dataset for OpenStreetMap of the streams we added (`added_streams.json`)

Status: POSTPONED (deferred with T930, owner 2026-11-20: after the bathymetry map, stocked lakes and charts).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T933 | A dataset for OpenStreetMap of the 324 streams we added (licence per city, missing-only, OSM import process) (owner) | POSTPONED (deferred with T930: after bathy map, stocked lakes, charts) | | owner 2026-11-20 "dataset for OSM" |

Source: owner 2026-11-20 00:03 (main chat), verbatim: "Then another one to work on creating dataset for OSM to add I added
streams." Goal: give back the streams this project added beyond the provincial FWA network so OpenStreetMap can carry them.

## What is there (2026-11-20)
`data/curated/waters/added_streams.json`: 324 streams (310 novel, 14 extensions of FWA lines) with geometry (EPSG:3005
segments), names, flow direction (DEM-oriented) and where each joins its receiver. Sources: Burnaby 242, Port Moody 50,
Squamish 30 (city open-data stream layers, each under that city's open government licence, see
`pipeline/atlas/waters/added_streams/SOURCES.md`), 1 already from OSM (Northrop Channel), 1 drawn by hand (Sheldens Creek
estuary channel). Abbotsford names also live there as `name_variants`.

## Work
1. Licence check per city (the blocker): OSM accepts data only under ODbL-compatible terms; each municipal OGL needs the
   OSM community's compatibility answer or the city's written waiver (osm.org/wiki/Import/ODbL_Compatibility). List the
   result per source; a source that is not compatible stays out.
2. Leave out what OSM already has: match each stream against current OSM waterways (Overpass), by geometry overlap and
   name; output only the missing ones and name/tag gaps on existing ones.
3. Build the file: GeoJSON/OSM XML in WGS84, OSM tags (`waterway=stream|river|canal|ditch|drain` from `ftype`, `name`,
   `intermittent`/`tunnel=culvert` where the source says), lines drawn downstream (OSM direction rule), source and date in
   `source=*` on the changeset, not on each way.
4. The import process, not an upload: an Import wiki page, a post to the OSM Canada community / imports list, a dedicated
   import account, small reviewed changesets (MapRoulette or the Tasking Manager). The owner (or a local mapper) does the
   edits; Claude prepares the files, the page text and a review map.
Depends: none. Size: M (+ community time). Owner: yes (the accounts and the posting).
