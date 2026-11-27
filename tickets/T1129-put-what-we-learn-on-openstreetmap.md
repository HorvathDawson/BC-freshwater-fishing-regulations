# T1129 — Put what we learn on OpenStreetMap: missing BC streams and the names anglers use

Status: OPEN — after the Play launch (its own chat); research and one small trial first

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1129 | Put what we learn on OpenStreetMap: missing BC streams and the names anglers use | OPEN — after the Play launch (its own chat); research and one small trial first | | owner 2026-11-25; licence table per source, then a small trial import by the owner |

Source: owner 2026-11-25 (planning chat): "add a new ticket for this ... putting what we learn on osm", with a
thread of 2026-11-19 pasted, in which he proposed the import to an OpenStreetMap contributor and got advice back.
The repo is public: the thread is summarised here, without account names. Related: T224 (name variants from local
sources), T235 (attribution and licences; the app already uses 177 accepted OSM names and one OSM geometry under
ODbL), T1118 (name variants bound across management units: wrong ones must not be exported), T1119 (we take boat
launches from OSM: this is the other direction).

## What the owner proposed (his words, 2026-11-19)
"I would probably want to do it as a mass import. I haven't checked how many streams are missing from OSM but from
my experience I assume close to a third of the streams in rural BC could be missing (including a lot of named
streams). Most of those would be a straight import from the BC Fresh Water Atlas. Then the more complicated one from
a source perspective would be the lakes since the name variants come from a bunch of sources."

His count: about 2,650 waters (about 2,200 lakes, 450 streams): about 2,000 unnamed in the Fresh Water Atlas with a
name from a source (1,650 lakes, 380 streams), about 1,080 officially named with another name anglers use (1,000
lakes, 70 streams). Some waters have more than one source, so the rows add up to more than the totals.

| Source | Unnamed in FWA, named from this source | Already named, another name from this source |
|---|---|---|
| Lake depth (bathymetric) map titles | ~570 | ~300 |
| Angler lake-info map labels | ~620 | ~40 |
| Fish stocking records | ~140 | ~770 |
| Names set by hand (reservoirs, fixes) | ~425 | 2 |
| City stream maps (e.g. Burnaby, Port Moody) | ~325 | ~17 |
| Fishing regulations synopsis wording | ~50 | ~110 |
| BC lake surveys | ~25 | 8 |
| Water Survey of Canada gauge names | ~17 | 5 |
| DFO salmon regulation pages | 5 | 10 |
| Federal vessel restriction schedule | 3 | 1 |

"Then obviously a lot of geometry would be added too."

## The advice he got back (summarised)
- JOSM is the tool. Some of these waters already exist in OSM from earlier imports (NRCan among them), so the work
  is conflation, not a blind add (the OSM wiki's Conflation page).
- Start with one or a few small imports to learn the process before a large one.
- The import guidelines mean proposing it to the local (or global) community first; a proposal from an account with
  few prior edits is received more cautiously, another reason to start small.
- Check each source's licence against OSM's (the OSM Foundation's Licence Compatibility page). The Fresh Water Atlas
  is believed to be under the BC Open Government Licence; every other source needs its own check.
- If it is too much, someone in the community may run the import from a dataset he provides.

## Work, in order (nothing is uploaded to OSM by an agent: every edit there is the owner's, from his account)
1. Licence table, one row per source above: who publishes it, its licence, whether OSM's compatibility list accepts
   it, and the evidence (a link). Expected hard rows (planning chat's guess, to be checked): the angler lake-info
   map labels and the stocking records (not government open data), the synopsis wording, the city maps (each city
   its own licence). The names set by hand are the owner's own and can be given. The 177 names that came FROM OSM
   are left out. A source that is not compatible is dropped from the export, and the table says so.
2. Measure, read-only: how much of the FWA stream network in BC is missing from OSM (per region, named and
   unnamed), how many of our named waters OSM already has with the same or another name, and how much geometry an
   import would add. This answers "close to a third".
3. An export tool: for the compatible sources only, a file JOSM can open, one feature per water with the OSM tags
   (`name`, `alt_name`, `waterway` / `natural=water`, the source tag the import guidelines ask for), matched against
   what OSM already holds so that an existing water gets a name added, not a duplicate. Names from T1118's list of
   doubtful bindings are held back until that audit clears them.
4. The proposal page for the OSM community (the import guidelines' steps: wiki page, the imports and talk-ca
   lists), drafted for the owner to post, with the licence table and the counts.
5. One small trial the owner runs himself: one watershed or one city's streams, reviewed in JOSM, uploaded by him.
   Then decide the size and pace of the rest, or whether a community member runs it from our dataset.
6. Keeping in step: what changes in OSM after the import is not ours to overwrite; decide whether we ever re-export
   (new names only) and how.

## Open, for the owner
- When: proposed after the Play launch, as its own chat; the licence table and the measurement (steps 1–2) are cheap
  and could run earlier beside another chat.
- Whether the alternate names anglers use go in as `alt_name` or `loc_name` (the community will have a view).

Acceptance (first stage): the licence table with evidence per row, the measured counts, and a trial file for one
small area that the owner has opened in JOSM. Severity: none (a contribution, not a defect). Model: Opus for the
conflation and export, Sonnet for the licence table and the proposal page. Size: L over several sessions; the first
stage is M. Owner: yes (posts the proposal, makes every upload, answers the community).
