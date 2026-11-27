# Refiling Noons p4 / Seymour p3: read-only finding (L30, 2026-11-25)

Data: main checkout curated + promoted atlas (registry.json, graph dump), data/source FWA streams. Nothing edited.

## 1. What the two lines are, and why they sit in the river's item

| blk | added_streams.json row | source layer, src_id | klass / receiver | wsc | length |
|---|---|---|---|---|---|
| -2000000390 | streams[3] "Tunnel Creek" + name_variants[3] (kind added) | City of Burnaby waterways, 247 | novel, tidal | 900-078372 | 216 m |
| -2200000392 | streams "Turner Creek" + name_variant (kind added) | City of Port Moody ESA streams, 112 | novel, tidal | 900-057239 | 2,198 m |
| -2200000393 | "Turner Creek Trib.1", drains INTO -2200000392 | Port Moody, 115 | novel, receiver added | 900-057239-436900 | 583 m |

**The cause is mechanical. It is neither a name match nor the topology resolver.**
- A tidal added stream (one that drains straight to the sea) gets a watershed code from
  `pipeline/atlas/waters/added_streams/coastal.py` `estimate_coastal_wsc`. That function inherits the NEAREST coastal
  FWA stream's level-1 and level-2 code, then bumps it only until no other ADDED stream uses it
  (`used_coastal`, build_dataset.py:1102/1112). It never checks the FWA codes.
- So the first city line near a river gets that river's exact code. Tunnel Creek got 900-078372, which is the Seymour
  River's code. Turner Creek got 900-057239, which is Noons Creek's code.
- The atlas then names an unnamed piece after the owner of its code (`graph.names.resolve_names` → `wsc.code_owner`).
  The registry folds it into that river's item (`registry.build._wsc_gnis_map`).
- The result: the Seymour River item holds the Tunnel Creek line and answers to "Tunnel Creek" (a municipal name
  source). The Noons Creek item holds the Turner Creek line and answers to "Turner Creek".

**Why the earlier "leave out" did not take effect.**
- The earlier "leave out" is the owner's 2026-11-11 acceptance in `data/curated/waters/unplaced_piece_calls.json`
  `accepted`: rows for noons_creek.r1 / -2200000392:0 and seymour_river.r1 / -2000000390:0.
- That file is read only by the reach's unplaced gate (`reach.build.load_unplaced_calls`). It says "this rule leaves
  the piece out". It never touches the registry, so the piece stays in the item and shows as a part.
- The added-streams generator reads `added_stream_calls.json`. It does not read this file.

## 2. Is there an FWA line for the same stream?

- **Tunnel Creek:** no.
  - No FWA line lies within 1 km except the Seymour River main line (1,068 m away). The next nearest is an unnamed
    line, 360455980, 1.67 km away.
  - The only FWA "Tunnel Creek" (356499930) is in the Interior.
  - Its neighbours, Gull Creek and Crabtown Creek T1, are also city lines, not FWA lines.
  - So "take the FWA version" here can only mean dropping the city line.
- **Turner Creek:** no FWA line carries the name near Port Moody. The FWA Turner Creeks are 356301038, 356546889 and
  360504907, the nearest about 27 km west. Candidates for "the real line":

  | FWA line | distance | length |
  |---|---|---|
  | West Noons Creek 360595504 (gazetted) | 261 m | 3.3 km |
  | unnamed 360548495 | 433 m | 701 m |
  | Noons Creek 360880849 | 678 m | — |

  Which of these is the real stream is the gotcha.
- **Side find:** a "duplicate" name variant puts the city's "West Noons Creek" on the unnamed FWA line 360244416,
  while FWA's gazetted West Noons Creek is 360595504. That looks like the same kind of mislabel. I have listed it,
  not judged it.

**What "take the FWA version" means in the curated files** (proposal):
- (a) `data/curated/waters/added_stream_calls.json` `exclude_src_ids`: add `{"source":"port_moody","src_ids":["112","115"]}`
  and `{"source":"burnaby","src_ids":["247"]}`, each with its why. This keeps a future regeneration from minting them.
- (b) `added_streams.json`: remove the 3 stream rows and their 3 name_variants. A regeneration does this; a hand edit
  gives the same result for these rows. check_curated only checks the file's structure.
- (c) `unplaced_piece_calls.json`: its 2 accepted rows would then name pieces that no longer exist, and a stale row
  STOPS the reach run. So they must be removed. They are the owner's rows, so this needs his OK. His ruling tonight
  plausibly covers it.
- Trib.1 must go with Turner Creek. Otherwise it is a line with no receiver, and the generator would re-resolve it to
  something else.

## 3. Where gotchas live

`pipeline/docs/GOTCHAS.md`: `## <title>` entries, newest first. Its "Kept city-layer names" entry already says "the
Seymour River item holds the city's Tunnel Creek (`-2000000390`)". The owner's look also needs a line in
`tickets/OWNER-UNFILED.md` (or the new ticket's `## Owner` block).

Proposed GOTCHAS entry, newest first:

    ## Two city stream lines we left out for FWA's: which is the real stream? (owner 2026-11-25)
    Owner: "We want them to be refiled properly so let's take the fwa version for now but add a gotcha to check on
    which one is the real stream line." The city lines Tunnel Creek (Burnaby src 247, blk -2000000390, 216 m) and
    Turner Creek (Port Moody src 112, blk -2200000392, 2,198 m, with Turner Creek Trib.1 src 115) are excluded
    (added_stream_calls.json). They had been filed under the Seymour River and Noons Creek because a tidal city line
    inherits the nearest FWA river's watershed code (`added_streams/coastal.py`). FWA has no line for Tunnel Creek
    within 1 km. For Turner Creek the FWA candidates are West Noons Creek 360595504 (261 m) and the unnamed 360548495
    (433 m). If one of the city lines is the real stream, put it back as its own water, not as a piece of the river.

Owner line: `- [ ] 2026-11-25 — Tunnel Creek (Burnaby) and Turner Creek (Port Moody): we took FWA's lines; look at
the map and say which line is the real stream (city or FWA; for Turner: West Noons Creek or the unnamed 360548495).
Default: FWA's, the city lines left out.`

## 4. The smallest change, its parity and the reruns

- **Change:** (a) + (b) + (c) above. No code change is needed for these two.
- **Optional root fix, a separate proposal:** in `estimate_coastal_wsc`, seed `used` with the FWA coastal codes too,
  so a city line never shares a river's code. This changes the code of the 3 colliders only.
- **Expected atlas parity:**
  - Three sections vanish: -2000000390:0, -2200000392:0 and -2200000393:0.
  - The Seymour River item loses -2000000390:0 and its "Tunnel Creek" name source.
  - The Noons Creek item loses -2200000392:0 and its "Turner Creek" name source.
  - Trib.1 belongs to no item.
  - Nothing else should move. The two lines touch only Turner Creek Trib.1.
- **Reach:** noons_creek.r1 and seymour_river.r1 lose their `unclassified` diagnostic. The zone and region rules drop
  those 3 sections. No other binding should change.
- **Reruns:** added_streams regeneration (or the hand edit), then atlas (~32 min, promote), then reach + tiles +
  deliver (~33 min).
- **Parts afterwards:**
  - Seymour River: tidal p0, p1 (seymour_river.r1), p2 (no-access). p3 is gone.
  - Noons Creek: tidal p0, p1-p3. p4 is gone.
  - Both nameless parts disappear.

## 5. Other added streams misfiled the same way

Of the 320 added streams, 4 are filed under a water of another name:
- Tunnel Creek → Seymour River (FWA code collision).
- Turner Creek → Noons Creek (FWA code collision).
- **Capitol Creek** (Burnaby src 297, blk -2000000141, code 900-053687) → item `wsc:900-053687` "Squatters Creek".
  It shares its code with the unnamed FWA line 360570081, which carries the city's Squatters Creek name. This is the
  same collision.
- Northrop Channel (OSM) → "Northrop Spawning Channel". This is a name variant of the same water, not a misfile.

That makes 3 code-collision misfiles: the two here plus Capitol Creek. I fixed none of them.

---

