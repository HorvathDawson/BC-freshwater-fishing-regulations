# T927 — Search finds the book's boundary places (where a water's rules change)

Status: IN REVIEW — owner: OWNER-TODO "T927 = every placed cut searchable" (owner looks at the pictures, then main chat
review + merge; ships with the next data release: a new core table, see "Lane 2026-11-20").

Source: owner 2026-11-19 (main chat), verbatim: "we should make the search know where all the cuts are so a user could
search a cut location from book and find location. Thoughts?" Measured the same evening (read-only, scratchpad
`search-probe/`): `cuts.json` holds 505 authored cuts on 231 waters (473 distinct labels: 370 landmarks — 274 properly
named — 109 confluences, 18 lake ends, 5 area lines, 1 border). All 505 are placed (`splits.resolved.json`, stream key +
metres) and worded in the release (`display.ends`); 477 are a part's start or end, 454 have a map mark. About 100 more
places exist only in book words, mostly generic ("within 100 m of outlet"). Today "Vedder Crossing" finds only the town and
"Greendale" finds Glendale Lake.

## Work
- Data: a small core table (`cut_place`: water, the place's words, its search key, stream key + metres; ~15 KB) built by
  the bundle from `splits.resolved.json` and the `display.ends` words — a PLACE (rule 5), never a section id.
- Result row: "Greendale Trestle — on the Cowichan River, where its rules change"; it opens the water at that place
  (the existing place lookup), the part on each side shown.
- Ranking: below water names, above typo matches; when a town shares the name, both show.
- Pitfalls: a label on several waters ("tidal boundary" on 8) always names its water; generic labels ("lower falls",
  "signs") only with the water's name in the query; near-duplicates (a bridge and "200 m up/downstream" of it) one row;
  a lake-edge cut opens the stream side; tidal limits come from `tidal.resolved.json`.
## Acceptance
"Vedder Crossing", "Greendale Trestle", "Limonite Creek" (the Zymoetz boundary) each find their place first among places;
pictures of the result rows and the opened place to the owner.
Depends: a release (new core table). Size: M. Owner: yes (look). Owner 23:49: all the placed cuts (see below).

## Owner 2026-11-19 23:49 (main chat): yes
Verbatim: "We do want to add all the cuts on a river as a searchable thing too. So you could search something like ten Mile
pool and get chuckwalla river?" Yes: `cuts.json` `chuckwalla_river__fishing_boundary_signs_at_ten_mile_pool_200_m_downstream`
is placed. Acceptance adds "Ten Mile Pool" → Chuckwalla River. "All the cuts": every placed cut, not only the named 274
(generic labels still need the water's name in the query, per Pitfalls).

## Lane 2026-11-20 (lane/t926-t927, commit 8cce181ab)
Built as the Work section says, with these specifics:
- Bundle `cut_place` (split_id, blk, m): every CURATED cut of `splits.resolved.json` (`source: curated` — the 505 of
  `cuts.json` + 2 curated tidal-limit cuts = 507), once, at its host row (a braid sweep's extra row never speaks). Release
  core `cut_place` (ord, words, alt, generic, blk, m, lon, lat; additive, still core/1; contract + pack regenerated): the
  words are the part ends' own (`display.ends`, the ", approximate location (… confidence)" clause taken off, so it works
  with or without the L2 lane's change), the cut's label and other names in `alt`, the water(s) from `split.at`, else the
  waters whose sections hold the place; the mark's position where `part_mark.token` names it (459 of 513 rows). 513 rows,
  503 distinct places, 506 of 507 cuts on a water (the one left: an Okanagan River Oxbows slope cut no section holds).
- Generic (80 rows): the cut's own label has no capital outside brackets ("lower falls", "tidal boundary (DFO Subarea
  3-18)", "cable car crossing (200 m downstream of Vedder Crossing bridge)"). Found only when the query also names the
  water ("liumchen falls"; a kind word like "creek" does not count).
- Matching (core `queryCuts`): exact · prefix · a word of the words or another name (never "anywhere inside", never by a
  typo), else every query word starting a word of the place or of its water's name. Two places on one blue line within
  500 m are one row (Ten Mile Pool's two cuts). At most 6. Group "Where rules change": after the names as typed, before
  a typo's waters; a town of the same name keeps its own group ("Vedder Crossing" shows both).
- Row: "<words>" / "on the <water>, where its rules change". Tap: the map opens 1.5 km round the mark, marks it and lights
  the water. Go (›): the water's Map at the part holding the place (`place.ts` `resolvePartPlace`, the named-link
  lookup; Ten Mile Pool opens Chuckwalla River "Part 3 of 3 · Ten Mile Pool (100 m upstream) → Source").
Measured (probe over the side release's rows): named places found by their own words 424 / 433 rows, first among places
410 (the rest share one label on several waters, e.g. "CN Railway Bridge (tidal boundary)" — the row names each water);
generic found with the water's first word 78 / 80. "Ten Mile Pool", "Greendale Trestle", "Vedder Crossing" (→ Vedder
Crossing Bridge), "Limonite Creek" (→ the Zymoetz's Limonite Creek confluence) each first among places. Keystroke time
unchanged.
Proof: a side deliver (`data/generated/side/t926-t927/out`, release r-8d7bd8d70ff6) vs live r-6258ffded676: bundle
differs only in `alias` (+13, T926), `name_source` (+13, their sources), `cut_place` (new) and `meta` (build path,
schema digest); release: only `core` (alias, name_source, cut_place) and the `name_source` of six core slices differ —
every regs, water, paint, lines, salmon file byte-identical; status index identical; vintage strict passed. No rule
binding moved.
Pictures (390 px, web, light; before = live release, after = the side release): `T927-shots/` before/after for "Ten Mile
Pool", "Greendale Trestle", "Vedder Crossing", plus `after-ten-mile-pool-selected.png` (the mark on the map) and
`after-ten-mile-pool-opened.png` (the water opened at its part). The dev server served no web fonts (both sets alike).
Note: the map pin uses the town pin's colour (`palette.place`), and on a phone the half sheet sits just under the mark.

## Owner 2026-11-20 ~06:34 (main chat dialog): pictures approved
## Review fixes 2026-11-20 (lane/t926-t927)
- A one-word query that is only a kind of place ("bridge", "falls", "confluence", "signs", "boundary", "mouth",
  "outlet", "inlet", or a water kind) does not find places by a word inside them; a place starting with it still does
  ("Bridge River confluence") (6cae36cbd).
- No silent drops (445b1bdce): `core_cut_place` counts, prints and returns every curated cut on no water (the
  Okanagan River Oxbows slope cut today); `placePartOf` reports a place no part holds (the water still opens, on its
  own part). The confidence-clause strip is gone: since T780 `display.ends` says the place alone.
- Tests: the mini release's "Mock Creek Falls" read through the release reader (`makeReleaseSource().searchIndex()`) and
  found; `placePartOf` + `waterOpener.searched(item, at)` open the Map on the part holding the place.
