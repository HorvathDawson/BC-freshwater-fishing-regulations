# T927 — Search finds the book's boundary places (where a water's rules change)

Status: OPEN.

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
