# T665 — Tidal: cut every creek where it leaves the tidal polygon; remove lines wholly inside it

Status: PARTIAL — f1f928029, 227c53b08, a3518f5d2, 2a21460e0, 76f067600, 56b18e8bb; left (Claude): promote the side build (`handoff/PLAN/app-integration/next-rebuild.md` steps 7 to 9) after the owner's go on `next-rebuild-results.md`; the shore rule into RULINGS.md and MAP.md

Source: owner 2026-11-11, on the T660 follow-up (A–J): "every item in the 103 can be removed. And the plan in general
looks solid just need to figure out a clean way to implement it." Next atlas rebuild (with T664 and the nine waiting DFO
cuts); NOT build 27. Left alone until build 27 is out.

Measured in `T660-tidal-tributaries-not-cut-at-boundary.md` ("Owner follow-up 2026-11-11 (A–J)"),
`handoff/PLAN/app-integration/T660-owner-followup.json`, `handoff/PLAN/app-integration/T660-inside-named.json`, maps in
`handoff/PLAN/app-integration/T660-shots/`.

## Owner rulings
1. A creek that joins water tidal under the curated limits ALWAYS gets a cut; only the piece inside tidal water goes
   tidal. No 50% skip, never all-or-nothing ("Removing the entire section is bad").
2. Every line wholly inside the tidal polygon is removed: the 103 unnamed AND all 103 named (206 lines, 66.4 km).
3. Blue line over tidal shading is fine where there is no reason to remove it. The 191 freshwater receivers are as
   wanted (the polygon is not the regulation boundary). T662 dropped.
4. Short slivers (under 5 m) can be removed when unnamed.
5. Serpentine, Cowichan, Copper Creek: already `mouth`; nothing to change.

## Work (design first: the owner wants a clean way to implement it)
- Cut point = where the line leaves the DFO tidal polygon (lane's recommendation over the crossing top): of the 161
  tidal cuts 55 move < 25 m, 101 move up (+14.0 km tidal), 5 move down (−0.2 km); +12 cuts on creeks with no crossing run
  (Fraser delta, Steveston 356242813); 1 of the 25 skipped runs changes (356070273). One cut per line, at the highest
  exit (3 lines re-enter the polygon).
- The 371 cuts into freshwater receivers make nothing tidal: propose dropping them (owner has not ruled).
- Removal: proposed as an atlas sidecar `inside_tidal.json` read by `reach.outside` and the tiles. 26 named lines carry a
  water rule: show what each rule binds after removal before promoting. Not included: 15 lines ≥ 95% inside (15.3 km).
- Add the shore rule to RULINGS.md. Binding diff + maps to the owner before promote.

## Open with the owner
- Chambers Creek (360880075): the owner did not follow the explanation; explain again with the map, no change proposed.

Depends: T660. Size: L.

## Owner decisions 2026-11-11 (on `app-integration/T665-design.md` D1–D6)
- D1: drop the 371 shore cuts into freshwater rivers.
- D2: a creek flowing into a removed inside line is cut at its polygon exit too (dry run gives count and map first).
- D3: a line that leaves and re-enters is cut at the FIRST real exit (gaps under 25 m bridged).
- D4: **the 33 creeks with the owner's "limit is the mouth" tick keep the limit at the END of the mapped line** ("line end
  keeps it consistent"): no polygon-edge cut on them. The dry run writes the 33 with the distance between line end and
  polygon edge and a map, linked from OWNER-TODO for a later double check. This overrides the design's default.
- D5: Brae Slough (wholly tidal) is DROPPED from search, like the unnamed tidal lines (overrides the design's default).
  It is an owner-found name: the found-name row is kept on file and reported, not deleted.
- D6: the three creeks whose mouth is outside the polygon: no change.

## Owner answers 2026-11-11 on the dry run (`app-integration/T665-dryrun.md`, maps `T665-dryrun-map.html`)
- Cuts that leave nothing tidal: DROPPED. A creek is cut only where the piece below the cut goes tidal
  (`shore.A_CUT_THAT_MAKES_NOTHING_TIDAL_IS_NOT_MADE`).
- D4 WITHDRAWN ("just do the 33 cuts and then remove tidal part"): the 33 mouth-tick creeks are cut at the polygon exit
  like every other creek and the part inside goes tidal. Applied to the 59 further `mouth`-row creeks too (coordinator's
  reading, listed separately in the dry-run doc for the owner to object). Their `mouth` rows are superseded: proposed as
  `app-integration/T665-tidal-limits-mouth-rows.patch`, NOT applied (reviewed data).
- The eight main rivers with no limit of their own that would go tidal (Bella Coola, Mamin, Apple ×3, Kshwan, Sarita,
  Bedwell): HELD until the owner ticks each (`shore.HELD_FOR_OWNER`; map `T665-main-rivers-map.html`).
- Kept as a default the owner may reverse: a water with its own `split`/`whole` limit is never cut by the polygon
  (`shore.THE_OWNERS_LIMIT_GOVERNS_ITS_OWN_WATER`).
- Reference for waters with no listed limit — DFO's glossary, "tidal boundary"
  (https://www.pac.dfo-mpo.gc.ca/fm-gp/rec/definitions-eng.html): unless otherwise specified, the tidal boundary is a
  straight line drawn between the two most seaward points of land on either side of the mouth of a river or stream; a
  fishery officer may also mark one with a triangular sign; other specified boundaries are on the Pacific Fishery
  Management Area maps. The owner worries the polygon over-cuts where it runs up an estuary (the Pitt case): every cut
  whose polygon exit lies more than 100 m up a narrow channel is HELD too (second group on the main-rivers page), no
  change to the rule yet.

## Side build 2026-11-11 (the rebuild lane; `app-integration/next-rebuild-results.md`)
Built twice; the second build is the candidate (`data/generated/side/next-rebuild/atlas/full_next`), not promoted.
- **226 shore cuts** (build 27: 532), **215 lines tidal whole (67.7 km)**, 20.4 km of creek tidal below 184 cuts; 15
  strips (0.3 km) that were tidal are fresh (the cut moved down to the polygon exit). Parity, the reach run and the
  per-rule binding diff by place have no unexplained difference; one explained gain (Ucona River, 33 m, into the
  Strathcona Park waters rule).
- Fixed on the way, each with a test (`pipeline/tests/test_next_rebuild.py`):
  1. The atlas now cuts by the reach's own tidal set (`shore.reach_tidal`, `build.water_registry`), and the build stops
     if the registry it writes disagrees. The graph's reading had 70 sections against 124.
  2. A held line keeps the cut build 27 made (`HELD_KEEPS_ITS_CUT`, 30 lines): the polygon rule alone dropped 26 cuts
     on the 61 held lines, 19 of them with a tidal piece below.
  3. The rule judged the outlets of "the piece below the cut" on the line's lowest section; on a line not cut yet that
     is the whole lower river. The first build missed 6 cuts (5 tidal crossings of build 27 would have gone fresh) and
     made 10 for nothing. Now by where each edge leaves the line and joins the next (`outlet_edges`).
  4. The braid sweep would have stopped the build on Kainet Creek (held).
- **93 lines held** (61 + 32): 2 more up the channel (Cohilukthan, Deas), Kainet Creek (braid), and 29 rivers and
  creeks the precise rule makes tidal that the dry-run pages showed as "not cut" (Kitimat, Bella Coola, Kemano,
  Kwatna, Quaal, Khutze …). All 93 are exactly as build 27 has them (measured).
- Against the owner's dry-run map: 4 creeks into Fraser side channels are newly cut; the Brem River group (5 lines) and
  Kainet Creek are not cut. Listed as owner items in the results file.
- `--accept-boundary-loss` is needed for a net loss of 31 registry boundaries (77 `shore:` ids, 2 `length:`, 1 lake
  edge), not 363.
- `T665-orphans.json` is rewritten from the side build: 82 items change their tidal sections, Brae Slough alone becomes
  wholly tidal, no id moves.
- No `sidecars.py` writer for `inside_tidal.json`: the file is the build's alone (messages corrected).
