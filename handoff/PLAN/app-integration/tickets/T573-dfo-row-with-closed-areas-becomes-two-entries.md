# T573 — A DFO row that names closed areas inside its words becomes two entries (the Thompson)

Status: IN REVIEW — owner confirms / moves each closed area in the review app (entry B is saveable since 2026-11-10); the build side below waits on him

Owner 2026-11-10, on the Thompson (T572): "we need a way to represent this … it is like it should become 2 entries?"

## The shape
- ENTRY A, the open water: DFO's rules for the place, on the whole stretch its words name (Kamloops Lake outlet →
  the signs just below Gold Pan Provincial Park, 92.1 km).
- ENTRY B, the closed areas: one rule, "No fishing, year-round", on three stretches, each with DFO's own words:
  Deadman 3,616 m · Juniper 4,000 m · Ashcroft 2,342 m.
- NO NEW PRECEDENCE: the salmon chain is "narrowest first" (`deliver.salmon.chain_order`: tier, then the smaller
  extent). B is narrower than A, so B wins on its own stretches by the rule that already exists.
- Six cuts are not built: five new for B (Deadman upper signs, Juniper upper signs, Juniper lower signs, the
  Bonaparte's mouth, the Hwy 97C bridge) and A's Gold Pan signs, which the owner's own save proposed.

## What is built (2026-11-10), all of it a proposal
- `pipeline/regs/dfo_salmon/closures.py`: derives entry B from a stored row's exclusion list (`source_text.excludes`)
  — `python -m pipeline.regs.dfo_salmon.closures` writes `data/generated/review/dfo_closure_entries.json`. No fetch,
  no feed, no places file written; no existing place id changes (tests: `pipeline/tests/test_dfo_closures.py`, on
  the stored rows). Derived only where the words say no fishing / closed AND the list names stretches ("Deadman -
  from … to …"): the two Thompson wordings (the older one says only "except:" and takes it from the live wording's
  identical list).
- The review app: the Thompson place shows "This DFO row is two entries": A (the place itself, its proposal the
  whole stretch) and B (the rule, each area with its length, its two ends and DFO's words); the map draws B's
  stretches in red ("closed year-round (entry B, a proposal)"). `dfo.closure_view`; B's placement is in
  `dfo_fix_proposals.json` under `<place id>#closed`.
- The earlier four-open-stretches proposal (T572) is replaced by A = the whole stretch.

## NOT built — the build side, waiting on the owner
1. He accepts the six cuts (saving entry A's extent proposes Gold Pan; entry B's five need a place to be saved on).
2. Entry B becomes a location in `data/curated/dfo_salmon/places/region-3.json` (id `…#closed`, same water, its
   three pieces as its extent) — `dfo_salmon/entries.py` must seed it from `closures.derive` so a re-scrape keeps it.
3. Its rule ("No fishing, year-round": all salmon, take 0) must be in the rules the bundle and the live salmon feed
   read for that location (`dfo_salmon/typed.py` / `feed.py`; the feed is served by the worker — the other chat's
   code: no app/ or worker file was touched here, and whether one must change is not verified on a built bundle).
4. Then the atlas rebuild for the six cuts, the reach step and the bundle. Not started. The rebuild is the usual one
   run through `scripts/heavy.sh` (see the release runbook); it waits on step 1.
- Babine Lake (two wordings): "excluding … those waters within a 400 m radius of … Morrison Creek, Six Mile Creek" is a
  radius closure around tributary mouths, not named stretches: not derived; it needs its own decision.
- In the review app entry B cannot be SAVED yet (it is not a place); the owner reviews it on entry A's screen.

## ONE MODEL, TWO SHAPES (owner 2026-11-10: "i think we could model babine similar? … draw circles at the right radius")
An excluded area is a PIECE of entry B, keyed to its own item in the row's exclusion list. A piece is:
- `stretch` — between two cuts on a stream (the Thompson: "Deadman - from … to …");
- `circle` — a centre and a radius in metres (Babine Lake: "within a 400 m radius of … Morrison Creek, …");
- `line` — a line across a lake (Babine: "closed east of a line from Gullwing Creek to south shore"): named and
  listed, never placed here (a lake divided by a line is deferred).

THE STORED SHAPE (`closures.PieceGeometry`, validated wherever it is read; the same record is what a save will write):
```
{"key": "<12 hex: piece_key of DFO's words for the item>", "water_id": "6:babine-lake", "name": "Morrison Creek",
 "item": "<DFO's words it was curated for>", "on_item": "<registry water it lies on>",
 "kind": "circle",  "centre": [lon, lat], "radius_m": 400, "centre_from": "…", "confidence": "high|medium|low"}
{…same head…, "kind": "stretch", "points": [{"kind": "split"|"new", "split_id": "…"}, {…}]}
```
A stretch has exactly two ends and no centre; a circle a centre in B.C. and a radius, no ends; anything else is refused.
Placements live in `data/generated/review/dfo_closed_area_geometry.json` (proposals) until the owner accepts them.

BABINE LAKE, as derived from the stored rows: the live wording names 12 creeks (the older one 13: Pinkut Creek was
dropped), each a 400 m circle, plus the Gullwing Creek line. THE RULE ON ENTRY B IS WHAT THE WORDS SAY: the row
"exclud[es] … those waters" from the Babine Lake opening and says "Also closed east of a line …"; it names no season
and never says "no fishing". So entry B's rule reads "Closed" with no dates — whether that means no fishing for salmon
year-round there is the owner's reading to confirm.
CENTRES (proposals, from our own network: each creek's mouth, nearest the lake's outline): placed 7 — Twain, Tachek,
Five Mile, Four Mile, Big Loon (mouth on the outline: high), Morrison (298 m off) and Pierre (229 m off: medium).
NOT placed 6 — Six Mile, Pendleton, Hazelwood, Sockeye (no stream of that name reaches the lake in the registry),
Tsezakwa (its named line ends 2.3 km from the lake), Pinkut (11.6 km: and not on the live page).
A circle is drawn at TRUE GROUND RADIUS (`closures.circle_polygon`: a geodesic ring, within 1 % at 55° N in every
direction), over the lake, in the closed colour; clicking its line in the panel flies the map to it.

## ENTRY B IS NEVER FREE-STANDING (owner 2026-11-10: "circles should only show up when that entry is in the dfo page
too (if they remove it circles should go away)")
`closures.attach` joins placements to the row PER ITEM, by `piece_key` (DFO's words for the item, case and spacing
aside), against the items DFO's LIVE page carries for the water (`closures.live_keys`):
- item removed → only that piece goes "not live": kept on file, never drawn, never bound (Babine's Pinkut Creek today);
- clause or place removed → no entry B is derived / every piece of an older wording is not live;
- item restored → placed again from the file, nothing re-curated (waters never move, rules churn);
- item reworded → "changed, check again" (matched by its name, the geometry offered and flagged, never silently kept);
  a radius in the words that is not the stored radius → "radius differs", both numbers said.
Tests: `test_dfo_closures.py::test_entry_b_is_never_free_standing_item_removed_clause_removed_restored_reworded`.

## LATER (not built; each waits on the owner)
- EDITING a piece in the review app (move a circle's centre by a map click, change a radius, set a stretch's ends) and
  SAVING it: the app's one write is a mark, so this needs a mark that carries a `PieceGeometry` and its write-through
  into a curated file; today the placements are read-only proposals.
- THE BUILD: circles are not bound into the atlas / reach (a lake divided into closure areas is deferred; a polygon
  that straddles sections is marked, not cut). When taken up: the reach step would mark the lake's sections a circle
  touches and carry the circle as the closure's own geometry in the bundle; the app would draw the circle on the lake
  and answer "closed" inside it. Stretches integrate as in "NOT built" above. `attach`'s rule goes with them: a piece
  not on the live page is neither bound nor drawn.
- Derivation is one new module, `pipeline/regs/dfo_salmon/closures.py`, with no call site in the scrape, the typed
  feed or `entries.py` yet (T549's lane: see the note there).


## ENTRY B IS SAVEABLE (2026-11-10) — one mark per closed area, a curated file of its own
- THE MARK: scope `closed_area`, target `<water id>#<piece key>`, one check (P). "Confirm" carries the placement it
  confirms (`Mark.piece`, a `closures.PieceGeometry`: its water and key must be the target's); "Something's wrong"
  carries none and needs a note. No other mark may carry a placement. Its prints are DFO's words for the item.
- THE FILE: `data/curated/dfo_salmon/closed_areas.json` (`pipeline/regs/review/closed_areas.py`), a FILE domain
  (`closed_areas`) written in the request like sloughs / names / tidal: only confirmed areas, each with
  `confirmed_by` / `confirmed_on`. A later "wrong" takes its area out (the proposal shows again). The file does not
  exist until his first confirm. NOTHING ELSE MOVES: no places file changes by a byte and no existing mark's prints
  are touched (test: `test_review_closed_areas.py::test_confirming_a_circle_writes_the_file…` hashes the places tree
  before and after; `git diff --stat data/curated/dfo_salmon` is empty after the sandbox click-through).
- WHAT IS SHOWN: his confirmed placement wins over our proposal for the same item (`dfo.closure_view`); each area says
  "confirmed by you <date>" / "you said something's wrong" / "our proposal: not confirmed" / "not placed".
- IN THE APP (the place's "B · closed areas" block, `frontend/src/components/ClosedAreas.tsx`), per area:
  Confirm · Move it / change the radius (circle: click the map or drag its marker, type the radius) · Change its ends
  (stretch: each end is an existing cut by search, or a click on the river = a new cut resolved on the water and
  stamped in the mark) · Place it yourself (an unplaced mouth, the Gullwing line: two clicks) · Something's wrong.
  The draft is teal on the same map, drawn by the server's own rule (`GET …/closed-area-preview`, read only) before it is saved.
- THE SCHEMA grew: `kind: "line"` with `line: [[lon, lat], [lon, lat]]`; a stretch carries its NEW ends' definitions
  with it (`new_splits`, each a `marks.NewSplit`), so one record stands alone.
- Not counted in the place's "done": the areas are their own marks; the place's state is still its extent (entry A).

## THE OWNER'S ANSWERS, 2026-11-10
1. BABINE'S RULE: "babine is closed in the circles (no fishing salmon)". Entry B's rule on Babine reads "No fishing
   for salmon", recorded as HIS reading of DFO's word "closed" (`closures.OWNER_READINGS`, shown beside DFO's own word:
   `rule.dfo_words` / `rule.reading`). The derivation alone still reads "Closed".
2. REUSE AN EXISTING CUT: "if one is near one that already existed use the already existing one". NEAR = 200 m along
   the water's line (`dfo.NEAR_M`). Every new end shows its nearest existing cut and the metres; within 200 m the
   existing cut is offered ("use it instead") in the list and in the editor right after a map click.
   Thompson's new cuts, nearest existing cut on the river: Deadman upper signs 486 m (gauge 08LF033) · Juniper upper
   signs 2,810 m (Battle Creek confluence) · Juniper lower signs 1,190 m (Battle Creek) · Bonaparte mouth 868 m (CNR
   bridge) · Hwy 97C bridge 3,209 m (CNR bridge) — none within 200 m, all stay new cuts. The OTHER reading of the
   Juniper upper signs (1.5 km below the park) lands 37 m from the existing Battle Creek confluence cut, so that
   candidate IS the existing cut. Babine's centres: nearest existing cut 2 – 23 km away, except Pinkut Creek (its fish
   fence cut is 70 m up the creek: the centre stays on the shore, as he asked in 3).
3. THE MOUTHS: "where stream meets lake (not including the connecting stream)". `pipeline/regs/dfo_salmon/
   closure_mouths.py` (`python -m pipeline.regs.dfo_salmon.closure_mouths` rewrites the circle proposals): the centre is
   where the creek's own channel meets the lake polygon's outline, from the source network; the in-lake construction
   line is dropped first. Re-derived 2026-11-10:
   high — Morrison (moved 11 m), Pierre (64 m), Twain (0), Tachek (773 m: the old centre was the connector's end),
   Five Mile (0), Four Mile (0), Big Loon (0), Pinkut (new);
   medium — Tsezakwa: it does not reach Babine Lake; its channel enters Nilkitkwa Lake (2.1 km down the outlet), the
   centre is on that lake's outline (BC Geographic Names' point for the creek is 60 m from it);
   low — Pendleton: no stream of the name; BC Geographic Names has Pendleton Bay; the largest UNNAMED stream meeting
   the shore within 1.5 km of the bay point (order 3, 590 m from it);
   not placed — Six Mile, Hazelwood, Sockeye: no stream of the name (or Sixmile / Heazlewood) in the network within
   6 km, none in BC Geographic Names at the lake; what was tried is shown on each ("Place it yourself").
4. JUNIPER'S UPPER SIGNS: both readings are marked on the map and listed as buttons ("4 km above the lower signs":
   a 4.0 km closed area · "1.5 km below Juniper Beach Park": the Battle Creek cut, 1.2 km); he picks one or clicks
   his own. (`alternatives` in the proposals file.)
5. THE WORDS WERE CUT OFF at "except:" — DFO keeps the excluded items as a list apart; they are put back in the
   sentence (`checklist.full_words`, served as `verbatim.full`): the Thompson and Babine read whole.
6. PINKUT CREEK: "we should have both mapped out in case they go back to old wording". GENERAL RULE: every excluded
   item of ANY stored wording of the place is listed, placed and reviewable; only live ones take effect. An item only
   an older wording names is under "Not on the live page — kept ready", drawn grey and dashed in the review app only,
   never bound, never in the app. Its mark does not depend on liveness (the prints are DFO's words for the item), so
   when the words return it is live again with no re-curation. Thompson's older wording names the same three areas.
   Tests: `test_dfo_closures.py::test_an_item_only_an_older_wording_names_is_reviewable_not_live_and_live_again…`,
   `test_review_closed_areas.py::test_prints_are_dfos_words_from_any_stored_wording…`.
