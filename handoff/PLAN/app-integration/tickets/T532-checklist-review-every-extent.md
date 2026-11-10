# T532 — Checklist review for every extent (DFO places and the regulations' own extents), one interface

Status: IN REVIEW — owner looks

Owner 2026-11-09, on the round-4 mockup (shots: scratchpad t525-r4-*.png; copied to shots/T532-mockup-*.png): "definitely
better. need to make it play nice with entire list + future items when dfo adds new ones. also should have similar quality
(ideally the same interface) for the normal non dfo extents too because eventually we still need to manually go through all
of those items."

## The screen (from the mockup)
- Header: place name, source (DFO region / synopsis page), progress "12 of 287", prev/next, filters (unchecked / has ✗ / ?).
- Big map: the extent now in ONE colour, pending (saved, not built) dashed; numbered pins = rows.
- Rows = the CLAIMS: quoted source phrase → what our extent does (few words) → ✓ / ? / ✗. Hover/tap a row = its pin.
- One decision: "All correct" (enabled when every row is ✓ or ticked) / "Something's wrong" (pick rows + note). After a save:
  one status line. Nothing else visible by default (no ids, no older-wording lists, no legends of 6).

## Rows are GENERATED, never hand-written (scale: all DFO places, new DFO items, every regulation extent)
- From the extent (one model for both sources: `Extent` ops, items, splits/cuts, tributaries, within/outside areas,
  undrawn parts, rest/siblings): water(s) · from (cut) · to (cut / mouth / source) · each extra piece · tributaries ·
  each exception (outside areas/items, closures named in the words) · area membership.
- Each row is paired with the phrase in the source text that names it (cut labels/quotes, `extent_text`,
  `unresolved_locators`, DFO `source_text`); state: ✓ built and matches, ✗ everything else (owner 2026-11-09: "always
  add ✗ when unsure — data must be rock solid"; there is no ?): a cut saved but not built ("not built yet; check again
  after the rebuild"), the words say something the extent does not (an exception, a tributary call, a place with no cut).
- Words that no row covers are their own row ("words not checked: …"), so nothing hides.
- New DFO items (live queue) and changed wording arrive as unchecked places; a check is stale when its words or extent
  change (re-check, never silently kept).

## One water at a time (owner 2026-11-09)
"a water has all of the locations for it together … we want to do an entire river / waterbody at one time". The queue and
prev/next are grouped by water: every place on a water (its DFO places AND its regulation extents) before the next water;
progress per water ("Chilliwack River: 3 of 7") and overall; the water's map stays while stepping through its places.
Test: the queue order never leaves a water before all its places are listed.

## Both sources, one interface
- DFO places (T525 data path, marks, write-through, all R1–R7 guards kept).
- Regulation extents (synopsis rules, `pipeline.atlas.reach` bindings; the review app's existing extents tab): same row
  generator, same screen, same save (a mark), same status line.
- One shared module for rows (Python, with the reach builder: AGENTS 16 — the review app never re-implements a resolver).

## Done when
- Every DFO place and every regulation extent opens in the checklist; counts and filters work over the whole list.
- Tests: row generator over every curated place (no crash, every source phrase covered or listed as unchecked), a planted
  missing exception / tributary mismatch / pending cut each yields ✗ or ?, stale-on-change.
- Shots of the 3 mockup places + 2 regulation extents, owner look.

## Owner 2026-11-09 (later): a whole water at one time
Review a whole river or waterbody at one time, never jump between waters: the queue and prev/next are grouped by water
(every place on one water before the next; in phase 3 a water's DFO places and regulation extents together), progress
per water ("Chilliwack River: 3 of 7") as well as overall, and the water's map stays loaded while stepping through its
places (rows and pins change, the map stays). Pinned by `test_the_queue_never_leaves_a_water_before_all_its_places`.

## Phase 1 — the row generator (2026-11-09)
- `pipeline/regs/review/checklist.py`: `rows(source, extents, …)` → numbered rows {kind, quote, ours, state, reason,
  anchor}; kinds water / where / from / to / also / area / tributaries / except / unchecked. Pure: the backend hands in
  a `Context` (cut labels, built cuts = the served atlas's, pending = in the cuts file or proposed by a mark but not
  built, water names, pins). `dfo_source` / `rule_source` (the row's name with its ✱ "Incl. Tribs", `extent_text` /
  `unresolved_locators`, else the verbatim's place phrases); `digest` (words + extent, pieces as a set) → stale.
- States: ✓ built and named by the words; ? saved / not built, or words no row checks; ✗ an exception not cut out, a
  tributary call against the words (or tributaries walked with no word for them), a named end with no cut, a carve-out
  the words never state, the whole water where the words name a part, a cut no word names.
- A check is a mark: `marks.Checklist` {digest, rows (n, kind, state, ticked, wrong)}; its one check is P; "all
  correct" needs every row ✓ or ticked, "wrong" names its rows. POST /api/marks refuses a checklist whose digest is
  not the place's now.
- Backend `curation-review/backend/checks.py` + GET /api/checks/{queue,progress,dfo/<id>,rule/<entry>/<rule>}: the
  queue by water (a water = its registry items; DFO places then regulation rules), filters keep a whole water.
- Counts (served atlas, 2026-11-09): DFO 332 places → 1,085 rows ✓ 709 · ? 134 · ✗ 242; regulations 3,354 rules →
  ✓ 10,018 · ? 178 · ✗ 503 (CI, cuts file as built: `test_review_checklist.py` prints them).
- Tests `pipeline/tests/test_review_checklist.py`: every curated DFO place and every rule with extents (no crash, every
  place phrase quoted by a row or listed as unchecked), planted mutations (exception dropped, tributaries flipped,
  pending / unknown cut, a named end with no cut), digest stale on words / extent, mark rules, queue by water.

## Phase 2 — the DFO screen (2026-11-09)
- The DFO tab (`#dfo/<id>`) is the checklist (`frontend/src/components/CheckReview.tsx`, logic `src/checklist.ts`, run
  by `node --test tests/checklist.test.ts`): header with the place, "N of M checked" over the whole list, "<water>: i of
  n", ‹ pos of total ›; filters all / unchecked / has ✗ / has ? (a filter keeps whole waters); a water picker and a
  search. Big map (the existing MapPanel): the extent now in one colour, a save not drawn yet dashed (`pending`),
  numbered pins per row (`pins`, tap a pin = its row); the view fits the first place of a water and stays while its
  other places are stepped (one MapPanel per water).
- Rows → "All correct" (enabled when every row is ✓ or ticked) / "Something's wrong" (pick rows + note → a wrong
  mark, problem from the rows' kinds) → one status line ("Checked <date>: all correct · 6 rows, 1 ticked by eye", or
  stale: "check it again"; plus the extent save's line when one waits). "Change the extent instead…" opens the T525
  place screen (`DfoLocation`, Change picked, ← back) — every guard R1–R7 is its own; a check never carries a fix and
  always sends `extent_before`.
- A place whose own save has not landed yet (waits for its cut / the write-through) is HELD: a check would supersede
  that save, so neither button is offered (and POST /api/marks refuses it); the rows show the saved pieces with ?.
  The digest is the curated extent's, so a landing makes the check stale.
- New DFO places on the live page arrive as unchecked rows (the queue reads the live DFO rows); historic rewordings
  ride their place (as before), the checklist marks the place only.
- The round-3 queue/list screen is gone (its place screen stays as the change editor).
- Shots: `shots/T532-dfo-chilliwack.png`, `-dfo-forks.png`, `-dfo-chemainus.png`, `-dfo-chilliwack-wrong.png`,
  `-dfo-change.png`.
- Tests: `test_curation_review_screen.py::test_a_checklist_is_saved_only_on_the_rows_the_place_has_now` (stale digest
  refused, open rows refused, held place refused, P-only mark kept, queue by water, nothing under data/curated).

## Phase 3 — regulation extents, same screen (2026-11-09)
- Entries → "Extents checklist" (`#checks/<entry>#<rule>` or a DFO place id): the same `CheckReview`, the same rows
  (`checks.rule_rows`: the row's name with its ✱, `extent_text` / `unresolved_locators` / the verbatim's place phrases,
  the builder's tributary call), the reach drawn in the one colour (`rule_resolved_reach`, capped at 3,000 sections),
  the save a RULE mark (P only; awaiting-build entries are refused by the existing gate, in its own words).
- Sources picker: both / regulations / DFO. ONE WATER ACROSS BOTH (`checks.one_water`): places sharing a registry
  water are one water (the DFO Chilliwack/Vedder places and the book's Chilliwack rows together, DFO first); a zone
  table with no water of its own is its own group.
- Owner look on phase 2 folded in: a locator is one phrase ("100 m from the confluence of … Slesse Creek" stays with
  FROM; a closing "downstream" is TO's own); the map's old chrome is off in the checklist (status line, reach picker,
  6-entry legend; the stale-marks toast is a count in the filter bar, orphans stay loud); a pin sits at its own anchor
  (a cut at its point, a natural end at the water's mouth / source), water and tributary rows have no pin.
- Counts over the whole list (served atlas, 2026-11-09): DFO 287 places, rows ✓ 710 · ? 132 · ✗ 241 (places by worst:
  ✗ 182, ? 24, ✓ 81); regulations 3,399 rules, rows ✓ 10,009 · ? 173 · ✗ 512 (✗ 391, ? 121, ✓ 2,887). All unchecked.
- Shots: `shots/T532-regs-campbell-r5.png` (between two cuts, all ✓), `shots/T532-regs-campbell-r4.png` (tributaries
  only, except the Quinsam; the Strathcona Dam cut no word names: ✗); the DFO shots re-taken with the fixes above.
- Tests: `test_a_regulation_extent_is_checked_with_the_same_rows_and_a_rule_mark`,
  `test_a_locator_is_one_phrase_and_a_closing_direction_is_the_ends_own`, `test_one_water_joins_both_sources_…`.

## Owner decisions / notes
- A ✗ is "look at this", not "wrong": e.g. tributaries walked with no word for them, a carve-out the words never
  state, a cut no word names (the rule's words are elsewhere in the row). Tick it by eye or report it.
- A place whose own save has not landed is held (no check offered) until the write-through / rebuild lands it.

## Older wordings + no "?" (owner 2026-11-09)
- "it should save extent to all variants of location text" — and "we cant blindly assume variants … they must match and
  that requires human check too". The checklist (and the T525 editor's save, R5) lists each older wording of the place
  (`dfo._groups()`) with its own DFO words, what differs from the shown place's words marked (`checklist.wordDiff`), and
  its extent now in words; each has a "same place" tick that starts UNTICKED (`components/OlderWordings.tsx`).
- A ticked wording gets the check, and the shown place's extent when its own differs (`dfo.fix_taking`: the shown
  pieces on existing cuts + the tributary call it binds with, with its own `extent_before`); its checklist is the rows the
  owner checked, its digest its own as it will bind (`checks.digest_as`), refused per wording when it moved or while a
  save of its own waits to land. A wording holding a field a fix cannot say (R4) or that cannot take the extent is not
  tickable and is refused by name if sent. The line under them names exactly what the save writes ("Also saves on 2
  older wordings: …; 1 takes this extent: …"). The server writes only wordings in `targets` AND listed in `members`
  (`dfo.marks_for_group`).
- Unticked wordings stay unchecked: the place they ride carries `older_unconfirmed` in the queue ("N older wordings not
  confirmed" in the header) and stays under the "unchecked" filter until each is checked.
- No ? state: a row is ✓ only when positively confirmed, every other row is ✗ (`checklist.STATES == (ok, bad)`; the "has
  ?" filter is gone; `marks.CheckedRow` takes ok / bad).
- Data (2026-11-09): 15 groups, 18 older wordings; 2 groups hold a wording whose extent differs (both can take it).
- Tests: `test_review_dfo.py::test_a_save_on_older_wordings_writes_each_ticked_one_and_gives_a_differing_one_this_extent`,
  `::test_an_older_wording_holding_what_a_fix_cannot_say_is_refused_by_name`, `::test_a_check_on_older_wordings_carries_each_ones_own_digest`,
  `test_review_checklist.py::test_a_pending_or_unknown_cut_is_never_ok` (pending → ✗), `frontend/tests/checklist.test.ts`.

## Owner 2026-11-09 (after trying it): earlier work counts, lead with the fix, the water in sight
"the review app doesnt show how many I already did unless they are gone? the ui is unintuitive too. we should for a river
show all historical boundaries too and have a nice way to confirm if the same location different wording items are
correct (have them grouped then you confirm them to start) then we should be able to combine other items too". Also:
P1 quick confirm and Tidal are done: off the bar.
- Nav: Entries → "P1 quick confirm", the top "Tidal" tab and DFO → "Tidal" are off the bar (`TopNav.tsx`); the views
  and routes stay (`#p1/`, `#tidal/` still open).
- WHAT COUNTS AS DONE (`checks.DONE_STATES`): a check (`checked`), a wrong check (`wrong`), and `earlier` — "a VERIFIED
  mark made before the checklist (it carries none) that the marks' own staleness still calls verified". DFO: the queue's
  `verify == "verified"` (`dfo._verify` = `marks.status`, a landed fix holding); a regulation rule: `marks.status` on
  the entry's prints now says verified AND its P answer is y. No second staleness rule. An earlier wrong / unsure /
  stale mark stays not done and the status line says what it was. Header: "80 of 287 done · 207 left"; status line:
  "Verified earlier (2026-11-02), before the checklist."; filter chips: to check / needs extent / verified earlier /
  changed / has ✗. An older wording verified earlier counts as confirmed (`older_unconfirmed`).
- A place with no extent (DFO state unbound / needs_cut, or a "no extent" row) LEADS WITH THE FIX: one line of what it
  needs ("No extent yet: this place needs two points on Fraser River."), its one proposal (the editor's own:
  `proposalOf`, offered as the editor offers it) in words and violet on the map, "Use this proposal" (opens the T525
  editor with it in the draft: `DfoLocation initialDraft`) / "Set the extent…"; "All correct" is not the primary button
  there. Nothing is applied here: the editor's save is the only write, every guard its own.
- THE WATER'S PLACES above the rows (`GET /api/checks/water?place=`, `checks.water`): every place on the water, older
  wordings and no-longer-live ones too (and its regulation extents under "both"), each with its words, its extent in
  words and one chip (done / verified earlier / reported wrong / needs extent / changed / has ✗ / to check); click =
  open. Same places sit under one heading ("same place, 3 wordings"), grouped by data that exists (`checks.same_place`:
  an older wording of the place, DFO's `duplicate_of`, the identical extent). The map draws every DFO extent on the
  water faintly (`/api/checks/water/reach`, one reach per distinct extent, tributaries not walked), the shown one
  strong; hovering a place in the list draws its extent darker. Regulation extents are listed, not drawn faintly.
- "These N wordings are the same place" (`OlderWordings`, `checklist.tickAll`): ticks every older wording that can be
  ticked (they still start unticked; one save, the existing group save). "Same place as…" on a no-extent place: pick
  another DFO place on the water that has an extent (DFO's own rewording link first) → the editor opens with that
  place's pieces as this place's draft; the owner confirms and saves there.
- The row for words no row covers reads "no row checks these words · read DFO's words against the map, then tick"
  (still ✗ until ticked).
- Tests: `test_review_checklist.py` (earlier marks, the P guard, progress, grouping, the row's words),
  `frontend/tests/checklist.test.ts`. Shots: `.playwright-mcp/t532-ux-1.png` … `-3.png`.
- Not done: a group save across places that are only linked by `duplicate_of` / identical extent (the server writes
  a group only for `dfo._groups` members: "Same place as…" covers them one at a time); faint geometry for regulation
  extents.

## Owner 2026-11-09, round 2: the editor INLINE, and words that say what happens
"when i click use this proposal … it just opened up a completely different page … hard to follow … view extent there
then you can edit it if needed … show the original text"; "it should be more obvious what the text means … need to know
when it saves".
- No page swap: every way into the editor opens it INSIDE the side panel (`DfoLocation embedded`), under "This place ·
  DFO's words"; the checklist map stays and draws the draft in teal (`EditLayers`: the server's `/extent-preview` reach
  when every point is a cut that exists, the points labelled either way; fits to the draft once). The save is the
  editor's own (same mark, R1–R7, T and R still answered). "open the full editor" keeps the old page one link away.
- A place with an extent says it in words with "Edit this extent (draft, nothing saved)"; a click on a where / from /
  to / also / tributaries row opens the editor (it is not focused on that row's piece).
- DFO's rule lines, the extent now and the proposal's details are in a closed "more" fold; new cuts in their own fold.
- Words: "Preview this proposal on the map (nothing saved)" · "Draw it myself" · "Copy the extent from another place
  on this river" / "Preview its extent here (nothing saved)" · "Save this extent" (the only button that writes an
  extent; off until the draft changes something) · "Discard my changes" · "Confirm: the extent matches DFO's words
  (saves)" · "Report a problem with it" / "Save the problem report".
- One state line at the top (`checklist.placeState`), exactly one of: "Nothing saved yet" · "You are previewing a
  draft: not saved" · "Saved (date) — waiting to be drawn on the map" · "Saved and on the map" · "Confirmed by you
  (date)" · "Verified earlier (date)" (a mark holds a date, not a time); after a save "Saved: <the extent>" stays
  until he moves on. Leaving with an unsaved draft asks once. Under the header: "1 Look at the extent on the map
  against DFO's words · 2 Fix it if needed (preview, then save) · 3 Confirm it".
- Not done: a draft that names a NEW cut is shown as its points only (the stretch draws after the rebuild builds the
  cut); a row click does not focus its piece; T and R are still two answers before the save.

## Owner 2026-11-09, round 3 (inline editor)
- "if you save extent you assume it is correct?" — the inline editor asks no T / R. A mark needs both answered
  (`verdict_from_answers`), so they are sent as y and the note says so: "T and R not asked: implied yes by the owner
  saving this extent from the checklist". The full editor page keeps its questions; "Report a problem with it" is how
  the words or rules are called wrong.
- "could we make this mock up a new section … even just a mock one made in app" — a draft of ONE piece `between` two
  points that names a cut not built yet draws an APPROXIMATE stretch: the water's own lines on the map walked the
  shortest way between the vertices nearest the two points (`approxStretch.approxBetween`, in the app, never saved),
  dashed, labelled "approximate: the exact stretch is drawn after the next rebuild" in the key and the panel (with
  its length), or the reason it could not be drawn. Not done for upstream_of / downstream_of with a new point: the
  client's lines carry no flow direction to pick the end from.
- "if proposal is on an existing cut then it should highlight that cut" — a proposal / draft point that is an existing
  cut rings that cut's own marker (violet for the proposal, teal for the draft); only a new cut gets a marker ("new
  cut, not built").

## Owner 2026-11-09, round 4
- Every new cut of a proposal / draft has its marker, whatever its anchor: its position is the server's own
  (`api.dfoAnchor`, `newCutChecks`) — an offset from a cut ("23 m below … dyke gates") has no coordinate of its own.
  Only when the server gives none is it put at the base cut, labelled "position approximate". The approximate stretch
  then draws for these too.
- After "Save this extent" or "Confirm…": "Saved: …" shows, then the next NOT-done place on the same water opens
  (`checklist.nextAfterSave`, list order, wrapping); none left → "<Water>: all N places done" with one button to the
  next water with work. A problem report or a refusal stays on the place.
- WHAT COUNTS AS DONE, added: `saved` — "the owner's extent save is in force and waits only for its new cut to be
  built or the write-through to apply it" (`dfo.saved_status` waits_cut / not_landed). Counted in done (overall, per
  water, the list chip "saved, waits build"), filter chip "waiting on a build N", state line "Saved by you (date):
  done — waiting only for the new cut to be built". The mark and its verdict are untouched. Once nothing waits, the
  ordinary rules judge the place again.

## Owner 2026-11-09, round 5: "i dont want to have to review the items 2 times??"
- THE RULE (`checks.saved_kind`, derived from the save itself — the mark in force carries an extent fix, no checklist,
  not a defer, neither T nor R answered n — against the place now; no mark is rewritten):
  - `saved` (DONE): "the place holds the extent he saved (fix == the curated extent), or the save waits only for its
    new cut" — "Saved by you (date): done — built as saved" / "… waiting only for the new cut to be built". Whether
    the save used a new cut or only existing ones. Never asked again; no "earlier mark was: not sure".
  - `stale` (comes back as changed): "the marks' own staleness says the place moved since (its words, its rules, or —
    after his save had landed — its extent is no longer the one he saved)". The status line says what he saved and
    what the place holds now.
  - `unapplied` (NOT done, never re-asked): "the extent is not the one he saved, every cut it names is built, and
    nothing moved since the mark: the save never reached the place" — red state line and chip "saved, not applied",
    its own filter chip when there is one, reported through `failed(...)`.
- Real data 2026-11-09: DFO 92 of 287 done (saved 83: 82 built as saved, 1 waiting on its cut; verified earlier 9),
  2 changed, 0 not applied.
- Tests: `test_review_checklist.py::test_the_owners_extent_save_is_a_review_…` (with the mutation cases),
  `::test_a_place_reads_by_the_owners_extent_save_…`, `frontend/tests/checklist.test.ts`.

## Owner 2026-11-09, round 5 addition: read a same-place group together; refute a grouping
"menu here is hard to see the same wording things at same time to verify also how would i refute same wording".
- DONE (part 1): the water list has a "taller list" / "shorter list" button (62 % of the panel, remembered) and a
  selected row scrolls its whole group into view; the panel shows "Same place, N wordings — read them together":
  every wording of the shown place's group stacked, full text, each with live / no longer live, the dates DFO's page
  carried it, its extent in words, why it is grouped, and a link to open it.
- NOT DONE (part 2), needs its own change: "Not the same place: take this wording out of the group". The store has
  no such decision. What exists: a `variant_of` mark ATTACHES a wording (`proposals` writes `duplicate_of` +
  `duplicate_confirmed: true`), and `review.dfo.groups` already keeps a wording apart when its file says
  `duplicate_confirmed: false` — but nothing writes that, and it only undoes a `duplicate_of` link. Needed:
  (a) a mark field (e.g. `not_variant_of: <location>`) on the DFO scope, with its model rules and refusal words;
  (b) `proposals` + the DFO write-through writing `duplicate_confirmed: false` (and reversing it: a later
  `variant_of` mark already sets it true); (c) `review.dfo.groups` / `places` honouring it for wordings grouped by
  the alike-wording matcher too, not only by `duplicate_of`; (d) `checks.same_place` reading it, so the identical-
  extent grouping does not put the wording straight back; (e) the button, its "saved" line, tests.

## Owner 2026-11-09, round 5 additions 2 and 3
- A same-place group in the water list is ONE CLOSED BOX: border and background around exactly its rows, the header
  inside at the top ("These 2 wordings are the same place"), clear space above and below.
- Every DFO row of the list says "live" or "no longer live, last seen <date>" (`WaterPlace.last_seen`).
- THE LIST'S COVERAGE, measured 2026-11-09. Alouette: 6 DFO wordings in the files (3 live, 3 no longer live), 6 in
  the list — none dropped. Whole list: 332 water-bound DFO wordings, 301 shown in some water's list, 31 not:
  19 no-longer-live wordings on waters with NO live wording (the whole water is off the queue: `_dfo_places` keeps a
  dormant place only when its water is still on the live page) and 12 lake sub-areas (hidden everywhere as "lake
  split later", owner 2026-10-28). No DFO location holds more than one wording of its own. To show the 19: let
  `_dfo_places` / `checks.water` take dormant-only waters (a queue filter "no longer on the live page"), an owner call.
- NOT DONE, with the refute decision (one change): "Add a wording to this group" / "Same place as…" on a list row.
  The attach half exists (`variant_of` mark → `duplicate_of` + `duplicate_confirmed: true`, the full editor's "more
  about this place" → attach), but it cannot be undone in the app until the refute decision exists, and the owner's
  rule is that a grouping he makes is reversible. Build both together: the mark field for "not the same place", the
  write-through for it, the group readers honouring it, then the two buttons on the group box.

## Round 6 (owner 2026-11-09): the grouping is his decision; a save being applied is never "not applied"
- BUG FIXED: his Alouette save (`…:above-216th-street-including-the-n`, two pieces) read "NOT applied … reported as a
  fault" although commit 044533d84 applied it. Cause: the DFO write-through applies in the BACKGROUND seconds after
  the mark; a place read in that window had fix != curated with every cut built, which round 5 called `unapplied`.
  The comparison itself was right (two pieces, items [] vs no item_id, tributaries: pinned by
  `test_the_owners_two_piece_alouette_save_reads_as_built_as_saved`). Now `checks.saved_kind` asks the write-through
  (`apply_outcome`): saving / waiting / just saved → "Saved by you (date): done — being applied to the map data now";
  `unapplied` only when it REFUSED the save (its reason shown) or holds no record of it (an earlier session's save).
  The screen re-reads the place when the write-through's word changes. The false failure was only an on-screen
  banner line and a console line in his browser (`failures.ts` keeps them in memory): nothing was stored; the line
  is now cleared when the place stops being `unapplied`.
- TAKE A WORDING OUT / PUT ONE IN (the change written above, built): mark field `not_variant_of` (DFO only, its one
  check T, never with `variant_of`); its write-through (`proposals._apart_proposal`) sets the entry model's own
  CONTESTED state — `duplicate_confirmed: false`, `duplicate_of` cleared — so the wording stands alone and needs its
  own extent; `review.dfo.group_why` keeps a contested wording apart from either side; `checks.same_place` never puts
  it back by an identical extent. Putting in is the existing `variant_of` mark (`duplicate_of` + confirmed true),
  which also undoes a take-out; a take-out undoes an add. Both send `extent_before` (T525 R3 — the full editor's
  older "attach" button does not, and is refused by the server since T525: not fixed here).
- Buttons (the read-together block): per wording "Not the same place: take this wording out of the group (saves)";
  "Add a wording to this group" (pick from the water's rows) → "Add it: same place (saves)"; on an ungrouped place
  "Same place as…" → "Group them: same place (saves)"; a taken-out wording says "Your decision: … stands alone. Put
  it back below to undo." The state line says "Saved just now: …" with how to undo.
- WHOSE CLAIM, on every grouped row: "your decision: linked as the same place" (`duplicate_confirmed` true), "our
  proposal: matched as a rewording" (an unconfirmed `duplicate_of`), "our proposal: an older wording, its words alike"
  (`review.dfo.groups`), "the identical extent" (a fact). DFO itself publishes no rewording link: the earlier text
  "DFO lists it as a rewording" was wrong and is gone from the checklist.
- A mark that takes a wording out or puts it in becomes that wording's mark in force (as the editor's attach always
  did): a wording that was done by an earlier save is then judged by the new mark (taken out: to check, it needs its
  own extent; put in: it rides the other place's extent and reads "Verified earlier").

## Round 6 additions (owner 2026-11-09)
- ONE EXTENT FOR EVERY WORDING OF THE PLACE ("saving one extent in the group didnt handle the reworded version too"):
  the inline editor's save is "Save this extent for all N wordings of this place" (primary; the existing group save:
  one mark per wording, each through the write-through; the words under it name the wordings, say when one's own
  extent is REPLACED, and that saving for all counts as his confirming the grouping) or "Save for this wording
  only". A wording that cannot take it is named with why; one taken out of the group is not a member. The save line
  adds ", and the same extent given to 1 other wording of this place: …". A half-done group: the wording without an
  extent leads with "Give this wording the same extent as “…” (saves)" — one click, the editor's own mark.
- ONE SCROLL for the place panel (words, group block, rows, older wordings, buttons); no nested scrollers, nothing
  sticky at the bottom (the inline save bar too); the state line sticks to the TOP; "taller list" is 45 %.
- THE ALLCO FENCE CUT WITHDRAWN at its source: `data/generated/review/dfo-split-hunt.json` — the hunted cut
  `allco_fence` is removed and its three Alouette locations name the curated cut
  `alouette_river__alouette_boundary_signs` (owner: "we have boundary sign cut already for alco"). Regenerated with
  `python -m pipeline.regs.dfo_salmon.suggest` into a scratch folder; ONLY its four Alouette rows were carried into
  `dfo-suggestions.json` (the "2 signs" wording now proposes "downstream of Alouette boundary signs", no new cut; the
  three wordings he has since bound drop out). A full regeneration would also change 16 other rows (places saved
  since the file was written) — not done here; the two .md summaries are not regenerated.
  STILL THERE: the fence point rides his earlier unsure mark on the "two signs" wording (marks.jsonl, left alone), so
  `marks.proposals` still lists `alouette_river__allco_park_boundary_signs_allco_hatchery_fence` as a proposed cut
  (it is in neither the curated cuts nor the built atlas). `proposal_overrides.json` can only set a confidence.
  FOLLOW-UPS (no work now): (1) an owner "withdrawn" call in `proposal_overrides.json`, read by `marks.proposals` and
  `proposals.build`, so a superseded mark's cut is never built; (2) a new-cut proposal within N m of a curated cut on
  the same water flagged "an existing cut is here" (count not taken).
- A PLACE DONE BY HIS SAVE ASKS NOTHING ("i cant accept it ?"): no tick list, no "Also saves on…", no Confirm button —
  only the done line, "Edit the extent (draft, nothing saved)" and "Report a problem with it". ONE RULE for a group's
  other wordings (`checklist.groupStillNeeds`): each wording is done by its own state (saved / verified earlier /
  checked); the ones that still need him are named as links ("1 other wording of this place still needs you — open
  it: …"), else "Every other wording of this place is done too."
- THE NEXT WATER, always there ("is there a button to go to next river?"): "Next water with work left →" in the screen
  header from anywhere (`checklist.nextWaterWithWork`); when the open water has none left, "<Water>: all N places
  done." with "Go to the next water with work left: <Water>" under the water list's header and in the done block.

## Round 7 (owner 2026-11-09 / 11-10)
- DFO'S HEADING: "Morice River (including tributaries)" is shown again (title, water list header) and the tributaries
  row reads it. The qualifier is dropped at the SCRAPE: the places files keep the bare name and the water's
  `tributaries` flag (56 waters true, 1 false, 104 none); `checklist.dfo_heading` puts it back from the flag — DFO's
  meaning, not its bytes. No stored name was changed.
- A PIECE THAT BINDS NOTHING SAYS WHY (`dfo.dead_why`, in the preview): his Forks piece on Bulkley River ran between
  the MORICE's confluence cut and a Bulkley cut, so nothing lay between them on the Bulkley; the message names the
  stray cut, the water it is on, and the nearest cut on the piece's own water ("Morice River → Bulkley River").
- THE FORKS PROPOSAL (ours, `dfo_fix_proposals.json`, both the Morice and the Bulkley wording): the whole zone as two
  pieces on EXISTING built cuts — the Morice's last 100 m, and the Bulkley from 100 m up the upper Bulkley to 1 km
  below the confluence. The distances are DFO's own ("starts 100 meters upstream of the confluence and ends
  approximately 1 kilometre downstream"). A place that already HAS an extent now shows its proposal in the panel
  with "Preview this proposal on the map (nothing saved)"; a proposal of several pieces previews every piece.
- HIDE DONE (default on, remembered) and EASIEST FIRST (remembered), in the list and in a water's list; the count
  says how many done are hidden. THE ORDER (`checks.EASE`, the word on each row): confirm only → proposal ready
  (high confidence, built cuts) → one click (a twin of a saved wording) → proposal to check (medium / low, or ours
  from his note) → changed → new cut needed → no proposal → has ✗ → fault. Waters: least total work first (the sum of
  their not-done places' ranks, then how many are left); a water is never split; "Next water with work left" follows.
- HIS PROBLEM REPORTS since 2026-11-09 (the marks store): Sheldens Creek → T571; Thompson River → T572. No report on
  the Morice reached the store: "Report a problem with it" is two steps and the save box was easy to miss — it is now
  scrolled into view and says "Step 2 of 2 — nothing is saved until 'Save the problem report'".
- A SAME-PLACE LINK THE FILE ALREADY HOLDS IS APPLIED: his Serpentine "Same place as…" (11-10) was written by the
  write-through (`duplicate_of` + confirmed true in region-2.json), but the proposal builder re-offered the now
  edit-less proposal and `apply` / fast-fail read it "refused: not applicable: no edit" (1 NOT LANDED). An edit-less
  variant proposal is now counted applied, as the take-out already was; fast-fail passes on the real store.
