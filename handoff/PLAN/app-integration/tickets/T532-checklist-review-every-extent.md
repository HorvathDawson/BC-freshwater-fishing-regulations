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
