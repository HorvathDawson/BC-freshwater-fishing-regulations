# T755 — Review app: what the owner's overnight marks said it could not do (2026-11-15)

Status: IN PROGRESS (review-app lane, 2026-11-15: code in; left (Claude): main chat applies the targeted suggestions merge, the owner restarts his copy and re-marks the places listed below)

Source: a read-only triage of the 84 marks saved overnight (`~/bcfish-session-backup-2026-11-12/marks-triage-1115.md`) and
T742. His notes, verbatim:

- mark 2578, Region 3 Fraser "Mainstem waters of the Fraser River": "wrong: row 2 (from — ours: from Region 3 – Region 8
  boundary) · make sure that when i checked fraser as water for the mainstem items it includes all channels too. like
  sea bird island etc."
- mark 2583, Bella Coola "All tributaries to the Bella Coola River.": "this one should be tributaries only?"
- mark 2586, Bella Coola "(not including Atnarko River)", whole: "make sure this excludes atnarko"
- mark 2619, Nitinat "downstream of Parker Creek, except in the areas and times described below": "this extent is weird
  because it should be all other parts but within an extent. this is the proper extent but need to make sure the excep
  tthe other listed parts shows through."
- mark 2651, Qualicum "All open portions of the Qualicum River": refused at the write-through ("op other_places has no
  water of its own"), found only later.
- (scope additions through the main chat, verbatim) "need to also fix all other tributary only items like on thre
  bulkley too. fix the proposals after adding ability for it"; "also we need to make sure all items with tributaries in
  the name etc have tributaries in extent."; "need to make sure all rules under a water with (includes tributaries) in
  the name gets tributaries too."

## What was wrong, what changed

1. **Region-line ends named by another water's atlas row.** `checks._ctx_now` (and `dfo.cut_labels`, the split
   features, a piece's end, the dead-piece words, the synopsis extent words) took `area:N`'s one atlas row, which sits
   on another water. One namer now, `reuse.cut_name(sid, items)`: a region line read on a water is that water's
   crossing (T742's `region_ends`): the Fraser's `area:3` reads "Region 3 boundary · km 453 …" with its own pin; a line
   crossed twice and named bare says so; read on no water, "Region N boundary". The checklist takes it through
   `checklist.Context.on_water`.
2. **A water's own channels.** An extent naming the Fraser (`item_id: gnis:39325`) held on the river's own sections:
   Mission → Spuzzum bound 41 sections, not 50 (Maria Slough by Seabird Island, Jeperson Side Channel: 19.5 km out).
   Now `pipeline/atlas/reach/channels.py`: a DFO place's extent that names a water holds on every item of the place's
   water that is a side channel of it (takes water and returns it, a stream, shorter; a channel off a channel too) —
   at resolution, so the preview, the reach run, old marks and new ones agree, and no mark or file is rewritten. The
   editor and the live draft line show "binds N sections · K km — its own side channels included: …". Which marks
   re-bind: `handoff/PLAN/curation-review/channel-inclusion-2026-11-15.md`.
3. **Tributaries only, tributaries excepted.** `ExtentFix.tributaries_only` and `tributary_excludes` (registry ids),
   DFO marks only; unset on every older mark (its line is byte for byte the same; the apply makes no call for it).
   The editor: "holds: as the place has it / the water and its tributaries / tributaries only / the water only" and
   "except these tributaries: <search>"; one call for every piece. Preview, write-through (`proposals.build`: the
   place's `tributaries_only` / `tributary_excludes`) and the editor opening on the place's own call. A real-data test
   proves Bella Coola tributaries only holds no section of the Bella Coola or the Atnarko (the data lane: 2583 as saved
   covers the mainstem and wins a tie, mainstem chinook reads closed).
4. **Qualicum refused late.** `other_places` takes no water (its validator says so); the editor now cannot send one
   (choosing that op unticks the waters, the "on" row says why), and a save the write would refuse is refused AT THE
   SAVE (`dfo.refused_later`: 422 "NOT SAVED — the write would be refused: … Untick the water(s) …"). A refusal that
   still comes from the background apply shows at once on the checklist, whatever place is open, until closed.
5. **Visibility.** (a) A place marked before shows "You marked this place before, <date> (<verdict>) … This save
   differs: <tributaries / excepted / extent>" before the save. (b) Every place shows "The saved extent binds N
   sections · K km (T km of it tributaries) — the water itself is W km" with a warning when it walks tributaries the
   place's words never name, or is over 4× the water and ≥ 40 km (Redsand Lake 3,086 km; Bella Coola whole 10,582 km).
   (c) DFO's words against the extent being saved: "DFO's words are on the tributaries only…", "…take the tributaries
   in; this extent has none", "…leave them out; this extent walks them", "…leave out X; this extent does not except
   it", the water's own heading too. Warnings never block or change a save.
6. **Suggester.** `suggest.tributary_form`: "all tributaries of/to X" → tributaries only; "other than / except /
   excluding / not including <a named water>" → that water excepted (pinned to the water's tributaries by watershed
   code; a name not pinned is said and the row drops to low); "and / including tributaries" in or out; the WATER's
   heading ("and tributaries", DFO's "(including tributaries)") inherited by every place under it; a place whose own
   words are narrower (main stem, excluding tributaries, tributaries only) keeps them and carries
   `tributary_conflict` for the owner (owner 2026-11-15: "need to make sure all rules under a water with (includes
   tributaries) in the name gets tributaries too": a place under such a water is suggested with them; the review app's
   warning reads the water's heading too). A bound place whose call differs from its words gets a row too (state
   `tributary_call`, its own extent with the call). The generated file was NOT written: a targeted merge waits for the
   main chat (37 rows; a full regenerate would change 101 rows that are not targets).

7. **Mouth / tidal ends** (owner 2026-11-15: "make sure all the mouth/ tidal splits are at the correct one (not the
   proposed one but at the tidal ones we give every stream)"; audit `handoff/PLAN/curation-review/mouth-tidal-ends-audit-2026-11-15.md`).
   The split hunt no longer makes a new cut where the atlas has one: a hunted point within 25 m of a curated cut or a
   coast crossing on the same line IS that cut (Chapman 3.6 m, Stave 0.7 m off their tidal boundaries), and a point at
   the line's bottom (≤ 2 m: Illiance, Kitsault, Wilauks at m 0) is the mouth — an open end, which the reach run trims at
   the stream's tidal boundary (T632): "upstream of signs near the mouth" becomes the whole river, "between A and the
   mouth" downstream of A. The editor's split list offers "the tidal boundary (open end, no cut)" as its own choice, and
   a coast crossing is named "tidal boundary (coast crossing)", never "shore".

## Left

- Speed (6): rebuilding only the saved place's derived results after a save was not started; the saved-size walk is
  cached per place and places-file stamp (Bella Coola 11.8 s cold once, then 0 s).
- Nitinat 2619 ("all other parts but within an extent"): a rest limited to a stretch is not an op; not changed here.

## Follow-up 2026-11-15 (worktree lane `review/followup-1115`)

How it was checked: my own copy of the app on :8796/:5182 from `~/bcfish-review-followup`. It reads the main
checkout's built data through symlinks, with `CURATED_ROOT` set to a scratch copy of `data/curated` (the owner's
marks as of 19:2xZ). Write-through and inputs sync were OFF. `/api/ready` named the scratch marks file before any
save. The main `marks.jsonl` holds no line of mine (the test marks carry reviewer `timing-test`), and the main
`dfo-suggestions.json` md5 is the same before and after.

1. **BC border as a pickable end.**
   - `reuse.border_end` / `border_ends` read every `border:<blk>:<n>` cut from the atlas's own section bounds. That is
     917 cuts, with no rebuild needed: the bound's `route_measure` gives the point, and the sections' `out_of_bc` give
     the side.
   - Each cut is named by the one namer (`cut_name`), for example "BC–Yukon border · km 102 — outside BC upstream
     (to km 146)". The neighbour comes from where BC's outline runs (`border_neighbour`); every one of the 917 cuts
     got one.
   - Every picker offers them:
     - the DFO map list (`_split_feature` / `_split_point`);
     - the cut search (`search_splits`: "border", "bc", "yukon"…);
     - the regs editor's boundaries (`with_region_ends`, `border_end`);
     - the checklist's labels and pins;
     - the regs side's `entry_split_points`.
   - The atlas still writes no `border:` rows into `splits.resolved.json`. That is the longer-term fix, but it is not
     needed now.
2. **"Cannot edit" says why.**
   - `dfo.cannot_edit_words` (in `extent_now.cannot_edit` and on the region-cascade scopes) gives one plain line per
     field the editor cannot say, plus what to do: "confirm it as it is … or mark it wrong and say in the note what
     should change".
   - It shows where the edit button would be (CheckReview), in the needs-extent block, in the DFO tab and in the
     Region cascade (`CannotEditNote`).
   - All 15 places have a reason (test). No place's data changed.
3. **Speed after a save.**
   - Before: a save rebuilt every DFO row and every checklist place, re-grouped every proposed cut, and re-parsed every
     region file once per place. A full garbage collection over the ~14 M loaded objects cost 2.1 s, landing on the
     next request.
   - Now:
     - `reuse.marks_delta` proves what the store gained, and only the saved place's rows are rebuilt, with its
       water's other places (`dfo._patched_rows`).
     - Only its checklist place is rebuilt (`checks._patched_places`).
     - A save that proposes no cut keeps the proposals, real ids and context (`proposals_stamp`).
     - `gc.freeze()` runs after the warm (`settle_gc`).
     - `proposals.Sources.parsed` gives the finders copies, and registry.json is read once per build.
     - The size walk no longer holds a place up: the reach says `size_pending` and the screen asks `/size` after.
       Sizes are kept per place's own reach input, so a write-through rewrite re-walks only that place.
     - The water's faint layer takes no size walk at all.
   - Patched rows and places equal a full rebuild, byte for byte, on real data (2 saves).
   - Timings (API level, the requests the checklist's `afterSave` makes; warm; write-through off):

     | | before | after |
     |---|---|---|
     | save request | 0.92 s | 0.43 s |
     | click → list usable (queue + progress + water) | 2.0 s | 0.55 s |
     | click → next place loaded | 2.5–3.9 s | 0.67 s |
     | queue + water after a save, when a full GC hit | 4.4 s | 0.15 s |
     | Bella Coola whole reach | waited on its size (1.1 s warm, 11.8 s cold) | 0.02 s, the size 1.1 s after |
   - Not covered: with write-through ON, the apply rewrites the region file a moment later. That rebuilds the rows
     once in full (the file is an input), but no longer re-walks every size.
4. **"Old extent still in force".**
   - The rule is one predicate, `audit.old_extent_in_force`: the last save is wrong or unsure, it has no fix, and
     the place still has an extent (its own or the one it rides).
   - It shows on the checklist header, as a chip in the water list, and as the filter "old extent in force". Audit
     section B's text says the flag; its keys are unchanged.
   - In the scratch copy it flags 4 places: Fraser R3 mainstem, Thompson (rides ~2), Morice, Sheldens.
   - What ships is unchanged.

Tests:
- `pipeline/tests/test_review_followup_1115.py` (14 tests) and `curation-review/frontend/tests/followup1115.test.ts`.
- `test_review_checklist`'s memo test pins the full-rebuild path.
- Frontend node tests: 52 pass. tsc is clean.
- The 63 review/curation/dfo/audit test files in CI's no-data tier, unfiltered: 886 passed.
