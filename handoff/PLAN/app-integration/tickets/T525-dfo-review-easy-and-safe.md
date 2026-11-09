# T525 — The DFO review screen is easy to follow and hard to get wrong

Status: IN REVIEW — owner restarts the review app and looks

Owner 2026-11-09: "make app easier to review too right now the info is really hard to follow and extents already
defined get lost when confirming vs redo. etc etc it is just hard to use right now and feels like i will make bad data".

## What
- Audit the DFO review flow end to end (curation-review/frontend DfoReview, ExtentFixEditor, useMarking; backend dfo.py)
  as the owner uses it: what a place shows, what each answer (confirm / redo / fix / unsure) does to the existing
  curated extent, where the current extent disappears or is silently replaced.
- One clear layout: DFO's words → the CURRENT curated extent (words + map colour) → what this mark will do to it
  (keep / replace / add a piece) → save. Confirming never loses the existing extent; redoing starts FROM it.
- Every save states its effect before it is made; nothing is changed without the owner seeing old vs new.
- Builds on T524 (pieces, tributary default from the title, "replaces extent" banner); lands after it in the same files.
- Proof: a walkthrough with shots of 4 real places (whole water, between two cuts, two-river T524 case, a landed fix).

## Audit (2026-11-09, read-only, lines at HEAD 56bd9ee48; T524 in flight shifts them)
Data today: 352 places, 187 with a curated extent; 102 DFO marks in force, 93 with a fix. **94 curated places have
no fix in their mark** (87 never marked), so their editor opens blank. 6 places hold 2–4 extents. Extents carry
`within_area`, `outside_areas`, `feature_types` and `watershed` (22 uses), and the editor cannot express any of them.

**A. The extent is lost or silently replaced (data risk)**
- A1 A fix REPLACES `binding.extents` wholesale (`proposals.py:996` `["extents"] = [ex]`; T524 keeps it a replace).
  `extent_of` (`proposals.py:432-460`) writes only op/splits/item_id(s)/area_id, so a change DROPS `within_area`,
  `outside_areas`, `feature_types` and `watershed`, and collapses a multi-extent place. `before` is read at APPLY time
  (`proposals.py:1020`), so the strict check (`:257/:267`) never catches a file that moved after the owner looked.
- A2 The editor seeds from the MARK's fix and not from the curated extent (`DfoReview.tsx:452-454` `savedFixOf`; T524
  `savedFixesOf`). With no fix it shows an empty shell (`ExtentFixEditor.tsx:77-80`): op = the notice's parsed op, no
  points, `items: []` (= every item of the water, so a one-item extent widens), and no tributary call. A redo
  therefore starts from nothing. This is the owner's "extents already defined get lost".
- A3 Confirming resends the old mark's fix (`DfoReview.tsx:641` sends `extent_fix` whenever the editor holds one).
  T210 lets a verified mark carry a fix that uses only existing cuts (`marks.py:577-581`), so y y y Enter re-applies
  the old fix over a curated extent that has changed since (another mark, a T185 apply, a re-anchor) and REVERTS it.
  None are pending today (0 diffs), but nothing structural stops it.
- A4 A group save copies one fix onto every member, and all members are ticked by default (`DfoReview.tsx:450-451,
  661`; `dfo.py:916-933`). The member rows do not show each member's own extent, so a member that differs is
  overwritten unseen.
- A5 Every latest mark lands, whatever its verdict (`export.py:98`, `proposals.py:844/966`). "Save extent" with
  nothing answered posts P ? (`DfoReview.tsx:637`) and writes through in seconds. The UI says the opposite:
  "recorded in the mark, never in a file (T185…)" (`ExtentFixEditor.tsx:132`) and "proposals only (T185)"
  (`DfoReview.tsx:998`). Both have been false since the T325 write-through.

**B. The extent is not shown**
- B1 Magenta means "what binds now" until a preview runs, then it means the PROPOSED extent (`DfoReview.tsx:555-556`,
  legend `:1014`). The current extent disappears exactly when old vs new matters. Teal (the mark's fix) may or may
  not equal the current extent, so three overlapping colours tell the owner nothing clear.
- B2 "The extent now" prints raw ids, e.g. `between(cowichan_river__cnr_trestle, …)` (`:773-775`). Only the mark's
  fix gets words (`:830`).
- B3 After a later mark without a fix, the screen says "no extent saved on this location yet — curate it below"
  (`:834`) while the curated file still holds the extent (`proposals.py:966` skips it). The editor's "remove"
  (`ExtentFixEditor.tsx:136`) reads as deleting the extent but only drops the draft.
- B4 A save says "saved ✓ … your extent: …" and jumps to the next place (`DfoReview.tsx:414-419, 646-650`) before
  the background write-through says landed or refused. The owner never sees the result on the place.

**C. Confirm vs redo is ambiguous**
- C1 Two buttons write the same kind of mark: "Save extent" (`:957`, P ? automatically, then advances, so T/R/P are
  never answered) and "Record mark" (`:995`).
- C2 Once a fix has landed (Cowichan 66 Mile Trestle: P ? with new cut `n1`, now the real cut
  `white_triangle_boundary_sign_loc`), the editor reopens on the old fix with a NEW point. y y y is then held: "a new
  split … answer P n" (`:614-618`). To confirm, the owner must press "remove", which looks like deleting the extent.
- C3 P is auto-set to n when the draft differs (`:609-612`), so any edit reads as "placement wrong", whether the owner
  meant a redo or a small tweak.
- C4 The server refuses "defer" whenever the editor holds a fix ("an extent fix is proposed on a wrong or unsure mark",
  `marks.py:577-581`). It refuses "Save extent" with only T/R answered the same way (P is filled only when nothing is
  answered, `:637`).
- C5 "accept suggestion", "use this fix" and the two-cut helper overwrite the draft silently (`:592, :871, :936`).
  `:936` also narrows items to the first item.
- Wording: "op", "scope", "binding", split vs cut, "T185", "the latest mark wins", "proposals only", "bound"; the
  source line "op tributaries_only · anchors …" (`:749-751`); long T/R/P questions. `useMarking.tsx` is not used by
  the DFO screen.

## Design: one flow per place
Mockup: two real places (Morice Lake = whole water; Cowichan 66 Mile Trestle = between two cuts, a landed fix), at
scratchpad `t525-mockup.html`. Map and section counts in it are illustrative.
1. **DFO says**: title + specific area verbatim, rows folded, page link. Nothing else.
2. **The extent now**: words from `binding.extents` + tributaries (T524 `extentWords`), the section count, and who
   set it (your mark of <date> / the parse). PINK SOLID on the map, ALWAYS drawn. Parts the editor cannot express are
   listed and marked "kept".
3. **Your call**: T and R as small y/n rows, then four choices:
   ✓ Right, keep it · ✎ Change it · ? Not sure · ✗ Wrong, fix later.
   - Keep / Not sure / Wrong send NO `extent_fix`. Effect line: "Keeps the extent now. The curated file does not
     change."
   - Change opens the editor SEEDED FROM THE EXTENT NOW (each curated extent becomes a piece, with items and
     tributaries copied). It never opens blank or on an old mark. Effect line, live: same → "Nothing changes";
     differs → "REPLACES" with now (struck) vs new, both with section counts (`dfoExtentPreview`); extra piece →
     "ADDS a piece". The draft is TEAL DASHED on top of the pink.
4. **One save button that names its effect**: "Save: keep extent" / "Save: replace extent" (amber) / "Save: add a
   piece". After saving, stay on the place until the write-through answers ("the extent now: …" or the refusal), then
   j moves on.

Rules to enforce in code:
- R1 Confirm never carries a fix (this removes A3 and C2).
- R2 The draft seeds from the curated extents. An old mark's fix is shown only when it differs from now ("your mark
  of 10-26 said X; load it into the draft").
- R3 The mark records `extent_before` (the curated extents the owner saw). `proposals.build` uses it as `before`, so
  a strict apply refuses an overwrite if the file has moved since.
- R4 A change to an extent with fields the editor cannot express is blocked with the reason, or those fields are
  carried over. They are never dropped silently.
- R5 Group: a member whose extent differs starts unticked and shows its extent.
- R6 Suggestion, "use this fix" and the two-cut helper fill the draft only under "Change it", and the effect line
  says REPLACES.
- R7 A defer drops an untouched draft. The wording drops "T185", "proposals", "never in a file", "op" and "scope".
  "Cut" is used everywhere.

Reuse: ExtentFixEditor (T524 pieces), the MapPanel `saved` layer for "now" and `preview` for the draft, T524
`fix_vs_current` / `extentWords`, `api.dfoExtentPreview`, and the write-through outcome (GET
/api/review/write-through).
Tests: `effectOf(now, draft)` (pure); seed round trip (`extent_of(seed(binding)) == binding.extents` on every curated
place); keep posts no `extent_fix`; a stale `extent_before` makes the apply refuse; a differing group member starts
unticked; Cowichan confirm records verified with no fix.
Order: lands after T524, in the same files.

## Implemented (2026-11-09, on T524)
- Guards: the editor opens on the curated extent (`proposals.fixes_of_extents`, served as `extent_now`; 172 of 187
  curated extents round-trip exactly, 15 hold a field a fix cannot say and offer no change); Keep / Not sure / Wrong
  post no `extent_fix` (`frontend/src/dfoExtent.ts` `markBody`); every DFO mark sends `extent_before` and `dfo.mark_for`
  refuses a missing or moved one, per group member too; `proposals.build` refuses replacing an extent holding
  `within_area` / `outside_areas` / …; a group member whose extent differs starts unticked and shows its extent; the
  editor / new-split wording says the save writes the curated file.
- Layout: DFO says → the extent now (words, magenta, always drawn) → T, R and four calls → the effect (keep / REPLACES /
  ADDS, now struck vs new) → one save button naming its effect. The draft is teal; after a save the place stays and
  reloads once the write-through has run.
- T524 review folded in: effective tributary call (`dfo.tributaries_now`), pieces compared as a set, `fix_extents`
  reuses `extent_of` with the proposals' point resolution, preview calls `DS.dead_pieces`, duplicate tributary rule and
  `savedFixOf` removed, TSX string pins replaced by `node --test tests/dfoExtent.test.ts` (run from test_review_dfo).
- Review pass 2 (coordinator 11-09): a half-made piece holds the save and is named in red (`incompletePieces`; the
  mark model refuses it too); every cut and water reads by its label / name (`dfo.cut_labels`, `dfo.item_names`; a
  cut with no label shows as an error, `extentWords.ts`); the mark keeps `extent_before` (`marks.SeenExtent`) and
  `proposals.build` refuses a mark whose place moved since (a mark whose own fix landed is not held).

## Round 3 (2026-11-09, owner: "still really bad, so confusing … multiple proposal streams … a save doesn't visibly update")
- **One proposal per place**: Claude's fix proposal if there is one, else the suggestion (`dfoExtent.oneProposal`), in
  words + violet on the map, one "use this" (fills a Change; the effect line says REPLACES). Not shown when it is the
  extent now or while a save of yours waits to land. Reason, caveat source, hunt evidence, the other source: under
  "details".
- **One status line** after a save (`dfo.saved_status` + the write-through outcome, `dfoExtent.statusLine`): "Saving…",
  "Refused: …", "Saved <date>. The extent now: …", "Saved. Takes effect after the next rebuild builds the new cut X.",
  "Saved, not applied yet. Its cuts are built now: it lands with the next DFO save.", "Deferred. Nothing changed."
  The save banner and the "your mark said … not the extent now" row are gone (what you saved: a disclosure on the line).
- Fewer things: no ids, MU, "binding", T-numbers; queue rows lose the fix-vs / +split / group / row-count chips;
  waters, notes and other places on the water fold under "more about this place"; the note box opens on demand; a
  change naming a not-built cut says "takes effect after the next rebuild builds …" before the save.
- Guards R1–R7 unchanged (markBody, extent_before, seeded editor, differing group member unticked, server refusals).
- Found: Chilliwack/Sumas (~2): Barrow Town is built now; its saved two-piece fix has not landed because the
  write-through has not run since (a dry run says it would apply). The next DFO save lands it.
- Shots: `shots/T525-r3-forks.png`, `shots/T525-r3-forks-change.png`, `shots/T525-r3-chilliwack-sumas.png`,
  `shots/T525-r3-whole-water.png`.
