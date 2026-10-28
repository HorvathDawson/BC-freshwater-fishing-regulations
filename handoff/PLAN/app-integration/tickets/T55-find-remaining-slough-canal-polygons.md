# T55 — Find the sloughs and canals still drawn as lakes (POSTPONED)

Status: IN PROGRESS — 2026-10-28: the review app (:5183, "Sloughs" tab, `#sloughs/`) shows every v2 candidate (2,424) with its PRE-MARK; ★ OWNER confirms there (one, or "accept all visible"), each a mark (scope `slough`, `call` stream / not stream). Next: confirmed streams → registry fold → reach + deliver + binding diff, as a side build. The tab is the ONLY slough review place (owner 10-28): the old review page (artifact, retired) is fully carried — 150 verdicts as `page_verdict`, 8 free-text notes as `page_note`, its database saved in `handoff/PLAN/sloughs/marks-final-2026-10-28.json` (151 verdicts + 58 spot checks). Earlier: finder v2 + that page (read into the automark), automark d4a22561. Prior fec02c29, 96b0eb73, 669fd065, a59f3dc3, ac8a0e1c.

Owner 2026-10-22 (late), decisions.md "sloughs and canals look like streams in Conditions": the book treats sloughs and
canals as streams. Many such polygons are already marked as stream items; "the rest are still to be found … and stays
postponed". This ticket is that postponed part. Do not start it with the braid / canal geometry work, which stays
PAUSED (decisions.md names BRAIDS-HANDOFF.md for it; that file is not in the repo as of 2026-10-22 — the only braid
material is handoff/PLAN/app-data/side-channels/shots/klesilkwa-braid.png).

## What is done (not this ticket)
A polygon ALREADY folded into a stream item (`item.kind = 'stream'`, `section_span.shape = 1`; 146 sections on 48 items)
is walked as a stream (point 2 below); one the walk leaves unanswered takes its water's line answer
(`build_panels.stream_polygons`, now the fallback; `pipeline/tests/test_stream_polygon_panels.py`).
On the real graph that answers 38 of the 45 polygons that drew grey beside coloured lines; the other 7 (Nation River
1698439, 1698992, 1700170, 1703535; West Road (Blackwater) 1263111, 1406976; Honna 1444372) stay grey with the
unanswered lines around them — re-examined under point 2: still grey, honestly.

## What is left
1. **Find the slough / canal / channel polygons that are still lake (or wetland) items** and decide, per water, whether
   it is a stream under the book. Once it is a stream item (registry: `pipeline/atlas/registry/flowing.py` folds it
   into its river), the colouring follows with no further change.
2. ~~**Lines boxed between stream polygons.**~~ DONE 2026-10-22 (late), owner "a polygon-only slough or canal gets
   gauges like a stream": the walk reads the WATER'S kind (`build_panels.flowing`, pinned by
   `pipeline/tests/test_polygon_walk.py`). A stream item's polygon is sized by the lines beneath it, offered a panel and
   walked through, never a lake crossing. Real graph: stream polygons answered 38 → 59 of 145; Nicomen 59620 answered;
   Seton Canal and Lewis Slough (polygon-only) answered; Bowman and Hansen Slough stay grey with their unanswered
   lines. The 7 named above (Nation 4, West Road 2, Honna 1) STAY GREY: re-examined, every line beside them is
   unanswered too, so a line in their place would be grey. Numbers and the one big side effect (the two Nicomen Slough
   gauges now climb into the Fraser like any side-channel gauge: 71,668 sections) are in decisions.md under
   "a polygon-only slough or canal gets gauges like a stream". Needs a `pipeline.deliver` rerun.
3. **OWNER QUESTION raised by 2:** should a gauge on a side channel speak for the river it branched from and that
   river's tributaries? Today's walk says yes (29,520 live answers depend on it); see decisions.md.

## Candidates (cheap, from the live bundle by name; not exhaustive)
- **OKANAGAN RIVER OXBOWS**: 41 items (32 lake, 9 wetland). Oxbows are cut off from the river, so likely NOT streams —
  confirm against the synopsis before reclassifying.
- No lake or wetland item is named "Slough", "Canal" or "Channel" today: the named ones are already stream items. The
  remaining cases are therefore UNNAMED lake polygons on a stream's course (FWA river/manmade polygons whose name the
  registry drops — see "lake must not answer to its river"). Finding them needs the graph: lake nodes with a stream
  item's line on both sides and no lake item of their own. Not counted here (the 585 MB graph was not loaded).

## Acceptance
- A list of candidate polygons with the evidence for each (book wording, FWA edge/waterbody type, graph neighbours),
  reviewed by the owner before any reclassification (bulk reclassify: dry-run + confirm).
- Each accepted one becomes part of its stream item; its section then paints with the stream's flow colour in every
  Conditions view (the existing bundle invariant in test_stream_polygon_panels.py covers it).
- A true lake keeps lake treatment.

## Pre-marks in the review app — 2026-10-28 (owner decisions)
Owner: (a) the ≤ 44 m candidates are "stream" ONLY AS PRE-MARKS in the review app, a proposed answer he confirms or
changes; (b) wide marsh / wetland channels (> 44 m, FWA marsh / swamp) are stream too, pre-marked the same way;
(c) intermittent ones are not bulk-folded, they stay on his list.

Built (`pipeline/regs/review/slough.py`, `curation-review/backend/sloughs.py`, `frontend/src/components/SloughReview.tsx`;
reads `handoff/PLAN/curation-review/slough-automark.json` + `handoff/PLAN/sloughs/v2/candidates_v2.json.gz`, never writes them):
| group | count | what |
|---|---|---|
| pre-marked stream | 936 | 801 narrow (automark "auto", ≤ 44 m) + 135 wide marsh / swamp |
| your list (no pre-mark) | 324 | 201 wide lake-drawn, 113 intermittent (FWA "…-Intermittent"; 72 of them were automark "auto"), 10 with no measured width (automark read them as 0 m — e.g. Back Channel, a 74 ha marsh) |
| marked on the review page | 150 | the owner's artifact verdicts, shown as his (artifact `verdicts` re-read 2026-10-28: 151 docs, unchanged; the 151st, 329097416 Cruickshank, has its verdict cleared and only a note, so it sits in `default` with the note as `page_note`) |
| not for review | 1,014 | LAKE class, sewage / tailing ponds (automark "default") |

- A pre-mark is NOT a mark: the queue shows "pre-mark: stream · narrow / wide marsh" with why (width, FWA type) and the
  satellite pixel shares; the detail has the outline on the map, a Google Maps satellite link and the finder's evidence.
- The owner's call is a mark (scope `slough`, P y + `call` "stream" / "not stream"; P ? = unsure; defer with a note) —
  `marks.Mark.call`, prints `slough:<id>` (meaning: stream, FWA, width, area; binding: the outline). "accept all
  visible" marks every shown, unmarked, pre-marked row stream in ONE commit (`targets`; the safety pin follows it).
- Tests: `pipeline/tests/test_curation_review_slough.py` (pre-marks never write; one call and a batch are marks in a temp
  store; data/curated untouched); `test_curation_review_safety` pin updated. Shot: `handoff/PLAN/app-integration/T55-shots/`.
- Not done here: folding confirmed streams into their rivers (registry → atlas → binding diff → reach + deliver).
