# T55 — Find the sloughs and canals still drawn as lakes (POSTPONED)

Status: BLOCKED (owner) — shrunk (owner 2026-11-03): what is left here is the owner confirming slough marks in the review app; the fold, side build and gate are T322, the lock T328. Was: IN PROGRESS — 2026-10-28: the review app (:5183, "Sloughs" tab, `#sloughs/`) shows every v2 candidate (2,424) with its PRE-MARK; ★ OWNER confirms there (one, or "accept all visible"), each a mark (scope `slough`, `call` stream / not stream). Next: confirmed streams → registry fold → reach + deliver + binding diff, as a side build. The tab is the ONLY slough review place (owner 10-28): the old review page (artifact, retired) is fully carried — 150 verdicts as `page_verdict`, 8 free-text notes as `page_note`, its database saved in `sloughs/marks-final-2026-10-28.json` (in git history at 03be2862e) (151 verdicts + 58 spot checks). Earlier: finder v2 + that page (read into the automark), automark d4a22561. Prior fec02c29, 96b0eb73, 669fd065, a59f3dc3, ac8a0e1c.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T55 | sloughs/canals still drawn as lakes | BLOCKED (owner) — only the owner's slough confirmations left; the fold is T322 | fec02c29, 96b0eb73; prior 669fd065, a59f3dc3, ac8a0e1c, d4a22561 | review app "Sloughs" tab: 811 confirmed (801 narrow pre-marks + 10 single; folded by T322); still to call: 135 wide-marsh pre-marks + 314 on the owner list (113 intermittent) → ★ … (detail: ticket file) |

Owner 2026-10-22 (late), decisions.md "sloughs and canals look like streams in Conditions": the book treats sloughs and
canals as streams. Many such polygons are already marked as stream items; "the rest are still to be found … and stays
postponed". This ticket is that postponed part. Do not start it with the braid / canal geometry work, which stays
PAUSED (decisions.md names BRAIDS-HANDOFF.md for it; that file is not in the repo as of 2026-10-22 — the only braid
material is docs/research/side-channels/klesilkwa-braid.png).

## What is left
- **The owner's calls** in the review app (:5183, "Sloughs" tab, `#sloughs/`; the ONLY slough review place, owner
  10-28). Still to call: 135 wide marsh / swamp pre-marks (> 44 m) and the owner's list (324: 201 wide lake-drawn, 113
  intermittent (FWA "…-Intermittent"; 72 of them were automark "auto"), 10 with no measured width (automark read them as
  0 m — e.g. Back Channel, a 74 ha marsh)). Owner 2026-10-28: (a) the ≤ 44 m candidates are "stream" ONLY AS
  PRE-MARKS in the review app, a proposed answer he confirms or changes; (b) wide marsh / wetland channels (> 44 m, FWA
  marsh / swamp) are stream too, pre-marked the same way; (c) intermittent ones are not bulk-folded, they stay on his list.
- A call is a mark (scope `slough`, P y + `call` "stream" / "not stream"; P ? = unsure; defer with a note),
  `marks.Mark.call`, prints `slough:<id>`; "accept all visible" marks every shown, unmarked, pre-marked row stream in ONE
  commit. Each confirmed stream then folds into its river through the T322 path (side build + binding-diff gate).
- Still grey, honestly (every line beside them is unanswered too): Nation River 1698439, 1698992, 1700170, 1703535; West
  Road (Blackwater) 1263111, 1406976; Honna 1444372.
- Unchecked candidates: **OKANAGAN RIVER OXBOWS**, 41 items (32 lake, 9 wetland). Oxbows are cut off from the river, so
  likely NOT streams — confirm against the synopsis before reclassifying. The rest are UNNAMED lake polygons on a
  stream's course; finding them needs the graph (lake nodes with a stream item's line on both sides, no lake item).

Done: polygon walk by the water's kind (`build_panels.flowing`, `pipeline/tests/test_polygon_walk.py`, 2026-10-22);
review-app tab + pre-marks (`pipeline/regs/review/slough.py`, `curation-review/backend/sloughs.py`,
`frontend/src/components/SloughReview.tsx`, `pipeline/tests/test_curation_review_slough.py`; shot
`docs/app-integration/T55-shots/`); 811 confirmed (`data/curated/decided/sloughs.json`: 801 "accept all
visible" + 10 single) folded by T322; old inputs archived by T328. The old review page's 150 verdicts are carried as
`page_verdict`, 8 notes as `page_note` (`sloughs/marks-final-2026-10-28.json` (in git history at 03be2862e)).

## Acceptance
- Each accepted one becomes part of its stream item; its section then paints with the stream's flow colour in every
  Conditions view (the existing bundle invariant in test_stream_polygon_panels.py covers it).
- A true lake keeps lake treatment; nothing is reclassified without a dry run and the owner's confirm.

## Owner

- [ ] 2026-10-28 — Confirm the slough / canal marks in the review app, Sloughs tab (`#sloughs/`): the wide-marsh pre-marks and the rest still to call (Q4); the 7 grey polygons (Nation, West Road, Honna) stay grey until you do. Default: nothing is reclassified without a dry run and your confirm.
