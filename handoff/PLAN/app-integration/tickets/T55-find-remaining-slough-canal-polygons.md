# T55 — Find the sloughs and canals still drawn as lakes (POSTPONED)

Status: IN PROGRESS — finder done: 865 candidates (65 stream by book, 236 likely, 331 unsure, 233 lake controls) in handoff/PLAN/sloughs/candidates.json.gz; ★ OWNER marks them on https://claude.ai/artifact/Es3Mokt7Fs2yNvm7JgQua8 (marks read back with ArtifactData, collection "verdicts"). Next: confirmed ones become stream items (registry fold) → reach + deliver + binding diff, as a side build separate from T67. Point 2 (polygon walk) built on T101's branch; the 7 grey polygons (Nation, West Road, Honna) re-examined: honestly grey. Side-channel gauges: Q44 answered 2026-10-23, built under T101. Prior fec02c29, 96b0eb73, 669fd065, a59f3dc3, ac8a0e1c.

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
