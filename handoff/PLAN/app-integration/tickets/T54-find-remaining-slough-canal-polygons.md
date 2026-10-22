# T54 — Find the sloughs and canals still drawn as lakes (POSTPONED)

Owner 2026-10-22 (late), decisions.md "sloughs and canals look like streams in Conditions": the book treats sloughs and
canals as streams. Many such polygons are already marked as stream items; "the rest are still to be found … and stays
postponed". This ticket is that postponed part. Do not start it with the braid / canal geometry work, which stays
PAUSED (decisions.md names BRAIDS-HANDOFF.md for it; that file is not in the repo as of 2026-10-22 — the only braid
material is handoff/PLAN/app-data/side-channels/shots/klesilkwa-braid.png).

## What is done (not this ticket)
A polygon ALREADY folded into a stream item (`item.kind = 'stream'`, `section_span.shape = 1`; 146 sections on 48 items)
takes its water's line answer: `build_panels.stream_polygons`, pinned by `pipeline/tests/test_stream_polygon_panels.py`.
On the real graph that answers 38 of the 45 polygons that drew grey beside coloured lines; the other 7 (Nation River
1698439, 1698992, 1700170, 1703535; West Road (Blackwater) 1263111, 1406976; Honna 1444372) stay grey with the
unanswered lines around them — point 2 below is what would reach them.

## What is left
1. **Find the slough / canal / channel polygons that are still lake (or wetland) items** and decide, per water, whether
   it is a stream under the book. Once it is a stream item (registry: `pipeline/atlas/registry/flowing.py` folds it
   into its river), the colouring follows with no further change.
2. **Lines boxed between stream polygons.** The panel walk cannot step through a polygon (no stream magnitude, and it
   counts one as a lake crossing), so a short line between two polygons of the same slough gets no panel — Nicomen
   Slough 59620 (57 m), and items with few panelled lines (Nelson 1 of 4, Daly 1 of 4, Zaitscullachan 1 of 4). Fix by
   letting the walk read the WATER'S kind (`_kind` / `area_of` through a stream item's polygon); it changes other
   sections' panels, so diff `section_panel` before and after on a real build before promoting.

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
