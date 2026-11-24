# T690 — Whiteswan Lake's inlet & outlet streams: every inlet No Fishing, the exception only below the falls

Status: DONE — c41d54742. Every Whiteswan inlet and the outlet down to the falls are No Fishing; the 12 inlets became items in the final full_next and shipped in r-adc4e248a163.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T690 | Whiteswan Lake's inlet & outlet streams: every inlet (15) and the outlet down to the falls No Fishing; only the outlet below the falls is the exception (Aug 1-Mar 31 closed; rainbow 5, release the rest) | DONE | c41d54742 | — |

Closed by the 2026-11-20 audit: the final full_next with T690 (12 inlet items, r1 16 sections) went into candidate r-adc4e248a163, on staging 2026-11-13 (chain finished, attributed diff names T690); T790 item 4 lists T690 as shipped.
`whiteswan_lakes_inlet_outlet_streams.r1` in that build's binding diff (2 → 16 sections expected)

Source: owner 2026-11-12, answering T637's "say if the name or the piece is wrong", verbatim: "It is this (no Fishing
EXCEPT the outlet stream downstream of the falls 2.4 km downstream of Whiteswan Lake, No Fishing Aug 1-Mar 31, rainbow )
so not the inlet. The outlet is below the falls. Inlet is all no fishing (every inlet) trout daily quota = 5 (catch and
release all other species) when open)". RULINGS E26.

## The book (printed p.40, PDF p.42; row 4-24)
"WHITESWAN LAKE'S INLET & OUTLET STREAMS — No Fishing EXCEPT the outlet stream downstream of the falls 2.4 km downstream
of Whiteswan Lake, No Fishing Aug 1-Mar 31, rainbow trout daily quota = 5 (catch and release all other species) when
open". The coordinator's reading (every inlet and the outlet down to the falls closed all year; below the falls closed
Aug 1-Mar 31, rainbow 5 and everything else released when open) matches the printed words at every point; nothing
stopped. Claude's own reading, labelled: "inlet streams" = each stream that flows straight into the lake, whole from the
lake to its source; an inlet's own tributaries are not inlet streams (they keep the zone rules).

## What was there
r1 bound 2 sections: the outlet from the lake to the falls (356560775:599, 2,424 m) and blue line 356560775 above the lake
(356560775:8623, the T637 "Whiteswan Lake inlet stream", fed by Moose Lake). The lake has 15 inlets (lake_in edges in
the candidate atlas): that one, Cave Creek (gnis:11699), Inlet Creek (gnis:10239) and 12 unnamed lines with no item.
r2-r4 (below the falls) bound 356560775:0 (0-599.2 m, the falls cut to the White River). The falls cut
(`whiteswan_lake_s_inlet_outlet_streams__whiteswan_outlet_falls`, an obstacle point at 599.2 m, 2,424 m below the lake =
the book's 2.4 km) already exists: no new cut.

## Built (c41d54742)
- `name_variants.json`: the 12 unnamed inlet blue lines (whole) named "Whiteswan Lake inlet stream" (source regulation,
  display); each becomes its own item `wsc:300-625474-814939-312002-<code>` at the next atlas build (dry run on the
  candidate graph: 12 items, one section each). The T637 name stays (it is an inlet, it stays closed).
- `region-4.json` (backed up first, diff = this entry only): r1 gains `{op: whole, item_ids: [the 14 inlet items]}`;
  the falls extents of r1-r4 are scoped to the outlet item. `book_name_links.json`: note only (the row's water stays the
  outlet).
- Bound (dry run, `build_reach` on the Whiteswan subgraph): live atlas — r1 = outlet above falls + 356560775:8623 + Cave
  Creek + Inlet Creek (4; the 12 unnamed have no item yet and are skipped); after the atlas build — r1 = 16 sections; r2-r4
  = 356560775:0 in both.
- Tests: `pipeline/tests/test_whiteswan_inlets_t690.py` (the real row bound on a Whiteswan-shaped graph: inlets and the
  outlet above the falls closed, never the lake, an inlet's tributary or Moose Lake; the exception only below the falls;
  dates and quota; every inlet named). Fails on the old row (2 tests).

## The card an angler sees (from the rules; not rendered on a device)
- An inlet stream: "No Fishing EXCEPT the outlet stream downstream of the falls 2.4 km downstream of Whiteswan Lake" — closed
  all year (the water's own closure silences the zone's quotas).
- The outlet above the falls: the same, closed all year.
- The outlet below the falls, Jul 1: open — rainbow trout daily quota 5 (replaces Region 4's "2 from streams"), catch and
  release all other game fish (crayfish may be kept), with the Region 4 gear rules.
- The outlet below the falls, Sep 1: "No Fishing Aug 1-Mar 31" — closed.
- Final atlas build 2026-11-12 (`full_next`, not promoted): the 12 inlets are items (Whiteswan Lake inlet stream); r1 binds
  16 sections (was 2); r2-r4 unchanged. Side effect: 7 short unnamed headwater lines (271-923 m) above two of the named
  inlets are now kept as sections; they carry the Region 4 zone rules and the White River's tributary rules (r2-r4), not r1
  (Claude's reading: a tributary of an inlet is not an inlet of the lake).
