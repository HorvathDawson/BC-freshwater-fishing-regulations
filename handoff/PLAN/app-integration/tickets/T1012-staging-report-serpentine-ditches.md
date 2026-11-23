# T1012 — Staging report 2026-11-22: Serpentine River shows ditches on its mainstem

Status: IN PROGRESS — registry fix 034f9a7b8, proved on a side atlas (lane L6, 2026-11-22); left (Claude): ship it with the 0.9.0 data release's atlas rebuild (promote needs `--accept-boundary-loss`: 2 automatic lake boundaries, below).

Source: `GET /reports` on the STAGING feeds worker, read 2026-11-22 18:15 (report `e552b9f0b31e2698`). Android 0.8.5
(build 43, 558c527), staging, release `r-741815c6090d`, tiles `4ae7b62955bc9990.fc51e82f7c91`; water · map, salmon
mode, Serpentine River (`gnis:24978`) part 2, zoom 11 at 49.112, -122.788. Reason "rule". Verbatim: "Has ditches on
mainstem? Seem like they shouldn't be included".

## Work
1. List the lines in `gnis:24978` part 2 that FWA codes as ditches (edge type 2300 / named "ditch"), with their gnis.
   `registry.build.DITCHES_LEAVE_RIVER_ITEMS` drops a 2300-only ditch with no gnis of its own; say why each one shown
   stayed (own gnis = the river's? a mixed edge type? a salmon-mode DFO place drawing it?).
2. If they should leave: fix where the item is built (registry), never in a reader; diff the reach run's rule bindings
   (rule 63) before it ships. If they are the river by FWA's own name, show the owner a picture and ask.

Depends: none. Size: S–M. Owner: maybe (a picture if FWA names them the river).

## Found and fixed (lane L6, 2026-11-22)
- **What part 2 shows.** The report's point (49.112, -122.788) is on the Serpentine's bundle part `part_ix` 2 (4
  sections). Two are not the river: `355995609:0` (2,844 m) and the 1450-only stubs of `355998101` (17 m + 11 m).
  Part `part_ix` 3 holds three more: `355993717:1283`, `355993718:0`, `355996889:0`. All carry no gnis and only the
  river's inherited name (`side_channel`).
- **Why they stayed.** They are the 168th St drainage grid that `DITCHES_LEAVE_RIVER_ITEMS` (T67) was written for
  (its comment names blk 355995609 and 355996889), but FWA codes each piece **2300 + 1450**: the ditch with its water
  crossings (`WATER_CROSSING`, T660). `is_manmade_secondary_only` asked "every line 2300", so they were back in the river.
  Not FWA naming them the river, so no picture for the owner.
- **Fix (registry, where the item is built):** `is_ditch` reads `fwa_edge_types.is_ditch_lines`: every line a 2300 or a
  1450, at least one 2300. A crossing alone, or 1000 + 2300, is still the river (`test_registry.py`, with a mutation
  pin back to the 2300-only reading). The braid sweep reads the same predicate.
- **Province-wide (live atlas probe):** 12 such pieces, 13.9 km, in 7 rivers: Serpentine 4, Kootenay 3, Mather Creek,
  Hanson Creek, Nicomekl, Bridge Creek, Boss Creek 1 each. Left alone: the two `355998101` stubs (28 m, 1450 only,
  no 2300 on them) — too short to draw at z11.
- **Side atlas** (`pipeline.atlas.build --full`, scratch dir, 42 min; never the live atlas, never promoted):
  registry diff vs live: exactly those 12 sections leave those 7 items, no item added or removed, no other section moves.
  Parity (`promote --dry-run`): sections 4,612,870 -> 4,612,858; boundaries 11,395 -> 11,393: the automatic lake
  boundaries `serpentine_river__lake_329292861` and `mather_creek__lake_329523958` go (they were where a ditch met a
  lake; no curated file names either). The 3 gauge cuts it could not place (08HD024, 08ME002, 08MH032) are in the live
  atlas's `split_unplaced.json` too.
- **Reach run on the side atlas, binding diff vs the live run (AGENTS 63):** 16 rules and 3 DFO places, every one only
  LOSES sections, all attributable: Serpentine r1-r4 (the 4 ditch pieces, plus 2 unnamed lines that drain INTO a
  ditch, `356102834:0` 12 m and `356112428:0` 554 m, which the tributary walk reached through the ditch and now does
  not: they keep the zone rules), Kootenay upper r1-r7 (-3 each), Nicomekl r2-r6 (-1 each), DFO
  `2:serpentine-river:downstream-of-168th-street`, `…-at-both`, `2:nicomekl-river:downstream-of-208th-street` (-1 each).
  0 unresolved, 0 licensing, 0 tidal/outside, 0 diagnostics changed.
- **Needs the data release** (an atlas rebuild, then reach, bundle, tiles): the 0.9.0 release build.
