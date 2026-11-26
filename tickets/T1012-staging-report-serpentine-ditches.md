# T1012 — Staging report 2026-11-22: Serpentine River shows ditches on its mainstem

Status: IN PROGRESS — registry fix 034f9a7b8, proved on a side atlas (lane L6, 2026-11-22); left (Claude): ship it with the 0.9.0 data release's atlas rebuild (promote needs `--accept-boundary-loss`: 2 automatic lake boundaries, below).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1012 | Staging report: Serpentine River (`gnis:24978`) part 2 shows ditches on its mainstem in salmon mode | IN PROGRESS — fix 034f9a7b8 (2300 + 1450 ditches leave 7 rivers, side-atlas binding diff attributed); left (Claude): ship with the 0.9.0 atlas rebuild | | staging report 2026-11-22 (`e552b9f0b31e2698`) |

Source: `GET /reports` on the STAGING feeds worker, read 2026-11-22 18:15 (report `e552b9f0b31e2698`). Android 0.8.5
(build 43, 558c527), staging, release `r-741815c6090d`, tiles `4ae7b62955bc9990.fc51e82f7c91`; water · map, salmon
mode, Serpentine River (`gnis:24978`) part 2, zoom 11 at 49.112, -122.788. Reason "rule". Verbatim: "Has ditches on
mainstem? Seem like they shouldn't be included".

## Done (lane L6, 2026-11-22, 034f9a7b8)
- **What part 2 shows.** The report's point is on the Serpentine's bundle part `part_ix` 2: `355995609:0` (2,844 m)
  and the 1450-only stubs of `355998101` (17 m + 11 m) are not the river; `part_ix` 3 holds `355993717:1283`,
  `355993718:0`, `355996889:0`. All carry no gnis, only the river's inherited name (`side_channel`).
- **Why they stayed.** The 168th St drainage grid `DITCHES_LEAVE_RIVER_ITEMS` (T67) was written for, but FWA codes each
  piece **2300 + 1450** (the ditch with its water crossings, `WATER_CROSSING`, T660), and `is_manmade_secondary_only`
  asked "every line 2300". Not FWA naming them the river, so no picture for the owner.
- **Fix (registry):** `is_ditch` reads `fwa_edge_types.is_ditch_lines`: every line a 2300 or a 1450, at least one 2300.
  A crossing alone, or 1000 + 2300, is still the river (`test_registry.py`, mutation pin). The braid sweep reads the same
  predicate. Province-wide: 12 pieces, 13.9 km, in 7 rivers (Serpentine 4, Kootenay 3, Mather Creek, Hanson Creek,
  Nicomekl, Bridge Creek, Boss Creek 1 each); the two `355998101` stubs (28 m, 1450 only) stay.
- **Side atlas** (scratch, 42 min, never promoted): exactly those 12 sections leave those 7 items. Parity (`promote
  --dry-run`): sections 4,612,870 -> 4,612,858; boundaries 11,395 -> 11,393: the automatic lake boundaries
  `serpentine_river__lake_329292861` and `mather_creek__lake_329523958` go (no curated file names either). The 3
  unplaced gauge cuts (08HD024, 08ME002, 08MH032) are in the live `split_unplaced.json` too.
- **Binding diff vs the live run (AGENTS 63):** 16 rules and 3 DFO places only LOSE sections, all attributable:
  Serpentine r1-r4 (the 4 ditch pieces, plus `356102834:0` 12 m and `356112428:0` 554 m that drain INTO a ditch: they
  keep the zone rules), Kootenay upper r1-r7 (-3 each), Nicomekl r2-r6 (-1 each), DFO
  `2:serpentine-river:downstream-of-168th-street`, `…-at-both`, `2:nicomekl-river:downstream-of-208th-street` (-1 each).
  0 unresolved, 0 licensing, 0 tidal/outside, 0 diagnostics changed.

## Work (left)
Ship with the 0.9.0 data release (an atlas rebuild, then reach, bundle, tiles); promote with `--accept-boundary-loss`
for the 2 lake boundaries above.

Depends: none. Size: S–M. Owner: maybe (a picture if FWA names them the river).
