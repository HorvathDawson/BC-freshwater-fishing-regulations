# T278 — Re-cut the app's golden regs fixtures after the tidal cuts (part renumbering)

Status: DONE — re-cut on r-fa1915a56adb (LATEST), golden parity green; tidal parts never listed in the water map.

## Problem
Release r-62e2f3a8c81e's 23 new tidal cuts renumber golden parts: Fraser (x15p*), Campbell (x16p3–p5), Brunette (x23p0),
Coquitlam (x37*), possibly Skeena, Thompson, West Road. taps.py's declaration table (PART_MOVED / PART_GONE /
PART_RECOUNTED) has no category for a part that BECAME TIDAL (no freshwater rules, off the map).

## Work
1. Add a PART_TIDAL declaration (old part → new freshwater part(s) + the tidal remainder), app-side matching exclusion.
2. Declare every renumbered golden part (from the release's split diff, mechanically); re-run sync.mjs + `pnpm -C app regs-fixture`.
3. The half-made cut is at the coordinator scratchpad `lane/regs-fixture-r-177206551d7e` (reference only).

## Acceptance
sync.mjs + regs-fixture complete on r-62e2f3a8c81e; golden parity green; each declaration cites its cut.

Depends: c1029 release. Size: S–M. Owner: no.

## Outcome (2026-10-29)
- taps.py: new `PART_TIDAL` {state: (freshwater part or None, store tidal part, why)}; checked against the store (the
  tidal part is unkeyed and unlabelled, no store part keeps the golden label). Whole-tidal states go to taps.json `tidal`
  (not `states`); a tidal remainder is tapped with `tidal` on the state (extent words excluded, like PART_GROWN).
  PART_MOVED now holds a closed golden state (no label) to the store part being closed. Each entry cites its cut
  (bundle `tidal` table: section -> split):
  - Fraser `split:fraser_river__the_cpr_bridge_at_mission` (29 sections, part 16): x15p3 tidal; parts 4..16 -> 3..15
    (x15p4..9, 11..13, 16 PART_MOVED; x15p14/15/17/18 gone now_in 13/15).
  - Campbell `split:campbell_river__cement_block_boundary_sign` (5, part 10): x16p3 remainder -> part 5; x16p4 -> 3, x16p5 -> 4.
  - Coquitlam `split:coquitlam_river__mary_hill_bypass_bridge` (1, part 3): x37p1 tidal; x37p2 back at 2.
  - Brunette `split:brunette_river__burlington_northern_railway_brid` (1, part 2): x23p0 remainder (label from the bridge).
  - Skeena `split:skeena_river__tidal_boundary` (1, part 9): x19p0 already PART_GROWN, reason extended.
  - Nitinat Lake entry `r1:nitinat_lake@1-3`: x31p (the golden's only tidal water) tidal, no key.
  120 tapped + 11 gone + 3 tidal = 134. hydrate.mjs reads `tidal`; regs-fixture re-cut (r-0a0a962d3352).
- App/conformance: `TidalTap` + `Tap.tidal` (core testing/golden.ts); exclusions.ts `PART_TIDAL_IDS` excluded in every
  block; regs-web states treat tidal as GONE. The release has no tidal key left, so the tidal banner is pinned only by
  the frozen `edge_x31p_1010.json` (fixtures.ts `frozenTidalPage`, not re-cut); the x31p tests read it. Nitinat r1 now
  carries the pipeline's "every section it selects (1) is tidal water" note (oracles.ts ORACLE_TIDAL_NOTED). Tallies
  re-pinned; handles re-pinned by section id (atlas full.prev -> full): unnamed.release, waterGroup, WaterMapView,
  WaterScreen.unnamed, WaterMode.
- Water map (coordinator 2026-10-29, owner T166 "no parts on tidal water"): `PartRow.tidal` (reads.ts: an unkeyed part
  whose `xpart.province_except` holds "tidal"); `loadWaterGroup` leaves it out (core `waterGroup` drops its stretches,
  sections and marks with it); the SEO model's water middle skips it. waterMap.test pins the Skeena (store part 9 tidal,
  never an entry); the fork test passes unchanged.
- Re-cut again on r-fa1915a56adb (Annacis/Cannery/Morey/Parsons channels whole-tidal: none is a golden water; d2858cc80
  rebinds Nitinat Lake r1): x31p is a tidal-keyed water again, so it is tapped as before (its banner tests back on the
  live release, no frozen-page path, no tidal note); 121 tapped + 11 gone + 2 tidal (x15p3, x37p1). regs fixture
  r-29e03ae863a9; handles unchanged (07f861e5422be466).
