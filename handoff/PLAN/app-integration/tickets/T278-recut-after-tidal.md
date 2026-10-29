# T278 — Re-cut the app's golden regs fixtures after the tidal cuts (part renumbering)

Status: OPEN (c1029 lane 2026-10-29: `node app/tools/regs-recut/sync.mjs` stops in taps.py).

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
