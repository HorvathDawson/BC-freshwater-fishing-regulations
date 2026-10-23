# T64 — Tiles: size wins (S2) + the pending tile rebuild

Status: OPEN. One tile build covers: parcels from z11 (style already asks for it), basins coverage simplify (100 m),
and the S2 easy size wins. Never replace the tiles dir: promote pmtiles + meta + layers/*.geojsonl file by file.
Tiles and bundle share a vintage (rebuild both or check the handle digest). Acceptance: size table before/after;
private land visible from z11; no basin seams; strict + smoke.

2026-10-22 (release size, not tiles): `store.common.train_zdict` is now best-of-8 sample offsets (keeps the dictionary
that packs the table smallest; deterministic). One offset let a 0.6% rows_row change swap the dictionary (regs-3 gz
319,894 → 427,578) and refuse the release at 14,027,828 B; same answers now pack to 13,844,550 B gz (gate 14,000,000).
Store step 8.4 s → ~31 s. Pinned in `test_regs_store.py` (two-build identity + a 0.6% append swings < 3%, old rule +9%).
