# T64 — Tiles: size wins (S2) + the pending tile rebuild

Status: OPEN. One tile build covers: parcels from z11 (style already asks for it), basins coverage simplify (100 m),
and the S2 easy size wins. Never replace the tiles dir: promote pmtiles + meta + layers/*.geojsonl file by file.
Tiles and bundle share a vintage (rebuild both or check the handle digest). Acceptance: size table before/after;
private land visible from z11; no basin seams; strict + smoke.

2026-10-22 (release size, not tiles): `store.common.train_zdict` is now best-of-8 sample offsets (keeps the dictionary
that packs the table smallest; deterministic). One offset let a 0.6% rows_row change swap the dictionary (regs-3 gz
319,894 → 427,578) and refuse the release at 14,027,828 B; same answers now pack to 13,844,550 B gz (gate 14,000,000).
Store step 8.4 s → ~31 s. Pinned in `test_regs_store.py` (two-build identity + a 0.6% append swings < 3%, old rule +9%).

2026-10-23 (release size, the dictionary trainer again): the T128/T103/T104/T127 answers (sha 821bc30d) were refused at
14,010,163 B (regs-3 319,894 → 450,423). REAL content cost, packed with the live release's own dictionaries
(r-36fbc5326506): old answers 13,844,550 (= live, exact) → new 13,859,022, **+14,472 B**. The other +151 KB was all
`rows_row`'s dictionary: its 8 candidates (6 whole rows each) all packed it to ≥1,044,024 B where the live one did
890,072. Fix (`store.common.train_zdict`): prescore every sample offset (≤512) on every 16th blob, and score the first 8
plus the prescore's top 4 on the whole table (the first 8 always stay finalists, so never worse than best-of-8); a
region-shard table's rows are weighted by how many shards ship them (`release.shards.row_regions`, the release's own
cut — pinned equal to `shard_rows`). Weighting changes nothing on today's data (every shard row ships, ~1.02 copies);
it is there so a table whose bulk ships nowhere is not trained on it. Result: new answers **13,843,064** (−156,936
under; regs-3 319,120), live answers 13,817,723 (content cost under the new trainer +25,341). Store 30 s → ~60 s,
release 9 s. Q55 (raise the gate / drop private land from access) is not forced by this change; still the owner's.
