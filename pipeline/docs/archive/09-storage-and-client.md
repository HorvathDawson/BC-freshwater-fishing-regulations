# 06 — Storage & Client Delivery (from scratch)

Grounded in the measured current state (verified 2026-07-25). We keep the good ideas
(static R2 + one Cloudflare Worker, path-versioned immutable shards, edge caching) and
rebuild the *shape* of the data around a self-identifying `section_id`.

## Current state (measured) — the problem

- **`tier0.json` = 24.9 MB (raw), ~5 MB gz, loaded eagerly at boot.** 90% is `search_index`;
  **~16 MB of that is embedded `fids[]` highlight lists** (713,721 fid strings) whose only
  job is to highlight the clicked feature.
- **Click path** = `queryRenderedFeatures` → fid → Worker `/api/resolve` → fid-shard →
  reach_id → reach-shard → reg_set_index → in-memory join against tier0 `reg_sets` +
  `regulations`. Shards on disk = 923 MB (12,288 files).
- **Mobile** = one ~1.2 GB SQLite (tables `fids`, `polys`, `reaches`, `reg_sets`,
  `regulations`, `search` + FTS5), dominated by the same fid↔reach mapping.

Two-thirds of the boot payload and most of the shard/DB plumbing exist only to translate
**fid → reach** because tiles today are keyed by fid and carry no regulation identity.

## The core move: self-identifying `section_id` tiles

Section tiles carry `section_id` (and cheap display keys) as feature properties. That single
change cascades:

1. **Highlight via `section_id`** using MapLibre `feature-state` / filter — **delete the 16
   MB `fids[]`** from the payload entirely.
2. **Delete the `fid → reach` chain** — no fid-shards, no `/api/resolve` round-trip for the
   common case. Click → `section_id` straight off the tile.
3. **Mobile drops the `fids` and `polys` tables** and per-reach fid blobs → the DB shrinks
   proportionally.

## Proposed delivery shape

### Vector tiles (PMTiles) — one geometry spine
- Layer `sections` keyed by `section_id`, props: `section_id, display_name, min_zoom,
  stream_order`, and a compact `reg_set_index`. **Each section is single-reg-set** (Fraser-
  type per-MU differences are separate sections via curated `mu_boundary` splits, `06`), so
  there is **no per-zone reg_set and no render-clip layer** — a big simplification over the
  earlier draft.
- Layer `under_lake_sections` (floor z10) + `lakes`/polys keyed by `waterbody_key`
  (unchanged join for lakes).
- The MU **base-reg overlay** (`06`) reuses the existing `wmu`/`regions` tile layers for
  point-in-MU resolution — no new geometry needed for it.
- Side channels (separate BLK sections) get higher `min_zoom` → drop out when zoomed out,
  exactly as required.

### Boot payload — tiny bootstrap, everything else lazy
Split today's tier0 into:
- **`boot.json`** (target ≪ 1 MB gz): per-section `{section_id, display_name,
  name_variants[], bbox, min_zoom, zone_ids}` — just enough for search + labels.
- **`reg_sets.json` + `regulations.json`** (~2.6 MB today): **lazy**, fetched on first click
  or first search-result open, then cached (IndexedDB web / on-device mobile). The reg *text*
  is not needed to render the map or run search.
- **`nearby_towns` / full `name_variants`** (~1.7 MB): lazy or server-side.

Result: first paint needs the basemap + section tiles + a sub-1 MB bootstrap, versus ~5 MB
gz today.

### Regulation resolution on click
`section_id` (from tile) → look up `reg_set_index` (from tile prop or a small
`section→reg_set` chunk) → expand against `reg_sets` + `regulations` (lazy-loaded once). No
per-click network in the steady state. If we prefer to keep reg data server-side, a single
`/api/section/{id}` Worker route returns the expanded regs (the Worker already has the shard
infra); but tile-embedded `reg_set_index` + lazy tables is lighter and offline-friendly.

### Search
- Keep client Fuse.js but over the **tiny bootstrap** (names + variants only). Lazy-load
  `nearby_towns` only when a town query needs it.
- Or: a prefix-bucketed **search shard** set (or Worker FTS mirroring the mobile FTS5) so the
  client downloads ~nothing for search at boot. Recommend starting with bootstrap-Fuse
  (simplest) and measuring.

### Encoding
`search_index`/section attributes are highly repetitive JSON (same keys × ~19k+). Consider a
**columnar or flatbuffer/protobuf** encoding for `boot.json` and reg chunks — meaningful wins
beyond gzip. Optional; do after the structural wins (they dwarf compression).

## Mobile, rebuilt from the ground up

Same `section_id` spine → unify the data path with web:
- Tables: `sections(section_id, display_name, bbox, min_zoom, zone_ids, reg_set_index …)`,
  `reg_sets`, `regulations`, `search_fts`. **Drop `fids`, `polys`, per-reach fid lists.**
- Tap = `queryRenderedFeatures` → `section_id` → `SELECT reg_set_index FROM sections` → expand
  (identical logic to web). Same PMTiles range-read from R2.
- The DB shrinks substantially (the fid/poly mapping tables and reach fid blobs were the
  bulk). Search stays FTS5 + Fuse rerank.

## Versioning / caching (keep, adapt)
- Path-versioned immutable artifacts (`v2/…`), `data_version.json` cache-buster, ETag +
  IndexedDB for the bootstrap. Worker keeps edge-caching tiles/chunks. `SHARD_VERSION` →
  `SECTION_VERSION`. Cutover = version bump; rollback = previous version still on R2.

## Ranked wins (from the measurement)
1. Self-identifying `section_id` tiles → delete `fids[]` (≈16 MB) + `/api/resolve` fid chain. **Biggest.**
2. Tier0 → sub-1 MB bootstrap; reg text + towns lazy.
3. Mobile: drop `fids`/`polys` tables.
4. Server-side/prebuilt search (optional).
5. Columnar/binary bootstrap (optional).
