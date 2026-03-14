# Regulation Index Design — V2 Decoupled Architecture

## Goal

Decouple regulation data from PMTiles so that:
- **PMTiles are immutable** — physical water features only, never rebuilt for regulation changes
- **Regulations are lightweight JSON** — updated independently (in-season, annual synopsis)
- **Frontend can highlight, search, and display regulations** without any regulation data on tiles

## Core Concept: Reaches

A **reach** is a maximal contiguous group of segments on the same waterbody that share
the **exact same regulation set**. This is the atomic unit for highlighting, searching,
and regulation display.

```
reach_id = md5(fwa_watershed_code | display_name | sorted_reg_ids)[:12]
```

Hash: MD5 truncated to 6 bytes (12 hex chars). At ~600K reaches, collision probability
is negligible (birthday bound for 48-bit space is ~16M).

This is identical to the old `frontend_group_id = md5(watershed_code|name|reg_ids)` —
but it lives **in JSON only**, never baked into tiles.

### Why reaches must cover every segment

If "Kootenay River" has 200 segments and only segments 1–80 have synopsis regs, those
80 form reach A. But segments 81–200 still have zone + provincial base regs. If we
DON'T assign them a reach, any highlighting by WSC or BLK will include them alongside
reach A — wrong behavior. So segments 81–200 form reach B (different reg set = different
reach). **Every segment belongs to exactly one reach.**

### Why WSC, not BLK

A single named waterbody (e.g., "Thompson River") spans **multiple BLKs**. When a user
clicks any segment, the entire waterbody's reach(es) should be available for
highlighting and navigation. WSC groups the full named waterbody; BLK is too granular.

BLK (blue_line_key) remains useful as a sub-grouping within a WSC, but the primary
reach grouping key is `fwa_watershed_code`.

### Reach boundaries form at

- **Regulation boundaries**: where synopsis-specific regs start/end on a waterbody
- **Zone boundaries**: where a stream crosses from zone A to zone B (different zone regs)
- **MU boundaries**: where management unit rules differ
- **Tributary inheritance**: regs with "includes tributaries" propagate via FWA watershed
  code prefix tree (e.g., parent code `300-123456` matches all child codes starting
  with `300-123456-...`). **This is tier 1 — must be handled at build time.**
- **Tier 2 (future)**: landmark-based scoping ("upstream of Hwy 1 bridge")

### Polygon features (lakes, wetlands, manmade)

Polygons use `waterbody_key` as their identifier (not `fid`). Tile properties for
polygons are `{ waterbody_key, display_name, area }`. The frontend uses `waterbody_key`
directly for both regulation lookup and highlighting — no separate reach_segments
entry needed.

Stream tiles carry `{ fid, display_name, blk, stream_order, fwa_watershed_code }`.
The frontend uses `fid` to look up the reach.

---

## Data Model: Five JSON Sections

The regulation index is a **single JSON file** (`regulation_index.json`) shipped to the
frontend alongside `fresh_water_atlas.pmtiles`. It contains five top-level sections:

### 1. `regulations` — reg_id → regulation info

The **master regulation dictionary**. One entry per regulation. For now, each entry is
the raw/unparsed synopsis row. In future, individual rules with parsed scope, dates,
restriction details.

```jsonc
{
  "R2_ALICE_LAKE_2-7": {
    "water": "ALICE LAKE",
    "region": "Region 2",
    "mu": ["2-7"],
    "raw_regs": "Daily quota of 2 ...",
    "symbols": ["C"],
    "source": "synopsis",      // "synopsis" | "provincial" | "zone" | "in_season"
    "page": 42,
    "image": "p42_alice_lake.webp"
  },
  "ZONE_2_BASE": {
    "raw_regs": "General zone 2 regulations ...",
    "source": "zone",
    "zone": "2"
  },
  "PROV_BASE_TROUT": {
    "raw_regs": "Province-wide trout regulations ...",
    "source": "provincial"
  },
  "INSEASN_2026_03_KOOTENAY": {
    "raw_regs": "Emergency closure effective March 10 ...",
    "source": "in_season",
    "effective_date": "2026-03-10"
  }
}
```

**Size estimate**: ~1,602 synopsis regs + ~20 zone regs + ~10 provincial regs +
~20 in-season → ~1,650 entries. At ~300 bytes avg → **~500 KB** (negligible).

### 2. `reaches` — reach_id → reach metadata

The **reach dictionary**. Associates each reach with its regulation set, display info,
and feature type. This is what the InfoPanel reads.

```jsonc
{
  "a1b2c3d4e5f6": {
    "dn": "Alice Lake",           // display_name
    "nv": ["Alice Lk"],           // name_variants (only on reaches, not duplicated in search)
    "ft": "lake",                 // feature_type: "stream" | "lake" | "wetland" | "manmade"
    "rg": ["Region 2"],           // regions this reach spans
    "ri": 0,                      // reg_set_index — scalar pointer into reg_sets array
    "wsc": "300-123456",          // fwa_watershed_code (streams) or waterbody_key (polygons)
    "mz": 9                       // minzoom (lowest of its segments)
  }
}
```

**`reg_sets`** deduplication (same as old system): regulations are referenced by index
into a dedup array rather than inlined, since many reaches share the same reg set.

```jsonc
"reg_sets": [
  "PROV_BASE_TROUT,ZONE_2_BASE,R2_ALICE_LAKE_2-7",   // index 0
  "PROV_BASE_TROUT,ZONE_3_BASE",                       // index 1
  ...
]
```

**Size estimate**: ~600K reaches (≈ unique WSC count + zone/MU splits). Must be
validated by running the builder on actual atlas data before finalizing.
At ~100 bytes avg (short keys, scalar ri) → **~60 MB** raw, **~5-7 MB gzipped**.

### 3. `reach_segments` — reach_id → [fid, fid, ...]

The **highlighting table**. Maps each **stream** reach to the linear_feature_id fids it
contains. This is how the frontend knows which tile features to illuminate.

```jsonc
{
  "f7e8d9c0b1a2": ["70012345", "70012346", "70012347", ...]  // stream: linear_feature_ids
}
```

**Polygon features are NOT in reach_segments.** Lakes, wetlands, and manmade features
use `waterbody_key` for both lookup and highlighting — the tile already carries this
property, so `setFilter(['==', ['get', 'waterbody_key'], key])` works natively.
No separate fid list needed.

**Size estimate** (streams + under-lake streams only):
- ~2.6M total stream fids across ~550K stream reaches
- Forward map: 550K keys × 16 bytes + 2.6M fids × 10 bytes ≈ **35 MB** raw
- **Gzipped: ~6-8 MB** (numeric fid sequences compress well)

### 3b. `poly_reaches` — waterbody_key → reach_id

A lightweight reverse map for polygon features. When a user clicks a lake/wetland/
manmade tile feature, the frontend looks up: `poly_reaches[waterbody_key] → reach_id`.

```jsonc
{
  "329532498": "a1b2c3d4e5f6",    // Alice Lake waterbody_key → reach_id
  ...
}
```

**Size estimate**: ~720K polygon features × ~25 bytes = **~18 MB** raw, **~2 MB gzipped**.

### 4. `search_index` — searchable name → reach references

The **Fuse.js-compatible search table**. One entry per unique named waterbody,
searchable by display_name and name_variants.

```jsonc
[
  {
    "dn": "Alice Lake",                      // display_name (Fuse.js weight 3)
    "nv": ["Alice Lk"],                      // name_variants (Fuse.js weight 2)
    "reaches": ["a1b2c3d4e5f6"],             // all reach_ids for this waterbody
    "bbox": [-123.4, 49.1, -123.3, 49.2],   // bounding box for fly-to
    "ft": "lake",                            // feature_type
    "rg": ["Region 2"],                      // all regions
    "mz": 9                                  // min_zoom
  },
  {
    "dn": "Kootenay River",
    "nv": [],
    "reaches": ["f7e8d9c0b1a2", "c3d4e5f6a7b8"],  // multiple reaches (zone split)
    "bbox": [-116.8, 49.0, -115.5, 50.8],
    "ft": "stream",
    "rg": ["Region 4", "Region 8"],
    "mz": 5
  }
]
```

**Every named waterbody** appears here — whether it has synopsis-specific regs or only
base zone/provincial regs. This ensures all named streams, lakes, and wetlands are
searchable.

**Unnamed features** (no `display_name`): Not in the search index. They ARE in
`reach_segments` so they can be highlighted when clicked on the map. Their regulations
are looked up via `reaches[reach_id].ri → reg_sets[i]`.

**Size estimate**: ~19,000 named waterbodies (matching old system). At ~150 bytes avg
→ **~3 MB** raw, **~400 KB gzipped** (negligible).

---

## Total Size Budget

| Section | Raw JSON | Gzipped | Notes |
|---------|----------|---------|-------|
| `regulations` | ~500 KB | ~80 KB | ~1,650 entries |
| `reg_sets` | ~200 KB | ~30 KB | Dedup'd reg-ID strings |
| `reaches` | ~60 MB | ~6 MB | ~600K reach entries (VALIDATE) |
| `reach_segments` | ~35 MB | ~7 MB | 2.6M stream fid mappings |
| `poly_reaches` | ~18 MB | ~2 MB | 720K polygon waterbody_key→reach |
| `search_index` | ~3 MB | ~400 KB | ~19K named waterbodies |
| **TOTAL** | **~117 MB** | **~16 MB** | Old system: 45 MB raw / ~8 MB gz |

**The ~600K reach estimate must be validated** by running the builder on real atlas
data before implementation. If it's significantly higher, the architecture still holds
but the load strategy becomes more critical.

---

## Loading Strategy (MANDATORY)

**15-16 MB gzipped cannot be loaded as a single blocking request.** Mobile users on
3G (~1.5 Mbps) would wait ~80 seconds; even 4G (~12 Mbps) is ~10 seconds for download
plus 3-6 seconds JSON parse plus 2-4 seconds to build the segmentIndex. Total TTI on
4G: ~16-20 seconds. **Unacceptable.**

### Phase 1: Immediate load (~600 KB gzipped)

Load on app start — enough for search and regulation display:

```
search_index  (~400 KB gz)  → Fuse.js search works
regulations   (~80 KB gz)   → InfoPanel can display reg details
reg_sets      (~30 KB gz)   → Regulation set dedup
```

**User can search, read regulations, and navigate immediately.**

### Phase 2: On-demand load (per viewport or first click)

Load when the user interacts with the map:

```
reaches       (~6 MB gz)    → reach metadata for InfoPanel
poly_reaches  (~2 MB gz)    → polygon click → reach resolution
```

Can be deferred until first map click, or loaded lazily per-region.

### Phase 3: Lazy/sharded reach_segments (~7 MB gz total)

The largest section. Options (pick one):

**Option A: Regional shards** (recommended)
Split `reach_segments` into 8 files by region. Only load the shard(s) covering the
user's current viewport. A user zoomed into Region 4 loads ~1 MB, not 7 MB.

**Option B: Binary format**
Ship as a typed binary file (sorted Int32Array of fids + offset table for reaches).
Eliminates JSON parse overhead. ~4 MB gzipped, instant load into typed array.

**Option C: Full load with Web Worker**
Load the full `reach_segments` JSON in a Web Worker to avoid blocking the main thread.
Build `segmentIndex` (fid → reach_id Map) in the worker. Lookups via `postMessage`.

### IndexedDB caching

All loaded sections are cached in IndexedDB with ETag validation (same pattern as V1).
Repeat visits load from cache instantly. Service Worker caches for offline use.

### segmentIndex memory budget

Building a JS `Map` from 2.6M stream entries:
- Memory: ~130-200 MB heap (each Map entry has ~50-100 bytes overhead)
- Build time: 1-3 seconds desktop, 4-8 seconds mobile

**This must be benchmarked before committing.** If memory is too high:
- Use a compact representation (sorted array + binary search)
- Or keep lookups in a Web Worker (doesn't count against main thread heap)
- Or use the regional-shard approach (only ~300K entries loaded at a time)

### Compression Strategy

Cloudflare R2 serves gzip/brotli automatically. Raw JSON never reaches the client.

**Additional potential optimizations** (if benchmarks require):
- Integer-encoded reach_ids (replace 12-char hex with array indices)
- Delta-encoded fid arrays (sequential linear_feature_ids compress better)
- MessagePack/CBOR instead of JSON (smaller, faster parse)

---

## Frontend Data Flow

### Click → InfoPanel (Stream)

```
1. User clicks stream tile segment
   Tile returns: { fid: "70012345", blk: "356346234", display_name: "Kootenay River",
                   fwa_watershed_code: "300-123456", stream_order: 4 }

2. Lookup fid → reach_id
   segmentIndex["70012345"] → "f7e8d9c0b1a2"
   (segmentIndex is a Map built from inverting reach_segments — see Loading Strategy)

3. Get reach metadata
   reaches["f7e8d9c0b1a2"] → { dn: "Kootenay River", ri: 3, ft: "stream", ... }

4. Get regulation IDs
   reg_sets[3] → "PROV_BASE_TROUT,ZONE_4_BASE,R4_KOOTENAY_RIVER_4-3"

5. Get regulation details
   regulations["R4_KOOTENAY_RIVER_4-3"] → { raw_regs: "...", source: "synopsis", ... }
   regulations["ZONE_4_BASE"] → { ... }
   regulations["PROV_BASE_TROUT"] → { ... }

6. InfoPanel renders all regulations for this reach
```

### Click → InfoPanel (Polygon — lake/wetland/manmade)

```
1. User clicks lake tile polygon
   Tile returns: { waterbody_key: "329532498", display_name: "Alice Lake", area: 50000 }

2. Lookup waterbody_key → reach_id
   poly_reaches["329532498"] → "a1b2c3d4e5f6"

3. Get reach metadata (same as stream step 3)
   reaches["a1b2c3d4e5f6"] → { dn: "Alice Lake", ri: 0, ft: "lake", ... }

4-6. Same as stream flow
```

### Click → Highlighting (Stream)

```
1. From step 2 above: reach_id = "f7e8d9c0b1a2"

2. Get all fids in this reach
   reach_segments["f7e8d9c0b1a2"] → ["70012345", "70012346", "70012347", ...]

3. Apply MapLibre filter
   Small reaches (< ~500 fids — covers p99+ of reaches):
     setFilter('hl-streams', ['in', ['get', 'fid'], ['literal', [...fids]]])

   Large reaches (> 500 fids — rare, specific to major rivers):
     Use BLK-based pre-filter: collect unique BLKs from the fid list,
     then setFilter('hl-streams', ['in', ['get', 'blk'], ['literal', [...blks]]])
     NOTE: BLK filter is approximate (may include fids from adjacent reaches
     on the same BLK if zone splits occur within a BLK). Accept this minor
     over-highlighting for the rare large-reach case, or use feature-state API.
```

### Click → Highlighting (Polygon)

```
1. Tile gives waterbody_key directly.
   setFilter('hl-lakes-fill', ['==', ['get', 'waterbody_key'], '329532498'])
   No reach_segments lookup needed.
```

### Search → Selection

```
1. User types "Koot" in SearchBar

2. Fuse.js searches search_index by dn + nv fields
   Match: { dn: "Kootenay River", reaches: ["f7e8...", "c3d4..."], bbox: [...], ... }

3. If single reach → select directly
   If multiple reaches → show DisambiguationMenu (one per reach, labelled by region)

4. User selects reach → map.flyTo(bbox) + highlight + InfoPanel (same as click flow)
```

### In-Season Updates

In-season changes (emergency closures, openings) are handled by **rebuilding the
regulation_index.json** rather than a runtime overlay. Rationale:

- The regulation_index is a JSON file, not a tile set. Rebuilds take seconds, not hours.
- Adding a regulation to a reach changes its `reach_id` (hash includes reg_ids).
  A runtime patch would violate the hash invariant and break deep links.
- The build pipeline is already automated; adding a scraper trigger is trivial.

```
1. in_season_scraper runs (manual or cron)
2. Outputs in_season_changes.json (same as today)
3. regulation_builder.py reads synopsis + zone + provincial + in_season → regulation_index.json
4. Deploy regulation_index.json to R2 (new ETag → frontend cache-busts)
```

**Staleness window**: Defined by scraper + deploy frequency. With a 1-hour cron,
worst case is 1 hour stale. Frontend shows "Last updated: {timestamp}" prominently.

**For future real-time needs**: A lightweight `in_season_overlay.json` could add
*new regulations* to the `regulations` dict and append them to existing reaches
(treating the overlay as additive-only, not modifying reach_ids). But the simpler
full-rebuild approach is correct for now.

---

## Build Pipeline

```
match_table.json + atlas.pkl + zone_regs + provincial_regs
        ↓
  regulation_builder.py         ← NEW MODULE
        ↓
  regulation_index.json         ← Single output file for frontend
        ↓  (also)
  in_season_overlay.json        ← Hot-swappable overlay
```

### regulation_builder.py responsibilities

1. **Load atlas** — all streams, lakes, wetlands, manmade with fids and WSC/WBK
2. **Load match_table.json** — synopsis regs linked to FWA refs
3. **Load zone/provincial base regs** — apply per zone/province-wide
4. **Assign every segment to a reach**:
   - Group segments by WSC (streams) or waterbody_key (polygons)
   - Within each group, sub-group by regulation set (zone + synopsis + provincial)
   - Each sub-group = one reach
   - reach_id = hash(wsc|display_name|sorted_reg_ids)
5. **Build all four tables**: regulations, reaches, reach_segments, search_index
6. **Serialize** to regulation_index.json (orjson for speed)

### Data dependencies

| Input | Source | What it provides |
|-------|--------|-----------------|
| `atlas.pkl` | FreshWaterAtlas | All segments with fid, WSC, BLK, display_name, WBK, minzoom |
| `match_table.json` | BaseEntryBuilder | Synopsis reg → FWA refs (waterbody_key, gnis_id, etc.) |
| Zone reg definitions | Config/manual | Zone → base regulation entries |
| Provincial reg definitions | Config/manual | Province-wide regulation entries |
| `in_season_changes.json` | InSeasonScraper | Live in-season changes |

---

## Highlighting Strategy: The MapLibre Filter Problem

MapLibre's `setFilter(['in', ['get', 'fid'], ['literal', [...]]])` has performance
limits. Testing thresholds:

| fids in filter | Expected perf | Approach |
|---------------|---------------|----------|
| 1–50 | Instant | Direct `in` filter |
| 50–500 | Fast | Direct `in` filter (MapLibre handles well) |
| 500–2000 | Acceptable | Direct `in` filter with debounce |
| 2000+ | May lag | Use coarser filter (BLK or WSC) as pre-filter |

**Median reach size**: ~4 segments (matches BLK median). **p99**: ~41 segments.
**Max**: 4,148 (single BLK). Most reaches will be well under the performance threshold.

For the rare very large reaches (>2000 segments), the frontend can:
1. Use `['==', ['get', 'blk'], value]` if the reach aligns with a single BLK
2. Use `['in', ['get', 'blk'], ['literal', [blk1, blk2, ...]]]` for multi-BLK reaches
   (far fewer BLKs than fids)

---

## Open Questions

1. **WSC completeness**: `fwa_watershed_code` is loaded for every stream segment in the
   atlas (see `freshwater_atlas.py`). **Verify**: are there any segments where this field
   is null/empty? If so, BLK + display_name is the fallback grouping key.
   _Action: Run a quick check during regulation_builder.py development._

2. **Under-lake streams**: 325K segments are hidden in PMTiles but exist in the atlas.
   They MUST be included in `reach_segments` so that clicking a visible lake finds the
   correct regulation set (the lake may share a regulation with the stream flowing through
   it). If they are excluded, clicking a lake that shares regulations with its underlying
   stream produces incomplete results.

3. **Admin boundaries**: Parks and eco reserves are not in the synopsis — they have
   their own regulation source. For the initial build, admin boundaries are *not* part
   of the reach system. They can be overlaid visually and queried separately via their
   `admin_id` tile property. Consider a future `admin_regulations.json` if needed.

4. **Reg ID format** (settled):
   - Synopsis: `"{region}_{name}_{mus}"` (e.g., `"R2_ALICE_LAKE_2-7"`)
   - Zone: `"ZONE_{zone_id}_BASE"` (e.g., `"ZONE_2_BASE"`)
   - Provincial: `"PROV_{category}"` (e.g., `"PROV_BASE_TROUT"`)
   - In-season: `"INSEASN_{year}_{seq}_{name}"` (e.g., `"INSEASN_2026_03_KOOTENAY"`)

5. **segmentIndex memory budget**: 2.3M stream entries in a JS Map ≈ 130-200 MB heap.
   Must benchmark in Chrome/Safari before committing to in-memory approach. Alternatives
   if too large: (a) Web Worker with message passing, (b) sorted typed array + binary
   search, (c) regional binary shards loaded on demand.
   _Action: Build a quick prototype with 2.3M entries during frontend development._

6. **Future landmark scoping**: When tier 2 (landmark-based sub-segments) arrives,
   reaches will split further within a WSC. The architecture supports this — new reaches
   are added, existing ones shrink, reach_segments updated. No tile changes needed.

---

## Migration Path from V1

| V1 Concept | V2 Equivalent |
|-----------|---------------|
| `frontend_group_id` on tiles | `reach_id` in JSON only |
| `waterbody_data.json` (monolith) | `regulation_index.json` (structured) |
| `compact[fgid] → reg_set_index` | `segmentIndex[fid] → reach_id → reg_set` |
| `regulations` dict in waterbody_data | `regulations` dict in regulation_index |
| `identity_meta` for synopsis dedup | Absorbed into `regulations` entries |
| `regulation_segments` nested in waterbodies | `reach_segments` top-level (flat) |
| PMTiles with regulation data | PMTiles regulation-free |
| Tile rebuild on reg change | JSON update only (tiles stay) |

---

## Summary

The regulation index is a single JSON file with five sections that together provide
everything the frontend needs for highlighting, searching, and displaying regulations
— all without any regulation data on the tiles. The tiles remain a permanent, immutable
physical map of BC's water features. When regulations change, only the JSON is rebuilt
and redeployed (seconds, not hours). In-season changes trigger a full index rebuild
(fast, automated) rather than a runtime overlay, preserving the hash invariant.
