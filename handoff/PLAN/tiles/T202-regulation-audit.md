# T202: app tile archive audit (measure + propose)

Scope (owner correction 2026-10-27): the whole app atlas we ship, which is every layer in
`atlas-<ver>.pmtiles`. The basemap is separate (see the end). This round only measured and proposed: no
rebuild of live tiles, no promote, no pipeline edits, no commits.

Live: release `r-ea426985a467` → tiles `b6b5000e171023c9.fa017a1e7896`, **239.3 MB** (238.9 MB of tiles, gzip-6
MVT, extent 4096, z4–13, camera overzooms to z18).

Method: `pipeline.tools.audit.tile_sizes` for per-zoom and per-layer numbers. A scratch decoder (`attr_scan.py`,
`cf2.py` in the session scratchpad) handled two more jobs. (1) It split every unique tile into geometry, tags, ids,
key/value tables and vertices. (2) It priced each trim by re-encoding the tiles without that thing and gzipping the
result. The re-encode reproduces the live tile bytes exactly. z≤9 used every tile; z10–13 used every 10th tile and
scaled by zoom. A bbox tippecanoe rebuild (−123..−121, 49..50, z9–13, the same flags as `tippe._common`) matched
the live interior tiles to 0.0% before any variants were tried.

## Where the bytes go

| zoom | MB (gz) | unique tiles | | layer | MB (gz share) | raw MB | geom MB | features | vertices |
|---|---|---|---|---|---|---|---|---|---|
| z13 | 168.3 | 124,219 (130,405 addressed) | | stream | 146.4 | 172.7 | 111.3 | 2.47 M | 37.5 M |
| z12 | 40.6 | 31,722 | | lake | 42.2 | 51.0 | 25.0 | 691 k | 8.8 M |
| z11 | 18.4 | 8,253 | | under_lake | 15.0 | 18.4 | 13.4 | 709 k | 3.5 M |
| z10 | 7.3 | 2,140 | | mu | 13.5 | 16.2 | 9.9 | 202 k | 3.0 M |
| z9 | 2.6 | 583 | | parcel | 10.6 | 12.5 | 12.0 | 24 k | 4.0 M |
| z4–8 | 1.7 | 231 | | park | 5.6 | 6.7 | 2.8 | 37 k | 0.9 M |
| | | | | watershed | 2.1 | 2.5 | 0.8 | 31 k | 0.2 M |
| | | | | region / indigenous / rest | 3.5 | | | | |

- **Geometry makes up about 70% of raw bytes.** z13 alone is 70% of the archive.
- **Properties, in raw bytes (keys, values and tag pairs):**
  - stream: `section_id` 21.7, `ord` 8.0, `name` 4.3
  - lake: `section_id` 7.0, `area_m2` 6.2, `water` 3.2, `name` 1.1
  - mu: `mu_id` 3.1
  - park: 3.3
- **Ids:** MVT ids are 3-byte varints (9.9 MB raw on streams). Native needs them, so they stay.
- **Gzip barely works on a single tile:** 284.5 MB raw becomes 238.9 MB gzipped (−16%). Tiles are small, and
  `section_id` and `area_m2` values are unique per feature, so they don't compress.
- **Biggest tiles** are Metro Vancouver and the Fraser Valley at z11–12, 50–56 KB each. Over 90% of those is
  `parcel` (the dissolved urban lot fabric). The z6 tile at Kamloops is 26 KB (basin, stream, park).

## What the app reads (style.json + layers.source.json + map/src)

| layer | read by the app | shipped, never read |
|---|---|---|
| stream | `ord` (width ramp, arrows), `name` (labels z11+), MVT id (taps, feature-state); web also promotes `section_id` | `section_id` *property* (duplicate of the MVT id; only web `promoteId` reads it); `name` at z4–10 |
| lake | `water` (`== / != "stream"` filters), `name` + `area_m2` (label filter and sort key, **only when named**), MVT id | `section_id` property; `area_m2` on unnamed lakes (92% of lake features); `water:"lake"` (absent already reads as "not stream") |
| wetland | `water`, MVT id | `section_id` property, `name` |
| park / eco_reserve / no_access / indigenous_land | `kind`, `name`, `area_id` (web `promoteId`) | — |
| region | `region_id` (label, useVisible) | `name` |
| mu | `mu_id` (label), polygon (4% fill + dotted edge) | — |
| **watershed, wma, sign_zone** | **no style layer, no code reader** | whole layers (≈2.3 MB) |
| under_lake, parcel, not_fishing_water, basin | geometry only (basin through the `basins` source) | — |

Taps only query section layers (`TAP_GROUPS`) and read `feature.id`. `water-copy` keeps `ord`. Nothing reads a
`section_id` *property* except the web `promoteId`.

## Ranked proposals (bytes saved × risk)

Savings are measured province-wide in gz MB unless marked *(bbox exp)*. Items 1–4 can be stacked. Measured
together, 1 + 3 + 5a + the low-zoom names give **−33.4 MB**.

| # | change | saves | what the app loses | risk / work |
|---|---|---|---|---|
| 1 | **Stop shipping the `section_id` property** on stream/lake/wetland (MVT id already carries it). In `tippe._copy_layer`, pop the property after stamping `id`. In `runtime-style.ts`, `promoteId` stops listing the section layers (GL JS then uses the MVT id, as native already does). | **−18.4** (stream −14.2, lake/wetland −4.2) | nothing (same integer, now read from `feature.id` on web too) | low-med: web change and contract/test updates (`featureId`, `test_tile_mvt_ids`, promoteId tests). Retires the old "pre-MVT-id atlas on web" fallback; `compatible` already pins the vintage. |
| 2 | **Lake/wetland attribute trim:** `area_m2` only on named features; omit `water` when it is `"lake"`. | **−6.0** | nothing: `lake__label` checks `has name` first, and `!= "stream"` is still true when the key is absent | low, pipeline only: the contract has to allow an absent attr |
| 3 | **Drop the undrawn layers** `watershed`, `wma`, `sign_zone` from the tiles (they stay in areas.json / bundle) | **≈−2.3** | nothing today. **Owner check:** are any planned for display? | low |
| 4a | **Low-zoom detail 2048** (`--low-detail=11` with `-S 5` so the ground tolerance is unchanged) at z4–12 | **≈−7** (−10% at z11–12 *(bbox exp)*; CF −6.4) | nothing: ≤0.5 px at the 2× a z≤12 tile is ever drawn at | low, one flag pair per band |
| 4b | as 4a with `--low-detail=10 -S 2.5` | ≈−13 (−19% at z11–12 *(bbox exp)*) | ≤1 px snap on z≤12 tiles at their x.99 zoom | low-med, needs shots |
| 5 | **z13 grid 2048** (`--full-detail=11`, `--simplification-at-maximum-zoom=2.5`; same 3.9 m ground tolerance) | **≈−16** (z13 −9.9% *(bbox exp)*; streams −10%) | vertices move ≤0.8 m (≤4 px at z18), inside today's 3.9 m simplification envelope. Shared nodes still coincide. | med-low: shots at z16–18 at the owner's spots. Better than raising `-S` (SMAX 8 saves the same bytes for 6 m of error). |
| 6 | **mu in its own source capped at z12, overzoomed** (the `basins` pattern) | ≈−4.5 (mu-only build 9.71 → 5.26 MB) | the MU dotted edge at z13+ comes from z12 geometry: ≤~8 m off instead of ~4 m | med: owner call, app source change |
| 7 | **Native download: archive with uncompressed tiles + transfer compression; web keeps gzip tiles** (split archives, T182 pattern) | download −46 (gzip-9 whole file 192.6 MB) / −54 (brotli 185.0) / −58 (zstd-19 181.0) vs 239.3 | nothing visible. **Disk +45 MB** on the phone (284.5 MB raw). | med: R2 `Content-Encoding` and the app downloader must decode it (gzip is decoded by NSURLSession/OkHttp for free; br/zstd are not). Owner trade: download vs disk. |
| — | names at zooms with no label (stream <z11, lake <z8) | −0.4 | nothing | not worth a third band |
| — | per-tile brotli instead of gzip | −13.6 (−5.7%) | — | **no**: pmtiles JS has no brotli by default and Native support is unverified |
| — | gzip-9 instead of 6 | −1.3 | — | not worth a repack step |
| — | merge `under_lake` pieces | −1.8 (one feature per tile); linemerge *grows* the input 303 k → 403 k parts | — | no: the input pieces don't join |
| — | Visvalingam simplification | −2.9% *(bbox exp)*, mostly parcel; stream +1.6% | shape change | no |

### Not lossless (owner options, priced)
- `under_lake` is 15.0 MB (6%) of construction lines. Raising its minzoom, e.g. to z11, saves part of the 7.2 MB it costs at z10–12.
- `parcel` is 10.6 MB, and it is all of the biggest tiles. Heavier simplification or closing gaps at z11–12 would cut
  city tiles hard, but the private-land edge would visibly change.
- mu as boundary lines plus label points: −4.6 MB, but it loses the 4% fill tint and the per-tile label repetition.
- `-S` at the top zoom 5 → 8 / 10: −6.7% / −9.4% *(bbox exp)*. Error grows to 6–8 m; option 5 is strictly better.

## Recommendation
- **Do now, in one rebuild:** 1 + 2 + 3 + 4a. About **−34 MB → ~205 MB**, and the app loses nothing.
  Item 1 is the only one with app work.
- **Then, with shots:** 5 (≈−16 → ~189 MB), and 4b if the shots show nothing.
- **Owner decisions:** 6 (mu cap) and 7 (native transfer compression vs +45 MB of disk).

## Basemap overlap
Not measured here. The ticket's 2026-10-27 z13 rule (keep trails/FSRs/resource roads and their names, drop urban
street density) is basemap-only and has its own measurement (`tile_sizes --audit-zoom 13`). The only overlap is
item 7: the same split download (uncompressed tiles + transfer compression) would apply to `basemap.pmtiles`.

Artifacts (scratchpad, session-local): `attr_scan.py`, `cf2.py`, `live_attr.json`, `cf2.json`, `t202/` (bbox
inputs, variant pmtiles, `run.sh`, `cmp.py`).

## Round 2 (owner follow-ups, 2026-10-27): measure + propose only

Error figures are maximum positional error on the ground: the simplification tolerance plus half a grid unit, at
49°N (BC's worst case; about 13% smaller at 55°N). A z13 tile is 3.21 km across, so one unit is 0.78 m at detail
12 (4096), 1.57 m at detail 11 and 3.13 m at detail 10. Today's z13 is detail 12 at `-S 5`, which is ≈4.3 m.

### 1. wma / watershed
- **wma has 35 areas.** Keeping only `CRESTON VALLEY WILDLIFE MANAGEMENT AREA`:
  - standalone build: 0.29 → 0.01 MB;
  - in the archive: **≈−0.2 MB** (wma's whole share is 0.2 MB).
  - The filter belongs on the `wildlife_management_areas` def in `data/curated/waters/areas.json` (`tile_layer`).
    The other WMAs stay cut and stamped but are not drawn: `not_drawn` exists per *def*, so this needs a second def
    or a where-clause.
- **watershed already ships only the Liard River:** one feature, 1.56 MB of input. So keeping only the Liard
  saves 0.
  - Its 2.1 MB in the archive comes from a tile-sized square, plus `area_id` and `name`, in every z13 tile across
    142,000 km².
  - **No app or bundle code reads the tile layer.** It has no style layer and nothing in app/packages queries it.
    The Conditions view colours the `basin` layer; the "watershed groups" in `core/trust.ts` are gauge groups from
    the bundle. Liard membership reaches rules through `areas.json` / `in_areas`, never through the tiles.
  - Options: drop the layer, −2.1 MB with nothing lost today. Or keep the outline and drop its two properties,
    about −0.8 MB raw, but a future label would need them.

### 2. sign_zone: what it is
- **Source:** two hand-drawn rings in `data/curated/waters/added_areas.geojson` (`kind: "sign_zone"`, both reviewed
  2026-09-21), defined as the `sign_zones` area in `areas.json`.
  - Fraser River, Landstrom Bar: 11.5 ha, `cuts: gnis:39325`.
  - Skeena River, Kispiox confluence: 14.2 ha, `cuts: gnis:2936`.
- **Geometry:** `Polygon`. Each is a small quadrilateral across the river.
- **Written by:** `export_admin` (`pipeline/deliver/tiles/export.py`), through the def's `tile_layer: sign_zone`.
  The same def drives the section cuts (`pipeline/atlas/splits/area_splits.py`, RULINGS E9), so the river is already
  split at the ring.
- **What the app does with it: nothing draws it.** There is no `sign_zone` entry in `app/packages/map/style/layers.source.json`
  and no code reads it, so the polygons ship and are never shown. What the reader sees today is the river sections
  cut at the ring, coloured by their rules.
- **Size if kept: under 1 KB.** That is 14 tile pieces at z9–13, too small to measure. Keeping it is free.
- **To display it:** a style layer is needed (fill + edge + label, and probably a tap that opens the `cuts` water).
  That is app work, a new ticket.

### 3. mu capped at z12 + overzoom (owner OK)

| how | archive saving | MU edge error at z13+ |
|---|---|---|
| features get `maxzoom: 12` in the same high band (z12 is then simplified at `-S 10`) | **≈−7.3 MB** (re-encode of the live z13 without `mu`) | ≤ ~16 m |
| mu built in its own pass with z12 as top zoom (`-S 5`), then tile-joined | **≈−6.9 MB** (−7.3, plus +0.4 MB for a finer z12, from the mu-only build) | ≤ ~8 m |

App side: a `mu` source on the same archive with `maxzoom: 12`, the same pattern as `basins`.

The saving is larger than the mu-only build suggested (−4.45 MB). On its own, mu's z13 squares dedupe, but inside
the atlas every tile also holds streams, so nothing dedupes.

### 4. Phone storage: the uncompressed-native idea is dropped
It was larger on disk (+45 MB). The only variants that are smaller on *both* download and disk:
- **gzip-9 repack:** −1.3 MB. Every reader stays compatible.
- **Per-tile brotli:** −13.6 MB (−5.7%). Not recommended: the web PMTiles reader needs a custom decompressor, and
  MapLibre Native support is unverified.

Nothing else found.

### 5. parcel
- **Per zoom, in the archive:** z11 2.0 MB, z12 3.15 MB, z13 5.4 MB. Total **10.6 MB, 4.4% of the archive.** It is
  ≥90% of every one of the biggest tiles (Metro Vancouver / Fraser Valley z11–12).
- **What it is:** one feature, a MultiPolygon of 122,469 parts. It is already the dissolve of 1.29 M private lots
  with no attributes. **"Merge adjacent parcels with the same access value" saves nothing:** there is one value, and
  touching lots are already merged. What keeps the parts apart is the road allowances between them, and closing
  those gaps would change the drawn edge.
- Measured on parcel-only builds, province-wide, z11–13 (live flags = base, 11.73 MB):

| variant | parcel MB | saves | max edge error |
|---|---|---|---|
| base (z11/z12 `-S 10`, z13 `-S 5`) | 11.73 | — | z11 ≈33 m, z12 ≈16 m, z13 ≈4.3 m |
| low-zoom detail 2048, same ground tolerance (`--low-detail=11 -S 5`) | 11.10 | −0.63 | z11 ≈34 m, z12 ≈17 m (+1 grid unit) |
| low-zoom detail 1024 (`--low-detail=10 -S 2.5`) | 10.57 | −1.16 | z11 ≈38 m, z12 ≈19 m |
| z11–12 at `-S 20` | 10.42 | −1.31 | z11 ≈64 m, z12 ≈32 m |
| z11–12 at `-S 40` | 9.16 | −2.57 | z11 ≈127 m, z12 ≈64 m |
| parts < 0.5 ha shown from z13 only (47,756 parts, 11,048 ha) | 11.14 | −0.59 | small lots appear one zoom later |
| parts < 2 ha shown from z13 only (84,597 parts, 48,315 ha) | 10.17 | −1.56 | most town lots appear at z13 |
| z13 at detail 2048, `-S 2.5` | 11.06 | −0.67 | z13 ≈4.7 m |
| z13 at `-S 10` | 10.98 | −0.75 | z13 ≈8.2 m |
| tippecanoe tiny-polygon reduction on | 11.73 | 0 | no effect: it acts on whole features, and this is one feature |

Parcel's ceiling is small. Even `-S 40` plus dropping parts under 2 ha at low zoom saves only about 4 MB. The cheap,
invisible one is the low-zoom detail (−0.6 to −1.2 MB), and it comes along with the archive-wide 4a/4b from round 1.

### 6. High zooms without removing any stream: shape options
- Measured on a z9–13 bbox build (−123..−121, 49..50) with every layer except parcel, which is priced in §5. The
  live flags (`-S 5`, detail 12) give z13 = 2.479 MB.
- Province figures scale the z13 % change to the live z13 without parcel (162.9 MB).
- z≤12 is unchanged in every row; round 1's 4a/4b cover it.
- Every stream and every vertex shared with another feature is kept (`--no-simplification-of-shared-nodes` stays).
  Straight runs are already reduced to their end points by Douglas–Peucker; the only levers left are tolerance and
  grid.

| z13 option | z13 change | ≈ province MB | max error | note |
|---|---|---|---|---|
| detail 12, `-S 1` | +45.5% | +74 | 1.2 m | the pre-T85 top zoom, for scale |
| detail 12, `-S 3` | +12.6% | +20.5 | 2.7 m | |
| detail 12, `-S 4` | +5.2% | +8.5 | 3.5 m | |
| **detail 12, `-S 5` (live)** | 0 | 0 | **4.3 m** | |
| detail 12, `-S 6` | −4.0% | −6.5 | 5.1 m | |
| detail 11, `-S 2.5` | −9.2% | −15.0 | 4.7 m | same tolerance as live, coarser grid (round-1 item 5) |
| detail 11, `-S 3` | −12.6% | −20.5 | 5.5 m | |
| detail 10, `-S 1.25` | −16.4% | −26.7 | 5.5 m | same tolerance as live; 3.1 m grid can look angular at z17–18 |
| detail 12, `-S 8` | −9.7% | −15.8 | 6.7 m | |
| detail 11, `-S 4` | −17.4% | −28.3 | 7.1 m | |
| detail 10, `-S 2` | −23.7% | −38.6 | 7.8 m | |
| detail 12, `-S 10` | −13.7% | −22.3 | 8.2 m | |
| detail 11, `-S 5` | −20.7% | −33.7 | 8.6 m | |
| detail 12, `-S 15` | −20.4% | −33.2 | 12.1 m | |

- **Reading the table:** at any given error, a coarser grid beats a larger tolerance. For example, at ~5.5 m
  detail 10 `-S 1.25` saves −16.4%, against about −6% for detail 12 at the same error. The efficient lines are:
  - ≈4.7 m: d11 / 2.5, −15 MB
  - ≈5.5 m: d10 / 1.25, −27 MB
  - ≈7–8 m: d11 / 4 (−28 MB) or d10 / 2 (−39 MB)
- **Caveat:** the bbox is coast-mountain and Fraser Valley water. A province-wide confirmation run would come with
  the real rebuild.
- **Check for every row:** screenshots at z16–18 on the owner's spots, against the imagery layer. A river that
  sits 5–8 m off its imagery bank shows up first.

### Round 2 totals
On top of round 1's zero-loss set (−34 MB), with the decisions above:
- mu z12 cap: −7 MB
- watershed dropped: already counted in round 1
- wma to Creston only: −0.2 MB
- one z13 line from §6: −15 to −39 MB

Result: **≈ 160–180 MB, from 239 MB**, and no stream removed.
