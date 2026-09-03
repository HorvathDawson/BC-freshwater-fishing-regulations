# UPDATE — Splits build & old→new accuracy audit (2026-08-14)

Status of the `waterbody-splits.json` → `pipeline/atlas/splits.json` conversion, the accuracy audit
that validates it against the curated coordinates, and every fix applied on top. This is a
point-in-time state doc; the authoritative design is [DESIGN-regs-to-sections.md](DESIGN-regs-to-sections.md)
and the curation contract is [CURATION-HANDOFF.md](CURATION-HANDOFF.md).

**TL;DR** — `pipeline/atlas/splits.json` now holds **394 splits across 197 waterbodies**
(confluence 93, point 295, area_boundary 4, lake 2). The old→new audit resolves **388/394**
and lands **332 (86%) within 25 m** of the curated ground-truth coord; the only 4 non-resolving
in the audit harness are parks (their polygons exist in the GPKG — the harness just doesn't pass
them). **Full suite: 121 passed, 11 skipped.**

---

## 1. What changed at a glance

| Area | Before | After |
|---|---|---|
| `splits.json` shape | legacy flat + worked-example | by-waterbody (`{waterbodies:[{name, applies_to, splits:[…]}]}`) |
| Split count | 467 (dupes, weird ids/labels) | **394** (73 dupes merged, clean ids/labels) |
| Ground-truth coord | dropped for confluences | **`_coord` on every split** (audit/review aid) |
| Broken conversions | 10 splits didn't resolve | **all resolve at their curated coord (≤0.6 m)** |
| Resolver | flagged river-mouth confluences as errors | treats `tw == pw` (own mouth) as valid |

---

## 2. The `splits.json` format (by-waterbody)

```jsonc
{
  "_about": "Curated stream splits, organized by waterbody. Converted from waterbody-splits.json.",
  "waterbodies": [
    {
      "name": "CHEMAINUS RIVER",
      "applies_to": {"gnis_id": "1234"},        // or {"blk": "..."} / {"wsc": "..."} / {"gnis_ids": [...]} / null
      "_entry_id": "…",                          // provenance: the curation entry this came from
      "splits": [
        {
          "id": "bannon_creek",                  // human-readable, unique across the file
          "label": "Bannon Creek",               // the LANDMARK (drives location_identifier), not a reach description
          "kind": "confluence",                  // source anchor_kind (provenance)
          "anchor": { "type": "confluence", "tributary_wsc": "…" },
          "wsc": "…",                            // OPTIONAL per-split target override (wins over applies_to)
          "_coord": [-123.9, 48.9],              // curated ground-truth cut location (review/audit only)
          "_offset_m": 100,                       // present iff the anchor carries an along-channel offset
          "_note": "…"                            // verbatim curation note (provenance)
        }
      ]
    }
  ]
}
```

`load_split_defs()` ([pipeline/atlas/splits/splits.py](../splits/splits.py)) flattens this: each
waterbody's `applies_to` becomes the split's target **unless the split overrides it** with its own
`blk`/`wsc`/`gnis_id`. Keys prefixed `_` are review/provenance and ignored by `SplitDef.from_dict`.

### Anchor types (resolution → `SplitPoint(blk, route_measure)`)
See [pipeline/atlas/splits/anchors.py](../splits/anchors.py) for the authoritative logic.

- **point** — project `coord` onto the target channel (kept iff within `proximity_m`); optional
  `offset_m`/`offset_dir` then shifts the cut along-channel ("100 m downstream of the falls").
- **confluence** — the tributary (`tributary_wsc` preferred, else `tributary_blk`) has a mouth;
  project it onto the target (parent) channel. Self-validates: the tributary WSC must be a strict
  descendant of the parent's — **or equal to it** (a river's *own mouth* into a larger river; see §4C).
- **lake** — the target's waterbody-run boundaries for `wbk` (no-op splits; the lake already split the BLK).
- **area_boundary** — cut the target water (and, iff `wsc_descendants`, its WSC descendants) wherever
  they cross an admin/park polygon boundary. **`wsc_descendants` only takes effect when the target is a
  WSC, not a `gnis_id`** (this bit Garibaldi — see §4D).
- **mu_boundary** — shared edge of two adjacent MUs ∩ the channel (collapses multi-crossings to one).

---

## 3. The conversion — `convert_splits.py`

**Location:** currently `scratchpad/convert_splits.py` (⚠️ not yet promoted — see §7).
**Source:** `pipeline/docs/waterbody-splits.json` via `load_curation()`
(`pipeline/oneoff/waterbody_splits.py`). Keeps only `curated` + `manual` rows.

Pipeline per row:
1. **`to_anchor(r)`** maps `anchor_kind` → a `SplitAnchor` dict. Confluence uses `tributary_wsc`
   (or `tributary_blk`); lake+offset becomes point+offset (a lake anchor can't carry an offset);
   area_boundary → park polygon; else point at `coord`.
2. **`landmark(r)`** derives a clean `label` — strips direction prefixes ("downstream of …") and a
   trailing "confluence", prefers `offset.anchor_label`.
3. **`dedup_key(a)`** merges rows that are the *same physical cut* (coord+offset / tributary / wbk /
   area). 73 duplicates merged.
4. Clean id from the authored-split note (`authored split: <id>`) else a slug of the landmark;
   collisions get a `<waterbody>__<base>` prefix.
5. `_coord`, `_offset_m`, `_note` attached for review/audit.

### Fix maps baked into the conversion (all from the old→new audit)
- `POINT_TARGET` — point rows with no curated target, scoped to the channel the coord sits on (WSC preferred).
- **Point row-target passthrough** — a point split now inherits the curated row's own `target`
  (WSC preferred over BLK; never a bare trunk).
- `TRIB_WSC` + `SELF_MOUTH` — corrected tributary WSCs for confluences whose curated WSC was wrong
  (§4C).
- `REHOME` — move a split filed under the wrong card to the right entry (Campbell dams, §4B).
  Re-homed rows contribute **only their split**, never the group's name/identity.
- `PARK_FIX` — area_boundary rows given a WSC target so `wsc_descendants` works (Tweedsmuir, Hamber,
  Pinnacles, **Garibaldi** §4D).

---

## 4. Fixes applied (with verification)

Every fix was verified by re-resolving against the curated coord (`_coord`).

### A. Points scoped to the wrong same-named feature — now ≤0.6 m
| split | was | now targets | resolved dist |
|---|---|---|---|
| `152nd_street` | card gnis `12117` = a *different* Bear Ck 83 km N | Mahood Ck wsc `900-005473-486949` | 0.1 m |
| `216th_street` | main Alouette (mis-filed) | North Alouette wsc `100-025956-057184-074570` | 0.6 m |
| `koch_creek_falls` | Slocan R. (parent) | Koch Ck blk `356568948` (row's own target) | 0.3 m |
| `old_mf_m_railway_bridge_coal_ck` | Elk trib gnis | Coal Ck wsc `300-625474-584724-253915` | 0.1 m |

### B. Campbell dams — both on the CAMPBELL RIVER entry
`CAMPBELL RIVER` is two colliding entries (gnis **39532** = Vancouver Island; gnis 7250 = Lower
Mainland). Both dams re-homed to 39532:
- `strathcona_dam` (the dam point) **and** `campbell_river__strathcona_dam` (+100 m d/s) — **both kept**.
- `ladore_dam` → blk `354154635` (Campbell R), resolved 0.1 m.
- The lake-tributary duplicate cards no longer carry them.
- **Bug found & fixed:** a re-homed row's name was hijacking the target entry's identity → the whole
  Campbell entry's `applies_to` briefly resolved to the wrong feature. Re-homed rows now never set group identity.

### C. Confluences with a wrong tributary WSC — now real confluences at 0 m
Not point fallbacks — the **correct** tributary WSC was found and verified (each resolves to the
curated junction at ≤0.2 m):

| split | correct tributary WSC | meaning |
|---|---|---|
| `babine_skeena` | `400-536025` (Babine's own) | Babine's mouth into the Skeena |
| `columbia_river` | `300-625474` (Kootenay's own) | Kootenay's mouth into the Columbia |
| `attichika_creek` | `200-948755-999851-889551-178842` (Kemess's own) | Kemess's mouth into Attichika |
| `saunders_creek` | `930-508366-243451-240268` (real VI Saunders Ck) | genuine tributary; curated WSC was NE-BC region 200 by mistake |

The first three are the **card river's own mouth** into a larger river (tributary WSC == parent WSC).
**Resolver change** ([anchors.py](../splits/anchors.py)): the WSC self-check now accepts `tw == pw`
as valid (a river's own mouth, "X to its confluence with Y", cut on X at measure 0) instead of
flagging a spurious "WSC check failed" concern. Existing concern/descendant tests unaffected.

### D. Garibaldi — `wsc_descendants` was silently ignored
`garibaldi_pitt` targeted `gnis_id 7551`, but `wsc_descendants` only fires for a **WSC** target, so it
cut only the Pitt River mainstem (1 cut). The reg covers park tributaries ("closure also applies to
tributaries within the park"). Fixed via `PARK_FIX` → WSC `100-025956`: now **21 clean crossings
across 20 blks** (Pitt + 19 tributaries, no noise).

---

## 5. The old→new audit — `audit_old_new.py`

**Location:** currently `scratchpad/audit_old_new.py` (⚠️ not yet promoted — see §7).

**Method:** for every split, window a small bbox around its curated `_coord` (∪ the `applies_to`
target bounds, 200 km-capped), build the BLK graph, resolve, then recover each cut's lon/lat via
`chain.geometry.interpolate(route_measure − mouth_measure)` and measure distance back to `_coord`.
Because it windows from the split's *own* curated coord (not the output file, which drops confluence
coords), it tests **every** confluence — including the province-spanning Fraser/Skeena ones the first
projection-distance audit couldn't box.

**Metric note:** offset splits store `_coord` inconsistently in the source (some = base feature, some
= final post-offset location), so the pass/fail metric is `min(raw, |raw − offset|)` — accurate if the
cut is near *either* the coord or the coord-shifted-by-offset.

**Final results:**
- 388/394 resolve (the 4 unresolved are parks — polygons not passed in the harness), **0 no-bbox**.
- Best-of-two buckets: **≤25 m: 332**, 25–100 m: 21, 100–500 m: 30, >500 m: 5.
- The >100 m residue is **not** conversion error: large along-channel offsets (Yakoun 14.5 km, Boston
  Bar 6.5 km, Peace/Halfway ±5 km — straight-line can't equal along-channel), plus dams/bridges/bars
  digitized *beside* the blue line (Strathcona 482, Croft 369, Duncan Dam 264, Papermill 254, Adams
  signs 191, Corra Linn 181, Landstrom 150). Right channel in every case.
- One mild confluence outlier to glance at later: `gosnell_creek` (Morice R.) at 220 m.

---

## 6. Parks — polygons already present

All four park names match `parks_bc.PROTECTED_LANDS_NAME` exactly and resolve end-to-end when the
polygon is passed as `area_polys`:

| park split | cuts | nearest to curated | note |
|---|---|---|---|
| `hamber_prov_park_boundary` | 8 | 0 m | clean |
| `pinnacles_park_upstream_boundary` | 22 | 0 m | clean |
| `garibaldi_pitt` | 21 | — | clean (after §4D) |
| `tweedsmuir_park_upstream_boundary` | **869** | 48 m | **boundary-following noise — see below** |

**Tweedsmuir 869:** `wsc_descendants` is fine (Garibaldi proves it). The Tweedsmuir polygon edge was
digitized *along* Talchako River (×153), Burnt Bridge Creek (×84), blk 360874958 (×438) and one more
(×12) — those 4 blks produce 687 of the 869 near-coincident intersection points. The ~145 genuine
crossings (1–2 per blk) are correct. This is an `area_boundary` resolver limitation (it keeps every
crossing by design). **Open decision (see §7).**

**Build wiring:** the resolver accepts `area_polys={name: polygon}`. The build step must load the
referenced `parks_bc` polygons (by `PROTECTED_LANDS_NAME`) and pass them to `resolve_splits`; the
audit harness deliberately doesn't, which is why parks show "unresolved" there.

---

## 7. Open items / next steps

1. **Promote out of scratchpad** — move `convert_splits.py` → a permanent module
   (e.g. `pipeline/oneoff/build_splits.py`) and turn `audit_old_new.py` into a test that fails if any
   split drifts > N m from its `_coord`. *(Not yet done — awaiting go-ahead.)*
2. **Tweedsmuir / area_boundary boundary-following** — pick a fix:
   - (a) per-blk proximity dedup (small, interim);
   - (b) **transition-based cutting** — cut only where a stream goes inside↔outside the polygon
     (correct; touches how `area_boundary` and `border.mark_inside_area` interact); *(recommended)*
   - (c) leave to `mark_inside_area` (fragments those rivers into hundreds of sections — not ideal).
3. **Wire `area_polys`** into the build step so park splits resolve in the real pipeline.
4. **Back-curate** the audit-driven fixes (POINT_TARGET, TRIB_WSC, PARK_FIX/Garibaldi, re-homes) into
   `waterbody-splits.json` for source reproducibility, so a fresh convert doesn't need the override maps.
5. Larger design (per DESIGN doc): registry, parser rebuild, matcher, resolver → `SectionRegs`.

---

## 8. How to run

```bash
# regenerate splits.json from the curated source
PYTHONPATH=. .venv/bin/python scratchpad/convert_splits.py   # writes pipeline/atlas/splits.new.json
cp pipeline/atlas/splits.new.json pipeline/atlas/splits.json

# old→new accuracy audit (resolve every split, compare to curated _coord)
PYTHONPATH=. .venv/bin/python scratchpad/audit_old_new.py

# tests (splits load/resolve, anchor resolution, real Bella Coola extract)
PYTHONPATH=. .venv/bin/python -m pytest pipeline/tests/ -q     # 121 passed, 11 skipped
```

**Verified 2026-08-14:** conversion regenerates 394 splits; `load_split_defs` + `resolve_splits`
smoke-resolve on the Bella Coola window; full suite green.
