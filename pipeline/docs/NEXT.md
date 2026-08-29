# Next up — not started

---

## 0. ⚠️ FIRST THING: `output/v2/full_named` exists and is NOT yet safe to switch to

The side-channel naming bug is **fixed** — and it was never a code bug. `output/v2/full`
was simply **stale**, built before the directional guard in `names.py`. Rebuilding fixes it:

| | side-channel names | leaked onto a larger water |
|---|---|---|
| `output/v2/full` (still the active build) | 4,780 | **2,135** |
| `output/v2/full_named` (new) | 19,739 | **0** |

McLennan Creek: 9 sections → 1. Verified by rebuilding the same bbox with current code.

**But the new build changes far more than names**, and switching blindly would break curation:

```
items      19,722 -> 19,857   (28 removed, 163 added, 3,295 CHANGED)
sections   49,639 -> 80,236   (+30,597, +62%)
```

* **The 4 `area:` items are gone**, replaced by 163 finer-grained ones
  (`area:ecological_reserves:…`, `area:chilkoot_trail:…`). So `pitt_river.r1`'s
  `area_id='area:within_garibaldi_park'` no longer resolves — the Garibaldi case we just
  got working would dangle again.
* **`cut_not_found` jumps 5 → 364.** The curated splits resolve against the old section
  geometry; with 62% more sections the boundaries moved. 364 rules lose their binding.

Reach builder against the new build: **2,562 bound / 475 unresolved** (vs 2,866 / 139 today).

### What this needs before adopting — the registry-resync workflow

1. Re-resolve the curated splits against `full_named` and inspect `cut_not_found`
   (`pipeline/tools/reparse_candidates.py` and the reach builder's `--against` diff both help).
2. Re-point the two `within(area)` extents at the new area ids.
3. `prune_remapped` → `backfill_matched` → hand off `parse-missing` (HUMAN-ONLY).
4. Expect entry ids to remap again, and locks to drop as they did last time — **finish the
   current curation pass first, or accept another remap.**

`output/v2/full` remains the active build; nothing has been switched. Decide deliberately.

Parked work, with the thinking done so it can start cold.

---

## 1. PMTiles — what does it actually need?

**Question asked: "all it needs is the graph + registry, done?"**

**Almost — geometry is the missing input, and it is not in either of them.**

`StreamNode` stores no geometry on purpose ("the graph is pure topology + attributes… a
geometry-free consumer never pays for shapely"). Geometry lives in a sidecar:

| input | where | size | carries |
|---|---|---|---|
| `graph.pkl` | build dir | 636 MB | topology, bounds, names, `out_of_bc` — **no geometry** |
| `registry.json` | build dir | 12 MB | item → section ids, boundaries, variants |
| **`geometries.pkl`** | build dir | **2.05 GB** | **the actual lines, by `node_id`** |
| `graph.gpkg` | build dir | 3.7 GB | the same, queryable — what `reuse` reads for the map |

So: **graph + registry + geometries**. All three already exist in `output/v2/full`.

### It does NOT need the reach builder

Worth being explicit, because it changes the ordering. Tiles carry **identity, not
regulation** — `section_id` plus the packager's dense id — and highlighting is done with
MapLibre `feature-state` at runtime. That is what deletes v1's fid→reach chain (`tier0.json`
was 24.9 MB, **16 MB of it `fids[]` highlight lists**).

**Consequence: tiles can be built now, in parallel with everything else.** They depend only
on a completed build, not on curation, not on resolution, not on the bundle schema. The one
hard rule is the manifest pins tiles and data together and the client refuses a mixed pair.

### Two artifacts, one blocker

1. **Regulable water** — measured: **180.5 MB of WKB out of 2,180.7 MB (8.3%)**. Through
   tippecanoe at v1's settings that is **~15–30 MB**: shippable offline. Not the problem.
2. **The basemap** — this is the blocker. `data/bc.pmtiles` is the **Protomaps basemap**
   (z0–15, `buildings/pois/landuse/…`), not BC's water; doc 10 ⑲'s original framing of this
   was simply wrong. Needs a stripped extract: **earth / water / roads / places, z4–12**.
3. **Bathymetry contours** (`archive/…/bathymetry_polygons.gpkg`, 2,744 polygons, 17.3 MB)
   are a third tile layer and are bundleable. The 2,685 scanned **map sheets (738 MB)** are
   never bundled — web links out, mobile fetches per lake on demand.

### Open questions before starting

- Zoom range and minzoom-per-`stream_order` (v1 assigned minzooms; is that still wanted?).
- Do `out_of_bc` pieces ship? They are kept in the graph for dotted display but BC regs do
  not apply — they must not be tappable as regulated water.
- Lake sections are polygons (`lake:{wbk}`), streams are lines. Same layer or two?
- Which properties ride along: `section_id` and dense id certainly; `display_name` and
  `stream_order` are useful for labels and line weight but cost bytes on 49.5k features.

---

## 2. The 27 entries with stale `matched`

`backfill_matched` stamps **0** of them — it only fixes entries whose original *export* had
a match. These are `noreg_*` entries where the **live** matcher, with today's registry and
overrides, finds an item the original export did not (Endako River, Hidden Lake, Little
Stawamus Creek, Copper River, …).

Nothing is broken: `pipeline.reach.covered` falls back to a live re-match, and the builder
and review app agree exactly. But the entry files stay stale, and attaching an item to a
`no_registry` entry is a **curation decision**, not a migration. Either curate them through
the review app's attach-item flow, or write a small tool that stamps from the live match —
a decision, not a chore.

## 3. Tributary walk

547 rules are `tributaries_pending`; **176 of them sit on a BOUNDED extent** ("between A and
B, including tributaries"), which is the hard case. Reach-scoped, recursive, barrier-aware,
validated against a flow walk — never a watershed-code prefix (that over-includes the
Kootenay by 3,205 km). Design: `13-build-plan.md` step 8.

## 4. Resolver bug — Mitchell River

One confirmed defect: a lake boundary carrying a split id as an **alias** resolves both ids
to the same measure, collapsing `between` to nothing. Aliases need to carry which *end* they
mean. Full trace: `RESOLVER-HANDOFF.md` §3.

## 5. The 73 locked-entry changes

Investigated and **benign**: Coldwater River went 1 → 12 sections, all named "Coldwater
River" on separate blue lines — the river's **side channels**, which the added-streams build
now includes. The registry got more complete, not wrong. Still worth a curator's eye, since
those rules were confirmed against a 1-section river.
