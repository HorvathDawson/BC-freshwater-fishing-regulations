# 03 — Gap Analysis: what the section model must still cover

Every behavior the current pipeline performs, and how the section model handles it. The
items the user flagged are marked ⭐. "Gap" = something the raw idea did not mention that
the current code does.

## Hard contracts (breaking these breaks the shipped clients)

These are non-negotiable regardless of internal redesign:

1. **Stable `fid` (linear_feature_id) + `waterbody_key` join keys.** PMTiles ship geometry
   keyed by `fid`; regs join client-side by fid/wbk. → The section model must still emit a
   **fid → section** mapping so tiles keep working. Sections are a layer *on top of* fids,
   not a replacement for them. (See gap G1.)
2. **The 6-key regulation index contract** `{regulations, reg_sets, reaches,
   reach_segments, poly_reaches, search_index}` and the tier0 search-entry shape with
   embedded `segments[]`. Change internals freely, but the serialized shape the client
   reads must be migrated deliberately, in lockstep with `webapp/`.
3. **`shard_version` coupling** between R2 JSON shards and mobile SQLite (one in-memory dict
   → two serializers, must not drift).

---

## ⭐ G1 — Tile join / fewer unique IDs

**Current:** IDs = fid (per micro-segment), blk, gnis_id, wsc, wbk, reg_id, admin_id, +50k
codes. Tiles key on fid; reaches key on md5(wsc|name|regs).

**New:** section id = `(gnis_id, location_identifier, lake_wbk)`. This is fewer *logical*
atoms, but **fid cannot be dropped** — it is the tile geometry key (contract #1). So:
- Keep `fid` as the geometry key in tiles (unchanged).
- Introduce `section_id` as the new logical key. Ship a **fid → section_id** shard
  (replaces/renames the current fid → reach shard).
- A section carries its reg_sets; the client resolves fid → section_id → reg_sets.
- `reg_id` stays (regulation identity is orthogonal). `blk`, `wsc` become section
  *attributes* rather than grouping keys.

**Net:** genuinely fewer grouping keys (section replaces the dynamic `(wsc,name,reg_set)`
reach key), but the fid tile-join survives. This is the single most important thing not to
lose.

---

## ⭐ G2 — Name propagation to segments (happens during combine)

**Current:** three automatic layers in `graph_builder.py` — BLK single-name backfill →
watershed-context annotation → upstream-BFS-along-same-WSC to inherit the nearest named
stream's name for unnamed side channels (`inherited_gnis_names`). Plus **manual overrides**
(`feature_display_names.json`, e.g. Seabird Island) applied at display time via
`DisplayNameResolver`.

**New:** the idea says "apply name overrides before combining." Correct, but note the
**automatic** propagation is separate from and additional to the manual overrides:
- Manual overrides (Seabird Island) → apply **first**, before combining, so the override
  name defines the grouping identity. ✅ matches the idea.
- Automatic inheritance for genuinely-unnamed side channels → still needed to decide which
  section an unnamed run belongs to. Options: (a) port the existing upstream-BFS-along-WSC
  heuristic into the combine step, or (b) treat unnamed runs as their own sections
  (gnis null) attached to the parent section via the tree. **Recommendation:** keep the
  existing inheritance heuristic during combine — it's well-tested (`annotate_unnamed_context`)
  and users expect side channels to group with their mainstem.
- `DisplayNameResolver`'s 5-level stream / 4-level polygon priority chain must still produce
  the section display name. Move it to run **per section** instead of per fid.

**Gap:** the idea folds "name overrides" and "name propagation" together; they are two
mechanisms and both must run, overrides before automatic.

---

## ⭐ G3 — Unnamed depth filtering

**Current:** `filter_unnamed_depth(threshold=2)` (opt-in `-u`) drops unnamed edges ≥2
WSC-hops from a named stream. Separately, **minzoom** (BLK-magnitude percentiles) gates
visibility so tiny creeks only appear at z11-12.

**New (user's suggestion):** maybe ignore depth filtering and show all streams at higher
zooms. **This is viable** because minzoom already provides zoom-gated visibility — depth
filtering is a *graph-size* optimization, and the whole point of the redesign is that we no
longer BFS the giant graph at runtime, so graph size matters less. **But** check the tile
size budget: tiles are still built from all fids; dropping depth filtering means more
geometry at high zooms. Current tippecanoe budget is `--maximum-tile-bytes=2500000`.
**Recommendation:** drop depth filtering as a *graph* step; rely on minzoom + tippecanoe
tile-byte limits for tile size. Recompute section minzoom (G7).

---

## ⭐ G4 — Gazette functions become simpler

**Current:** two families —
- Regulation-side `_natural_search` (`matching/base_entry_builder.py`): title-cased GNIS
  name index + zone/MU filter. This is what decides which FWA feature a synopsis row means.
- Live-data anglerinfo chain (`recurring/anglerinfo/*`): matches external stocking/
  bathymetry/marker datasets to FWA by name+identifier cross-validation. **This is the
  genuinely complex part**, and it is *not* about which regulation applies — it's data
  enrichment. The redesign simplifies the **regulation-side** search (match to a section by
  name, not to a scatter of fids), but the anglerinfo chain's complexity comes from messy
  external data and is **not automatically simplified** by the section model.

**How it simplifies:** with sections keyed by `(gnis, location, wbk)`, `_natural_search`
resolves a synopsis name to **one section (or a small set)** instead of to a gnis_id that
then fans out to many fids across zones. Zone/MU disambiguation attaches to the section.

**Gap / clarification:** scope the "gazette simplification" claim to the regulation-side
search. Do not expect the anglerinfo live-data pipeline to shrink much — plan it as a
separate track if it needs work.

---

## ⭐ G5 — effective_includes_tributaries for lakes AND streams

**Current:** `effective_includes_tributaries(parsed)` decides whether Phase 3 BFS runs;
`tributary_only` always expands; else expands iff ≥1 rule is effectively trib-scoped. Works
for streams (stream seeds) and lakes (lake outlet fids → seeds).

**New:** the logic itself is unchanged (it's a property of the parsed regulation, not of the
graph). What changes is the *expansion*: instead of BFS over micro-segments, walk the
contracted tree upstream from the section (stream) or from the lake's outlet section (lake).
Must preserve:
- Stream case: from the reg's section(s), collect all upstream sections in the tree.
- Lake case: from the lake, collect upstream sections via the lake's inlet(s).
- `tributary_only`: assign only the discovered upstream sections, **not** the named
  section/lake itself (current `if not trib_only:` guard at Phase 2 must be preserved).

**Gap:** none in the logic, but the barrier rules (G6) must be encoded in the tree.

---

## ⭐ G6 — Tributary-breaking barriers must be encoded in the contracted graph

The current BFS has three runtime rules that STOP or SHAPE propagation. In the section
model these must become **properties of the contracted tree** so a simple upstream walk is
correct without re-implementing the BFS gymnastics:

1. **Lake barriers** — stop at the next regulated lake. → In the tree, a lake is a node.
   Upstream walk from a section naturally stops when it reaches a lake node *if* we mark
   lake nodes as barriers that the walk does not cross (unless the seed IS that lake). This
   is cleaner than the current "set of all regulated wbks minus self" because it's
   structural. **Design point:** the walk must be parameterizable — "cross lakes" vs "stop
   at lakes" — because the barrier set was seed-relative.

2. **Mainstem WSC exclusion / no backtracking** — current BFS excludes the seed's WSC +
   parents to avoid walking *down* the mainstem. **Key insight:** in a *properly directed*
   contracted tree rooted at the outlet, an upstream walk only ever visits upstream
   children — it *cannot* backtrack down the mainstem, because downstream is the opposite
   direction. So a large chunk of the excluded-WSC complexity **disappears for free** if the
   tree is correctly directed. ⚠️ **Verify this** on real confluences before deleting the
   logic — the current hack may also be compensating for FWA data where a seed's own
   watershed code appears on multiple branches. Treat as "likely simplifies, confirm with a
   test on Adams/Fraser confluences."

3. **Edge type 2300 (connector edges)** — "may traverse through consecutive 2300 edges but
   cannot exit a 2300 back into a normal edge." → When contracting, a 2300 run becomes a
   section (or is folded into the adjacent section) with an `edge_type=2300` marker. The
   upstream walk must honor the same "enter-but-don't-exit" rule at the section level. **Do
   not lose this** — missing/wrong 2300 handling silently over- or under-propagates regs.
   Missing `edge_type` currently raises; keep that strictness.

**Gap:** the idea says "make sure connections are broken for stuff that breaks tributaries"
but does not say *how*. Answer: bake barriers into node/edge attributes of the contracted
tree (lake=barrier node, 2300=marked section, directionality kills backtracking), and keep
the walk parameterizable for the seed-relative cases.

---

## G7 — minzoom / magnitude recompute (not in the idea)

**Current:** stream minzoom from **per-BLK** max magnitude → percentile. A section spans
possibly multiple BLKs. → Recompute **per-section** max magnitude → percentile, then stamp
each member fid's tile minzoom from its section (or keep per-fid magnitude but ensure a
section is coherent in zoom). Do not let a section flicker in/out across zooms.

## G8 — Zones: split geometry, or attribute + UI? (user asked)

**Current:** zones are NOT geometry splits. `only_within_zones` prunes resolved features to
WMU zone polygons; Phase 4 assigns zone-wide/provincial base regs by MU polygon
intersection; reaches aggregate `rg/z/mu` from their regs.

**Recommendation: keep zone as a section attribute; do NOT split sections by zone.**
Reasons: (a) splitting geometry by admin boundary multiplies section count and couples
stable geometry to mutable admin data; (b) a single physical reach can legitimately carry
different regs in different zones — model that as multiple reg_sets on the section, resolved
by the client using the zone attribute, exactly as today. Only split geometry where a
**physical** boundary (lake/confluence/hand split) exists. Surface this as a decision in
`05`.

## G9 — Base / provincial regulations (Phase 4) (not in the idea)

**Current:** zone-wide + provincial-park base regs assigned by MU/park polygon intersection,
routed `direct/admin/zone_wide`. The idea only discusses named-feature matching. → Base regs
must still apply to sections: a section inherits the base reg of the zone(s)/park(s) it lies
in (spatial intersection of the section's fids). Preserve `has_direct_target` /
`admin_targets` / `zone_wide` routing at the section level.

## G10 — Reaches vs Sections reconciliation (subtle, important)

**Current** "reach" = dynamic group `(wsc, display_name, sorted reg_set)`. A stream breaks
into a new reach wherever the applicable reg set changes.

**New** "section" = static-ish geometric unit `(gnis, location, wbk)` from physical + hand
boundaries. These are **not the same thing**. Reconciliation:
- Section = geometry/identity atom (stable, from physical + hand splits).
- Reach = section further partitioned wherever the reg_set differs along it. I.e.
  `reach = section × reg_set`. Most sections will be one reach; a section crossing a zone
  boundary or a partial hand-split may host >1 reg_set → >1 reach.
- This keeps the client contract (`reaches`) intact while making sections the clean
  upstream-tree unit. **Do not conflate them** — the tree walk is over sections; the display
  grouping is over reaches.

## G11 — Under-lake streams (idea covers this ✅)

Current atlas already classifies under-lake streams as a separate class and tiles them as a
`under_lake_streams` layer (tile floor z10). The idea's "split on lakes, keep under-lake
separate" aligns. Preserve the classification; a section that runs under a lake gets
`lake_wbk` set and is kept distinct from open-channel sections (matches the section identity
tuple).

## G12 — Location_text / Landmark seam already exists (leverage it)

`parsing/models.py::ParsedRule.location_text` and `matching/reg_models.py::Landmark{fid:
Optional[str]}` are already the designed hook for spatial resolution of "upstream of X"
language. The hand-split system in `04` is the implementation of `Landmark.fid` resolution.
Reuse these models rather than inventing new ones.

---

## Summary table

| # | Concern | In idea? | Verdict |
|---|---------|----------|---------|
| G1 | fid tile join / fewer IDs | partial | Keep fid; add section_id; ship fid→section |
| G2 | name propagation vs overrides | partial | Two mechanisms; overrides first, auto-inherit still needed |
| G3 | unnamed depth filtering | yes | Drop it; rely on minzoom + tile-byte budget |
| G4 | gazette simplification | yes | Reg-side simplifies; anglerinfo chain does not |
| G5 | effective_includes_tributaries | implied | Logic unchanged; expansion = tree walk |
| G6 | tributary barriers | partial | Bake into tree (lake node, 2300 marker, directionality) |
| G7 | minzoom recompute | no | Recompute per-section |
| G8 | zones | asked | Attribute, not geometry split |
| G9 | base/provincial regs | no | Must still apply to sections spatially |
| G10 | reach vs section | no | reach = section × reg_set; don't conflate |
| G11 | under-lake | yes | Preserve classification, set lake_wbk |
| G12 | Landmark seam | no | Reuse existing models |
