# 08 — Matching on Sections + Invariants to Preserve

The user's guidance: **matching stays the same — we just store the right info on sections so
it works.** This doc pins the matching model onto sections and lists what MUST carry over
from the legacy system even though we're rebuilding structure and client.

## Matching model (unchanged logic, section-keyed)

Regulations resolve to features by identifier, override-wins-over-base, exactly as today —
now the *target* is a section (or lake/wbk) instead of a scatter of fids.

**What each section must store to be matchable** (populate in `blk-chains`/`sections`):
- `gnis_id`, `blk`, `fwa_watershed_code`, `lake_wbk` (if lake-adjacent),
- `name_tuples` (all variants + source) for gazette natural-search,
- `location_identifier` + `boundary ids` (so "upstream of Adams Lake" resolves),
- `mu_ids` it passes through (for `only_within_zones`/MU include-exclude; drives whether a
  curated `mu_boundary` split is needed — `06`),
- `member_segment_ids` + `tributary_section_ids` (for tributary expansion).

A section stores only **named-waterbody-specific** regs (normally one reg set; Fraser-type
per-MU differences are handled by curated `mu_boundary` splits so each section is still one
reg set). It does **not** store base/zone-wide regs.

**Simple path** (the vast majority): `gnis_id`/name → the section(s) of that gnis, optionally
narrowed by `location_identifier`. This is `_natural_search` targeting sections.

**Complex path** (override-only): `waterbody_keys, fwa_watershed_codes, blue_line_keys,
linear_feature_ids, waterbody_poly_ids, admin_targets, only_within_zones`. Each still
resolves to sections — a blk/lfid/wsc maps to the section(s) covering it; admin/poly resolve
spatially. Override precedence order preserved (`match_table.py` L150-156).

**Retire the split hacks:** the Adams-style duplicate override rows (both halves → same
gnis) become two real sections; the override instead carries the split definition (`04`) or
targets `section_id`/`location_identifier`. Hand `linear_feature_ids`/`blue_line_keys` splits
become `splits.json` anchors.

## Tributary expansion (Phase-3 equivalent)

- `effective_includes_tributaries(parsed)` is **unchanged** — it's a property of the parsed
  regulation (entry flag + per-rule override; `tributary_only` always expands). Port
  verbatim; keep `test_tributary_logic.py` / `test_tributary_override.py`.
- Expansion = assign the reg to the section's precomputed `tributary_section_ids` (walk done
  once at graph time, `03` Step 6), honoring lake barriers / directionality.
- `tributary_only` → assign only the tributary sections, not the named section itself (keep
  the current `if not trib_only:` guard).
- Lakes: from a lake node, expand to upstream sections via its inlets.

## Base / provincial regulations → MU overlay (not per-section)

Preserve the Phase-4 MU-granular resolution (`_resolve_mu_set` = zone→MUs + include −
exclude), but emit it as a standalone **`mu_id → base_reg_set` overlay** (`06`), not baked
onto sections. Provincial park closures resolve by park polygon → MUs/sub-extents. The
client shows the overlay by the located/clicked MU. This is the change that removes the UI
bloat and keeps sections regulation-light.

## Hard invariants (survive the clean-slate)

Because we're free to change client + storage, the invariants are narrower than "keep the
old shape" — they are the things that would silently corrupt data or break users:

1. **`section_id` is stable and deterministic** across builds (hash of `blk` + ordered
   boundary ids + `lake_wbk`; never from fid order). Deep links, caches, and the tile↔reg
   join depend on it.
2. **The build is deterministic** — same inputs → same section ids, same assignments (the
   pipeline already leans on this; keep golden-file tests).
3. **Name provenance is preserved** — `(name, source)` tuples must carry through to search +
   display (don't flatten to a scalar).
4. **Regulation coverage is complete** — every section that should carry a base/zone/named
   reg does (no reach ends up with zero applicable rules — the reason
   `effective_includes_tributaries` inherits the entry flag).
5. **Under-lake vs open-channel distinction preserved** (`WATERBODY_KEY ∈ lakes∪manmade`).
6. **The curated data carries over unchanged**: `overrides.json`, `feature_display_names.json`
   (Seabird etc.), and the new `splits.json`. These are hand-authored truth.
7. **Match-table validation stays strict** (extend `test_overrides_validation.py`;
   `location_identifier` uniqueness within a gnis).

## What is explicitly NOT an invariant anymore (free to redesign)

- The `fid → reach` shard chain and `/api/resolve` (replaced by self-identifying tiles, `09`).
- `tier0.json`'s shape and its embedded `fids[]` (replaced by tiny bootstrap + lazy chunks).
- The mobile `fids`/`polys` tables (dropped).
- The dynamic `(wsc, display_name, reg_set)` reach key (replaced by section + zone_reg_map).
- `filter_unnamed_depth` (dropped; rely on per-section minzoom + tile-byte budget).

## KEPT after the spike (do NOT delete — `12` proved these necessary)

- **The WSC-hierarchy (excluded-WSC / parent-WSC) filter** — reframed as "a tributary is a
  WSC-descendant reachable upstream." SCC condensation is a no-op on FWA; this filter is what
  stops the Chehalis→Harrison confluence-parent leak.
- **The `EDGE_TYPE=2300` connector barrier** — lake-collapse does not replace it (canals
  bypass lake polygons); required to stop the Kootenay↔Columbia leak. Keep the strict
  missing-edge_type guard.

## Watch-list — ambiguous matching cases (flag during curation)

- **Bowron Lake 5-16 — "Park waters other than Bowron Lake"** (flagged 2026-08-09): resolves by an
  **admin boundary** (Bowron Lake Provincial Park polygon) **minus** the named lake, not a clean
  gnis/name match. Curated `n/a` as a *split* (no reach cut), but the reg still has to attach to the
  *park-waters set* via `admin_targets`/park-polygon → sub-extent. **Look out for this:** verify it
  lands on the park's other waters and does NOT accidentally match Bowron Lake itself (the named
  lake is excluded). This is the "admin bound but not clearly one" pattern — watch for similar
  "Park waters other than X" / "all lakes in the park except Y" entries.
- **Sumallo River 2-2 — "includes 'Cedar' Lake, at Sunshine Valley"** (flagged 2026-08-10): NOT a split
  (curated n/a), but an **include** clause — the Sumallo River water must ALSO match/attach to "Cedar"
  Lake (a name-variant / co-located waterbody at Sunshine Valley). Ensure the matcher pulls Cedar Lake
  in under the Sumallo entry (name-tuple / co-membership), don't drop it. Same class as other
  "includes X Lake/Creek" extent clauses (Seton canal, Vaseux lagoons, McArthur slough).
- **Bull River 4-22 — "Quinn Creek [Includes Tributaries]"** (flagged 2026-08-10): the trout/char C&R
  applies to Quinn Creek AND its tributaries as a whole set — an **include**, NOT a point split
  (curated n/a). The matcher must assign that reg to Quinn Creek's whole WSC subtree via tributary
  inheritance. FWA GNIS name is **"Quinn (Queen) Creek"**, WSC `300-625474-636250-492930` (a name
  variant — the reg says "Quinn", FWA says "Quinn (Queen)"). Ensure the name-tuple match resolves
  "Quinn Creek" → this WSC. (The Galbraith→Van and Aberfeldie Dam→Tie Mill Dam reaches ARE curated
  point splits; only Quinn Creek is a tributary include.)

## Tests to port / add

Port: `test_tributary_logic`, `test_tributary_override`, `test_feature_resolver_dispatch`
(retarget to sections), `test_display_name_resolver` (now name-tuple priority),
`test_overrides_validation`, `test_trim_wsc`, sharder tests (retarget to section shards).
Add: blk-chain contiguity, name-tuple provenance, lake-node collapse, directionality/no-
backtracking, section coverage, substring cut accuracy, zone_reg_map collapse (Similkameen),
section-split labels (Adams/Wigwam).
