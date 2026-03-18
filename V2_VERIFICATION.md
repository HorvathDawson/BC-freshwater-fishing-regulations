# V2 Verification Checklist

Summary of v1 known errors and their resolutions, plus a checklist to verify each is handled correctly in v2.

---

## Resolved in V1 (verify still works in V2)

### Tributary BFS Traveling Up Mainstem
**V1 Issue**: Tributaries BFS would travel upstream along the mainstem instead of only following actual tributaries. Morris Creek was tagged as a tributary of Chehalis River instead of Pit River. Similkameen River was cut off because matching was zone-limited.
**V1 Fix**: All regs go to all segments of a match; tributary enrichment restricted to actual tributaries.
- [x] Morris Creek is NOT listed under Chehalis River regulations
- [x] Tributaries BFS does not travel up mainstem (parent WSC exclusion)
- [x] Similkameen River has all segments across zones

### Bear River Cross-Region
**V1 Issue**: Bear River appeared in both Region 1 and Region 2 but is fully in Region 2.
**V1 Fix**: Handled with tests.
- [ ] Bear River shows only Region 2 regulations
    * **Root cause**: `_precompute_mu_features()` already has full two-pass hysteresis (pass 1: exact STRtree intersection, pass 2: buffered, only including WSC groups that entered via the exact pass). The issue is that fid 206000766 genuinely straddles the R1/R2 boundary — the underlying FWA geometry crosses into R1. This is a real spatial boundary issue, not a missing algorithm.
    * **Fix options**: (a) Clip FWA geometries to the BC region boundary polygons before MU assignment, or (b) accept the edge case and rely on manual override if needed.

### Tsitika River Eco Reserve Grouping
**V1 Issue**: Multiple eco reserves with different names were grouped together because same reg code.
**V1 Fix**: Each eco reserve is its own separate entry.
- [ ] Tsitika River eco reserves are separate entries, not merged
    * **Root cause**: `PROV_ECO_RESERVES_CLOSED` is a single reg_id applied to ALL eco reserves. The identity of which specific eco reserve (admin_id, name) is discarded after spatial assignment. All fids on Tsitika River in different eco reserves get the same `(wsc, display_name, reg_set_str)` grouping key → merged into one reach.
    * **Fix needed**: Either generate per-reserve reg_ids (e.g. `PROV_ECO_RESERVE_1184_CLOSED`) or include admin zone identity in the reach grouping key. Also note: Lower Tsitika River Park (type=PROVINCIAL_PARK) has no matching base regulation at all.

### Unnamed Waterbody in Liumchen Eco Reserve
**V1 Issue**: Polygon split on admin boundary didn't work cleanly.
**V1 Fix**: Fixed along the way.
- [x] Liumchen eco reserve unnamed waterbody renders correctly

### Admin Zone Unnamed Stream Naming
**V1 Issue**: Admin-matched unnamed streams got the regulation name as display name, making everything share the same name and polluting search.
**V1 Fix**: Naturally resolved — unnamed streams don't appear in search index.
- [x] Unnamed streams in admin zones don't have regulation names as display names
- [x] Unnamed streams don't appear in search results

### Nicomen Slough Lakes
**V1 Issue**: Slough lakes weren't grouped with the slough.
**V1 Fix**: Direct match on all + GNIS ID.
- [ ] Nicomen Slough and its lakes are properly linked
    * **Root cause**: Override has `waterbody_poly_ids` for 7 slough lakes, but `feature_resolver.py` has no `_resolve_by_waterbody_poly_ids()` handler — the field is silently ignored. The atlas also has no `poly_id → waterbody_key` index. GNIS 21105 only captures the stream, not the lakes (they have different GNIS IDs).
    * **Fix option A** (simple): Convert the 7 `waterbody_poly_ids` to `waterbody_keys` by querying the GPKG, then use the existing `waterbody_keys` resolver.
    * **Fix option B** (systematic): Build a `poly_id → waterbody_key` index in atlas, add `_resolve_by_waterbody_poly_ids()` to feature_resolver. Also affects BLUEY LAKE POTHOLES override (10 poly_ids).

### Name Variations / Ignored Matches
**V1 Issue**: Ignored matches couldn't link to another regulation to display and search under both names.
**V1 Fix**: Implemented name_variants system — both names searchable and displayed.
- [ ] Name variants appear in search results (Fuse.js searches `name_variants`)
    * **Code is correct**: `_propagate_variant_of()` in MatchTable adds "BEAR RIVER" to AMOR DE COSMOS CREEK's `name_variants` at runtime. `reg_id_variants` in reach_builder propagates it to reaches and search index. Likely needs a fresh pipeline run to reflect in deployed data (the match_table.json base entry stores `name_variants: []`, but runtime propagation from overrides.json populates it).
- [x] "Also known as" shows in InfoPanel for features with variants

### Campbell Lake / John Hart Lake Tributary Leaking
**V1 Issue**: Campbell Lake (lower) tributary-only regulation leaked into Upper Campbell Lake area. Streams inside lakes weren't hidden when park regulations applied.
**V1 Fix**: Not an issue with zone regulations present since everything is regulated. Lakes block tributary propagation.
- [x] Zone regulations cover these areas
- [x] Tributary BFS stops at lakes that have regulations

### Parker Lake Grouping
**V1 Issue**: Small unnamed lake grouped with Parker Lake on same blue_line_key via admin matching.
**V1 Fix**: Admin matches split on name; unnamed items not grouped with named.
- [x] Parker Lake doesn't include unnamed adjacent waterbodies

### Dinosaur Lake Hidden Streams
**V1 Issue**: WBK 329657308 not a visible polygon but streams underneath were removed.
**V1 Fix**: Streams only filtered when a matching polygon exists.
- [x] Dinosaur Lake streams are visible if no polygon is matched

### Chilliwack Lake Stream Segments
**V1 Issue**: Streams inside Chilliwack Lake not hidden due to waterbody_key mismatches in FWA data.
**V1 Fix**: Fixed waterbody_key handling.
- [x] Chilliwack Lake under-lake streams are properly hidden

### Dean River Parsing
**V1 Issue**: Complex multi-segment regulation text lost classified water notes. Parser didn't verify all rules were captured from verbatim text.
**V1 Fix**: Parser model validation now checks rule_text is present in regs_verbatim.
- [ ] Dean River has all regulation segments parsed (closures, gear restrictions, classified water)
    * it failed parsing so defaulted to sentence for now. we will have to rerun parse etc.
- [ ] No rules silently dropped from complex multi-part regulations
    *unsure as of now

### Kootenay River Split at Columbia Lake
**V1 Issue**: Entry split into 2 segments at Columbia Lake boundary.
**V1 Fix**: Expected behavior — different regulation zones create separate reaches.
- [x] Kootenay River segments are correct (downstream of Idaho border vs upstream of Koocanusa)

### Piggott Creek / Oyster Creek
**V1 Issue**: Piggott Creek not recognized as tributary of Oyster Creek above an odd junction.
**V1 Fix**: Graph connectivity issue — verify in v2 graph.
- [ ] Piggott Creek tributary relationship is correct
    * **Tributary data IS flowing correctly**: `tributary_reg_ids` field is populated on reaches and decoded by the frontend. Regulation group badges show "Tributary of" correctly.
    * **"Tributary of [Parent Name]" subtitle text is broken**: `from_tributary` boolean has been replaced by a 3-way `source` field (`'direct' | 'tributary' | 'admin'`) on all `name_variants`. The InfoPanel now renders tributary variants under "Also known as" and admin variants under a separate "In [park name]" context line.
    * **Status**: Resolved via provenance system refactor. `source` field replaces `from_tributary` throughout the entire stack.

### Kettle River Missing Segment
**V1 Issue**: 500m zone buffer captured some segments but not all.
**V1 Fix**: Buffer handling improved.
- [ ] Kettle River has all segments (no border cutoff gaps)
    * **Root cause**: No BC boundary filtering exists anywhere in the pipeline. Streams south of the border (in the US) are loaded from FWA graph and pass through to tiles and shards unfiltered.
    * **Fix needed**: Add BC boundary clip in `FreshWaterAtlas._load_streams()` using the WMU union (same pattern as existing tidal boundary exclusion). This would unify the step between tiles and data as requested.

---



---

## V2-Specific Items to Verify

### Name Variants Pipeline
- [x] `name_variants` populated in reaches from `match_entry.name_variants` *(code verified)*
- [ ] `name_variants` field non-empty in tier0.json for entries with variants *(needs pipeline run)*
- [x] Display name excluded from `name_variants` (no self-reference) *(code verified)*
- [ ] Search finds features by variant names *(needs runtime verify)*
- [x] InfoPanel shows "Also known as" aliases *(code verified)*

### Feature Display Names
- [x] `feature_display_names.json` entries applied via shared `DisplayNameResolver` *(code verified)*
- [x] Resolver used by both tile_exporter and reach_builder — consistent display names *(code verified)*
- [ ] Previously unnamed side channels now have names in search and display *(needs runtime verify)*
- [ ] Prerender generates pages for newly named features *(needs runtime verify)*

### Zone Regulation Labels
- [x] Zone regulations show "Region X Zone Regulations" header *(code verified — InfoPanel groups by region+zone)*
- [x] Different regions' zone regs are separate groups *(code verified)*

### Tributary Source Tagging (`tributary_reg_ids` field)
- [x] `tributary_reg_ids` field populated per reach with inherited tributary reg_ids *(code verified)*
- [x] Regulation group "Tributary of" badge shows on inherited reg headers *(code verified)*
- [x] "Tributary of [Parent Name]" subtitle — resolved via `source` provenance field replacing `from_tributary` *(refactored)*
- [x] Parent WSC excluded from BFS seeds *(code verified)*

### Deep Link / URL Restoration
- [x] URL parsing uses full key names after key rename *(code verified)*
- [ ] `/waterbody/<wbg>/` works for named streams *(needs runtime verify)*
- [ ] `/waterbody/<wbg>/` works for lakes *(needs runtime verify)*
- [ ] `?f=<reach_id>` works for unnamed streams — resolves via API *(needs runtime verify)*
- [ ] `?s=<reach_id>` selects specific section tab on load *(needs runtime verify)*
- [x] `rids` parameter in r2-worker still works *(code verified)*

### Prerender / SEO
- [x] Prerender reads full key names (`display_name`, `feature_type`, `name_variants`, `waterbody_group`) *(code verified)*
- [ ] 18,762+ pages generated *(needs pipeline run)*
- [ ] Correct type labels in generated pages *(needs runtime verify)*
- [ ] `sitemap.xml` contains all prerendered URLs *(needs runtime verify)*

### Display Name Consistency (NEW — v8 key rename session)
- [x] `DisplayNameResolver` is single shared utility for both tiles and shards
- [x] Both tile_exporter and reach_builder receive same 3 data sources (feature_display_names.json, match_table.json, overrides.json)
- [x] Tile exporter stores resolved name in tile `display_name` property (not raw GNIS)
- [ ] Minor gap: tile_exporter doesn't pass `direct_reg_name` to resolver (reach_builder does) — affects only unnamed features without match_table BLK entries

### Provenance / match_type (NEW — v8 key rename session)
- [x] `match_type` property on `_EntryBase` and `OverrideEntry` — single source of truth
- [x] `match_type` included in synopsis regulation output from `_build_synopsis_regulations()`
- [x] `SynopsisReg` TypeScript interface includes `match_type?: 'direct' | 'admin' | 'unmatched'`
- [x] `_has_direct_ids` on OverrideEntry checks all identifier fields including `ungazetted_waterbody_id`
- [x] `match_entry=None` logs warning and sets `match_type="unmatched"`


## Open Issues (still need fixing in V2)

### 5. Admin Area Polygon Splitting
**Issue**: When an admin area intersects a lake or polygon, regulations should only apply to the intersected portion. Currently the whole polygon gets the regulations.
**Status**: Not implemented — would require polygon splitting.
- [ ] Decide if polygon splitting is needed
- [ ] If yes, implement intersection clipping for admin boundaries

### 9. Stocked Lake Info
**Issue**: GoFishBC has stocked lake data at their API. Could enrich our data with stocking info.
**Status**: Not implemented — feature request.
- [ ] Evaluate stocked lake data integration

### 11. Missing Admin Boundaries
**Issue**: Knap, Malcolm, and Rubble Creek admin boundaries missing from data.
**Status**: Not fixed.
- [ ] Verify these admin boundaries exist in current data
- [ ] Add missing boundaries if needed

### 14. Tidal Boundary Issues
**Issue**: Multiple rivers have incorrect tidal boundary placement. Streams below tidal boundary should show "refer to tidal regulations" instead of freshwater regs.
**Rivers affected**: Campbell, Englishman, Nanaimo, Little Qualicum, East Newcastle Creek, Salmon, Adam, Somass, Gordon, San Juan, Rainy, Clowhom Lake(?), Jordan, Koprino.
- [ ] Tidal boundary layer is applied correctly
- [ ] Rivers below tidal boundary excluded from freshwater regs or show tidal notice
- [ ] Check each listed river for correct tidal cutoff

### 15. Lake Revelstoke Streams
**Issue**: FWA data issue — upper reservoir (WBK 328998980) has different waterbody_key than lake polygon (329484653). Streams in upper section not recognized as under-lake.
**Status**: Known FWA data limitation.
- [ ] Verify current behavior at Lake Revelstoke
- [ ] Document as known limitation if unfixable

### 16. Eco Reserve Stream Grouping
**Issue**: Should eco reserves be grouped into stream sections in the info panel?
**Status**: Undecided.
- [ ] Decide on eco reserve display strategy

### Tributary Propagation Past Lakes
**Issue**: Should tributary BFS stop at lakes? Is a stream entering a lake really a "tributary" of the downstream river?
**Status**: Decided — BFS stops at lakes that have regulations.
- [ ] Verify BFS does not propagate past regulated lakes
