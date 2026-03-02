

# Dead Code Audit

Comprehensive dead code analysis across `regulation_mapping/`, `fwa_pipeline/`, `synopsis_pipeline/`, and `data/`.

---

### 1. Unused Functions / Methods

| File | Lines | Dead Code | Rationale |
|------|-------|-----------|-----------|
| `fwa_pipeline/metadata_builder.py` | 177–249 | `process_stream_chunk()` | Legacy multiprocessing worker function. Superseded by inline code in `MetadataBuilder.extract_streams()` (lines 301–370). Never called anywhere in the codebase. |
| `fwa_pipeline/metadata_gazetteer.py` | 291–299 | `MetadataGazetteer.get_features_in_zone()` | Defined but never called from any file. The regulation mapper builds its own zone→feature index via `build_feature_index()` instead. |
| `fwa_pipeline/metadata_gazetteer.py` | 287–289 | `MetadataGazetteer.get_zone_metadata()` | Defined but never called from any file. Zone metadata is accessed directly through `self.metadata` dict in consuming code. |
| `regulation_mapping/scope_filter.py` | 87–97 | `ScopeFilter.filter_directional()` | Stub method (returns input unmodified). `apply_scope()` never dispatches to it — all non-WHOLE_SYSTEM types hit the generic fallback at line 82. |
| `regulation_mapping/scope_filter.py` | 99–111 | `ScopeFilter.filter_segment()` | Same as above — stub never dispatched to. |
| `regulation_mapping/scope_filter.py` | 113–125 | `ScopeFilter.filter_buffer()` | Same as above — stub never dispatched to. |
| `regulation_mapping/scope_filter.py` | 127–139 | `ScopeFilter.filter_named_part()` | Same as above — stub never dispatched to. |
| `regulation_mapping/scope_filter.py` | 141–153 | `ScopeFilter.filter_vague()` | Same as above — stub never dispatched to. |
| `regulation_mapping/scope_filter.py` | 169–171 | `ScopeFilter.reset_stats()` | Never called from any file (0 callers with `.reset_stats()`). |
| `regulation_mapping/linker.py` | 699–701 | `WaterbodyLinker.reset_stats()` | Never called from any file. |
| `regulation_mapping/regulation_mapper.py` | 1391–1398 | `RegulationMapper.reset_stats()` | Never called from any file. |
| `regulation_mapping/tributary_enricher.py` | 389–397 | `TributaryEnricher.reset_stats()` | Never called from any file. |
| `regulation_mapping/cli_helpers.py` | 47–52 | `format_percentage()` | Never imported or called. `regulation_pipeline.py` defines its own `_format_percentage()` at line 313 with different signature. |
| `regulation_mapping/linking_corrections.py` | 286–287 | `ManualCorrections.has_skip_entry()` | Convenience boolean wrapper never called. Callers use `get_skip_entry()` directly and check for `None`. |
| `regulation_mapping/linking_corrections.py` | 290–291 | `ManualCorrections.has_direct_match()` | Same pattern — `get_direct_match()` is used instead. |
| `regulation_mapping/linking_corrections.py` | 294–295 | `ManualCorrections.has_admin_direct_match()` | Same pattern — `get_admin_direct_match()` is used instead. |
| `regulation_mapping/linking_corrections.py` | 307–308 | `ManualCorrections.has_name_variation_link()` | Same pattern — `get_name_variation_link()` is used instead. |
| `data/data_extractor.py` | 139–157 | `FWADataAccessor.get_attributes()` | Defined but never called from any Python file. Only referenced in `PROPOSAL_ADMIN_POLYGON_INTERSECTIONS.md` (documentation). |
| `data/data_extractor.py` | 195–215 | `FWADataAccessor.get_geometry_dict()` | Defined but never called from any file. |
| `data/data_extractor.py` | 217–248 | `FWADataAccessor.get_features_by_code()` | Defined but never called from any file. |
| `synopsis_pipeline/utils.py` | 10–45 | `normalize_name()` | Defined but never called from any file. The gazetteer has its own `_normalize_for_index()` method instead. |

### 2. Unused Imports

| File | Line | Import | Rationale |
|------|------|--------|-----------|
| `regulation_mapping/regulation_pipeline.py` | 10 | `import shutil` | Imported at module level but `shutil` is never referenced anywhere in the file. |
| `regulation_mapping/linker.py` | 23 | `SkipEntry` | Imported from `linking_corrections` but never used as a type annotation or value. Only appears in a comment at line 126. |

### 3. Commented-Out Code Blocks

| File | Lines | Description |
|------|-------|-------------|
| `regulation_mapping/linker.py` | 622–637 | Commented-out debug block for Similkameen river. Contains `logger.warning(...)` calls and `exit(0)`. Debug code left behind after investigation. |

### 4. Legacy / Superseded Code

| File | Lines | Description |
|------|-------|-------------|
| `fwa_pipeline/metadata_builder.py` | 177–249 | `process_stream_chunk()` — entire function is a leftover from a multiprocessing architecture that was replaced with inline serial processing in `extract_streams()`. The helper functions it calls (`find_zones_spatial`, `_merge_zone_lookups`) are still live because `extract_streams()` also uses them. |

### Not Dead (Verified Live)

The following items were investigated and confirmed **not dead**:

- `clean_watershed_code()` (graph_builder.py:71) — called at line 246 in the same file
- `_merge_zone_lookups()` (metadata_builder.py:161) — called by live `extract_streams()` code
- `find_zones_spatial()` (metadata_builder.py:118) — called extensively throughout the builder
- `get_config_path()` (synopsis_pipeline/config.py:18) — called internally at line 48
- `get_feature_by_id()` (metadata_gazetteer.py:354) — called by regulation_mapper.py
- `get_duplicate_mappings()` (linker.py:298) — called in `__main__` coverage test at line 1097
- `_validate_region_mu_match()` (linker.py:312) — called at lines 586 and 679
- `_input_field()`, `_normalize_text()`, `_normalize_date_text()` (models.py) — all called within models.py
- All `NameVariationLink` class and data — actively used by linker.py and `__init__.py`
- All functions in `compare_sessions.py` — used within the module's `main()` / `generate_report()` flow
- All functions in `regulation_mapper.py` `__all__` exports — public API used by pipeline and tests

---

# Known Errors & Issues

------------
 
5. when a admin area intersects a lake or other polygon we should only put the regulations added into the intersected portion of the polygon. this means splitting the polygon? could also split streams if we desire.

7. need to make the naming better for admin zone unnamed streams. we shouldnt add regulation name if it was an admin match. because then everything has the name. or at least dont show in search? 

8. need to add tidal region into the map. (will remove other regulations and say refer to tidal regulations)

9. we will want to add in the stocked lake info probably here https://www.gofishbc.com/wp-json/wpgmza/v1/features can query it. then we can link to lake fwa and add info for stoked info. 

10. Unnamed Waterbody in liumchen eco reserve split weird. the split on boundary did not work clean here why? 

11. fix missing knap malcom and rubble creek admin boundaries, etc...

12. reparse synopsis 

13. bear river is in region 1 and 2 but should only be in 2 because it is fully in 2 not straddling border...


===============
Complete
===============

---


Should we make it so that ignored matches can link to another regulation which will then be able to display (and allow search) with all name variations. Similarly name variations should make sure both names are available.
- done and implemented. 

---

Campbell lake is not removing the streams inside of it because the park is there changing some of the regulations... then the streams are gone for the first unregulated section of it?
    - So looking more into it issue one john hart l;ake is tributaries only as regulation so for now the stream is gone while the lake doesn't have regulations because of that.
    - cambell lake (lower) has its tributaries (it is only a tributary only regulation too) leaking up into the upper campbell lake area. this means we get streams up there with tributaries. (MAYBE ALL LAKES SHOULD BLOCK LAKE TRIBUTARIES FROM PROGRESSING...). Also the lake became visible because of the park waters regulations but the streams didnt go away  because it doesnt recognize it.
- not an issue when zone regulations are there since everything is regulated.
- might need future though but most likely unneccasry. 
---

parker lake (Waterbody Key 29311070, Area In Hectares 66.0851953431, GNIS ID 1 22600, GNIS Name 1 Parker Lake)). has a little tiny lake friend??? It grouped on the same bluelinekey "lakes_blue_line_359456560_0".. so looking more it seems like things matched with admin areas will not have the waterbody key as an allowed grouping object. which then means all items in the same blk will get grouped together. sometimes gettiung the name of what ever named items are there. so this means when we match a named item in a admin area we need to make it split on the name too? or at least add the waterbodykey to the matched waterboidy keys. Issue will be if we later split the polygons for the different regulation zones (eco zone on one end of lake) then it will have same waterbody key and name but different regs. how to handle this. should mostly work. may need to get creative with waterbodie poyl id etc... ULTIMATELY WE ONLY CARE ABOUT THIS IF SOMETHING HAS A NAME WE DONT WANT UNNAMED ITEMS GROUPED IN WITH IT. -- updated the merging groups and also made sure the admin matches are done inline before mergeing instead of a weird 4th stage to the pipeline... still might want to allow merging of items in nationalparks/ eco reserves? but i am not sure aboutt that could be excessive.

it all just got split up.

---

Dinosaur lake WBK: 329657308 is not a visible polygon but the river is removed under it? Theory is if it iis regulated but not in regulations because it is Error or similar.

- All streams were being filtered regardless of the match or not

---

JUST NEED TO DECIDE: Do we want the river flowing through a lake that has includes tributaries true to add the regulations to the streams entering the lake too?
 - decided to have them stop at a lake that has regulations.

---

---

Similkameen River cut off early? - this is because we only match in the zone... need to figure out how to handle this more generally. 
 - this should be fixable by making it so if a stream (same watershed) is represented in a different zone with regulations but not in another zone then it should get the combined regulations from all zones that the stream has regulations in maybe? 
- could try to see if direct match works just to verify...

all regs go to all segments of a match now

---

maybe it shouldnt go past lakes for stream tributaries too? is it really a tributary of both? or is it only lake/only stream


---

Kettle river is missing segment - This is because of 500m zone buffer it gets some of the segments but not all there...

Do we really care about across the border?? just remove those on export (use no zone?)

---

chilliwack lake doesnt get rid of stream segments
- had a bunch of issues here. mostly waterbody_keys were different in differen locations

---

make sure tributaries enrichment only gets tributaries of provided edges does not travel up mainstem giving more then we want...

---

another issue - not a tributary of chealhis but instead one of pit river... think same issue as the it travels up mainstem instead of just regs provided
===
Morris Creek
Listed as: MORRIS CREEK
REGULATIONS
CHEHALIS RIVER
===

---

why is this entrysplit into 2 segments at columbia lake (becoems tributary of columbia lake)

====
Kootenay River
Listed as:
KOOTENAY RIVER (downstream of Idaho border)
KOOTENAY RIVER (upstream of Koocanusa Reservoir)
===
