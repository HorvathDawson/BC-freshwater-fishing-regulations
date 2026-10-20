# Research A — pipeline data map (partial; relayed by the coordinator from the research agent's report)

Chain: `python -m pipeline build` stages reach → tiles → deliver (build.py:38,138-146); atlas only with --atlas; vintage.report at the end (build.py:268-273). Basemap is not a stage (nothing rebuilds basemap.pmtiles, Sep 25).
`pipeline.deliver all` (__main__.py:113-140), each step a subprocess: bundle → verdicts → status_index → export (+guide) → answers (.json + .gz) → store (regs.sqlite, atomic os.replace build.py:212) → vintage (atlas.meta section_handles vs bundle meta; registry digest).

Sizes: atlas/full pkl/json ≈ 6.3 GB (middle-man); reaches/full ≈ 20.4 GB (rule_section.jsonl 20 GB; middle-man); tiles: atlas.pmtiles 530.7 MB, atlas.meta.json 97 B, basemap.pmtiles 918.4 MB, layers/*.geojsonl ≈ 4.3 GB (tippecanoe inputs; water_map_export also reads stream/lake layers by section_id).
bundle/: bundle.sqlite 102.7 MB, verdicts.sqlite 46.7 MB (pipeline-only), regs.sqlite 14.2 MB, status_index.bin 1.66 MB.
regs/: ui-rules-answers.json 38.0 MB (.gz 1.78 MB), ui-rules-export.json 6.9 MB, ui-rules-guide.json 1.1 MB.
Stocking/bathymetry tables in the bundle are EMPTY (stock_water, stock_code, release, chart = 0 rows).

bundle.sqlite tables the app READS (app/packages/data/src/bundle/queries.ts): alias, basin_member, chart, entry, gauge, gauge_clim (4.1 MB), item, item_section, lake_gauge, meta, place, place_water (4.9 MB), release, section_down (1.3 MB), section_gauge (2.5 MB), section_panel (5.0 MB), panel_member, stock_water. (queries.ts also has `FROM near` / `JOIN towns`, probably CTEs — unconfirmed.)
NOT read by the app (≈70 of 103 MB): section_ruleset (42.3 MB, 1.96M rows), ruleset+ruleset_by_entry (19 MB), section_licensing (7.8 MB), split, section_span, span_end, section_touch, item_down, trib_nearer, part, part_section, unnamed_part, steelhead_*, province_except, mu_set, outside_bc, tidal, section_straddle, section_home, region_place, gauge_stats, item_alias, rule, rule_ix, rule_key, licensing tables. Their pipeline readers: status_index (section_ruleset, tidal, outside_bc, item_section), store build (part, part_section, item, section_ruleset.up/place, region_place, unnamed_part), answers/export (ruleset, rule, rule_key, designation, licensing_set), water_map_export (section_down, part, part_section).

verdicts.sqlite: read by status_index.py:209, export_ui_rules.py:5648, answers.py:155/:421. Never shipped.
status_index.bin (BCSI v2): header magic + version 2 + section_handles + reach_digest; 366 day profiles; codes 0 base/1 own/2 closed/3 tidal/4 outside; computed from verdicts + bundle tables. Shipped (core/src/statusIndex.ts, ui/src/hooks.ts:508) keyed by tile feature id = sid.
ui-rules-export/guide.json: read by answers, store, page_data, water_map_export, tools, ~35 tests. Not read by anything shipped. Middle-man.
ui-rules-answers.json: read by store build, page_data.py:71, water_map_export.py:668. No sid data inside (parts: item → part keys; unnamed). Middle-man.
regs.sqlite (regs-store/4): (1) lossless re-encoding of answers.json (build refuses unless decode == raw, build.py:203-206); (2) lossless export SUBSET (rule/licensing words, slim entries, licences, species, conduct, rulesets, slim waters; store/common.py:170-186); (3) section_akey (sid → akey u16, 8 KB blocks; from bundle part_section + answers parts, else section_ruleset.up + unnamed.parts; 0 = outside BC) and section_place (sid → place). At READ time it needs NOTHING from bundle.sqlite (decode.py opens only the store). Not yet read by app/packages.
sid = row index in atlas section_handles.txt = tile feature id = status_index key (digest d768d6e135e62b99). Tile id mechanism (top-level id vs promoteId) NOT VERIFIED.
No section geometry/midpoint/bbox in any shipped artifact; geometry only in atlas pkl, tile layers, and watermap.js (projected SVG paths, prototype only).
Topology: section_down (sid → down_sid, 108k rows, shipped), item_down, trib_nearer, section_touch, span_end, split (not shipped). part.rep_sid exists.
Prototype chain (none ship): page_v36.html 5.3 MB (page_data.py from the three regs JSONs) → v36snap.js 15.4 MB (snap.mjs) → oracle.json, regs-diagram/data.js; watermap.js 288 KB (water_map_export from bundle + status_index + export/answers + tile layers); port goldens ≈ 10.9 MB (sync.mjs; taps.py/verify_map.py read regs.sqlite).

NOT VERIFIED (delegated, not reported): atlas/reach writer lines; tile feature properties, promoteId, tippecanoe flags; gauge feed shape/producer; bc_outside.geojson producer; fish/font listing; full per-table writer/reader refs.
