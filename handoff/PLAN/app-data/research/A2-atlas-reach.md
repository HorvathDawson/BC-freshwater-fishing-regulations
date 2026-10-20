# Research A2 — atlas + reach artifacts (relayed by the coordinator)

Atlas: `python -m pipeline.atlas.build --full --out <dir>` (build.py:320 main); refuses an --out whose digest is shipped; built to <dir>_next, swapped by pipeline.atlas.promote. pipeline.atlas.sidecars rewrites registry/region_home/item_points/merge_report/splits.resolved.
- graph.pkl 614 MB (build.py:991): StreamGraph nodes/edges; main source for bundle, tiles, reach. Not middle-man.
- geometries.pkl 2.17 GB (:992): node_id → shapely line EPSG:3005; read by bundle (place_water, gauges), tiles export.py:138, regions, reach position, gauges, curation.
- blk_chains.pkl 2.57 GB (:990): NO run-time reader (only test_no_slivers determinism hash). DEAD OUTPUT.
- waterbody_polys.pkl 763 MB (:987, no meta): read by bundle (area_ha), tiles, regions, reach, sidecars, curation.
- registry.json 115 MB (registry/io.py:63): items {id,name,kind,variants,mus,section_ids,ref_ids,part_of,aliases,boundaries}; parser source of truth; feeds bundle item table, tiles, reach, dfo, tools.
- section_handles.txt 26 MB (section_handles.py:74): the graph key set in fixed order (1,957,202 = 1,235,835 stream pieces + 721,367 lakes); sid = 1-based line number, 0 = none; sorted numeric blk, measure, lakes last. digest_for = sha256[:16] → reach report.handles, bundle meta.section_handles, atlas.meta.json. registry_digest_for = sha256 canonical registry[:16].
- splits.resolved.json 1.4 MB: bundle spans/place_names, curation, dfo, tools.
- region_home.json 5 KB → bundle section_home; item_points.json (dfo dossier only); area_catalog.gpkg 38 MB (regions, bundle area names); merge_report.json, summary.txt, *.meta.json (input_hashes empty; is_stale has no callers) = dead/human-only.
vintage.report compares TILES vs BUNDLE only (handles, then registry digest); reach↔atlas pairing enforced in bundle/build.py:118-159, 846-884.

Reach: `python -m pipeline.atlas.reach.cli --build … --out …`; writes via io.py:74-173 write_run.
- rule_section.jsonl 20.1 GB: one row per (rule, section) {entry_id, rule_id, scope, section_id}; only the bundle (rules.py intern_sets → section_ruleset/ruleset) and the reach diff read it. MIDDLE-MAN (fully expanded form of the bundle's interned rulesets).
- licensing_section.jsonl 136 MB, steelhead_presence.jsonl 122 MB, straddle, tidal, outside_bc, rule_unresolved, licensing_placement → bundle only. MIDDLE-MEN.
- rule_extent.jsonl → only water_map_export (prototype). rule_diagnostic/licensing_diagnostic → no reader (human audit).
- report.json: pairing digests (handles, registry_digest, entries_digest, digest → bundle meta.reach_digest).
Findings: unread outputs (blk_chains 2.57 GB, metas, merge_report, summary, diagnostics); every reach jsonl is a middle-man for bundle.sqlite.
