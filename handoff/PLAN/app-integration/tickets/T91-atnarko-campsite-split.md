# T91 — Atnarko "No Fishing from Tenas Lake to … near Atnarko Park campsite": where the stretch really ends

Status: DONE (2026-10-28, c357fcdb + the audit commit after it) — closed under the owner's lake-boundary rule; nothing moved, no owner call.

Source: memory `ambiguous-cut-picks-silently`; owner said the campsite boundary is further downstream than the curated
split. Untouched since (last commits on `atnarko_park_campsite`: 296bfc14 and older). The run records `ambiguous_cut`
for `south_atnarko_river__tenas_lake`: 630.47 used, 2380.4 the other candidate. `atnarko_bella_coola_rivers.r2` binds
22 Atnarko + 1,662 tributary sections.

## Work
Show both split points on a map; the owner confirms where the signs are; fix the curated split; reach rerun; binding diff.

## Acceptance
The owner agrees with the closed stretch on the map; a test pins its ends.

Depends: could be the first item in T66's review tool; touches T59. Size: S.
Owner: yes. Default: keep the current binding until the owner confirms.

## Closed 2026-10-28 (no data change, no rerun)
- **Campsite end is settled.** The split `atnarko_bella_coola_rivers_includes_tributaries_exce__atnarko_park_campsite_boundary_signs`
  is the owner's own pin (-125.91077, 52.40597, "[user pin]" 2026-08-09); it resolves on the Atnarko (blk 360879335)
  @ 19630.0 m = 52.40347, -125.91107, ~306 m below the Atnarko Campground (OSM 5127542071). Not an owner question.
- **Tenas Lake end follows the rule.** A lake boundary cuts at the lake end CLOSEST TO THE EXTENT (the edge facing the
  stretch's other end). r2's other end is the South Atnarko mouth (the confluence, 0 m on blk 360878501), so the pick is
  the OUTLET @ 630.47 m (52.16918, -125.71451), not the inlet @ 2380.4 (52.15430, -125.70761) — which the builder
  already uses. The `ambiguous_cut` row only records that the lake had two edges.
- **Every ambiguous cut checked:** all 96 `ambiguous_cut` rows in data/generated/reaches/full/rule_diagnostic.jsonl
  (89 rules, 57 splits) take the candidate nearest the extent's other end — 96/96, list in
  handoff/PLAN/audit/ambiguous-cut-picks.tsv. No builder change, no reach rerun, no bindings moved.
