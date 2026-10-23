# T112 — One entry walker: validators and remappers each skip different extent lists

Status: DONE — e23ca33d (lane-T112, with T119; one code commit because the app's split check was replaced by T119's gate).

Source: owner 2026-10-22 (unify checks that decide the same thing); unification audit 2026-10-22, finding matching 1
(CONFIRMED).

## Decision being unified
"Which extents and item references does this catalogue entry hold?"

## Where it is decided today
- `pipeline/regs/parsing/validate_catalogue.py:146-157`: skips `rules[].tributary_excludes`.
- `remap_boundaries.py:27-35`: skips every licensing extent.
- `curation-review/backend/reuse.py:1826` `_check_splits`: skips both `tributary_excludes` lists.
- `_referenced_item_ids` and `model_api.py:337`: walk all four.

How they differ: 62 curator-written split references in rule `tributary_excludes` are never validated, and after a
registry rebuild about 30 licensing split references go dead with no warning.

## Work
- `iter_extents(entry) -> (path, extent, role)` and `iter_item_refs(entry)` in a new
  `pipeline/regs/parsing/catalogue_parts/walk.py`. All five places use them.
- Pin: a grep test that no other module reads `tributary_excludes` / licensing extents by key outside `walk.py`.

## Acceptance
Run `canonicalise_splits` over the corpus with the new walker; any new refusals are real typos and get fixed or listed.
No answers change (the walker only widens checking).

Rebuild: none. Depends: none. Size: S. Owner: no.

## As built — 2026-10-23

- `pipeline/regs/parsing/catalogue_parts/walk.py`: `iter_extent_lists(entry) -> (path, list, role)`,
  `iter_extents(entry) -> (path, extent, role)`, `iter_item_refs(entry) -> (path, item_id)`, `ref_key(path)`
  (`matched` / `item_id` / `item_ids` / `outside_items`), `scoped_items(extent)`, `describe(path)`. Roles: `scope`, `rule`,
  `rule_exclude`, `licensing`, `licensing_exclude`. Paths are the entry's own JSON keys.
- Moved onto it: `validate_catalogue.canonicalise_splits` (now returns `(path, msg)`, addressed to `…splits`; message
  "unknown split id …"), `remap_boundaries.apply_remap` (`_extents` deleted), `split_parents.refs_to_parents` (+ new
  `refs_to_parents_at` with paths, so the app's string-parsing `_split_parent_path` is deleted), `model_api.warnings_of`
  and `_corpus_tokens`, `reuse._extent_lists`. `reuse._check_splits`, `_referenced_item_ids`, `_bindable_ids` deleted —
  replaced by T119's gate with `validate_catalogue.cut_point_menu` (matched items + every extent's scoped items, read
  through `iter_item_refs`).
- `parse_context.cut_points(item)`: the one spelling of a boundary's id + aliases (`split:` stripped), read by the parse
  menu and by `validate_catalogue.registry_cut_points`.
- `outside_items` is an item reference but NOT a binding: `refs_to_parents_at` skips it (`ref_key`). The first cut of
  the walker read it as a binding and refused 4 Creston Valley records naming Kootenay Lake (`wbk:328974235`) in
  `outside_items` — which `reach.extent.whole_water_sections` supports on purpose (subtracting the parent takes out its
  parts and ghost). That would have broken the reach run (`build.split_parent_refs` refuses). Pinned.

## Measured (corpus 1,525 entries, live atlas registry, read-only)

- Extents by role: scope 1,357, rule 3,490, rule_exclude 98, licensing 114, licensing_exclude 6. Split references:
  scope 31, rule 654, **rule_exclude 62** (the ticket's 62, never validated before), **licensing 25 + licensing_exclude 5
  = 30** (the ticket's ~30, never remapped before).
- `canonicalise_splits` over every entry with the registry menu (`cut_point_menu`): old 0 refusals, new 0. No typos.
- Split parents (registry and curated polygons): old 0, new 0.
- Answers: unchanged by construction — no deliver module touched; the one reach-run reader (`split_parent_refs`) gives the
  same (empty) list on the corpus. No scratch bundle needed.

## Pins (`pipeline/tests/test_entry_walker.py`)

Whole-value walker tests over one extent in each of the five places, each consumer seeing all five (canonicalise,
alias rewrite, remap dry-run, cut-point menu, split parents, app warnings), and an AST pin: no function in the
checker/remapper modules (validate_catalogue, remap_boundaries, ingest_catalogue, split_parents, model_api, reuse)
reads `tributary_excludes`, or a licensing record's `extents`, by key — one allowance, `reuse.rule_resolved_reach`
(counts one rule's authored carve-outs), itself pinned to exist. Mutations run (each restored):
walker drops rule carve-outs → 9 tests fail; walker drops licensing → 9; remap lists its own places →
`test_remap_sees_all_five_places`; split parents read `outside_items` → `test_a_split_parent_may_be_subtracted_never_bound`
+ the corpus gate test; a checker reads `tributary_excludes` → `test_no_checker_or_remapper_lists_extent_places_itself`.

## Left / follow-ups

- Out of the ticket's five places but the same shape, left alone (each walks all five already, on another shape):
  `review/fingerprint.entry_split_ids` / `_placed` (per-record placement fields), `tools/export_ui_rules._extents` (the
  export doc), `flowing.absorbed_refs` (generic deep scan over `ID_KEYS`). Moving them is a refactor with no defect.

## Checks (2026-10-23, resumed lane)

- Targeted: walker + gate + validator + ingest + parse-context + end-to-end + catalogue + dispatch + curation-review
  API/writes/safety + review reaches + T117 walks: all pass (the corpus gate test ran against the live build).
- Full suite (heavy.sh, unfiltered): 5,158 passed, 15 failed, 28 errors. The 15 fail identically with this change
  stashed (area_boundary curated zones, dataflow gate 4, dfo_salmon archives, entry_id inline parse in review/,
  export_codec on-disk pair, fixture_release, tidal_gauges, update_hydat_script) — pre-existing, not this lane. The 28
  errors are source-data tests (GeoPackage absent in the worktree).
