# T112 — One entry walker: validators and remappers each skip different extent lists

Status: OPEN.

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
