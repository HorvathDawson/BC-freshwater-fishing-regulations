# Reach builder

Built and running: `pipeline/atlas/reach/`. This page says what it is, from the code (rewritten 2026-11-10, T561; the
2026-09 sketch it replaces, with its open-policy table, is in git history). Rulings on how the book is read are
`RULINGS.md`; the binding rules for agents are `AGENTS.md` 10–16 and 63.

## What it is

A regulation says *"between the Tamihi Bridge and Vedder Crossing"*. The map is made of numbered pieces of water
(sections). The reach builder turns the first into the second for every rule of the corpus, and reports what changed
since a previous run.

It **does not cut water** — it selects among the sections the atlas's sectionizer made from `waters/cuts.json`
(AGENTS 10). Reaches resolve by route measure on the cut's own blue line, never by a flow walk (AGENTS 11).

Live run (`data/generated/reaches/full/report.json`, read 2026-11-10): 1,515 entries, **3,401 rules — 3,376 bound,
25 unresolved** (`no_sections_for_items` 21, `no_extents` 2, `no_sections` 2), `tributaries_pending` **0**,
`unclassified` 2. A full run took 553 s, almost all of it loading the graph; do not build parallelism (AGENTS 43).

## Parts

```
pipeline/atlas/reach/
  models.py        RuleBinding · Diagnostic · Outcome · Reason          (pure dataclasses; totality enforced here)
  build.py         the pass: entries + atlas -> bindings                (orchestration, extents, scope, `rest`)
  extent.py        one authored extent -> sections                       (cuts, between, within, watershed, flow placement)
  classify.py      a resolved reach -> bound | unresolved(reason)        (every policy constant, one table)
  covered.py       the items an entry regulates                          (shared with the review app, AGENTS 16)
  outside.py       region limits, outside B.C., tidal, rowed waters
  licensing.py     licensing records and designations                    (place_record, first classified downstream)
  steelhead.py     book-known steelhead water and presence               (AGENTS 54)
  dfo_salmon.py    the DFO salmon chain per section
  water_kind.py    the water kind every filter reads                     (the registry's, AGENTS 55)
  position.py      each piece's nearest placed pieces on its line       (geometry the placement reads)
  io.py            read entries · write the run's tables + report.json
  word_reads.py    the printed words the reach reads, and the guard on them (T679 D4: a lost read stops the run)
  diff.py          two runs -> rules whose sections changed
  binding_diff.py  two runs -> binding differences, ranked
  cache.py         POLICY_VERSION and its changelog (no cache: see below)
  cli.py           python -m pipeline.atlas.reach.cli
```

## Running it

```bash
python -m pipeline.atlas.reach.cli --build data/generated/atlas/full --out data/generated/reaches/full
python -m pipeline.atlas.reach.cli --build data/generated/atlas/full_next --out data/generated/reaches/full_next \
                                   --against data/generated/reaches/full
```

`python -m pipeline build` runs it as its `reach` stage (AGENTS 62); a placement-only change needs only this step and
the bundle, not an atlas rebuild. It is `cli`, not `build`: `build.py` is the library and has no `__main__`.

## Outputs (`data/generated/reaches/<build>/`)

| file | what |
|---|---|
| `rule_section.jsonl.gz` | the bindings, keyed `(entry_id, rule_id)` — `rule_id` is unique only within an entry (AGENTS 8) |
| `rule_unresolved.jsonl` | every unresolved rule with its typed `reason` and detail — never a silent empty |
| `rule_extent.jsonl` | the authored extents beside their resolution, so a binding can be explained |
| `rule_diagnostic.jsonl` | ambiguous cuts, unclassified pieces, tributary growth, clips — kept, not dropped |
| `licensing_section.jsonl`, `licensing_placement.jsonl`, `licensing_diagnostic.jsonl` | licensing records |
| `steelhead_presence.jsonl`, `tidal.jsonl`, `outside_bc.jsonl`, `dfo_salmon.jsonl.gz` | facts the bundle READS (AGENTS 56) |
| `report.json` | counts, outcomes, reason histogram, diagnostics, the input and output digests |

## Properties

| property | what it means |
|---|---|
| **Deterministic** | sorted iteration, no clock; `report.json` `digest` is identical across runs on the same inputs (AGENTS 19) |
| **Total** | every rule ends `bound` or `unresolved` with a typed reason — never absent, never bound-and-empty (`RuleBinding.__post_init__`, AGENTS 14) |
| **Auditable** | the authored extent is stored beside its resolution |
| **One implementation** | the review app calls `classify` / `covered` through `entry_reaches` (AGENTS 16) |

**No cache.** `cache.py` once held a per-entry result cache keyed on the entry, the build and the policy. Nothing
called it, and it could not be made safe: since policy 14 an answer depends on OTHER entries (which waters have a row
of their own, `outside.rowed_waters`), which a per-entry key does not see. It was deleted (T561). What stays is
`POLICY_VERSION` — the policy's changelog, bumped with a line whenever `classify.py` changes an outcome (AGENTS 20).

## Policy (`classify.py`)

Each decision is one named constant, pinned by `test_reach_builder.test_policy_version_is_bumped_when_classify_changes`.

| constant | value | meaning |
|---|---|---|
| `STRADDLERS_INCLUDED_FOR` | `frozenset()` | an unclassified piece is never auto-included; it is reported (AGENTS 12) |
| `AMBIGUOUS_CUT_IS_FATAL` | `False` | a cut at two measures takes the one nearest the rule's other end, always with a diagnostic |
| `PARTIAL_EXTENTS_BIND` | `True` | a rule with several extents binds what resolved and records the rest |
| `AREA_CARVE_OUTS_UNBIND` | `True` | an area rule with a carve-out nothing draws stays unbound (AGENTS 13) |
| `FEATURE_TYPES_AFTER_WALK` | `True` | `feature_types` limits the whole reach, after the tributary walk |
| `WALK_BEFORE_AREA` | `True` | a watershed limited to an area is walked first, clipped after |
| `OUTSIDE_BC_SUBTRACTED` | `True` | water outside B.C. leaves every binding |
| `NOT_FISHING_WATER_SUBTRACTED` | `True` | sewage basins, tailing ponds and dumps leave every binding |
| `COMPLEMENT_UNKNOWN_IF_A_SIBLING_DOES_NOT_BIND` | `True` | "other parts" is unknown while a sibling is unbound — never the whole water |
| `COMPLEMENT_WITHHOLDS_STRADDLERS` | `True` | a sibling's straddling piece is withheld from "other parts" too |
| `UNDRAWN_PART_DOES_NOT_WALK` | `True` | a rule held as an undrawn part never walks tributaries |

## Reason vocabulary (`models.Reason`)

`rule_unresolved.reason` is a closed set, extended only deliberately:

```
no_registry             no_extents              no_sections_for_items   locators_unresolved
cut_not_found           cuts_collapsed          parallel_branches       empty_after_scope
scope_unresolved        area_scope              area_id_dangling        no_sections
outside_bc              tidal                   complement_unknown      needs_corpus
tributaries_pending     no_tributaries          unknown
```

`tributaries_pending` means the walk could not run (no seed), never that it is unbuilt; a binding flagged pending is
never complete (AGENTS 15).
