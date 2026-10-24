"""The regulations, from the reach builder's output into the bundle.

FOUR TABLES, AND ONE OF THEM IS THE WHOLE DESIGN.

`entry` and `rule` are transcription — the curated corpus, carried verbatim. The interesting
pair is `section_ruleset` and `ruleset`, and what they are is a compression that only works
because of something true about regulations rather than about data: a rule applies to a
STRETCH of river, and a stretch is many sections. The obvious table — one row per (section,
rule) — is 1,720,243 rows and 69.6 MB on a 51.8 MB bundle. Those rows carry 1,905 distinct
answers. Interning them costs 12.1 MB and reads four times faster.

THIS MODULE CONSUMES, IT DOES NOT DERIVE. Everything about which water a rule covers —
the four tributary states, the per-rule carve-outs, the reach-scoped walk with its three
guards — is decided in `pipeline.atlas.reach` and arrives here already resolved. Re-deriving
any of it from the curated flags would put a second implementation of "which water does this
rule cover" in the codebase, which is exactly how three copies of the trust rule happened.
The flags are read here for ONE thing (`uncertain`) and that is not a re-derivation.

THE MODULES: `tables.write` writes the tables; a rule's row is `row`, what it lifts `lifts`, what it
is (keeps, closes, the statement it makes) `predicates`; pointer rows `see`; the regions a row's water
lies in `regions`; steelhead presence `presence`; the reach run's diagnostic tables `diagnostics`.
"""
# The package's face: every name its modules define, so `<package>.X` keeps meaning what it did.
# A test that switches a module-level value must set it on the module that READS it (see each module).
# ruff: noqa: F401
from pipeline.deliver.bundle.rules.predicates import (
    _zone_region, AGGREGATE_GROUPS, ORIGINS, release_origins, CLOSURE_CONDITIONS, CLOSURE_GRADES,
    catch_and_release, closure_grade, keeps, _leaves, statement, same_statement,
)
from pipeline.deliver.bundle.rules.row import (
    _when, _mus_of, _specificity, _NOT_CONDITIONS, _rule_row,
)
from pipeline.deliver.bundle.rules.lifts import (
    LIFT_KEYS, SIZE_CLAUSE_OVERRIDE, is_size_clause, overrides_size_clause, ANY_SIZE, prints_any_size,
    size_clause_caution, NAMES_THE_FISH, ANY_SIZE_BASIS, _species_of, _targets, _lift_terms, _when_term,
    named_leaves, _is_species_closure, _NO_EXCEPTIONS, _SEE_TABLES, sends_to_tables, prints_its_exemptions,
    zone_closures, _kept_lengths, _named_lifts, is_size_constraint, _any_size_lifts, is_blanket_closure,
    blanket_closures, lift_kind, _equivalent_closures, _exempts,
)
from pipeline.deliver.bundle.rules.see import (
    _see_column, entry_name, see_name, see_pointer_rows, ONE_WAY_IN_ACROSS_ROWS,
)
from pipeline.deliver.bundle.rules.regions import (
    WATER_REGIONS_BY_HOME, regions_of_waters, _own_rows, co_bound_regions, own_entry_regions,
    equivalent_regions,
)
from pipeline.deliver.bundle.rules.presence import (
    write_steelhead_presence, prove_steelhead_presence,
)
from pipeline.deliver.bundle.rules.diagnostics import (
    ambiguous_cuts, run_rows,
)
from pipeline.deliver.bundle.rules.tables import (
    _jsonl, intern_sets, check_corpus, read_entries, write,
)
