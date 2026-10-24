"""Reading the regulations back OUT of a built bundle — the only source a reader may use.

    rules(bundle)        every rule as the bundle ships it, `conditions` hoisted to the top level  (records)
    source_of(rule)      WHO wrote a rule and WHAT it binds to — two typed axes, and the ladder rank
                         derived from them (the export's `provenance`)                             (source)
    in_force, covers …   what one rule says and what one section is                                (facts)
    effective_rules      who speaks — the ladder, executable: the reference reader                 (ladder)
    requirements_in_force  which licensing records hold on a section, on a day                     (licensing)

NOTHING HERE READS A CURATED FILE. A fallback to the entry files is how a reader answers with
something the bundle never agreed to (a size fix shipped half-applied that way), so there is none.
"""
# The package's face: every name its modules define, so `<package>.X` keeps meaning what it did.
# A test that switches a module-level value must set it on the module that READS it (see each module).
# ruff: noqa: F401
from pipeline.deliver.bundle.read.records import (
    BUNDLE, rules, _DERIVED_COLUMNS, rules_from, rid,
)
from pipeline.deliver.bundle.read.source import (
    Authority, Scope, Source, region_words, _area_words, source_of,
)
from pipeline.deliver.bundle.read.facts import (
    _day, IN_FORCE, _days_of, in_force, speaks_for, names_fish, not_yet_mapped, _steelhead_min_cm,
    steelhead_rules, home_region, straddles, steelhead_water, STEELHEAD_CODES, steelhead_presence,
    as_rainbow, _spoken_lengths, covers, released_on_water, _base_dimension,
)
from pipeline.deliver.bundle.read.ladder import (
    _RULES_BY_PATH, MOOT_SIZE_CLAUSE_HIDDEN, WATER_CLOSURE_DOMINANT, OWN_ROW_BEATS_INHERITED,
    OWN_ROW_REPLACES_INHERITED, NEARER_ROW_BEATS_FARTHER, SPEAKERS, SPEAKER_STATES, LOSS_REASONS,
    LOSER_STATES, ASKABLE_ORIGINS, VIAS, place, _ladder_place, _NEARER_BY_PATH, _nearer_of, _rules_of,
    base_region, _count, stricter, effective_rules, origin_matters, _leaf_fish, effective_rules_bound, _Ask,
    _step0_steelhead_sizes, _step1_in_force, _step3_lifts, _own_replacement, _step3b_own_row_replaces,
    _step2_about_this_fish, _step4_competition, _step4a_water_dates_on_a_water_kind, _step4b_zone_release,
    _on_its_water, _own_water_closure, _step4c_closures, _released_lengths, _step4d_size_release, _beaten_by,
    _same_row_dated_release, _same_row_releaser, _step5_water_release, _step5b_moot_size_clause,
    _step6_two_regions, _answer,
)
from pipeline.deliver.bundle.read.licensing import (
    designations_in_force, stamp_waived_here, steelhead_stamp_period, requirements_in_force,
    LICENSING_TABLES, considered_records, section_kind, as_bound,
)
