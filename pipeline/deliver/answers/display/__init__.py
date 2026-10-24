"""DERIVED DISPLAY FACTS — what the page re-derives today from fields, shipped instead.

PER RULE (consumer Stage 2.3, 2.4, 6.3), from the rule's own fields:

    kind     the page's 15-step kind test (`kindOf`), in its order and its vocabulary: gear,
             conduct, vessel, anglerclosure, duty, exempt, standing, while, possession, annual,
             sizecap, subcap, gate, pool, size — plus `possession_cap` (a possession limit with
             a number; the page calls it annual: page bug d10, rows F6). `closure` marks a gate that is a closure ("No
             fishing for …") rather than a release ("Release every …") — asked of
             `rules.closure_grade`, the one closure predicate (AGENTS 56).
    bands    `lengths` expanded into contiguous bands from 0 cm up, first range wins:
             [[from_cm, to_cm | null (no top), take | null]] (`bands`).
    plain    the rule as one plain sentence (`sayRule`): "In streams, keep up to 2 trout and char a
             day, all kinds together, hatchery only." — generated ONCE here, so which fish ("trout"
             is trout and char unless char are excluded, AGENTS 46), which origin, which size and
             which dates are said by code under test. null where the page writes no sentence (it
             then falls back to the rule's `label`).

PER PART OF A WATER (Stage 3.1, 3.2, 5.6), from the export's decoded model — its waters, parts,
runs, splits and entries are the bundle's, composed by the export (`codec.expand` is the
reference decoder; the pair's `about.bundle` digests must equal the bundle's):

    runs     where it runs ("From the mouth up to the Slesse Creek confluence")      `runsLabel`
    place    the entry it comes from ("Upstream of Kamloops Lake")                    `partPlace`
    hint     what sets it apart ("No fishing · also …")                               `partLabel`
    label    the picker's name for it, tie-broken                                     `partLabels`
    km       its sort key (highest km of its main-stem runs; side channels where they join)
    closed_all_year   closed to every game fish on all 366 days — the status index's own answer
             (`status_index.set_profile`), not the page's `isBroad` re-reading
    group / picker    the closed-all-year merge, the mouth-upward order and the entry headings
    steelhead_line    possible_with_rules | known_with_rules | known_no_rules | null   (5.6)

Wording follows the page (v35) so a 1-to-1 check can compare strings; the page keeps styling.

THE MODULES: `rule` (per rule), `part` (per part, `Water`), `builders` (every rule, every part), `unnamed`
(the unnamed water's names), `subsets` (G3), `strip` (the open-days strip), `decided` (how this was
decided), `section` (the answers file's `display` section); `decisions` holds `DECISIONS`.
"""
# The package's face: every name its modules define, so `<package>.X` keeps meaning what it did.
# A test that switches a module-level value must set it on the module that READS it (see each module).
# ruff: noqa: F401
from pipeline.deliver.answers.display.rule import (
    INF, _js, _num, _is, _gt0, kind_of, is_closure_gate, cut_points, size_shape, bands, written_name,
    POSSESSION_HOME, _home, UNLESS_OPENED, caught_sentence, unless_opened, _MANY, plain, _plain, _DASHED,
    title, _title, CANT_FISH_TITLE, where_of, cant_fish_words, rule_facts, cut_words,
)
from pipeline.deliver.answers.display.part import (
    _win_txt, _js_round, Water, part_label, _region, part_place, _THE_LESS, begins_at, end_name, _run_txt,
    _joining, runs_label, _SIGN_WORDS, _end_short, _run_short, short_label, short_labels, part_km,
    part_labels, picker, STEELHEAD_LINES, steelhead_line,
)
from pipeline.deliver.answers.display.decisions import (
    DECISIONS,
)
from pipeline.deliver.answers.display.builders import (
    build_rules, when_problems, _CM_SAID, _LONGER_NONE_OVER, size_problems, unless_opened_here, closers_of,
    closed_all_year, paper_licence, record_kept, produce_parts,
)
from pipeline.deliver.answers.display.unnamed import (
    PLACE_TEMPLATE, regions_label, place_label, places_table,
)
from pipeline.deliver.answers.display.subsets import (
    WINS, row_sources, subset_asks, subset_says, add_subsets,
)
from pipeline.deliver.answers.display.strip import (
    STRIP, STRIP_BIT, strip_legend, strip_code, compose,
)
from pipeline.deliver.answers.display.decided import (
    REASON_WORDS, reasons_table, _region_of, _lc_place, trib_owner, _fx, _parts, _days, _days_txt,
    INHERITED_KINDS, inherited_kind, inherited_line, applies_here, book_pages, licensing_books, add_sources,
    _unless_words,
)
from pipeline.deliver.answers.display.section import (
    STATUS_NAMES, section_scope, section_prepare, inherited_here, inherited_caution, section_static,
)
