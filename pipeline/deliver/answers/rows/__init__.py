"""THE ROWS OF TODAY'S CARD — consumer Stages 5.2 step 5 to 5.8, from the reader's traced ladder.

The consumer page v35 (`archive/pipeline/answers-reference-v35/page_v35.js`, the SPECIFICATION ported here) turns the ladder's
speaking rules into the card an angler reads: the number they may keep after the winner's clauses
(5.2 step 5), the lines under it (steps 6-11), the fish grouped into one row per shared limit with
the fish that go back and the cross-references (5.3), the conditions on keeping (`rowConds`), the
real daily limit when every kind has its own smaller cap (5.7, "Really 2 a day here"), and each
kind's keep range and band numbers (5.8). This module computes exactly those, from:

  * the reader's traced verdict per fish and origin (the `ladder` section's value: `read.
    effective_rules_bound(trace=True, origin=…)`), which says which rule SPEAKS, which was
    displaced or lifted and by whom, and which is partly lifted;
  * the rules' own fields (the bundle's rule table) and D's display facts (`display.kind_of`, the
    page's 15-step kind; `display.bands`, the size bands);
  * the part: its water kind, whether a rainbow over 50 cm is a steelhead there, the steelhead
    presence and whether the steelhead rules apply.

IT NEVER RE-DECIDES A RULE. Which rules speak is the reader's; this module only arranges the
speakers into the page's answer (winner, number, lines, rows) the way the page arranges its own
ladder's. Every place the page is unclear, or where our reader and the page's ladder name things
differently, is written down in `DECISIONS`.

Words are the page's (it keeps presentation): every value here is structured — rule ids, fish
codes, numbers, line types, band edges. A rule is named by its id ("entry::rule"); the encoder
writes it as the export's rule index.

THE MODULES, in the page's order: `rule` (R), `evaluate` (5.2), `card_model` (5.1/5.3), `conds` (5.6-5.8),
`part` (Part), `wire` (the projection), `groups` (totals, kept elsewhere), `assemble` (`produce`),
`presence`, `weekdays`, `section` (the answers file's `rows` section); `decisions` holds `DECISIONS`.
"""
# The package's face: every name its modules define, so `<package>.X` keeps meaning what it did.
# A test that switches a module-level value must set it on the module that READS it (see each module).
# ruff: noqa: F401
from pipeline.deliver.answers.rows.decisions import (
    DECISIONS,
)
from pipeline.deliver.answers.rows.rule import (
    INF, R, _keepish, _covers, _ge, _bands, size_lines, _js, _keep_range,
)
from pipeline.deliver.answers.rows.evaluate import (
    _stable_sort, eval_sp, _SAME_FIELDS, restates, _COMBINED, _two_places, combined_words, restated_words,
    same_statement, nearer_than, row_restates, fish_range, _range_released, _size_release, fish_answer,
    main_res, counts_toward,
)
from pipeline.deliver.answers.rows.card_model import (
    A_DISPLACED_POOL_IS_NO_CROSS_REFERENCE, _icu_key, _fact_key, _min_max, _origin2_rule, _line_key,
    FACT_ORDER, species_at, build_model,
)
from pipeline.deliver.answers.rows.conds import (
    _who_n, _steel_carve, _not_counted, Raised, raised, raises, _raise_of, _limit_take, row_conds, _short,
    _COVER, keep_range, fish_bands, _against, species_items, origin_lines, _shared_caps,
    eff_cap, _under, _shared_counts,
)
from pipeline.deliver.answers.rows.part import (
    Part, open_fish,
)
from pipeline.deliver.answers.rows.wire import (
    _num, _LINE_FIELDS, line_out, stored_role, res_out, fact_out, cond_out, badge_of, scope_of,
)
from pipeline.deliver.answers.rows.groups import (
    _kept, total_of, _names, _names_short, _range_words, _clauses, _clause_take, _lift_words, group_of,
    A_TROUT_CHAR_TOTAL_OF_ONE_KIND_IS_A_GROUP, _trout_char, raise_of, written_name_of, _scope_name,
    _kept_origin, _line, elsewhere_of, over_elsewhere, parent_of, A_ROW_HELD_BY_ITS_TOTALS_GROUP,
)
from pipeline.deliver.answers.rows.assemble import (
    A_ONE_KIND_GROUP_ONLY_ON_A_DRAWN_ROW, produce,
)
from pipeline.deliver.answers.rows.presence import (
    PRESENCE_WORDS, presence_words, place_presence,
)
from pipeline.deliver.answers.rows.weekdays import (
    weekdays_words, weekdays_short, weekdays_label, weekdays_when, _week_answer, _WEEK_RANK, say_weekdays,
)
from pipeline.deliver.answers.rows.section import (
    section_scope, open_states, refs, section_derive, no_answer_gate,
)
