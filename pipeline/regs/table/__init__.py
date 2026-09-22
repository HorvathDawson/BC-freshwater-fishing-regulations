"""WHAT A RULE IS, AND WHICH RULES APPLY HERE. Nothing about what they come to.

A rule in the catalogue is a flat bag of ~50 optional fields. Nothing in it says which fields
describe WHAT A RULE IS ABOUT, which describe WHERE AND WHEN it binds, or how two of either
compare — so every consumer rebuilt both, and the page ended up with three precedence ladders
plus a fourth override applied straight to a table cell. Four mechanisms, four sets of guards,
four places for a defect. These modules are that vocabulary, and only that:

    corpus.py     the rules, and the section data: which rules fall on which stretch
    subject.py    what a rule is about, as one comparable value, with NO optional fields
    size.py       a length bound, with its polarity decided once — a floor, a ceiling, a slot
    authority.py  WHO wrote it and WHAT it binds to — two typed axes, never one integer
    where.py      the places a rule names
    state.py      which rules apply to a region, an area inside it, or a stretch of one water

THE SETTLING LAYER HAS BEEN REMOVED, to be rebuilt properly. What went: `ledger.py` (allowances
as counters, settled by the ladder), `build.py`, `rows.py`, `clauses.py`, `lifts.py`,
`outcome.py`, `applies.py`, the eight `method_*` gear modules, `provenance.py`, `comply.py`,
`quota_print.py`, `display.py`, `oracle.py` and `trace.py` — about 5,000 lines.

Two things went with it that were not settling and will be wanted back:

    quota_print.py  read each region's printed quota box out of the synopsis PDF and matched
                    every line to the rule that accounts for it. 384 of 384 agreed. It is the
                    only thing that has ever checked these rules against their source, and it
                    needed a settled ledger to do the matching.
    comply.py       every rule handed in is findable in what comes out — the guarantee that
                    settling drops nothing silently.

See `pipeline/docs/06-ui-data-contract.md` for what the rebuilt layer has to produce, and
`05-table-generation.md` for how the removed one worked.
"""
