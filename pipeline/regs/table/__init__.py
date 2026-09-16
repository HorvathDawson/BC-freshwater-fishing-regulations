"""The quota table as a VALUE, resolved in the pipeline rather than in the browser.

A rule in the catalogue is a flat bag of ~50 optional fields. Nothing in it says which fields
describe WHAT A RULE IS ABOUT, which describe WHAT YOU MAY DO, or how two of either compare — so
every consumer rebuilt both, and the page ended up with three precedence ladders plus a fourth
override applied straight to a table cell. Four mechanisms, four sets of guards, four places for
a defect, and a steady trickle of them.

    subject.py    what a rule is about, as one comparable value, with NO optional fields
    outcome.py    what you may do, as one point on a TOTAL ORDER — so "stricter" is a comparison
    applies.py    whether it bites here and now, always — or only in a season or an undrawable spot
    authority.py  WHO wrote it and WHAT it binds to — two typed axes, never one integer
    ledger.py     every allowance as a COUNTER, settled by the ladder: the model
    build.py      rules in, ledger out — stage 1 the region's standing table, stage 2 overrides
    rows.py       the table, DERIVED from the ledger: one row per fish treated alike
    oracle.py     may I keep this fish? — decided from the same counters the rows show
    comply.py     the guarantee: every rule handed in is findable in the ledger that comes out
"""
