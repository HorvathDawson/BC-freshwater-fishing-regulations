"""The quota table as a VALUE, resolved in the pipeline rather than in the browser.

A rule in the catalogue is a flat bag of ~50 optional fields. Nothing in it says which fields
describe WHAT A RULE IS ABOUT, which describe WHAT YOU MAY DO, or how two of either compare — so
every consumer rebuilt both, and the page ended up with three precedence ladders plus a fourth
override applied straight to a table cell. Four mechanisms, four sets of guards, four places for
a defect, and a steady trickle of them.

    subject.py   what a rule is about, as one comparable value, with NO optional fields
    outcome.py   what you may do, as one point on a TOTAL ORDER — so "stricter" is a comparison
    applies.py   whether it bites here and now, always — or only in a season or an undrawable spot
    clauses.py   sub-limits, pooled quotas, and lifts
    resolve.py   the fold: authority wins, except a take of zero; the answer is chain[0]
    build.py     one interned ruleset in, one finished table out
    comply.py    the guarantee: every rule handed in is findable in the table that comes out
"""
