"""WHICH RULES APPLY HERE. Three modules, and nothing that decides what they come to.

    corpus.py     the rules, and the section data — which rules fall on which stretch of which
                  water, and the accessors over it
    authority.py  WHO wrote a rule and WHAT it binds to — two typed axes, never one integer.
                  `rank` is derived from the pair and is never stored.
    state.py      selection: the rules of a region, of a named area inside it, or of one
                  stretch of one water

THE SETTLING LAYER HAS BEEN REMOVED, to be rebuilt properly: `ledger.py` (allowances as
counters, settled by the ladder), `build.py`, `rows.py`, `clauses.py`, `lifts.py`, `outcome.py`,
`applies.py`, the eight `method_*` gear modules, `provenance.py`, `comply.py`, `quota_print.py`,
`display.py`, `oracle.py`, `trace.py` — and with them `subject.py`, `size.py` and `where.py`,
which were its vocabulary and had no reader left once it went.

Two of those were NOT settling and will be wanted back:

    quota_print.py  read each region's printed quota box out of the synopsis PDF and matched
                    every line to the rule accounting for it. 384 of 384 agreed. It is the only
                    thing that has ever checked these rules against their source, and it needed
                    a settled ledger to do the matching.
    comply.py       every rule handed in is findable in what comes out — the guarantee that
                    settling drops nothing silently.

`subject.expand` went too, and it had a twin: `catalogue.expand_species` answered the same
question one level deep where this answered it transitively, and they disagreed on an open group
("everything with fins") — one returned nothing, the other the claim itself. There is one
expander now, in `catalogue`, beside the group tables it reads.

See `pipeline/docs/06-ui-data-contract.md` for what the rebuilt layer has to produce, and
`05-table-generation.md` for how the removed one worked.
"""
