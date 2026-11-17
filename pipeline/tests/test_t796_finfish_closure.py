"""T796 A6 (owner 2026-11-16, question 30: "Closes the whole page (Recommended)") — DFO's "Finfish closure" closes ALL
fishing on its stretch on its dates, not only salmon.

Ten DFO rows say it (Stamp 4, Nitinat 3, Somass, Sproat, Tahltan). `typed.py` read each as a salmon closure only, so the
Stamp hatchery-lagoon stretch on Oct 10 showed the trout card open. The typed closure now carries `finfish: true` and
the verdict it gives `f: 1` — the flag the app reads from the same verdict the salmon card reads, to close the page.
"""
from __future__ import annotations

from pipeline.deliver import salmon as S
from pipeline.regs.dfo_salmon import feed as FD
from pipeline.regs.dfo_salmon import live as L
from pipeline.regs.dfo_salmon import typed as T

OCT_10, NOV_10 = 1010, 1110


def _row(limits, dates="Aug 25 to Oct 31", species="All"):
    return {"species": species, "dates": dates, "limits_gear": limits}


def test_a_finfish_closure_is_typed_as_a_closure_of_all_fishing():
    [r] = T.typed_rules(_row("Finfish closure"), "x", 0)
    assert (r["type"], r["take"], r["may_target"], r["species"], r["finfish"]) == \
        ("retention_limit", 0, False, ["SA"], True)
    [plain] = T.typed_rules(_row("No fishing for salmon"), "x", 1)
    assert "finfish" not in plain, "a salmon closure stays a salmon closure"
    assert T.as_catalogue(r) == {k: v for k, v in r.items() if k != "finfish"}


def test_the_verdict_says_all_fishing_is_closed_on_the_closures_dates():
    """MUTATION: drop the `f` in `verdict_on` and the page stays open for trout."""
    rules = T.typed_rules(_row("Finfish closure"), "x", 0)
    v = S.verdict_on([rules], "CO", OCT_10)
    assert (v["s"], v.get("f")) == ("closed", 1)
    assert S.verdict_on([rules], "CO", NOV_10) is None, "outside its dates it says nothing"
    salmon_only = T.typed_rules(_row("No fishing for salmon"), "x", 0)
    assert "f" not in S.verdict_on([salmon_only], "CO", OCT_10)


def test_the_feed_and_the_live_year_carry_it():
    """The typed feed (`feed.type_rows`) and a place's year in the live feed (`live.location_calendar`) give the same
    verdict, `f` included — what the worker publishes and the app composes."""
    rules, err = FD.type_rows("f" * 16, [_row("Finfish closure")])
    assert err is None and rules[0]["finfish"] is True
    v, g = S._Interner(), S._Interner()
    v(S.NOT_LISTED)
    cal = L.location_calendar(rules, v, g)
    days = cal["sp"][S.SPECIES.index("CO")]
    i = S.DAYS.index(OCT_10)
    ix = next(days[k + 1] for k in range(len(days) - 2, -1, -2) if days[k] <= i)
    assert v.items[ix].get("f") == 1
