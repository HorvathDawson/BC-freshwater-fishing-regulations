"""T770 (owner 2026-11-16, question 13: "Show both (Recommended)") — a chain's gear lines are every place's.

A chain's gear lines on a day are EVERY place's lines that hold that day, narrowest place first; the same words once.
It was the narrowest place with any line, and a whole-river hook rule vanished under a stretch's "fly fishing only"
(Little Qualicum, Sep to Nov), and B(ii)'s "No fishing for eulachon" under a tributary's bait line (Lakelse all year,
Kitsumkalum to Jun 15). A row about another fish is read apart from its place's gear rows. The live feed composes the
same (`live.compose`: each place's year, concatenated per day).
"""
from __future__ import annotations

from pipeline.deliver import salmon as S
from pipeline.regs.dfo_salmon import live as L

OCT_1, JUN_1 = 1001, 601


def _when(frm, to, open_ended=False):
    return {"dates": [{"from_month": frm[0], "from_day": frm[1], "to_month": to[0], "to_day": to[1]}],
            **({"open_ended": True} if open_ended else {})}


def _gear(words, frm=None, to=None, kind="bait_restriction", **kw):
    """A gear row; no dates: all year (DFO's empty dates cell)."""
    return {"rule_id": words, "type": kind, "verbatim": words, "gear": [{"slot": "bait", "ban": ["any_bait"]}],
            "when": _when(frm, to, **kw) if frm else {}}


def _eulachon(frm=(4, 1), to=(3, 31), **kw):
    return {"rule_id": "eu", "type": "advisory", "verbatim": "No fishing for eulachon",
            "review_reason": "another fish, not salmon: eulachon", "when": _when(frm, to, **kw)}


FLY = _gear("Fly-fishing only", (9, 1), (11, 30), kind="tackle_restriction")
HOOK = _gear("single barbless hook less than 15mm from point to shank", kind="tackle_restriction")
BAIT = _gear("No natural bait allowed")


def test_a_wider_places_line_shows_beside_a_narrower_ones():
    """Little Qualicum, Oct 1: the stretch's fly-only and the river's hook rule, narrowest first. MUTATION: the old
    "narrowest place with any line" shows the fly-only alone."""
    assert S.gear_on([[FLY], [HOOK]], OCT_1) == ("Fly-fishing only (Sep 1–Nov 30)",
                                                  "single barbless hook less than 15mm from point to shank")
    assert S.gear_on([[FLY], [HOOK]], JUN_1) == ("single barbless hook less than 15mm from point to shank",)


EULACHON = "Eulachon closed all year: No fishing for eulachon"


def test_the_eulachon_row_is_its_own_closure_line_never_a_gear_line():
    """(Owner 2026-11-16 19:09, "As a closure line (Recommended)") Lakelse: its own bait line all year and B(ii)'s
    "No fishing for eulachon" behind it. The bait line is the gear line; the eulachon row is the chain's CLOSURE line
    (`o`), whole: "Eulachon closed all year: No fishing for eulachon". MUTATION: count the eulachon row as gear again
    and it is back under "No natural bait allowed"."""
    assert S.gear_on([[BAIT], [_eulachon()]], OCT_1) == ("No natural bait allowed",)
    assert S.closures_on([[BAIT], [_eulachon()]], OCT_1) == (EULACHON,)


def test_no_line_ever_displaces_the_eulachon_closure_line():
    """(T770's guarantee) a narrower place's bait line, a second bait line and a dated closure of the same fish beside
    it: the all-year closure line stays, every day, once."""
    chain = [[BAIT], [_eulachon()], [BAIT, _eulachon((4, 1), (6, 30))]]
    for md in (OCT_1, JUN_1, 101, 229):
        got = S.closures_on(chain, md)
        assert got[0] == EULACHON, (md, got)
    assert S.closures_on(chain, JUN_1) == (EULACHON, "Eulachon closed Apr 1–Jun 30: No fishing for eulachon")
    # one place printing both: neither hides the other
    assert S.closures_on([[_eulachon((4, 1), (6, 30)), BAIT, _eulachon()]], JUN_1) == \
        ("Eulachon closed Apr 1–Jun 30: No fishing for eulachon", EULACHON)


def test_in_one_place_the_eulachon_row_is_read_apart_from_the_gear_rows():
    """An "until further notice" eulachon row and a dated bait row of ONE place: the dated bait row never hides it.
    MUTATION: read them together and the eulachon line goes."""
    place = [_gear("Bait ban", (9, 1), (12, 31)), _eulachon((11, 1), (11, 30), open_ended=True)]
    assert S.gear_on([place], OCT_1) == ("Bait ban (Sep 1–Dec 31)",)
    got = S.closures_on([place], OCT_1)
    assert len(got) == 1 and got[0].startswith("Eulachon closed ") and got[0].endswith(": No fishing for eulachon"), got


def test_the_chain_carries_the_closure_line_in_o_apart_from_g():
    """The release's chain: `g` the gear lines, `o` the closure lines (an index into `gears`), the live feed's
    composition the same (`live.compose`). MUTATION: drop `o` and the closure line is gone from the file."""
    chain = [[BAIT], [_eulachon()]]
    v, g = S._Interner(), S._Interner()
    v(S.NOT_LISTED)
    cal = S.chain_calendar(chain, v, g)
    assert [g.items[i] for i in cal["g"][1::2]] == [["No natural bait allowed"]]
    assert [g.items[i] for i in cal["o"][1::2]] == [[EULACHON]]
    locs = {str(i): L.location_calendar(c, v, g) for i, c in enumerate(chain)}
    got = L.compose(["0", "1"], locs, v.items, gears=g.items, gear_intern=g)
    assert got["o"] == cal["o"] and got["g"] == cal["g"]
    assert "o" not in S.chain_calendar([[BAIT]], v, g)


def test_the_live_feed_composes_the_same_gear():
    """`live.location_calendar` per place + `live.compose` == `chain_calendar`, for a chain whose places all have
    lines (a merged list goes through the gear interner)."""
    chain = [[FLY], [BAIT], [HOOK], [_eulachon()]]
    v, g = S._Interner(), S._Interner()
    v(S.NOT_LISTED)
    want = S.chain_calendar(chain, v, g)
    locs = {str(i): L.location_calendar(c, v, g) for i, c in enumerate(chain)}
    got = L.compose([str(i) for i in range(len(chain))], locs, v.items, gears=g.items, gear_intern=g)
    assert [g.items[i] if i >= 0 else None for i in got["g"][1::2]] == \
        [g.items[i] if i >= 0 else None for i in want["g"][1::2]]
    assert got["g"][::2] == want["g"][::2]
    assert got.get("o") == want.get("o")
