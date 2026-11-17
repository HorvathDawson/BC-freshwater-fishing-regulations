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


def test_a_bait_line_never_displaces_the_eulachon_line():
    """Lakelse: its own bait line all year, B(ii)'s eulachon row behind it — both, every day."""
    assert S.gear_on([[BAIT], [_eulachon()]], OCT_1) == ("No natural bait allowed", "No fishing for eulachon (Apr 1–Mar 31)")


def test_the_same_words_show_once_at_the_narrowest_place():
    assert S.gear_on([[BAIT], [_eulachon()], [BAIT]], OCT_1) == ("No natural bait allowed",
                                                                  "No fishing for eulachon (Apr 1–Mar 31)")


def test_in_one_place_the_eulachon_row_is_read_apart_from_the_gear_rows():
    """An "until further notice" eulachon row and a dated bait row of ONE place: the dated bait row no longer hides it
    (a place's open-ended rows speak only where none of ITS KIND is dated). MUTATION: read them together and the
    eulachon line goes."""
    place = [_gear("Bait ban", (9, 1), (12, 31)), _eulachon((11, 1), (11, 30), open_ended=True)]
    got = S.gear_on([place], OCT_1)
    assert got[0] == "Bait ban (Sep 1–Dec 31)" and len(got) == 2 and got[1].startswith("No fishing for eulachon"), got


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
