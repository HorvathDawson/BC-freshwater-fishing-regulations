"""THE TROUT TABLE'S DATE RULE (trout table plan, T810 ticket E; spec 3.3): `answers/trout/timeline.py` with the words
of `answers/trout/words.py`. No data: CI's no-data tier.

Every expected sentence is written out whole, from the rule (owner 15:36: a coming closure says what closes and no
reopening day; a reopening day only on something closed today; a change that only loosens reads "rule changes <day>").
The waters named are the plan's cases, built here as SHAPES (their days as stated in each test), not read from the
book. Each `case_*` returns what the module says; each rule test pins it; each MUTATION test turns one rule off and
shows the same case then says something else (so the rule test would fail).
"""
from __future__ import annotations

import pytest

from pipeline.common.calendar_spec import day_index
from pipeline.deliver.answers.trout import timeline as T
from pipeline.deliver.answers.trout import words as W

ALL = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
WEEK = {"weekdays": ALL[:5], "hours": None}
WEEKEND = {"weekdays": ALL[5:], "hours": None}
WHOLE = {"weekdays": ALL, "hours": None}
NIGHT = {"start": {"solar": "sunset", "offset_min": 60}, "end": {"solar": "sunrise", "offset_min": -60}}
OUT = {"weekdays": ALL, "hours": {**NIGHT, "in": False}}
IN = {"weekdays": ALL, "hours": {**NIGHT, "in": True}}

#: rule indexes a closed answer's `win` names, and the fish each closing rule holds
TROUT_CLOSURE, STEELHEAD_CLOSURE = 7, 8
RULE_FISH = {TROUT_CLOSURE: ["TROUT_CHAR"], STEELHEAD_CLOSURE: ["ST"]}


def D(m, d):
    return day_index(m, d)


def dec(status, daily=None, lines=(), win=0):
    return {"status": status, "win": win, "daily": daily,
            "lines": [{"t": t, "a": a, "b": b, "take": n, "r": 0} for t, a, b, n in lines]}


def keep(n, *lines):
    return dec("keep", n, lines)


REL = dec("release")
CLOSED = dec("closed", win=TROUT_CLOSURE)
ST_CLOSED = dec("closed", win=STEELHEAD_CLOSURE)


def fr(**fish):
    """A rows frame: {fish: answer for both origins, or (hatchery, wild)}."""
    return {"fish": {S: ({"hatchery": a[0], "wild": a[1]} if isinstance(a, tuple) else {"hatchery": a, "wild": a})
                     for S, a in fish.items()}}


def year(starts, frames, moments=None, **kw):
    return T.KeyYear(starts, frames, moments, scope_of=lambda w: RULE_FISH[w], **kw)


def said(y, j, row):
    c = y.next_change(j, row)
    return None if c is None else W.date_line(c)


# --------------------------------------------------------------------------------------------
# The cases
# --------------------------------------------------------------------------------------------

def fraser_r3():
    """Fraser River, Region 3 shape: trout and char keep (steelhead release) all year but Oct 1-Oct 31, when trout
    and char close (a TROUT_CHAR closure); the segment on Jan 1 is the calendar's own cut (same answers as Nov 1)."""
    open_ = fr(RB=keep(5), DV=keep(1, ("rel", 0, 60, None)), ST=REL)
    shut = fr(RB=CLOSED, DV=CLOSED, ST=CLOSED)
    return year([1, D(10, 1), D(11, 1)], [open_, shut, open_])


def case_fraser_lines():
    y = fraser_r3()
    return {j: {r: said(y, j, r) for r in ("RB", "DV", "ST")} for j in range(3)}


def case_fraser_placed():
    y = fraser_r3()
    boxes = [T.BoxRows("keep", ("RB", "DV"), ((0, 1, "upto"),)), T.BoxRows("release", ("ST",))]
    on_jul = T.place_dates(boxes, {r: said(y, 0, r) for r in ("RB", "DV", "ST")})
    closed = [T.BoxRows("closed", ("RB", "DV", "ST"))]
    on_oct = T.place_dates(closed, {r: said(y, 1, r) for r in ("RB", "DV", "ST")}, form="closed")
    return on_jul, on_oct


def case_loosen_only():
    """Rainbow trout keep 2 of 30 cm or longer until Mar 31, then 5 of any size."""
    y = year([1, D(4, 1)], [fr(RB=keep(2, ("rel", 0, 30, None))), fr(RB=keep(5))])
    c = y.next_change(0, "RB")
    return (None, None) if c is None else (W.date_line(c), W.sheet_sentence(c))


def case_tighter():
    """Three tighter changes on Nov 1: a lower count, a size floor, and wild fish going back."""
    a = year([1, D(11, 1)], [fr(RB=keep(5)), fr(RB=keep(1))])
    b = year([1, D(11, 1)], [fr(RB=keep(2)), fr(RB=keep(2, ("rel", 0, 50, None)))])
    c = year([1, D(11, 1)], [fr(RB=keep(2)), fr(RB=(keep(2), REL))])
    return [(W.date_line(y.next_change(0, "RB")), W.sheet_sentence(y.next_change(0, "RB"))) for y in (a, b, c)]


def case_mixed_counts_as_tighter():
    """Count up (2 -> 4) and size floor up (any -> 40 cm): tighter in one part, looser in another = tighter, and only
    what tightens is said."""
    y = year([1, D(6, 15)], [fr(RB=keep(2)), fr(RB=keep(4, ("rel", 0, 40, None)))])
    return said(y, 0, "RB")


def bish_creek():
    """Bish Creek shape: rainbow trout keep all year; steelhead release, closed May 1-Jun 30 by a steelhead closure."""
    rb = keep(2)
    return year([1, D(5, 1), D(7, 1)], [fr(RB=rb, ST=REL), fr(RB=rb, ST=ST_CLOSED), fr(RB=rb, ST=REL)])


def case_bish_creek():
    y = bish_creek()
    may20 = 1                       # May 20 falls in the May 1 segment
    boxes = [T.BoxRows("keep", ("RB",), ((0, 0, "upto"),)), T.BoxRows("closed", ("ST",))]
    placed = T.place_dates(boxes, {r: said(y, may20, r) for r in ("RB", "ST")})
    return said(y, 0, "ST"), said(y, may20, "ST"), said(y, may20, "RB"), placed


def elk_river():
    """Elk River shape: trout and char release Jan 1-Mar 31, closed Apr 1-Jun 14, keep Jun 15-Oct 31, release
    Nov 1-Dec 31."""
    rel, shut, kp = fr(RB=REL, CT=REL), fr(RB=CLOSED, CT=CLOSED), fr(RB=keep(2), CT=keep(2))
    return year([1, D(4, 1), D(6, 15), D(11, 1)], [rel, shut, kp, rel])


def case_elk_river():
    y = elk_river()
    lines = [said(y, j, "RB") for j in range(4)]
    bar = T.year_bar([y.answer(0, r).state for r in ("RB", "CT")], water_open=True)
    no_bar = T.year_bar([y.answer(2, r).state for r in ("RB", "CT")], water_open=True)
    return lines, bar, no_bar


def adams_river():
    """Adams River shape: Dolly Varden/bull trout keep 1 of 80 cm or longer, but 60 cm or longer on Jul 31 alone."""
    d80, d60 = keep(1, ("rel", 0, 80, None)), keep(1, ("rel", 0, 60, None))
    return year([1, D(7, 31), D(8, 1)], [fr(DV=d80), fr(DV=d60), fr(DV=d80)])


def case_adams():
    y = adams_river()
    before = y.next_change(0, "DV")
    return said(y, 0, "DV"), before is not None and before.short, said(y, 1, "DV"), \
        [(j, r) for j, r, _c in T.short_changes(y)]


def case_feb29():
    """Keep until Feb (the change on Mar 1); the same with the year cut on day 60 (a rule ending Feb 28); and with a
    stored day-60-only segment that no day reads."""
    a = year([1, D(3, 1)], [fr(RB=keep(2)), fr(RB=REL)])
    b = year([1, 60], [fr(RB=keep(2)), fr(RB=REL)])
    c = year([1, 60, 61], [fr(RB=keep(2)), fr(RB=CLOSED), fr(RB=REL)])
    return said(a, 0, "RB"), said(b, 0, "RB"), W.sheet_sentence(b.next_change(0, "RB")), said(c, 0, "RB"), \
        said(a, 1, "RB")


def case_over_dec31():
    """A run over Dec 31: keep Dec 1 to Jan 31 (cut on Jan 1 by the calendar), release from Feb 1; and a change on
    Jan 1 itself."""
    a = year([1, D(2, 1), D(12, 1)], [fr(RB=keep(2)), fr(RB=REL), fr(RB=keep(2))])
    b = year([1, D(12, 1)], [fr(RB=REL), fr(RB=keep(2))])
    return said(a, 2, "RB"), said(b, 1, "RB")


def case_whole_water():
    """The display frame closes the whole water from Nov 1."""
    y = year([1, D(11, 1)], [fr(RB=keep(2), DV=keep(1)), fr(RB=CLOSED, DV=CLOSED)], water_closed=[False, True])
    line = said(y, 0, "RB")
    return line, T.place_dates([T.BoxRows("keep", ("RB", "DV"), ((0, 1, "upto"),))], {"RB": line, "DV": line})


# --------------------------------------------------------------------------------------------
# The rule tests
# --------------------------------------------------------------------------------------------

FRASER = {0: {"RB": "until Sep 30, then closed to trout and char",
              "DV": "until Sep 30, then closed to trout and char",
              "ST": "until Sep 30, then closed to trout and char"},
          1: {"RB": "reopens Nov 1", "DV": "reopens Nov 1", "ST": "reopens Nov 1"},
          2: {"RB": "until Sep 30, then closed to trout and char",
              "DV": "until Sep 30, then closed to trout and char",
              "ST": "until Sep 30, then closed to trout and char"}}


def test_fraser_r3_closing_sep_30_says_what_closes_and_no_reopening():
    assert case_fraser_lines() == FRASER


def test_fraser_r3_card_line_and_closed_top_line():
    on_jul, on_oct = case_fraser_placed()
    assert on_jul == {"top": None, "card": "Until Sep 30, then closed to trout and char", "box": {}, "band": {},
                      "row": {}}
    assert on_oct == {"top": "reopens Nov 1", "card": None, "box": {}, "band": {}, "row": {}}


LOOSEN = ("rule changes Apr 1", "The rule changes on Apr 1.")


def test_a_loosen_only_change_reads_rule_changes_with_no_details():
    assert case_loosen_only() == LOOSEN


TIGHTER = [("until Oct 31, then only 1", "From Nov 1: only 1."),
           ("until Oct 31, then 50 cm (20 in) or longer", "From Nov 1: 50 cm (20 in) or longer."),
           ("until Oct 31, then hatchery only", "From Nov 1 every wild one goes back.")]


def test_a_tighter_change_says_what_tightens():
    assert case_tighter() == TIGHTER


def test_a_change_tighter_in_one_part_and_looser_in_another_is_tighter():
    assert case_mixed_counts_as_tighter() == "until Jun 14, then 40 cm (16 in) or longer"


BISH = ("until Apr 30, then closed for steelhead", "reopens Jul 1", None,
        {"top": None, "card": None, "box": {"1": "reopens Jul 1"}, "band": {}, "row": {}})


def test_bish_creek_a_fish_closed_today_says_its_reopening_on_its_row():
    assert case_bish_creek() == BISH


ELK = (["until Mar 31, then closed to trout and char", "reopens Jun 15", "until Oct 31, then release",
        "until Mar 31, then closed to trout and char"], True, False)


def test_elk_river_release_then_closed():
    assert case_elk_river() == ELK


ADAMS = ("rule changes Jul 31", True, "until Jul 31, then 80 cm (31 in) or longer", [(0, "DV"), (2, "DV")])


def test_adams_river_80_cm_rule_changes_jul_31():
    """Spec 3.3 rule 6: the row's date line follows rules 2 and 3; the one-day 60 cm is a short change (warned)."""
    assert case_adams() == ADAMS


FEB29 = ("until Feb 28, then release", "until Feb 28, then release", "From Mar 1 every one goes back.",
         "until Feb 28, then release", "until Dec 31")


def test_feb_29_is_never_a_date_and_a_change_on_mar_1_ends_feb_28():
    assert case_feb29() == FEB29


DEC31 = ("until Jan 31, then release", "until Dec 31, then release")


def test_a_run_over_dec_31_wraps_into_next_year():
    assert case_over_dec31() == DEC31


def test_a_whole_water_closure_is_closed_to_all_fishing_on_the_card():
    assert case_whole_water() == ("until Oct 31, then closed to all fishing",
                                  {"top": None, "card": "Until Oct 31, then closed to all fishing", "box": {},
                                   "band": {}, "row": {}})


def test_a_coming_closure_never_carries_a_reopening_day():
    for lines in case_fraser_lines().values():
        for line in lines.values():
            assert line == "reopens Nov 1" or "reopens" not in line


def test_a_reopening_day_only_on_something_closed_today():
    y = bish_creek()
    for j in range(3):
        for r in ("RB", "ST"):
            c = y.next_change(j, r)
            if c is not None and c.kind == "reopens":
                assert y.answer(j, r).state == "closed"
    with pytest.raises(T.TimelineError):
        T.place_dates([T.BoxRows("keep", ("RB",), ((0, 0, "upto"),)), T.BoxRows("keep", ("ST",), ((0, 0, "upto"),))],
                      {"RB": None, "ST": "reopens Jul 1"})


def test_no_change_all_year_is_no_date():
    y = year([1, D(5, 1)], [fr(RB=keep(2), EB=keep(5)), fr(RB=keep(2), EB=REL)])
    assert (said(y, 0, "RB"), said(y, 1, "RB")) == (None, None)
    assert said(y, 0, "EB") == "until Apr 30, then release"


def test_a_fish_no_rule_speaks_for_has_no_row_and_no_date():
    y = year([1, D(5, 1)], [fr(RB=keep(2)), fr(RB=keep(2), EB=keep(5))])
    assert y.rows(0) == ["RB"] and y.next_change(0, "EB") is None


def test_twin_rows_are_dated_by_their_own_origin():
    """Kitimat shape: wild 1 and hatchery 2 apart; on Nov 1 the wild ones go back, the hatchery stay."""
    y = year([1, D(11, 1)], [fr(ST=(keep(2), keep(1))), fr(ST=(keep(2), REL))])
    assert y.rows(0, twins=("ST",)) == ["ST/hatchery", "ST/wild"]
    assert (said(y, 0, "ST/hatchery"), said(y, 0, "ST/wild")) == (None, "until Oct 31, then release")


def test_a_fish_no_rule_speaks_for_later_stops_the_build():
    y = year([1, D(5, 1)], [fr(RB=keep(2)), fr(EB=keep(5))])
    with pytest.raises(T.TimelineError, match="no rule speaks"):
        y.next_change(0, "RB")


def test_weekday_moments_date_only_the_segment_weekdays():
    """Weekend keep 2 / weekday release from Jun 1; from Sep 1 every day release: only the weekend table changes."""
    y = year([1, D(6, 1), D(6, 1), D(9, 1)], [fr(RB=REL), fr(RB=REL), fr(RB=keep(2)), fr(RB=REL)],
             [WHOLE, WEEK, WEEKEND, WHOLE])
    assert [said(y, j, "RB") for j in range(4)] == ["rule changes Jun 1", None, "until Aug 31, then release",
                                                    "rule changes Jun 1"]


def test_a_tighter_change_that_differs_by_weekday_stops_the_build():
    y = year([1, D(6, 1), D(6, 1)], [fr(RB=keep(5)), fr(RB=keep(1)), fr(RB=REL)], [WHOLE, WEEK, WEEKEND])
    with pytest.raises(T.TimelineError, match="weekday"):
        y.next_change(0, "RB")


def test_the_table_is_dated_from_outside_an_hours_window():
    """Inside a night window the rainbow trout close from Jun 1; outside, nothing changes all year."""
    y = year([1, D(6, 1), D(6, 1)], [fr(RB=keep(2)), fr(RB=keep(2)), fr(RB=CLOSED)], [WHOLE, OUT, IN])
    assert [said(y, j, "RB") for j in range(3)] == [None, None, "reopens Jan 1"]


def test_placement_strip_band_and_row():
    """Rule 7: the box's strip when every row of the box shares the line; a shared white box when all its rows share
    it; else each row."""
    boxes = [T.BoxRows("keep", ("RB", "CT", "EB"), ((0, 1, "shared"), (2, 2, "upto"))),
             T.BoxRows("keep", ("DV", "LT"), ((0, 1, "upto"),))]
    a = "until Oct 31, then release"
    got = T.place_dates(boxes, {"RB": a, "CT": a, "EB": "rule changes Apr 1", "DV": a, "LT": a})
    assert got == {"top": None, "card": None, "box": {"1": a}, "band": {"0:0": a},
                   "row": {"0:2": "rule changes Apr 1"}}


def test_a_column_date_over_30_characters_goes_under_the_name():
    long_ = "until Sep 30, then closed to trout and char"
    short = "until Mar 31"
    boxes = [T.BoxRows("keep", ("RB",), ((0, 0, "upto"),)), T.BoxRows("release", ("ST", "DV"))]
    assert T.place_dates(boxes, {"RB": None, "ST": long_, "DV": long_})["row"] == {"1:0": long_, "1:1": long_}
    assert T.place_dates(boxes, {"RB": None, "ST": short, "DV": short})["box"] == {"1": short}


def test_the_year_bar_only_when_every_fish_shares_release_or_closed_on_an_open_water():
    assert [T.year_bar(s, o) for s, o in ((["release", "release"], True), (["closed"], True),
                                          (["release", "closed"], True), (["keep", "keep"], True),
                                          (["release"], False))] == [True, True, False, False, False]


# --------------------------------------------------------------------------------------------
# Mutations: each rule off, its case says something else
# --------------------------------------------------------------------------------------------

def test_mutation_looser_skipped_loses_rule_changes(monkeypatch):
    monkeypatch.setattr(T, "A_LOOSER_SAYS_NO_DETAILS", False)
    assert case_loosen_only() == (None, None)   # nothing dated: the looser change is skipped (the old rule)
    assert case_adams()[0] is None


def test_mutation_tighter_called_looser(monkeypatch):
    monkeypatch.setattr(T, "A_TIGHTER_IS_WORDED", False)
    assert case_tighter() != TIGHTER
    assert case_tighter()[0][0] == "rule changes Nov 1"


def test_mutation_closure_marked_whole_water(monkeypatch):
    monkeypatch.setattr(T, "A_CLOSURE_SCOPE_FROM_THE_RULE", False)
    assert case_fraser_lines() != FRASER
    assert case_fraser_lines()[0]["RB"] == "until Sep 30, then closed to all fishing"


def test_mutation_reopening_day_on_a_coming_closure(monkeypatch):
    monkeypatch.setattr(W, "A_NO_REOPENING_ON_A_COMING_CLOSURE", False)
    assert case_fraser_lines()[0]["RB"] == "until Sep 30, then closed to trout and char, reopens Nov 1"
    with pytest.raises(W.WordsError):            # and the grammar refuses it (J)
        W.parse_date_line(case_fraser_lines()[0]["RB"])


def test_mutation_date_shifted_by_a_day(monkeypatch):
    monkeypatch.setattr(T, "A_UNTIL_SAYS_THE_LAST_DAY", False)
    assert case_fraser_lines()[0]["RB"] == "until Oct 1, then closed to trout and char"
    assert case_over_dec31() != DEC31


def test_mutation_feb_29_read_as_a_day(monkeypatch):
    monkeypatch.setattr(T, "A_FEB29_READS_FEB28", False)
    got = None
    try:
        got = case_feb29()
    except W.WordsError:                         # Feb 29 named as a date: refused
        pass
    assert got != FEB29


def test_mutation_no_year_wrap(monkeypatch):
    monkeypatch.setattr(T, "A_YEAR_WRAPS", False)
    assert case_over_dec31() != DEC31
    assert case_elk_river() != ELK


def test_mutation_column_rule_off(monkeypatch):
    monkeypatch.setattr(W, "COLUMN_MAX", 1000)
    with pytest.raises(AssertionError):
        test_a_column_date_over_30_characters_goes_under_the_name()
