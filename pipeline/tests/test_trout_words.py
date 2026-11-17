"""THE TROUT TABLE'S DATE WORDS (trout table plan, T810 ticket G): `answers/trout/words.py` — every date line and
sheet sentence whole (AGENTS 33), and the grammar the proof (J) reads them back with. No data: CI's no-data tier.
"""
from __future__ import annotations

import pytest

from pipeline.common.calendar_spec import day_index
from pipeline.deliver.answers.trout import words as W
from pipeline.deliver.answers.trout.timeline import Change

INF = float("inf")


def D(m, d):
    return day_index(m, d)


def ch(kind, day, last, then=(), lasts=None):
    return Change(kind, day, last, tuple(then), lasts)


#: (change, its date line, its sheet sentence): one per form and per "then" part, written out whole
CATALOGUE = [
    (ch("tighter", D(10, 1), D(9, 30), [("closed", "trout_char")]),
     "until Sep 30, then closed to trout and char", "From Oct 1 it is closed to trout and char."),
    (ch("tighter", D(11, 1), D(10, 31), [("closed", "water")]),
     "until Oct 31, then closed to all fishing", "From Nov 1 the water is closed to all fishing."),
    (ch("tighter", D(5, 1), D(4, 30), [("closed", ("ST",))]),
     "until Apr 30, then closed for steelhead", "From May 1 it is closed."),
    (ch("tighter", D(5, 1), D(4, 30), [("closed", ("DV", "LT"))]),
     "until Apr 30, then closed for Dolly Varden/bull trout and lake trout", "From May 1 it is closed."),
    (ch("tighter", D(11, 1), D(10, 31), [("release",)]),
     "until Oct 31, then release", "From Nov 1 every one goes back."),
    (ch("tighter", D(11, 1), D(10, 31), [("hatchery_only",)]),
     "until Oct 31, then hatchery only", "From Nov 1 every wild one goes back."),
    (ch("tighter", D(8, 1), D(7, 31), [("floor", 80)]),
     "until Jul 31, then 80 cm (31 in) or longer", "From Aug 1: 80 cm (31 in) or longer."),
    (ch("tighter", D(8, 1), D(7, 31), [("ceil", 50)]),
     "until Jul 31, then 50 cm (20 in) or less", "From Aug 1: 50 cm (20 in) or less."),
    (ch("tighter", D(6, 15), D(6, 14), [("count", 1)]),
     "until Jun 14, then only 1", "From Jun 15: only 1."),
    (ch("tighter", D(6, 15), D(6, 14), [("band", 1, 50, INF)]),
     "until Jun 14, then only 1 measuring 50 cm (20 in) or longer",
     "From Jun 15: only 1 measuring 50 cm (20 in) or longer."),
    (ch("tighter", D(6, 15), D(6, 14), [("band", 0, 60, 90)]),
     "until Jun 14, then none between 60 cm (24 in) and 90 cm (35 in)",
     "From Jun 15: none between 60 cm (24 in) and 90 cm (35 in)."),
    (ch("tighter", D(6, 15), D(6, 14), [("possession", 2)]),
     "until Jun 14, then only 2 in your possession", "From Jun 15: only 2 in your possession."),
    (ch("tighter", D(11, 1), D(10, 31), [("hatchery_only",), ("floor", 50), ("count", 1)]),
     "until Oct 31, then hatchery only, 50 cm (20 in) or longer, only 1",
     "From Nov 1: hatchery only, 50 cm (20 in) or longer, only 1."),
    (ch("opens", D(4, 1), D(3, 31)), "until Mar 31", "From Apr 1 it may be kept."),
    (ch("looser", D(4, 1), D(3, 31)), "rule changes Apr 1", "The rule changes on Apr 1."),
    (ch("reopens", D(11, 1), D(10, 31)), "reopens Nov 1", "It reopens on Nov 1."),
    (ch("tighter", D(3, 1), D(2, 28), [("release",)]), "until Feb 28, then release", "From Mar 1 every one goes back."),
    (ch("tighter", D(1, 1), D(12, 31), [("release",)]), "until Dec 31, then release", "From Jan 1 every one goes back."),
]


@pytest.mark.parametrize("change,line,sheet", CATALOGUE, ids=[c[1] for c in CATALOGUE])
def test_every_form_whole(change, line, sheet):
    assert (W.date_line(change), W.sheet_sentence(change)) == (line, sheet)


@pytest.mark.parametrize("change,line,sheet", CATALOGUE, ids=[c[1] for c in CATALOGUE])
def test_the_grammar_reads_every_line_back(change, line, sheet):
    got = W.parse_date_line(line)
    assert got.form == change.kind
    if change.kind in ("tighter", "opens"):
        assert (got.last, got.day) == (change.last, None)
    else:
        assert (got.day, got.last) == (change.day, None)
    assert got.then == change.then
    s = W.parse_sheet_sentence(sheet)
    assert (s.form, s.day) == (change.kind, change.day)
    if change.kind == "tighter" and change.then[0][0] != "closed":
        assert s.then == change.then


def test_the_card_line_stands_alone_with_a_capital_and_reads_back():
    assert W.card_line("until Oct 31, then closed to all fishing") == "Until Oct 31, then closed to all fishing"
    assert W.parse_date_line("Until Oct 31, then closed to all fishing").then == (("closed", "water"),)


def test_box_count_form_reads_back():
    assert W.then_words((("in_all", 1),)) == "1 in all"
    assert W.parse_then("1 in all") == (("in_all", 1),)


@pytest.mark.parametrize("text", [
    "until Sep 30, then closed to trout and char, reopens Nov 1",   # a coming closure with its reopening day
    "until Sep 31, then release",                                    # no such day
    "until Feb 29, then release",                                    # Feb 29 is never a date (P06)
    "until Sep 30, then release today",                              # "today" is the {day} slot's
    "until Monday Sep 30, then release",                             # a weekday
    "until Sep 30, then 50 cm (19 in) or longer",                    # inches not the cm's
    "until Sep 30, then keep 5",                                     # not a then form
    "until Sep 30, then release, only 1",                            # a release said with a limit
    "rule changes on Apr 1",
    "Closed until Oct 31",
    "",
])
def test_an_unreadable_line_is_refused(text):
    with pytest.raises(W.WordsError):
        W.parse_date_line(text)


def test_reopening_is_marked():
    assert W.is_reopening("reopens Nov 1") and not W.is_reopening("until Sep 30, then closed to trout and char")


def test_the_30_character_column_rule():
    assert W.COLUMN_MAX == 30
    assert W.column_fits("until Sep 30, then release")                       # 26
    assert not W.column_fits("until Sep 30, then closed to trout and char")  # 43


def test_no_date_line_names_feb_29():
    with pytest.raises(W.WordsError):
        W.day_words(60)
    assert (W.day_words(59), W.day_words(61)) == ("Feb 28", "Mar 1")


def test_mutation_a_weekday_in_a_date_is_refused(monkeypatch):
    """The guard is on every composer: a day phrase that names a weekday stops the line."""
    monkeypatch.setattr(W, "day_words", lambda day: "Monday, Sep 30")
    for change, _line, _sheet in CATALOGUE:
        with pytest.raises(W.WordsError, match="weekday"):
            W.date_line(change)
        with pytest.raises(W.WordsError, match="weekday"):
            W.sheet_sentence(change)


def test_mutation_the_round_trip_catches_a_composer_drift(monkeypatch):
    """A composer whose words the grammar cannot read fails the round trip (spec G)."""
    real = W.part_words
    monkeypatch.setattr(W, "part_words", lambda p: "release every one" if p == ("release",) else real(p))
    line = W.date_line(ch("tighter", D(11, 1), D(10, 31), [("release",)]))
    with pytest.raises(W.WordsError):
        W.parse_date_line(line)
