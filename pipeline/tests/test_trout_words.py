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


# ------------------------------------------------------------------------------- the table's NOW words (ticket F)

#: every NOW form the table builder writes, whole, with what the grammar reads back
NOW = [
    (W.top_up_to(5), "Keep 5 trout and char {day}", W.parse_top, ("up_to", (5,))),
    (W.top_up_to(2, ["5 if they’re rainbow trout"], [(2, "Dolly Varden/bull trout")]),
     "Keep 2 trout and char {day}, or 5 if they’re rainbow trout and no more than 2 Dolly Varden/bull trout", W.parse_top,
     ("up_to", (2, 5, 2))),
    (W.top_up_to(2, plus=[(2, "Dolly Varden/bull trout")]), "Keep 2 trout and char {day}, plus 2 Dolly Varden/bull trout", W.parse_top,
     ("up_to", (2, 2))),
    (W.top_apart([(6, W.fish_words(["RB", "ST"]))], 4),
     "Keep 6 rainbow trout or steelhead and 4 other trout and char {day}", W.parse_top, ("apart", (6, 4))),
    (W.TOP_RELEASE, "Release every trout and char {day}", W.parse_top, ("release", ())),
    (W.TOP_CLOSED, "Closed for trout and char {day}", W.parse_top, ("closed", ())),
    (W.TOP_NONE, "Keep no trout and char {day}", W.parse_top, ("none", ())),
    (W.head_keep(5), "Keep 5 in all", W.parse_head, ("keep", 5)),
    (W.HEAD_RELEASE, "Release", W.parse_head, ("release", None)),
    (W.band_upto(5), "counts toward the 5", W.parse_band, ("upto", 5, None)),          # T810 G2, owner 15:46
    (W.band_own(3), "only 3", W.parse_band, ("own", 3, None)),
    (W.band_shared(1, 4), "1 total from these 4", W.parse_band, ("shared", 1, 4)),    # owner 2026-11-18 00:52
    (W.band_shared(3, 2), "only 3 between them", W.parse_band, ("shared", 3, 2)),   # T810 G2 Q2
    (W.band_shared(4, 3), "4 total from these 3", W.parse_band, ("shared", 4, 3)),
    (W.band_in(2, W.band_shared(1, 2)), "counts toward the 2 · only 1 between them", W.parse_band, ("shared", 1, 2)),
    (W.band_in(6, W.band_shared(2, 6)), "counts toward the 6 · 2 total from these 6", W.parse_band, ("shared", 2, 6)),
    (W.band_wild(2), "only 2 wild in all", W.parse_band, ("shared", 2, None)),  # T810 G2 Q9
    (W.strip_size_except(1, 50, W.fish_words(["LT"])), "only 1 over 50 cm (20 in), not counting lake trout",
     W.parse_strip, (("size_except", 1, 50, ("LT",)),)),                         # T810 G2 Q3b
    (W.STRIP_ANY_SIZE, "any size", W.parse_strip, (("any",),)),                  # T810 G2 Q6
    (W.box_possession("in your possession: no more than 2 trout and char in all (fish at home don’t count)"),
     "In your possession: no more than 2 trout and char in all", lambda t: W.parse_possession("i" + t[1:]),
     ("count", 2, W.parse_fish("trout and char"), ())),                          # T810 G2 Q4
    (W.possession_quiet(3, 2, "lake trout", "only 1 over 50 cm (20 in)"),
     "3 a day, but never more than 2 lake trout with you, and only 1 of them over 50 cm (20 in)", W.parse_quiet,
     ("possession", 3, 2, ("LT",), ((1, ((50, W.INF),)),))),                     # T810 G2 Q10
    (W.size_release(60, 90), "release over 60 cm (24 in) and under 90 cm (35 in)", W.parse_size,
     ((0, 60), (90, W.INF))),                                                    # T810 G2, Tagish lake trout
    (W.CLOSED_ALL_YEAR, "closed all year", W.parse_row_line, ("closed_year",)),  # T810 G2 Q8
    (W.stop_duty("stop fishing this river after 1 a day", "creek"), "after keeping 1, stop fishing this creek today",
     W.parse_stop_duty, (1, "creek")),                                           # T810 G2 Q7
    (W.band_possession(2), "only 2 in your possession", W.parse_band, ("possession", 2, None)),
    (W.strip_size(1, 50), "only 1 over 50 cm (20 in)", W.parse_strip_size, (1, 50, None)),
    (W.strip_size(1, 50, 1), "only 1 over 50 cm (20 in), 2 if one is a hatchery steelhead", W.parse_strip_size,
     (1, 50, 2)),
    (W.stop_duty("stop fishing this river after 1 a day"), "after keeping 1, stop fishing this river today",
     W.parse_stop_duty, (1, "river")),
]


@pytest.mark.parametrize("got,whole,parse,back", NOW, ids=[x[1] for x in NOW])
def test_every_now_form_whole_and_read_back(got, whole, parse, back):
    assert got == whole
    assert parse(whole) == back


def test_the_other_now_words_whole():
    assert W.fold_up_to(2) == "2 a day"                    # owner 23:35: the Other fish rows' form
    assert W.parse_fold("2 a day") == ("up_to", (2,))
    assert W.fold_up_to(2, [5]) == "Up to 2 or 5"           # two numbers: no Other fish form yet (asked)
    assert W.fold_apart([(6, W.short_fish("RB"))], 4) == "Up to 6 rainbow, 4 others"
    assert W.tag_of("This lake’s rule") == "Lake’s rule"
    assert W.strip_wild(2, "40 cm (16 in) or less") == "only 2 wild, each 40 cm (16 in) or less"
    assert W.uses_the(1, 50) == "uses the 1 over 50 cm (20 in)"
    assert W.count_line(1, ["LT"]) == "only 1, counting any lake trout you keep"
    assert W.size_count_line(1, 50, ["EB", "LT"]) == "only 1 over 50 cm (20 in), counting any brook or lake trout you keep"
    assert W.steelhead_sub("rainbow trout over 50 cm (20 in)") == "a rainbow over 50 cm (20 in)"
    from pipeline.deliver.answers.display.rule import POSSESSION_HOME
    assert W.possession_row(f"in your possession: 1 day’s limit, not 2 ({POSSESSION_HOME})") == \
        "in your possession: 1 day’s limit, not 2"
    assert W.possession_quiet(3, 2, W.fish_words(["LT"])) == "3 a day, but never more than 2 lake trout with you"
    assert W.together_origin(2, "wild", "under 40 cm (16 in)") == \
        "only 2 wild trout and char in all, each under 40 cm (16 in)"
    assert W.together_lakes(1) == "only 1 from both lakes together"
    assert W.sheet_keep("1 total from these 4", 5) == "Keep · 1 total from these 4 · counts toward the 5"
    assert W.sheet_keep("counts toward the 5 · 1 total from these 4", 5) == "Keep · 1 total from these 4 · counts toward the 5"
    assert W.one_day(D(7, 31), ["60 cm (24 in) or longer"]) == "On Jul 31 only: 60 cm (24 in) or longer."
    with pytest.raises(W.WordsError):
        W.parse_band("up to 5")
    with pytest.raises(W.WordsError):
        W.possession_row("in your possession: 2")
