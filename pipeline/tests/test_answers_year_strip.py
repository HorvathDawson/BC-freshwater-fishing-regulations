"""THE YEAR STRIP (rows 30) — every rows row carries its year: run-length [[status, days]] over the 365 book
days, status keep | release | closed | none (`pipeline/deliver/answers/year.py`). No data: CI's no-data tier.

The colour of a day is the PROTOTYPE's established rule (page v36 dayStatus, ROUND14 5), now decided here and
not in the app (owner rule 2026-10-21): the most permissive of the row's members' stored answers (each fish's
leading origin answer, page mainRes: the keepable one, hatchery first) over the day's moments outside any hours
window; "none" where no member has an answer. Feb 29 is not a book day (it reads Feb 28's, P06).

Every expected value is written out whole, from the rule above — never read back from the implementation.
"""
from __future__ import annotations

import copy

import pytest

from pipeline.deliver.answers import model as M
from pipeline.deliver.answers import year as Y
from pipeline.deliver.answers.answers import AnswersError

ALL = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
WEEK = {"weekdays": ALL[:5], "hours": None}
WEEKEND = {"weekdays": ALL[5:], "hours": None}
WHOLE = {"weekdays": ALL, "hours": None}
NIGHT = {"start": {"solar": "sunset", "offset_min": 60}, "end": {"solar": "sunrise", "offset_min": -60}}
OUT = {"weekdays": ALL, "hours": {**NIGHT, "in": False}}
IN = {"weekdays": ALL, "hours": {**NIGHT, "in": True}}


def dec(status):
    return {"status": status}


def frame(fish, rows):
    """A rows frame: `fish` {code: (hatchery status | None, wild status | None)}, `rows` [members]."""
    return {"spp": sorted(fish),
            "fish": {S: {"hatchery": None if h is None else dec(h), "wild": None if w is None else dec(w)}
                     for S, (h, w) in fish.items()},
            "rows": [{"kind": "x", "members": list(m)} for m in rows],
            "steelhead_line": None}


def years(frames):
    return [[r["year"] for r in f["rows"]] for f in frames]


# 1. a weekday split day ----------------------------------------------------------------------
def test_1_a_weekday_split_day_is_its_most_permissive_moment():
    # Kootenay Lake's Lower West Arm shape: kokanee release Mon-Fri, keep Sat-Sun from Apr 10 (day 101) to the end
    # of the year; before it, release every day. Day 101 = Apr 10 on the leap calendar = book day 100.
    starts = [1, 101, 101]
    at = [WHOLE, WEEK, WEEKEND]
    fs = [frame({"KO": ("release", "release")}, [["KO"]]),
          frame({"KO": ("release", "release")}, [["KO"]]),
          frame({"KO": ("keep", "keep")}, [["KO"]])]
    got = Y.stamp_years(fs, starts, at)
    want = [["release", 99], ["keep", 266]]
    assert years(got) == [[want], [want], [want]]


def test_1b_weekday_split_with_no_keep_takes_release_over_closed():
    starts = [1, 1]
    at = [WEEK, WEEKEND]
    fs = [frame({"RB": ("closed", "closed")}, [["RB"]]), frame({"RB": ("release", "release")}, [["RB"]])]
    assert years(Y.stamp_years(fs, starts, at)) == [[[["release", 365]]], [[["release", 365]]]]


def test_1c_an_hours_window_never_colours_the_day():
    # a night closure: outside the window keep, inside it closed — the day is the outside's answer
    starts = [1, 1, 182, 182]
    at = [OUT, IN, OUT, IN]           # a start repeats once per moment group
    fs = [frame({"RB": ("keep", "keep")}, [["RB"]]), frame({"RB": ("closed", "closed")}, [["RB"]]),
          frame({"RB": ("release", "release")}, [["RB"]]), frame({"RB": ("closed", "closed")}, [["RB"]])]
    want = [["keep", 180], ["release", 185]]       # Jan 1-Jun 29 (book days 1-180), Jun 30 on (182 = Jun 30)
    assert years(Y.stamp_years(fs, starts, at)) == [[want]] * 4


def test_1d_inside_the_window_never_colours_the_day_even_where_it_is_more_permissive():
    # the day reads outside the window (page v36: the moment with `in: false`), never the best of the two
    fs = [frame({"RB": ("release", "release")}, [["RB"]]), frame({"RB": ("keep", "keep")}, [["RB"]])]
    assert years(Y.stamp_years(fs, [1, 1], [OUT, IN])) == [[[["release", 365]]], [[["release", 365]]]]


# 2. a closure ----------------------------------------------------------------------------------
def test_2_a_closure_runs_its_days_and_the_rows_members_answer():
    # the Vedder shape: keep, closed May 31-Jun 29 (days 152-181), keep again; the release-only fish its own row
    starts = [1, 152, 182]
    open_ = frame({"RB": ("keep", "release"), "CT": ("release", "release")}, [["RB"], ["CT"], []])
    shut = frame({"RB": ("closed", "closed"), "CT": ("closed", "closed")}, [["RB"], ["CT"], []])
    got = Y.stamp_years([open_, shut, open_], starts, None)
    rb = [["keep", 150], ["closed", 30], ["keep", 185]]
    ct = [["release", 150], ["closed", 30], ["release", 185]]
    none = [["none", 365]]
    assert years(got) == [[rb, ct, none]] * 3


def test_2b_the_leading_origin_answer_is_the_keepable_one_hatchery_first():
    starts = [1, 61, 122, 183, 245]
    fs = [frame({"RB": ("release", "keep")}, [["RB"]]),        # wild keeps -> keep
          frame({"RB": ("keep", "closed")}, [["RB"]]),         # hatchery keeps -> keep
          frame({"RB": ("release", "closed")}, [["RB"]]),      # neither: hatchery's -> release
          frame({"RB": (None, "closed")}, [["RB"]]),           # no hatchery answer: wild's -> closed
          frame({"RB": ("closed", "release")}, [["RB"]])]      # neither: hatchery's -> closed (not wild's release)
    got = Y.stamp_years(fs, starts, None)
    want = [["keep", 120], ["release", 61], ["closed", 184]]   # 59 + 61 / 61 / 62 + 122 (Feb 29 is no book day)
    assert years(got)[0] == [want]


def test_2c_no_limit_is_keep_and_the_most_permissive_member_colours_the_row():
    starts = [1, 32]
    fs = [frame({"LW": ("no_limit", "no_limit"), "MW": ("closed", "closed")}, [["LW", "MW"]]),
          frame({"LW": ("closed", "closed"), "MW": ("release", "release")}, [["LW", "MW"]])]
    assert years(Y.stamp_years(fs, starts, None))[0] == [[["keep", 31], ["release", 334]]]


# 3. Feb 29 ------------------------------------------------------------------------------------
def test_3_feb_29_is_no_book_day():
    # a segment starting on Feb 29 (day 60) alone, and one on Mar 1 (61): the strip has 365 days, Feb 28 is day
    # 59 of the book, Mar 1 its 60th; Feb 29's own answer appears nowhere
    starts = [1, 60, 61]
    fs = [frame({"RB": ("keep", "keep")}, [["RB"]]),
          frame({"RB": ("closed", "closed")}, [["RB"]]),
          frame({"RB": ("release", "release")}, [["RB"]])]
    assert years(Y.stamp_years(fs, starts, None))[0] == [[["keep", 59], ["release", 306]]]


def test_3b_the_book_days_are_365():
    assert len(Y.BOOK_DAYS) == 365 and 60 not in Y.BOOK_DAYS and Y.BOOK_DAYS[:2] == (1, 2) \
        and Y.BOOK_DAYS[58:60] == (59, 61) and Y.BOOK_DAYS[-1] == 366


# 4. a row with no answers ------------------------------------------------------------------------
def test_4_a_row_with_no_members_or_no_answers_is_none_all_year():
    fs = [frame({"RB": ("keep", "keep"), "WSG": (None, None)}, [[], ["WSG"], ["RB", "WSG"]])]
    assert years(Y.stamp_years(fs, [1], None)) == [[[["none", 365]], [["none", 365]], [["keep", 365]]]]


def test_4b_a_fish_the_frame_does_not_ask_about_is_no_answer():
    fs = [frame({"RB": ("keep", "keep")}, [["KO"]])]
    assert years(Y.stamp_years(fs, [1], None)) == [[[["none", 365]]]]


# the pass ------------------------------------------------------------------------------------------
def test_the_inputs_are_not_changed_and_the_year_is_the_rows_last_field():
    fs = [frame({"RB": ("keep", "keep")}, [["RB"]])]
    before = copy.deepcopy(fs)
    got = Y.stamp_years(fs, [1], None)
    assert fs == before
    assert list(got[0]["rows"][0]) == ["kind", "members", "year"]
    assert got[0]["rows"][0]["year"] == [["keep", 365]]


def test_a_frame_per_segment_or_it_refuses():
    with pytest.raises(AnswersError) as e:
        Y.stamp_years([frame({}, [])], [1, 100], None)
    assert str(e.value) == "answers: the year strip has 1 rows frames for 2 segments"
    with pytest.raises(AnswersError) as e:
        Y.stamp_years([frame({}, []), frame({}, [])], [1, 1], None)
    assert str(e.value) == "answers: the year strip needs the moments of a key that repeats a start day"


def test_a_row_already_stamped_is_refused():
    fs = Y.stamp_years([frame({"RB": ("keep", "keep")}, [["RB"]])], [1], None)
    with pytest.raises(AnswersError) as e:
        Y.stamp_years(fs, [1], None)
    assert str(e.value) == "answers: a rows row already carries its year strip"


# the model ----------------------------------------------------------------------------------------
ROW = {"kind": "release", "pool": None, "win": 3, "members": ["RB"], "all_members": ["RB"],
       "daily": None, "narrow": None, "everyone": [], "groups": [], "prot": None, "wins": None,
       "lift_notes": [], "scope": None}
FRAME = {"spp": ["RB"], "fish": {"RB": {"hatchery": None, "wild": None}}, "steelhead_line": None}


def _rows(year):
    return {**FRAME, "rows": [{**ROW, "year": year}]}


def test_the_model_holds_the_year_to_365_days_of_runs():
    M.validate_section("rows", [_rows([["release", 150], ["closed", 30], ["release", 185]])])
    for bad in ([["release", 364]], [["release", 366]], [["release", 300], ["release", 65]],
                [["nolimit", 365]], [["no_limit", 365]], [["keep", 0], ["keep", 365]], [], [["keep", "365"]]):
        with pytest.raises(M.ShapeError):
            M.validate_section("rows", [_rows(bad)])
    with pytest.raises(M.ShapeError):
        M.validate_section("rows", [{**FRAME, "rows": [ROW]}])                 # required: every row has its year
