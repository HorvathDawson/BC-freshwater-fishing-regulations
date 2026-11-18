"""THE FISH SHEET (trout table plan, T810 ticket H; spec 3.4, contract 2.3 `FishSheet`, Q-D6): `answers/trout/sheets.py`
writes each table row's sheet — its head, its "Here" lines (every one G's whole sentence, none saying "today"), its
Sources cut to 4 — and the table's `elsewhere` lines.

Anchors: (a) no data: G's sheet and elsewhere words, whole, and what they refuse; (b) every key of the committed mini
release (every sheet valid, no "today", Sources at most 4, ending as sentences); (c) the named fish on the live bundle
(`needs_bundle`), each sheet whole: Skeena River Sedan Cr to Mill Cr (one hatchery-only steelhead row, the stop duty,
the yearly limit and record duty), Adams River (Dolly Varden 80 cm and, on Jul 31, its one-day line), Bowron Lake (the
park's possession), Lois Lake (the wild limit across both boxes, the steelhead stamp); (d) a planted stamp drop and the
MUTATIONS.

The book (synopsis 2025-2027, printed pages): Skeena p.(Region 6 table: hatchery steelhead only, 1 over 50 cm, stop
fishing after the steelhead quota, 10 steelhead a year recorded on the licence); Adams River (Region 3: 80 cm Dolly
Varden, Jul 31 at 60 cm); Bowron Lake p.43 (Bowron Lake 2 a day, park possession); Lois Lake p.24 ("Wild trout/char
daily quota = 2 (no wild trout 40 cm or more)", hatchery rainbow trout 6).
"""
from __future__ import annotations

import copy

import pytest

from pipeline.deliver.answers.model.trout import FishSheet
from pipeline.deliver.answers.trout import sheets as S
from pipeline.deliver.answers.trout import table as T
from pipeline.deliver.answers.trout import words as W
from pipeline.tests.test_trout_table import CASES, live, mini  # noqa: F401  (module fixtures)


def sheet_text(fr, key: str) -> str:
    """One row's sheet, whole: its head, then its Here lines (a bold one marked **)."""
    r = next(r for b in fr["table"]["boxes"] for r in b["rows"] if r["key"] == key)
    s = r["sheet"]
    return "\n".join([s["head"]] + [("**" if x["bold"] else "") + x["t"] for x in s["here"]])


# ------------------------------------------------------------------------------------------------- (a) no data: G's words

def test_the_sheet_words_whole():
    assert W.sheet_keep("counts toward the 5", 5) == "Keep · counts toward the 5"
    # T810 G2 Q2: a raised box's other fish ("Keep 5, brook trout up to 20") say their own number
    assert W.sheet_keep("counts toward the 5", 20) == "Keep · counts toward the 5 and the 20"
    assert W.sheet_shared("only 2 wild in all", "brown trout") == "Only 2 wild in all: this fish and brown trout."
    assert W.sheet_keep("1 total from these 4", 5) == "Keep · 1 total from these 4 · counts toward the 5"
    assert W.sheet_size("30 cm (12 in) or longer") == "Size: 30 cm (12 in) or longer."
    assert W.sheet_size("any size") == "Any size."
    assert W.sheet_sub("a rainbow over 50 cm (20 in)") == "Here a steelhead is a rainbow over 50 cm (20 in)."
    assert W.sheet_box(5, "brook trout and lake trout") == "Counts toward the 5 in all, together with brook trout and lake trout."
    assert W.sheet_own(3) == "This fish: only 3."
    assert W.sheet_shared("counts toward the 5 · 1 total from these 4", "brown trout and rainbow trout") == \
        "1 total from these 4: this fish and brown trout and rainbow trout."
    assert W.sheet_uses(50) == "A kept one is always over 50 cm (20 in), so it is that one."
    assert W.sheet_stop(1, "river", "steelhead") == ("Once you have kept 1 steelhead here, stop fishing this river for the "
                                                     "rest of the day: the whole river, not only this stretch.")
    assert W.sheet_licence(["10 a year", W.licence_stamp("Steelhead Conservation Surcharge Stamp",
                                                          "Only if you fish for steelhead, even to release them")]) == \
        ("Licence: 10 a year · Steelhead Conservation Surcharge Stamp, only if you fish for steelhead, even to release "
         "them.")
    assert W.sentence("only 1, counting any lake trout you keep") == "Only 1, counting any lake trout you keep."
    assert W.parse_duty_stop("stop fishing this creek after 2 a day") == ("creek", 2)
    assert W.parse_uses("uses the 1 over 50 cm (20 in)") == (1, 50)


def test_the_sheet_words_refuse():
    for bad in (lambda: W.sentence("keep 2 today"), lambda: W.sheet_shared("only 3", "x"),
                lambda: W.sheet_sub("over 50 cm"), lambda: W.licence_stamp("Stamp", "if you fish"),
                lambda: W.sheet_licence([])):
        with pytest.raises(W.WordsError):
            bad()


def test_the_elsewhere_lines_lose_today_and_refuse_any_other_form():
    """The stored question's " elsewhere today?" is said without "today" (the app's link word says the day)."""
    assert W.elsewhere_line("Kept 2 elsewhere today?", "Keep 3 more here") == "Kept 2 elsewhere? Keep 3 more here."
    assert W.elsewhere_line("Kept 1 at a lake?", "Keep 4 more here. Fish from lakes count toward the 5 a day.") == \
        "Kept 1 at a lake? Keep 4 more here. Fish from lakes count toward the 5 a day."
    for q, a in (("If you keep 4 hatchery rainbow trout here", "they don’t use up your 2"),
                 ("Kept 2 elsewhere today?", "none from other streams today"), ("Kept 2 today elsewhere?", "x")):
        with pytest.raises(W.WordsError):
            W.elsewhere_line(q, a)


# --------------------------------------------------------------------------------------- (b) every key of the mini release

def test_every_sheet_of_the_mini_release_is_whole_and_says_no_today(mini):
    n = 0
    for k, starts, moms, fs, ds in mini.keys:
        for fr in T.compose_key(fs, ds, starts, moms, mini.rules, licences=mini.lics[k]):
            if fr is None:
                continue
            assert all("today" not in t.lower() for t in fr["table"]["elsewhere"])
            for b in fr["table"]["boxes"]:
                for r in b["rows"]:
                    sh = r["sheet"]
                    FishSheet.model_validate(sh)
                    assert len(sh["src"]) <= S.SOURCES_MAX and list(sh["src"]) == list(r["src"][:S.SOURCES_MAX])
                    for x in sh["here"]:
                        assert "today" not in x["t"].lower() and x["t"].endswith((".", "?", "!")), x
                    n += 1
    assert n >= 20, n                                          # not vacuous


# ------------------------------------------------------------------------------------------- (c) the named fish, live

#: (case, row key) -> the sheet, whole
EXPECTED = {
    ('skeena-sedan-mill-jul15', 'ST'): (
        'Keep · 1 total from these 4 · counts toward the 5\n'
        'Here a steelhead is a rainbow over 50 cm (20 in).\n'
        'Hatchery only: wild ones go back.\n'
        'Counts toward the 5 in all, together with brook trout, lake trout, brown trout, cutthroat trout and rainbow trout.\n'
        '1 total from these 4: this fish and brown trout, cutthroat trout and rainbow trout.\n'
        'Only 1 over 50 cm (20 in).\n'
        'A kept one is always over 50 cm (20 in), so it is that one.\n'
        'Steelhead are known to be in this water.\n'
        '**Once you have kept 1 steelhead here, stop fishing this stream for the rest of the day: the whole stream, not only this stretch.\n'
        'Licence: 10 a year · record each on your licence right away.'
    ),
    ('skeena-sedan-mill-jul15', 'DV'): (
        'Put it back'
    ),
    ('adams-jul15', 'DV'): (
        'Keep · only 1 between them · counts toward the 4\n'
        'Size: 80 cm (31 in) or longer.\n'
        'Counts toward the 4 in all, together with lake trout, rainbow trout, brook trout, brown trout and cutthroat trout.\n'
        'Only 1 between them: this fish and lake trout.\n'
        'Only 1 over 50 cm (20 in).\n'
        'A kept one is always over 50 cm (20 in), so it is that one.'
    ),
    ('adams-jul31', 'DV'): (
        'Keep · only 1 between them · counts toward the 4\n'
        'Size: 80 cm (31 in) or longer.\n'
        'Counts toward the 4 in all, together with lake trout, rainbow trout, brook trout, brown trout and cutthroat trout.\n'
        'Only 1 between them: this fish and lake trout.\n'
        'Only 1 over 50 cm (20 in).\n'
        'A kept one is always over 50 cm (20 in), so it is that one.\n'
        'On Jul 31 only: 60 cm (24 in) or longer.'
    ),
    ('bowron-jul15', 'DV'): (
        'Keep · only 1 · counts toward the 2\n'
        'Any size.\n'
        'Counts toward the 2 in all, together with brook trout, brown trout, cutthroat trout, lake trout and rainbow trout.\n'
        'This fish: only 1.\n'
        'Only 1 over 50 cm (20 in).\n'
        'In your possession: no more than 2 trout and char in all (fish at home don’t count).'
    ),
    ('bowron-jul15', 'LT'): (
        'Keep · counts toward the 2\n'
        'Any size.\n'
        'Counts toward the 2 in all, together with brook trout, brown trout, cutthroat trout, rainbow trout and Dolly Varden/bull trout.\n'
        'Only 1 over 50 cm (20 in).\n'
        'In your possession: no more than 2 trout and char in all (fish at home don’t count).'
    ),
    ('lois-jul15', 'RB/wild'): (
        'Keep · counts toward the 6\n'
        'Size: under 40 cm (16 in).\n'
        'Counts toward the 6 in all, together with brook trout, brown trout, cutthroat trout, Dolly Varden/bull trout, lake trout and steelhead.\n'
        'Only 2 wild, each under 40 cm (16 in).'
    ),
    ('lois-jul15', 'ST'): (
        'Keep · counts toward the 6\n'
        'Any size.\n'
        'Hatchery only: wild ones go back.\n'
        'Counts toward the 6 in all, together with brook trout, brown trout, cutthroat trout, Dolly Varden/bull trout, lake trout and rainbow trout.\n'
        'Steelhead are known to be in this water.\n'
        'Licence: 10 a year · record each on your licence right away · Steelhead Conservation Surcharge Stamp, only if you fish for steelhead, even to release them.'
    ),
    ('lois-jul15', 'LT'): (
        'Keep · only 1 between them · counts toward the 4\n'
        'Size: 60 cm (24 in) or longer.\n'
        'Hatchery only: wild ones go back.\n'
        'Counts toward the 6 in all, together with brook trout, brown trout, cutthroat trout, Dolly Varden/bull trout, rainbow trout and steelhead.\n'
        'Only 1 between them: this fish and Dolly Varden/bull trout.\n'
        'Only 1 over 50 cm (20 in), not counting rainbow trout or steelhead.\n'
        'A kept one is always over 50 cm (20 in), so it is that one.'
    ),
}


@pytest.mark.needs_bundle
@pytest.mark.parametrize("case,key", sorted(EXPECTED), ids=[f"{c}:{k}" for c, k in sorted(EXPECTED)])
def test_the_named_fish_sheets_whole(live, case, key):
    assert sheet_text(live.frame(case), key) == EXPECTED[(case, key)]


@pytest.mark.needs_bundle
def test_the_skeena_elsewhere_lines_whole(live):
    assert live.frame("skeena-sedan-mill-jul15")["table"]["elsewhere"] == (
        "Your own limit, counted across every water in Region 6 you fish, lakes and streams alike.",       # T810 L
        "Kept 2 elsewhere? Keep 3 more here, but no more than 1 brown trout, cutthroat trout, rainbow trout or steelhead "
        "from streams in all.",
        "Kept trout or char over 50 cm (20 in) elsewhere? It counts here too: at most 1 over 50 cm (20 in) a day in all.")


@pytest.mark.needs_bundle
def test_the_adams_one_day_line_is_the_sheets_last(live):
    """Spec 3.3 rule 6: the 80 cm stays on the row and in the sheet's size; the one looser day is the sheet's last line,
    on Jul 31 only, and the Jul 15 sheet never says it."""
    assert sheet_text(live.frame("adams-jul31"), "DV").endswith("\nOn Jul 31 only: 60 cm (24 in) or longer.")
    assert "only:" not in sheet_text(live.frame("adams-jul15"), "DV")


# ----------------------------------------------------------------------------------- (d) a planted drop and the mutations

def _lois_without_the_steelhead_stamp(live):
    rows, ds, starts, moms = live.key("lois-jul15")
    lic = copy.deepcopy(list(live.licences("lois-jul15")))
    for f in lic:
        for p in f["profiles"]:
            p["documents"] = [d for d in p.get("documents") or () if d["doc"] != "steelhead_stamp"]
    from pipeline.deliver import calendar as CAL
    from pipeline.deliver.answers import encode as E
    j = E.segment_index(starts, CAL.day_of(CASES["lois-jul15"][2]), moms, "Wednesday", inside=False)
    return T.compose_key(rows, ds, starts, moms, live.rules, licences=lic)[j]


@pytest.mark.needs_bundle
def test_a_stamp_the_licence_answer_dropped_is_not_on_the_sheet(live):
    """Contract 2.2: the stamps are read from the POST-PASS licence answer — a document it no longer lists for the fish
    (as `drop_closed_steelhead_stamp` drops one) is not said, though the row still carries the stamp."""
    got = sheet_text(_lois_without_the_steelhead_stamp(live), "ST")
    assert got.endswith("\nLicence: 10 a year · record each on your licence right away.")


@pytest.mark.needs_bundle
def test_mutation_stamps_not_read_from_the_licence_answer_fail(live, monkeypatch):
    monkeypatch.setattr(S, "A_STAMP_FROM_THE_POST_PASS_LICENCE", False)
    assert "Steelhead Conservation Surcharge Stamp" in sheet_text(_lois_without_the_steelhead_stamp(live), "ST")


@pytest.mark.needs_bundle
def test_mutation_sources_not_cut_fail(live, monkeypatch):
    """Q-D6: the Skeena steelhead's rules number more than 4; the sheet cites the first 4."""
    fr = live.frame("skeena-sedan-mill-jul15")
    row = next(r for b in fr["table"]["boxes"] for r in b["rows"] if r["key"] == "ST")
    assert len(row["src"]) > S.SOURCES_MAX and row["sheet"]["src"] == row["src"][:S.SOURCES_MAX]
    monkeypatch.setattr(S, "A_SOURCES_CUT_TO_FOUR", False)
    with pytest.raises(ValueError, match="at most 4"):          # the model refuses the sheet: the build stops
        live.frame("skeena-sedan-mill-jul15")
