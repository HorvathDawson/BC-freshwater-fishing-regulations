"""T104 — ONE "when" in words (owner 2026-10-22: one decision = one function used everywhere).

"How is a rule's `when` (dates, weekdays, hours, unparsed seasons) said in words?" is decided by
`pipeline.deliver.answers.common.when_words(when, style)` and nowhere else. Before it, `display._plain` said
dates only, `display._title` dates and weekdays, `labels._when_words` everything, and `rows.py` four more
weekday wordings — so Campbell River's night closure (`when.hours` 21:00–05:00) shipped as "No fishing, Aug
1–Dec 31", a closure all day, and Kootenay Lake's Lower West Arm plain said "Keep up to 5 kokanee a day" on
the rule held only Saturdays and Sundays.

  * whole strings on the real rules' fields (AGENTS 33);
  * THE GATE (`display.when_problems`, run by `display.build_rules`): a rule with hours or weekdays names them
    in its title and its plain sentence — a mutation dropping hours from `_title` fails it;
  * THE PIN: no other module of `pipeline/deliver/answers` spells a weekday or a time of day itself, the
    catalogue's label reads the same function, and `rows.py`'s weekday words are calls of it.
"""
from __future__ import annotations

import ast
from pathlib import Path
from types import SimpleNamespace

import pytest

from pipeline.deliver.answers import common as C
from pipeline.deliver.answers import display as D
from pipeline.deliver.answers import rows as RW
from pipeline.deliver.answers.common import AnswersError, when_words

ROOT = Path(__file__).resolve().parents[2]
ANSWERS = ROOT / "pipeline/deliver/answers"

AUG_DEC = {"from_month": 8, "from_day": 1, "to_month": 12, "to_day": 31}
NIGHT_CLOCK = {"start": {"at": "21:00", "offset_min": 0}, "end": {"at": "05:00", "offset_min": 0}}
NIGHT_SUN = {"start": {"solar": "sunset", "offset_min": 60}, "end": {"solar": "sunrise", "offset_min": -60}}
WEEKEND = ["Saturday", "Sunday"]
WEEK = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]

# the live rules' own fields (bundle r-36fbc5326506), cut to what the words read
CAMPBELL_R4 = {"entry": "r2:campbell_river@2-4", "rule": "campbell_river.r4", "type": "retention_limit",
               "family": "retention", "species": ["ALL_GAME_FISH"], "take": 0, "may_target": 0,
               "when": {"dates": [AUG_DEC], "hours": NIGHT_CLOCK}}
PITT_R4 = {"entry": "r2:pitt_river@2-8", "rule": "pitt_river.r4", "type": "retention_limit",
           "family": "retention", "species": ["ALL_GAME_FISH"], "take": 0, "may_target": 0,
           "when": {"hours": NIGHT_SUN}}
LWA_R3 = {"entry": "r4:kootenay_lake_lower_west_arm", "rule": "kootenay_lake_lower_west_arm.r3",
          "type": "retention_limit", "family": "retention", "species": ["KO"], "take": 5,
          "when": {"weekdays": WEEKEND}}
LWA_R4 = {"entry": "r4:kootenay_lake_lower_west_arm", "rule": "kootenay_lake_lower_west_arm.r4",
          "type": "retention_limit", "family": "retention", "species": ["KO"], "take": 0,
          "when": {"weekdays": WEEK}}


# --- the one function, every style, whole strings ----------------------------------------------------------

def test_dates_hours_weekdays_unparsed_in_every_style():
    w = {"dates": [AUG_DEC], "hours": NIGHT_CLOCK}
    assert when_words(w, "title") == "Aug 1–Dec 31, 21:00 to 05:00"
    assert when_words(w, "plain") == "Aug 1–Dec 31, 21:00 to 05:00"
    assert when_words(w, "label") == "Aug 1-Dec 31, 21:00 to 05:00"
    assert when_words({"hours": NIGHT_SUN}, "plain") == "one hour after sunset to one hour before sunrise"
    assert when_words({"hours": {"start": {"solar": "sunrise", "offset_min": -30},
                                 "end": {"solar": "sunset", "offset_min": 120}}}, "title") == \
        "30 minutes before sunrise to 2 hours after sunset"
    assert when_words({"weekdays": WEEKEND}, "title") == "Sat–Sun"
    assert when_words({"weekdays": WEEKEND}, "plain") == "on Saturdays and Sundays"
    assert when_words({"weekdays": WEEKEND}, "label") == "on Saturdays and Sundays"
    assert when_words({"weekdays": WEEKEND}, "status") == "Weekends"
    assert when_words({"weekdays": WEEKEND}, "days") == "Saturday and Sunday"
    assert when_words({"weekdays": WEEK}, "title") == "Mon–Fri"
    assert when_words({"weekdays": WEEK}, "plain") == "Monday to Friday"
    assert when_words({"weekdays": WEEK}, "label") == \
        "on Mondays and Tuesdays and Wednesdays and Thursdays and Fridays"
    assert when_words({"weekdays": WEEK}, "status") == "Weekdays"
    assert when_words({"weekdays": WEEK}, "days") == "Monday to Friday"
    assert when_words({"weekdays": ["Monday", "Wednesday"]}, "title") == "Mon, Wed"
    assert when_words({"weekdays": ["Monday", "Wednesday"]}, "status") == "Mon, Wed"
    assert when_words({"weekdays": ["Monday", "Wednesday"]}, "plain") == "on Mondays and Wednesdays"
    two = {"dates": [{"from_month": 6, "from_day": 1, "to_month": 6, "to_day": 30}, AUG_DEC],
           "weekdays": WEEKEND, "unparsed": ["To be determined"]}
    assert when_words(two, "title") == "Jun 1–30, Aug 1–Dec 31, Sat–Sun, as printed: To be determined"
    assert when_words(two, "label") == \
        "Jun 1-Jun 30 and Aug 1-Dec 31, on Saturdays and Sundays, as printed: To be determined"
    assert when_words(None, "plain") == "" and when_words({}, "title") == ""


def test_a_weekday_style_refuses_a_when_it_cannot_say_whole():
    with pytest.raises(AnswersError):
        when_words({"weekdays": WEEKEND, "dates": [AUG_DEC]}, "status")
    with pytest.raises(AnswersError):
        when_words({"weekdays": WEEKEND}, "shouty")


# --- the real rules: title and plain both name hours and weekdays ------------------------------------------

def test_a_night_closure_says_its_hours_in_title_and_plain():
    assert D.title(CAMPBELL_R4) == "No fishing, Aug 1–Dec 31, 21:00 to 05:00"
    assert D.plain(CAMPBELL_R4) == "No fishing, Aug 1–Dec 31, 21:00 to 05:00."
    assert D.title(PITT_R4) == "No fishing, one hour after sunset to one hour before sunrise"
    assert D.plain(PITT_R4) == "No fishing, one hour after sunset to one hour before sunrise."


def test_a_weekday_rule_says_its_days_in_title_and_plain():
    assert D.title(LWA_R3) == "Keep up to 5 kokanee a day, Sat–Sun"
    assert D.plain(LWA_R3) == "Keep up to 5 kokanee a day, on Saturdays and Sundays."
    assert D.title(LWA_R4) == "Release kokanee, Mon–Fri"
    assert D.plain(LWA_R4) == "Release every kokanee, Monday to Friday."


def test_rows_weekday_words_are_the_one_function():
    assert RW.weekdays_words(WEEKEND) == when_words({"weekdays": WEEKEND}, "plain") == "on Saturdays and Sundays"
    assert RW.weekdays_short(WEEK) == "Mon–Fri"
    assert RW.weekdays_label(WEEKEND) == "Weekends"
    assert RW.weekdays_when(WEEKEND) == "Saturday and Sunday"


def test_the_catalogue_label_reads_the_same_function():
    from pipeline.regs.parsing.catalogue_parts.dates import When
    from pipeline.regs.parsing.catalogue_parts import labels as LB
    w = When.model_validate({"dates": [AUG_DEC], "hours": NIGHT_SUN, "weekdays": ["Friday", "Saturday"],
                             "unparsed": ["To be determined"]})
    r = SimpleNamespace(when=w)
    assert LB._when_words(r) == ("Aug 1-Dec 31, on Fridays and Saturdays, one hour after sunset to one hour "
                                 "before sunrise, as printed: To be determined")
    assert LB._when_words(SimpleNamespace(when=None)) == ""
    assert w.hours.words() == "one hour after sunset to one hour before sunrise"
    assert w.dates[0].words() == "Aug 1-Dec 31"


# --- the gate -----------------------------------------------------------------------------------------------

def _build(*rules):
    B = SimpleNamespace(index={(x["entry"], x["rule"]): i for i, x in enumerate(rules)},
                        rules={(x["entry"], x["rule"]): x for x in rules})
    return D.build_rules(B)


def test_the_gate_passes_the_real_shapes():
    got = _build(CAMPBELL_R4, PITT_R4, LWA_R3, LWA_R4)
    assert [f["title"] for f in got] == ["No fishing, Aug 1–Dec 31, 21:00 to 05:00",
                                        "No fishing, one hour after sunset to one hour before sunrise",
                                        "Keep up to 5 kokanee a day, Sat–Sun", "Release kokanee, Mon–Fri"]


def test_the_gate_fails_when_title_drops_the_hours(monkeypatch):
    real = D._title

    def no_hours(x):
        return real({**x, "when": {k: v for k, v in (x.get("when") or {}).items() if k != "hours"}})
    monkeypatch.setattr(D, "_title", no_hours)
    with pytest.raises(AnswersError, match="campbell_river.r4.*title.*21:00 to 05:00"):
        _build(CAMPBELL_R4)


def test_the_gate_fails_when_plain_drops_the_weekdays(monkeypatch):
    real = D._plain

    def no_days(x, names=None):
        return real({**x, "when": {k: v for k, v in (x.get("when") or {}).items() if k != "weekdays"}}, names)
    monkeypatch.setattr(D, "_plain", no_days)
    with pytest.raises(AnswersError, match="kootenay_lake_lower_west_arm.r3.*plain.*Saturday"):
        _build(LWA_R3)


# --- the pin: no second copy --------------------------------------------------------------------------------

_DAY_NAMES = {d for d in C.WEEKDAYS} | {d[:3] for d in C.WEEKDAYS} | {d + "s" for d in C.WEEKDAYS}


def _offenders(path: Path, src: str = None):
    tree = ast.parse(src if src is not None else path.read_text(encoding="utf-8"))
    out = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and \
                (node.value in _DAY_NAMES or node.value in ("solar", "offset_min", "sunset", "sunrise")):
            out.append(f"{path.name}:{node.lineno} {node.value!r}")
        if isinstance(node, ast.Name) and node.id == "WEEKDAYS":
            out.append(f"{path.name}:{node.lineno} WEEKDAYS")
        if isinstance(node, ast.Attribute) and node.attr == "WEEKDAYS":
            out.append(f"{path.name}:{node.lineno} .WEEKDAYS")
    return out


def test_no_other_answers_module_spells_a_weekday_or_a_time_of_day():
    """`common.py` holds the words; `model.py` only TYPES the weekdays (`Literal[WEEKDAYS]`) and the clock
    fields; every other module asks `when_words`."""
    bad = []
    for p in sorted(ANSWERS.glob("*.py")):
        if p.name in ("common.py", "model.py"):
            continue
        bad += _offenders(p)
    assert not bad, bad


def test_the_pin_catches_a_weekday_spelled_again():
    p = ANSWERS / "display.py"
    src = p.read_text(encoding="utf-8")
    assert not _offenders(p, src)
    mutated = src + '\n\ndef _short(days):\n    return "Sat–Sun" if days == ["Saturday", "Sunday"] else ""\n'
    assert [o.split(" ", 1)[1] for o in _offenders(p, mutated)] == ["'Saturday'", "'Sunday'"]


def test_the_label_and_the_rows_words_call_when_words():
    labels = (ROOT / "pipeline/regs/parsing/catalogue_parts/labels.py").read_text(encoding="utf-8")
    fn = labels[labels.index("def _when_words"):]
    fn = fn[:fn.index("\ndef ")]
    assert "when_words(" in fn and "hours.words()" not in fn and 'f"{d}s"' not in fn
    dates = (ROOT / "pipeline/regs/parsing/catalogue_parts/dates.py").read_text(encoding="utf-8")
    for cls in ("class DateRange(", "class Hours("):        # review: a date range's words were a second copy
        body = dates[dates.index(cls):]
        body = body[body.index("def words("):]
        body = body[:body.index("\n    def ", 1) if "\n    def " in body[1:] else len(body)]
        body = body[:body.index("\nclass ")] if "\nclass " in body else body
        assert "when_words(" in body and "MONTHS" not in body, cls
    rows = (ANSWERS / "rows.py").read_text(encoding="utf-8")
    for name in ("weekdays_words", "weekdays_short", "weekdays_label", "weekdays_when"):
        body = rows[rows.index(f"def {name}("):]
        body = body[:body.index("\ndef ")]
        assert "when_words(" in body, name


@pytest.mark.needs_bundle
def test_every_live_rule_with_hours_or_weekdays_names_them():
    """The gate over the live bundle: `build_rules` raises on any rule that drops them."""
    from pipeline.tests.conftest import need, BUNDLE_HINT
    p = C.bundle_path()
    need(None, "bundle", p, BUNDLE_HINT)
    B = C.load(p)
    facts = D.build_rules(B)
    order = sorted(B.index, key=B.index.__getitem__)
    held = [(k, f) for k, f in zip(order, facts)
            if (B.rules[k].get("when") or {}).get("hours") or (B.rules[k].get("when") or {}).get("weekdays")]
    assert len(held) >= 20
    got = {k[1]: (f["title"], f.get("plain")) for k, f in held}
    assert got["campbell_river.r4"] == ("No fishing, Aug 1–Dec 31, 21:00 to 05:00",
                                        "No fishing, Aug 1–Dec 31, 21:00 to 05:00.")
    assert got["kootenay_lake_lower_west_arm.r4"] == ("Release kokanee, Mon–Fri",
                                                      "Release every kokanee, Monday to Friday.")
