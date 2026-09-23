"""`pipeline.deliver.bundle.rules` — the row it writes, and the summary it prints about it.

The summary matters more than a printed line usually would: it is what a person reads to decide
whether a province-wide rebuild is healthy, and there is no second place to check.
"""

import json

from pathlib import Path

import pipeline.deliver.bundle.rules as rules_mod
from pipeline.deliver.bundle.rules import _rule_row


RULE = {
    "rule_id": "x.r1",
    "type": "retention_limit",
    "species": ["BT"],
    "take": 2,
    "verbatim": "Trout daily quota 2",
}


def _fields():
    """The row, by position — the thing that drifts."""
    return _rule_row("r1:x@1-1", dict(RULE), uncertain=True)


def test_the_uncertain_flag_is_the_only_field_that_says_uncertain():
    """**The bug this file exists for.** The summary counted `r[8]`, which is
    `json.dumps(species)` — a string that is never empty ("[]" at minimum) and so always
    truthy. Every build reported every rule as uncertain: 3,422 of 3,422. The column itself
    was always right, so nothing downstream broke and nothing failed; only the number a person
    reads to judge a build was meaningless, and it was wrong in the safe-looking direction.

    Pinned by VALUE rather than by index, so moving a field cannot quietly restore it.
    """
    on = _rule_row("r1:x@1-1", dict(RULE), uncertain=True)
    off = _rule_row("r1:x@1-1", dict(RULE), uncertain=False)
    differ = [i for i, (a, b) in enumerate(zip(on, off)) if a != b]
    assert len(differ) == 1, f"uncertain changed {len(differ)} fields: {differ}"
    assert on[differ[0]] == 1 and off[differ[0]] == 0


def test_an_empty_json_collection_is_truthy_and_so_can_never_stand_in_for_a_flag():
    """Why the old index was silently wrong rather than loudly wrong. Several fields are JSON
    strings, and a JSON string for an EMPTY collection is "[]" — two characters, and truthy.
    A `if r[i]` test on any of them is true for every rule in the corpus.

    This rule names no `species_except` and no windows, so those fields are as empty as the
    row ever gets; they are still truthy."""
    row = _rule_row("r1:x@1-1", dict(RULE), uncertain=False)
    blanks = [f for f in row if isinstance(f, str) and f in ("[]", "{}")]
    assert blanks, "expected an empty-collection JSON string in the row"
    assert all(bool(b) for b in blanks), "'[]' is truthy — that is the trap"


def test_the_row_is_the_length_the_insert_expects():
    """The INSERT names its columns because a positional list once wrote a 42 MB bundle with
    zero entries in it. This holds the row to the same count from the other side."""
    assert len(_fields()) == 18


def test_species_survive_as_json_not_python_repr():
    # By NAME — an index here moved when `when_`/`while_` became columns.
    assert json.loads(_named(dict(RULE))["species"]) == ["BT"]


def _cols():
    """The INSERT's own column list, so a test can name a field instead of an index."""
    import re
    src = Path(rules_mod.__file__).read_text()
    m = re.search(r'INSERT INTO rule \((.*?)\)\s*"\s*"VALUES', src, re.S)
    return [c.strip() for c in re.sub(r'"\s*"', "", m.group(1)).split(",")]


def _named(raw, **kw):
    return dict(zip(_cols(), _rule_row("r1:x@1-1", raw, uncertain=False, **kw)))


def test_the_season_ships_from_when_and_only_from_when():
    """EVERY SEASON WAS LOST. The column was filled from the rule's `windows`/`dates` — prose-era
    fields a catalogue rule does not have — so all 3,269 rules shipped `[]`, which the app reads
    as ALL YEAR: every seasonal closure in force every day. The season is `when`."""
    got = _named({**RULE, "when": {"dates": [
        {"from_month": 6, "from_day": 1, "to_month": 6, "to_day": 30}],
        "weekdays": ["Saturday"]}})
    when = json.loads(got["when_"])
    assert when["dates"] == [{"from_month": 6, "from_day": 1, "to_month": 6, "to_day": 30}]
    assert when["weekdays"] == ["Saturday"]
    # one home: not also in `conditions`
    assert "when" not in json.loads(got["conditions"] or "{}")
    # no season = NULL = all year, and a prose `dates` key is refused by the model, never read
    assert _named(dict(RULE))["when_"] is None
    import pytest
    with pytest.raises(Exception):
        _rule_row("r1:x@1-1", {**RULE, "dates": ["Jun 1-30"]}, uncertain=False)


def test_hours_and_unparsed_seasons_ship_too():
    """A night closure's hours and a season nobody could read both reach the client — an
    unparsed season dropped here would make the rule all year."""
    got = _named({**RULE, "when": {"hours": {"start": {"at": "21:00"}, "end": {"at": "05:00"}},
                                   "unparsed": ["To be determined"]}})
    when = json.loads(got["when_"])
    assert when["hours"]["start"]["at"] == "21:00" and when["unparsed"] == ["To be determined"]


def test_while_is_a_column_because_it_decides_an_outcome():
    """"Only non-game fish may be speared" is take 0 on every game fish WHILE spear fishing.
    The app looked for `conditions.method` — retired — and every river in B.C. read CLOSED."""
    got = _named({**RULE, "take": 0, "may_target": False, "species": ["ALL_GAME_FISH"],
                  "while": ["spear_fishing"], "verbatim": "only non-game fish may be speared"})
    assert json.loads(got["while_"]) == ["spear_fishing"]
    assert "while" not in json.loads(got["conditions"] or "{}")
    assert _named(dict(RULE))["while_"] is None


def test_standing_is_a_column_because_it_decides_an_outcome():
    """"No fishing within 23 m downstream of any fishway" holds everywhere at places no dataset
    draws. Read as the closure it literally is, it painted every section of B.C. CLOSED."""
    got = _named({**RULE, "take": 0, "may_target": False, "species": ["ALL_GAME_FISH"],
                  "standing": True, "review_reason": "no dataset of fishways",
                  "verbatim": "Within 23 m downstream of the lower entrance to any fishway"})
    assert got["standing"] == 1 and "standing" not in json.loads(got["conditions"] or "{}")
    assert _named(dict(RULE))["standing"] == 0
