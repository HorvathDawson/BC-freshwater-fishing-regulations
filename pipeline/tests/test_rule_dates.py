"""A RULE KEEPS THE DATES ITS PRINTED SENTENCE CARRIES (audit of 2026-09-24).

An absent `when` is ALL YEAR, so a season lost between the page and the rule silently widens the
rule to every day. The model refuses the shapes that did that (`catalogue._own_dates_carried`,
`catalogue._dates_lost`); each is pinned here on a synthetic row AND on the real corpus, by
breaking a real rule and watching the model refuse it (the test that proves a check is not a
tautology — "checks seeded from input launder").
"""
from __future__ import annotations

import copy
import json

import pytest

from pipeline.regs.parsing import catalogue as C
from pipeline.regs.parsing.catalogue import CatalogueEntry, CatalogueRule


@pytest.fixture(scope="module")
def raw():
    from pipeline.regs.parsing.io import read_all_entries
    return read_all_entries()


def _r(**kw) -> dict:
    kw.setdefault("extents", [{"op": "whole"}])
    return kw


def _entry(text: str, *rules: dict) -> dict:
    return {"entry_id": "r9:test@9-9", "name": "TEST", "regs_verbatim": text, "rules": list(rules)}


D = {"dates": [{"from_month": 5, "from_day": 1, "to_month": 10, "to_day": 31}]}


# --------------------------------------------------------------------------- the rule's own words
def test_a_rule_whose_sentence_prints_dates_carries_them():
    ok = _r(rule_id="a.r1", type="bait_restriction", verbatim="Bait ban, May 1-Oct 31",
            gear=[{"slot": "bait", "ban": ["any_bait"]}], when=D)
    CatalogueRule.model_validate(ok)
    with pytest.raises(ValueError, match="no `when` — an absent `when` is ALL YEAR"):
        CatalogueRule.model_validate({k: v for k, v in ok.items() if k != "when"})
    wrong = dict(ok, when={"dates": [{"from_month": 6, "from_day": 1, "to_month": 6,
                                      "to_day": 30}]})
    with pytest.raises(ValueError, match="neither the dates its sentence prints"):
        CatalogueRule.model_validate(wrong)


def test_the_complement_and_the_other_half_are_readings_of_the_words():
    # "Open June 16-Apr 30" is a closure on the other days (Fulton River)
    CatalogueRule.model_validate(_r(
        rule_id="f.r1", type="retention_limit", verbatim="Open June 16-Apr 30 each year",
        species=["ALL_GAME_FISH"], take=0, may_target=False,
        when={"dates": [{"from_month": 5, "from_day": 1, "to_month": 6, "to_day": 15}]}))
    # a sentence with a window for one place and "all year" for another: this rule is the other
    CatalogueRule.model_validate(_r(
        rule_id="p.r1", type="retention_limit", species=["BT"], take=0, may_target=True,
        verbatim="Bull trout from the Liard River watershed Aug 15-Oct 15, and from the Peace "
                 "River watershed all year"))
    # a lift names the window of the rule it lifts, not its own
    CatalogueRule.model_validate(_r(
        rule_id="l.r1", type="retention_limit", species=["ALL_GAME_FISH"],
        verbatim="Exempt from July 15-Aug 31 summer closure",
        exempts=[{"default_id": "summer_stream_closure"}]))


def test_a_date_cut_off_after_its_month_is_refused():
    with pytest.raises(ValueError, match="a date cut off"):
        CatalogueRule.model_validate(_r(
            rule_id="c.r7", type="bait_restriction", gear=[{"slot": "bait", "ban": ["any_bait"]}],
            verbatim="bait ban, downstream of the southern entrance to the tunnel, Nov"))
    # …and on the row, when no rule quoted the cut words
    with pytest.raises(ValueError, match="the row ends in a month with no day"):
        CatalogueEntry.model_validate(_entry(
            "Trout/char catch and release, bait ban, Nov",
            _r(rule_id="c.r1", type="bait_restriction", verbatim="bait ban",
               gear=[{"slot": "bait", "ban": ["any_bait"]}])))


# --------------------------------------------------------------------------- the row and siblings
def test_a_printed_window_no_rule_carries_is_lost():
    text = "No Fishing Dec 1-Mar 31\nTrout daily quota = 2"
    row = _entry(text,
                 _r(rule_id="t.r1", type="retention_limit", verbatim="No Fishing",
                    species=["ALL_GAME_FISH"], take=0, may_target=False),
                 _r(rule_id="t.r2", type="retention_limit", verbatim="Trout daily quota = 2",
                    species=["TROUT"], take=2))
    with pytest.raises(ValueError, match="prints Dec 1-Mar 31 and no rule"):
        CatalogueEntry.model_validate(row)
    row["rules"][0]["when"] = {"dates": [{"from_month": 12, "from_day": 1, "to_month": 3,
                                          "to_day": 31}]}
    CatalogueEntry.model_validate(row)


def test_a_clause_inside_a_dated_siblings_sentence_is_dated():
    text = "Trout daily quota = 2 (none under 30 cm), May 1-Oct 31"
    quota = _r(rule_id="t.r1", type="retention_limit", verbatim=text, species=["TROUT"], take=2,
               when=D)
    size = _r(rule_id="t.r2", type="retention_limit", verbatim="none under 30 cm",
              species=["TROUT"], lengths=[{"max_cm": 30, "take": 0}])
    with pytest.raises(ValueError, match="part of t.r1's sentence"):
        CatalogueEntry.model_validate(_entry(text, quota, size))
    CatalogueEntry.model_validate(_entry(text, quota, dict(size, when=D)))


def test_the_date_straight_after_a_quote_is_its_date():
    text = "Trout daily quota = 2 (none under 30 cm), May 1-Oct 31"
    cut = _r(rule_id="t.r1", type="retention_limit", verbatim="Trout daily quota = 2 (none under "
             "30 cm)", species=["TROUT"], take=2)
    with pytest.raises(ValueError, match="date straight after its words"):
        CatalogueEntry.model_validate(_entry(text, cut))
    CatalogueEntry.model_validate(_entry(text, dict(cut, when=D)))


def test_a_and_b_then_the_date_governs_both():
    text = "Trout/char catch and release and bait ban, June 15-Aug 31"
    w = {"dates": [{"from_month": 6, "from_day": 15, "to_month": 8, "to_day": 31}]}
    cr = _r(rule_id="a.r1", type="retention_limit", verbatim="Trout/char catch and release",
            species=["TROUT_CHAR"], take=0, may_target=True)
    bait = _r(rule_id="a.r2", type="bait_restriction", verbatim="bait ban, June 15-Aug 31",
              gear=[{"slot": "bait", "ban": ["any_bait"]}], when=w)
    with pytest.raises(ValueError, match="the date governs every clause before it"):
        CatalogueEntry.model_validate(_entry(text, cr, bait))
    CatalogueEntry.model_validate(_entry(text, dict(cr, when=w), bait))


def test_a_semicolon_or_a_list_comma_is_not_the_shape():
    """The book repeats a date per clause when it means them apart; after `;` the next clause
    is its own ("Trout/char catch and release; bait ban, June 15-Oct 31" — Sand Creek), and a
    date closing a comma-joined item is that item's ("ALL STEELHEAD, Bull trout from streams,
    Aug 1-Oct 31" — Region 3)."""
    w = {"dates": [{"from_month": 6, "from_day": 15, "to_month": 10, "to_day": 31}]}
    text = "Trout/char catch and release; bait ban, June 15-Oct 31"
    CatalogueEntry.model_validate(_entry(
        text,
        _r(rule_id="s.r1", type="retention_limit", verbatim="Trout/char catch and release",
           species=["TROUT_CHAR"], take=0, may_target=True),
        _r(rule_id="s.r2", type="bait_restriction", verbatim="bait ban, June 15-Oct 31",
           gear=[{"slot": "bait", "ban": ["any_bait"]}], when=w)))
    text = "And you must release: ALL STEELHEAD, Bull trout from streams, Aug 1-Oct 31."
    CatalogueEntry.model_validate(_entry(
        text,
        _r(rule_id="z.r5", type="retention_limit", verbatim="ALL STEELHEAD", species=["ST"],
           take=0, may_target=True),
        _r(rule_id="z.r6", type="retention_limit", verbatim="Bull trout from streams, Aug 1-Oct 31",
           species=["BT"], take=0, may_target=True,
           when={"dates": [{"from_month": 8, "from_day": 1, "to_month": 10, "to_day": 31}]})))


def test_a_window_held_elsewhere_must_still_need_the_exemption():
    """The two Zone B windows are listed because another row holds them. A listing no longer
    needed is refused, so the list cannot go stale."""
    assert set(C.WINDOWS_HELD_ELSEWHERE) == {("z7b:trout_char_quota", "Oct 16-Aug 14"),
                                             ("z7b:trout_char_quota", "Aug 15-Oct 15")}
    text = "NOTE: Bull trout may only be retained from Oct 16-Aug 14."
    row = dict(_entry(text, _r(
        rule_id="n.r1", type="retention_limit", species=["BT"], take=1, verbatim=text,
        when={"dates": [{"from_month": 10, "from_day": 16, "to_month": 8, "to_day": 14}]})),
        entry_id="z7b:trout_char_quota")
    with pytest.raises(ValueError, match="take it off the list"):
        CatalogueEntry.model_validate(row)


# --------------------------------------------------------------------------- the real corpus
def test_no_within_clause_of_a_dated_quota_is_undated(raw):
    n = 0
    for e in raw.values():
        ce = CatalogueEntry.model_validate(e)
        rules = {r.rule_id: r for r in ce.rules}
        for r in ce.rules:
            p = rules.get(r.within or "")
            if p is not None and p.when is not None and p.when.dates:
                n += 1
                assert r.when is not None and r.when.dates, (ce.entry_id, r.rule_id)
    assert n == 15


def test_coquihalla_keeps_its_last_line_season(raw):
    """The last printed line wraps "…, Nov" / "1-Mar 31"; extraction kept "Nov" only, and the
    catch and release and the bait ban below the tunnels read all year."""
    ce = CatalogueEntry.model_validate(raw["r2:coquihalla_river@2-17"])
    assert ce.regs_verbatim.endswith("lower most railway tunnel, Nov 1-Mar 31")
    for rid in ("coquihalla_river.r6", "coquihalla_river.r7"):
        r = next(x for x in ce.rules if x.rule_id == rid)
        assert r.when.words() == "Nov 1-Mar 31" and not r.review_reason


@pytest.mark.parametrize("eid,rid,why", [
    # the rule's own sentence prints the dates
    ("r3:fraser_river@3-14", "fraser_river.r2", "neither the dates|no `when`"),
    ("r7:monkman_lake@7-21", "monkman_lake.r2", "no `when`"),
    # "A and B, dates"
    ("r4:abruzzi_creek@4-23", "abruzzi_creek.r2", "governs every clause"),
    ("r4:elk_river_downstream_of_elko_dam@4-2", "elk_river_downstream_of_elko_dam.r1",
     "governs every clause|part of"),
    # the date straight after the quote / a within clause
    ("r6:cheslatta_lake@6-4", "cheslatta_lake.r2", "date straight after|within|governs"),
    ("r3:adams_river_upstream_of_adams_lake@3-37", "adams_river_upstream.r5", "within"),
    # a row whose only carrier of a window loses it
    ("r7:kakwa_lake@7-19", "kakwa_lake.r1", "no rule or licensing record carries"),
    ("r2:chilliwack_vedder_rivers_does_not_include_sumas_river_see_ma@2-4",
     "chilliwack_vedder_rivers.r4", "governs every clause"),
])
def test_breaking_a_real_rule_is_refused(raw, eid, rid, why):
    e = copy.deepcopy(raw[eid])
    CatalogueEntry.model_validate(e)
    r = next(x for x in e["rules"] if x["rule_id"] == rid)
    r.pop("when")
    with pytest.raises(ValueError, match=why):
        CatalogueEntry.model_validate(e)


def test_stripping_any_real_rules_dates_is_nearly_always_caught(raw):
    """The measurement behind the checks: remove the dates from each dated rule in the corpus,
    one at a time, and count what the model refuses. The ones it cannot see are named — the
    shapes that need a human reading (a place phrase between the clauses and the date, a
    "months of February and July", a comma list) — so a new blind spot shows up here."""
    n, missed = 0, []
    for e in raw.values():
        for i, r in enumerate(e.get("rules") or []):
            if not (r.get("when") or {}).get("dates"):
                continue
            m = copy.deepcopy(e)
            w = {k: v for k, v in m["rules"][i]["when"].items() if k != "dates"}
            if w:
                m["rules"][i]["when"] = w
            else:
                m["rules"][i].pop("when")
            n += 1
            try:
                CatalogueEntry.model_validate(m)
                missed.append((e["entry_id"].split("@")[0], r["rule_id"]))
            except ValueError:
                pass
    assert n >= 560
    assert sorted(missed) == sorted([
        # (coquihalla_river.r2 was here: its season was the bait ban's across a `;`, and it
        # carries none now — the 2026-09-25 ruling)
        ("r2:coquihalla_river", "coquihalla_river.r6"),
        *[("r3:mahood_lake_see_map_on_page_28_for_area_closure", f"mahood_lake.r{k}")
          for k in (2, 3, 4, 6, 7, 8)],
        ("r6:tchesinkut_lake", "tchesinkut_lake.r1"),
        ("r7:crooked_river", "crooked_river.r2")]), json.dumps(missed)


# --------------------------------------------------------------------------- ruling of 2026-09-25
def test_a_date_after_a_semicolon_is_only_its_own_clauses():
    """USER RULING (2026-09-25): in "catch and release; bait ban, June 15-Oct 31" the date is the
    bait ban's. A clause on the other side of a `;` carries no date the line prints only across
    it — in either direction."""
    w = {"dates": [{"from_month": 6, "from_day": 15, "to_month": 10, "to_day": 31}]}
    text = "Trout/char catch and release; bait ban, June 15-Oct 31"
    cr = _r(rule_id="s.r1", type="retention_limit", verbatim="Trout/char catch and release",
            species=["TROUT_CHAR"], take=0, may_target=True)
    bait = _r(rule_id="s.r2", type="bait_restriction", verbatim="bait ban, June 15-Oct 31",
              gear=[{"slot": "bait", "ban": ["any_bait"]}], when=w)
    CatalogueEntry.model_validate(_entry(text, cr, bait))
    with pytest.raises(ValueError, match="printed in another clause of its line, across a `;`"):
        CatalogueEntry.model_validate(_entry(text, dict(cr, when=w), bait))
    # the other direction: "No Fishing Aug 1-Oct 31; bait ban" (Diana Creek)
    aug = {"dates": [{"from_month": 8, "from_day": 1, "to_month": 10, "to_day": 31}]}
    text = "No Fishing Aug 1-Oct 31; bait ban"
    shut = _r(rule_id="d.r1", type="retention_limit", verbatim="No Fishing Aug 1-Oct 31",
              species=["ALL_GAME_FISH"], take=0, may_target=False, when=aug)
    ban = _r(rule_id="d.r2", type="bait_restriction", verbatim="bait ban",
             gear=[{"slot": "bait", "ban": ["any_bait"]}])
    CatalogueEntry.model_validate(_entry(text, shut, ban))
    with pytest.raises(ValueError, match="across a `;`"):
        CatalogueEntry.model_validate(_entry(text, shut, dict(ban, when=aug)))
    # "A and B, date" is still one clause: the date governs both (no `;` between them)
    text = "Trout/char catch and release and bait ban, June 15-Aug 31"
    w2 = {"dates": [{"from_month": 6, "from_day": 15, "to_month": 8, "to_day": 31}]}
    CatalogueEntry.model_validate(_entry(text, dict(cr, when=w2), dict(
        bait, verbatim="bait ban, June 15-Aug 31", when=w2)))


def test_coquihallas_fly_only_is_not_dated_by_the_bait_bans_season(raw):
    """p.22, COQUIHALLA RIVER, printed line 2: "Fly fishing only; bait ban upstream of the northern
    entrance to the upper most railway tunnel, Jul 1-Oct 31". The date is the bait ban's."""
    ce = CatalogueEntry.model_validate(raw["r2:coquihalla_river@2-17"])
    rules = {r.rule_id: r for r in ce.rules}
    assert rules["coquihalla_river.r2"].verbatim == "Fly fishing only"
    assert rules["coquihalla_river.r2"].when is None
    assert rules["coquihalla_river.r3"].when.words() == "Jul 1-Oct 31"
    # MUTATION: spread the date back across the `;` and the model refuses the row
    broken = copy.deepcopy(raw["r2:coquihalla_river@2-17"])
    for r in broken["rules"]:
        if r["rule_id"] == "coquihalla_river.r2":
            r["when"] = {"dates": [{"from_month": 7, "from_day": 1, "to_month": 10,
                                    "to_day": 31}]}
    with pytest.raises(ValueError, match="coquihalla_river.r2: its `when` .* across a `;`"):
        CatalogueEntry.model_validate(broken)
