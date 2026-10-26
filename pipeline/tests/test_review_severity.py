"""The entry's severity (owner 2026-10-26): one chip — is there a REAL issue to review — its
reasons, and the queue's first order."""
import pytest

from pipeline.regs.review import precheck as P
from pipeline.regs.review import severity as S


def f(code, rid="x.r1"):
    return {"code": code, "severity": P.CHECKS.get(code, ("A", ""))[0], "rule_id": rid, "what": ""}


@pytest.mark.parametrize("code", ["gate", "binds_nothing", "unresolved", "unplaced_pieces",
                                  "ambiguous_cut", "parser_uncertain", "unresolved_locator",
                                  "undrawn_part"])
def test_the_real_issues_are_high(code):
    assert S.entry_severity([f(code)])["level"] == "high"


def test_the_owners_tiers_whole():
    """High: parser-uncertain, missing splits (a named undrawn part, a locator nothing draws),
    unresolved, ambiguous cut, binding nothing, unused curated cuts. Medium: cut risks that are
    real. Low: facts and hints. Every pre-check code has a level (a new one is high until said)."""
    assert set(P.CHECKS) <= set(S.CODE_LEVEL)
    assert {c: lv for c, (lv, _) in S.CODE_LEVEL.items() if c in P.CHECKS} == {
        "gate": "high", "binds_nothing": "high", "unresolved": "high", "unplaced_pieces": "high",
        "ambiguous_cut": "high", "split_to_verify": "medium", "parser_uncertain": "high",
        "unresolved_locator": "high", "undrawn_part": "high", "cut_picked": "low",
        "quote_not_in_row": "medium", "unquoted_text": "low"}
    assert S.entry_severity([], unused_splits=2) == {
        "level": "high", "reasons": ["high: unused curated cut ×2"]}
    assert S.entry_severity([f("brand_new_check")])["level"] == "high"


def test_noise_is_low_and_nothing_is_none():
    assert S.entry_severity([], risks=["cut", "several waters"]) == {
        "level": "low", "reasons": ["low: cuts its water", "low: several waters"]}
    assert S.entry_severity([f("unquoted_text", None), f("cut_picked")])["level"] == "low"
    # a risk factor that is a real issue arrives as its pre-check, never from the risk list
    assert S.entry_severity([], risks=["no match", "unresolved", "flag"]) == {
        "level": "none", "reasons": []}


def test_the_reasons_most_severe_first_each_once_with_its_rules():
    got = S.entry_severity([f("unquoted_text", None), f("parser_uncertain", "a.r2"),
                            f("split_to_verify"), f("parser_uncertain", "a.r1")],
                           risks=["cut"], unused_splits=1)
    assert got == {"level": "high", "reasons": [
        "high: parser-uncertain ×2 (a.r1, a.r2)", "high: unused curated cut",
        "medium: split to verify (x.r1)", "low: unquoted row text", "low: cuts its water"]}


def test_the_order_and_the_filter():
    assert sorted(["none", "low", "high", "medium"], key=S.rank) == ["high", "medium", "low", "none"]
    assert [lv for lv in S.LEVELS if S.matches(lv, "issues")] == ["high", "medium"]
    assert all(S.matches(lv, None) and S.matches(lv, "") for lv in S.LEVELS)
    assert [lv for lv in S.LEVELS if S.matches(lv, "low")] == ["low"]


def test_the_order_is_pinned_by_mutation(monkeypatch):
    """Mutant: a parser flag read as a hint -> the entry is no longer high."""
    monkeypatch.setitem(S.CODE_LEVEL, "parser_uncertain", ("low", "parser-uncertain"))
    assert S.entry_severity([f("parser_uncertain")])["level"] == "low"


def test_the_corpus_severities_measured():
    """Measured 2026-10-26 on the catalogue alone (no reach run, no unused-cut count, no split to
    verify): 137 high (mostly parser flags and named undrawn parts), 353 low (cuts, several waters,
    unquoted text), 1025 none. With the run, its unresolved / ambiguous / unplaced rules join high."""
    from collections import Counter

    from pipeline.regs.review import tiers as T
    from pipeline.tests.test_review_screen import _corpus
    got = Counter()
    for e in _corpus():
        b = bool(e.get("matched"))
        got[S.entry_severity(P.prechecks(e, binds=b), risks=T.risks(e, binds=b))["level"]] += 1
    for lv, want in {"high": 137, "low": 353, "none": 1025}.items():
        assert abs(got[lv] - want) <= max(3, want // 20), (lv, got[lv], want)
