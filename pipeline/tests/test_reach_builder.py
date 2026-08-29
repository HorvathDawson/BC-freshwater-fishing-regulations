"""Reach builder: totality, the policy decisions, and determinism.

The builder's whole job is that no rule can go missing and no failure can be silent, so
these tests are mostly about the boring guarantees rather than clever geometry — the
geometry is `test_review_reaches.py`'s job.
"""

from __future__ import annotations

import pytest

from pipeline.reach.classify import classify
from pipeline.reach.models import Diagnostic, Outcome, Reason, RuleBinding


class _Item:
    def __init__(self, sections=(), boundaries=()):
        self.section_ids = tuple(sections)
        self.boundaries = tuple(boundaries)


class _B:
    def __init__(self, bid, aliases=()):
        self.id = bid
        self.aliases = tuple(aliases)


def _rule(rid="r1", *, kind="closure", extents=None, locators=None):
    return {"rule_id": rid, "restriction_type": kind,
            "extents": extents if extents is not None else [{"op": "whole", "splits": []}],
            "unresolved_locators": locators or []}


def _reach(sections=(), unclassified=(), ambiguous=()):
    return {"sections": list(sections), "unclassified": list(unclassified),
            "ambiguous_cut": list(ambiguous), "waters": []}


REG = {"i1": _Item(["s1", "s2"], [_B("cut_a")])}


# --------------------------------------------------------------------------- #
# The invariants that make the output trustworthy
# --------------------------------------------------------------------------- #

def test_a_bound_rule_can_never_have_zero_sections():
    with pytest.raises(ValueError):
        RuleBinding("e", "r", Outcome.bound, ())


def test_an_unresolved_rule_can_never_lack_a_reason():
    with pytest.raises(ValueError):
        RuleBinding("e", "r", Outcome.unresolved, ())


def test_every_rule_gets_exactly_one_outcome():
    b, _ = classify("e", _rule(), [_reach(["s1"])], registry=REG, covered_ids=["i1"],
                    scope_clipped=False, entry_has_registry=True)
    assert b.outcome is Outcome.bound and b.reason is None


# --------------------------------------------------------------------------- #
# Policy: straddlers (doc 10 ㊴) — 40 rules / 46 pieces in the live corpus
# --------------------------------------------------------------------------- #

def test_an_unclassified_piece_is_NEVER_auto_included():
    """Measured: all 45 unclassified pieces in the live corpus are DETACHED, not straddling.
    Auto-including them for closures attached a 380 m stub hanging off Cowichan Lake to
    "No fishing, Cowichan Lake outlet to Greendale Trestle". The builder reports; the
    curator decides."""
    for kind in ("closure", "harvest", "gear_restriction", "vessel_restriction"):
        b, diags = classify("e", _rule(kind=kind), [_reach(["s1"], unclassified=["s9"])],
                            registry=REG, covered_ids=["i1"], scope_clipped=False,
                            entry_has_registry=True)
        assert "s9" not in b.sections, kind
        assert any(d.kind == "unclassified" for d in diags), kind


def test_an_ambiguous_cut_is_surfaced_not_swallowed():
    """One landmark at two measures has two honest readings; the resolver takes the lower.
    The Mitchell bug proved the lower can be wrong, so it must always be visible."""
    _, diags = classify("e", _rule(), [_reach(["s1"], ambiguous=[{"split_id": "x", "used": 1.0,
                                                                 "also_at": [9.0]}])],
                        registry=REG, covered_ids=["i1"], scope_clipped=False,
                        entry_has_registry=True)
    amb = [d for d in diags if d.kind == "ambiguous_cut"]
    assert amb and amb[0].payload["also_at"] == [9.0]


# --------------------------------------------------------------------------- #
# Policy: why a rule failed — the reason must be specific, never a bare None
# --------------------------------------------------------------------------- #

def test_no_extents_on_a_matched_entry_is_NOT_defaulted_to_whole():
    """The 12 real cases are 500 m bands and sign lines with no boundary to bind to.
    Defaulting to `whole` would apply a 500 m closure to an entire lake arm."""
    b, _ = classify("e", _rule(extents=[], locators=["500 m upstream of Causeway Road"]),
                    [], registry=REG, covered_ids=["i1"], scope_clipped=False,
                    entry_has_registry=True)
    assert b.outcome is Outcome.unresolved
    assert b.reason is Reason.no_extents
    assert b.sections == ()
    assert "Causeway" in b.detail


def test_no_extents_with_no_registry_is_reported_as_no_registry():
    b, _ = classify("e", _rule(extents=[]), [], registry=REG, covered_ids=[],
                    scope_clipped=False, entry_has_registry=False)
    assert b.reason is Reason.no_registry


def test_zero_section_item_gets_its_own_reason():
    reg = {"i0": _Item([])}
    b, _ = classify("e", _rule(), [None], registry=reg, covered_ids=["i0"],
                    scope_clipped=False, entry_has_registry=True)
    assert b.reason is Reason.no_sections_for_items


def test_a_cut_that_is_not_on_the_water_is_named():
    b, _ = classify("e", _rule(extents=[{"op": "upstream_of", "splits": ["ghost"]}]),
                    [None], registry=REG, covered_ids=["i1"], scope_clipped=False,
                    entry_has_registry=True)
    assert b.reason is Reason.cut_not_found
    assert "ghost" in b.detail


def test_a_cut_bound_by_alias_counts_as_found():
    reg = {"i1": _Item(["s1"], [_B("real", aliases=["split:aka"])])}
    b, _ = classify("e", _rule(extents=[{"op": "upstream_of", "splits": ["aka"]}]),
                    [None], registry=reg, covered_ids=["i1"], scope_clipped=False,
                    entry_has_registry=True)
    assert b.reason is not Reason.cut_not_found


def test_within_area_is_distinguished_from_a_dangling_area_id():
    b1, _ = classify("e", _rule(extents=[{"op": "within", "area_id": "area:known"}]),
                     [None], registry={"area:known": _Item()}, covered_ids=["i1"],
                     scope_clipped=False, entry_has_registry=True)
    assert b1.reason is Reason.area_scope
    b2, _ = classify("e", _rule(extents=[{"op": "within", "area_id": "nope"}]),
                     [None], registry=REG, covered_ids=["i1"], scope_clipped=False,
                     entry_has_registry=True)
    assert b2.reason is Reason.area_id_dangling


def test_empty_after_the_row_scope_is_its_own_reason():
    """The Peace case: the rule resolves, then the row's own scope removes it all.
    That is a curation inconsistency, not an unresolvable extent."""
    b, _ = classify("e", _rule(), [_reach([])], registry=REG, covered_ids=["i1"],
                    scope_clipped=True, entry_has_registry=True)
    assert b.reason is Reason.empty_after_scope


# --------------------------------------------------------------------------- #
# Policy: several extents
# --------------------------------------------------------------------------- #

def test_several_extents_are_unioned():
    b, _ = classify("e", _rule(extents=[{"op": "whole"}, {"op": "whole"}]),
                    [_reach(["s1"]), _reach(["s2"])], registry=REG, covered_ids=["i1"],
                    scope_clipped=False, entry_has_registry=True)
    assert set(b.sections) == {"s1", "s2"}


def test_a_partly_resolved_rule_binds_what_resolved_and_says_so():
    """Dropping the whole rule would under-apply a regulation that is partly known."""
    b, diags = classify("e", _rule(extents=[{"op": "whole"}, {"op": "whole"}]),
                        [_reach(["s1"]), None], registry=REG, covered_ids=["i1"],
                        scope_clipped=False, entry_has_registry=True)
    assert b.outcome is Outcome.bound and set(b.sections) == {"s1"}
    assert any(d.kind == "partial_extents" for d in diags)


def test_sections_are_sorted_so_output_is_deterministic():
    b, _ = classify("e", _rule(), [_reach(["s9", "s1", "s5"])], registry=REG,
                    covered_ids=["i1"], scope_clipped=False, entry_has_registry=True)
    assert list(b.sections) == sorted(b.sections)


# --------------------------------------------------------------------------- #
# IO + diff: determinism, and "did my confirmed work change?"
# --------------------------------------------------------------------------- #

def _result(rows):
    """rows: [(entry_id, rule_id, sections|None, reason)] -> a ReachResult-alike."""
    from pipeline.reach.build import ReachResult
    from pipeline.reach.models import BuildReport
    bindings = []
    for eid, rid, secs, reason in rows:
        if secs:
            bindings.append(RuleBinding(eid, rid, Outcome.bound, tuple(secs)))
        else:
            bindings.append(RuleBinding(eid, rid, Outcome.unresolved, (), reason))
    rep = BuildReport(n_rules=len(bindings))
    for b in bindings:
        rep.outcomes[b.outcome.value] = rep.outcomes.get(b.outcome.value, 0) + 1
    return ReachResult(bindings, [], rep)


def test_a_run_is_byte_identical_when_written_twice(tmp_path):
    from pipeline.reach.io import write_run
    res = _result([("e1", "r1", ["s2", "s1"], None), ("e2", "r1", None, Reason.no_registry)])
    write_run(tmp_path / "a", res, [])
    write_run(tmp_path / "b", res, [])
    for name in ("rule_section", "rule_unresolved", "rule_extent", "rule_diagnostic"):
        a = (tmp_path / "a" / f"{name}.jsonl").read_bytes()
        b = (tmp_path / "b" / f"{name}.jsonl").read_bytes()
        assert a == b, name


def test_the_digest_ignores_timing_but_tracks_sections(tmp_path):
    from pipeline.reach.io import digest
    base = _result([("e1", "r1", ["s1"], None)])
    same = _result([("e1", "r1", ["s1"], None)])
    moved = _result([("e1", "r1", ["s2"], None)])
    base.report.seconds = 9.9        # timing must not affect the digest
    assert digest(base) == digest(same)
    assert digest(base) != digest(moved)


def test_diff_reports_gained_and_lost_sections(tmp_path):
    from pipeline.reach.diff import diff_runs
    from pipeline.reach.io import write_run
    write_run(tmp_path / "before", _result([("e1", "r1", ["s1", "s2"], None)]), [])
    rep = diff_runs(tmp_path / "before", _result([("e1", "r1", ["s2", "s3"], None)]))
    assert len(rep.changes) == 1
    c = rep.changes[0]
    assert c.added == ("s3",) and c.removed == ("s1",) and c.kind == "rebound"


def test_diff_flags_a_change_inside_a_CONFIRMED_entry_first(tmp_path):
    """A small change to signed-off work outranks a large change to unreviewed work —
    the curator's question is 'does what I confirmed still mean what I confirmed?'"""
    from pipeline.reach.diff import diff_runs
    from pipeline.reach.io import write_run
    before = _result([("locked_e", "r1", ["s1"], None), ("open_e", "r1", ["a"], None)])
    after = _result([("locked_e", "r1", ["s1", "s2"], None),
                     ("open_e", "r1", ["a", "b", "c", "d", "e", "f"], None)])
    write_run(tmp_path / "before", before, [])
    rep = diff_runs(tmp_path / "before", after, locked_ids={"locked_e"})
    assert rep.changes[0].entry_id == "locked_e"      # despite a smaller blast radius
    assert len(rep.locked_changes) == 1
    assert "CONFIRMED" in rep.summary()


def test_diff_notices_an_outcome_flip_not_just_section_churn(tmp_path):
    from pipeline.reach.diff import diff_runs
    from pipeline.reach.io import write_run
    write_run(tmp_path / "before", _result([("e1", "r1", ["s1"], None)]), [])
    rep = diff_runs(tmp_path / "before",
                    _result([("e1", "r1", None, Reason.cut_not_found)]))
    assert rep.changes[0].kind == "outcome"
    assert "cut_not_found" in rep.changes[0].after


# --------------------------------------------------------------------------- #
# Cache: the key must cover everything that can change an answer
# --------------------------------------------------------------------------- #

def _entry(**over):
    e = {"entry_id": "e1", "matched": ["i1"], "scope": [], "tributaries": {},
         "rules": [{"rule_id": "r1", "restriction_type": "closure",
                    "extents": [{"op": "whole", "splits": []}]}]}
    e.update(over)
    return e


def test_cache_key_changes_when_the_extent_changes():
    from pipeline.reach.cache import entry_key
    a = _entry()
    b = _entry(rules=[{"rule_id": "r1", "restriction_type": "closure",
                       "extents": [{"op": "upstream_of", "splits": ["x"]}]}])
    assert entry_key(a, "full") != entry_key(b, "full")


def test_cache_key_changes_with_the_BUILD():
    """Section ids churn ~6% per rebuild, so the same entry resolves differently."""
    from pipeline.reach.cache import entry_key
    assert entry_key(_entry(), "full") != entry_key(_entry(), "full_new")


def test_cache_key_changes_with_the_CLASSIFIER_POLICY():
    """A straddler-policy change alters outcomes without touching entry or build."""
    from pipeline.reach.cache import entry_key
    assert entry_key(_entry(), "full", "1") != entry_key(_entry(), "full", "2")


def test_cache_key_changes_with_matched_and_tributary_flags():
    from pipeline.reach.cache import entry_key
    base = entry_key(_entry(), "full")
    assert entry_key(_entry(matched=["i2"]), "full") != base
    assert entry_key(_entry(tributaries={"included": True}), "full") != base


def test_cache_key_IGNORES_curation_metadata():
    """`reviewed_at` changes on every save; keying on it would evict constantly for
    answers that did not move."""
    from pipeline.reach.cache import entry_key
    assert entry_key(_entry(reviewed_at="x", revisit_note="n", locked=True), "full") \
        == entry_key(_entry(reviewed_at="y", revisit_note="m", locked=False), "full")


def test_cache_serves_a_hit_and_recomputes_after_invalidate():
    from pipeline.reach.cache import ReachCache
    c = ReachCache("full")
    calls = {"n": 0}

    def compute():
        calls["n"] += 1
        return ([], [])

    e = _entry()
    c.get_or_compute(e, compute)
    c.get_or_compute(e, compute)
    assert calls["n"] == 1 and c.hits == 1 and c.misses == 1
    c.invalidate(e)
    c.get_or_compute(e, compute)
    assert calls["n"] == 2


def test_tributaries_only_with_no_tributaries_is_unresolved_not_an_empty_bind():
    """A 'tributaries only' rule whose reach has no tributaries selects NOTHING. Shipping
    that as a bound rule covering zero water is a silent drop."""
    b, _ = classify("e", _rule(), [_reach(["s1"])], registry=REG, covered_ids=["i1"],
                    scope_clipped=False, entry_has_registry=True,
                    tributaries=True, tributaries_only=True,
                    expand_tributaries=lambda reach, only=False: set())
    assert b.outcome is Outcome.unresolved and b.reason is Reason.no_tributaries


def test_a_tributary_rule_binds_the_expanded_set_and_reports_the_growth():
    b, diags = classify("e", _rule(), [_reach(["s1"])], registry=REG, covered_ids=["i1"],
                        scope_clipped=False, entry_has_registry=True, tributaries=True,
                        expand_tributaries=lambda reach, only=False: set(reach) | {"t1", "t2"})
    assert set(b.sections) == {"s1", "t1", "t2"}
    assert not b.tributaries_pending, "expanded rules are complete, not pending"
    d = [x for x in diags if x.kind == "tributaries"][0]
    assert d.payload["direct"] == 1 and d.payload["added"] == 2


def test_without_an_expander_a_tributary_rule_is_flagged_INCOMPLETE():
    """554 rules extend to tributaries. Binding only their direct sections and looking
    identical to a fully-resolved rule is silent under-application."""
    b, diags = classify("e", _rule(), [_reach(["s1"])], registry=REG, covered_ids=["i1"],
                        scope_clipped=False, entry_has_registry=True, tributaries=True)
    assert b.tributaries_pending is True
    assert any(d.kind == "tributaries_pending" for d in diags)


def test_policy_version_is_bumped_when_classify_changes():
    """A stale POLICY_VERSION serves confidently wrong cached answers. This pins the
    classifier's observable policy so the constant cannot silently fall behind."""
    from pipeline.reach import cache, classify
    policy = {
        "straddlers_included_for": sorted(classify.STRADDLERS_INCLUDED_FOR),
        "ambiguous_cut_is_fatal": classify.AMBIGUOUS_CUT_IS_FATAL,
        "partial_extents_bind": classify.PARTIAL_EXTENTS_BIND,
    }
    expected = {"straddlers_included_for": [], "ambiguous_cut_is_fatal": False,
                "partial_extents_bind": True}
    assert policy == expected, (
        f"classify.py policy changed to {policy} — bump cache.POLICY_VERSION "
        f"(currently {cache.POLICY_VERSION!r}) and update this test together")
