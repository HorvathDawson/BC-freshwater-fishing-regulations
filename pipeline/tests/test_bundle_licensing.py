"""Licensing, placed by the reach builder and written into the bundle — and the checks that stop a
bundle shipping a ruleset or a licensing set that names something it does not hold.

Every test here builds a real (tiny) bundle from `schema.sql` and a real reach-run directory, and
asserts against the ROWS WRITTEN — never against the input they were written from (a check
seeded from its input launders; see AGENTS and the table-is-a-ledger note).
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from pipeline.atlas.reach import licensing as reach_lic
from pipeline.atlas.reach.io import write_run
from pipeline.atlas.reach.licensing import LicensingPlacement, is_province_wide, place_record
from pipeline.atlas.reach.models import Outcome, Reason, RuleBinding
from pipeline.deliver.bundle import licensing as bundle_lic
from pipeline.deliver.bundle import rules as bundle_rules
from pipeline.deliver.bundle.build import SCHEMA
from pipeline.regs.parsing.catalogue import CatalogueEntry

# --------------------------------------------------------------------------- the reach side


def _binder(sections=("a:1", "a:2"), trib=(), pending=False, fail=None):
    """A stand-in for `build_reach`, recording what it was asked."""
    asked = []

    def reach(rule):
        asked.append(rule)
        if fail:
            return RuleBinding("e", rule["rule_id"], Outcome.unresolved, (), fail, "why",
                               tributaries_pending=pending), []
        return RuleBinding("e", rule["rule_id"], Outcome.bound, tuple(sections),
                           tributaries_pending=pending, via_tributary=tuple(trib)), []
    return reach, asked


ENTRY = {"entry_id": "r4:elk@4-2", "extents": [{"op": "upstream_of", "splits": ["x"]}],
         "includes_tributaries": True}


def test_a_record_with_no_extents_inherits_its_entrys_never_whole():
    reach, asked = _binder()
    p, _ = place_record(ENTRY, {"kind": "designation", "id": "elk"}, reach)
    assert p.placement == "sections" and asked[0]["extents"] == ENTRY["extents"]
    # …and on an entry with none it is handed NOTHING — the builder then says `no_extents`
    reach, asked = _binder(fail=Reason.no_extents)
    p, _ = place_record({"entry_id": "zp:x"}, {"kind": "designation", "id": "d"}, reach)
    assert asked[0]["extents"] == [] and p.placement == "unresolved" and p.reason == "no_extents"


def test_only_what_resolution_reads_reaches_the_resolver():
    reach, asked = _binder()
    place_record(ENTRY, {"kind": "designation", "id": "elk", "tributaries_only": True,
                         "includes_tributaries": None, "unit": "elk_river",
                         "tributary_excludes": [{"op": "whole", "item_id": "gnis:1"}]}, reach)
    assert set(asked[0]) == {"rule_id", "type", "extents", "includes_tributaries",
                             "tributaries_only", "tributary_excludes"}
    assert asked[0]["tributaries_only"] is True and asked[0]["tributary_excludes"]


def test_province_wide_and_on_designation_requirements_bind_no_section():
    reach, asked = _binder()
    p, _ = place_record(ENTRY, {"kind": "requirement", "id": "cwl", "on": "classified_period"},
                        reach)
    assert p.placement == "on_designation" and not asked
    p, _ = place_record(ENTRY, {"kind": "requirement", "id": "basic",
                                "extents": [{"op": "within", "area_kind": "region"}]}, reach)
    assert p.placement == "province" and not asked
    # narrower than the province is a PLACE, resolved like any other
    assert not is_province_wide([{"op": "within", "area_kind": "region", "feature_types": ["stream"]}])
    assert not is_province_wide([{"op": "within", "area_kind": "national_parks"}])
    # a designation is never province-wide by shape; it goes to the resolver
    place_record(ENTRY, {"kind": "designation", "id": "d",
                         "extents": [{"op": "within", "area_kind": "region"}]}, reach)
    assert asked


def test_a_placement_is_never_bound_and_empty_or_unresolved_and_unexplained():
    with pytest.raises(ValueError):
        LicensingPlacement("e", "r", "designation", "sections")
    with pytest.raises(ValueError):
        LicensingPlacement("e", "r", "designation", "unresolved")
    with pytest.raises(ValueError):
        LicensingPlacement("e", "r", "designation", "province", sections=("a:1",))


class _Result:
    bindings, diagnostics, report = [], [], None


def test_the_run_writes_trib_pending_never_a_clean_binding(tmp_path):
    """AGENTS 15: a walk not done is never complete, so every section of a pending binding says
    so — even the direct ones."""
    from pipeline.atlas.reach.models import BuildReport
    r = _Result()
    r.report = BuildReport()
    r.licensing = [
        LicensingPlacement("e1", "d", "designation", "sections", sections=("a:1", "a:2"),
                           via_tributary=("a:2",)),
        LicensingPlacement("e2", "d", "designation", "sections", sections=("b:1",),
                           tributaries_pending=True),
        LicensingPlacement("e3", "q", "requirement", "province"),
    ]
    r.licensing_diagnostics = []
    write_run(tmp_path, r, [])
    rows = [json.loads(x) for x in (tmp_path / "licensing_section.jsonl").read_text().splitlines()]
    assert [(x["entry_id"], x["section_id"], x["scope"]) for x in rows] == [
        ("e1", "a:1", "reach"), ("e1", "a:2", "trib"), ("e2", "b:1", "trib_pending")]
    placed = [json.loads(x) for x in
              (tmp_path / "licensing_placement.jsonl").read_text().splitlines()]
    assert [p["placement"] for p in placed] == ["sections", "sections", "province"]
    rep = json.loads((tmp_path / "report.json").read_text())
    assert rep["licensing_digest"] and rep["digest"]


# --------------------------------------------------------------------------- the bundle side

V_DES = "ALL tributaries are Class II waters when open"
V_NOT = "Part described is NOT a Classified Water"
V_REQ = "A permit is required for fishing on all waters within the Creston Valley WMA."


def _entries():
    elk = CatalogueEntry.model_validate({
        "entry_id": "r4:elk@4-2", "name": "ELK", "regs_verbatim": V_DES, "matched": ["gnis:1"],
        "licensing": [{"kind": "designation", "id": "elk_river", "classified": "II",
                       "unit": "elk_river", "unit_name": "Elk River", "verbatim": V_DES,
                       "review_reason": "the Coal Creek carve-out is not drawn"}]})
    coal = CatalogueEntry.model_validate({
        "entry_id": "r4:coal@4-23", "name": "COAL", "regs_verbatim": V_NOT, "matched": ["gnis:2"],
        "licensing": [{"kind": "not_classified", "id": "not_classified", "verbatim": V_NOT}]})
    cv = CatalogueEntry.model_validate({
        "entry_id": "z4:cv", "name": "CV", "regs_verbatim": V_REQ,
        "licensing": [{"kind": "requirement", "id": "permit", "verbatim": V_REQ,
                       "doing": {"act": "fishing"},
                       "satisfied_by": [{"hold": ["creston_valley_wma_permit"]}],
                       "extents": [{"op": "within", "area_id": "area:wma:cv"}]}]})
    return [elk, coal, cv]


def _run(tmp: Path, sections: list[tuple], placements: list[tuple]) -> Path:
    run = tmp / "run"
    run.mkdir()
    (run / "licensing_placement.jsonl").write_text("".join(
        json.dumps({"entry_id": e, "record_id": r, "kind": k, "placement": p, "reason": why,
                    "detail": "", "tributaries_pending": False}) + "\n"
        for e, r, k, p, why in placements))
    (run / "licensing_section.jsonl").write_text("".join(
        json.dumps({"entry_id": e, "record_id": r, "kind": k, "section_id": s, "scope": v}) + "\n"
        for e, r, k, s, v in sections))
    return run


PLACED = [("r4:elk@4-2", "elk_river", "designation", "sections", None),
          ("r4:coal@4-23", "not_classified", "not_classified", "sections", None),
          ("z4:cv", "permit", "requirement", "unresolved", "no_sections_for_items")]
SID = {"s:1": 1, "s:2": 2, "s:3": 3}


class _Cov:
    def __init__(self):
        self.rows = {}

    def filled(self, t, n):
        self.rows[t] = n


def _write(tmp, sections, placements=PLACED, entries=None):
    db = sqlite3.connect(":memory:")
    db.executescript(SCHEMA.read_text())
    bundle_lic.write(db, _run(tmp, sections, placements), entries or _entries(), _Cov(), SID)
    return db


def test_an_unlisted_classified_and_not_classified_section_fails_the_build(tmp_path, monkeypatch):
    """Design validator 7: one of the two is wrong, and the build says which two."""
    monkeypatch.setattr(bundle_lic, "ACKNOWLEDGED_CONFLICTS", {})
    with pytest.raises(SystemExit, match=r"r4:elk@4-2#elk_river\s+vs\s+not_classified "
                                         r"r4:coal@4-23#not_classified"):
        _write(tmp_path, [("r4:elk@4-2", "elk_river", "designation", "s:1", "trib"),
                          ("r4:coal@4-23", "not_classified", "not_classified", "s:1", "reach")])


def test_an_acknowledged_conflict_ships_contested_and_a_stale_one_fails(tmp_path, monkeypatch):
    pair = (("r4:elk@4-2", "elk_river"), ("r4:coal@4-23", "not_classified"))
    monkeypatch.setattr(bundle_lic, "ACKNOWLEDGED_CONFLICTS", {pair: "not drawn"})
    db = _write(tmp_path, [("r4:elk@4-2", "elk_river", "designation", "s:1", "trib"),
                           ("r4:elk@4-2", "elk_river", "designation", "s:2", "trib"),
                           ("r4:coal@4-23", "not_classified", "not_classified", "s:1", "reach")])
    got = dict(db.execute("SELECT sid, via FROM designation_section").fetchall())
    assert got == {1: "contested", 2: "trib"}           # only where BOTH bind
    assert db.execute("SELECT via FROM not_classified_section").fetchall() == [("reach",)]
    # once the curator draws the carve-out, the listed pair no longer conflicts — and the list
    # must not outlive its reason
    (tmp_path / "b").mkdir()
    with pytest.raises(SystemExit, match="no longer conflict"):
        _write(tmp_path / "b", [("r4:elk@4-2", "elk_river", "designation", "s:2", "trib"),
                                ("r4:coal@4-23", "not_classified", "not_classified", "s:1",
                                 "reach")])


def test_an_acknowledged_conflict_needs_the_curator_told(tmp_path, monkeypatch):
    pair = (("r4:elk@4-2", "elk_river"), ("r4:coal@4-23", "not_classified"))
    monkeypatch.setattr(bundle_lic, "ACKNOWLEDGED_CONFLICTS", {pair: "not drawn"})
    es = _entries()
    raw = es[0].model_dump(by_alias=True, exclude_none=True)
    raw["licensing"][0]["review_reason"] = ""
    es[0] = CatalogueEntry.model_validate(raw)
    with pytest.raises(SystemExit, match="no review_reason"):
        _write(tmp_path, [("r4:elk@4-2", "elk_river", "designation", "s:1", "trib"),
                          ("r4:coal@4-23", "not_classified", "not_classified", "s:1", "reach")],
               entries=es)


def test_records_ship_with_label_quote_structure_and_placement(tmp_path, monkeypatch):
    monkeypatch.setattr(bundle_lic, "ACKNOWLEDGED_CONFLICTS", {})
    db = _write(tmp_path, [("r4:elk@4-2", "elk_river", "designation", "s:2", "trib")])
    (label, verbatim, record, placement, uncertain) = db.execute(
        "SELECT label, verbatim, record, placement, uncertain FROM designation").fetchone()
    assert label == "Class II Classified Water (licence unit: Elk River)."
    assert verbatim == V_DES and placement == "sections" and uncertain == 0
    assert json.loads(record)["unit_name"] == "Elk River"
    # an unplaced requirement is SHIPPED, uncertain, with its reason — never dropped
    assert db.execute("SELECT placement, uncertain, unresolved FROM requirement").fetchone() == (
        "unresolved", 1, "no_sections_for_items: ")
    assert db.execute("SELECT count(*) FROM licence").fetchone()[0] > 10


def test_the_run_and_the_corpus_must_agree_both_ways(tmp_path, monkeypatch):
    monkeypatch.setattr(bundle_lic, "ACKNOWLEDGED_CONFLICTS", {})
    with pytest.raises(SystemExit, match="never placed"):
        _write(tmp_path, [], placements=PLACED[:2])
    (tmp_path / "b").mkdir()
    with pytest.raises(SystemExit, match="no longer has"):
        _write(tmp_path / "b", [],
               placements=PLACED + [("r9:gone@9-1", "x", "designation", "sections", None)])


def test_a_bound_section_the_atlas_does_not_know_stops_the_build(tmp_path, monkeypatch):
    monkeypatch.setattr(bundle_lic, "ACKNOWLEDGED_CONFLICTS", {})
    with pytest.raises(SystemExit, match="not in the handle"):
        _write(tmp_path, [("r4:elk@4-2", "elk_river", "designation", "s:404", "reach")])


def test_a_run_from_before_licensing_is_refused(tmp_path):
    db = sqlite3.connect(":memory:")
    db.executescript(SCHEMA.read_text())
    (tmp_path / "old").mkdir()
    with pytest.raises(SystemExit, match="predates them"):
        bundle_lic.write(db, tmp_path / "old", _entries(), _Cov(), SID)


# --------------------------------------------------------------------------- the ruleset check

RULE_V = "Trout daily quota = 2"


def _rules_fixture(tmp: Path, bound: list[tuple], unresolved: list[tuple]):
    entries = tmp / "entries"
    entries.mkdir()
    (entries / "region-1.json").write_text(json.dumps({"region": "1", "entries": [{
        "entry_id": "r1:x@1-1", "name": "X", "regs_verbatim": RULE_V, "matched": ["gnis:1"],
        "rules": [{"rule_id": "x.r1", "type": "retention_limit", "species": ["RB"], "take": 2,
                   "verbatim": RULE_V, "extents": [{"op": "whole"}]}]}]}))
    run = tmp / "run"
    run.mkdir()
    (run / "rule_section.jsonl").write_text("".join(
        json.dumps({"entry_id": e, "rule_id": r, "section_id": s, "scope": "reach"}) + "\n"
        for e, r, s in bound))
    (run / "rule_unresolved.jsonl").write_text("".join(
        json.dumps({"entry_id": e, "rule_id": r, "reason": "no_extents", "detail": ""}) + "\n"
        for e, r in unresolved))
    # no licensing in this corpus, but the run must still say so
    (run / "licensing_placement.jsonl").write_text("")
    (run / "licensing_section.jsonl").write_text("")
    build = tmp / "atlas"
    build.mkdir()
    (build / "section_handles.txt").write_text("s:1\ns:2\n")
    db = sqlite3.connect(":memory:")
    db.executescript(SCHEMA.read_text())
    return db, run, entries, build


def test_a_stale_reach_run_cannot_ship_a_ruleset_naming_a_rule_that_is_gone(tmp_path):
    """The bundle's ruleset named 144 rules the `rule` table did not have. The reader's JOIN
    dropped them without a word."""
    db, run, entries, build = _rules_fixture(
        tmp_path, [("r1:x@1-1", "x.r1", "s:1"), ("r1:x@1-1", "x.r9_removed", "s:1")], [])
    with pytest.raises(SystemExit, match="no longer has"):
        bundle_rules.write(db, run, entries, _Cov(), build_dir=build)


def test_a_rule_the_run_never_saw_is_refused_too(tmp_path):
    db, run, entries, build = _rules_fixture(tmp_path, [], [])
    with pytest.raises(SystemExit, match="never saw 1"):
        bundle_rules.write(db, run, entries, _Cov(), build_dir=build)


def test_a_current_run_writes_and_every_ruleset_row_names_a_rule(tmp_path, monkeypatch):
    monkeypatch.setattr(bundle_lic, "ACKNOWLEDGED_CONFLICTS", {})
    db, run, entries, build = _rules_fixture(tmp_path, [("r1:x@1-1", "x.r1", "s:2")], [])
    bundle_rules.write(db, run, entries, _Cov(), build_dir=build)
    assert db.execute("SELECT count(*) FROM ruleset rs LEFT JOIN rule r USING(entry_id, rule_id)"
                      " WHERE r.rule_id IS NULL").fetchone() == (0,)
    assert db.execute("SELECT sid FROM section_ruleset").fetchall() == [(2,)]


def test_the_written_ruleset_is_checked_not_just_the_input(tmp_path, monkeypatch):
    """Mutation pin: if interning ever emitted a rule the corpus does not have, the check over
    the WRITTEN rows is what stops it — the input-side check would not see it."""
    db, run, entries, build = _rules_fixture(tmp_path, [("r1:x@1-1", "x.r1", "s:2")], [])
    real = bundle_rules.intern_sets
    monkeypatch.setattr(bundle_rules, "intern_sets", lambda rows: (
        lambda ss: (ss[0], [s + [("r1:x@1-1", "ghost", "reach")] for s in ss[1]]))(real(rows)))
    with pytest.raises(SystemExit, match="ruleset names rules that are not in `rule`"):
        bundle_rules.write(db, run, entries, _Cov(), build_dir=build)


def test_placed_kinds_are_the_four_about_a_place():
    assert reach_lic.PLACED_KINDS == bundle_lic.PLACED == (
        "designation", "requirement", "not_classified", "alternative")


# ------------------------------------------------------ a water's own designation beats a walk's

def _P(eid, rid, sections, trib=(), kind="designation", pending=False):
    return reach_lic.LicensingPlacement(eid, rid, kind, "sections", sections=tuple(sections),
                                        via_tributary=tuple(trib), tributaries_pending=pending)


def _D(rid="x", **kw):
    """A designation record. All year, no stamp clause, unless said."""
    from pipeline.regs.parsing.catalogue import Designation
    return Designation.model_validate({"id": rid, "classified": "II", "unit": rid,
                                       "unit_name": rid,
                                       "verbatim": "Class II water Mar 1-May 31, Apr 1-Apr 30",
                                       "review_reason": "test", **kw})


def _recs(*ps, **over):
    return {(p.entry_id, p.record_id): over.get(p.record_id, _D(p.record_id)) for p in ps}


def test_a_waters_own_designation_takes_its_sections_out_of_anothers_tributary_walk():
    """The Bulkley's walk reached the Suskwa, Class I with its own designation, and those sections
    carried both classes; the Elk's unit sat over Wigwam, Michel, Forsyth and Abruzzi, each with
    its own unit licence. A water's own designation wins — as a water's own rule does."""
    bulkley = _P("r6:bulkley", "bulkley", ["b1", "b2", "s1", "s2", "m1"], trib=["s1", "s2", "m1"])
    suskwa = _P("r6:suskwa", "suskwa", ["s1", "s2", "s3"], trib=["s3"])
    got, diags = reach_lic.own_beats_inherited([bulkley, suskwa], _recs(bulkley, suskwa))
    by = {p.record_id: p for p in got}
    assert by["bulkley"].sections == ("b1", "b2", "m1")      # Morice-like m1: nobody's own
    assert by["bulkley"].via_tributary == ("m1",)
    assert by["suskwa"] == suskwa                             # its own reach is never taken
    assert [(d.rule_id, d.kind, d.payload["removed"], d.payload["to"]) for d in diags] == [
        ("bulkley", "trib_yields_to_own", 2, ["r6:suskwa#suskwa"])]


def test_only_the_inherited_half_yields_and_only_to_a_designation():
    # both name the section by reach: stays ambiguous (validator 6 reports it), neither loses it
    a, b = _P("e:a", "a", ["x"]), _P("e:b", "b", ["x"])
    assert reach_lic.own_beats_inherited([a, b], _recs(a, b))[0] == [a, b]
    # a requirement or not_classified binding by reach takes nothing from a designation
    walk = _P("e:a", "a", ["r", "t"], trib=["t"])
    req = _P("e:q", "q", ["t"], kind="requirement")
    assert reach_lic.own_beats_inherited([walk, req], _recs(walk))[0] == [walk, req]
    # two walks meeting on a section: neither is the water's own, both keep it
    w1, w2 = _P("e:a", "a", ["r1", "t"], trib=["t"]), _P("e:b", "b", ["r2", "t"], trib=["t"])
    assert reach_lic.own_beats_inherited([w1, w2], _recs(w1, w2))[0] == [w1, w2]
    # a pending walk's direct sections are its own
    pend = _P("e:p", "p", ["t"], pending=True)
    got, _ = reach_lic.own_beats_inherited([walk, pend], _recs(walk, pend))
    assert got[0].sections == ("r",)


SPRING = {"dates": [{"from_month": 3, "from_day": 1, "to_month": 5, "to_day": 31}]}
APRIL = {"dates": [{"from_month": 4, "from_day": 1, "to_month": 4, "to_day": 30}]}
STAMP = "Steelhead Stamp mandatory Mar 1-May 31, Apr 1-Apr 30"


@pytest.mark.parametrize("own,inherited,why", [
    # a spring-only own designation under an all-year walk: nine months unclassified
    (dict(when=SPRING), {}, "period"),
    (dict(when={"unparsed": ["when open"]}), dict(when=SPRING), "period"),
    # the walk obliges the stamp, the own designation does not / only for part of it / waives it
    ({}, dict(steelhead_stamp_during={"when": SPRING, "verbatim": STAMP}),
     "stamp"),
    (dict(steelhead_stamp_during={"when": APRIL, "verbatim": STAMP}),
     dict(steelhead_stamp_during={"when": SPRING, "verbatim": STAMP}),
     "stamp"),
    (dict(steelhead_stamp_waived={"verbatim": "Steelhead Stamp not required"}), {}, "stamp"),
    # a Class I walk over a water whose own designation is only Class II
    ({}, dict(classified="I", verbatim="Class I water"), "class"),
])
def test_an_inherited_designation_yields_only_to_one_that_says_as_much(own, inherited, why):
    """The reviewer's guard: yielding drops the inherited designation from the section, so the
    own one must cover its period, oblige the stamp no less, and sleep under a closure only when
    the inherited one does. Otherwise both stay and the pair is reported."""
    walk = _P("e:w", "w", ["r", "t"], trib=["t"])
    mine = _P("e:o", "o", ["t"])
    recs = {("e:w", "w"): _D("w", **inherited), ("e:o", "o"): _D("o", **own)}
    assert reach_lic.why_not_yield(recs[("e:o", "o")], recs[("e:w", "w")]) == why
    got, diags = reach_lic.own_beats_inherited([walk, mine], recs)
    assert got == [walk, mine]
    assert [(d.kind, d.payload["why"], d.payload["own"]) for d in diags] == [
        ("trib_kept_beside_own", why, "e:o#o")]


def test_a_suspension_on_one_side_only_keeps_both():
    from pipeline.regs.parsing.catalogue import Designation
    walk = _P("e:w", "w", ["r", "t"], trib=["t"])
    mine = _P("e:o", "o", ["t"])
    sleeping = Designation.model_construct(**{**_D("o").__dict__, "suspended_while": [object()]})
    assert reach_lic.why_not_yield(sleeping, _D("w")) == "suspended"
    assert reach_lic.own_beats_inherited([walk, mine], {("e:w", "w"): _D("w"),
                                                        ("e:o", "o"): sleeping})[0] == [walk, mine]


def test_an_own_designation_that_says_more_still_wins():
    walk = _P("e:w", "w", ["r", "t"], trib=["t"])
    mine = _P("e:o", "o", ["t"])
    # the walk is spring-only with no stamp; the own one is all year and obliges the stamp
    recs = {("e:w", "w"): _D("w", when=SPRING),
            ("e:o", "o"): _D("o", steelhead_stamp_during={"when": SPRING, "verbatim": STAMP})}
    got, diags = reach_lic.own_beats_inherited([walk, mine], recs)
    assert got[0].sections == ("r",) and diags[0].kind == "trib_yields_to_own"


def test_a_section_two_designations_own_is_contested_and_takes_nothing_from_a_walk():
    walk = _P("e:w", "w", ["r", "t"], trib=["t"])
    a, b = _P("e:a", "a", ["t"]), _P("e:b", "b", ["t"])
    got, diags = reach_lic.own_beats_inherited([walk, a, b], _recs(walk, a, b))
    assert got[0] == walk
    assert [(d.kind, d.payload["why"]) for d in diags] == [("trib_kept_beside_own", "contested")]


def test_what_a_carve_out_removes_goes_to_the_excluded_waters_own_designation():
    """Burnt Bridge Creek: the Atnarko's carve-out removed 561 sections, Burnt Bridge's own walk
    held 552. The other 9 go to Burnt Bridge's designation, as tributary water — and only there."""
    bb = _P("r5:burnt", "burnt", ["b1", "b2"], trib=["b2"])
    other = _P("r5:other", "other", ["o1"])
    carved = {("r5:atnarko", "atnarko"): ({"b1", "b2", "lake", "pond"}, {"gnis:2100"})}
    claims = {"gnis:2100": ["r5:burnt"]}
    got, diags = reach_lic.carve_outs_to_owner([bb, other], carved, claims)
    assert got[0].sections == ("b1", "b2", "lake", "pond")
    assert got[0].via_tributary == ("b2", "lake", "pond")
    assert got[1] == other
    assert [(d.entry_id, d.kind, d.payload["sections"], d.payload["from"]) for d in diags] == [
        ("r5:burnt", "carve_out_handed_to_owner", ["lake", "pond"], "r5:atnarko#atnarko")]
    assert reach_lic.carve_out_orphans(got, carved, claims) == []
    # an owner with two designations, or none: nothing is guessed, and the check reports it
    two = [bb, _P("r5:burnt", "burnt_b", ["b9"])]
    assert reach_lic.carve_outs_to_owner(two, carved, claims)[0] == two
    assert reach_lic.carve_out_orphans(two, carved, claims)
    assert reach_lic.carve_outs_to_owner([other], carved, claims)[0] == [other]


def test_a_carve_out_removes_nothing_the_excluded_waters_designation_does_not_hold():
    """The Atnarko carve-out took Burnt Bridge Creek above Sitkatapa out of its designation; 9 of
    the sections it removed were then held by no designation. The check names them."""
    bb = _P("r5:burnt", "burnt", ["b1", "b2"])
    carved = {("r5:atnarko", "atnarko"): ({"b1", "b2", "lake"}, {"gnis:2100"})}
    claims = {"gnis:2100": ["r5:burnt"]}
    d = reach_lic.carve_out_orphans([bb], carved, claims)
    assert [(x.rule_id, x.payload["orphans"], x.payload["owners"]) for x in d] == [
        ("atnarko", ["lake"], ["r5:burnt"])]
    held = _P("r5:burnt", "burnt", ["b1", "b2", "lake"])
    assert reach_lic.carve_out_orphans([held], carved, claims) == []
    # nobody claims the excluded water with its tributaries: nothing is asserted
    assert reach_lic.carve_out_orphans([bb], carved, {}) == []


def test_an_unplaced_rule_ships_its_reason_and_an_entry_ships_every_water_it_matched(
        tmp_path, monkeypatch):
    """`rule.unresolved` is the reach builder's "reason: detail" (NULL when placed), as the
    licensing tables carry theirs; `entry.matched` is every matched water, not just the first."""
    monkeypatch.setattr(bundle_lic, "ACKNOWLEDGED_CONFLICTS", {})
    db, run, entries, build = _rules_fixture(tmp_path, [], [("r1:x@1-1", "x.r1")])
    bundle_rules.write(db, run, entries, _Cov(), build_dir=build)
    assert db.execute("SELECT uncertain, unresolved FROM rule").fetchall() == [
        (1, "no_extents: ")]
    assert db.execute("SELECT item_id, matched FROM entry").fetchall() == [
        ("gnis:1", '["gnis:1"]')]
    (tmp_path / "b").mkdir()
    db, run, entries, build = _rules_fixture(tmp_path / "b", [("r1:x@1-1", "x.r1", "s:2")], [])
    bundle_rules.write(db, run, entries, _Cov(), build_dir=build)
    assert db.execute("SELECT uncertain, unresolved FROM rule").fetchall() == [(0, None)]
