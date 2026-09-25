"""Licensing: the records on `CatalogueEntry.licensing`, what the model refuses, and what the
migrated corpus says.

Each refusal names the real defect it exists for — the licensing rules gave wrong answers in
ways a reader could not see (see pipeline/docs and the Stage A migration):

  * nine Kootenay waters told B.C. residents they needed no Classified Waters Licence,
  * nine steelhead-stamp waivers could strike the provincial "stamp if you fish for steelhead",
  * five classified waters kept their class only as an adjective on a stamp rule,
  * the Atnarko's "not required until reopened" was free text contradicting its own field,
  * the under-16 non-resident's accompaniment rode on a conduct token beside `required: false`,
  * the keep-only stamps had no species and no size.
"""

from __future__ import annotations

import json
import re

import pytest

from pipeline.common.curated import CURATED
from pipeline.regs.parsing.catalogue import (
    CONDUCT_ACTS, PROVINCIAL_ANGLER_DOCUMENTS, Alternative, CatalogueEntry, CatalogueFile,
    CatalogueRule, Designation, Doing, Exemption, LicenceTerms, NotClassified, Path, Requirement,
    RuleType, Who, label, licensing_label, residency_said,
)

NRA_NON_GUIDED = {"residency": ["non_resident_alien"], "guidance": ["non_guided"]}


def _desig(**kw):
    base = {"id": "u", "classified": "II", "unit": "u", "unit_name": "U",
            "verbatim": "Class II water Sept 1-Apr 30",
            "when": {"dates": [{"from_month": 9, "from_day": 1, "to_month": 4, "to_day": 30}]}}
    base.update(kw)
    return Designation(**base)


def _req(**kw):
    base = {"id": "r", "doing": {"act": "fishing"}, "satisfied_by": [{"hold": ["basic_licence"]}],
            "verbatim": "you must have a valid basic licence"}
    base.update(kw)
    return Requirement(**base)


# --------------------------------------------------------------------------- Who

def test_who_refuses_every_member_of_a_partition_axis():
    """"All residencies" has one spelling — say nothing — like a length band open at both ends."""
    with pytest.raises(ValueError, match="everyone"):
        Who(residency=["resident", "non_resident", "non_resident_alien"])
    with pytest.raises(ValueError, match="everyone"):
        Who(age=["under_16", "16_plus"])
    # `status` is not a partition: most anglers hold none of the three.
    assert Who(status=["indian_bc_resident", "metis", "disabled"]).status


def test_who_refuses_empty_and_duplicates():
    with pytest.raises(ValueError, match="empty"):
        Who()
    with pytest.raises(ValueError, match="twice"):
        Who(residency=["resident", "resident"])


def test_who_overlap_and_words():
    a = Who(residency=["non_resident", "non_resident_alien"], age=["under_16"])
    assert a.words() == "non-residents or non-resident aliens under 16"
    assert Who(**NRA_NON_GUIDED).words() == "non-guided non-resident aliens"
    assert a.overlaps(Who(residency=["non_resident_alien"]))
    assert not a.overlaps(Who(residency=["resident"]))


@pytest.mark.parametrize("text, said", [
    ("Michel Creek classified licence required for non-resident anglers",
     {"non_resident", "non_resident_alien"}),
    ("Angling prohibited for non-guided non-resident aliens", {"non_resident_alien"}),
    ("There are no limits on the number of days which a Canadian resident may fish",
     {"resident", "non_resident"}),
    ("If you are under 16 and not a resident of B.C., you do not require any licence",
     {"non_resident", "non_resident_alien"}),
    ("B.C. resident | permits angling in all classified waters", {"resident"}),
    ("NON-GUIDED or GUIDED Non-Resident or Non-Resident Alien",
     {"non_resident", "non_resident_alien"}),
    ("Class II water Sept 1-Apr 30", None),
])
def test_residency_is_read_off_the_sentence(text, said):
    got = residency_said(text)
    assert (set(got) if got else None) == said


# --------------------------------------------------------------------------- angler_closure

def test_angler_closure_needs_who_it_closes_to_and_it_must_match_the_sentence():
    v = "Angling prohibited for non-guided non-resident aliens on Saturdays and Sundays"
    ok = CatalogueRule(rule_id="k.r1", type=RuleType.angler_closure, verbatim=v,
                       closed_to=NRA_NON_GUIDED,
                       when={"weekdays": ["Saturday", "Sunday"]})
    assert label(ok) == ("Angling closed to non-guided non-resident aliens, on Saturdays and "
                         "Sundays")
    with pytest.raises(ValueError, match="needs closed_to"):
        CatalogueRule(rule_id="k.r1", type=RuleType.angler_closure, verbatim=v)
    with pytest.raises(ValueError, match="not what the sentence says"):
        CatalogueRule(rule_id="k.r1", type=RuleType.angler_closure, verbatim=v,
                      closed_to={"residency": ["non_resident"]})
    with pytest.raises(ValueError, match="belongs to angler_closure"):
        CatalogueRule(rule_id="k.r1", type=RuleType.retention_limit, verbatim=v,
                      species=["ALL_GAME_FISH"], take=0, may_target=False,
                      closed_to=NRA_NON_GUIDED)


@pytest.mark.parametrize("closed_to", [
    {"guidance": ["non_guided"]},                               # every non-guided RESIDENT too
    {"residency": ["non_resident_alien"]},                      # guided aliens too
    {"residency": ["non_resident_alien"], "guidance": ["guided"]},
    {"age": ["16_plus"]},
])
def test_an_angler_closure_names_exactly_who_its_sentence_names(closed_to):
    """Leaving an axis out WIDENS a closure. Checked only when `closed_to` named a residency, a
    closure to "non-guided" anglers — residents included — validated against a sentence about
    non-guided non-resident aliens."""
    v = "Angling prohibited for non-guided non-resident aliens on Saturdays and Sundays"
    with pytest.raises(ValueError, match="not what the sentence says"):
        CatalogueRule(rule_id="k.r1", type=RuleType.angler_closure, verbatim=v,
                      closed_to=closed_to)


def test_a_requirement_or_terms_cannot_drop_the_residency_its_sentence_names():
    with pytest.raises(ValueError, match="not what the sentence says"):
        _req(verbatim="Michel Creek classified licence required for non-resident anglers")
    with pytest.raises(ValueError, match="not what the sentence says"):
        LicenceTerms(id="t", document="classified_waters_licence", max_per_licence_year=1,
                     verbatim="Non-Resident Aliens may only purchase one Classified Waters "
                              "Licence for the Dean River per licence year.")
    # "whether GUIDED or NON-GUIDED" is everyone on that axis: say nothing
    v = ("EXCEPTION: Non-Resident Aliens (whether GUIDED or NON-GUIDED) may only purchase one "
         "Classified Waters Licence for the Dean River per licence year.")
    assert LicenceTerms(id="t", document="classified_waters_licence", max_per_licence_year=1,
                        who={"residency": ["non_resident_alien"]}, verbatim=v)
    with pytest.raises(ValueError, match="guidance"):
        LicenceTerms(id="t", document="classified_waters_licence", max_per_licence_year=1,
                     who={"residency": ["non_resident_alien"], "guidance": ["guided"]},
                     verbatim=v)


@pytest.mark.parametrize("field, value", [
    ("document", "classified_waters_licence"), ("required", False), ("water_class", "II"),
    ("licence_name", "Michel Creek"), ("allocation", "draw"), ("issuing_jurisdiction", "Yukon"),
    ("on_retention", True), ("grantor", "landowner"), ("permitted", False),
    ("angler_class", {"residency": "non_resident"}),
])
def test_the_licensing_fields_are_refused_on_a_rule(field, value):
    with pytest.raises(ValueError):
        CatalogueRule(rule_id="x.r1", type=RuleType.advisory, verbatim="x", **{field: value})


@pytest.mark.parametrize("t", ["document_required", "access_permission"])
def test_the_licensing_rule_types_are_refused(t):
    with pytest.raises(ValueError):
        CatalogueRule(rule_id="x.r1", type=t, verbatim="x")


# --------------------------------------------------------------------------- Designation

def test_a_designation_holds_a_stamp_period_or_a_waiver_never_both():
    with pytest.raises(ValueError, match="opposite facts"):
        _desig(steelhead_stamp_during={"when": {"dates": [{"from_month": 12, "from_day": 1,
                                                            "to_month": 4, "to_day": 30}]},
                                       "verbatim": "Steelhead Stamp mandatory Dec 1-Apr 30"},
               steelhead_stamp_waived={"verbatim": "Steelhead Stamp not required"})


def test_the_stamp_period_sits_inside_the_classified_period():
    """Copper Creek: class Sept 1-Apr 30, stamp Dec 1-Apr 30 — inside, across the year end."""
    ok = _desig(steelhead_stamp_during={
        "when": {"dates": [{"from_month": 12, "from_day": 1, "to_month": 4, "to_day": 30}]},
        "verbatim": "Steelhead Stamp mandatory Dec 1-Apr 30"})
    assert ok.steelhead_stamp_during
    with pytest.raises(ValueError, match="outside the classified"):
        _desig(steelhead_stamp_during={
            "when": {"dates": [{"from_month": 6, "from_day": 1, "to_month": 6, "to_day": 30}]},
            "verbatim": "Steelhead Stamp mandatory June 1-June 30"})


def test_a_designation_agrees_with_the_class_its_sentence_prints():
    with pytest.raises(ValueError, match="verbatim says Class I,"):
        _desig(verbatim="Class 1 water Sept 1-Apr 30")


def test_a_waiver_must_quote_a_waiver_and_a_stamp_period_must_quote_a_mandate():
    with pytest.raises(ValueError, match="must quote"):
        _desig(steelhead_stamp_waived={"verbatim": "Steelhead Stamp mandatory Dec 1-Apr 30"})
    with pytest.raises(ValueError, match="quotes a waiver"):
        _desig(steelhead_stamp_during={"when": {}, "verbatim":
                                       "Steelhead Stamp not required unless fishing for steelhead"})


def test_until_reopened_is_a_suspension():
    with pytest.raises(ValueError, match="suspended_while"):
        _desig(verbatim="Class II water Mar 1-May 31. NOTE: Classified Waters Licence or "
                        "Steelhead Stamp not required until reopened to steelhead fishing",
               when={"dates": [{"from_month": 3, "from_day": 1, "to_month": 5, "to_day": 31}]})


def test_a_designation_dates_are_printed_in_its_sentence():
    with pytest.raises(ValueError, match="not printed"):
        _desig(when={"dates": [{"from_month": 4, "from_day": 1, "to_month": 10, "to_day": 31}]})


def test_the_named_licence_is_the_unit():
    """"Michel Creek classified licence required for non-resident anglers" names a licence unit —
    it is NOT a requirement scoped to non-residents (which told residents they need nothing)."""
    v = ("Class II water when open, including tributaries - Michel Creek classified licence "
         "required for non-resident anglers")
    assert _desig(verbatim=v, when=None, unit="michel_creek", id="michel_creek").unit
    with pytest.raises(ValueError, match="names the 'michel creek' licence"):
        _desig(verbatim=v, when=None, unit="alexander_creek")


def test_not_classified_must_say_so():
    assert NotClassified(id="n", verbatim="Part described is NOT a Classified Water")
    with pytest.raises(ValueError, match="does not say"):
        NotClassified(id="n", verbatim="Class II water")


# --------------------------------------------------------------------------- the entry

def _entry(licensing, rules=None, regs=None, entry_id="r6:x@6-1"):
    return CatalogueEntry.model_validate({
        "entry_id": entry_id, "name": "X",
        "regs_verbatim": regs or ("No Fishing for steelhead. Bait ban. Class II water Mar 1-May "
                                  "31. NOTE: Classified Waters Licence or Steelhead Stamp not "
                                  "required until reopened to steelhead fishing. Steelhead Stamp "
                                  "not required"),
        "rules": rules if rules is not None else [
            {"rule_id": "x.r1", "type": "retention_limit", "verbatim": "No Fishing for steelhead",
             "species": ["ST"], "take": 0, "may_target": False, "extents": [{"op": "whole"}]},
            {"rule_id": "x.r2", "type": "bait_restriction", "verbatim": "Bait ban",
             "gear": [{"slot": "bait", "ban": ["any_bait"]}], "extents": [{"op": "whole"}]}],
        "licensing": licensing})


_SUSP = {"kind": "designation", "id": "x", "classified": "II", "unit": "x", "unit_name": "X",
         "verbatim": "Class II water Mar 1-May 31",
         "when": {"dates": [{"from_month": 3, "from_day": 1, "to_month": 5, "to_day": 31}]},
         "review_reason": "stamp period on reopening not printed"}


def test_a_suspension_names_a_closure_in_this_entry():
    note = ("NOTE: Classified Waters Licence or Steelhead Stamp not required until reopened to "
            "steelhead fishing")
    e = _entry([{**_SUSP, "suspended_while": [{"rule_id": "x.r1", "verbatim": note}]}])
    sib = {r.rule_id: r for r in e.rules}
    assert licensing_label(e.licensing[0], sib).endswith(
        " Not in force while “No fishing for steelhead” applies.")
    with pytest.raises(ValueError, match="not a closure"):
        _entry([{**_SUSP, "suspended_while": [{"rule_id": "x.r2", "verbatim": note}]}])
    with pytest.raises(ValueError, match="names no rule"):
        _entry([{**_SUSP, "suspended_while": [{"rule_id": "x.r9", "verbatim": note}]}])


def test_two_designations_in_one_entry_are_two_units():
    """"There are two separate Class II waters on the Skeena River (non-residents … require
    separate licences)"."""
    d = {**_SUSP, "review_reason": "x"}
    with pytest.raises(ValueError, match="share unit"):
        _entry([d, {**d, "id": "y"}])


def test_steelhead_country_records_the_stamp_clause():
    d = {k: v for k, v in _SUSP.items() if k != "review_reason"}
    with pytest.raises(ValueError, match="steelhead country"):
        _entry([d])
    assert _entry([d], entry_id="r4:x@4-1")            # Kootenay Class II prints no stamp
    assert _entry([{**d, "steelhead_stamp_waived": {"verbatim": "Steelhead Stamp not required"}}])


def test_every_quote_on_a_record_is_in_the_passage():
    d = {**_SUSP, "steelhead_stamp_waived": {"verbatim": "Steelhead Stamp not mandatory"}}
    with pytest.raises(ValueError, match="not a contiguous substring"):
        _entry([d])


def test_an_entry_may_hold_only_licensing_but_not_nothing():
    only = _entry([{**_SUSP}], rules=[
        {"rule_id": "x.r1", "type": "retention_limit", "verbatim": "No Fishing for steelhead",
         "species": ["ST"], "take": 0, "may_target": False, "extents": [{"op": "whole"}]}])
    assert only.licensing
    with pytest.raises(ValueError, match="says nothing"):
        _entry([], rules=[])


# --------------------------------------------------------------------------- Doing / Path

def test_doing_needs_its_subject():
    with pytest.raises(ValueError, match="needs species"):
        Doing(act="targeting")
    with pytest.raises(ValueError, match="only with act: retaining"):
        Doing(act="targeting", species=["ST"], lengths=[{"min_cm": 50}])
    with pytest.raises(ValueError, match="never how many"):
        Doing(act="retaining", species=["RB"], lengths=[{"min_cm": 50, "take": 1}])


def test_an_except_that_subtracts_nothing_is_refused():
    """`zp:salmon_stamp`: SALMON except KO — kokanee is not in SALMON."""
    with pytest.raises(ValueError, match="subtracts nothing"):
        Doing(act="retaining", species=["SALMON"], species_except=["KO"])
    assert Doing(act="retaining", species=["SALMON"], species_except=["CH"])


def test_a_path_is_one_way_and_says_whose_quota():
    with pytest.raises(ValueError, match="exactly one"):
        Path(hold=["basic_licence"], accompanied_by={"who": {"age": ["16_plus"]}})
    with pytest.raises(ValueError, match="whose quota"):
        Path(accompanied_by={"who": {"age": ["16_plus"]}})
    with pytest.raises(ValueError, match="not to `hold`"):
        Path(hold=["basic_licence"], quota="own")
    p = Path(accompanied_by={"who": {"age": ["16_plus"]}}, quota="counts_to_companion")
    assert p.model_dump(by_alias=True, mode="json")["quota"] == "counts_to_companion"


# --------------------------------------------------------------------------- Requirement

def test_a_requirement_is_documents_or_a_duty():
    with pytest.raises(ValueError, match="exactly one of satisfied_by"):
        _req(conduct=["carry_paper_licence"])
    with pytest.raises(ValueError, match="exactly one of satisfied_by"):
        _req(satisfied_by=[])
    with pytest.raises(ValueError, match="not a registered act"):
        _req(satisfied_by=[], conduct=["be_accompanied_by_licensed_adult"])


def test_a_superior_requirement_is_not_met_by_a_provincial_document():
    with pytest.raises(ValueError, match="provincial"):
        _req(authority="superior", satisfied_by=[{"hold": ["basic_licence"]}])
    assert _req(authority="superior", satisfied_by=[{"hold": ["national_park_permit"]}])


def test_who_except_must_subtract_something():
    with pytest.raises(ValueError, match="subtracts nothing"):
        _req(who={"residency": ["resident"]}, who_except={"residency": ["non_resident"]})


def test_the_under_16_non_resident_is_both_kinds_of_visitor():
    """`basic_licence.r4` was `residency: non_resident` — the under-16 ALIEN fell out."""
    v = ("If you are under 16 and not a resident of B.C., you do not require any licence or "
         "stamp to sport fish, but you must be accompanied by a person 16 years or older")
    path = [{"accompanied_by": {"who": {"age": ["16_plus"]}}, "quota": "counts_to_companion"}]
    with pytest.raises(ValueError, match="not what the sentence says"):
        _req(who={"age": ["under_16"], "residency": ["non_resident"]}, satisfied_by=path,
             verbatim=v)
    r = _req(who={"age": ["under_16"], "residency": ["non_resident", "non_resident_alien"]},
             satisfied_by=path, verbatim=v)
    assert licensing_label(r) == (
        "Non-residents or non-resident aliens under 16: to fish, be accompanied by someone 16 or "
        "over who holds the licences and stamps this fishing requires (any fish you keep count "
        "toward your companion's limit).")


# --------------------------------------------------------------------------- terms / others

def test_terms_must_set_something_and_counts_are_positive():
    with pytest.raises(ValueError, match="set nothing"):
        LicenceTerms(id="t", document="classified_waters_licence", verbatim="x")
    with pytest.raises(ValueError):
        LicenceTerms(id="t", document="classified_waters_licence", verbatim="x",
                     max_consecutive_days=0)
    with pytest.raises(ValueError, match="contradict"):
        LicenceTerms(id="t", document="classified_waters_licence", verbatim="x",
                     unlimited_days=True, max_consecutive_days=8)


def test_an_alternative_is_place_scoped():
    with pytest.raises(ValueError):
        Alternative(id="a", alternative_to={"entry_id": "zp:basic_licence", "id": "b"},
                    satisfied_by=[{"hold": ["yukon_angling_licence"]}], extents=[],
                    verbatim="x")


def test_an_exemption_names_each_document_once():
    with pytest.raises(ValueError, match="twice"):
        Exemption(id="e", who={"status": ["metis"]},
                  documents=["basic_licence", "basic_licence"], verbatim="x")


# =========================================================================== the corpus

DIR = CURATED.regulations.entries.catalogue


def _corpus():
    out = []
    for p in sorted(DIR.glob("region-*.json")):
        out += CatalogueFile.model_validate(json.loads(p.read_text(encoding="utf-8"))).entries
    return out


@pytest.fixture(scope="module")
def corpus():
    es = _corpus()
    if not es:
        pytest.skip("no catalogue on disk")
    return {e.entry_id: e for e in es}


def _rec(corpus, eid, rid):
    return next(x for x in corpus[eid].licensing if x.id == rid)


def _find(corpus, prefix):
    return next(e for k, e in corpus.items() if k.startswith(prefix))


def test_no_licensing_rule_is_left(corpus):
    left = [(e.entry_id, r.rule_id) for e in corpus.values() for r in e.rules
            if r.type.value in ("document_required", "access_permission")]
    assert not left


def test_the_kootenay_named_licences_bind_every_angler(corpus):
    """Nine waters: "Class II … - X classified licence required for non-resident anglers". Every
    angler needs a CWL there; X is the unit a non-resident's day licence names."""
    want = {"alexander_creek_downstream": "michel_creek", "alexander_creek_upstream":
            "michel_creek", "cadorna_creek": "elk_river", "fording_river_downstream":
            "elk_river", "morrissey_creek": "elk_river", "hellroaring_creek": "st_mary_river",
            "perry_creek": "st_mary_river", "lodgepole_creek_downstream": "wigwam_river",
            "lodgepole_creek_upstream": "wigwam_river"}
    for prefix, unit in want.items():
        e = _find(corpus, f"r4:{prefix}")
        ds = [x for x in e.licensing if isinstance(x, Designation)]
        assert [d.unit for d in ds] == [unit], e.entry_id
        # nothing on these entries is scoped to a residency — the CWL is for everyone
        assert not [x for x in e.licensing if getattr(x, "who", None) is not None]
    cwl = _rec(corpus, "zp:classified_waters_licence", "classified_waters_licence")
    assert cwl.on == "classified_period" and cwl.who == Who(age=["16_plus"])


def test_a_waiver_cannot_reach_the_steelhead_angler(corpus):
    """Chilko, Dean upper, Horsefly, Seymour, Ecstall, Skeena 2, West Road, both Stellakos: the
    waiver is a designation fact; the provincial 'stamp if you fish for steelhead' is a separate
    requirement with a separate trigger."""
    waived = [(e.entry_id, x.unit) for e in corpus.values() for x in e.licensing
              if isinstance(x, Designation) and x.steelhead_stamp_waived is not None]
    assert len(waived) == 9, waived
    st = _rec(corpus, "zp:steelhead", "steelhead_targeting")
    assert st.doing.act == "targeting" and st.doing.species == ["ST"]
    assert [p.hold for p in st.satisfied_by] == [["steelhead_stamp"]]
    assert st.on is None and st.restates is None       # nothing a designation can switch off
    skeena = _find(corpus, "r6:skeena_river_mainstem_only")
    two = next(x for x in skeena.licensing if x.unit == "skeena_river_2")
    four = next(x for x in skeena.licensing if x.unit == "skeena_river_section_4")
    assert two.steelhead_stamp_waived and four.steelhead_stamp_during
    assert four.review_reason, "Section 4's upper bound is not printed and must be flagged"


def test_the_five_classes_that_sat_on_a_stamp_rule_are_designations(corpus):
    want = {"r6:babine_river": "I", "r6:sustut_river": "I", "r6:telkwa_river": "II",
            "r5:west_road_blackwater_river": "II", "r6:stellako_river": "II"}
    for prefix, cls in want.items():
        d = [x for x in _find(corpus, prefix).licensing if isinstance(x, Designation)]
        assert len(d) == 1 and d[0].classified == cls, prefix


def test_until_reopened_sleeps_under_the_steelhead_closure(corpus):
    for prefix, closure in (("r5:atnarko_bella_coola_rivers", "atnarko_bella_coola_rivers.r13"),
                            ("r5:burnt_bridge_creek", "burnt_bridge_creek.r2")):
        e = _find(corpus, prefix)
        d = next(x for x in e.licensing if isinstance(x, Designation))
        assert [s.rule_id for s in d.suspended_while] == [closure]
        c = next(r for r in e.rules if r.rule_id == closure)
        assert c.take == 0 and c.may_target is False and c.species == ["ST"]
        # the stamp period on reopening is unknown, never "none"
        assert d.steelhead_stamp_during.when.unparsed and not d.steelhead_stamp_during.when.dates


def test_the_under_16_non_resident_is_an_accompaniment_path(corpus):
    r = _rec(corpus, "zp:basic_licence", "under_16_non_resident")
    assert r.who == Who(age=["under_16"], residency=["non_resident", "non_resident_alien"])
    # TWO WAYS (decision 10, 2026-09-24): be accompanied, and the catch counts to the companion;
    # or buy your own licence and stamps, and keep your own quota (printed p.6).
    acc, own = r.satisfied_by
    assert acc.accompanied_by.who == Who(age=["16_plus"]) and acc.quota == "counts_to_companion"
    assert own.as_ == Who(residency=["non_resident", "non_resident_alien"], age=["16_plus"])
    assert own.quota == "own"
    assert not r.conduct and "be_accompanied_by_licensed_adult" not in CONDUCT_ACTS


def test_keep_only_stamps_name_the_fish_and_the_size(corpus):
    want = {("zp:kootenay_rainbow_stamp", "kootenay_rainbow_stamp"): (["RB"], 50),
            ("zp:shuswap_rainbow_stamp", "shuswap_rainbow_stamp"): (["RB"], 50),
            ("zp:shuswap_char_stamp", "shuswap_char_stamp"): (["LT", "BT"], 60)}
    for (eid, rid), (sp, cm) in want.items():
        r = _rec(corpus, eid, rid)
        assert r.doing.act == "retaining" and r.doing.species == sp
        assert [b.min_cm for b in r.doing.lengths] == [cm]
    salmon = _rec(corpus, "zp:salmon_stamp", "salmon_stamp")
    assert salmon.doing.species == ["SALMON"] and not salmon.doing.species_except


def test_the_indian_resident_exemption_covers_every_provincial_document(corpus):
    ex = _rec(corpus, "zp:basic_licence", "indian_bc_resident")
    assert sorted(d.value for d in ex.documents) == sorted(PROVINCIAL_ANGLER_DOCUMENTS)
    # Métis "are required … to hold appropriate angling licences" — the default, never exempt.
    for e in corpus.values():
        for x in e.licensing:
            if isinstance(x, Exemption):
                assert "metis" not in x.who.status


def test_provincial_requirements_are_stated_once(corpus):
    """A zone or water row that repeats a provincial obligation `restates` it and adds nothing:
    same trigger, same period key, and no document the original does not name."""
    by_key = {(e.entry_id, x.id): x for e in corpus.values() for x in e.licensing}
    seen = 0
    for e in corpus.values():
        for x in e.licensing:
            if not isinstance(x, Requirement) or x.restates is None:
                continue
            seen += 1
            t = by_key.get((x.restates.entry_id, x.restates.id))
            assert isinstance(t, Requirement), f"{e.entry_id}#{x.id}: restates nothing"
            assert t.restates is None, "a restatement of a restatement"
            assert x.doing == t.doing and x.on == t.on, f"{e.entry_id}#{x.id} adds a trigger"
            said = {d for p in t.satisfied_by for d in p.hold}
            assert {d for p in x.satisfied_by for d in p.hold} <= said, f"{e.entry_id}#{x.id}"
    assert seen >= 6
    ids = [(e.entry_id, x.id) for e in corpus.values() for x in e.licensing
           if isinstance(x, Requirement) and x.restates is None
           and x.on == "classified_period"]
    assert ids == [("zp:classified_waters_licence", "classified_waters_licence")]


def test_units_are_consistent_across_the_corpus(corpus):
    """Designations sharing a unit share a licence, so they share a class; every unit a term
    names exists, or the term says why not."""
    cls: dict = {}
    for e in corpus.values():
        for x in e.licensing:
            if isinstance(x, Designation):
                assert cls.setdefault(x.unit, x.classified) == x.classified, x.unit
    for e in corpus.values():
        for x in e.licensing:
            if isinstance(x, LicenceTerms):
                missing = [u for u in x.units if u not in cls]
                assert not missing or x.review_reason, (x.id, missing)
            if isinstance(x, Alternative):
                t = corpus[x.alternative_to.entry_id]
                assert any(isinstance(y, Requirement) and y.id == x.alternative_to.id
                           for y in t.licensing), x.id


#: Entries that carry the (CW) glyph and print no class of their own — each is a cross-reference
#: or a closure, and none may be given a designation it does not print.
_CW_WITHOUT_A_CLASS = {
    "r4:bighorn_ram_creek@4-2",                          # "see Wigwam River"
    "r6:kwinamass_river@6-14",                           # "See Ksi X'anmas River"
    "r4:fording_river_upstream_of_josephine_falls@4-23",  # prints only "No Fishing"
}


def test_the_classified_symbol_cross_checks_the_designations(corpus):
    """The (CW) glyph is extracted mechanically — the one classified evidence the parse did not
    produce — so it is the CHECK, never the binding. (51 of 72 glyphs were lost from the entries;
    this runs on the ones that survive.)"""
    bad = [k for k, e in corpus.items() if "Classified" in e.symbols
           and not any(isinstance(x, Designation) for x in e.licensing)
           and k not in _CW_WITHOUT_A_CLASS]
    assert not bad, bad


def test_every_water_that_prints_a_class_has_a_designation(corpus):
    """The idiom check that catches an OMITTED designation — the shape cannot see absence."""
    pat = re.compile(r"\bclass (ii|i|1|2) waters?\b")
    bad = [k for k, e in corpus.items() if not k.startswith("z")
           and pat.search(e.regs_verbatim.lower().replace("*", ""))
           and not any(isinstance(x, Designation) for x in e.licensing)]
    assert not bad, bad


def test_every_licensing_record_has_a_label(corpus):
    for e in corpus.values():
        sib = {r.rule_id: r for r in e.rules}
        for x in e.licensing:
            assert licensing_label(x, sib).strip(), (e.entry_id, x.id)


# =========================================================================== stage B: the gaps

def test_a_stamp_period_may_not_be_empty():
    """An empty `When` is ALL YEAR, so an empty stamp period put the stamp on the water every day
    — while the page printed a window, or printed that it could not say which."""
    with pytest.raises(ValueError, match="reads as all year"):
        _desig(steelhead_stamp_during={"when": {}, "verbatim":
                                       "Steelhead Stamp mandatory Dec 1-Apr 30"})
    # a period the page does not give is `unparsed`, which is not empty
    assert _desig(steelhead_stamp_during={
        "when": {"unparsed": ["from reopening to steelhead fishing"]},
        "verbatim": "Steelhead Stamp mandatory Dec 1-Apr 30"})


def test_covers_every_unit_is_not_about_some_units():
    with pytest.raises(ValueError, match="every_unit names units"):
        LicenceTerms(id="t", document="classified_waters_licence", covers="every_unit",
                     units=["michel_creek"], verbatim="permits angling in all classified waters")
    assert LicenceTerms(id="t", document="classified_waters_licence", covers="every_unit",
                        verbatim="permits angling in all classified waters")


def test_an_annual_licence_is_sold_for_the_year():
    """"one ANNUAL angling licence per licence year" without `sold` counted every basic licence —
    and a visitor may buy as many one-day licences as they like."""
    v = "You are only permitted one annual angling licence per licence year."
    with pytest.raises(ValueError, match="annual licence"):
        LicenceTerms(id="t", document="basic_licence", max_per_licence_year=1, verbatim=v)
    t = LicenceTerms(id="t", document="basic_licence", max_per_licence_year=1,
                     sold="per_licence_year", verbatim=v)
    assert licensing_label(t) == ("Annual basic angling licence: at most 1 licence per licence "
                                  "year.")
    # "an annual limited entry draw" is not an annual licence
    assert LicenceTerms(id="t", document="classified_waters_licence", allocation="draw",
                        verbatim="must enter an annual limited entry draw held each spring")


_INDIAN = ("If you are an Indian and a resident of B.C., you are not required to obtain any type "
           "of fishing licence or stamp to sport fish in non-tidal waters.")


def test_an_exemption_is_checked_against_its_sentence():
    """The one record that REMOVES an obligation: a `who` wider than the sentence releases
    anglers the book never released, and a short document list keeps charging the ones it did."""
    ok = Exemption(id="e", who={"status": ["indian_bc_resident"]},
                   documents=list(PROVINCIAL_ANGLER_DOCUMENTS), verbatim=_INDIAN)
    assert ok
    with pytest.raises(ValueError, match="who.status"):
        Exemption(id="e", who={"status": ["metis"]},
                  documents=list(PROVINCIAL_ANGLER_DOCUMENTS), verbatim=_INDIAN)
    with pytest.raises(ValueError, match="who.residency"):
        Exemption(id="e", who={"status": ["indian_bc_resident"],
                               "residency": ["resident", "non_resident"]},
                  documents=list(PROVINCIAL_ANGLER_DOCUMENTS), verbatim=_INDIAN)
    with pytest.raises(ValueError, match="ANY licence or stamp"):
        Exemption(id="e", who={"status": ["indian_bc_resident"]},
                  documents=["basic_licence"], verbatim=_INDIAN)
    with pytest.raises(ValueError, match="who.guidance"):
        Exemption(id="e", who={"status": ["indian_bc_resident"], "guidance": ["guided"]},
                  documents=list(PROVINCIAL_ANGLER_DOCUMENTS), verbatim=_INDIAN)


def test_an_alternative_is_never_the_whole_province():
    with pytest.raises(ValueError, match="whole province"):
        Alternative(id="a", alternative_to={"entry_id": "zp:basic_licence", "id": "basic_licence"},
                    satisfied_by=[{"hold": ["yukon_angling_licence"]}],
                    extents=[{"op": "within", "area_kind": "region"}], verbatim="x")


def test_an_alternative_on_a_row_that_is_no_water_names_its_place():
    """On `zp:basic_licence` a `whole` has no water to be the whole OF."""
    v = "B.C. and Yukon angling licences are valid on all parts of Morley Lake"
    alt = {"kind": "alternative", "id": "yukon", "verbatim": v,
           "alternative_to": {"entry_id": "zp:basic_licence", "id": "basic_licence"},
           "satisfied_by": [{"hold": ["yukon_angling_licence"]}]}
    base = {"entry_id": "zp:basic_licence", "name": "Basic licence", "regs_verbatim": v}
    with pytest.raises(ValueError, match="name no place"):
        CatalogueEntry(**base, licensing=[{**alt, "extents": [{"op": "whole"}]}])
    assert CatalogueEntry(**base, licensing=[
        {**alt, "extents": [{"op": "whole", "item_id": "gnis:1"}]}])
    # a WATER row may say `whole` — it is the whole of that water
    assert CatalogueEntry(**base | {"entry_id": "r6:morley_lake@6-25", "matched": ["gnis:1"]},
                          licensing=[{**alt, "extents": [{"op": "whole"}]}])


@pytest.mark.parametrize("kind", ["designation", "requirement", "not_classified", "alternative"])
def test_includes_tributaries_inside_an_extent_is_refused(kind):
    """The reach builder reads the flag on the RECORD, never on an extent — "the Fraser River
    Watershed (including tributaries)" written that way bound the mainstem alone."""
    ex = [{"op": "whole", "includes_tributaries": True}]
    with pytest.raises(ValueError, match="does not read on an extent"):
        if kind == "designation":
            _desig(extents=ex)
        elif kind == "requirement":
            _req(extents=ex)
        elif kind == "not_classified":
            NotClassified(id="n", extents=ex, verbatim="not a Classified Water")
        else:
            Alternative(id="a", alternative_to={"entry_id": "e", "id": "r"},
                        satisfied_by=[{"hold": ["yukon_angling_licence"]}], extents=ex,
                        verbatim="x")


# --------------------------------------------------------------------------- labels, read back

def _labels(corpus):
    """Every label as the BUNDLE writes it: with the corpus's unit names and cross-references."""
    units = {x.unit: x.unit_name for e in corpus.values() for x in e.licensing
             if isinstance(x, Designation)}
    refs = {(e.entry_id, x.id): x for e in corpus.values() for x in e.licensing}
    return {(e.entry_id, x.id): licensing_label(x, {r.rule_id: r for r in e.rules}, units=units,
                                                refs=refs)
            for e in corpus.values() for x in e.licensing}


def test_the_salmon_stamp_keeps_not_kokanee(corpus):
    """The book: "a salmon of any legal size or species (other than kokanee)". The group already
    leaves kokanee out; the label must say so rather than read as every salmon."""
    got = _labels(corpus)[("zp:salmon_stamp", "salmon_stamp")]
    assert got == ("Anglers 16 and over need a Conservation Surcharge Stamp for salmon to keep "
                   "salmon (not kokanee).")


def test_the_one_per_year_basic_licence_is_the_annual_one(corpus):
    assert _labels(corpus)[("zp:licence_administration", "basic_one_per_year")] == \
        "Annual basic angling licence: at most 1 licence per licence year."


def test_an_alternative_names_the_licence_it_stands_in_for(corpus):
    got = _labels(corpus)[("r6:morley_lake@6-25", "yukon_licence")]
    assert got == ("A Yukon angling licence is also accepted here, in place of a basic angling "
                   "licence.")
    assert "zp:" not in got and "#" not in got


def test_terms_name_licence_units_as_printed_never_as_slugs(corpus):
    labels = _labels(corpus)
    got = labels[("z5:dean_river_classified", "dean_class_i_main_draw")]
    assert got == ("Class I Classified Waters Licence for non-guided non-resident aliens (Dean "
                   "River Class I - Main Section): by annual limited-entry draw.")
    slugs = {x.unit for e in corpus.values() for x in e.licensing if isinstance(x, Designation)}
    slugs |= {u for e in corpus.values() for x in e.licensing if isinstance(x, LicenceTerms)
              for u in x.units}
    leaked = [(k, s) for k, v in labels.items() for s in slugs if "_" in s and s in v]
    assert not leaked, leaked


def test_every_licensing_label_is_a_sentence(corpus):
    """Starts with a capital, ends with a full stop, and carries no id, slug or enum spelling."""
    bad = [(k, v) for k, v in _labels(corpus).items()
           if not v[:1].isupper() or not v.endswith(".")
           or re.search(r"\b[a-z]+_[a-z_]+\b|[a-z0-9]+:[a-z_]|#", v)]
    assert not bad, bad[:5]


def test_a_designation_label_reads_as_sentences(corpus):
    labels = _labels(corpus)
    skeena = "r6:skeena_river_mainstem_only@6-10"
    assert labels[(skeena, "skeena_river_2")] == (
        "Class II Classified Water, Jul 1-Sep 30 (licence unit: Skeena River 2). Steelhead Stamp "
        "not required here unless you fish for steelhead.")
    assert labels[(skeena, "skeena_river_section_4")] == (
        "Class II Classified Water, Jul 1-Dec 31 (licence unit: Skeena River Section 4). "
        "Steelhead Stamp required whatever you fish for, Jul 1-Dec 31.")


# --------------------------------------------------------------------------- presumes

def _duty(**kw):
    base = {"id": "produce", "doing": {"act": "fishing"}, "conduct": ["produce_licence_on_request"],
            "presumes": ["basic_licence"], "who": {"age": ["16_plus"]},
            "verbatim": "When fishing, you must produce your angling licence and photo ID, on "
                        "request of an officer."}
    base.update(kw)
    return Requirement(**base)


def test_a_duty_about_a_licence_names_the_licence():
    """"Produce your angling licence" bound an Indian resident of B.C., whom the book releases from
    every licence: the duty named no document, so the exemption could not reach it."""
    assert _duty().presumes
    with pytest.raises(ValueError, match="duty about a document"):
        _duty(presumes=[])


def test_presumes_is_checked_against_its_own_sentence():
    with pytest.raises(ValueError, match="does not name"):
        _duty(presumes=["basic_licence", "steelhead_stamp"])


def test_presumes_belongs_to_a_duty():
    with pytest.raises(ValueError, match="presumes belongs to a conduct duty"):
        _req(presumes=["basic_licence"])


def test_the_corpus_s_licence_duties_presume_the_basic_licence():
    got = {}
    for p in sorted(CURATED.regulations.entries.catalogue.glob("region-*.json")):
        for e in CatalogueFile.model_validate(json.loads(p.read_text())).entries:
            for x in e.licensing:
                if isinstance(x, Requirement) and x.conduct:
                    got[f"{e.entry_id}#{x.id}"] = [d.value for d in x.presumes]
    assert got["zp:licence_administration#produce_licence"] == ["basic_licence"]
    assert got["zp:licence_administration#carry_paper_licence"] == ["basic_licence"]
