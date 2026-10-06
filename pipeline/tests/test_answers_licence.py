"""The licence answer (`pipeline.deliver.answers.licence`) — consumer Stage 7.7, gap G5.

1. RULINGS on a hand-made corpus, each mutation-pinned: who matches, an exemption frees its
   documents, a restating record reads the restated one's `who` / `satisfied_by` / `water`, a
   requirement for the other kind of water drops out, a superior authority displaces (and its
   displaced requirements sell nothing, decision L2), an alternative is another path, prices pick
   the annual price written for the angler first.
2. THE LIVE BUNDLE: a licence key's answer is every one of its sections' answer (the key decides
   it); the 60 profiles answer as the book says for the basic licence, the under-16 non-resident
   and the status First Nations exemption.
"""
from __future__ import annotations

import random
from pathlib import Path

import pytest

from pipeline.deliver.answers import licence as L

P = {d: v[0] for d, v in L.PROFILE_DIMS}          # resident, 16+, not guided, no status


def corpus(**recs) -> L.Corpus:
    rs = {k: dict(v, kind=v.get("kind", "requirement")) for k, v in recs.items()}
    return L.Corpus(records=rs, index={k: i for i, k in enumerate(sorted(rs))},
                    requirements={k: r for k, r in rs.items() if r["kind"] == "requirement"},
                    exemptions=sorted((k, r) for k, r in rs.items() if r["kind"] == "exemption"),
                    alternatives=sorted((k, r) for k, r in rs.items()
                                        if r["kind"] == "alternative"),
                    terms=sorted((k, r) for k, r in rs.items() if r["kind"] == "licence_terms"))


BASIC = {"doing": {"act": "fishing"}, "who": {"age": ["16_plus"]},
         "satisfied_by": [{"hold": ["basic_licence"]}]}
TERMS = {"kind": "licence_terms", "document": "basic_licence", "sold": "per_licence_year",
         "fees_cad": {"resident": 41.15, "non_resident": 62.87}}
SENIOR = {"kind": "licence_terms", "document": "basic_licence", "sold": "per_licence_year",
          "fees_cad": {"resident": 5.71}, "who": {"residency": ["resident"],
                                                  "status": ["aged_65_plus"]}}


def h(*keys, displaced=None, desig=()):
    displaced = displaced or {}
    return {"holds": [k for k in keys if k not in displaced], "rows": list(keys),
            "displaced": displaced, "_desig": list(desig)}


def test_profiles_are_sixty_and_indexed_by_arithmetic():
    ps = L.profiles()
    assert len(ps) == 60 and len({tuple(p.values()) for p in ps}) == 60
    assert [L.profile_index(p) for p in ps] == list(range(60))
    assert any(p["status"] == "metis" for p in ps)


def test_who_paths_exemptions_and_prices():
    C = corpus(**{"zp:b#basic": BASIC, "zp:f#annual": TERMS, "zp:f#senior": SENIOR,
                  "zp:b#ibr": {"kind": "exemption", "who": {"status": ["indian_bc_resident"]},
                               "documents": ["basic_licence"]}})
    ref = C.index.__getitem__
    got = L.documents(C, h("zp:b#basic"), [], P, ref)
    assert [d["doc"] for d in got["documents"]] == ["basic_licence"]
    assert got["documents"][0]["prices"] == {"year": 41.15} and not got["none_needed"]
    senior = L.documents(C, h("zp:b#basic"), [], dict(P, status="aged_65_plus"), ref)
    assert senior["documents"][0]["prices"]["year"] == 5.71       # written for them, first
    young = L.documents(C, h("zp:b#basic"), [], dict(P, age="under_16"), ref)
    assert young["documents"] == [] and young["none_needed"] and young["others"]
    ibr = L.documents(C, h("zp:b#basic"), [], dict(P, status="indian_bc_resident"), ref)
    assert ibr["documents"] == [] and ibr["none_needed"]
    assert ibr["exempt"]["from"] == ["basic_licence"]
    assert ibr["requirements"][0]["paths"] == [{"need": [], "freed": ["basic_licence"]}]
    metis = L.documents(C, h("zp:b#basic"), [], dict(P, status="metis"), ref)
    assert metis == got                                             # decision L5


def test_restates_reads_the_restated_obligation_and_its_water():
    C = corpus(**{"zp:c#cwl": {"doing": {"act": "fishing"}, "who": {"age": ["16_plus"]},
                               "water": "stream", "on": "classified_period",
                               "satisfied_by": [{"hold": ["classified_waters_licence"]}]},
                  "z4:c#cwl": {"doing": {"act": "fishing"}, "on": "classified_period",
                               "restates": {"entry_id": "zp:c", "id": "cwl"}}})
    r = L._resolved(C, "z4:c#cwl")
    assert r["who"] == {"age": ["16_plus"]} and r["water"] == "stream"
    assert r["satisfied_by"] == [{"hold": ["classified_waters_licence"]}]


def test_superior_authority_displaces_and_sells_nothing():
    C = corpus(**{"zp:b#basic": BASIC,
                  "zp:s#park": {"doing": {"act": "fishing"}, "authority": "superior",
                                "satisfied_by": [{"hold": ["national_park_permit"]}]}})
    ref = C.index.__getitem__
    got = L.documents(C, h("zp:b#basic", "zp:s#park",
                           displaced={"zp:b#basic": ["zp:s#park"]}), [], P, ref)
    assert [d["doc"] for d in got["documents"]] == ["national_park_permit"]
    basic = next(x for x in got["requirements"] if x["req"] == ref("zp:b#basic"))
    assert basic["displaced_by"] == [ref("zp:s#park")]
    # mutation: without the displacement the provincial licence is sold too
    both = L.documents(C, h("zp:b#basic", "zp:s#park"), [], P, ref)
    assert {d["doc"] for d in both["documents"]} == {"basic_licence", "national_park_permit"}


def test_an_alternative_is_another_path():
    C = corpus(**{"zp:b#basic": BASIC,
                  "r6:m#yukon": {"kind": "alternative",
                                 "alternative_to": {"entry_id": "zp:b", "id": "basic"},
                                 "satisfied_by": [{"hold": ["yukon_angling_licence"]}]}})
    ref = C.index.__getitem__
    got = L.documents(C, h("zp:b#basic"), C.alternatives, P, ref)
    paths = got["requirements"][0]["paths"]
    assert paths == [{"need": ["basic_licence"]},
                     {"need": ["yukon_angling_licence"], "alt": ref("r6:m#yukon")}]


# --------------------------------------------------------------------------------------------
# The live bundle
# --------------------------------------------------------------------------------------------

@pytest.fixture(scope="module")
def live():
    from pipeline.deliver.answers import common
    p = common.bundle_path()
    if not Path(p).exists():
        pytest.skip(f"no bundle at {p}")
    db = common.connect(p)
    yield p, db, L.corpus(db)
    db.close()


def test_the_key_decides_the_answer(live):
    """`requirements_in_force` asked at any section of a licence key gives that key's answer."""
    from pipeline.deliver.answers.common import month_day
    path, db, C = live
    K = L.keys(db)
    rnd = random.Random(7)
    checked = 0
    for key, sids in K.items():
        for sid in rnd.sample(sids, min(3, len(sids))):
            for day in (20, 120, 200, 320):
                md = month_day(day)
                a = L.holds(db, C, sids[0], md)
                b = L.holds(db, C, sid, md)
                a.pop("_desig"), b.pop("_desig")
                assert a == b, (key, sid, md)
                checked += 1
    assert checked > 500


def test_live_profiles_answer_as_the_book_says(live):
    """Basic licence for a resident 16+ (p.5), none for a resident under 16, an accompanied path
    for a non-resident under 16, nothing for a status First Nations person living in B.C.
    (exempt from every licence and stamp, p.5)."""
    path, db, C = live
    K = L.keys(db)
    ref = C.index.__getitem__
    from pipeline.deliver.answers.common import month_day
    key = next(k for k in K if k.licensing_set is None and not k.province_except
               and not k.tidal)
    hh = L.holds(db, C, K[key][0], month_day(200))
    alts = L._alternatives_here(db, C, K[key][0])
    adult = L.documents(C, hh, alts, P, ref)
    assert adult["documents"][0]["doc"] == "basic_licence" and adult["documents"][0]["base"]
    assert adult["documents"][0]["prices"]["year"] > 0
    kid = L.documents(C, hh, alts, dict(P, age="under_16"), ref)
    assert kid["none_needed"] and not kid["documents"]
    visitor = L.documents(C, hh, alts, dict(P, age="under_16", residency="non_resident"), ref)
    u16 = next(x for x in visitor["requirements"]
               if any("accompanied_by" in p for p in x["paths"]))
    assert not visitor["none_needed"] and u16
    first = L.documents(C, hh, alts, dict(P, status="indian_bc_resident"), ref)
    assert first["none_needed"] and not first["documents"]


def test_build_is_deterministic(live):
    path, _, _ = live
    a = L.build(path, log=lambda *_: None)
    b = L.build(path, log=lambda *_: None)
    a.pop("_key_index"), b.pop("_key_index")
    assert a == b
    assert len(a["profiles"]) == 60 and all(len(t) == 60 for t in a["documents"])
