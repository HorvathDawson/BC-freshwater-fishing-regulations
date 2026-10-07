"""STEELHEAD REGULATIONS COME ONLY FROM THE BOOK; THE CURATED LIST IS A PRESENCE INDICATOR (user
rulings 2026-10-02, revised) — on a temporary corpus and a toy atlas, so each mechanism is pinned
without a build.

  - BOOK-KNOWN: every section any rule of a STEELHEAD ROW binds (a rule naming `ST`, the
    `anadromous_rainbow` flag, or a licensing record speaking of the Steelhead Stamp — its waiver
    included). There is no tributary walk. Book-known water carries the provincial set: on a
    steelhead-region stream by the base rules, elsewhere (a lake, a stream past the regions) by the
    twins (`Extent` op `steelhead_waters`); a book-known LAKE also gets its zone's wild release
    through that line's twin (`area_id`). `anadromous` = KNOWN ∧ a STREAM (`water_kind.kind_of`: a
    lake-typed water named as flowing — the Vedder Canal, a slough — is one) ∧ STEELHEAD RULES APPLY
    (`Presence.rules_apply`: the provincial set binds it) — user ruling 2026-10-03.
  - THE CURATED LIST (`steelhead.load_list` / `resolve_list`): its waters' own sections are KNOWN,
    and where steelhead rules apply on a listed stream a rainbow over 50 cm is a steelhead
    (`anadromous`) — NOTHING ELSE changes: no binding, no licensing placement (mutation-checked
    below). A listed water no steelhead rule binds (Region 8) is known with `rules` false and no
    `anadromous`: rainbow rules speak for every rainbow there. An unknown, ambiguous or duplicate entry stops the run; the curated copy carries the
    fingerprint of the generated list it was copied from.
  - a twin whose sibling does not bind is unresolved, never the whole known set; `build_reach`
    alone (the review app) cannot resolve the op.
"""
from __future__ import annotations

import json

import pytest

from pipeline.atlas.reach import steelhead as SH
from pipeline.atlas.reach.build import build_reach, build_reaches
from pipeline.atlas.reach.models import Outcome, Reason
from pipeline.common.models import NodeKind, StreamGraph, StreamNode
from pipeline.common.models.registry import RegistryItem
from pipeline.regs.parsing.catalogue import CatalogueEntry


def _node(nid, kind=NodeKind.stream):
    return StreamNode(node_id=nid, kind=kind, blk="1" if kind is NodeKind.stream else "",
                      down_m=0.0, up_m=1.0, length_m=1.0)


def _graph(*nodes):
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in nodes}
    return g


def _item(iid, secs, name="", kind="stream", mus=()):
    return RegistryItem(id=iid, name=name or iid, kind=kind, section_ids=tuple(secs), mus=mus)


# Cowichan River: c1 in Region 1 (a steelhead region), c8 in Region 8 (none). Tenas Lake (lk) and
# the Atnarko (a5) in Region 5. Two creeks called "Mill Creek", one in each of Regions 1 and 8.
# Vedder Canal (vc): a water DRAWN as a polygon (a lake node) in Region 2 whose name says it
# flows, so the registry made it a STREAM (`registry.flowing`, `item.kind`); the Chilliwack (h2)
# its river. Stellako River (s7) in Zone 7A, whose row only waives the stamp. Gravel Slough (gs) and
# Plain Lake (pl): polygon waters of Region 2 no row binds — a stream by the registry, and a lake.
G = _graph(_node("c1"), _node("c8"), _node("a5"), _node("lk", NodeKind.lake),
           _node("m1"), _node("m8"), _node("h2"), _node("vc", NodeKind.lake), _node("s7"),
           _node("gs", NodeKind.lake), _node("pl", NodeKind.lake))
REG = {
    "gnis:1": _item("gnis:1", ["c1", "c8"], "Cowichan River", mus=("1-4", "8-1")),
    "gnis:5": _item("gnis:5", ["a5"], "Atnarko River", mus=("5-11",)),
    "wbk:9": _item("wbk:9", ["lk"], "Tenas Lake", kind="lake", mus=("5-11",)),
    "gnis:21": _item("gnis:21", ["m1"], "Mill Creek", mus=("1-2",)),
    "gnis:28": _item("gnis:28", ["m8"], "Mill Creek", mus=("8-3",)),
    "gnis:2": _item("gnis:2", ["h2"], "Chilliwack River", mus=("2-4",)),
    "wbk:7": _item("wbk:7", ["vc"], "Vedder Canal", kind="stream", mus=("2-4",)),
    "gnis:7": _item("gnis:7", ["s7"], "Stellako River", mus=("7-12",)),
    "wbk:31": _item("wbk:31", ["gs"], "Gravel Slough", kind="stream", mus=("2-4",)),
    "wbk:32": _item("wbk:32", ["pl"], "Plain Lake", kind="lake", mus=("2-4",)),
    "area:region:1": _item("area:region:1", ["c1", "m1"], kind="area"),
    "area:region:2": _item("area:region:2", ["h2", "vc", "gs", "pl"], kind="area"),
    "area:region:5": _item("area:region:5", ["a5", "lk"], kind="area"),
    "area:region:7": _item("area:region:7", ["s7"], kind="area"),
    "area:region:8": _item("area:region:8", ["c8", "m8"], kind="area"),
}

STEELHEAD_REGIONS = ("1", "2", "5")
STREAMS = [{"op": "within", "area_id": f"area:region:{n}", "feature_types": ["stream"]}
           for n in STEELHEAD_REGIONS]
TWIN = lambda base, **kw: [{"op": "steelhead_waters", "siblings": [base], **kw}]  # noqa: E731


def _rule(rid, **kw):
    return {"rule_id": rid, "type": "retention_limit", "species": ["ST"],
            "verbatim": "All wild steelhead must be released.", **kw}


def _province():
    rules = []
    for base in ("steelhead.r1", "steelhead.r2", "steelhead.r4"):
        rules.append(_rule(base, extents=STREAMS, water="stream"))
        rules.append(_rule(base + "b", extents=TWIN(base)))
    stamp = {"kind": "requirement", "id": "steelhead_targeting", "extents": STREAMS,
             "water": "stream", "verbatim": "Steelhead Stamp"}
    twin = dict(stamp, id="steelhead_targeting_known",
                extents=TWIN("steelhead_targeting", outside_area_kind="national_parks"))
    twin.pop("water")
    return {"entry_id": SH.PROVINCE_STEELHEAD, "matched": [], "rules": rules,
            "licensing": [stamp, twin]}


def _zone5():
    """Region 5's "ALL STEELHEAD" on its streams, and its twin on Region 5's book-known waters."""
    base = [{"op": "within", "area_id": "area:region:5", "feature_types": ["stream"]}]
    return {"entry_id": "z5:trout_char_quota", "matched": [],
            "rules": [_rule("trout_char_quota.r6", extents=base),
                      _rule("trout_char_quota.r6b", extents=TWIN(
                          "trout_char_quota.r6", area_id="area:region:5",
                          feature_types=["lake", "wetland"]))]}


def _atnarko():
    """A steelhead row: its steelhead line binds the river; its spring closure binds the river
    "plus Tenas Lake" — the lake's only steelhead row is by that other line."""
    return {"entry_id": "r5:atnarko_river@5-11", "matched": ["gnis:5"], "anadromous_rainbow": True,
            "rules": [_rule("atnarko.r13", extents=[{"op": "whole"}]),
                      {"rule_id": "atnarko.r1", "type": "closure", "verbatim": "No Fishing",
                       "extents": [{"op": "whole"}, {"op": "whole", "item_id": "wbk:9"}]}]}


def _chilliwack():
    """A flagged row binding the river and the Vedder Canal (a lake-typed water named as flowing)."""
    return {"entry_id": "r2:chilliwack_vedder@2-4", "matched": ["gnis:2", "wbk:7"],
            "anadromous_rainbow": True,
            "rules": [{"rule_id": "cv.r1", "type": "bait_ban", "verbatim": "Bait ban",
                       "extents": [{"op": "whole"}]}]}


def _stellako():
    """A STAMP-WAIVER ROW: no rule names steelhead and it is not flagged, but its Class II
    designation prints "(Steelhead Stamp not required)" — steelhead are mentioned."""
    return {"entry_id": "r7:stellako_river@7-12", "matched": ["gnis:7"],
            "rules": [{"rule_id": "stellako.r2", "type": "retention_limit", "species": ["RB"],
                       "take": 0, "may_target": True,
                       "verbatim": "Rainbow trout catch and release",
                       "extents": [{"op": "whole"}]}],
            "licensing": [{"kind": "designation", "id": "stellako_river", "classified": "II",
                           "unit": "stellako_river", "unit_name": "Stellako River",
                           "extents": [{"op": "whole"}],
                           "steelhead_stamp_waived": {"verbatim": "Steelhead Stamp not required"},
                           "verbatim": "Class II water when open"}]}


def _run(entries, listed=()):
    return build_reaches(entries, REG, G, steelhead_list=[SH.ListWater(**w) for w in listed])


def _on(result, sid) -> set[str]:
    return {f"{b.entry_id}::{b.rule_id}" for b in result.bindings if sid in b.sections}


def _recs_on(result, sid) -> set[str]:
    return {p.record_id for p in result.licensing if sid in p.sections}


def _row(result, sid):
    return next((r for r in result.steelhead if r["section_id"] == sid), None)


def _code(result, sid):
    r = _row(result, sid)
    return r["steelhead"] if r else None


SET = {"zp:steelhead::steelhead.r1", "zp:steelhead::steelhead.r2", "zp:steelhead::steelhead.r4"}
TWINS = {f"{x}b" for x in SET}


def _regulation(result) -> dict:
    """Every rule binding and every licensing placement — what the list must never move."""
    return {"bindings": sorted((b.entry_id, b.rule_id, b.outcome.value, tuple(b.sections),
                                str(b.reason)) for b in result.bindings),
            "licensing": sorted((p.entry_id, p.record_id, p.placement, tuple(p.sections))
                                for p in result.licensing)}


def _anadromous(result) -> set[str]:
    """Where a rainbow over 50 cm is a steelhead."""
    return {r["section_id"] for r in result.steelhead if r["anadromous"]}


def _list_effect_problems(plain, got, listed) -> list[str]:
    """WHAT ADDING WATERS TO THE CURATED LIST MAY CHANGE (user ruling 2026-10-03): the presence code
    (only ever TO "known", only on the listed waters' sections) and `anadromous` (gained exactly on
    the listed sections that are streams — `water_kind.kind_of` — AND where steelhead rules apply,
    never lost) — nothing else: no rule binding, no licensing placement. Empty when that holds."""
    out = []
    if _regulation(got) != _regulation(plain):
        out.append("a rule binding or licensing placement moved")
    secs = {x for w in listed for x in REG[w["item_id"]].section_ids}
    before = {r["section_id"]: r["steelhead"] for r in plain.steelhead}
    after = {r["section_id"]: r["steelhead"] for r in got.steelhead}
    moved = {s for s in set(before) | set(after) if before.get(s) != after.get(s)}
    if not all(after.get(s) == SH.KNOWN for s in moved) or not moved <= secs:
        out.append(f"presence moved off the list or away from known: {sorted(moved)}")
    if {r["section_id"] for r in got.steelhead if r["listed"]} != secs:
        out.append("the listed sections are not the listed waters' sections")
    from pipeline.atlas.reach.water_kind import kind_of
    apply = SH.Presence.rules_apply(got.bindings)
    gain = {s for s in secs if kind_of(G, REG, s) == "stream" and s in apply}
    if _anadromous(got) != _anadromous(plain) | gain:
        out.append(f"anadromous {sorted(_anadromous(plain))} -> {sorted(_anadromous(got))}, "
                   f"want + {sorted(gain)}")
    rules = {r["section_id"] for r in got.steelhead if r["steelhead"] == SH.KNOWN and r["rules"]}
    if rules != {r["section_id"] for r in got.steelhead if r["steelhead"] == SH.KNOWN} & apply:
        out.append("`rules` is not 'the provincial set binds it'")
    return out


def test_the_corpus_twins_are_steelhead_waters_of_their_base():
    """The corpus: each provincial twin's one extent is the book-known waters minus its base; each
    zone wild release has a twin on the same area."""
    from pipeline.regs.parsing.io import read_all_entries
    raw = read_all_entries()
    e = CatalogueEntry.model_validate(raw["zp:steelhead"])
    got = {r.rule_id: r.extents for r in e.rules if r.rule_id.endswith("b")}
    assert got == {f"{b}b": TWIN(b) for b in ("steelhead.r1", "steelhead.r2", "steelhead.r4")}
    lic = {x.id: x.extents for x in e.licensing}
    assert lic["steelhead_targeting_known"] == TWIN("steelhead_targeting",
                                                    outside_area_kind="national_parks")
    for eid, base in (("z1:trout_quota", "trout_quota.r5"), ("z1:hg_quota", "hg_quota.r6"),
                      ("z2:trout_char_quota", "trout_char_quota.r7"),
                      ("z3:trout_char_quota", "trout_char_quota.r5"),
                      ("z5:trout_char_quota", "trout_char_quota.r6"),
                      ("z6:trout_char_quota", "trout_char_quota.r9")):
        rules = {r["rule_id"]: r for r in raw[eid]["rules"]}
        (bx,) = rules[base]["extents"]
        want = {"op": "steelhead_waters", "siblings": [base], "area_id": bx["area_id"]}
        if bx.get("outside_area"):
            want["outside_area"] = bx["outside_area"]
        want["feature_types"] = ["lake", "wetland"]
        assert rules[base + "b"]["extents"] == [want], eid
    from pipeline.regs.parsing.entry_models import Extent, Op
    x = Extent.model_validate(TWIN("steelhead.r1")[0])
    assert x.op is Op.STEELHEAD_WATERS and x.siblings == ["steelhead.r1"]
    for bad in ({"op": "steelhead_waters", "item_id": "gnis:1"},
                {"op": "steelhead_waters", "siblings": ["a"], "outside_area": "area:region:1"},
                {"op": "steelhead_waters", "siblings": ["a"], "item_ids": ["gnis:1"]}):
        with pytest.raises(ValueError):
            Extent.model_validate(bad)


def test_without_the_list_cowichan_is_possible_in_region_1_and_absent_in_region_8():
    got = _run([_province()])
    assert _code(got, "c1") == "possible" and _code(got, "c8") is None
    assert SET <= _on(got, "c1") and not _on(got, "c8")
    assert "steelhead_targeting" in _recs_on(got, "c1") and not _recs_on(got, "c8")
    # no steelhead row: the twins bind nothing and say so
    tw = {b.rule_id: b for b in got.bindings if b.rule_id.endswith("b")}
    assert all(b.outcome is Outcome.unresolved and b.reason is Reason.no_sections
               for b in tw.values()), tw


def test_a_listed_river_is_known_and_a_big_rainbow_is_a_steelhead_only_where_rules_apply():
    """THE CURATED LIST: Cowichan River, listed, is KNOWN on every section — "possible" turned
    "known" in Region 1, absent turned "known" in Region 8. In Region 1 steelhead rules apply, so a
    rainbow over 50 cm is a steelhead (`anadromous`); in Region 8 none does (`rules` false), so it
    stays a rainbow (user ruling 2026-10-03). NO rule moves: the same bindings and licensing
    placements as without the list. In Region 8 it carries no steelhead rule and no stamp."""
    entries = [_province(), _atnarko(), _zone5()]
    plain = _run(entries)
    listed = [{"name": "Cowichan River", "region": "1", "note": "winter run"}]
    got = _run(entries, listed)
    assert _code(got, "c1") == "known" and _code(got, "c8") == "known"
    assert _regulation(got) == _regulation(plain)
    assert _anadromous(got) == _anadromous(plain) | {"c1"}
    assert _list_effect_problems(plain, got, [{"item_id": "gnis:1"}]) == []
    assert SET <= _on(got, "c1") and not TWINS & _on(got, "c1")
    assert not _on(got, "c8") and not _recs_on(got, "c8")
    for s, apply in (("c1", True), ("c8", False)):
        r = _row(got, s)
        assert (r["entry_id"], r["regulations"], r["listed"], r["rules"], r["anadromous"]) == (
            SH.CURATED_LIST, False, True, apply, apply), r
    assert got.report.steelhead["curated"] == [
        {"item_id": "gnis:1", "listed": "'Cowichan River' (region 1)", "sections": 2}]
    assert got.report.steelhead["known_list_only"] == 2
    assert got.report.steelhead["anadromous_list_only"] == 1
    assert got.report.steelhead["known_no_rules"] == 1
    # the other Mill Creek and the rest of Region 1 are untouched
    assert _code(got, "m1") == "possible" and _code(got, "m8") is None


LISTS = [
    [{"item_id": "gnis:1"}],                     # a river across a steelhead region and Region 8
    [{"item_id": "gnis:28"}],                    # a Region 8 creek
    [{"item_id": "gnis:21"}],                    # a Region 1 creek ("possible" -> known)
    [{"item_id": "wbk:9"}],                      # a lake a steelhead row binds
    [{"item_id": "wbk:31"}],                     # a lake-typed slough no row binds (flows)
    [{"item_id": "wbk:32"}],                     # a lake no row binds (does not flow)
    [{"item_id": "gnis:5"}, {"item_id": "wbk:7"}],   # waters a steelhead row already binds
]


@pytest.mark.parametrize("listed", LISTS)
def test_adding_any_water_to_the_list_changes_only_presence_and_anadromous(listed):
    """Mutation over the list: whatever is added, every rule binding and licensing placement is the
    same; only the `steelhead` code may change (only to "known", only on the listed waters) and
    `anadromous` (gained exactly on the listed STREAM sections where steelhead rules apply — a
    Region 1 creek and a Region 2 slough yes, a Region 8 creek and a lake no)."""
    entries = [_province(), _atnarko(), _zone5(), _chilliwack(), _stellako()]
    plain, got = _run(entries), _run(entries, listed)
    assert _list_effect_problems(plain, got, listed) == []
    # and the check is not vacuous: a steelhead row DOES move the regulation
    assert _regulation(_run(entries[:2])) != _regulation(plain)


def test_the_list_effect_check_sees_every_other_change():
    """MUTATIONS of a correct run, each of which `_list_effect_problems` must refuse: a rule bound
    through the list (the twin reaching a listed stream), a listed stream where a big rainbow is not
    a steelhead (the pre-2026-10-03 reading), a listed stream NO steelhead rule binds made steelhead
    water (the first 2026-10-03 reading), a listed LAKE made steelhead water, steelhead water lost
    elsewhere, and a presence code moved off the list."""
    import copy
    entries = [_province(), _atnarko(), _zone5(), _chilliwack(), _stellako()]
    plain = _run(entries)
    listed = [{"item_id": "gnis:21"}, {"item_id": "gnis:28"}, {"item_id": "wbk:32"}]
    got = _run(entries, listed)
    assert _list_effect_problems(plain, got, listed) == []
    assert _row(got, "m1")["anadromous"] and not _row(got, "m8")["anadromous"]

    def mutated(fn):
        m = copy.deepcopy(got)
        fn(m)
        return _list_effect_problems(plain, m, listed)

    def bind(m):
        import dataclasses
        m.bindings[0] = dataclasses.replace(m.bindings[0],
                                            sections=tuple(m.bindings[0].sections) + ("m8",))

    def row(sid, **kw):
        return lambda m: next(r for r in m.steelhead if r["section_id"] == sid).update(kw)
    assert mutated(bind)
    assert mutated(row("m1", anadromous=False))                 # book-only anadromous
    assert mutated(row("m8", anadromous=True))                  # a big rainbow a steelhead where
    #                                                             no steelhead rule applies
    assert mutated(row("m8", rules=True))                       # `rules` without the set
    assert mutated(row("pl", anadromous=True))                  # a listed lake made steelhead water
    assert mutated(row("a5", anadromous=False))                 # lost on the book's water
    assert mutated(lambda m: m.steelhead.append({"section_id": "c8", "steelhead": SH.KNOWN,
                                                 "listed": False, "anadromous": False,
                                                 "rules": False, "regulations": False}))


def test_the_list_by_item_id_is_the_same_water():
    a = _run([_province()], [{"name": "Cowichan River"}])
    b = _run([_province()], [{"item_id": "gnis:1"}])
    assert a.steelhead == b.steelhead


@pytest.mark.parametrize("listed,why", [
    ([{"name": "Mill Creek"}], "ambiguous"),
    ([{"name": "Nowhere River"}], "no water of that name"),
    ([{"name": "Cowichan River", "region": "5"}], "no water of that name"),
    ([{"item_id": "gnis:404"}], "no such water"),
    ([{"item_id": "gnis:1", "mu": "5-11"}], "the registry puts"),
    ([{"item_id": "gnis:1"}, {"name": "Cowichan River"}], "already listed"),
])
def test_the_list_refuses_what_it_cannot_resolve(listed, why):
    with pytest.raises(SystemExit) as e:
        _run([_province()], listed)
    assert why in str(e.value)


def test_a_region_or_mu_resolves_the_ambiguous_name():
    got = _run([_province()], [{"name": "mill  creek", "mu": "8-3"}])
    assert _code(got, "m8") == "known" and not _on(got, "m8")


@pytest.mark.parametrize("bad", [{}, {"item_id": "a", "name": "b"}, {"name": "x", "region": "9"},
                                 {"name": "x", "mu": "1"}, {"name": "x", "extra": 1}])
def test_a_list_entry_is_refused_by_shape(bad):
    with pytest.raises(ValueError):
        SH.ListWater(**bad)


def test_the_curated_file_loads_and_is_documented():
    from pipeline.common.curated import CURATED
    path = CURATED.regulations.steelhead_waters
    doc = json.loads(path.read_text(encoding="utf-8"))
    said = " ".join(doc["$comment"])
    assert "item_id" in said and "region" in said and "binds NO rule" in said
    assert "anadromous_rainbow" in said and "2026-10-03" in said
    assert "pipeline.regs.steelhead.known_waters" in said
    assert len(SH.load_list()) == len(doc["waters"]) > 600


def test_the_curated_list_is_the_generated_list_it_says_it_is():
    """The curated copy records the fingerprint of the generated list it was copied from, and that
    fingerprint is its own `waters`. When the generator's last output is on disk, it must be that
    same list — a regenerated list that was not copied (or a hand edit to the copy) fails here."""
    from pathlib import Path
    doc = SH.load_document()
    assert doc.generated is not None, "the curated list does not say which generated list it is"
    assert SH.fingerprint(doc.waters) == doc.generated.fingerprint
    gen = Path(__file__).resolve().parents[2] / doc.generated.source
    if not gen.exists():
        pytest.skip(f"the generator's output {gen} is not on disk")
    out = json.loads(gen.read_text(encoding="utf-8"))
    assert SH.fingerprint(out["waters"]) == doc.generated.fingerprint, (
        f"{gen} is not the list data/curated/regulations/steelhead_waters.json was copied from — "
        f"copy it over (keeping the curated $comment) or regenerate deliberately")


def test_a_missing_list_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        SH.load_list(tmp_path / "nope.json")


def _tenas_own():
    """The lake's OWN row printing steelhead (as Khartoum's and Lois's do: "hatchery steelhead")."""
    return {"entry_id": "r5:tenas_lake@5-11", "matched": ["wbk:9"],
            "rules": [_rule("tenas.r1", extents=[{"op": "whole"}])]}


def test_a_lake_another_line_of_a_steelhead_row_binds_is_not_steelhead_water(monkeypatch):
    """FIX D13 (user ruling 2026-10-06): Tenas Lake is bound by the Atnarko row's spring closure,
    not by its steelhead line, and its own row prints no steelhead — so it is NOT steelhead water:
    no twin, no stamp twin, no zone release twin, no presence code. The river keeps the base rules
    and is known. MUTATION: `LAKES_ONLY_BY_OWN_ROW` off, the lake is known and carries the set (the
    2026-10-02 reading)."""
    got = _run([_province(), _zone5(), _atnarko()])
    assert _code(got, "lk") is None and _code(got, "a5") == "known"
    assert not (TWINS | SET) & _on(got, "lk")
    assert "z5:trout_char_quota::trout_char_quota.r6b" not in _on(got, "lk")
    assert "steelhead_targeting_known" not in _recs_on(got, "lk")
    assert "r5:atnarko_river@5-11::atnarko.r1" in _on(got, "lk"), "the closure itself still binds"
    assert SET <= _on(got, "a5") and not TWINS & _on(got, "a5")
    assert _row(got, "a5")["anadromous"] is True
    monkeypatch.setattr(SH, "LAKES_ONLY_BY_OWN_ROW", False)
    got = _run([_province(), _zone5(), _atnarko()])
    assert _code(got, "lk") == "known" and TWINS <= _on(got, "lk")


def test_a_lake_whose_own_row_prints_steelhead_carries_the_set():
    """The lake's OWN row prints steelhead (Khartoum, Lois): KNOWN, the twins, the stamp twin and the
    zone's release twin; a rainbow over 50 cm stays a rainbow on a lake. Listing a lake instead
    makes it known with no rule at all from the list."""
    got = _run([_province(), _zone5(), _atnarko(), _tenas_own()])
    assert _code(got, "lk") == "known"
    assert TWINS <= _on(got, "lk") and not SET & _on(got, "lk")
    assert "z5:trout_char_quota::trout_char_quota.r6b" in _on(got, "lk")
    assert "z5:trout_char_quota::trout_char_quota.r6" not in _on(got, "lk")
    assert "steelhead_targeting_known" in _recs_on(got, "lk")
    r = _row(got, "lk")
    assert (r["kind"], r["regulations"], r["anadromous"]) == ("lake", True, False)
    got = _run([_province(), _zone5(), _atnarko()], [{"item_id": "wbk:9"}])
    assert _code(got, "lk") == "known" and not (TWINS | {
        "z5:trout_char_quota::trout_char_quota.r6b"}) & _on(got, "lk")


def test_the_zone_twin_keeps_to_its_own_area_and_to_lakes():
    """A zone's twin binds the book-known LAKES of ITS area only: Region 5's release twin does not
    reach the Vedder Canal (Region 2), which the Chilliwack row makes book-known; and it never binds
    a stream, even one inside the area that its base leaves (a piece homed in another region)."""
    got = _run([_province(), _zone5(), _chilliwack()])
    assert "z5:trout_char_quota::trout_char_quota.r6b" not in _on(got, "vc")
    # the canal is a STREAM (`water_kind.kind_of`): Region 2's base binds it, not the twins
    assert SET <= _on(got, "vc") and not TWINS & _on(got, "vc")
    z = _zone5()
    # the base leaves the Atnarko piece (as if homed in another region) and binds elsewhere
    z["rules"][0]["extents"] = [{"op": "within", "area_id": "area:region:5",
                                 "outside_area": "area:region:5b", "feature_types": ["stream"]},
                                {"op": "within", "area_id": "area:region:1",
                                 "feature_types": ["stream"]}]
    REG["area:region:5b"] = _item("area:region:5b", ["a5"], kind="area")
    try:
        got = _run([_province(), z, _atnarko(), _tenas_own()])
        assert "z5:trout_char_quota::trout_char_quota.r6" not in _on(got, "a5")
        assert "z5:trout_char_quota::trout_char_quota.r6b" not in _on(got, "a5")
        assert "z5:trout_char_quota::trout_char_quota.r6b" in _on(got, "lk")
    finally:
        REG.pop("area:region:5b")


def test_a_lake_typed_water_named_as_flowing_is_a_stream():
    """A water drawn as a polygon whose name's head noun flows is a STREAM — decided ONCE by the
    registry (`common.water_kind.flows` -> `registry.flowing`, `item.kind`) and read by the reach
    as the owner's kind (`water_kind.kind_of`; user ruling 2026-10-03): the Vedder Canal, bound by
    the flagged Chilliwack/Vedder row, is steelhead water; Gravel Slough (no row) is a stream the
    provincial steelhead rules bind — "possible" — and, listed, steelhead water; Tenas Lake and
    Plain Lake stay lakes."""
    from pipeline.atlas.reach.water_kind import kind_of
    from pipeline.common.water_kind import flows
    assert flows("lake", "Vedder Canal") and flows("lake", "Gravel Slough")
    assert flows("lake", "Sumas Lake Canal") and flows("lake", "Rancheria River")
    assert flows("stream", "Unnamed") and not flows("lake", "Tenas Lake")
    for still in ("Bear Creek Reservoir", "Corn Creek Marsh", "River Lakes", "OKANAGAN RIVER OXBOWS",
                  "Fowl Creek Lake No. 1*", "Pete'S Pond Unnamed Lake At The Head Of San Juan River"):
        assert not flows("lake", still), still
    assert [kind_of(G, REG, s) for s in ("vc", "gs", "pl", "lk", "h2")] == [
        "stream", "stream", "lake", "lake", "stream"]
    assert kind_of(G, None, "vc") == "lake"                    # no registry: the graph's kind
    got = _run([_province()])
    assert _code(got, "gs") == "possible" and SET <= _on(got, "gs")
    assert _code(got, "pl") is None and not _on(got, "pl")
    got = _run([_province(), _chilliwack(), _atnarko()])
    assert _row(got, "vc")["anadromous"] is True and _row(got, "h2")["anadromous"] is True
    assert _row(got, "lk") is None                         # a lake no own row makes steelhead water
    assert got.report.steelhead["anadromous"] == 3                         # vc, h2, a5
    # LISTED, a lake-typed slough no row binds is steelhead water too; a plain lake is not
    got = _run([_province(), _chilliwack(), _atnarko()], [{"item_id": "wbk:31"},
                                                          {"item_id": "wbk:32"}])
    assert _row(got, "gs")["anadromous"] is True and _row(got, "pl")["anadromous"] is False
    assert got.report.steelhead["anadromous"] == 4


def test_a_stamp_waiver_row_is_a_steelhead_row():
    """"Steelhead Stamp not required" PRINTS steelhead: the Stellako row is a steelhead row, so
    its water in Zone 7A (no steelhead region) is book-known, carries the provincial set through the
    twins and the stamp twin, and a big rainbow there is a steelhead."""
    e = _stellako()
    assert SH.steelhead_row(e) and SH.prints_steelhead(e) and not SH.names_steelhead(e)
    assert SH.steelhead_row(CatalogueEntry.model_validate({
        **e, "name": "Stellako River", "regs_verbatim": "Rainbow trout catch and release "
        "Class II water when open (Steelhead Stamp not required)"}))
    bare = dict(e, licensing=[])
    assert not SH.steelhead_row(bare)
    got = _run([_province(), e])
    assert _code(got, "s7") == "known" and TWINS <= _on(got, "s7")
    assert "steelhead_targeting_known" in _recs_on(got, "s7")
    assert _row(got, "s7")["anadromous"] is True
    got = _run([_province(), bare])
    assert _code(got, "s7") is None and not TWINS & _on(got, "s7")


def test_there_is_no_tributary_walk():
    """`Presence` has no walk: book-known is what the steelhead rows' rules bind, nothing more."""
    assert not hasattr(SH, "WALK_RULE")
    import inspect
    assert "tributar" not in inspect.getsource(SH.Presence.close).lower()


def test_a_twin_whose_sibling_does_not_bind_is_unresolved():
    p = _province()
    for r in p["rules"]:
        if r["rule_id"] == "steelhead.r1":
            r["extents"] = [{"op": "within", "area_id": "area:region:4", "feature_types": ["stream"]}]
    got = _run([p, _atnarko()])
    b = next(b for b in got.bindings if b.rule_id == "steelhead.r1b")
    assert b.outcome is Outcome.unresolved and b.reason is Reason.complement_unknown


def test_build_reach_alone_cannot_resolve_the_known_waters():
    p = _province()
    twin = next(r for r in p["rules"] if r["rule_id"] == "steelhead.r1b")
    b, _ = build_reach(p, twin, REG, G)
    assert b.outcome is Outcome.unresolved and b.reason is Reason.needs_corpus


def test_the_catalogue_refuses_the_op_on_a_water_row_and_beside_another_extent():
    base = {"entry_id": "r1:x@1-1", "name": "X", "regs_verbatim": "All wild steelhead must be "
            "released.", "matched": ["gnis:1"], "extents": [{"op": "whole"}]}
    rule = {"rule_id": "x.r1", "type": "retention_limit", "species": ["ST"], "take": 0,
            "may_target": True, "verbatim": "All wild steelhead must be released.",
            "extents": TWIN("x.r0")}
    with pytest.raises(ValueError, match="steelhead_waters"):
        CatalogueEntry.model_validate({**base, "rules": [rule]})


def test_a_flagged_row_with_no_rule_makes_its_own_water_known():
    """A steelhead row's OWN WATER is book-known even where it prints no rule (the Kingcome, Wakeman,
    Seymour rows of Region 1 print only their Class II water and the stamp): its water carries the
    set and a big rainbow there is a steelhead. Its tributaries are not walked."""
    row = {"entry_id": "r8:mill_creek@8-3", "matched": ["gnis:28"], "anadromous_rainbow": True,
           "rules": [],
           "licensing": [{"kind": "designation", "id": "mill_creek", "classified": "II",
                          "unit": "mill_creek", "unit_name": "Mill Creek",
                          "extents": [{"op": "whole"}],
                          "steelhead_stamp_waived": {"verbatim": "Steelhead Stamp not required "
                                                                 "unless fishing for steelhead"},
                          "verbatim": "Class II water"}]}
    got = _run([_province(), row])
    r = _row(got, "m8")
    assert (r["steelhead"], r["regulations"], r["anadromous"], r["entry_id"]) == (
        "known", True, True, "r8:mill_creek@8-3")
    assert TWINS <= _on(got, "m8") and "steelhead_targeting_known" in _recs_on(got, "m8")
    assert got.report.steelhead["by_entry"]["r8:mill_creek@8-3"] == {"sections": 1}
    # without the flag (and nothing else naming steelhead) the water is nobody's steelhead water
    got = _run([_province(), dict(row, anadromous_rainbow=False, licensing=[])])
    assert _code(got, "m8") is None
    # EVERY STAMP WORDING PRINTS STEELHEAD (user ruling 2026-10-02, as corrected): a row with only
    # its Class II water and "Steelhead Stamp mandatory <dates>", or a waiver "unless fishing for
    # steelhead", is a steelhead row unflagged; a designation printing no steelhead is not
    cw = dict(row, anadromous_rainbow=False)
    assert SH.steelhead_row(cw) and SH.prints_steelhead(cw)
    got = _run([_province(), cw])
    assert _code(got, "m8") == "known" and _row(got, "m8")["anadromous"] is True
    during = dict(cw, licensing=[dict(cw["licensing"][0], when={"dates": [
        {"from_month": 4, "from_day": 1, "to_month": 10, "to_day": 31}]},
        verbatim="Class II water Apr 1-Oct 31", steelhead_stamp_during={
            "when": {"dates": [{"from_month": 4, "from_day": 1, "to_month": 6, "to_day": 30}]},
            "verbatim": "Steelhead Stamp mandatory Apr 1-June 30"})])
    del during["licensing"][0]["steelhead_stamp_waived"]
    assert SH.steelhead_row(during)
    assert SH.steelhead_row(CatalogueEntry.model_validate(
        {**during, "name": "Mill Creek", "regs_verbatim": "Class II water Apr 1-Oct 31; "
         "Steelhead Stamp mandatory Apr 1-June 30", "extents": [{"op": "whole"}]}))
    plain = dict(cw, licensing=[{k: v for k, v in cw["licensing"][0].items()
                                 if k != "steelhead_stamp_waived"}])
    assert not SH.steelhead_row(plain) and not SH.prints_steelhead(plain)
    got = _run([_province(), plain])
    assert _code(got, "m8") is None


def test_the_list_marks_only_water_in_bc():
    """A listed river that crosses the border (the Taku into Alaska) is known in B.C. only: the
    border holds for the indicator as it does for every binding."""
    g = _graph(_node("t1"), StreamNode(node_id="t9", kind=NodeKind.stream, blk="1", down_m=0.0,
                                       up_m=1.0, length_m=1.0, out_of_bc=True))
    reg = {"gnis:8231": _item("gnis:8231", ["t1", "t9"], "Taku River", mus=("6-26",)),
           "area:region:6": _item("area:region:6", ["t1"])}
    got = build_reaches([_province()], reg, g, steelhead_list=[SH.ListWater(item_id="gnis:8231")])
    assert _code(got, "t1") == "known" and _code(got, "t9") is None
    assert got.report.steelhead["curated"][0]["sections"] == 1


def test_a_slough_is_a_stream_for_every_rule():
    """EVERY "in streams" rule binds a lake-typed water whose name flows, and no "in lakes" rule
    does (user ruling 2026-10-03: a slough is a stream everywhere, not only for steelhead) — the
    one point is `water_kind.kind_of`, read by every `feature_types` filter. Mutation: with the
    graph's kind alone (no registry) the slough would answer to the lake rule instead."""
    closure = {"entry_id": "z2:spring_stream_closure", "matched": [], "rules": [
        {"rule_id": "spring.r1", "type": "closure", "verbatim": "No fishing in any stream",
         "species": ["ALL_GAME_FISH"],
         "extents": [{"op": "within", "area_id": "area:region:2", "feature_types": ["stream"]}]},
        {"rule_id": "lakes.r1", "type": "gear_restriction", "verbatim": "Set lining in lakes",
         "extents": [{"op": "within", "area_id": "area:region:2", "feature_types": ["lake"]}]}]}
    got = _run([closure])
    stream = next(b for b in got.bindings if b.rule_id == "spring.r1")
    lake = next(b for b in got.bindings if b.rule_id == "lakes.r1")
    assert set(stream.sections) == {"h2", "vc", "gs"} and set(lake.sections) == {"pl"}
    from pipeline.atlas.reach import extent
    real = extent._kind_of
    try:
        extent._kind_of = lambda g, s, reg=None: real(g, s, None)
        got = _run([closure])
        assert {s for b in got.bindings if b.rule_id == "lakes.r1" for s in b.sections} \
            == {"vc", "gs", "pl"}
    finally:
        extent._kind_of = real
