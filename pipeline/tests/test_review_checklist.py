"""T532: THE CHECKLIST ROWS — generated for every DFO place and every regulation rule with extents, honest states
(a planted missing exception / tributary mismatch / pending cut is ✗ or ?), stale on change, and the review queue
grouped by water (owner 2026-11-09: never leave a water before all its places are listed)."""
from __future__ import annotations

import json
from collections import Counter

import pytest

from pipeline.atlas.reach.classify import wants_tributaries
from pipeline.common.curated import CURATED
from pipeline.regs.review import checklist as C
from pipeline.regs.review import dfo as D
from pipeline.regs.review import marks as M

SHA = "0" * 64


def _curated_ctx() -> C.Context:
    """the curated cuts as built (no atlas in CI): every authored cut, by its label"""
    cuts = json.loads(CURATED.waters.cuts.read_text())["waterbodies"]
    labels = {s["id"]: s.get("label") or "" for w in cuts for s in w.get("splits") or []}
    return C.Context(labels=labels, built=frozenset(labels))


def _covered(rs: list[dict], src: C.Source, own: list[str], ctx: C.Context) -> list[str]:
    """every place phrase is in some row's quote (the unchecked row included) or names only the place's water"""
    named = C._tokens(src.water).union(*(C._tokens(ctx.items.get(i, i)) for i in own)) | {"upstream", "downstream"}
    quotes = " ".join(r["quote"] for r in rs)
    return [c.text for c in C._clauses(src.place, src.place_only) if c.text not in quotes and not c.toks <= named]


def _check(rs: list[dict]) -> None:
    assert [r["n"] for r in rs] == list(range(1, len(rs) + 1))
    for r in rs:
        assert r["state"] in C.STATES and r["kind"] in C.KINDS, r
        assert isinstance(r["quote"], str) and r["ours"], r
        assert r["state"] == C.OK or r["reason"], f"a ? / ✗ row says why: {r}"


def test_every_curated_dfo_place_has_rows_and_every_phrase_is_covered():
    ctx, by = _curated_ctx(), Counter()
    n = 0
    for _lid, (_s, loc, water) in D.locations(CURATED.dfo_salmon.places).items():
        if not loc.get("water_id"):
            continue
        n += 1
        b = loc.get("binding") or {}
        own = list((water or {}).get("item_ids") or [])
        src = C.dfo_source(loc, water)
        rs = C.rows(src, b.get("extents") or [], own_items=own, ctx=ctx, tributaries=b.get("tributaries"),
                    tributaries_only=bool(b.get("tributaries_only")),
                    tributary_excludes=b.get("tributary_excludes") or [])
        _check(rs)
        assert not _covered(rs, src, own, ctx), _lid
        by.update(r["state"] for r in rs)
    print(f"DFO places {n}: rows by state {dict(by)}")
    assert n > 300 and by[C.OK] and by[C.BAD]


def test_every_regulation_rule_with_extents_has_rows_and_every_phrase_is_covered():
    ctx, by, n = _curated_ctx(), Counter(), 0
    for f in sorted(CURATED.regulations.synopsis.glob("region-*.json")):
        for e in json.loads(f.read_text())["entries"]:
            for r in e.get("rules") or []:
                if not r.get("extents"):
                    continue
                n += 1
                own = list(e.get("matched") or [])
                src = C.rule_source(e, r)
                only = bool(r.get("tributaries_only"))
                rs = C.rows(src, r["extents"], own_items=own, ctx=ctx,
                            tributaries=wants_tributaries(r, e) and not only, tributaries_only=only,
                            tributary_excludes=r.get("tributary_excludes") or [])
                _check(rs)
                assert not _covered(rs, src, own, ctx), (e["entry_id"], r["rule_id"])
                by.update(x["state"] for x in rs)
    print(f"regulation rules {n}: rows by state {dict(by)}")
    assert n > 3000


CTX = C.Context(labels={"a_river__mill_bridge": "Mill Road bridge", "a_river__falls": "the falls",
                        "a_river__new_weir": "the new weir"},
                items={"gnis:1": "A River", "gnis:2": "Bee Creek"},
                built=frozenset({"a_river__mill_bridge", "a_river__falls"}),
                pending=frozenset({"a_river__new_weir"}))


def _rows(place, extents, **kw):
    return C.rows(C.Source(water="A River", place=place), extents, own_items=["gnis:1"], ctx=CTX, **kw)


def _states(rs):
    return {r["kind"]: r["state"] for r in rs}


def test_a_built_reach_named_by_its_words_is_all_ok():
    rs = _rows("from the Mill Road bridge to the falls", [{"op": "between", "splits": ["a_river__mill_bridge",
                                                                                        "a_river__falls"]}])
    assert [r["state"] for r in rs] == [C.OK] * len(rs)
    assert [r["kind"] for r in rs] == ["water", "from", "to", "tributaries"]
    assert rs[1]["anchor"]["cuts"] == ["a_river__mill_bridge"]


def test_a_missing_exception_is_bad_and_a_named_one_ok():
    words = "downstream of the Mill Road bridge except Bee Creek"
    ext = {"op": "downstream_of", "splits": ["a_river__mill_bridge"]}
    held = _rows(words, [{**ext, "outside_items": ["gnis:2"]}])
    assert _states(held)["except"] == C.OK
    dropped = _rows(words, [ext])                          # the planted mutation: the carve-out is gone
    assert [r for r in dropped if r["kind"] == "except"][0]["state"] == C.BAD
    assert [r for r in dropped if r["kind"] == "except"][0]["quote"] == "except Bee Creek"


def test_a_tributary_call_against_the_words_is_bad():
    ext = [{"op": "whole"}]
    assert _states(_rows("the whole river including tributaries", ext, tributaries=True))["tributaries"] == C.OK
    assert _states(_rows("the whole river including tributaries", ext, tributaries=False))["tributaries"] == C.BAD
    assert _states(_rows("", ext, tributaries=True))["tributaries"] == C.BAD       # walks with no word for it
    assert _states(_rows("", ext, tributaries=False))["tributaries"] == C.OK
    assert _states(_rows("excluding tributaries", ext, tributaries=True))["tributaries"] == C.BAD


def test_a_pending_or_unknown_cut_is_never_ok():
    # owner 2026-11-09: "always add ✗ when unsure" — there is no ? state
    assert C.STATES == (C.OK, C.BAD)
    pend = _rows("upstream of the new weir", [{"op": "upstream_of", "splits": ["a_river__new_weir"]}])
    assert _states(pend)["from"] == C.BAD and "not built yet; check again after the rebuild" in pend[1]["reason"]
    gone = _rows("upstream of the old dam", [{"op": "upstream_of", "splits": ["a_river__old_dam"]}])
    assert _states(gone)["from"] == C.BAD
    saved = _rows("upstream of the falls", [{"op": "upstream_of", "splits": ["a_river__falls"]}], landed=[False])
    assert _states(saved)["from"] == C.BAD


def test_words_no_row_checks_are_their_own_row_and_a_named_end_with_no_cut_is_bad():
    rs = _rows("downstream of the Mill Road bridge to signs at the old ferry; also the gravel bar",
               [{"op": "downstream_of", "splits": ["a_river__mill_bridge"]}])
    assert _states(rs)["to"] == C.BAD
    left = [r for r in rs if r["kind"] == "unchecked"]
    assert left and "gravel bar" in left[0]["quote"] and left[0]["state"] == C.BAD
    whole = _rows("upstream of the falls", [{"op": "whole"}])
    assert _states(whole)["where"] == C.BAD                   # the words name a part, ours is the whole water


def test_the_digest_moves_with_the_words_and_the_extent_never_with_the_piece_order():
    src = C.Source(water="A River", place="upstream of the falls")
    a, b = {"op": "upstream_of", "splits": ["a_river__falls"]}, {"op": "whole", "item_id": "gnis:2"}
    d = C.digest(src, [a, b])
    assert C.digest(src, [b, a]) == d
    assert C.digest(C.Source(water="A River", place="upstream of the old falls"), [a, b]) != d
    assert C.digest(src, [a]) != d
    assert C.digest(src, [a, b], tributaries=True) != d


def _mark(verdict, rows, **kw):
    return {"scope": "dfo", "target": "1:a:b", "verdict": verdict, "answers": {"P": "y" if verdict == "verified"
                                                                                else "n"},
            "reviewer": "t", "date": "2026-11-09", "pdf_sha256": SHA,
            "prints": {"text": "t", "records": [{"key": "entry", "meaning": "m", "binding": "b"}]},
            "checklist": {"digest": "0123456789abcdef", "rows": rows}, **kw}


def test_a_checklist_mark_is_all_correct_only_when_every_row_is_ok_or_ticked():
    ok, bad = {"n": 1, "kind": "water", "state": "ok"}, {"n": 2, "kind": "except", "state": "bad"}
    with pytest.raises(ValueError, match="every row"):
        M.Mark.model_validate(_mark("verified", [ok, bad]))
    M.Mark.model_validate(_mark("verified", [ok, {**bad, "ticked": True}]))
    with pytest.raises(ValueError, match="names the row"):
        M.Mark.model_validate(_mark("wrong", [ok, bad], problem="split missing", note="n"))
    m = M.Mark.model_validate(_mark("wrong", [ok, {**bad, "wrong": True}], problem="split missing", note="n"))
    assert json.loads(M.line(m))["checklist"]["digest"] == "0123456789abcdef"
    plain = M.Mark.model_validate({k: v for k, v in _mark("wrong", [ok], problem="p", note="n").items()
                                   if k != "checklist"} | {"answers": {"T": "y", "R": "y", "P": "n"}})
    assert "checklist" not in json.loads(M.line(plain))      # an old mark's line is unchanged


def _backend(monkeypatch, name):
    import sys
    from pathlib import Path
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / "curation-review" / "backend"))
    sys.modules.pop(name, None)
    return __import__(name)


def test_the_queue_never_leaves_a_water_before_all_its_places(monkeypatch):
    """owner 2026-11-09: a whole river at one time — every place of a water (its DFO places, then its regulation
    extents) comes before the next water, whatever order the sources listed them in"""
    B = _backend(monkeypatch, "checks")
    places = [{"source": s, "id": f"{s}{i}", "water_key": w} for i, (s, w) in enumerate(
        [("regs", "a"), ("dfo", "b"), ("dfo", "a"), ("regs", "c"), ("regs", "b"), ("dfo", "a"), ("regs", "a")])]
    got = B.by_water(places)
    keys = [p["water_key"] for p in got]
    seen: list[str] = []
    for k in keys:
        assert k == (seen[-1] if seen else k) or k not in seen, f"left a water and came back: {keys}"
        if not seen or seen[-1] != k:
            seen.append(k)
    assert keys == ["a"] * 4 + ["b"] * 2 + ["c"]
    assert [p["source"] for p in got if p["water_key"] == "a"] == ["dfo", "dfo", "regs", "regs"]
    assert sorted(p["id"] for p in got) == sorted(p["id"] for p in places)


def test_a_check_is_stale_when_its_digest_is_not_the_places_now(monkeypatch):
    B = _backend(monkeypatch, "checks")
    m = M.Mark.model_validate(_mark("verified", [{"n": 1, "kind": "water", "state": "ok"}]))
    assert B.check_state(None, "x") == "unchecked"
    assert B.check_state(m, "0123456789abcdef") == "checked"
    assert B.check_state(m, "fedcba9876543210") == "stale"


def test_a_locator_is_one_phrase_and_a_closing_direction_is_the_ends_own(monkeypatch):
    """owner look 2026-11-09 (Chilliwack): "100 m from the confluence of … Slesse Creek" is the FROM cut's own place, and
    the reach's other end is just "downstream"; the Sumas piece's end names signs with no cut (✗). Water and tributary
    rows carry no pin; a cut and a natural end each sit at their own point."""
    words = ("from a line between 2 fishing boundary signs on either side of the Chilliwack River 100 m from the "
             "confluence of the Chilliwack River and Slesse Creek downstream including that portion of the Sumas River "
             "from the Barrow Town Pump Station downstream to fishing boundary signs near the confluence with the "
             "Fraser River.")
    ctx = C.Context(labels={"chv__slesse": "fishing boundary signs 100 m below the Slesse Creek confluence",
                            "sumas__barrow": "Barrow Town Pump Station"},
                    items={"gnis:1": "Chilliwack River", "gnis:2": "Vedder River", "gnis:3": "Sumas River"},
                    built=frozenset({"chv__slesse", "sumas__barrow"}),
                    points={"chv__slesse": (1, 1), "sumas__barrow": (2, 2), "gnis:1": (9, 9),
                            C.end_key("mouth", ["gnis:1", "gnis:2"]): (3, 3), C.end_key("mouth", ["gnis:3"]): (4, 4)})
    rs = C.rows(C.Source(water="Chilliwack/Vedder River (including Sumas River)", place=words),
                [{"op": "downstream_of", "splits": ["chv__slesse"], "item_ids": ["gnis:1", "gnis:2"]},
                 {"op": "downstream_of", "splits": ["sumas__barrow"], "item_id": "gnis:3"}],
                own_items=["gnis:1", "gnis:2", "gnis:3"], ctx=ctx)
    by = [(r["kind"], r["quote"], r["state"], r["anchor"].get("coord")) for r in rs]
    assert by[1][0] == "from" and by[1][1].endswith("100 m from the confluence of the Chilliwack River and Slesse Creek")
    assert by[1][3] == [1, 1]
    assert by[2] == ("to", "downstream", C.OK, [3, 3])
    assert by[3][:2] == ("also", "from the Barrow Town Pump Station") and by[3][3] == [2, 2]
    assert by[4] == ("to", "to fishing boundary signs near the confluence with the Fraser River", C.BAD, [4, 4])
    assert "coord" not in rs[0]["anchor"] and all("coord" not in r["anchor"] for r in rs if r["kind"] == "tributaries")


def test_one_water_joins_both_sources_on_a_shared_registry_water(monkeypatch):
    B = _backend(monkeypatch, "checks")
    got = B.one_water([{"source": "regs", "water": "CHILLIWACK RIVER", "water_key": "g1", "items": ["g1"]},
                       {"source": "dfo", "water": "Chilliwack/Vedder", "water_key": "g1|g2", "items": ["g1", "g2"]},
                       {"source": "regs", "water": "Zone A", "water_key": "name:zone a", "items": []}])
    assert [p["water_key"] for p in got] == ["g1", "g1", "name:zone a"]
    assert [p["water"] for p in got] == ["Chilliwack/Vedder", "Chilliwack/Vedder", "Zone A"]
