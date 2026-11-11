"""T532: THE CHECKLIST ROWS — generated for every DFO place and every regulation rule with extents, honest states
(a planted missing exception / tributary mismatch / pending cut is ✗ or ?), stale on change, and the review queue
grouped by water (owner 2026-11-09: never leave a water before all its places are listed)."""
from __future__ import annotations

import json
from collections import Counter

import pytest

from pipeline.atlas.reach.classify import wants_tributaries
from pipeline.regs.review import checklist as C
from pipeline.regs.review import dfo as D
from pipeline.regs.review import marks as M
from pipeline.tests import under_test

SHA = "0" * 64


def _curated_ctx() -> C.Context:
    """the curated cuts as built (no atlas in CI): every authored cut, by its label"""
    cuts = json.loads(under_test.curated().waters.cuts.read_text())["waterbodies"]
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
    for _lid, (_s, loc, water) in D.locations(under_test.curated().dfo_salmon.places).items():
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
    for f in sorted(under_test.curated().regulations.synopsis.glob("region-*.json")):
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
    # taken out again at once: a `checks` left in sys.modules stays bound to THIS test's `reuse` (its marks store),
    # and a later test's fresh app then read its rule marks from the wrong store (order-dependent "unchecked")
    sys.modules.pop(name, None)
    mod = __import__(name)
    sys.modules.pop(name, None)
    return mod


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


# ---- owner 2026-11-09 (after trying it): earlier work counts; the water's places grouped

def _earlier_mark(verdict="verified", answers=None, **kw):
    """a mark made before the checklist: no `checklist` on it"""
    base = {k: v for k, v in _mark(verdict, []).items() if k != "checklist"}
    return M.Mark.model_validate({**base, "answers": answers or {"T": "y", "R": "y", "P": "y"}, **kw})


def test_a_verified_mark_from_before_the_checklist_is_done_only_while_the_marks_own_staleness_holds_it(monkeypatch):
    """owner 2026-11-09: "the review app doesnt show how many I already did". An earlier VERIFIED mark the existing
    machinery still calls verified is `earlier` (done); one it calls stale, or a wrong / unsure one, stays unchecked —
    no second staleness rule: the status handed in IS `marks.status`'s."""
    B = _backend(monkeypatch, "checks")
    m = _earlier_mark()
    assert B.check_state(m, "x", "verified") == "earlier"
    assert B.check_state(m, "x", "stale") == "unchecked"
    assert B.check_state(m, "x", None) == "unchecked"                      # never done on the verdict alone
    wrong = _earlier_mark("wrong", {"T": "y", "R": "y", "P": "n"}, problem="split missing", note="n")
    assert B.check_state(wrong, "x", "wrong") == "unchecked"
    got = B._with_check([], "x", wrong, "wrong")
    assert got["check"] == "unchecked" and got["earlier"] == {"date": "2026-11-09", "status": "wrong", "note": "n"}
    assert "checked" not in got
    # a checklist mark is judged by its digest alone, whatever the old status says
    ck = M.Mark.model_validate(_mark("verified", [{"n": 1, "kind": "water", "state": "ok"}]))
    assert B.check_state(ck, "fedcba9876543210", "verified") == "stale"
    assert "earlier" not in B._with_check([], "0123456789abcdef", ck, "verified")


def test_a_rule_mark_counts_as_earlier_only_with_its_place_check_answered_yes(monkeypatch):
    B = _backend(monkeypatch, "checks")
    from pipeline.regs.review import marks as MM
    monkeypatch.setattr(B.reuse, "_prints", lambda e: None)
    monkeypatch.setattr(B.reuse, "applied_keys", lambda: frozenset())
    said = {"status": "verified"}
    monkeypatch.setattr(MM, "status", lambda m, now, applied=(): dict(said))
    assert B._rule_earlier({}, None) is None
    assert B._rule_earlier({}, _earlier_mark()) == "verified"
    # (the model lets a verified mark skip P only as a variant link; the guard is on the answer itself)
    from types import SimpleNamespace
    no_p = SimpleNamespace(checklist=None, verdict="verified", answers={"T": "y"})
    assert B._rule_earlier({}, no_p) == "verified without the place check"
    said["status"] = "stale"
    assert B._rule_earlier({}, _earlier_mark()) == "stale"
    assert B.check_state(_earlier_mark(), "x", B._rule_earlier({}, _earlier_mark())) == "unchecked"


def test_progress_counts_earlier_work_as_done_per_water_and_overall(monkeypatch):
    B = _backend(monkeypatch, "checks")
    s = {"ok": 1, "bad": 0, "worst": "ok"}
    places = [{"water_key": "a", "water": "A", "check": c, "summary": s, **kw} for c, kw in [
        ("earlier", {}), ("checked", {}), ("unchecked", {"state": "needs_cut"}), ("stale", {})]]
    places.append({"water_key": "b", "water": "B", "check": "wrong", "summary": s, "state": "unbound"})
    got = B.progress_of(places)
    assert (got["total"], got["done"], got["left"], got["needs"]) == (5, 3, 2, 2)
    assert got["waters"] == {"a": {"water": "A", "total": 4, "done": 2}, "b": {"water": "B", "total": 1, "done": 1}}
    assert got["by_check"] == {"earlier": 1, "checked": 1, "unchecked": 1, "stale": 1, "wrong": 1}
    assert B._matches(places[0], "earlier") and not B._matches(places[0], "todo")
    assert B._matches(places[2], "needs") and not B._matches(places[1], "needs")


def test_the_waters_places_are_grouped_by_the_data_that_exists(monkeypatch):
    """same place = an older wording of the place, DFO's own rewording link, or the identical extent (same source) — a
    place with no extent matches nothing by extent; the group is named by its first place in list order"""
    B = _backend(monkeypatch, "checks")
    p = lambda i, **kw: {"id": i, "source": "dfo", "extent_key": "", "older_of": None, "duplicate_of": None, **kw}  # noqa: E731
    got = B.same_place([p("a", extent_key="k1"), p("a-old", older_of="a"), p("b"), p("c"),
                        p("d", duplicate_of="b"), p("e", extent_key="k1"), p("r", source="regs", extent_key="k1"),
                        p("f", duplicate_of="gone")])
    assert {i: g["same"] for i, g in got.items()} == {"a": "a", "a-old": "a", "e": "a", "b": "b", "d": "b", "c": "c",
                                                      "r": "r", "f": "f"}
    assert got["a-old"]["why"] == ["our proposal: an older wording, its words alike"]
    assert got["e"]["why"] == ["the identical extent"] and got["d"]["why"] == ["our proposal: matched as a rewording"]
    assert got["c"]["why"] == [] and got["b"]["why"] == ["our proposal: matched as a rewording"]
    # WHOSE CLAIM: a confirmed link is the owner's decision; a wording he took out is never grouped again
    got = B.same_place([p("a", extent_key="k1"), p("b", duplicate_of="a", confirmed=True),
                        p("c", extent_key="k1", apart=True, older_of="a")])
    assert got["b"] == {"same": "a", "why": ["your decision: linked as the same place"]}
    assert got["c"] == {"same": "c", "why": []}


def test_the_row_for_words_no_row_checks_says_what_to_do_and_stays_bad():
    """owner 2026-11-09: "words not checked · read them" told the owner nothing — the row says to read the source's
    words against the map, then tick; it is still ✗ (unsure is never ✓), and the wording is not in the digest"""
    loc = {"source_text": {"waters": "A River", "specific_area": "near the old cannery wharf"}}
    src = C.dfo_source(loc, None)
    r = [x for x in C.rows(src, [{"op": "whole", "item_id": "i1"}], own_items=["i1"], ctx=C.Context(
        labels={}, items={"i1": "A River"}, built=frozenset(), pending=frozenset(), points={})) if x["kind"] == "unchecked"]
    assert len(r) == 1 and r[0]["state"] == "bad"
    assert r[0]["reason"] == "read DFO's words against the map, then tick"
    assert C.rule_source({"name": "A"}, {"verbatim": "x"}).whose == "the regulation"
    import dataclasses
    assert C.digest(src, []) == C.digest(dataclasses.replace(src, whose="anyone"), [])


def _fix_mark(verdict="unsure", answers=None, **kw):
    """the owner's extent save: a mark with an extent fix and no checklist (P is ? while its cut is not built)"""
    base = {k: v for k, v in _mark("verified", []).items() if k != "checklist"}
    fix = {"scope": "entry", "op": "whole", "points": [], "items": [], "tributaries": None}
    return M.Mark.model_validate({**base, "verdict": verdict, "answers": answers or {"T": "y", "R": "y", "P": "?"},
                                  "note": "n", "extent_fix": [fix], **kw})


def test_the_owners_extent_save_is_a_review_done_while_the_place_holds_it_and_back_only_when_it_moved(monkeypatch):
    """owner 2026-11-09: "if you save extent you assume it is correct", "i dont want to have to review the items 2
    times". The rule is derived from the save (mark + what it saved vs the place now), never by rewriting marks."""
    B = _backend(monkeypatch, "checks")
    m = _fix_mark()
    k = B.saved_kind
    assert k(m, "replaces", "waits_cut", "unsure") == "saved"          # waits only for its new cut
    assert k(m, "applied", "landed", "unsure") == "saved"              # built as saved: never asked a second time
    assert k(_fix_mark("verified", {"T": "y", "R": "y", "P": "y"}), "same", "landed", "verified") == "saved"
    # MUTATION: the place no longer holds what he saved, or its words / rules moved -> it comes back as changed
    assert k(m, "replaces", "not_landed", "stale") == "stale"
    assert k(m, "applied", "landed", "stale") == "stale"
    # every cut built, nothing moved since the mark, yet the place does not hold it: never applied — a fault, not done
    assert k(m, "replaces", "not_landed", "unsure") == "unapplied"
    # …but NEVER while the write-through is still at it (applied in the background, seconds after the mark): the
    # owner's Alouette save of 2026-11-09 read "NOT applied" in that window, then landed (commit 044533d84)
    for st in ("saving", "waiting", "saved"):
        assert k(m, "replaces", "not_landed", "unsure", {"state": st}) == "applying"
    assert k(m, "replaces", "not_landed", "unsure", {"state": "refused", "why": "x"}) == "unapplied"
    v = B._saved_view("applying", None, {"state": "not_landed"}, m, {"state": "saving"})
    assert v["state"] == "applying"
    assert B._saved_view("unapplied", None, None, m, {"state": "refused", "why": "binding check"})["why"] == "binding check"
    assert "unapplied" not in B.DONE_STATES and "saved" in B.DONE_STATES and "unapplied" in B.CHECK_STATES
    # not his extent save: no fix, a checklist mark, a defer, or DFO's words / rules answered no
    assert k(None, "applied", "landed", None) is None and k(_earlier_mark(), None, None, "verified") is None
    wrong = _fix_mark("wrong", {"T": "n", "R": "y", "P": "?"}, problem="wrong water")
    assert k(wrong, "applied", "landed", "wrong") is None


def test_a_place_reads_by_the_owners_extent_save_never_as_an_earlier_unsure_mark(monkeypatch):
    B = _backend(monkeypatch, "checks")
    base = {"extents": [], "landed": [], "tributaries": None, "curated": [], "curated_tributaries": None, "only": False,
            "excludes": [], "held": None, "kind": None, "saved": None, "differs": None}
    x = dict(base)
    monkeypatch.setattr(B, "dfo_inputs", lambda loc, water, m, verify=None: x)
    loc = {"source_text": {"waters": "A River", "specific_area": "below the bridge"}}
    ctx = C.Context(labels={}, items={}, built=frozenset(), pending=frozenset(), points={})
    m = _fix_mark()
    x.update(kind="saved", saved={"state": "built", "date": "2026-11-09", "cuts": []})
    got = B.dfo_rows(loc, None, m, ctx, "unsure")
    assert got["check"] == "saved" and got["saved"]["state"] == "built" and "earlier" not in got
    x.update(kind="unapplied", saved={"state": "not_applied", "date": "2026-11-09", "cuts": []})
    got = B.dfo_rows(loc, None, m, ctx, "unsure")
    assert got["check"] == "unapplied" and "earlier" not in got
    x.update(kind="stale", saved=None, differs={"saved": [{"op": "whole"}], "now": []})
    got = B.dfo_rows(loc, None, m, ctx, "stale")
    assert got["check"] == "stale" and got["differs"]["saved"] == [{"op": "whole"}] and "earlier" not in got
    x.update(base)                                   # no extent save: the ordinary rules, the earlier mark said
    got = B.dfo_rows(loc, None, m, ctx, "unsure")
    assert got["check"] == "unchecked" and got["earlier"]["status"] == "unsure"
    s = {"ok": 1, "bad": 0, "worst": "ok"}
    p = B.progress_of([{"water_key": "a", "water": "A", "check": c, "summary": s} for c in ("saved", "unapplied", "unchecked")])
    assert (p["done"], p["left"]) == (1, 2)
    assert B._matches({"check": "unapplied", "summary": s}, "unapplied") and not B._matches({"check": "unapplied", "summary": s}, "todo")


def test_a_wording_is_taken_out_of_its_group_and_put_back_through_marks(tmp_path, monkeypatch):
    """owner 2026-11-09: "how would i refute same wording" / "have a way to add items to it from list". Taking out is a
    mark (`not_variant_of`) whose write-through sets the entry model's CONTESTED state; the group readers then keep it
    apart from either side; a later `variant_of` mark puts it back. One check (T) each; never both in one mark."""
    base = {k: v for k, v in _mark("verified", []).items() if k != "checklist"} | {"answers": {"T": "y"}}
    out = M.Mark.model_validate({**base, "not_variant_of": "1:a:c"})
    assert json.loads(M.line(out))["not_variant_of"] == "1:a:c"
    # a take-out is never "verified earlier": the wording stands alone and still needs its extent
    B = _backend(monkeypatch, "checks")
    assert B.check_state(out, "x", "verified") == "unchecked"
    assert B._with_check([], "x", out, "verified")["earlier"]["status"].startswith("taken out of its group")
    assert "not_variant_of" not in json.loads(M.line(M.Mark.model_validate({**base, "variant_of": "1:a:c"})))
    with pytest.raises(ValueError, match="not both"):
        M.Mark.model_validate({**base, "not_variant_of": "1:a:c", "variant_of": "1:a:d"})
    with pytest.raises(ValueError, match="taken out of itself"):
        M.Mark.model_validate({**base, "not_variant_of": "1:a:b"})
    with pytest.raises(ValueError, match="only a DFO wording"):
        M.Mark.model_validate({**base, "scope": "entry", "answers": {"T": "y", "R": "y", "P": "y"}, "not_variant_of": "x"})
    # the readers: alike words group a historic wording with the current one — until either is contested
    cur = {"status": "active", "source_text": {"specific_area": "downstream of the old mill bridge"}}
    old = {"status": "dormant", "source_text": {"specific_area": "downstream of the old mill bridge."}}
    assert D.group_why(old, cur, "1:a:c") is not None
    assert D.group_why({**old, "duplicate_confirmed": False}, cur, "1:a:c") is None
    assert D.group_why(old, {**cur, "duplicate_confirmed": False}, "1:a:c") is None
    # put back: the owner's confirmed link holds whatever the words
    assert D.group_why({**old, "duplicate_of": "1:a:c", "duplicate_confirmed": True}, cur, "1:a:c")[1] > 1
    # the write-through's edits: contested, link cleared; nothing to write when the place already says so
    from pipeline.regs.review import proposals as P
    f = tmp_path / "region1.json"
    f.write_text("{}")
    loc = {"location_id": "1:a:b", "duplicate_of": "1:a:c"}
    orig = P._find_location
    try:
        P._find_location = lambda src, lid: (f, loc) if lid == "1:a:b" else None
        P._rel, rel = (lambda p: str(p)), P._rel
        got = P._apart_proposal(out, "p1", {}, None)
        assert [(e["field"], e["after"]) for e in got["edits"]] == [("duplicate_of", None), ("duplicate_confirmed", False)]
        loc.update(duplicate_of=None, duplicate_confirmed=False)
        assert P._apart_proposal(out, "p1", {}, None)["edits"] == []
    finally:
        P._find_location, P._rel = orig, rel


def test_the_owners_two_piece_alouette_save_reads_as_built_as_saved():
    """THE EXACT SHAPE (marks.jsonl, 2:alouette-river-and-tributaries:above-216th-street-including-the-n): piece 1
    upstream_of a cut on the place's waters (items []), piece 2 upstream_of another cut on gnis:39404, tributaries
    true. Once the write-through wrote those two extents, fix == curated — whatever the piece order, an absent
    item_id for "the place's waters", and a place-level tributary flag."""
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "curation-review" / "backend"))
    try:
        import dfo as BD
    finally:
        sys.path.pop(0)
    from pipeline.regs.review import proposals as P
    fixes = [M.ExtentFix(scope="entry", op="upstream_of", items=[], tributaries=True,
                         points=[{"kind": "split", "split_id": "alouette_river__216_street_bridge"}]),
             M.ExtentFix(scope="entry", op="upstream_of", items=["gnis:39404"], tributaries=True,
                         points=[{"kind": "split", "split_id": "alouette_river__216th_street"}])]
    mine = [P.extent_of(f, lambda i, p: p.split_id, rest_at_build=True)[0] for f in fixes]
    curated = [{"op": "upstream_of", "splits": ["alouette_river__216th_street"], "item_id": "gnis:39404"},
               {"op": "upstream_of", "splits": ["alouette_river__216_street_bridge"]}]
    assert BD.same_extents(mine, curated)
    assert not BD.same_extents(mine, curated[:1])                    # a piece missing is a difference
    sys.modules.pop("dfo", None)


def test_dfos_heading_gets_its_tributary_qualifier_back_and_the_tributaries_row_reads_it():
    """owner 2026-11-09: "Morice River (including tributaries) in dfo page but in review app it isnt showing the includes
    tributary part" — the scrape keeps the qualifier as the water's flag; the heading and the row put it back"""
    loc = {"water": "Morice River", "source_text": {"waters": "Morice River", "specific_area": "below Lamprey Creek."}}
    assert C.dfo_heading(loc, {"tributaries": True}) == "Morice River (including tributaries)"
    assert C.dfo_heading(loc, {"tributaries": False}) == "Morice River (excluding tributaries)"
    assert C.dfo_heading(loc, {"tributaries": None}) == C.dfo_heading(loc, None) == "Morice River"
    # never against the place: its own words (or its own call) speak of tributaries -> the bare name
    babine = {"water": "Babine Lake", "source_text": {"waters": "Babine Lake", "specific_area": "Babine Lake excluding tributaries and …"}}
    assert C.dfo_heading(babine, {"tributaries": True}) == "Babine Lake"
    assert C.dfo_heading({**loc, "binding": {"tributaries": False}}, {"tributaries": True}) == "Morice River"
    kept = {"source_text": {"waters": "Alouette River and tributaries"}}
    assert C.dfo_heading(kept, {"tributaries": True}) == "Alouette River and tributaries"      # never said twice
    ctx = C.Context(labels={}, items={"i1": "Morice River"}, built=frozenset(), pending=frozenset(), points={})
    src = C.dfo_source(loc, {"tributaries": True})
    trib = lambda walks: next(r for r in C.rows(src, [{"op": "whole", "item_id": "i1"}], own_items=["i1"], ctx=ctx,   # noqa: E731
                                                tributaries=walks) if r["kind"] == "tributaries")
    assert trib(True)["state"] == "ok" and "including tributaries" in trib(True)["quote"].lower()
    assert trib(False)["state"] == "bad"                     # the heading takes them in, ours does not walk them


def test_easiest_first_ranks_a_place_by_the_work_it_is_and_says_why(monkeypatch):
    """owner 2026-11-10: "order them by easiest first" — the order, and the word shown on each row"""
    B = _backend(monkeypatch, "checks")
    e = lambda **kw: B.ease_of(**{"check": "unchecked", "has_extent": False, "bad": 0, **kw})   # noqa: E731
    got = [e(has_extent=True), e(proposal={"confidence": "high", "new_cut": False}), e(twin=True),
           e(proposal={"confidence": "low", "new_cut": False}), e(check="stale", has_extent=True),
           e(proposal={"confidence": "high", "new_cut": True}), e(), e(has_extent=True, bad=2),
           e(check="unapplied", has_extent=True)]
    assert [g["word"] for g in got] == ["confirm only", "proposal ready", "one click", "proposal to check", "changed",
                                        "new cut needed", "no proposal", "has ✗", "fault"]
    assert [g["rank"] for g in got] == list(range(9))
    assert all(B.ease_of(check=c, has_extent=True, bad=3) is None for c in B.DONE_STATES)        # done: no rank
    assert e(twin=True, proposal={"confidence": "high", "new_cut": False})["word"] == "one click"  # the twin wins


def test_a_dead_piece_says_why_in_plain_words_and_names_the_cut_to_pick(monkeypatch):
    """owner 2026-11-09 (the Forks): his piece on Bulkley River ran between the MORICE's confluence cut and a Bulkley
    cut — it bound nothing and the screen only said "binds NOTHING". Now: which cut is on which water, and the nearest
    cut on the piece's own water."""
    BD = _backend(monkeypatch, "dfo")
    from types import SimpleNamespace as NS
    from shapely.geometry import Point
    monkeypatch.setattr(BD.reuse, "_registry", lambda: {"m": NS(name="Morice River"), "b": NS(name="Bulkley River")})
    monkeypatch.setattr(BD.reuse, "_boundary_index", lambda: {
        "m_conf": ("m", NS(label="Bulkley River confluence (the Forks)")), "b_conf": ("b", NS(label="Morice River → Bulkley River")),
        "b_up": ("b", NS(label="100 m up the upper Bulkley")), "b_far": ("b", NS(label="Quick bridge"))})
    monkeypatch.setattr(BD.reuse, "_split_point", lambda s: {"m_conf": Point(0, 0), "b_conf": Point(1, 0), "b_up": Point(90, 0),
                                                            "b_far": Point(9000, 0)}[s])
    piece = lambda a, b, items: M.ExtentFix(scope="entry", op="between", items=items, tributaries=None,   # noqa: E731
                                            points=[{"kind": "split", "split_id": a}, {"kind": "split", "split_id": b}])
    fixes = [piece("m_conf", "m_conf", ["m"]), piece("m_conf", "b_up", ["b"])]
    got = BD.dead_why(fixes, [1], {"item_ids": ["m"]})
    assert got == ["piece 2 is on Bulkley River, but “Bulkley River confluence (the Forks)” is a cut on Morice River: a piece "
                   "runs between cuts on its own water. Pick the cut on Bulkley River instead — the nearest one there is "
                   "“Morice River → Bulkley River”."]
    assert "its cuts are there but no stretch lies between them" in BD.dead_why(fixes, [0], {"item_ids": ["m"]})[0]


def test_a_cut_carried_across_braids_is_drawn_on_its_own_water(monkeypatch):
    """owner 2026-11-10: "lamprey creek cut point shows up weird in morice review" — the resolved cuts file has one row
    for the host and one per braid the cut is carried across (`concern`); the map must take the host's"""
    R = _backend(monkeypatch, "reuse")
    rows = [{"split_id": "a", "blk": "host", "route_measure": 54533.3},
            {"split_id": "a", "blk": "braid", "route_measure": 446.2, "concern": "braid cut across a at 226 m from the host"},
            {"split_id": "a", "blk": "trib", "route_measure": 2677.8, "concern": "tributary of a braid cut across a"},
            {"split_id": "b", "blk": "in", "route_measure": 1.0}, {"split_id": "b", "blk": "out", "route_measure": 9.0},
            {"split_id": "c", "blk": "only", "route_measure": 5.0, "concern": "picked up"}]
    got = R.host_rows(rows)
    assert got["a"]["blk"] == "host" and got["b"]["blk"] == "out" and got["c"]["blk"] == "only"


def test_the_places_cache_goes_stale_on_each_thing_it_is_made_from_and_on_nothing_else(monkeypatch):
    """T591: the checklist's places are memoised (T532 r8, speed). Each of its five inputs must miss the memo — a
    missed one shows the owner a place as not done after he saved it — and a read with nothing changed must hit."""
    import sys
    B = _backend(monkeypatch, "checks")
    reuse, dfo = sys.modules["reuse"], sys.modules.get("dfo") or __import__("dfo")
    now = {"marks": 1, "rows": ("rows",), "ctx": ("ctx",), "entries": 1, "apply": 1}
    built = []
    monkeypatch.setattr(reuse, "_stamp", lambda paths: ("marks", now["marks"]))
    monkeypatch.setattr(reuse, "_dir_stamp", lambda d: ("entries", now["entries"]))
    monkeypatch.setattr(dfo, "_rows", lambda: now["rows"])
    monkeypatch.setattr(B, "context", lambda: now["ctx"])
    monkeypatch.setattr(B, "apply_stamp", lambda: (now["apply"],))
    monkeypatch.setattr(B, "_dfo_places", lambda: built.append(1) or [])
    monkeypatch.setattr(B, "_regs_places", lambda: [])
    monkeypatch.setattr(B, "_PLACES", reuse._Memo())
    B._places()
    B._places()
    assert len(built) == 1, "nothing changed: one computation"
    for part, new in (("marks", 2), ("rows", ("other rows",)), ("ctx", ("other ctx",)), ("entries", 2), ("apply", 2)):
        now[part] = new
        n = len(built)
        B._places()
        assert len(built) == n + 1, f"a change of {part} must recompute the places"
        B._places()
        assert len(built) == n + 1, f"…and only once ({part})"


def test_a_cut_search_lists_this_waters_cuts_before_any_other_waters(monkeypatch):
    """T590: a wording that names a tributary listed the tributary's cuts above the place's own. The asker's waters
    come first, each group in file order, and the limit is cut AFTER the ordering."""
    R = _backend(monkeypatch, "reuse")
    recs = {"t1": ("Deadman Creek", {"label": "bridge"}), "t2": ("Deadman Creek", {"label": "bridge two"}),
            "m1": ("Thompson River", {"label": "CNR bridge"}), "m2": ("Thompson River", {"label": "rail bridge"})}
    monkeypatch.setattr(R, "_split_records", lambda: recs)
    assert [h["id"] for h in R.search_splits("bridge")] == ["t1", "t2", "m1", "m2"]                 # no water named: file order
    got = R.search_splits("bridge", first=["thompson river "])
    assert [(h["id"], h["own"]) for h in got] == [("m1", True), ("m2", True), ("t1", False), ("t2", False)]
    assert [h["id"] for h in R.search_splits("bridge", limit=2, first=["Thompson River"])] == ["m1", "m2"], \
        "the limit keeps the asker's own cuts, not the first two in the file"
    assert R.search_splits("b") == []
