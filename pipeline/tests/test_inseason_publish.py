"""T520: the in-season links/2 — publish only FORMATS the resolved matches, and refuses (non-zero, nothing written)
an unresolved match, a stale stamp (a hand edit), an end with no words, a release off LATEST or not on disk, and a range
not landing on the release it names. The ranges' rule is the device's (`@app/core` `inSeasonCover`: a section whose
start lies in [m0, m1) on its stream) — one fixture holds both readers to it."""
from __future__ import annotations

import json

import pytest

from pipeline.regs.inseason import matches as M
from pipeline.regs.inseason import publish as PUB
from pipeline.regs.inseason import resolve as R

EXT = [{"op": "whole", "item_id": "gnis:1", "splits": []}]


def _doc(matches: dict, release: str = "r-1", words: dict | None = None) -> M.Matches:
    doc = M.Matches.model_validate({"format": "inseason-matches/2", "release": release, "matches": matches,
                                    "words": {"x": "X"} if words is None else words})
    # every resolved match stamped as resolve stamps it (a test of a stale stamp unstamps one)
    return doc.model_copy(update={"matches": {k: (M.stamped(m, release) if m.waters else m)
                                              for k, m in doc.matches.items()}})


def _m(**kw) -> dict:
    return {"region": "1", "water": "A", "extents": EXT, **kw}


def _rel(**kw) -> R.Release:
    # water gnis:1 (ord 1): stream handles 1..3 on blk 7 at 0, 100, 200; lake pieces 10, 11
    base = dict(name="r-1", handles="h", ord_of={"gnis:1": 1, "wbk:9": 2}, kind_of={"gnis:1": "stream", "wbk:9": "lake"},
                members={1: frozenset({1, 2, 3, 10, 11}), 2: frozenset({12})},
                starts={1: (7, 0), 2: (7, 100), 3: (7, 200)})
    return R.Release(**{**base, **kw})


def test_publish_formats_a_resolved_file():
    doc = _doc({"is-aaaaaaaaaaaa": _m(waters={"gnis:1": {"runs": [[7, 100, 300]], "lakes": [1], "ends": [["x", "source"]]},
                                              "wbk:9": {"all": True}},
                                      effective={"to": "2026-12-31"})}, words={"x": "X", "unused": "U"})
    links = PUB.compile_links(doc, "d")
    assert links == {"format": "inseason-links/2", "release": "r-1", "digest": "d", "words": {"x": "X"},
                     "notices": {"is-aaaaaaaaaaaa": {
        "to": "2026-12-31", "waters": {"gnis:1": {"runs": [[7, 100, 300]], "lakes": [1], "ends": [["x", "source"]]},
                                       "wbk:9": {"all": True}}}}}
    assert PUB.check_on_release(links, _rel()) == []


@pytest.mark.parametrize("doc,words", [
    (_doc({"is-aaaaaaaaaaaa": _m(unresolved="selects no section")}), "unresolved — selects no section"),
    (_doc({"is-aaaaaaaaaaaa": _m()}), "unresolved — no waters"),
    (_doc({"is-aaaaaaaaaaaa": _m(waters={"gnis:1": {"all": True}})}, release=""), "names no release"),
    (_doc({"is-aaaaaaaaaaaa": _m(waters={"gnis:1": {"runs": [[7, 0, 9]], "ends": [["nowhere", "source"]]}})}),
     "end 'nowhere' has no words"),
])
def test_publish_refuses_an_unresolved_match(doc, words):
    with pytest.raises(PUB.Refused) as ex:
        PUB.compile_links(doc, "d")
    assert words in str(ex.value)


def test_a_hand_edit_not_re_resolved_is_refused():
    """The stamp holds the extents, the release and the ranges: widening a range by hand (or editing the extents)
    without re-resolving is refused."""
    doc = _doc({"is-aaaaaaaaaaaa": _m(waters={"gnis:1": {"runs": [[7, 100, 300]], "ends": [["x", "source"]]}})})
    PUB.compile_links(doc, "d")
    m = doc.matches["is-aaaaaaaaaaaa"]
    wide = m.model_copy(update={"waters": {"gnis:1": M.Water(runs=[(7, 0, 300)], ends=[("x", "source")])}})
    for bad in (wide, m.model_copy(update={"extents": [M.InSeasonExtent(op="whole", item_id="gnis:2")]})):
        with pytest.raises(PUB.Refused, match="changed since they were resolved"):
            PUB.compile_links(doc.model_copy(update={"matches": {"is-aaaaaaaaaaaa": bad}}), "d")


def test_a_release_off_latest_or_not_on_disk_is_refused(tmp_path, monkeypatch):
    from pipeline.common import curated as C
    doc = _doc({"is-aaaaaaaaaaaa": _m(waters={"wbk:9": {"all": True}})})
    src = tmp_path / "m.json"
    src.write_text(M.dumps(doc))
    monkeypatch.setattr(M, "problems", lambda *a, **k: [])
    rel = tmp_path / "release"
    rel.mkdir()
    gen = type("G", (), {"release": rel, "inseason": tmp_path / "gen"})()
    monkeypatch.setattr(C, "GENERATED", gen)
    (rel / "LATEST").write_text("r-2\n")
    with pytest.raises(PUB.Refused, match="LATEST is r-2"):
        PUB.main(["--matches", str(src), "--out", str(tmp_path / "out")])
    (rel / "LATEST").write_text("r-1\n")
    with pytest.raises(PUB.Refused, match="not on disk"):
        PUB.main(["--matches", str(src), "--out", str(tmp_path / "out")])
    assert not (tmp_path / "out" / "matches.json").exists()


def test_one_fixture_two_readers():
    """`app/packages/core/src/inseason-cover.parity.json`: Python's range reading (+ lake places) gives each case's
    sections, as core's `inSeasonCover` does on the same file."""
    from pipeline.common.curated import REPO_ROOT
    fx = json.loads((REPO_ROOT / "app/packages/core/src/inseason-cover.parity.json").read_text(encoding="utf-8"))
    starts = {w["sid"]: (w["line"]["blk"], w["line"]["from"]) for w in fx["water"] if w["line"]}
    members = frozenset(w["sid"] for w in fx["water"])
    rel = R.Release(name="r", handles="h", ord_of={"i": 1}, kind_of={"i": "stream"}, members={1: members}, starts=starts)
    for c in fx["cases"]:
        got = R.covered_by_runs(c["runs"], members, starts) | {rel.lake_pieces("i")[i] for i in c["lakes"]}
        assert sorted(got) == c["expect"], c["name"]


def test_a_water_covering_nothing_cannot_be_written():
    with pytest.raises(ValueError):
        M.Water()                                      # neither all nor runs: empty coverage is not a water
    with pytest.raises(ValueError):
        M.Water(all=True, ends=[("a", "b")])           # a whole water has no ends


def test_publish_refuses_a_range_off_the_release():
    links = {"release": "r-1", "notices": {"is-a": {"waters": {
        "gnis:1": {"runs": [[7, 400, 500]], "lakes": [5], "ends": "x"},     # past its last section; 2 lake pieces
        "gnis:2": {"all": True}}}}}                                           # not a water of r-1
    bad = PUB.check_on_release(links, _rel())
    assert any("lands on no section" in b for b in bad)
    assert any("lake place 5" in b for b in bad)
    assert any("gnis:2 is not a water of r-1" in b for b in bad)
    assert PUB.check_on_release({**links, "release": "r-2"}, _rel()) == ["the release here is r-1, not r-2"]


def test_ranges_read_back_exactly_by_the_devices_rule():
    span = {1: (7, 0, 100), 2: (7, 100, 200), 4: (7, 300, 400), 5: (8, 0, 50)}
    runs = R.make_runs(span, span)
    assert runs == [[7, 0, 200], [7, 300, 400], [8, 0, 50]]        # 3 is not covered: two ranges on blk 7
    starts = {h: (b, lo) for h, (b, lo, _) in span.items()} | {3: (7, 200)}
    assert R.covered_by_runs(runs, {1, 2, 3, 4, 5}, starts) == {1, 2, 4, 5}
    # a zero-length last piece still has a range its start lies in
    assert R.make_runs({1: (7, 10, 10)}, {1: (7, 10, 10)}) == [[7, 10, 11]]


def test_place_words():
    """INTERIM token words (T517's release table replaces them): a place's NAME, never a sentence."""
    assert R.interim_words("x", "Spuzzum Creek → Fraser River") == "Spuzzum Creek confluence"
    assert R.interim_words("x", "fishing boundary signs near the Mobbs Creek confluence") == "the Mobbs Creek confluence"
    assert R.interim_words("x", "Josephine Falls") == "Josephine Falls"


@pytest.mark.parametrize("row,eff,want", [
    ({"from": "2026-07-04", "to": "2026-08-31"}, None, ("2026-07-04", "2026-08-31")),
    ({"from": "2026-07-04", "to": None}, M.Effective(to="2026-09-01"), ("2026-07-04", "2026-09-01")),
    # one printed window, no `to`: its end in the from year …
    ({"from": "2026-07-01", "parsed": {"kinds": ["closure"], "window": {"from_month": 7, "from_day": 30,
                                                                         "to_month": 9, "to_day": 15}}},
     None, ("2026-07-01", "2026-09-15")),
    # … rolling to the next year when it ends before it starts
    ({"from": "2026-10-15", "parsed": {"window": {"from_month": 10, "from_day": 15, "to_month": 1, "to_day": 31}}},
     None, ("2026-10-15", "2027-01-31")),
    ({"from": "2026-07-01", "parsed": {"kinds": ["rescind"], "window": {"to_month": 9, "to_day": 15}}},
     None, ("2026-07-01", None)),
])
def test_span_is_cores_rule(row, eff, want):
    """The same cases as `app/packages/core/src/inseason.test.ts` `inSeasonSpan`."""
    assert M.span(row, eff) == want


def test_the_curated_file_publishes():
    """The committed notice_waters.json is resolved, stamped, and formats with no atlas."""
    doc = M.load()
    links = PUB.compile_links(doc, "d")
    assert links["release"] and links["notices"]
    assert all(n["waters"] for n in links["notices"].values())
    json.dumps(links)
