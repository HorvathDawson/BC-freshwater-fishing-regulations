"""T520 / T673: the in-season links (/4: ONE release-independent file) — publish only FORMATS the resolved matches, and
refuses (non-zero, nothing written) an unresolved match, a stale stamp (a hand edit), an end with no words, a release
not on disk, a range touching no section of its water on LATEST, and — on the release it was resolved on — a range off
its sections' boundaries (a shifted measure). The ranges' rule is the device's (`@app/core` `inSeasonCover`: a section
whose line [from, to) overlaps [m0, m1) on its stream) — one fixture holds both readers to it."""
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
    # water gnis:1 (ord 1): stream handles 1..3 on blk 7 at [0, 100), [100, 200), [200, 300); lake pieces 10, 11
    base = dict(name="r-1", handles="h", ord_of={"gnis:1": 1, "wbk:9": 2}, kind_of={"gnis:1": "stream", "wbk:9": "lake"},
                members={1: frozenset({1, 2, 3, 10, 11}), 2: frozenset({12})},
                lines={1: (7, 0, 100), 2: (7, 100, 200), 3: (7, 200, 300)},
                regions={h: frozenset({"1"}) for h in (1, 2, 3, 10, 11, 12)})
    return R.Release(**{**base, **kw})


def test_publish_formats_a_resolved_file():
    doc = _doc({"is-aaaaaaaaaaaa": _m(waters={"gnis:1": {"runs": [[7, 100, 300]], "lakes": True, "ends": [["x", "source"]]},
                                              "wbk:9": {"all": True}},
                                      effective={"to": "2026-12-31"})}, words={"x": "X", "unused": "U"})
    links = PUB.compile_links(doc, "d")
    # (T673) no release in the file: keyed by item id, stretches by stream key + metres
    assert links == {"format": "inseason-links/4", "digest": "d",
                     "notices": {"is-aaaaaaaaaaaa": {
        "to": "2026-12-31", "waters": {"gnis:1": {"runs": [[7, 100, 300]], "lakes": True, "ends": [["x", "source"]]},
                                       "wbk:9": {"all": True}}}}}
    assert PUB.check_on_release(links, _rel()) == [] == PUB.exact_problems(links, _rel())
    assert PUB.check_on_release(links, _rel(name="r-9")) == []          # any release reads it


@pytest.mark.parametrize("doc,words", [
    (_doc({"is-aaaaaaaaaaaa": _m(unresolved="selects no section")}), "unresolved — selects no section"),
    (_doc({"is-aaaaaaaaaaaa": _m()}), "unresolved — no waters"),
    (_doc({"is-aaaaaaaaaaaa": _m(waters={"gnis:1": {"all": True}})}, release=""), "names no release"),
])
def test_publish_refuses_an_unresolved_match(doc, words):
    with pytest.raises(PUB.Refused) as ex:
        PUB.compile_links(doc, "d")
    assert words in str(ex.value)


def test_an_end_the_release_has_no_words_for_is_refused():
    """T517 review finding 1: the release's `display.ends` says every end (the build puts every curated notice's ends
    in it); publish refuses a link naming a token that release lacks — a notice changed since it was built."""
    doc = _doc({"is-aaaaaaaaaaaa": _m(waters={"gnis:1": {"runs": [[7, 100, 300]], "ends": [["x", "source"]]}})})
    links = PUB.compile_links(doc, "d")
    assert PUB.ends_problems(links, {"x": ["X"]}, "r-1") == []
    assert PUB.ends_problems(links, {}, "r-1") == ["is-aaaaaaaaaaaa: gnis:1 end 'x' has no words in r-1's display.ends"]


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


def test_a_file_resolved_on_an_older_release_publishes_and_a_release_not_on_disk_is_refused(tmp_path, monkeypatch,
                                                                                            capsys):
    """(T673) The file is not bound to a release: one resolved on r-1 publishes against LATEST r-2 when every range
    touches its water there (said on stderr; the boundary check is for the release it was resolved on only). A LATEST
    with no manifest, or a side release not on disk, is still refused."""
    from pipeline.common import curated as C
    from pipeline.deliver.answers.display import name_check
    doc = _doc({"is-aaaaaaaaaaaa": _m(waters={"wbk:9": {"all": True},
                                              "gnis:1": {"runs": [[7, 150, 250]], "ends": [["x", "source"]]}})})
    src = tmp_path / "m.json"
    src.write_text(M.dumps(doc))
    monkeypatch.setattr(M, "problems", lambda *a, **k: [])
    monkeypatch.setattr(R, "read_release", lambda d: _rel(name=d.name))
    monkeypatch.setattr(name_check, "release_waters", lambda d: ({}, {"x": ["X"]}))
    rel = tmp_path / "release"
    rel.mkdir()
    gen = type("G", (), {"release": rel, "inseason": tmp_path / "gen"})()
    monkeypatch.setattr(C, "GENERATED", gen)
    (rel / "r-2").mkdir()
    (rel / "r-2" / "manifest.json").write_text("{}")
    (rel / "LATEST").write_text("r-2\n")
    assert PUB.main(["--matches", str(src), "--out", str(tmp_path / "ok")]) == 0
    assert "resolved on r-1, this release is r-2" in capsys.readouterr().err
    out = json.loads((tmp_path / "ok" / "matches.json").read_text())
    assert out["format"] == "inseason-links/4" and "release" not in out
    # the same file read as if r-2 were the release it was resolved on: 150 / 250 are no section boundaries there
    monkeypatch.setattr(R, "read_release", lambda d: _rel(name="r-1"))
    with pytest.raises(PUB.Refused, match="does not start and end on its sections' boundaries"):
        PUB.main(["--matches", str(src), "--out", str(tmp_path / "out")])
    # (T560 item 6) LATEST naming a folder with no manifest is refused by the one live-release reader
    (rel / "LATEST").write_text("r-1\n")
    with pytest.raises(SystemExit, match="names r-1, which has no manifest"):
        PUB.main(["--matches", str(src), "--out", str(tmp_path / "out")])
    # a side release not on disk is still refused by name
    with pytest.raises(PUB.Refused, match="not on disk"):
        PUB.main(["--matches", str(src), "--out", str(tmp_path / "out"), "--side-release", str(tmp_path / "nowhere")])
    assert not (tmp_path / "out" / "matches.json").exists()


def test_one_fixture_two_readers():
    """`app/packages/core/src/inseason-cover.parity.json`: Python's range reading (+ lake places) gives each case's
    sections, as core's `inSeasonCover` does on the same file."""
    from pipeline.common.curated import REPO_ROOT
    fx = json.loads((REPO_ROOT / "app/packages/core/src/inseason-cover.parity.json").read_text(encoding="utf-8"))
    lines = {w["sid"]: (w["line"]["blk"], w["line"]["from"], w["line"]["to"]) for w in fx["water"] if w["line"]}
    members = frozenset(w["sid"] for w in fx["water"])
    rel = R.Release(name="r", handles="h", ord_of={"i": 1}, kind_of={"i": "stream"}, members={1: members}, lines=lines,
                    regions={})
    for c in fx["cases"]:
        got = R.touched(c["runs"], members, lines) | (set(rel.lake_pieces("i")) if c["lakes"] else set())
        assert sorted(got) == c["expect"], c["name"]


def test_a_water_covering_nothing_cannot_be_written():
    with pytest.raises(ValueError):
        M.Water()                                      # neither all nor runs: empty coverage is not a water
    with pytest.raises(ValueError):
        M.Water(all=True, ends=[("a", "b")])           # a whole water has no ends


def test_publish_refuses_a_range_off_the_release():
    links = {"notices": {"is-a": {"waters": {
        "gnis:1": {"runs": [[7, 400, 500]], "ends": "x"},     # past its last section
        "gnis:2": {"all": True}}}}}                            # not a water of r-1
    bad = PUB.check_on_release(links, _rel())
    assert any("gnis:1 range [7, 400, 500] touches no section" in b for b in bad)
    assert any("gnis:2 is not a water of r-1" in b for b in bad)
    lakeless = _rel(members={1: frozenset({1, 2, 3}), 2: frozenset({12})})          # gnis:1 with no lake piece
    lakes = {"notices": {"is-a": {"waters": {"gnis:1": {"runs": [[7, 0, 100]], "lakes": True}}}}}
    assert PUB.check_on_release(lakes, lakeless) == [
        "is-a: gnis:1 says its lake pieces are covered, but it has none in r-1"]


def test_a_shifted_measure_is_refused_on_the_release_it_was_resolved_on():
    """(T673) MUTATION: the ranges are their sections' own spans on the release they were resolved on; a measure moved
    by a metre either way (a hand edit, or a range read on another release's boundaries) is refused there."""
    good = {"notices": {"is-a": {"waters": {"gnis:1": {"runs": [[7, 100, 300]]}}}}}
    assert PUB.exact_problems(good, _rel()) == []
    for run in ([7, 101, 300], [7, 99, 300], [7, 100, 299], [7, 100, 301]):
        bad = PUB.exact_problems({"notices": {"is-a": {"waters": {"gnis:1": {"runs": [run]}}}}}, _rel())
        assert len(bad) == 1 and "a shifted measure" in bad[0], run
    # an open-ended section (a stream's one section) ends wherever the atlas measured it
    open_ = _rel(lines={1: (7, 0, 100), 2: (7, 100, 200), 3: (8, 0, 0xFFFFFFFF)}, members={1: frozenset({1, 2, 3})})
    assert PUB.exact_problems({"notices": {"is-a": {"waters": {"gnis:1": {"runs": [[8, 0, 4321]]}}}}}, open_) == []


def test_ranges_read_back_exactly_by_the_devices_rule():
    span = {1: (7, 0, 100), 2: (7, 100, 200), 4: (7, 300, 400), 5: (8, 0, 50)}
    runs = R.make_runs(span, span)
    assert runs == [[7, 0, 200], [7, 300, 400], [8, 0, 50]]        # 3 is not covered: two ranges on blk 7
    lines = dict(span) | {3: (7, 200, 300)}
    assert R.touched(runs, {1, 2, 3, 4, 5}, lines) == {1, 2, 4, 5}       # 3 sits between two ranges, touching neither
    # a zero-length last piece still has a range its start lies in
    assert R.make_runs({1: (7, 10, 10)}, {1: (7, 10, 10)}) == [[7, 10, 11]]


def test_place_words():
    """INTERIM token words (T517's release table replaces them): a place's NAME, never a sentence."""
    assert R.interim_words("x", "Spuzzum Creek → Fraser River") == "Spuzzum Creek confluence"
    assert R.interim_words("x", "fishing boundary signs near the Mobbs Creek confluence") == "the Mobbs Creek confluence"
    assert R.interim_words("x", "Josephine Falls") == "Josephine Falls"


def _span_cases():
    from pipeline.common.curated import REPO_ROOT
    fx = json.loads((REPO_ROOT / "app/packages/core/src/inseason-span.parity.json").read_text(encoding="utf-8"))
    return fx["cases"]


@pytest.mark.parametrize("case", _span_cases(), ids=lambda c: c["name"][:60])
def test_span_is_cores_rule(case):
    """(T548) ONE case file, two readers: `app/packages/core/src/inseason-span.parity.json` gives `expect` here and in
    core's `inSeasonSpan`; `inForce` is the same `from <= day <= to` test `joinInSeason` makes."""
    eff = M.Effective(**case["effective"]) if case.get("effective") else None
    frm, to = M.span(case["row"], eff)
    assert [frm, to] == case["expect"]
    for day, want in case["inForce"].items():
        assert (not (frm and frm > day) and not (to and to < day)) == want, day


def test_the_curated_file_publishes():
    """The committed notice_waters.json is resolved, stamped, and formats with no atlas."""
    doc = M.load()
    links = PUB.compile_links(doc, "d")
    assert links["notices"] and "release" not in links
    assert all(n["waters"] for n in links["notices"].values())
    json.dumps(links)


@pytest.mark.needs_bundle
def test_the_curated_ranges_resolve_on_the_release_under_test(request):
    """(T673) THE GUARD, on the data: every water of the committed file is one the release under test (LATEST, or the
    release command's candidate) holds and every range touches it there; on the release the file was resolved on, every
    range is exact. MUTATION: a range moved off its water is refused on any release, and a measure shifted by a metre
    on the release it was resolved on."""
    from pipeline.tests import under_test
    rel = R.read_release(under_test.release(request))
    doc = M.load(under_test.curated().inseason.notice_waters)
    links = PUB.compile_links(doc, "d")
    assert PUB.check_on_release(links, rel) == []
    exact = rel.name == doc.release
    if exact:
        assert PUB.exact_problems(links, rel) == []
    rid, item, w = next((rid, item, w) for rid, n in sorted(links["notices"].items())
                        for item, w in sorted(n["waters"].items()) if w.get("runs"))
    blk, m0, m1 = w["runs"][0]
    gone = {"notices": {rid: {"waters": {item: {**w, "runs": [[blk, m1 + 10_000_000, m1 + 10_000_100]]}}}}}
    assert any("touches no section" in b for b in PUB.check_on_release(gone, rel))
    if exact:
        shifted = {"notices": {rid: {"waters": {item: {**w, "runs": [[blk, m0 + 1, m1]]}}}}}
        assert any("a shifted measure" in b for b in PUB.exact_problems(shifted, rel))


# --------------------------------------------------------------------------- T623 F7, F20: own region, fresh water only

from types import SimpleNamespace  # noqa: E402

from pipeline.regs.inseason import points as P  # noqa: E402

#: a Fraser of five stem pieces on blk 7, 100 m each, handles 1..5, mouth upward: 1 tidal (below Mission: the release
#: places it in no region), 2 Region 2, 3 Region 3, 4 on the Region 3 / 5 line (in both), 5 Region 5
FRASER = {1: None, 2: {"2"}, 3: {"3"}, 4: {"3", "5"}, 5: {"5"}}


def _fraser(monkeypatch, named: set[int]):
    nodes = [SimpleNamespace(node_id=f"7:{h}", blk=7, kind="stream", down_m=100.0 * (h - 1), up_m=100.0 * h)
             for h in FRASER]
    pts = [P.Point("mouth", "Mouth", "mouth", 0.0, "7:1"), P.Point("mission", "the CPR Bridge at Mission", "split",
                                                                   100.0, "7:2"),
           P.Point("spuzzum", "Spuzzum Creek → Fraser River", "split", 200.0, "7:3"),
           P.Point("hells_gate", "Hell's Gate", "split", 250.0, None)]
    monkeypatch.setattr(P, "resolve", lambda reg, g, ext: {"sections": sorted(f"7:{h}" for h in named)})
    monkeypatch.setattr(P, "_stem_nodes", lambda reg, g, item: (7, nodes))
    monkeypatch.setattr(P, "points", lambda reg, g, item: pts)
    from pipeline.deliver.bundle import section_measure
    monkeypatch.setattr(section_measure, "span", lambda sid, n: (n.blk, int(n.down_m), int(n.up_m)))
    atlas = R.Atlas(reg={"gnis:39325": SimpleNamespace(name="Fraser River")},
                    g=SimpleNamespace(nodes={n.node_id: n for n in nodes}),
                    handle={n.node_id: h for h, n in zip(FRASER, nodes)}, ids=tuple(n.node_id for n in nodes),
                    digest="h")
    rel = R.Release(name="r-1", handles="h", ord_of={"gnis:39325": 1}, kind_of={"gnis:39325": "stream"},
                    members={1: frozenset(FRASER)}, lines={h: (7, 100 * (h - 1), 100 * h) for h in FRASER},
                    regions={h: frozenset(r) for h, r in FRASER.items() if r})
    return atlas, rel


def _match(region: str, op: str, splits: list[str]) -> M.Match:
    return M.Match(region=region, water="Fraser River",
                   extents=[M.InSeasonExtent(op=op, item_id="gnis:39325", splits=splits)])


def test_f7_a_region_3_notice_stops_at_the_region_3_boundary(monkeypatch):
    """F7. Notice `is-14c1a6f972a3` (Region 3): "No fishing for steelhead in the Fraser River (mainstem) upstream of
    Hells Gate January 1 to December 31". The book bounds the same rule (PRINTED p.30): "from Hells Gate upstream to the
    Region 3 boundary Jan 1-Dec 31". Owner 2026-11-11: "the banner stops at the Region 3 boundary." Upstream of Hells
    Gate the water runs on into Region 5: not covered; the piece the line crosses (in Regions 3 and 5) is, and the
    stretch ends at the Region 3 line, said as the release says it."""
    atlas, rel = _fraser(monkeypatch, named={3, 4, 5})                 # "upstream of Hells Gate": to the source
    waters, words = R.resolve_match(atlas, rel, _match("3", "upstream_of", ["hells_gate"]), {})
    w = waters["gnis:39325"]
    assert R.touched(w.runs, rel.item_sections("gnis:39325"), rel.lines) == {3, 4}
    assert w.ends == [("spuzzum", "region_line:3")]
    assert words["region_line:3"] == "Region 3 boundary"


def test_f20_a_freshwater_notice_starts_at_mission_not_on_the_tidal_stretch(monkeypatch):
    """F20. The Region 2 steelhead notice `is-8cd37f6e5bf9` is matched "downstream of the Spuzzum Creek confluence",
    which reaches the mouth. Owner 2026-11-11: "a freshwater notice starts at Mission, not on the tidal stretch" — the
    Fraser's tidal limit is the CPR Bridge at Mission; below it the release places the water in no region (tidal
    water), so no notice covers it, and the stretch's lower end is the place standing there."""
    atlas, rel = _fraser(monkeypatch, named={1, 2})
    waters, _ = R.resolve_match(atlas, rel, _match("2", "downstream_of", ["spuzzum"]), {})
    w = waters["gnis:39325"]
    assert w.runs == [(7, 100, 200)]
    assert R.touched(w.runs, rel.item_sections("gnis:39325"), rel.lines) == {2}
    assert w.ends == [("mission", "spuzzum")]


def test_a_notice_with_no_fresh_water_of_its_region_or_no_region_is_refused(monkeypatch):
    atlas, rel = _fraser(monkeypatch, named={1, 5})                    # tidal + Region 5 only
    with pytest.raises(R.Unresolved, match="no fresh water of Region 2"):
        R.resolve_match(atlas, rel, _match("2", "upstream_of", ["hells_gate"]), {})
    with pytest.raises(R.Unresolved, match="names no region"):
        R.resolve_match(atlas, rel, _match("", "upstream_of", ["hells_gate"]), {})


def test_the_region_a_section_lies_in_is_the_releases():
    """`fresh_in_region`: in the region (a line piece is in both), never in no region; "7B" / "Region 7B" are `7b`."""
    regions = {h: frozenset(r) for h, r in FRASER.items() if r} | {9: frozenset({"7b"})}
    assert R.fresh_in_region(set(FRASER), "3", regions) == {3, 4}
    assert R.fresh_in_region(set(FRASER), "5", regions) == {4, 5}
    assert R.fresh_in_region({1, 9}, "7B", regions) == R.fresh_in_region({1, 9}, "Region 7B", regions) == {9}
    assert R.fresh_in_region(set(FRASER), "provincial", regions) == {2, 3, 4, 5}     # every region, never tidal
    assert R.region_line("area:7A") == "region_line:7A" and R.region_line("area:Landstrom") == "area:Landstrom"


def test_fresh_regions_leaves_out_tidal_and_outside_bc(monkeypatch, tmp_path):
    """The store places a section by `section_place` (none: tidal water, outside B.C.); a key the book calls tidal
    (`akey.tidal`, Nitinat Lake) is left out too."""
    import sqlite3
    from pipeline.deliver.store import release as SR
    con = sqlite3.connect(":memory:")
    con.executescript("CREATE TABLE place (place INTEGER, regions TEXT); CREATE TABLE akey (akey INTEGER, tidal INTEGER);"
                      "INSERT INTO place VALUES (0, '[\"1\"]'), (1, '[\"2\",\"7A\"]');"
                      "INSERT INTO akey VALUES (1, 0), (2, 1);")
    st = SimpleNamespace(con=con, meta={"section_handles": "h"}, close=lambda: None,
                         section_akeys=lambda: {1: 1, 2: 2, 3: 1}, section_places=lambda: {1: 0, 2: 0, 3: 1})
    monkeypatch.setattr(SR, "province_store", lambda rel: st)
    assert R.fresh_regions(tmp_path, "h") == {1: frozenset({"1"}), 3: frozenset({"2", "7a"})}
    with pytest.raises(SystemExit, match="section handles"):
        R.fresh_regions(tmp_path, "other")
