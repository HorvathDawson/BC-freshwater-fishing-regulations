"""T697 (owner 2026-11-12: "Why would this cause a cut? All cuts have a name? So then it is mouth to cut the cut to head?
But yes unnamed lakes should be unnamed lake near x"). A water's parts run mouth → cut, cut → cut, cut → head:
  * an unnamed lake at the water's OWN top (Kanaka Creek begins at one) is its head, said "Source" like every water's;
    at its own bottom, "Mouth";
  * an unnamed lake two parts meet at mid-water (Slack Creek) is said "Unnamed lake near X" (the bundle's
    `crossing_near`, the source T517 uses for every crossing), else "Unnamed lake (km N)".
Synthetic waters through `builders._names` and the one end resolver (`part.end_words`)."""
import pytest

from pipeline.deliver.answers.display.builders import _names
from pipeline.deliver.answers.display.part import Water, end_words


def _run(frm, to, km_from, km_to):
    return {"from": frm, "to": to, "km_from": km_from, "km_to": km_to}


def _water(parts, splits=None, waters=None):
    w = {"name": "Kanaka Creek", "parts": [{"ruleset": i, "sections": 10 - i, "grp": i, "run": 0, "runs": runs}
                                           for i, runs in enumerate(parts)]}
    doc = {"waters": {"gnis:1": w, **(waters or {})}, "rulesets": [{} for _ in parts], "rules": {},
           "splits": splits or {}}
    return Water("gnis:1", w, doc)


def _said(W, crossings=None):
    got = _names(W, crossings or {})
    return [f"{end_words(W.doc, n['lo'], 'to')} → {end_words(W.doc, n['hi'], 'from')}" for n in got]


SPLITS = {"kanaka__falls": {"name": "Cliff Park Falls", "km": 8.2, "water_id": "gnis:1"}}


def test_an_unnamed_lake_at_the_waters_own_top_is_its_head():
    # Kanaka Creek: mouth → Cliff Park Falls, then the falls → the unnamed lake the creek begins at (24.8 km)
    W = _water([[_run("kanaka__falls", "mouth", 8.2, 0.0)], [_run("lake_outlet", "kanaka__falls", 24.8, 8.2)]],
               SPLITS)
    assert sorted(_said(W)) == ["Cliff Park Falls → Source", "Mouth → Cliff Park Falls"]


def test_an_unnamed_lake_at_the_waters_own_bottom_is_its_mouth():
    W = _water([[_run("kanaka__falls", "lake_inlet", 8.2, 0.0)], [_run("source", "kanaka__falls", 24.8, 8.2)]],
               SPLITS)
    assert sorted(_said(W)) == ["Cliff Park Falls → Source", "Mouth → Cliff Park Falls"]


def test_an_unnamed_lake_mid_water_is_said_near_what_stands_there():
    # Slack Creek: the rules change across an unnamed lake (outlet 2.31 km, inlet 3.0 km), no cut there
    W = _water([[_run("lake_outlet", "mouth", 2.31, 0.0)], [_run("source", "lake_inlet", 8.47, 3.0)]],
               waters={"gnis:2": {"name": "Foo Creek"}})
    near = {("gnis:1", "lake_outlet"): [(2.31, "confluence:gnis:2")], ("gnis:1", "lake_inlet"): [(3.0, "confluence:gnis:2")]}
    assert sorted(_said(W, near)) == ["Mouth → Unnamed lake near the Foo Creek confluence",
                                      "Unnamed lake near the Foo Creek confluence → Source"]


def test_an_unnamed_lake_mid_water_with_nothing_named_near_is_said_at_its_km():
    W = _water([[_run("lake_outlet", "mouth", 2.31, 0.0)], [_run("source", "lake_inlet", 8.47, 3.0)]])
    assert sorted(_said(W)) == ["Mouth → Unnamed lake (km 2.3)", "Unnamed lake (km 3) → Source"]


def test_a_named_lake_keeps_its_name():
    W = _water([[_run("lake_outlet:wbk:9", "mouth", 2.31, 0.0)], [_run("source", "lake_inlet:wbk:9", 8.47, 3.0)]],
               waters={"wbk:9": {"name": "Kanaka Lake"}})
    assert sorted(_said(W)) == ["Kanaka Lake → Source", "Mouth → Kanaka Lake outlet"]


@pytest.mark.needs_bundle
def test_province_wide_no_part_ends_at_a_lake_or_a_count(request):
    """Every water of several parts on the built export: no name says "A lake" / "a lake outlet", none counts stretches
    or parts, none repeats — and (owner 2026-11-12, "Yes, read the real boundary", RULINGS P55 follow-up) NO part end
    says "Unnamed lake …": the "near X" / "(km N)" words are a guarded last resort used by ZERO ends; a lake edge reads
    the real boundary standing there (the cut aliased onto it, the region line, the B.C. border — `bundle.spans`).
    2026-11-12 side bundle: Slack Creek reads "Region 8 boundary", Jones Creek "B.C. border", Gagnon Creek's outlet end
    the signs cut. ONE end stands where no line and no cut is (`OPEN`), pinned so it cannot grow or linger."""
    import re
    from pipeline.deliver.answers import common as C
    from pipeline.deliver.answers.display.builders import _crossings
    from pipeline.deliver.export.codec import expand
    from pipeline.tests import under_test
    d = under_test.export_dir(request)
    data, guide = C.load_export(d)
    doc = expand(data, guide)
    cr = _crossings(str(under_test.bundle(request)))
    bad, waters, last_resort = [], 0, {}
    for iid, w in sorted(doc["waters"].items()):
        W = Water(iid, w, doc)
        if len(W.parts) < 2:
            continue
        waters += 1
        said = [f"{end_words(doc, n['lo'], 'to')} → {end_words(doc, n['hi'], 'from')}"
                for n in _names(W, cr) if n["lo"] is not None]
        lake = [s for s in said if re.search(r"[Uu]nnamed lake", s)]
        if lake:
            last_resort[iid] = lake
        bad += [f"{iid} {s}" for s in said
                if re.search(r"\b[Aa] lake\b|\b(stretch|part) \d+ of\b|\d+ stretches", s)]
        bad += [f"{iid} said twice: {s}" for i, s in enumerate(said) if s in said[:i]]
        if iid == "gnis:11318":
            assert "Cliff Park Falls → Source" in said, said
    assert waters > 500
    assert bad == []
    # the last resort: used by NO end but the open one, which must still be there (resolve it, then empty `OPEN`)
    assert {i: len(v) for i, v in last_resort.items()} == {i: 1 for i in OPEN}, last_resort


#: Part ends with no real boundary to read, each tied to its open owner question. Gagnon Creek (gnis:7733): part 1
#: begins at the INLET (km 3.66) of a 57.8 ha unnamed lake (wbk 329465442); its fishing boundary signs are aliased onto
#: the lake's OUTLET edge (km 1.84, the nearer edge, `sectionizer._alias_to_lake_edge`), and no region line, border or
#: cut stands at the inlet. Open owner question (T697): is that lake part of Williston Lake?
OPEN = ("gnis:7733",)
