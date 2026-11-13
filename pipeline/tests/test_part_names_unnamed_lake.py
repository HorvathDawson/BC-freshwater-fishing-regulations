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
    2026-11-12 side bundle: Slack Creek reads "Region 8 boundary", Jones Creek "B.C. border". An end standing where no
    line and no cut is would be listed in `OPEN` with its owner question, pinned so it cannot grow or linger (none left)."""
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


#: Part ends with no real boundary to read, each tied to its open owner question. None left: Gagnon Creek (gnis:7733) was
#: the last — answered 2026-11-13 ("Mouth is still Williston", RULINGS P57): its signs are its mouth on Williston Lake
#: and its unnamed lake splits no rule set, so the creek is one part, "The mouth (at Williston Lake) → Source"
#: (candidate atlas full_next, side deliver 2026-11-13).
OPEN: tuple[str, ...] = ()


# ---------------------------------------------------------------------------------------------------------------------
# T711 (owner 2026-11-13): the part HEADER (`runs`) follows the part's name (the header says a source as every water's
# header does, "up to the top of the river": `end_name("source", "from")`). A top read "Cliff Park Falls → Source" while
# its header said "From the Cliff Park Falls up to a lake outlet": the name took P55's own-end rule, the header did not.
# Both now read `part.own_end`.
# ---------------------------------------------------------------------------------------------------------------------
def _headers(W):
    from pipeline.deliver.answers.display.part import runs_label, water_ends_km
    return [runs_label(W.doc, p, water_ends_km(W.w)) for p in W.parts]


def test_the_header_of_a_top_at_an_unnamed_lake_says_the_source():
    W = _water([[_run("kanaka__falls", "mouth", 8.2, 0.0)], [_run("lake_outlet", "kanaka__falls", 24.8, 8.2)]],
               SPLITS)
    assert _headers(W) == ["From the mouth up to the Cliff Park Falls", "From the Cliff Park Falls up to the top of the river"]


def test_the_header_of_a_bottom_at_an_unnamed_lake_says_the_mouth():
    W = _water([[_run("kanaka__falls", "lake_inlet", 8.2, 0.0)], [_run("source", "kanaka__falls", 24.8, 8.2)]],
               SPLITS)
    assert _headers(W)[0] == "From the mouth up to the Cliff Park Falls"


def test_a_mid_water_unnamed_lake_keeps_its_header_words():
    """Only the water's OWN ends: an unnamed lake two parts meet at is not the source (MUTATION: `own_end` ignoring km)."""
    W = _water([[_run("lake_outlet", "mouth", 2.31, 0.0)], [_run("source", "lake_inlet", 8.47, 3.0)]])
    assert _headers(W) == ["From the mouth up to a lake outlet", "From a lake up to the top of the river"]


def test_without_the_waters_ends_the_header_is_the_old_one():
    """MUTATION: the header built without the water's ends (the bug) says "a lake outlet" under a "→ Source" name."""
    from pipeline.deliver.answers.display.part import runs_label
    W = _water([[_run("kanaka__falls", "mouth", 8.2, 0.0)], [_run("lake_outlet", "kanaka__falls", 24.8, 8.2)]],
               SPLITS)
    assert runs_label(W.doc, W.parts[1]) == "From the Cliff Park Falls up to a lake outlet"


@pytest.mark.needs_bundle
def test_province_wide_every_source_top_has_a_source_header(request):
    """Every part on the built export whose name ends "→ Source" because an unnamed lake is the water's own top (P55's
    62) has a header that says the top of the river, never "a lake outlet"; and every part's header agrees with its name about
    the top: a "source" name end is never a header "lake outlet". Recomputed from the export with the code under test."""
    from pipeline.deliver.answers import common as C
    from pipeline.deliver.answers.display.builders import _crossings
    from pipeline.deliver.answers.display.part import runs_label, water_ends_km
    from pipeline.deliver.export.codec import expand
    from pipeline.tests import under_test
    doc = expand(*C.load_export(under_test.export_dir(request)))
    cr = _crossings(str(under_test.bundle(request)))
    p55, bad, old = 0, [], 0
    for iid, w in sorted(doc["waters"].items()):
        W = Water(iid, w, doc)
        if len(W.parts) < 2:
            continue
        for p, n in zip(W.parts, _names(W, cr)):
            main = [r for r in p.get("runs") or [] if not r.get("branch") and r.get("km_from") is not None]
            top = max(main, key=lambda r: r["km_from"]) if main else None
            if n["hi"] != "source" or not top or top.get("from") != "lake_outlet":
                continue
            p55 += 1
            h = runs_label(doc, p, water_ends_km(w))
            if "lake outlet" in h or "up to the top of the river" not in h:
                bad.append(f"{iid}: {h}")
            old += "a lake outlet" in runs_label(doc, p)        # the bug, for the mutation count
    assert bad == []
    assert p55 >= 50 and old == p55, (p55, old)             # P55 counted 62 tops on 2026-11-12 (62 on r-adc4e248a163)


def test_signs_that_are_the_place_read_as_the_signs():
    """T708 Gagnon (RULINGS P57: mouth → signs → Source): a sign cut whose words after "signs" name no place — a bare
    placement note, or "its mouth" — reads as the signs; a sign cut AT a named place still loses the sign-post words."""
    sp = {"g__signs": {"name": "fishing boundary signs (200 m upstream of the lake on the creek)", "km": 3.9,
                       "water_id": "gnis:1"},
          "g__old": {"name": "fishing boundary signs near its mouth", "km": 3.9, "water_id": "gnis:1"},
          "p__pool": {"name": "signs at the tail of the canyon pool (300 m downstream)", "km": 5.0, "water_id": "gnis:1"}}
    W = _water([[_run("g__signs", "mouth", 3.9, 0.0)], [_run("source", "g__signs", 9.0, 3.9)]], splits=sp)
    assert _said(W) == ["Mouth → Fishing boundary signs (200 m upstream of the lake on the creek)",
                        "Fishing boundary signs (200 m upstream of the lake on the creek) → Source"]
    assert end_words(W.doc, "g__old", "to") == "Fishing boundary signs near its mouth"
    assert end_words(W.doc, "p__pool", "to") == "The tail of the canyon pool (300 m downstream)"
