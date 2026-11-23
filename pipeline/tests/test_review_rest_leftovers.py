"""T775: the rest-of leftovers in the review app's backend — (1) a rest with no section of its own says it stands behind its
listed places, not 'binds NOTHING alone — the build refuses it'; (2) the editor gets the water's places on open, not only
after a preview; (3) the listed places are drawn beside the rest."""
from __future__ import annotations

import importlib
import sys
from types import SimpleNamespace

sys.path.insert(0, "curation-review/backend")

from pipeline.atlas.reach.build import REST_BEHIND            # noqa: E402
from pipeline.tests.test_review_audit import place            # noqa: E402


class _Dfo:
    """the backend `dfo` as it is NOW: another test may reload the backend modules, so a module-level import would
    patch a stale copy"""
    def __getattr__(self, k):
        return getattr(importlib.import_module("dfo"), k)


dfo = _Dfo()


def _reuse():
    return importlib.import_module("dfo").reuse


def _two_places(monkeypatch, rest_extent):
    a = place("6:river:a", words="the rest", extents=[rest_extent])
    b = place("6:river:b", words="the closure", extents=[{"op": "whole", "item_id": "gnis:1"}])
    for p in (a, b):
        p["water_id"] = "6:river"
        p.pop("_file", None)                                  # the file name is the loader's, not an EntryLocation field
    monkeypatch.setattr(importlib.import_module("dfo"), "_locations", lambda: {p["location_id"]: ("6", p, None) for p in (a, b)})
    return a, b


def test_a_place_that_is_no_rest_yet_still_lists_the_waters_places_on_open(monkeypatch):
    a, b = _two_places(monkeypatch, {"op": "whole", "item_id": "gnis:1"})
    assert dfo.rest_of_card(a) is None                                   # the card: no rest, no rest block
    got = dfo.rest_of_card(a, any_place=True)                            # the editor on open
    assert [p["id"] for p in got["places"]] == ["6:river:b"] and got["answers_first"] == []
    assert got["places"][0]["listed"] is False
    assert dfo.extent_now(a, None).get("rest_of", {}).get("places")      # `extent_now` carries it for any place


def test_a_rest_still_lists_what_it_leaves_out(monkeypatch):
    a, b = _two_places(monkeypatch, {"op": "rest", "item_id": "gnis:1"})
    got = dfo.rest_of_card(a)
    assert [(p["id"], p["listed"]) for p in got["places"]] == [("6:river:b", True)]


def test_the_listed_places_are_drawn_beside_the_rest(monkeypatch):
    reuse = _reuse()
    binding = SimpleNamespace(sections=["s1"], outcome=SimpleNamespace(value="bound"))
    diags = [SimpleNamespace(kind=REST_BEHIND, payload={"behind": {"6:river:b": ["s2", "s3"]}}),
             SimpleNamespace(kind="complement", payload={})]
    import pipeline.atlas.reach.build as B
    monkeypatch.setattr(B, "build_reach", lambda *a, **k: (binding, diags))
    monkeypatch.setattr(importlib.import_module("dfo"), "section_km", lambda ids: 0.0)
    monkeypatch.setattr(importlib.import_module("dfo"), "channels_in", lambda *a: [])
    monkeypatch.setattr(reuse, "_registry", lambda: {})
    monkeypatch.setattr(reuse, "_graph", lambda: SimpleNamespace(nodes={}))
    monkeypatch.setattr(reuse, "_sections_geojson",
                        lambda ids, kind: [{"type": "Feature", "properties": {"node_id": i}, "geometry": None} for i in ids])
    out = dfo._bind({"entry_id": "6:river:a"}, {"rule_id": "r", "extents": [1]}, 100)
    feats = out["geojson"]["features"]
    assert [f["properties"]["node_id"] for f in feats if not f["properties"].get("listed")] == ["s1"]
    assert sorted((f["properties"]["node_id"], f["properties"]["place"]) for f in feats if f["properties"].get("listed")) \
        == [("s2", "6:river:b"), ("s3", "6:river:b")]
    assert out["listed_drawn"] == {"6:river:b": 2}


def _preview_with_no_own_sections(monkeypatch, op, listed):
    """`extent_preview` of a ONE-piece draft whose own binding is 0 sections (the binding itself is stubbed)."""
    reuse = _reuse()
    from pipeline.regs.review import marks as M
    import pipeline.atlas.reach.dfo_salmon as DS
    a, b = _two_places(monkeypatch, {"op": "whole", "item_id": "gnis:1"})
    monkeypatch.setattr(importlib.import_module("dfo"), "_reach_input", lambda *x, **k: ({"entry_id": a["location_id"]}, {"rule_id": "r", "extents": [1]}))
    monkeypatch.setattr(importlib.import_module("dfo"), "_walks_tributaries", lambda *x: False)
    monkeypatch.setattr(importlib.import_module("dfo"), "_bind", lambda *x: {"resolvable": True, "n_sections": 0})
    monkeypatch.setattr(importlib.import_module("dfo"), "rest_of_card", lambda *x, **k: {"answers_first": [], "named": False,
                                                              "places": [{"id": "6:river:b", "listed": listed}]})
    monkeypatch.setattr(importlib.import_module("dfo"), "dead_why", lambda *x: ["binds nothing"])
    monkeypatch.setattr(DS, "dead_pieces", lambda *x, **k: [])
    monkeypatch.setattr(reuse, "_registry", lambda: {})
    monkeypatch.setattr(reuse, "_graph", lambda: SimpleNamespace(nodes={}))
    fix = M.ExtentFix(scope="entry", op=op, points=[], items=["gnis:1"], tributaries=None, area=None)
    return dfo.extent_preview(a["location_id"], fix)


def test_a_rest_with_no_section_of_its_own_stands_behind_its_listed_places_the_build_accepts_it(monkeypatch):
    got = _preview_with_no_own_sections(monkeypatch, "rest", listed=True)
    assert got.get("rest_stands_behind") is True and "dead_pieces" not in got
    # not a rest (or a rest that leaves nothing out): still the dead-piece words
    assert _preview_with_no_own_sections(monkeypatch, "whole", listed=True).get("dead_pieces") == [1]
    assert _preview_with_no_own_sections(monkeypatch, "rest", listed=False).get("dead_pieces") == [1]
