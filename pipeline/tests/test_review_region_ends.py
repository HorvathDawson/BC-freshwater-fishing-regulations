"""T742 C (owner 2026-11-14: "make sure the region cuts are in all the app"): every end picker in the review app offers
the region-line crossings of the water — named plainly the way the app names them ("Region 3 boundary"), with the km,
binding the crossing's own id — and the stretch where two regions meet (the Fraser km 418-453, Regions 3 and 5) is
named at both its ends, so the owner includes it (picks its far end) or leaves it out (picks its near end).
The world is a stand-in of the Fraser mainstem's crossings as the served atlas has them; nothing is read from disk."""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

BACKEND = Path(__file__).resolve().parents[2] / "curation-review" / "backend"
BLK = "356364114"
#: the Fraser mainstem's region crossings in the served atlas (route measure, m)
CROSSINGS = [("area:5", 418405.42), ("area:3", 452701.47), ("area:7A", 674465.15), ("area:5", 703745.58)]
#: the regions the sections between them are members of (the atlas's one membership)
SPANS = [(0, 418405.42, {"3"}), (418405.42, 452701.47, {"3", "5"}), (452701.47, 674465.15, {"5"}),
         (674465.15, 703745.58, {"5", "7a"}), (703745.58, 1_400_000, {"7a"})]


class _B:
    def __init__(self, bid, aliases=()):
        self.id, self.label, self.kind, self.ref, self.wbk = bid, "Region 3 – Region 5 boundary", "area", f"split:{bid}", ""
        self._aliases = aliases

    def answers_to(self):
        return [self.id, *self._aliases]


@pytest.fixture
def R(monkeypatch):
    monkeypatch.syspath_prepend(str(BACKEND))
    sys.modules.pop("reuse", None)
    import reuse
    item = SimpleNamespace(id="gnis:39325", name="Fraser River", ref_ids=(f"blk:{BLK}",),
                           boundaries=(_B("area:5", ("area:5@down", "area:5@up")), _B("area:3"), _B("area:7A")))
    monkeypatch.setattr(reuse, "_registry", lambda: {"gnis:39325": item})
    monkeypatch.setattr(reuse, "_region_rows", lambda: {BLK: list(CROSSINGS)})
    monkeypatch.setattr(reuse, "_blk_index", lambda: {BLK: [(d, u, f"{BLK}:{int(d)}") for d, u, _r in SPANS]})
    monkeypatch.setattr(reuse, "_section_regions", lambda: {f"{BLK}:{int(d)}": frozenset(r) for d, _u, r in SPANS})
    from shapely.geometry import Point
    monkeypatch.setattr(reuse, "_point_at", lambda blk, m: Point(1_400_000.0, 500_000.0 + m / 100))   # BC Albers
    reuse.region_ends.cache_clear()
    yield reuse
    reuse.region_ends.cache_clear()
    sys.modules.pop("reuse", None)


def test_the_fraser_offers_each_crossing_by_its_plain_name_km_and_the_id_that_binds_it(R):
    got = R.region_ends("gnis:39325")
    assert [(e["id"], e["km"], e["bindable"]) for e in got] == [
        ("area:5@down", 418, True), ("area:3", 453, True), ("area:7A", 674, True), ("area:5@up", 704, True)]
    assert [e["label"] for e in got] == [
        "Region 5 boundary · km 418 — where Region 3 and Region 5 meet starts here, upstream to km 453",
        "Region 3 boundary · km 453 — where Region 3 and Region 5 meet ends here (it runs down to km 418)",
        "Region 7A boundary · km 674 — where Region 5 and Region 7A meet starts here, upstream to km 704",
        "Region 5 boundary · km 704 — where Region 5 and Region 7A meet ends here (it runs down to km 674)"]
    assert (got[0]["below"], got[0]["above"]) == (["3"], ["3", "5"])
    lon, lat = got[0]["coord"]                     # lon/lat, never the atlas's metres (the map refused 500,000 as a latitude)
    assert -140 < lon < -110 and 47 < lat < 61
    # never the atlas label, which named the same neighbour pair on two different cuts
    assert not any("–" in e["label"] for e in got)


def test_a_picker_list_takes_the_crossings_in_place_of_the_bare_region_ids(R):
    bs = [{"id": "area:5", "label": "Region 3 – Region 5 boundary", "kind": "area", "curated": True, "item_id": "gnis:39325",
           "aliases": ["area:5@down", "area:5@up"]},
          {"id": "area:3", "label": "Region 3 – Region 5 boundary", "kind": "area", "curated": True, "item_id": "gnis:39325"},
          {"id": "fraser_river__hope", "label": "Hope bridge", "kind": "split", "curated": True, "item_id": "gnis:39325"}]
    got = R.with_region_ends(bs, lambda b: b.get("item_id"))
    assert [(b["id"], b.get("pickable", True)) for b in got] == [
        ("area:5@down", True), ("area:5@up", True), ("area:3", True), ("fraser_river__hope", True)]
    assert got[0]["label"].startswith("Region 5 boundary · km 418") and got[-1] is bs[-1]


def test_a_middle_crossing_is_listed_with_why_and_never_offered(R, monkeypatch):
    monkeypatch.setattr(R, "_region_rows", lambda: {BLK: [("area:5", 418405.42), ("area:5", 500000.0),
                                                          ("area:5", 703745.58)]})
    R.region_ends.cache_clear()
    mid = R.region_ends("gnis:39325")[1]
    assert (mid["id"], mid["bindable"]) == ("area:5", False) and "middle crossing" in mid["why"]
    got = R.with_region_ends([{"id": "area:5", "label": "x", "kind": "area", "curated": True}], lambda b: "gnis:39325")
    assert [b["pickable"] for b in got] == [True, False, True] and "middle crossing" in got[1]["label"]


def test_a_region_cut_is_said_plainly_wherever_it_is_named():
    sys.path.insert(0, str(BACKEND))
    try:
        sys.modules.pop("reuse", None)
        import reuse
        assert reuse.region_words("area:3") == "Region 3 boundary"
        assert reuse.region_words("area:5@down") == "Region 5 boundary (its lowest crossing of this water)"
        assert reuse.region_words("area:7A@up") == "Region 7A boundary (its highest crossing of this water)"
        assert reuse.region_words("area:Fraser River — Landstrom Bar sign zone") is None
        assert reuse.region_words("fraser_river__hope") is None
    finally:
        sys.path.remove(str(BACKEND))
        sys.modules.pop("reuse", None)


def test_the_cut_search_offers_the_askers_region_crossings_first(R, monkeypatch):
    monkeypatch.setattr(R, "_split_records", lambda: {"x": ("Thompson River", {"label": "Region 3 sign"})})
    got = R.search_splits("region", first=["Fraser River"])
    assert [h["id"] for h in got] == ["area:5@down", "area:3", "area:7A", "area:5@up", "x"]
    assert got[0]["own"] and got[0]["label"].startswith("Region 5 boundary · km 418")
    assert [h["id"] for h in R.search_splits("region")] == ["x"]          # no water named: no region crossings


# ---------------------------------------------------------------------------- owner 2026-11-15, mark 2578: ONE NAMER

def test_cut_name_reads_a_region_line_on_this_waters_crossing_never_another_waters_atlas_row(R, monkeypatch):
    """mark 2578: the checklist read "from — ours: from Region 3 – Region 8 boundary" (the one atlas row of `area:3`
    sits on another water). `cut_name` is the one namer: on the Fraser it is the Fraser's crossing at its km."""
    monkeypatch.setattr(R, "_splits_meta", lambda: {"area:3": {"label": "Region 3 – Region 8 boundary"}})
    monkeypatch.setattr(R, "_split_records", lambda: {})
    monkeypatch.setattr(R, "_boundary_index", lambda: {})
    assert R.cut_name("area:3", ["gnis:39325"]) == (
        "Region 3 boundary · km 453 — where Region 3 and Region 5 meet ends here (it runs down to km 418)")
    assert R.cut_name("area:5@up", ["gnis:39325"]).startswith("Region 5 boundary · km 704")
    # a line this water crosses twice, named without saying which crossing: said so, never one of them guessed
    assert R.cut_name("area:5", ["gnis:39325"]) == (
        "Region 5 boundary — this water crosses it 2 times (km 418, km 704): the extent does not say which")
    # read on no water: the plain name, never the atlas row
    assert R.cut_name("area:3") == "Region 3 boundary"
    assert R.cut_point_on("area:3", ["gnis:39325"]) is not None and R.cut_point_on("area:3") is None


def test_the_checklist_names_a_region_end_by_the_pieces_water():
    """the row generator takes the water-dependent name from `Context.on_water` (the backend hands in `cut_name`), and
    a cut with one name everywhere keeps its label"""
    from pipeline.regs.review import checklist as C
    names = {("area:3", ("gnis:39325",)): ("Region 3 boundary · km 453", (-122.28, 51.52))}
    ctx = C.Context(labels={"area:3": "Region 3 – Region 8 boundary", "spuzzum": "Spuzzum Creek → Fraser River"},
                    items={"gnis:39325": "Fraser River"}, built=frozenset({"area:3", "spuzzum"}),
                    on_water=lambda sid, items: names.get((sid, items)))
    rows = C.rows(C.Source(water="Fraser River", place="from the Region 3 boundary to Spuzzum Creek"),
                  [{"op": "between", "splits": ["area:3", "spuzzum"]}], own_items=["gnis:39325"], ctx=ctx)
    frm = next(r for r in rows if r["kind"] == "from")
    to = next(r for r in rows if r["kind"] == "to")
    assert frm["ours"] == "from Region 3 boundary · km 453" and frm["anchor"]["coord"] == [-122.28, 51.52]
    assert frm["state"] == "ok" and to["ours"] == "to Spuzzum Creek → Fraser River"
    assert all("Region 8" not in r["ours"] for r in rows)


def test_the_tributary_row_quotes_every_clause_it_takes():
    """(CI 2026-11-15, Bulkley ~2) "… other than Morice River and tributaries, Suskwa River and tributaries, and Two Mile
    Creek": the tributary row takes every clause that speaks of tributaries, so every one is on its quote"""
    from pipeline.regs.review import checklist as C
    src = C.Source(water="Bulkley River", place="all tributaries of the Bulkley River other than Morice River and "
                   "tributaries, Suskwa River and tributaries, and Two Mile Creek")
    rows = C.rows(src, [{"op": "whole", "item_id": "gnis:2195"}], own_items=["gnis:2195"], ctx=C.Context(),
                  tributaries=True, tributaries_only=True)
    [t] = [r for r in rows if r["kind"] == "tributaries"]
    assert "Morice River and tributaries" in t["quote"] and "Suskwa River and tributaries" in t["quote"]
