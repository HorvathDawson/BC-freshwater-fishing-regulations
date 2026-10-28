"""tidal_words: the switchable points (alternates) and the words-win rail locator."""
from shapely.geometry import LineString

from pipeline.tools.audit import tidal_words as TW


def _cand(**kw):
    c = {"id": "tidal:1", "water": "X River", "mouth": {"measure": 0.0, "coord": [0.0, 0.0]},
         "polygon": {"measure": 1000.0, "coord": [0.0, 0.01], "cuts": [300.0, 600.0, 1000.0]},
         "proposed": {"measure": 37.0, "above_mouth_m": 37.0, "coord": [0.0, 0.0003],
                      "split_name": "X River: CN Railway Bridge (tidal boundary)"},
         "sources": [], "triage": {"web": []},
         "words_point": {"measure": 37.0, "coord": [0.0, 0.0003], "words": "X River: CN Railway Bridge",
                         "source": "synopsis", "located_by": "OSM rail", "confidence": "high"}}
    c.update(kw)
    return c


def at(m):
    return [0.0, round(m / 100000, 6)]


def test_alternates_cover_words_polygon_mouth_dem():
    alts = TW.alternates(_cand(), at, {"dem_limit_m": 120.0, "dem_coord": [0.0, 0.0012], "status": "disagree"})
    kinds = [a["kind"] for a in alts]
    assert kinds.count("words") == 1 and kinds.count("polygon") == 3
    assert "mouth" in kinds and "dem" in kinds
    w = next(a for a in alts if a["kind"] == "words")
    assert w["proposed"] and w["supported"]
    up = [a for a in alts if a.get("uppermost")]
    assert len(up) == 1 and up[0]["measure"] == 1000.0
    assert next(a for a in alts if a["kind"] == "dem")["measure"] == 120.0


def test_dem_from_an_older_preverify_uses_the_judged_proposal():
    alts = TW.alternates(_cand(), at, {"delta_m": 100.0, "judged_above_mouth_m": 1000.0})
    assert next(a for a in alts if a["kind"] == "dem")["measure"] == 900.0


def test_proposal_on_no_alternate_is_listed():
    c = _cand(words_point=None, proposed={"measure": 500.0, "above_mouth_m": 500.0, "coord": [0, 0.005],
                                          "split_name": "X River: reused"})
    alts = TW.alternates(c, at, None)
    assert alts[0]["kind"] == "proposed" and alts[0]["proposed"]


def test_rail_crossing_takes_the_named_operator_not_the_nearest_line():
    segs = [{"fid": 1, "down": 0.0, "length": 1000.0}]
    geoms = {1: LineString([(0, 0), (0, 1000)])}
    ways = [{"id": 1, "tags": {"railway": "rail", "operator": "TransLink", "name": "SkyTrain"},
             "g": LineString([(-50, 900), (50, 900)])},
            {"id": 2, "tags": {"railway": "rail", "operator": "BNSF Railway"},
             "g": LineString([(-50, 400), (50, 400)])},
            {"id": 3, "tags": {"railway": "rail", "operator": "BNSF Railway", "service": "yard"},
             "g": LineString([(-50, 100), (50, 100)])}]
    hits = TW.crossings(segs, geoms, ways, TW._rail_pred(TW.BN))
    assert [h["measure"] for h in hits] == [400.0]
