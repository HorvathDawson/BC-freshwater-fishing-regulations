"""fwa_match — fuzzy, FWA-favouring classification of municipal lines against FWA streams."""

from shapely.geometry import LineString

from pipeline.added_streams.fwa_match import (_FwaIndex, classify, line_to_albers, match_features)
from pipeline.models import BlkChain


def _chain(blk, wsc, coords, gnis_name="Big River", gnis_id="111"):
    geom = line_to_albers(coords)
    return BlkChain(blk=blk, fwa_watershed_code=wsc, fids=(), geometry=geom,
                    mouth_measure=0.0, length_m=geom.length, name_tuples=(),
                    gnis_id=gnis_id, gnis_name=gnis_name)


# FWA mainstem: south (mouth) -> north, ~49.20 to 49.23
_FWA = _chain("1000", "100-100000", [(-123.0, 49.20), (-123.0, 49.23)])
_INDEX = _FwaIndex([_FWA])


def _muni(coords):
    return classify(line_to_albers(coords), _INDEX)


def test_exact_subset_is_duplicate():
    m = _muni([(-123.0, 49.205), (-123.0, 49.225)])       # lies on the FWA line, only part of it
    assert m.klass == "duplicate" and m.fwa_blk == "1000"


def test_offset_parallel_subset_is_duplicate_favour_fwa():
    m = _muni([(-123.00025, 49.205), (-123.00025, 49.225)])  # ~18 m east, parallel -> still FWA
    assert m.klass == "duplicate"


def test_extension_keeps_only_the_novel_tail():
    m = _muni([(-123.0, 49.21), (-123.0, 49.25)])         # hugs FWA then runs past its north end
    assert m.klass == "extension"
    assert m.fwa_blk == "1000" and m.fwa_wsc == "100-100000"
    assert m.clip3005 is not None and m.clip3005.length < m.line3005.length
    assert m.divergence is not None


def test_far_line_is_novel():
    m = _muni([(-122.5, 49.50), (-122.5, 49.52)])
    assert m.klass == "novel" and m.fwa_blk == ""


def test_named_line_partly_hugging_a_differently_named_fwa_is_novel():
    """Brackendale Creek regression. A NAMED municipal creek that runs along a DIFFERENTLY-named FWA stream
    for part of its length (partial coverage, mostly unique) is its OWN creek — not a duplicate of the FWA it
    merely hugs. Only a name match, or high (>= dup) coverage, makes it a duplicate."""
    dryden = _chain("1000", "100-100000", [(-123.0, 49.20), (-123.0, 49.24)], gnis_name="Dryden Creek")
    idx = _FwaIndex([dryden])
    # unique east -> onto Dryden (a mid-section overlap) -> back east unique; NEITHER end touches Dryden, so
    # it is not an end-tail extension, and only ~40% lies on the FWA.
    brack = line_to_albers([(-123.02, 49.205), (-123.0, 49.214),
                            (-123.0, 49.230), (-123.02, 49.238)])
    m = classify(brack, idx, muni_name="Brackendale Creek")
    assert m.klass == "novel", f"a differently-named, mostly-unique creek is novel, got {m.klass}"
    assert m.clip3005 is not None and m.clip3005.length < brack.length, "keeps only the unique reach"
    assert classify(brack, idx, muni_name="Dryden Creek").klass == "duplicate", "same name -> a re-draw"


def test_name_conflict_candidate_fwa_boss():
    feats = [{"type": "Feature", "properties": {"name": "Local Creek"},
              "geometry": {"type": "LineString", "coordinates": [(-123.0, 49.205), (-123.0, 49.225)]}}]
    _, cands, report = match_features(feats, [_FWA])
    assert len(cands) == 1
    c = cands[0]
    assert c.name == "Local Creek" and c.conflict and not c.display   # FWA named -> alias, not label
    assert c.target_blk == "1000" and c.target_gnis == "111"
    assert report["name_conflicts"]


def test_unnamed_fwa_gets_display_candidate():
    fwa_unnamed = _chain("2000", "100-200000", [(-123.0, 49.20), (-123.0, 49.23)],
                         gnis_name="", gnis_id="")
    feats = [{"type": "Feature", "properties": {"name": "Hidden Creek"},
              "geometry": {"type": "LineString", "coordinates": [(-123.0, 49.205), (-123.0, 49.225)]}}]
    _, cands, _ = match_features(feats, [fwa_unnamed])
    assert cands and cands[0].display and not cands[0].conflict     # FWA unnamed -> municipal can label
