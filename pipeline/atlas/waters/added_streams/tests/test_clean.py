"""clean_features — raw municipal props -> uniform cruft-free schema, MLS flattened; only source-scoped
name exclusions are filtered (bad/duplicated data), nothing else."""

from pipeline.atlas.waters.added_streams.clean import clean_features

_UNIFORM = {"source", "src_id", "name", "connect_to_hint", "ftype", "fish", "species", "trib_parent", "drainage"}


def _ls(coords, **props):
    return {"type": "Feature", "properties": props,
            "geometry": {"type": "LineString", "coordinates": coords}}


def test_abbotsford_strips_cruft_titlecases_and_keeps_ditches():
    feats = clean_features([
        _ls([[0, 0], [1, 1]], OBJECTID=5, STREAM_NAME="STONEY CREEK",
            FISH_CLASS="UNOFFICIAL: CLASS A (RED)", created_user="x", last_edited_date="y", GlobalID="g"),
        _ls([[2, 2], [3, 3]], OBJECTID=6, STREAM_NAME="DITCH-12-A", FISH_CLASS="UNOFFICIAL: CLASS C1 (GREEN)"),
    ], "abbotsford")
    assert set(feats[0]["properties"]) == _UNIFORM             # no created_user/GlobalID cruft survives
    assert feats[0]["properties"]["name"] == "Stoney Creek"    # ALL-CAPS title-cased
    assert feats[0]["properties"]["fish"] == "Class A"
    # ditch kept (nothing filtered), tagged, name blanked
    assert feats[1]["properties"]["ftype"] == "ditch" and feats[1]["properties"]["name"] == ""
    assert feats[1]["properties"]["drainage"] and not feats[0]["properties"]["drainage"]   # owner 2026-10-29


def test_port_moody_drainage_from_its_own_type_fields():
    """Owner 2026-10-29: a culvert / ditch / hidden-drainage piece is marked drainage (never added); a stream is not."""
    feats = clean_features([
        _ls([[0, 0], [1, 0]], OBJECTID=1, local_stream_name="Dallas Creek", theme="Culvert"),
        _ls([[1, 0], [2, 0]], OBJECTID=2, theme="Stream", stream_type="HIDDEN DRAINAGE"),
        _ls([[2, 0], [3, 0]], OBJECTID=3, local_stream_name="Dallas Creek", theme="Stream"),
    ], "port_moody")
    assert [bool(f["properties"]["drainage"]) for f in feats] == [True, True, False]


def test_squamish_name_alias_fixes_typo():
    """A misspelled StreamName ('Willson Slough') is normalised to the canonical 'Wilson Slough' so the
    slough's pieces merge as one channel instead of resolving into each other."""
    feats = clean_features([
        _ls([[0, 0], [1, 1]], OBJECTID=627, StreamName="Willson Slough", StreamDetail="Wilson Slough"),
        _ls([[1, 1], [2, 2]], OBJECTID=626, StreamName="Wilson Slough", StreamDetail="Wilson Slough"),
    ], "squamish")
    assert [f["properties"]["name"] for f in feats] == ["Wilson Slough", "Wilson Slough"]


def test_port_moody_drops_excluded_stoney_creek():
    """Port Moody's layer contains a 'Stoney Creek' that belongs to Burnaby — it must be dropped, while
    other Port Moody streams are kept. The exclusion is source-scoped (Burnaby keeps its Stoney Creek)."""
    feats = clean_features([
        _ls([[0, 0], [1, 0]], OBJECTID=290, local_stream_name="Stoney Creek"),
        _ls([[2, 0], [3, 0]], OBJECTID=7, local_stream_name="Schoolhouse Creek"),
    ], "port_moody")
    names = [f["properties"]["name"] for f in feats]
    assert "Stoney Creek" not in names and "Schoolhouse Creek" in names
    # source-scoped: Burnaby's Stoney Creek is NOT dropped
    burn = clean_features([_ls([[0, 0], [1, 0]], OBJECTID=1, WATERWAYNAME="Stoney Creek")], "burnaby")
    assert burn[0]["properties"]["name"] == "Stoney Creek"


def test_multilinestring_flattened_to_parts():
    feats = clean_features([{
        "type": "Feature", "properties": {"OBJECTID": 1, "WATERWAYNAME": "Beaver Creek"},
        "geometry": {"type": "MultiLineString", "coordinates": [[[0, 0], [1, 0]], [[1, 0], [2, 0]]]},
    }], "burnaby")
    assert len(feats) == 2 and all(f["geometry"]["type"] == "LineString" for f in feats)
    assert {f["properties"]["src_id"] for f in feats} == {"1.0", "1.1"}   # distinct part ids


def test_burnaby_trib_hierarchy_and_connect_hint():
    p = clean_features([_ls([[0, 0], [1, 0]], OBJECTID=1, WATERWAYNAME="Eagle Trib.1")], "burnaby")[0]["properties"]
    assert p["name"] == "Eagle Trib.1"
    assert p["trib_parent"] == "Eagle Creek" and p["connect_to_hint"] == "Eagle Creek"


def test_squamish_receiver_and_fish_normalisation():
    p = clean_features([_ls([[0, 0], [1, 0]], OBJECTID=2, StreamName="Raffuse Creek",
                            Contributing_to_DS="Mamquam River", Fish_Beari="Y", Spp_Pres="NA")], "squamish")[0]["properties"]
    assert p["connect_to_hint"] == "Mamquam River"
    assert p["fish"] == "yes"
    assert p["species"] == ""                                  # 'NA' dropped


def test_port_moody_topology_from_description():
    p = clean_features([_ls([[0, 0], [1, 0]], OBJECTID=3, local_stream_name="Schoolhouse South Tributary",
                            theme="Stream", stream_feature_code="KNOWN SALMON HABITAT",
                            description="TRIBUTARY TO SUTER BROOK")], "port_moody")[0]["properties"]
    assert p["connect_to_hint"] == "Suter Brook"
    assert p["ftype"] == "stream" and p["fish"] == "KNOWN SALMON HABITAT"


def test_junk_names_blanked_and_degenerate_dropped():
    feats = clean_features([
        _ls([[0, 0], [1, 0]], OBJECTID=1, local_stream_name="hide", theme="Stream"),   # junk name
        _ls([[0, 0]], OBJECTID=2, local_stream_name="X", theme="Stream"),              # <2 pts -> dropped
    ], "port_moody")
    assert len(feats) == 1 and feats[0]["properties"]["name"] == ""


# ------------------------------------------------------------------ T679 #13: the hand fixes are curated data

def test_the_hand_fixes_are_read_from_the_curated_calls_file():
    """(T679 #13) the drop-by-name / typo / drop-by-src_id / receiver fixes live in `waters/added_stream_calls.json`,
    read by `clean.calls()`: a listed src_id is dropped (one MLS part by its `.N` id), its neighbours kept; the
    Magnolia Creek receiver override is there for the generator."""
    from pipeline.atlas.waters.added_streams.clean import calls
    c = calls()
    assert c.receiver_override["squamish"] == {"594": "Magnolia Creek"}
    assert {"344", "279", "334.19", "41.1"} <= c.exclude_src_ids["burnaby"] and "528.2" not in c.exclude_src_ids["squamish"]
    feats = clean_features([_ls([[0, 0], [1, 0]], OBJECTID=344, WATERWAYNAME="Ancient Grove Trib.1"),
                            _ls([[1, 0], [2, 0]], OBJECTID=345, WATERWAYNAME="Ancient Grove Trib.1"),
                            {"type": "Feature", "properties": {"OBJECTID": 41, "WATERWAYNAME": "JR Trib.1"},
                             "geometry": {"type": "MultiLineString",
                                          "coordinates": [[[0, 0], [1, 0]], [[1, 0], [2, 0]], [[2, 0], [3, 0]]]}}],
                           "burnaby")
    assert sorted(str(f["properties"]["src_id"]) for f in feats) == ["345", "41.2"]


def test_the_calls_file_refuses_what_it_cannot_read(tmp_path):
    """(AGENTS 37) a missing file raises; so does another format, a source the generator does not read, a row with no
    `why`, and a src_id listed twice for one source — never an empty set of fixes."""
    import json

    import pytest

    from pipeline.atlas.waters.added_streams.clean import CALLS_FORMAT, calls
    with pytest.raises(FileNotFoundError):
        calls(tmp_path / "nope.json")
    p = tmp_path / "c.json"

    def refused(doc, words):
        p.write_text(json.dumps(doc))
        with pytest.raises(ValueError, match=words):
            calls(p)
    refused({"format": "other/1"}, "not a added_stream_calls/1 file")
    refused({"format": CALLS_FORMAT, "exclude_src_ids": [{"source": "vancouver", "src_ids": ["1"], "why": "x"}]},
            "names source 'vancouver'")
    refused({"format": CALLS_FORMAT, "name_aliases": [{"source": "squamish", "from": "a", "to": "b"}]}, "has no `why`")
    refused({"format": CALLS_FORMAT, "exclude_src_ids": [{"source": "burnaby", "src_ids": ["1"], "why": "x"},
                                                         {"source": "burnaby", "src_ids": ["1"], "why": "y"}]},
            "twice for burnaby")
