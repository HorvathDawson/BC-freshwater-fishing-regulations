"""clean_features — raw municipal props -> uniform cruft-free schema, MLS flattened; only source-scoped
name exclusions are filtered (bad/duplicated data), nothing else."""

from pipeline.added_streams.clean import clean_features

_UNIFORM = {"source", "src_id", "name", "connect_to_hint", "ftype", "fish", "species", "trib_parent"}


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
