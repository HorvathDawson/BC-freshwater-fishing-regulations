"""merge_channels — same-name connected ways become one channel (one blk), forks stay separate."""

import pytest

from pipeline.added_streams.merge import merge_channels


def _feat(coords, **props):
    return {"type": "Feature", "properties": props,
            "geometry": {"type": "LineString", "coordinates": coords}}


def test_same_name_touching_ways_merge_to_one_channel():
    feats = [
        _feat([[0, 0], [1, 0]], source="osm", osm_way_id=111, name="Foo Creek"),
        _feat([[1, 0], [2, 0]], source="osm", osm_way_id=112, name="Foo Creek"),
    ]
    chans = merge_channels(feats)
    assert len(chans) == 1
    ch = chans[0]
    assert ch.blk == -111                       # -min(way_id) across the merged ways
    assert ch.name == "Foo Creek"
    assert list(ch.geometry.coords) == [(0.0, 0.0), (1.0, 0.0), (2.0, 0.0)]


def test_different_names_touching_stay_separate():
    feats = [
        _feat([[0, 0], [1, 0]], osm_way_id=111, name="Foo Creek"),
        _feat([[1, 0], [1, 1]], osm_way_id=222, name="Bar Creek"),   # a fork: touches but named differently
    ]
    chans = merge_channels(feats)
    assert len(chans) == 2
    assert {c.name for c in chans} == {"Foo Creek", "Bar Creek"}


def test_disconnected_same_name_stay_separate():
    feats = [
        _feat([[0, 0], [1, 0]], osm_way_id=111, name="Foo Creek"),
        _feat([[9, 9], [10, 9]], osm_way_id=112, name="Foo Creek"),  # same name, not touching
    ]
    assert len(merge_channels(feats)) == 2


def test_explicit_blk_groups_and_wins():
    feats = [
        _feat([[0, 0], [1, 0]], source="manual", blk=-500, name="Hand Creek"),
        _feat([[1, 0], [2, 0]], source="manual", blk=-500, name="Hand Creek"),
    ]
    chans = merge_channels(feats)
    assert len(chans) == 1 and chans[0].blk == -500


def test_conflicting_blk_in_one_group_raises():
    feats = [
        _feat([[0, 0], [1, 0]], blk=-1, name="X"),
        _feat([[1, 0], [2, 0]], blk=-2, name="X"),   # touching + same name -> one group, but blk conflict
    ]
    with pytest.raises(ValueError):
        merge_channels(feats)


def test_manual_without_blk_or_wayid_raises():
    with pytest.raises(ValueError):
        merge_channels([_feat([[0, 0], [1, 0]], source="manual", name="No Id Creek")])


def test_same_name_sections_with_small_gap_merge_when_enabled():
    # two 'Gap Creek' sections with an ~11 m gap (0.0001 deg lat) between them
    feats = [
        _feat([[-123.0, 49.200], [-123.0, 49.201]], osm_way_id=1, name="Gap Creek"),
        _feat([[-123.0, 49.2011], [-123.0, 49.202]], osm_way_id=2, name="Gap Creek"),
    ]
    assert len(merge_channels(feats)) == 2                       # exact-only: stay separate
    merged = merge_channels(feats, gap_tol_m=15.0)
    assert len(merged) == 1                                      # gap-merge: one creek
    assert len(merged[0].geometry.coords) == 4                  # both sections chained (nothing dropped)


def test_gap_merge_does_not_join_different_names():
    feats = [
        _feat([[-123.0, 49.200], [-123.0, 49.201]], osm_way_id=1, name="A Creek"),
        _feat([[-123.0, 49.2011], [-123.0, 49.202]], osm_way_id=2, name="B Creek"),
    ]
    assert len(merge_channels(feats, gap_tol_m=15.0)) == 2      # different names never proximity-merge


def _max_internal_jump_m(ch):
    """Largest gap (metres) between consecutive vertices of a channel geometry — a large value means a
    fabricated straight edge was stitched across a real gap (the 'uphill' artifact)."""
    from pipeline.added_streams.merge import _TO_ALBERS
    pts = [_TO_ALBERS.transform(x, y) for x, y in ch.geometry.coords]
    return max((((pts[i][0] - pts[i + 1][0]) ** 2 + (pts[i][1] - pts[i + 1][1]) ** 2) ** 0.5
                for i in range(len(pts) - 1)), default=0.0)


def test_junction_of_three_arms_is_not_chained_into_a_straight_jump():
    # Three same-name arms converging at ONE node (a Y-junction) with ~2 m gaps — the real Little
    # Stawamus topology. _bridge must NOT fuse a junction and let _greedy_chain fork it into one line,
    # which bridges the fork with a long straight segment (the 4.2 km uphill edge in Squamish).
    def arm(lon0, lat0, dlon, dlat, n=5, step=10.0):
        m_lat, m_lon = 1.0 / 111195.0, 1.0 / 72700.0        # ~metres->deg near lat 49.2
        return [[lon0 + dlon * step * m_lon * i, lat0 + dlat * step * m_lat * i] for i in range(n)]
    feats = [
        _feat(arm(-123.0, 49.2, 0, -1), source="manual", name="Fork Creek"),          # south arm (from J)
        _feat(arm(-123.0, 49.200018, 0, +1), source="manual", name="Fork Creek"),     # north arm (~2 m gap)
        _feat(arm(-122.999972, 49.2, +1, 0), source="manual", name="Fork Creek"),     # east arm (~2 m gap)
    ]
    chans = merge_channels(feats, mint_missing_blk=True, gap_tol_m=15.0)
    for ch in chans:
        assert _max_internal_jump_m(ch) <= 15.0, \
            f"channel stitched a {_max_internal_jump_m(ch):.0f} m straight jump across a junction"
    # nothing dropped: every input vertex still lives in some channel
    assert sum(len(c.geometry.coords) for c in chans) >= 15


def test_overrides_and_connect_to_carried():
    ch = merge_channels([_feat([[0, 0], [1, 0]], blk=-7, name="C",
                               connect_to={"gnis_id": "10070"},
                               stream_magnitude=4, edge_type="1050")])[0]
    assert ch.connect_to == {"gnis_id": "10070"}
    assert ch.overrides["stream_magnitude"] == 4
    assert ch.overrides["edge_type"] == "1050"
