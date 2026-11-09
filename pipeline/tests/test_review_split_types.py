"""T355: the review app's new-split editor makes EVERY split type, and a proposal's local id never lands.

  round trip   each anchor type — point (+offset), confluence (+offset), lake both ends (+offset), lake
               outlet, area boundary (any layer, end, WSC subtree, waters left uncut), border — goes
               mark line -> decisions (in memory) -> proposal -> splits.json record with every field kept,
               the record the build's own `SplitDef.from_dict` accepts
  offsets      on every type the build can offset; an area boundary / the border refuse one (the build
               cannot), in the mark model as in `SplitAnchor`
  edit         a proposal edited in place (`NewSplit.amends`) replaces that cut, keeping its id
  real ids     no "n…" / hunt / tidal ref reaches a curated file: the extent names the slug the new
               split lands under, and the guard in `materialize` refuses anything else (mutation-pinned)
"""
from __future__ import annotations

import json

import pytest

from pipeline.common.models.splits import SplitDef
from pipeline.regs.review import export as E
from pipeline.regs.review import marks as M
from pipeline.regs.review import proposals as P
from pipeline.tests.test_review_proposals import COWICHAN, _mark, src  # noqa: F401 — the fixture

WATER = "gnis:12227"                                       # the Cowichan
LAKE = {"id": "wbk:329000001", "name": "Test Lake", "kind": "lake", "ref_ids": ["wbk:329000001"]}
TRIB = {"id": "gnis:99001", "name": "Test Creek", "kind": "stream",
        "ref_ids": ["blk:354000001", "gnis:99001", "wsc:920-123456-000001"]}
AT = (-123.835989, 48.757157)

#: each anchor type the panel offers: (the NewSplit fields, the splits.json anchor it must land as)
CASES = {
    "point+offset": ({"anchor": "bridge", "name": "the bridge", "coord": AT, "offset_m": -100},
                     {"type": "point", "coord": list(AT), "is_lonlat": True, "offset_m": 100.0,
                      "offset_dir": "downstream"}),
    "confluence+offset": ({"anchor": "confluence", "name": "Test Creek", "item_id": TRIB["id"], "offset_m": 50},
                          {"type": "confluence", "tributary_wsc": "920-123456-000001", "offset_m": 50.0,
                           "offset_dir": "upstream"}),
    "lake both ends": ({"anchor": "lake", "name": "Test Lake", "item_id": LAKE["id"]},
                       {"type": "lake", "wbk": "329000001"}),
    "lake both+offset": ({"anchor": "lake", "name": "Test Lake", "item_id": LAKE["id"], "offset_m": 200},
                         {"type": "lake", "wbk": "329000001", "offset_m": 200.0, "offset_dir": "upstream"}),
    "lake outlet": ({"anchor": "lake_outlet", "name": "Test Lake", "item_id": LAKE["id"]},
                    {"type": "lake", "wbk": "329000001", "edge": "outlet"}),
    "area boundary": ({"anchor": "area_boundary", "name": "Pacific Rim boundary", "area": "PACIFIC RIM",
                       "area_layer": "parks_nat", "area_end": "upstream", "wsc_descendants": True,
                       "exclude_gnis": ["17209"]},
                      {"type": "area_boundary", "area_layer": "parks_nat", "area_name": "PACIFIC RIM",
                       "area_name_field": "ENGLISH_NAME", "side": "upstream", "wsc_descendants": True,
                       "exclude_gnis": ["17209"]}),
    "park default layer": ({"anchor": "area_boundary", "area": "X PARK", "area_end": "downstream"},
                           {"type": "area_boundary", "area_layer": "parks_bc", "area_name": "X PARK",
                            "area_name_field": "PROTECTED_LANDS_NAME", "side": "downstream"}),
    "border": ({"anchor": "border", "name": "U.S. border", "coord": AT},
               {"type": "border", "coord": list(AT), "is_lonlat": True}),
}


def _registry(src) -> None:
    reg = src.build / "registry.json"
    doc = json.loads(reg.read_text())
    doc["items"] += [LAKE, TRIB]
    reg.write_text(json.dumps(doc))


def _dfo_mark(ns: dict, ref: str = "n1") -> M.Mark:
    return _mark(scope="dfo", target=COWICHAN, extent_fix=[{
        "scope": "entry", "op": "downstream_of", "items": [WATER], "points": [{"kind": "new", "split_id": ref}]}],
        new_splits=[{"ref": ref, "on_item": WATER, **ns}])


@pytest.mark.parametrize("case", list(CASES))
def test_every_anchor_type_round_trips_mark_to_curated_split(src, tmp_path, case):  # noqa: F811
    _registry(src)
    fields, want = CASES[case]
    mark = _dfo_mark(fields)
    # the mark's one line, read back: every field kept, nothing unset written
    back = M.Mark.model_validate(json.loads(M.line(mark)))
    assert back == mark
    line_split = json.loads(M.line(mark))["new_splits"][0]
    assert {k for k, v in fields.items() if v is not None} <= set(line_split)
    assert not [k for k in M.NEW_SPLIT_OPTIONAL if k in line_split and line_split[k] is None]
    # ... and the decisions as apply reads them: the line in a store, loaded (`export.load`, in memory)
    store = tmp_path / "marks.jsonl"
    store.write_text(M.line(back) + "\n")
    decided = E.load(store)
    props = {p["kind"]: p for p in P.build(decided, src)}
    new, ext = props["new_split"], props["extent"]
    assert new["applicable"], new["problems"]
    rec = new["edits"][0]["split"]
    assert {k: v for k, v in rec["anchor"].items() if not k.startswith("_")} == want
    sid = new["split_id"]
    assert rec["id"] == sid and not P.is_local_ref(sid)
    # the build's own validation takes the record as the waterbody carries it
    sd = SplitDef.from_dict({**rec, "anchor": {k: v for k, v in rec["anchor"].items() if not k.startswith("_")},
                             **new["edits"][0]["waterbody"]["applies_to"]})
    assert sd.anchor.type.value == want["type"]
    # landed with its extent: the curated files hold the slug, never the mark's "n1"
    texts = P.materialize([new, ext], src)
    splits = json.loads(texts[src.splits_path])
    got = next(sp for wb in splits["waterbodies"] for sp in wb.get("splits") or [] if sp["id"] == sid)
    assert {k: v for k, v in got["anchor"].items() if not k.startswith("_")} == want
    dfo_text = next(t for p, t in texts.items() if p.parent == src.dfo_dir)
    loc = next(x for x in json.loads(dfo_text)["locations"] if x["location_id"] == COWICHAN)
    assert loc["binding"]["extents"][0]["splits"] == [sid]
    assert '"n1"' not in "".join(texts.values())


@pytest.mark.parametrize("anchor,extra", [("area_boundary", {"area": "X PARK"}), ("border", {})])
def test_an_area_boundary_or_the_border_refuses_an_offset_as_the_build_does(anchor, extra):
    with pytest.raises(ValueError, match="cannot offset"):
        M.NewSplit(ref="n1", anchor=anchor, on_item=WATER, offset_m=100, **extra)
    from pipeline.common.models.splits import SplitAnchor
    a = {"area_boundary": {"area_layer": "parks_bc", "area_name": "X PARK"}, "border": {}}[anchor]
    with pytest.raises(ValueError, match="offset_m only valid"):
        SplitAnchor.from_dict({"type": anchor, **a, "offset_m": 100, "offset_dir": "upstream"})
    assert anchor not in M.OFFSET_ANCHORS


def test_area_fields_belong_to_an_area_boundary_and_a_known_layer():
    with pytest.raises(ValueError, match="belong to an area_boundary"):
        M.NewSplit(ref="n1", anchor="coord", coord=AT, wsc_descendants=True)
    with pytest.raises(ValueError, match="area_layer is one of"):
        M.NewSplit(ref="n1", anchor="area_boundary", area="X", area_layer="wmu")


def test_an_edit_in_place_replaces_the_proposal_keeping_its_id(src):  # noqa: F811
    """T355: the owner edits a proposed cut (a map point -> the park's boundary) from another location:
    the edit rides in a mark (`amends`: the cut it edits, by anchor), the group keeps its key, the cut
    lands as edited and the first mark's extent names it."""
    first = _dfo_mark({"anchor": "coord", "name": "park edge", "coord": AT})
    other = _mark(scope="dfo", target="1:cowichan-river:other", answers={"T": "y", "R": "y", "P": "?"},
                  new_splits=[{"ref": "cowichan_river__x_park_boundary", "on_item": WATER, "anchor": "area_boundary",
                               "area": "X PARK", "area_end": "downstream", "name": "X Park boundary",
                               # the cut it edits, by its anchor — never by an id
                               "amends": {"ref": "cowichan_river__park_edge", "anchor": "coord",
                                          "on_item": WATER, "coord": list(AT)}}])
    (g,) = M.proposals([first, other])
    assert g["id"] == "n1" and g["split"].anchor == "area_boundary" and len(g["members"]) == 2
    props = P.build([first, other], src)
    new = next(p for p in props if p["kind"] == "new_split")
    assert new["edits"][0]["split"]["anchor"]["type"] == "area_boundary"
    ext = next(p for p in props if p["kind"] == "extent")
    assert ext["edits"][0]["after"][0]["splits"] == [new["split_id"]]
    # an earlier owner override's set no longer applies to an edited cut (the edit is the later call)
    o = M.ConfidenceOverride.model_validate({"match": {"ref": "x", "anchor": "coord", "on_item": WATER,
                                                       "coord": list(AT)},
                                             "set": {"coord": [-123.9, 48.8]}, "by": "t", "date": "d"})
    (g2,) = M.proposals([first, other], [o])
    assert g2["split"].anchor == "area_boundary" and "amended" not in g2


def test_no_local_ref_reaches_a_curated_file(src):  # noqa: F811
    """THE GUARD: an extent naming a ref no new split took (an `anchor: split` naming a proposal, not a
    splits.json split) is refused, never written."""
    bad = _mark(scope="dfo", target=COWICHAN, extent_fix=[{
        "scope": "entry", "op": "downstream_of", "items": [WATER], "points": [{"kind": "new", "split_id": "n7"}]}],
        new_splits=[{"ref": "n7", "anchor": "split", "split_id": "n3", "on_item": WATER}])
    (ext,) = [p for p in P.build([bad], src) if p["kind"] == "extent"]
    assert not ext["applicable"] and any("local id never lands" in x for x in ext["problems"])


@pytest.mark.parametrize("sid", ["n1", "n1-2", "stamp_lagoon_outlet", "tidal-354091024", "r1_entry_0_1"])
def test_the_guard_refuses_every_local_ref_shape(src, sid):  # noqa: F811
    have = {"waterbodies": [{"splits": [{"id": "cowichan_river__cnr_trestle"}]}]}
    ext = {"id": "p", "edits": [{"kind": "dfo", "field": "extents",
                                 "after": [{"op": "downstream_of", "splits": [sid]}]}]}
    with pytest.raises(ValueError, match="local id never lands"):
        P._only_real_split_ids([ext], have)
    new = {"id": "p", "edits": [{"kind": "split", "split": {"id": sid}}]}
    with pytest.raises(ValueError, match="local id"):
        P._only_real_split_ids([new], have)
    flag = {"id": "p", "edits": [{"kind": "split_flag", "split_id": sid}]}
    with pytest.raises(ValueError, match="local id never lands"):
        P._only_real_split_ids([flag], have)


def test_the_guard_lets_real_and_minted_ids_through():
    have = {"waterbodies": [{"splits": [{"id": "cowichan_river__cnr_trestle"}]}]}
    ok = {"id": "p", "edits": [{"kind": "dfo", "field": "extents", "after": [{"op": "between", "splits": [
        "cowichan_river__cnr_trestle", "gauge__08HA011", "border:356570348:1", "lake:329016195"]}]}]}
    P._only_real_split_ids([ok], have)


def test_the_guard_is_pinned_by_mutation(src, monkeypatch):  # noqa: F811
    """MUTATION: with the guard gone, a local ref is written to the DFO file — the guard is what stops it."""
    bad = _mark(scope="dfo", target=COWICHAN, extent_fix=[{
        "scope": "entry", "op": "downstream_of", "items": [WATER], "points": [{"kind": "new", "split_id": "n7"}]}],
        new_splits=[{"ref": "n7", "anchor": "split", "split_id": "n3", "on_item": WATER}])
    monkeypatch.setattr(P, "_only_real_split_ids", lambda *_a, **_k: None)
    (ext,) = [p for p in P.build([bad], src) if p["kind"] == "extent"]
    assert ext["applicable"] and '"n3"' in "".join(P.materialize([ext], src).values())


def test_a_tidal_flag_on_a_local_ref_writes_nothing(src):  # noqa: F811
    """The tidal path flags an EXISTING split only: a ref that is not one makes no edit."""
    (p,) = P._tidal_flag("tidal-354091024", {}, src, [], "Cowichan")
    assert p["edits"] == [] and p["problems"]


# --------------------------------------------------------------------------- real ids from the anchor

def _ns(**kw):
    return M.NewSplit(**{"ref": "x", "on_item": WATER, **kw})


def test_the_same_anchor_always_gets_the_same_id_whatever_the_order(src):  # noqa: F811
    a = _ns(anchor="bridge", name="the bridge", coord=AT)
    b = _ns(anchor="dam", name="the bridge", coord=(-123.80, 48.76))      # same name: a clash
    c = _ns(anchor="sign", name="a sign", coord=(-123.70, 48.77))
    one = P.place_groups([("a", a, False, None), ("b", b, False, None), ("c", c, False, None)], src)
    two = P.place_groups([("c", c, False, None), ("b", b, False, None), ("a", a, False, None)], src)
    assert {k: v["id"] for k, v in one.items()} == {k: v["id"] for k, v in two.items()}
    ids = sorted(v["id"] for k, v in one.items() if k in "ab")
    assert ids == ["cowichan_river__the_bridge", "cowichan_river__the_bridge_2"]        # a clash: a suffix
    assert one["c"]["id"] == "cowichan_river__a_sign"
    assert not any(P.is_local_ref(v["id"]) for v in one.values())


def test_a_clash_with_splits_json_takes_the_next_suffix(src):  # noqa: F811
    have = {"cowichan_river__the_bridge": {}}
    got = P.place_groups([("a", _ns(anchor="bridge", name="the bridge", coord=AT), False, None)], src,
                         {"waterbodies": [{"name": "COWICHAN RIVER", "applies_to": {"gnis_id": "12227"},
                                           "splits": [{"id": "cowichan_river__the_bridge", "anchor": {"type": "point"}}]}]},
                         have)
    assert got["a"]["id"] == "cowichan_river__the_bridge_2"


def test_assign_ids_numbers_clashes_by_anchor_not_order():
    rows = [("p", "w__x", "bbb"), ("q", "w__x", "aaa"), ("r", "w__y", "ccc")]
    assert P.assign_ids(rows, {"w__x"}) == P.assign_ids(rows[::-1], {"w__x"}) == {
        "q": "w__x_2", "p": "w__x_3", "r": "w__y"}


def test_an_offset_cut_pair_says_up_and_downstream_not_2(src):  # noqa: F811
    """T523 (owner 11-09, Quatse): "100 m up and downstream of the weir" made `…_hatchery_weir_100_m` and `…_2`."""
    up = _ns(anchor="coord", name="Quatse River Hatchery weir (100 m upstream)", coord=AT, offset_m=100.0)
    down = _ns(anchor="coord", name="Quatse River Hatchery weir (100 m downstream)", coord=AT, offset_m=-100.0)
    got = P.place_groups([("u", up, False, None), ("d", down, False, None)], src)
    assert {k: v["id"] for k, v in got.items()} == {
        "u": "cowichan_river__quatse_river_hatchery_weir_u100m",
        "d": "cowichan_river__quatse_river_hatchery_weir_d100m"}
    # the hand ids' convention, whole metres and fractions alike
    assert P.split_base(_ns(anchor="coord", name="Morrison Creek into Puntledge River", coord=AT,
                            offset_m=-1500.0), "PUNTLEDGE RIVER") == \
        "puntledge_river__morrison_creek_into_puntledge_river_d1500m"
    assert P.split_base(_ns(anchor="coord", name="x", coord=AT, offset_m=2.5), "W") == "w__x_u2.5m"


def test_a_long_name_is_cut_at_a_word_never_into_a_clash():
    """T523 (Stamp): `robertson_creek_hatchery_lagoon` was both the inlet and the outlet (cut at 32)."""
    from pipeline.common import split_ids as I
    rows = [("in", I.base("STAMP RIVER", "Robertson Creek Hatchery lagoon inlet"), "a"),
            ("out", I.base("STAMP RIVER", "Robertson Creek Hatchery lagoon outlet"), "b")]
    assert P.assign_ids(rows, set()) == {"in": "stamp_river__robertson_creek_hatchery_lagoon_inlet",
                                         "out": "stamp_river__robertson_creek_hatchery_lagoon_outlet"}
    long = "word " * 20                                      # 99 chars: back to the last whole word in 60
    assert I.base("W", long) == "w__" + "_".join(["word"] * 12)
    assert I.word_slug("a" * 70) == "a" * 60                 # one word longer than the limit is cut at it
    # the same name twice is still a clash, numbered by anchor
    same = [("p", I.base("W", "the bridge"), "b"), ("q", I.base("W", "the bridge"), "a")]
    assert P.assign_ids(same, set()) == {"q": "w__the_bridge", "p": "w__the_bridge_2"}


def test_tidal_mouth_ids_keep_their_shape():
    """T523: the atlas mints tidal ids through the same `base` — a mouth is `<water>__tidal_boundary` as before."""
    from pipeline.common import split_ids as I
    assert I.base("Cook Creek", "tidal boundary") == "cook_creek__tidal_boundary"
    assert I.base("Fraser River (lower)", "Mission bridge (tidal boundary)") == \
        "fraser_river__mission_bridge_tidal_boundary"


def test_a_mark_holds_a_real_id_as_its_ref():
    long = "little_qualicum_river__little_qualicum_falls_park_bound_2"
    m = _dfo_mark({"anchor": "area_boundary", "area": "LITTLE QUALICUM FALLS PARK"}, ref=long)
    assert M.Mark.model_validate(json.loads(M.line(m))).new_splits[0].ref == long
