"""T743: the final marks audit (`pipeline.regs.review.audit`) — every check on a small fixture, a positive and a
negative case each, then the report (deterministic, `--since`) and the command end to end on files in tmp."""
from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from pipeline.regs.review import audit as A


# ------------------------------------------------------------------------------------------------ fixtures

def water(wid="1:river", items=("gnis:1",), tributaries=None, name="River"):
    return {"water_id": wid, "region": wid.split(":")[0], "region_number": 1, "name": name, "item_ids": list(items),
            "tributaries": tributaries}


def place(lid, *, wid="1:river", words="", heading="River", extents=(), tributaries=None, only=False, status="active",
          dup=None, confirmed=None, kind="water", tidal=False, file="region-1.json"):
    p = {"location_id": lid, "region": lid.split(":")[0], "region_number": 1, "kind": kind, "precedence": 3,
         "water": heading, "water_id": wid, "aliases": [], "section": None, "section_history": [], "inherits": [],
         "fingerprints": [], "source_text": {"waters": heading, "specific_area": words}, "status": status,
         "duplicate_of": dup, "duplicate_score": None, "duplicate_confirmed": confirmed, "locked": False,
         "reviewed_by": "", "reviewed_at": "", "review_note": "",
         "binding": {"extents": [dict(e) for e in extents], "tributaries": tributaries, "tributaries_only": only,
                     "tributary_excludes": [], "sections_cached": None, "notes": [], "spatial_caveat": False},
         "_file": file}
    if tidal:
        p["tidal_water"] = True
    return p


def mark(target, verdict="verified", *, fix=None, group=None, variant_of=None, call=None, note="", new_splits=()):
    m = {"scope": "dfo", "target": target, "rule_id": None, "verdict": verdict, "note": note}
    if fix is not None:
        m["extent_fix"] = fix
    for k, v in (("group", group), ("variant_of", variant_of), ("call", call)):
        if v is not None:
            m[k] = v
    if new_splits:
        m["new_splits"] = list(new_splits)
    return m


def fixpiece(op, *splits, items=(), tributaries=None, new=(), **extra):
    pts = [{"kind": "new" if s in new else "split", "split_id": s} for s in splits]
    return {"scope": "entry", "op": op, "points": pts, "items": list(items), "tributaries": tributaries,
            "area": None, **extra}


def inputs(places=(), waters=(), marks=(), **kw):
    return A.Inputs(lines=[(i + 1, m) for i, m in enumerate(marks)], places={p["location_id"]: p for p in places},
                    waters={w["water_id"]: w for w in waters}, **kw)


def kinds(fs, section=None):
    return sorted((f.kind, f.place) for f in fs if section is None or f.section == section)


def test_the_wording_table_reads_the_owners_cases():
    assert A.trib_meaning("All tributaries to the Bella Coola River.") == "only"
    assert A.trib_meaning("all tributaries of the Bulkley River other than the Suskwa River") == "only"
    assert A.trib_meaning("including tributaries") == "included"
    assert A.trib_meaning("Tatshenshini mainstem and all other tributaries") == "included"
    assert A.trib_meaning("mainstem waters only") == "excluded"
    assert A.trib_meaning("including its tributary containing the fishway") is None      # one named tributary
    assert A.says("one_bank", "on west bank between signs") and not A.says("one_bank", "on the east bank of the park")
    assert A.says("distance", "from 500 meters below the power line") and not A.says("distance", "Hwy 97 bridge")


# ------------------------------------------------------------------------------------------------ A

def test_A_leftover_link_from_a_superseded_save():
    ps = [place("1:river:a", dup="1:river:b", confirmed=True), place("1:river:b", extents=[{"op": "whole"}])]
    got = A.check_A(inputs(ps, [water()], [mark("1:river:a", variant_of="1:river:b"),
                                           mark("1:river:a", "wrong", note="no")]))
    assert ("leftover_link", "1:river:a") in kinds(got)
    # the last save keeps the link: nothing left over
    got = A.check_A(inputs(ps, [water()], [mark("1:river:a", "wrong", note="x"),
                                           mark("1:river:a", variant_of="1:river:b")]))
    assert ("leftover_link", "1:river:a") not in kinds(got)


def test_A_file_not_the_last_save_and_a_landed_new_cut_matches():
    ex = [{"op": "upstream_of", "splits": ["c1"]}]
    two = [mark("1:river:a", fix=[fixpiece("whole")]), mark("1:river:a", fix=[fixpiece("upstream_of", "c2")])]
    assert ("not_last_save", "1:river:a") in kinds(A.check_A(inputs([place("1:river:a", extents=ex)], [water()], two)))
    landed = [mark("1:river:a", fix=[fixpiece("whole")]),
              mark("1:river:a", "unsure", fix=[fixpiece("upstream_of", "n1", new=("n1",))])]
    assert kinds(A.check_A(inputs([place("1:river:a", extents=ex)], [water()], landed))) == []
    waits = A.check_A(inputs([place("1:river:a", extents=[{"op": "whole"}])], [water()], landed))
    assert ("last_save_waits_cut", "1:river:a") in kinds(waits)


def test_A_orphan_cut_made_from_a_mark():
    cuts = {"c_old": {"id": "c_old", "_note": "[T185 proposal 2026-11-01] x"},
            "c_used": {"id": "c_used", "_note": "[T185 proposal 2026-11-01] y"},
            "c_hand": {"id": "c_hand", "_note": "authored"}}
    ps = [place("1:river:a", extents=[{"op": "upstream_of", "splits": ["c_used"]}])]
    ms = [mark("1:river:a", fix=[fixpiece("upstream_of", "c_old")]), mark("1:river:a", fix=[fixpiece("upstream_of", "c_used")])]
    got = [f for f in A.check_A(inputs(ps, [water()], ms, cuts=cuts)) if f.kind == "orphan_cut"]
    assert [f.what for f in got] == ["c_old"] and got[0].lines == (1,)


# ------------------------------------------------------------------------------------------------ B

def test_B_wrong_with_the_old_extent_in_force():
    ex = [{"op": "upstream_of", "splits": ["c1"]}]
    ms = [mark("1:river:a", fix=[fixpiece("upstream_of", "c1")]), mark("1:river:a", "wrong", note="bad spot")]
    got = A.check_B(inputs([place("1:river:a", extents=ex)], [water()], ms))
    assert kinds(got) == [("wrong", "1:river:a")] and "set by line 1" in got[0].found and "ships: yes" in got[0].found
    # the wrong save's own fix is what the file holds: nothing to say
    ms = [mark("1:river:a", "wrong", note="x", fix=[fixpiece("upstream_of", "c1")])]
    assert A.check_B(inputs([place("1:river:a", extents=ex)], [water()], ms)) == []


# ------------------------------------------------------------------------------------------------ C

def test_C1_included_and_C5_blank():
    ex = [{"op": "whole"}]
    got = A.check_C(inputs([place("1:river:a", words="including tributaries", extents=ex)], [water()]))
    assert ("C1_included", "1:river:a") in kinds(got) and ("C5_blank", "1:river:a") in kinds(got)
    got = A.check_C(inputs([place("1:river:a", words="including tributaries", extents=ex, tributaries=True)], [water()]))
    assert got == []


def test_C2_heading_includes_or_is_the_tributaries():
    w = water(name="Alouette River and tributaries")
    got = A.check_C(inputs([place("1:river:a", heading=w["name"], extents=[{"op": "whole"}], tributaries=False)], [w]))
    assert kinds(got) == [("C2_heading", "1:river:a")]
    assert A.check_C(inputs([place("1:river:a", heading=w["name"], extents=[{"op": "whole"}])],
                            [water(name=w["name"], tributaries=True)])) == []
    got = A.check_C(inputs([place("1:river:a", heading="Somass River tributaries", extents=[{"op": "whole"}],
                                  tributaries=True)], [water()]))
    assert kinds(got) == [("C2_heading_only", "1:river:a")]
    assert A.check_C(inputs([place("1:river:a", heading="Somass River tributaries", extents=[{"op": "whole"}],
                                   tributaries=True, only=True)], [water()])) == []


def test_C3_mainstem_with_tributaries():
    p = place("1:river:a", words="mainstem waters only", extents=[{"op": "whole"}])
    assert ("C3_mainstem", "1:river:a") in kinds(A.check_C(inputs([p], [water(tributaries=True)])))
    assert A.check_C(inputs([p], [water(tributaries=False)])) == []


def test_C4_tributaries_only_with_and_without_the_mark_field():
    p = place("1:river:a", words="All tributaries to the River.", extents=[{"op": "whole"}], tributaries=True)
    got = A.check_C(inputs([p], [water()], [mark("1:river:a", fix=[fixpiece("whole", tributaries=True)])]))
    assert kinds(got) == [("C4_only", "1:river:a")] and "not the tributaries-only form" in got[0].found
    got = A.check_C(inputs([p], [water()], [mark("1:river:a", fix=[fixpiece("whole", tributaries=True,
                                                                             tributaries_only=True)])]))
    assert "not landed" in got[0].found
    done = place("1:river:a", words="All tributaries to the River.", extents=[{"op": "whole"}], tributaries=True,
                 only=True)
    assert A.check_C(inputs([done], [water()])) == []


# ------------------------------------------------------------------------------------------------ D

def test_D_rest_with_a_dormant_and_an_undrawable_sibling():
    ps = [place("1:river:rest", extents=[{"op": "rest"}]),
          place("1:river:old", status="dormant", extents=[{"op": "upstream_of", "splits": ["c1"]}]),
          place("1:river:new", extents=[{"op": "downstream_of", "splits": ["gone"]}])]
    got = A.check_D(inputs(ps, [water()], cuts={"c1": {"id": "c1"}}))
    # T761: a rest's dormant listed place takes nothing — said on its line, never a finding
    assert ("dormant_sibling", "1:river:rest") not in kinds(got) and ("undrawable_sibling", "1:river:rest") in kinds(got)
    assert "dormant (takes nothing): 1:river:old" in next(f for f in got if f.kind == "listed_rest").found
    assert next(f for f in got if f.kind == "undrawable_sibling").what == "1:river:new"
    assert "skips it and names it" in next(f for f in got if f.kind == "undrawable_sibling").found
    ok = [ps[0], place("1:river:new", extents=[{"op": "downstream_of", "splits": ["c1"]}])]
    assert kinds(A.check_D(inputs(ok, [water()], cuts={"c1": {"id": "c1"}}))) == [("listed_rest", "1:river:rest")]


def test_D_two_rests_on_one_water():
    ps = [place("1:river:r1", extents=[{"op": "rest"}]), place("1:river:r2", extents=[{"op": "rest"}])]
    assert sum(f.kind == "two_rests" for f in A.check_D(inputs(ps, [water()]))) == 2
    assert not any(f.kind == "two_rests" for f in A.check_D(inputs(ps[:1], [water()])))


# ------------------------------------------------------------------------------------------------ E

def test_E_a_cut_near_the_tidal_boundary_that_is_not_it():
    tidal = {"9": {"id": "r__tidal_boundary", "measure": 0.0, "limit": "mouth", "tier": "the mouth", "name": "R"}}
    pos = {"r__bridge": ("9", 56.6, "bridge near the mouth"), "r__tidal_boundary": ("9", 0.0, "tidal boundary"),
           "r__far": ("9", 5000.0, "far bridge")}
    near = place("1:river:a", words="upstream of the bridge", extents=[{"op": "upstream_of", "splits": ["r__bridge"]}])
    got = A.check_E(inputs([near], [water()], split_pos=pos, tidal=tidal))
    assert kinds(got) == [("not_tidal_boundary", "1:river:a")] and "56.6 m away" in got[0].found
    ok = [place("1:river:a", extents=[{"op": "between", "splits": ["r__tidal_boundary", "r__far"]}])]
    assert A.check_E(inputs(ok, [water()], split_pos=pos, tidal=tidal)) == []


def test_E_a_mark_cut_near_it_and_skipped_without_an_atlas():
    tidal = {"9": {"id": "r__tidal_boundary", "measure": 100.0, "limit": "split", "tier": "rank-1 split", "name": "R"}}
    cuts = {"r__new": {"id": "r__new", "_note": "[T185 proposal 2026-11-15] x"}}
    got = A.check_E(inputs([], [], split_pos={"r__new": ("9", 300.0, "x")}, tidal=tidal, cuts=cuts))
    assert [f.kind for f in got] == ["mark_cut_near_tidal"]
    assert A.check_E(inputs([], [], split_pos={"r__new": ("9", 900.0, "x")}, tidal=tidal, cuts=cuts)) == []
    assert A.check_E(inputs([], [], cuts=cuts)) == []


# ------------------------------------------------------------------------------------------------ F

def _graph():
    N = lambda blk, d, u: SimpleNamespace(blk=blk, down_m=d, up_m=u, length_m=u - d)
    nodes = {"100:0": N("100", 0, 1000), "100:1000": N("100", 1000, 2000), "200:0": N("200", 0, 700)}
    edges = [SimpleNamespace(from_node="200:0", to_node="100:0", at_measure=500.0)]
    return SimpleNamespace(nodes=nodes, edges=edges, down_adj={"200:0": [0]}, up_adj={"100:0": [0]})


def _reg():
    return {"gnis:1": {"id": "gnis:1", "kind": "stream", "section_ids": ["100:0", "100:1000"], "ref_ids": ["gnis:1"]},
            "blk:200": {"id": "blk:200", "kind": "stream", "name": "Island Side Channel", "section_ids": ["200:0"],
                        "ref_ids": ["blk:200", "gnis:1", "wsc:100"]}}


def test_F_a_side_channel_beside_the_place_is_dropped():
    pos = {"a": ("100", 0.0, "a"), "b": ("100", 800.0, "b"), "c": ("100", 1200.0, "c"), "d": ("100", 1800.0, "d")}
    p = place("1:river:a", extents=[{"op": "between", "splits": ["a", "b"], "item_id": "gnis:1"}])
    got = A.check_F(inputs([p], [water(items=("gnis:1", "blk:200"))], split_pos=pos, registry=_reg(), graph=_graph()))
    assert kinds(got) == [("dropped_channel", "1:river:a")] and "item-restricted" in got[0].found
    far = place("1:river:a", extents=[{"op": "between", "splits": ["c", "d"], "item_id": "gnis:1"}])
    assert A.check_F(inputs([far], [water()], split_pos=pos, registry=_reg(), graph=_graph())) == []
    held = place("1:river:a", extents=[{"op": "between", "splits": ["a", "b"]}])
    assert A.check_F(inputs([held], [water(items=("gnis:1", "blk:200"))], split_pos=pos, registry=_reg(),
                            graph=_graph())) == []


# ------------------------------------------------------------------------------------------------ G H

def test_G_a_group_mark_not_resaved():
    ps = [place("1:river:a", words="from 500 m below the line"), place("1:river:b", words="from the line")]
    ms = [mark("1:river:a", group="grp:1:river:a"), mark("1:river:b", group="grp:1:river:a")]
    got = A.check_G(inputs(ps, [water()], ms))
    assert kinds(got) == [("group_mark", "1:river:a"), ("group_mark", "1:river:b")]
    assert "names '500 m'" in next(f for f in got if f.place == "1:river:a").found
    assert A.check_G(inputs(ps, [water()], ms + [mark("1:river:a"), mark("1:river:b")])) == []


def test_H_one_bank_saved_whole_width():
    p = place("6:river:a", words="on west bank between signs", extents=[{"op": "between", "splits": ["x", "y"]}])
    assert kinds(A.check_H(inputs([p], [water()]))) == [("whole_width", "6:river:a")]
    q = place("6:river:a", words="the channel on the east bank of the park", extents=[{"op": "whole"}])
    assert A.check_H(inputs([q], [water()])) == []


# ------------------------------------------------------------------------------------------------ I J K

def test_I_not_landed_grouped_by_what_it_waits_on():
    landed = {"not_landed": [{"domain": "dfo", "status": "waiting", "id": "p1", "marks": ["dfo|1:river:a|"],
                              "why": "a cut changes: make the side rebuild and read its binding diff first"},
                             {"domain": "dfo", "status": "refused", "id": "p2", "marks": ["dfo|1:river:b|"],
                              "why": "not applicable: op other_places has no water of its own"}],
              "allowed": [{"domain": "tidal", "status": "waiting", "marks": ["tidal|tidal:1|"], "why": "T359"}]}
    got = A.check_I(inputs([place("1:river:a"), place("1:river:b")], [water()], landed=landed))
    assert sorted(f.kind for f in got) == ["a side rebuild (the cut changes)", "allowed to wait (NOT_LANDED_OK)",
                                           "refused: not applicable: op other_places has no water of its own"]
    assert A.check_I(inputs(landed={"not_landed": [], "allowed": []})) == []
    assert A.check_I(inputs()) == []                                    # not run: the section says so (skipped)


def test_J_unmarked_and_riding():
    ps = [place("1:river:a"), place("1:river:b", dup="1:river:c", confirmed=True), place("1:river:c"),
          place("1:river:d", status="dormant")]
    got = A.check_J(inputs(ps, [water()], [mark("1:river:c")]))
    assert kinds(got) == [("rides_marked", "1:river:b"), ("unmarked", "1:river:a")]


def test_K_offsets_vs_wording():
    cuts = {"g": {"id": "g", "label": "Gosnell confluence", "anchor": {"type": "confluence", "tributary_wsc": "1"}},
            "g_d100m": {"id": "g_d100m", "label": "signs", "anchor": {"type": "confluence", "tributary_wsc": "1",
                                                                         "offset_m": 100, "offset_dir": "downstream"}},
            "pl": {"id": "pl", "label": "power line", "anchor": {"type": "point", "coord": [0, 0]}}}
    k2 = place("6:river:a", words="from Gosnell Creek to Lamprey Creek.", extents=[{"op": "between", "splits": ["g_d100m"]}])
    k1 = place("6:river:b", words="from 500 meters below the power line", extents=[{"op": "upstream_of", "splits": ["pl"]}])
    assert kinds(A.check_K(inputs([k1, k2], [water()], cuts=cuts))) == [("K1_distance_no_offset", "6:river:b"),
                                                                        ("K2_offset_not_worded", "6:river:a")]
    fine = [place("6:river:a", words="from signs approximately 100 m downstream of Gosnell Creek",
                  extents=[{"op": "between", "splits": ["g_d100m"]}]),
            place("6:river:b", words="from the power line", extents=[{"op": "upstream_of", "splits": ["pl"]}])]
    assert A.check_K(inputs(fine, [water()], cuts=cuts)) == []


# ------------------------------------------------------------------------------------------------ report

def test_the_report_is_deterministic_and_since_shows_new_and_resolved():
    ps = [place("1:river:a"), place("1:river:b")]
    inp = inputs(ps, [water()], [mark("1:river:b")], skipped={"I": "not run"})
    md1, doc1 = A.document(inp, A.run_checks(inp))
    md2, _ = A.document(inp, A.run_checks(inp))
    assert md1 == md2 and "| `1:river:a` |" in md1 and "**Not run:** not run" in md1
    later = inputs(ps, [water()], [mark("1:river:a")])
    md, doc = A.document(later, A.run_checks(later), prev=doc1)
    assert doc["since"] == {"new": ["J|unmarked|1:river:b|"], "resolved": ["J|unmarked|1:river:a|"]}
    assert "### resolved (1)" in md


def test_the_command_end_to_end(tmp_path):
    d = tmp_path / "places"
    d.mkdir()
    ps = [place("1:river:a", words="All tributaries to the River.", extents=[{"op": "whole"}])]
    (d / "region-1.json").write_text(json.dumps({"waters": [water()], "locations": [
        {k: v for k, v in p.items() if k != "_file"} for p in ps]}))
    (tmp_path / "marks.jsonl").write_text(json.dumps(mark("1:river:a")) + "\n")
    (tmp_path / "cuts.json").write_text(json.dumps({"waterbodies": []}))
    (tmp_path / "tidal.json").write_text(json.dumps({"format": "tidal/1", "waters": []}))
    (tmp_path / "syn").mkdir()
    args = ["--marks", str(tmp_path / "marks.jsonl"), "--places", str(d), "--cuts", str(tmp_path / "cuts.json"),
            "--tidal-limits", str(tmp_path / "tidal.json"), "--synopsis", str(tmp_path / "syn"),
            "--build", str(tmp_path / "no-atlas"), "--no-landed"]
    assert A.main([*args, "--out", str(tmp_path / "r.md")]) == 0
    doc = json.loads((tmp_path / "r.json").read_text())
    assert [f["kind"] for f in doc["findings"]] == ["C4_only", "C5_blank"] and set(doc["skipped"]) == {"E", "F", "I"}
    assert A.main([*args, "--out", str(tmp_path / "r2.md"), "--since", str(tmp_path / "r.json")]) == 0
    assert json.loads((tmp_path / "r2.json").read_text())["since"] == {"new": [], "resolved": []}
    (tmp_path / "marks.jsonl").write_text("not json\n")
    assert A.main([*args, "--out", str(tmp_path / "r3.md")]) == 2                     # cannot run: 2, nothing written
    assert not (tmp_path / "r3.md").exists()


def test_F_a_tidal_channel_is_left_out_and_an_unplaced_window_says_so():
    pos = {"a": ("100", 0.0, "a"), "b": ("100", 800.0, "b")}
    p = place("1:river:a", extents=[{"op": "between", "splits": ["a", "b"], "item_id": "gnis:1"}])
    tidal = {"200": {"id": None, "measure": None, "limit": "whole", "item": "blk:200", "tier": "whole", "name": ""}}
    assert A.check_F(inputs([p], [water()], split_pos=pos, registry=_reg(), graph=_graph(), tidal=tidal)) == []
    unbuilt = place("1:river:a", extents=[{"op": "between", "splits": ["a", "zz"], "item_id": "gnis:1"}])
    got = A.check_F(inputs([unbuilt], [water()], split_pos=pos, registry=_reg(), graph=_graph()))
    assert [f.kind for f in got] == ["dropped_channel_window_unknown"]


def test_I_paths_are_cut_to_file_names():
    row = {"status": "refused", "domain": "tidal", "marks": [], "why": "/x/y/curated/waters/tidal_limits.json: would drop 11"}
    assert A.wait_category(row) == "refused: tidal_limits.json: would drop 11"
