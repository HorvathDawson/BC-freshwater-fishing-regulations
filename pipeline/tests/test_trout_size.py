"""Trout table plan, ticket A0: the size estimate and the contract's three counts (`pipeline/tools/audit/trout_size.py`).

Anchor (b): the committed mini release (`under_test.mini()`), copied and ALTERED THROUGH THE REAL STORE TABLES (the
helper of `test_answers_diff.py`: the regs store reassembled, edited, written back into every release file, digest and
manifest renewed). Each count is re-run with one planted case and must move by exactly that case, naming its water.
"""
from __future__ import annotations

import json

import pytest

from pipeline.deliver.store import decode as D
from pipeline.deliver.store.common import unpack
from pipeline.tests import under_test
from pipeline.tests.test_answers_diff import Store, _copy
from pipeline.tools.audit import trout_size as TS

TC = TS.TC


def _counts(rel):
    a = TS.Audit(rel, log=lambda *x, **k: None)
    return a, {"mixed": a.mixed_rows(), "angler": a.angler_only(), "hours": a.hours_window()}


@pytest.fixture(scope="module")
def base():
    return _counts(under_test.mini())


def _used_rows(a: TS.Audit) -> dict:
    """{rows_row index: an akey whose cell's frame holds it}."""
    out = {}
    for k, s, ref in a.cells():
        for i in a.rows["frames"][ref][2]:
            out.setdefault(i, k + 1)
    return out


# ------------------------------------------------------------------------------------------- (1) mixed rows, Q-D4

def test_a_keep_row_holding_kokanee_beside_trout_is_counted_mixed(base, tmp_path):
    a0, c0 = base
    used = _used_rows(a0)
    i = next(i for i, r in enumerate(a0.rows["rows"])
             if i in used and r["kind"] == "keep" and TS._fish(r) and TS._fish(r) <= TC)
    rel = _copy(tmp_path, "mixed")
    S = Store(rel)
    v = S.get("rows_row", i)
    v["members"] = [*v["members"], "KO"]
    S.put("rows_row", i, v)
    S.save()
    a, c = _counts(rel)
    m0, m = c0["mixed"]["members"], c["mixed"]["members"]
    assert m["rows"] == m0["rows"] + 1
    assert m["by_kind"].get("keep", 0) == m0["by_kind"].get("keep", 0) + 1
    planted = [p for p in m["patterns"] if p["kind"] == "keep"]
    assert len(planted) == 1 and planted[0]["other"] == ["KO"] and a.name(used[i]) in planted[0]["waters"]
    # the other counts did not move
    assert c["angler"]["close_trout_alone"] == c0["angler"]["close_trout_alone"]
    assert c["hours"]["answer_keys"] == c0["hours"]["answer_keys"]


# ------------------------------------------------------------------------------------- (2) angler-only, Q-D7

def test_an_angler_closure_of_trout_alone_is_counted_by_rule_and_by_answer(base, tmp_path, monkeypatch):
    a0, c0 = base
    # a trout fish's decided answer that is closed: its deciding rule becomes an angler closure of trout and char
    rix = next(d["win"] for f in sorted(TC) for ref in range(len(a0.rows["frames"]))
               for d in (a0.frame(ref)["fish"].get(f, {}).get("hatchery"),) if d and d["status"] == "closed")
    rid = a0.R.rule_ids[rix]
    rel = _copy(tmp_path, "angler")
    S = Store(rel)
    (b,) = S.con.execute("SELECT j FROM rule WHERE rix = ?", (rix,)).fetchone()
    r = json.loads(unpack(b, S.zd.get("rule")))
    r["type"] = "angler_closure"
    r["fields"] = {**r["fields"], "species": ["TROUT_CHAR"], "closed_to": {"age": ["16_plus"]}}
    S.con.execute("UPDATE rule SET j = ? WHERE rix = ?", (json.dumps(r).encode(), rix))
    S.save()
    # the export digest names the corpus it was cut from; the planted rule is not in it (the test reads it unchecked)
    real = D.Store.export_subset
    monkeypatch.setattr(D.Store, "export_subset", lambda self, check=True: real(self, check=False))
    a, c = _counts(rel)
    g, g0 = c["angler"], c0["angler"]
    assert (g["angler_rules"], g["name_a_fish"], g["close_trout_alone"]) == \
        (g0["angler_rules"] + 1, g0["name_a_fish"] + 1, g0["close_trout_alone"] + 1)
    assert g["rules_closing_trout_alone"] == [rid]
    assert rid in g["answers_closed_by_one"] and g["answers_closed_by_one"][rid]["answer_keys"] >= 1
    # a closure naming a fish that is not trout or char is scoped, never "trout alone"
    r["fields"]["species"] = ["RB", "KO"]
    S2 = Store(rel)
    S2.con.execute("UPDATE rule SET j = ? WHERE rix = ?", (json.dumps(r).encode(), rix))
    S2.save()
    g = _counts(rel)[1]["angler"]
    assert (g["name_a_fish"], g["close_trout_alone"]) == (g0["name_a_fish"] + 1, g0["close_trout_alone"])


# ------------------------------------------------------------------------------ (3) a trout answer inside an hours window

def _hours_pair(a: TS.Audit):
    """(akey, outside segment, inside segment) of a key whose water is open outside an hours window and closed inside
    (the mini world's night closure)."""
    w = a.wire
    for k, key in enumerate(w["keys"]):
        if key[10] is None:
            continue
        moms = [w["moments"][i] for i in w["segment_moments"][key[10]]]
        starts = w["segments"][key[9]]
        for o, mo in enumerate(moms):
            if not (mo["hours"] and not mo["hours"]["in"]):
                continue
            i = next((j for j, mj in enumerate(moms) if starts[j] == starts[o] and mj["hours"] and mj["hours"]["in"]),
                     None)
            if i is not None and a.status(k, o) not in ("closed", "tidal") and a.status(k, i) == "closed":
                return k + 1, o, i
    raise AssertionError("the mini release has no water closed inside an hours window")


def test_a_trout_answer_that_changes_inside_an_hours_window_is_counted(base, tmp_path):
    a0, c0 = base
    akey, o, i = _hours_pair(a0)
    k = akey - 1
    out_law = TS.trout_law(a0.frame(a0.rows["at"][k][o]))
    other = next(ref for ref in range(len(a0.rows["frames"]))
                 if TS.trout_law(a0.frame(ref)) and set(TS.trout_law(a0.frame(ref))) == set(out_law)
                 and TS.trout_law(a0.frame(ref)) != out_law
                 and all(v[0] is not None and v[0][0] != "closed" for v in TS.trout_law(a0.frame(ref)).values()))
    rel = _copy(tmp_path, "hours")
    S = Store(rel)
    S.cell(akey, i, "display", S.cell(akey, o, "display"))   # the water stays open inside the window
    S.cell(akey, i, "rows", other)                           # and its trout answer differs there
    S.save()
    a, c = _counts(rel)
    h, h0 = c["hours"], c0["hours"]
    assert h["answer_keys"] == h0["answer_keys"] + 1
    assert h["water_closed_inside"]["answer_keys"] == h0["water_closed_inside"]["answer_keys"] - 1
    ex = [x for x in h["examples"] if x["answer_key"] == akey]
    assert ex and ex[0]["water"] == a.name(akey) and ex[0]["weekdays"] == 7
    assert all(" outside, " in line and " inside " in line for line in ex[0]["fish"])
    in_law = TS.trout_law(a0.frame(other))
    assert set(h["fish"]) == {f for f in out_law if out_law[f] != in_law.get(f)}


# ------------------------------------------------------------------------------------------------------- the estimate

def test_the_estimate_prices_the_side_and_cutover_forms_of_a_region_file(base):
    a, _ = base
    rep = a.size(["1"])["regs-1"]
    assert rep["baseline_gz"] == rep["manifest_gz"]                     # gzipped as the release gzips
    assert rep["rows_row_cut"]["repack_differs"] == 0                  # the re-pack reproduces every stored row
    assert rep["rows_row_cut"]["rows"] > 0 and rep["rows_row_cut"]["packed_bytes_freed"] > 0
    nulls = sum(1 for k, s, ref in a.cells() if a.status(k, s) in ("closed", "tidal")
                or not TS.trout_law(a.frame(ref)))
    regs1 = {(akey, s) for akey, s in _region_cells(a, "regs-1")}
    assert rep["cells"] == len(regs1) and 0 < rep["null_cells"] <= nulls
    for variant in TS.BODIES:
        v = rep[variant]
        assert v["side_gz"] > rep["baseline_gz"] and v["section_packed"] > 0
        assert v["cutover_gz"] < v["side_gz"]
        assert v["families"]["trout_frame"]["distinct"] >= v["families"]["trout_table"]["distinct"]
    assert rep["lean"]["section_packed"] <= rep["full"]["section_packed"]


def _region_cells(a: TS.Audit, name: str):
    import sqlite3
    import tempfile
    from pathlib import Path
    from pipeline.tools.audit import answers_diff as AD
    raw = AD._file_bytes(a.dir, a.R.manifest, name)
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "r.sqlite"
        p.write_bytes(raw)
        con = sqlite3.connect(str(p))
        out = con.execute("SELECT akey, s FROM cell").fetchall()
        con.close()
    return out


def test_the_cli_writes_its_report(tmp_path, capsys):
    assert TS.main([str(under_test.mini()), "--regions", "1", "--out", str(tmp_path)]) == 0
    rep = json.loads((tmp_path / "report.json").read_text())
    assert {"mixed_rows", "angler_only", "hours_window", "size"} <= set(rep)
    assert "TROUT TABLE A0" in (tmp_path / "report.txt").read_text()
    assert TS.main([str(tmp_path / "nowhere"), "--no-size", "--out", str(tmp_path)]) == 2


# ================================================================================ ticket A1: the cutover form, measured
#
# Anchor: the mini chain built WITH `--trout-table` (a side release, as `test_trout_section.py` builds it), measured
# against the committed mini release (the same chain, flag off).

@pytest.fixture(scope="module")
def side(tmp_path_factory):
    from pipeline.deliver.release.manifest import latest
    from pipeline.tools import mini_release as M
    root = tmp_path_factory.mktemp("trout-a1")
    M.build(root / "b", trout_table=True)
    return latest(root / "b" / "release")


@pytest.fixture(scope="module")
def a1(side):
    return TS.measure(side, under_test.mini(), log=lambda *x, **k: None)


def test_a1_the_flag_off_form_is_the_release_built_without_the_flag(a1):
    # every region file of the flag-off store is the committed (flag-off) release's, to the byte's gzip
    for name, z in a1["regions"].items():
        assert z["off_gz"] == z["live_gz"], name
        assert z["side_gz"] > z["off_gz"]
    # the water files less the section's parts: what is left is the meta's digests (other answers, other hashes)
    for name, z in a1["water"].items():
        assert abs(z["off_gz"] - z["live_gz"]) <= 64, (name, z)
        assert z["added"] > 0
    assert set(a1["zdicts"]) == set(TS._trout_tables())


def test_a1_the_side_form_is_priced_as_the_release_prices_it(a1, side):
    from pipeline.deliver.release import build as B
    for k, (n, _f) in B.web_moments(side).items():
        assert a1["moments"]["side"][k]["used"] == n, k
    assert a1["moments_live"]["first_tap"]["used"] == B.web_moments(under_test.mini())["first_tap"][0]


def test_a1_the_cutover_form_cuts_the_covered_rows_and_carries_the_page_row(side, tmp_path):
    from pipeline.deliver.store.decode import Store
    from pipeline.deliver.store.release import province_store
    st = province_store(side)
    sp = tmp_path / "side.sqlite"
    st.con.execute(f"VACUUM INTO '{sp}'")
    st.close()
    figs, ids, six = TS.Cutover(sp).build(tmp_path / "cut.sqlite")
    assert figs["cover_mismatch"] == [] and figs["repack_differs"] == 0
    assert figs["mixed_rows_split"] > 0 and figs["rows_cut"] > 0 and figs["groups_cut"] > 0
    cut = Store(tmp_path / "cut.sqlite", verify=False)
    rows = {i: cut.val("rows_row", b) for i, b in cut.con.execute("SELECT id, j FROM rows_row")}
    frames = {i: cut.val("rows_frame", b) for i, b in cut.con.execute("SELECT id, j FROM rows_frame")}
    assert frames == {i: json.loads(json.dumps(v)) for i, v in six.items()}
    assert all(len(f) == 6 for f in frames.values())
    covered = 0
    for rf, tf in cut.con.execute("SELECT rows, trout FROM cell WHERE trout IS NOT NULL"):
        covers = cut.val("trout_frame", cut.con.execute("SELECT j FROM trout_frame WHERE id = ?", (tf,)).fetchone()[0])[2]
        rs = frames[rf][2]
        for i, rid in enumerate(rs):
            fish = TS._fish(rows[rid])
            if i in covers:
                covered += 1
                assert fish and fish <= TC                     # a covered row holds trout and char only (split)
                assert not set(TS.CUT_FIELDS) & set(rows[rid])  # Q-D9
            else:
                assert not fish & TC                           # the rest holds none
    assert covered > 0
    # every region's shard reaches only rows the store holds
    for r, got in ids.items():
        assert got["rows_row"] <= set(rows), r
    cut.close()


def test_a1_a_split_that_ships_other_covers_stops_the_run(side, monkeypatch):
    from pipeline.deliver.answers.trout import table as TT
    from pipeline.tools.audit import answers_diff as AD
    real = TT.split_mixed

    def wrong(frame):                                   # the split's covers one row short
        got, covers = real(frame)
        return got, covers[:-1]
    monkeypatch.setattr(TT, "split_mixed", wrong)
    with pytest.raises(AD.DiffError, match="other covers"):
        TS.measure(side, log=lambda *x, **k: None)


def test_a1_refuses_a_release_without_the_section_and_the_cli_writes_its_report(side, tmp_path):
    assert TS.main([str(under_test.mini()), "--cutover", "--out", str(tmp_path / "a")]) == 2
    assert TS.main([str(side), "--cutover", "--live", str(under_test.mini()), "--out", str(tmp_path / "b")]) == 0
    rep = json.loads((tmp_path / "b" / "report.json").read_text())
    assert {"regions", "water", "moments", "verdict", "largest", "zdicts"} <= set(rep)
    assert rep["verdict"]["cutover_over"] == {}
    assert "TROUT TABLE A1" in (tmp_path / "b" / "report.txt").read_text()
