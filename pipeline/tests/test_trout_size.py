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

def test_the_estimate_refuses_a_release_already_cut_over(base):
    """(T810 L) The estimate prices the section added and the trout groups cut from a release made BEFORE the cutover
    (A0's question, measured on the real form by A1). The committed mini release is the cutover form now: the estimate
    refuses it, saying why, rather than pricing the section twice."""
    a, _ = base
    with pytest.raises(TS.SizeError, match="already holds the trout section"):
        a.size(["1"])


def test_the_cli_writes_its_report(tmp_path, capsys):
    assert TS.main([str(under_test.mini()), "--regions", "1", "--out", str(tmp_path)]) == 2      # T810 L: cut over
    assert TS.main([str(under_test.mini()), "--no-size", "--out", str(tmp_path)]) == 0
    rep = json.loads((tmp_path / "report.json").read_text())
    assert {"mixed_rows", "angler_only", "hours_window"} <= set(rep)
    assert "TROUT TABLE A0" in (tmp_path / "report.txt").read_text()
    assert TS.main([str(tmp_path / "nowhere"), "--no-size", "--out", str(tmp_path)]) == 2


# ================================================================================ ticket A1: the cutover form, measured
#
# A1 measured a SIDE release (rows 33 + the section, built with the flag before the cutover) in its three forms: the
# result is in the T810 ticket (first_tap 4,256,614 of 4,400,000). Since the cutover (ticket L) every release IS the
# cutover form and the flag is gone, so no side release can be built to measure: the tool refuses one already cut over
# (the release's own size gates price it), and says why.

def test_a1_refuses_a_release_already_cut_over(tmp_path):
    from pipeline.tools.audit import answers_diff as AD
    with pytest.raises(AD.DiffError, match="already the cutover form"):
        TS.measure(under_test.mini(), log=lambda *x, **k: None)
    assert TS.main([str(under_test.mini()), "--cutover", "--out", str(tmp_path / "a")]) == 2
