"""T786: no place ships on a "wrong" save (owner 2026-11-17: "The safety net should be a build warning I think then we
fix it and do not ship "wrong" places.."). `pipeline.regs.review.wrong_saves` lists every target in a scope that draws
an extent whose LATEST save is wrong / unsure with no extent fix; fast-fail and the release preflight refuse while it
lists one, and only a dated `WRONG_SAVE_OK` row naming the line passes it. Synthetic stores only (anchor a)."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.regs.review import wrong_saves as W

REPO = Path(__file__).resolve().parents[2]
FIX = [{"scope": "entry", "op": "whole", "points": [], "items": ["gnis:1"]}]


def _m(target, verdict, scope="dfo", **kw):
    return {"scope": scope, "target": target, "rule_id": kw.pop("rule_id", None), "verdict": verdict,
            "reviewer": "owner", "date": "2026-11-17", "note": kw.pop("note", f"{verdict} on {target}"), **kw}


def _store(tmp_path: Path, *marks) -> Path:
    p = tmp_path / "marks.jsonl"
    p.write_text("".join(json.dumps(m) + "\n" for m in marks), encoding="utf-8")
    return p


def _gate(tmp_path, *marks, allow=None):
    return W.check(marks=_store(tmp_path, *marks), allow=allow or {})


def test_latest_wrong_fires_naming_the_place_line_and_verdict(tmp_path, capsys):
    got = _gate(tmp_path, _m("1:river:a", "verified"), _m("1:river:a", "wrong"))
    assert not got["ok"] and [(r["target"], r["line"], r["verdict"]) for r in got["wrong"]] == [("1:river:a", 2, "wrong")]
    rc, said = W.say(got)
    assert rc == 1 and "dfo 1:river:a: mark line 2, verdict wrong, no extent fix" in said and "fix and re-save it" in said
    assert W.main(["--check", "--marks", str(tmp_path / "marks.jsonl")]) == 1
    assert "mark line 2, verdict wrong" in capsys.readouterr().out


def test_wrong_then_verified_clears(tmp_path):
    got = _gate(tmp_path, _m("1:river:a", "wrong"), _m("1:river:a", "verified"))
    assert got["ok"] and got["wrong"] == [] and W.say(got)[0] == 0


def test_wrong_with_an_extent_fix_clears(tmp_path):
    assert _gate(tmp_path, _m("1:river:a", "wrong", extent_fix=FIX))["ok"]


def test_unsure_with_no_fix_is_listed_like_wrong(tmp_path):
    """The marks audit treats "unsure" as "wrong" (`audit.old_extent_in_force`); so does the gate."""
    got = _gate(tmp_path, _m("1:river:a", "unsure"), _m("1:river:b", "unsure", extent_fix=FIX))
    assert [r["target"] for r in got["wrong"]] == ["1:river:a"]


def test_every_scope_that_draws_an_extent_and_none_other(tmp_path):
    got = _gate(tmp_path, _m("r1:e@1", "wrong", scope="entry"), _m("r1:e@1", "wrong", scope="rule", rule_id="r1:e@1#2"),
                _m("w1#k", "wrong", scope="closed_area", piece={"water_id": "w1"}), _m("1:river:a", "wrong"),
                _m("tidal:1", "wrong", scope="tidal"), _m("s1", "wrong", scope="split"), _m("q", "unsure", scope="q154"))
    assert sorted(r["scope"] for r in got["wrong"]) == ["closed_area", "dfo", "entry", "rule"]
    assert "(rule r1:e@1#2)" in W.say(got)[1]


def test_a_cleared_last_line_lists_nothing_and_a_later_wrong_lists_again(tmp_path):
    assert _gate(tmp_path, _m("1:river:a", "wrong"), _m("1:river:a", "cleared"))["ok"]
    assert not _gate(tmp_path, _m("1:river:a", "cleared"), _m("1:river:a", "wrong"))["ok"]


def test_live_and_dormant_alike_the_store_is_read_not_the_places(tmp_path):
    """The gate reads no place file, so a dormant place's wrong save cannot drop out (Region 3 Fraser, 11-16)."""
    assert [r["target"] for r in _gate(tmp_path, _m("3:fraser-river:dormant", "wrong"))["wrong"]] == ["3:fraser-river:dormant"]


def test_the_allowance_passes_only_the_line_it_names(tmp_path):
    ok = {"dfo|1:river:a|": {"date": "2026-11-17", "line": 1, "why": "owner: ships as it is until T999"}}
    got = _gate(tmp_path, _m("1:river:a", "wrong"), allow=ok)
    assert got["ok"] and [r["line"] for r in got["allowed"]] == [1]
    rc, said = W.say(got)
    assert rc == 0 and "allowed (2026-11-17, line 1: owner: ships as it is until T999)" in said
    # a newer wrong save on the same place is not covered by the old row, and the old row is named as unused
    got = _gate(tmp_path, _m("1:river:a", "wrong"), _m("1:river:a", "wrong", note="again"), allow=ok)
    assert not got["ok"] and got["unused_allow"] == ["dfo|1:river:a|"]


@pytest.mark.parametrize("row", [{"line": 1, "why": "x"}, {"date": "2026-11-17", "why": "x"},
                                 {"date": "2026-11-17", "line": 1, "why": " "}, {"date": "17 Nov", "line": 1, "why": "x"}])
def test_an_incomplete_allowance_is_refused_never_an_excuse(tmp_path, row, capsys):
    with pytest.raises(ValueError, match="WRONG_SAVE_OK"):
        _gate(tmp_path, _m("1:river:a", "wrong"), allow={"dfo|1:river:a|": row})


def test_an_unreadable_store_is_exit_2_never_a_pass(tmp_path, capsys):
    p = tmp_path / "marks.jsonl"
    p.write_text('{"scope": "dfo", "target": "1:river:a", "verdict": "wrong"}\n{not json\n', encoding="utf-8")
    assert W.main(["--check", "--marks", str(p)]) == 2
    assert W.main(["--check", "--marks", str(tmp_path / "missing.jsonl")]) == 2
    assert "could not run" in capsys.readouterr().err


def test_mutation_reading_the_first_save_instead_of_the_latest_misses_a_new_wrong(tmp_path, monkeypatch):
    """The mutation: a gate that keeps the FIRST save per place (a verified one) lets the owner's later "wrong" ship.
    The real one reads the latest."""
    marks = (_m("1:river:a", "verified"), _m("1:river:a", "wrong"))
    assert not _gate(tmp_path, *marks)["ok"]

    def first(lines):
        out = {}
        for n, m in lines:
            out.setdefault(W.mark_key(m), (n, m))
        return out
    monkeypatch.setattr(W, "in_force", first)
    assert _gate(tmp_path, *marks)["ok"]


def test_mutation_counting_any_fix_field_lets_a_closed_area_wrong_through(tmp_path, monkeypatch):
    """A wrong closed-area mark's `piece` is not a fix (`closed_areas.build` lands only verified marks)."""
    m = _m("w1#k", "wrong", scope="closed_area", piece={"water_id": "w1"})
    assert not _gate(tmp_path, m)["ok"]
    monkeypatch.setattr(W, "has_fix", lambda x: bool(x.get("extent_fix") or x.get("piece")))
    assert _gate(tmp_path, m)["ok"]


def test_fast_fail_runs_the_gate_without_a_waiting_pass():
    text = (REPO / "scripts" / "fast-fail.sh").read_text(encoding="utf-8")
    line = next(ln for ln in text.splitlines() if "pipeline.regs.review.wrong_saves" in ln)
    assert line.startswith("step ") and line.rstrip().endswith("--check")


def test_the_curated_allowance_is_whole():
    """Every committed row is dated, names its line and says why (checked by the same reader the gate uses)."""
    W._checked_allow(W.WRONG_SAVE_OK)
