"""T94: the gauge promote changes one reviewed station at a time, and shows demotions before writing.

WHY. On 2026-10-10 the command `check_curated` printed (a full `match && promote`) demoted 25
stations and rewrote 1,042 rows of the curated `matches.json` when the owner wanted two reviewed
stations added: the size signal reads magnitudes from the CURRENT bundle and matched lines move,
so a full promote is not a no-op. These tests run `promote.main` end to end against TEMP copies
of the two curated files (never the real ones), with the disk inputs swapped for a fixture.

Mutations that fail these tests (each tried by hand, T94):
    M1  delete the `down and not accept_demotions` refusal in `main`
            -> test_a_full_promote_with_a_demotion_refuses_and_writes_nothing
    M2  `merge` ignores `only` (returns the fresh result whole)
            -> test_only_changes_exactly_that_row
    M3  `demotions` compares `>=` instead of `>` (an unchanged trust counts as a demotion)
            -> test_demotions_is_rank_order_whole_value
    M4  `_stations` accepts a bare match list as a baseline (returns {} for it)
            -> test_a_bare_list_baseline_is_refused
    M5  `fix_command` prints the template unfilled
            -> test_check_curated_prints_the_surgical_command
"""

from __future__ import annotations

import json

import pytest

from pipeline.gauges.generate import promote as P
from pipeline.gauges.matches import StationMatch


def _m(sid: str, name: str, dist: float) -> StationMatch:
    return StationMatch(sid, "matched", "name+radius", dist, lon=-120.0, lat=50.0,
                        wsc=f"100-{sid}", name=name, blk=f"9{sid}")


STATIONS = [{"station": "A", "name": "ALPHA RIVER NEAR X"},
            {"station": "B", "name": "BRAVO CREEK AT Y"},
            {"station": "C", "name": "CHARLIE LAKE NEAR Z"}]


def _row(sid, station_name, name, dist, trust, **extra):
    """A promoted row spelled out whole — the shape `build` must produce."""
    return {"station": sid, "station_name": station_name, "status": "matched",
            "resolved_by": "name+radius", "distance_m": dist, "reason": None,
            "lon": -120.0, "lat": 50.0, "wbk": [], "wsc": [f"100-{sid}"], "also": [],
            "name": name, "blk": f"9{sid}",
            "evidence": {"name_agrees": extra.pop("name_agrees", True),
                         "name_is_exact": extra.pop("name_is_exact", True),
                         "distance_m": dist, "area_km2": None, "mag": None, "area_agrees": None,
                         "stated_kind": None, "kind_agrees": None},
            "trust": trust, **extra}


#: The committed baseline: A and B promoted `auto`, nothing queued.
BEFORE = {"A": _row("A", "ALPHA RIVER NEAR X", "Alpha River", 50.0, "auto"),
          "B": _row("B", "BRAVO CREEK AT Y", "Bravo Creek", 50.0, "auto")}

#: The fresh match: A now lands on the wrong water (a would-be DEMOTION, auto -> candidate),
#: B moved 10 m (a rewrite, no demotion), C is new and agrees on everything.
FRESH = [_m("A", "Beta Creek", 40.0), _m("B", "Bravo Creek", 60.0), _m("C", "Charlie Lake", 5.0)]


@pytest.fixture
def files(tmp_path, monkeypatch):
    out, cand = tmp_path / "matches.json", tmp_path / "candidates.json"
    out.write_text(P.dump(P.MATCHES_ABOUT, BEFORE), "utf-8")
    cand.write_text(P.dump(P.CANDIDATES_ABOUT, {}), "utf-8")
    monkeypatch.setattr(P, "load_inputs", lambda path: (FRESH, STATIONS, {}, {}, {}))

    def run(*args):
        return P.main(["--matches", str(tmp_path / "raw.json"), "--out", str(out),
                       "--candidates", str(cand), *args])
    return out, cand, run


def _read(p):
    return json.loads(p.read_text("utf-8"))["stations"]


def test_a_full_promote_with_a_demotion_refuses_and_writes_nothing(files, capsys):
    out, cand, run = files
    before = (out.read_bytes(), cand.read_bytes())
    assert run() == 2
    assert (out.read_bytes(), cand.read_bytes()) == before
    said = capsys.readouterr().out
    assert "1 DEMOTION(S)" in said and "  A  auto -> candidate" in said and "REFUSED" in said


def test_dry_run_lists_the_demotion_and_writes_nothing(files, capsys):
    out, cand, run = files
    before = (out.read_bytes(), cand.read_bytes())
    assert run("--dry-run") == 0
    assert (out.read_bytes(), cand.read_bytes()) == before
    assert "  A  auto -> candidate" in capsys.readouterr().out


def test_only_changes_exactly_that_row(files):
    out, cand, run = files
    cand_before = cand.read_bytes()
    assert run("--only", "C") == 0
    assert _read(out) == {**BEFORE, "C": _row("C", "CHARLIE LAKE NEAR Z", "Charlie Lake", 5.0, "auto")}
    assert cand.read_bytes() == cand_before
    # Every row it was not asked about is byte-for-byte the baseline's (B's 10 m move is NOT taken).
    assert out.read_text("utf-8") == P.dump(P.MATCHES_ABOUT, _read(out))


def test_only_obeys_the_demotion_rule_for_the_stations_it_names(files):
    out, cand, run = files
    before = out.read_bytes()
    assert run("--only", "A") == 2
    assert out.read_bytes() == before
    assert run("--only", "A", "--accept-demotions") == 0
    assert _read(out) == {"B": BEFORE["B"]}
    assert _read(cand) == {"A": _row("A", "ALPHA RIVER NEAR X", "Beta Creek", 40.0, "candidate",
                                     name_agrees=False, name_is_exact=False, why="name_agrees")}


def test_a_full_promote_with_accept_demotions_writes_the_fresh_result_whole(files):
    out, cand, run = files
    assert run("--accept-demotions") == 0
    assert _read(out) == {"B": _row("B", "BRAVO CREEK AT Y", "Bravo Creek", 60.0, "auto"),
                          "C": _row("C", "CHARLIE LAKE NEAR Z", "Charlie Lake", 5.0, "auto")}
    assert list(_read(cand)) == ["A"]


def test_only_a_station_the_fresh_match_lacks_is_an_error(files):
    out, _, run = files
    before = out.read_bytes()
    with pytest.raises(SystemExit, match="not in the fresh match: Z"):
        run("--only", "Z")
    assert out.read_bytes() == before


def test_a_bare_list_baseline_is_refused(files, tmp_path):
    """The pre-T94 matcher wrote its raw list over the curated file; that baseline has no trust, so
    what a promote would demote is unknown — refuse rather than call it 'no demotions'."""
    out, _, run = files
    out.write_text(json.dumps({"stations": [{"station": "A"}]}), "utf-8")
    before = out.read_bytes()
    assert run() == 2
    assert out.read_bytes() == before
    with pytest.raises(SystemExit, match="--only needs a promoted baseline"):
        run("--only", "C")


def test_demotions_is_rank_order_whole_value():
    before = {s: {"trust": t} for s, t in
              [("r", "reviewed"), ("a", "auto"), ("w", "auto"), ("same", "auto"), ("gone", "bound")]}
    promoted = {"r": {"trust": "auto"}, "w": {"trust": "wrong"}, "same": {"trust": "auto"}}
    candidates = {"a": {}}
    # reviewed -> auto and auto -> candidate lose trust; auto -> wrong RAISES it (ORDER); a station
    # in neither file is below everything.
    assert P.demotions(before, promoted, candidates) == [
        ("a", "auto", "candidate"), ("gone", "bound", "absent"), ("r", "reviewed", "auto")]


def test_one_definition_of_a_demotion():
    """No second copy: the rank comparison lives in `demotions` and nowhere else in the module."""
    import inspect
    src = inspect.getsource(P)
    assert src.count("_rank(now) > _rank(was)") == 1


def test_check_curated_prints_the_surgical_command(tmp_path):
    from pipeline.tools import check_curated as C
    gauge = next(a for a in C.ARTIFACTS if a.name == "gauge match")
    assert "--only {stations}" in gauge.regenerate
    roster = tmp_path / "roster.json"
    roster.write_text(json.dumps([{"station": s, "active": True} for s in ("A", "NEW1", "NEW2")]))
    frozen = tmp_path / "frozen.json"
    frozen.write_text(json.dumps({"stations": {"A": {}}}))
    cand = tmp_path / "cand.json"
    cand.write_text(json.dumps({"stations": {}}))
    import dataclasses
    art = dataclasses.replace(gauge, roster=roster, frozen=frozen, candidates=cand)
    assert C.check(art)[0] == "STALE"
    assert C.fix_command(art) == (
        "python -m pipeline.gauges.generate.match --build data/generated/atlas/full"
        " && python -m pipeline.gauges.generate.promote --only NEW1 NEW2")


def test_the_matcher_refuses_to_write_over_the_curated_file(tmp_path):
    from pipeline.gauges.generate import match as M
    from pipeline.gauges.matches import MATCH_FILE
    with pytest.raises(SystemExit, match="refusing to write the raw match over the curated"):
        M.main(["--build", str(tmp_path), "--out", str(MATCH_FILE)])
