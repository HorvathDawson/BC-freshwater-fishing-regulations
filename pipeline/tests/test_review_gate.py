"""Curation review phase 5 (T136, curation-review PLAN §4 "gate before full review").

  * THE PRINTED REBUILD TARGETS WHAT THE TOOL SERVES (`vintage.Target`, `rebuild_commands`): the live
    tree gets `pipeline build` (and, after a split, the side atlas's reach run, the binding diff
    between the two runs, and the promote WITH tiles — `deliver all` refuses a bundle whose tiles are
    another atlas's, so the old "--promote --no-tiles" could never finish); a tool pointed at copies
    gets the chain spelled out on the copies — no live path in it — and every printed command parses
    with its own CLI; a half-live target gets no command.
  * THE FALSE-STALE MEASURE (`marks.moved`, `gate.stale_report`): a mark stale with only the parts a
    build writes moved is a rebuild's doing, an edit's otherwise.
  * THE PARITY GATE (`parity`): rule by rule, outcome + reason + section set; only a typed cause
    explains a difference; the streamed run reads exactly what the writer wrote.
  * THE PILOT (`gate.pilot_pick`, `gate.pilot_report`, `Mark.seconds`) and the ready signal (`reuse.warm`).

Whole values throughout; each key behaviour is pinned by a named mutation (see each `*_mutation` test).
"""
from __future__ import annotations

import json
import shlex
import sys
from pathlib import Path

import pytest

from pipeline.common.curated import GENERATED, REPO_ROOT
from pipeline.regs.review import gate as G
from pipeline.regs.review import marks as M
from pipeline.regs.review import parity as P
from pipeline.regs.review import vintage as V

REPO = Path(__file__).resolve().parents[2]
BACKEND = REPO / "curation-review" / "backend"
SHA = "0" * 64


# --------------------------------------------------------------------------------------------
# The printed rebuild
# --------------------------------------------------------------------------------------------

def scratch(tmp: Path, **over) -> V.Target:
    d = dict(entries=tmp / "curated" / "catalogue", splits=tmp / "curated" / "splits.json",
             build=tmp / "atlas" / "full", reach=tmp / "reaches" / "full",
             bundle=tmp / "deliver" / "bundle.sqlite", release=tmp / "deliver" / "release",
             tiles=tmp / "tiles")
    d.update(over)
    return V.Target(**d)


def test_the_live_tree_prints_pipeline_build_and_after_a_split_the_whole_atlas_chain():
    live = V.Target.canonical()
    assert live.kind() == "canonical"
    assert V.rebuild_commands([V.Waiting.reach_entry]) == V.rebuild_commands(
        [V.Waiting.reach_entry], live) == [
        "scripts/heavy.sh build 30 -- python -m pipeline build --no-tiles"]
    b = Path(GENERATED.build()).relative_to(REPO_ROOT)
    r = Path(GENERATED.reaches / GENERATED.build().name).relative_to(REPO_ROOT)
    assert V.rebuild_commands([V.Waiting.split_unbuilt], live) == [
        "scripts/heavy.sh atlas 30 -- python -m pipeline build --atlas",
        f"scripts/heavy.sh reach 30 -- python -m pipeline.atlas.reach.cli --build {b}_next --out {r}_next",
        f"python -m pipeline.atlas.reach.binding_diff {r} {r}_next  # per-rule bindings, live vs the "
        "side atlas's reach run — read it before promoting (AGENTS 63)",
        "scripts/heavy.sh build 30 -- python -m pipeline build --promote"]
    assert V.rebuild_commands([]) == []


def test_a_scratch_target_gets_the_chain_on_its_own_paths(tmp_path):
    t = scratch(tmp_path)
    assert t.kind() == "scratch"
    s = {k: str(getattr(t, k)) for k in ("entries", "splits", "build", "reach", "bundle", "release", "tiles")}
    reach = (f"scripts/heavy.sh reach 30 -- python -m pipeline.atlas.reach.cli --build {s['build']} "
             f"--entries {s['entries']} --out {s['reach']}")
    deliver = (f"scripts/heavy.sh deliver 30 -- python -m pipeline.deliver all --build {s['build']} "
               f"--reaches {s['reach']} --entries {s['entries']} --tiles {s['tiles']} "
               f"--out-dir {tmp_path / 'deliver'}")
    assert V.rebuild_commands([V.Waiting.bundle_entry], t) == [reach, deliver]
    assert V.rebuild_commands([V.Waiting.split_moved, V.Waiting.reach_entry], t) == [
        f"scripts/heavy.sh atlas 30 -- python -m pipeline.atlas.build --full --splits {s['splits']} "
        f"--out {s['build']}_next",
        f"scripts/heavy.sh reach 30 -- python -m pipeline.atlas.reach.cli --build {s['build']}_next "
        f"--entries {s['entries']} --out {s['reach']}_next",
        f"python -m pipeline.atlas.reach.binding_diff {s['reach']} {s['reach']}_next  # per-rule "
        "bindings, live vs the side atlas's reach run — read it before promoting (AGENTS 63)",
        f"python -m pipeline.atlas.promote {s['build']}_next --target {s['build']} --tiles {s['tiles']} "
        f"--bundle {s['bundle']}",
        reach,
        f"scripts/heavy.sh tiles 30 -- python -m pipeline.deliver.tiles --build {s['build']} "
        f"--out {s['tiles']} --release-root {s['release']} --bundle {s['bundle']}",
        deliver]


LIVE_ROOTS = ("data/generated", "data/release", "data/curated")


def _live_named(cmds: list[str]) -> list[str]:
    """Every printed command naming a live path (repo-relative, or under the repo's data/)."""
    data = str(Path(REPO_ROOT) / "data")
    return [c for c in cmds if any(f" {r}" in c for r in LIVE_ROOTS) or data in c
            or " --atlas" in c or "pipeline build" in c]


def test_a_scratch_target_prints_no_live_path(tmp_path):
    t = scratch(tmp_path)
    for reasons in ([V.Waiting.reach_entry], [V.Waiting.split_unbuilt], [V.Waiting.no_reach_run]):
        assert _live_named(V.rebuild_commands(reasons, t)) == []


def test_the_scratch_check_is_pinned_by_mutation(tmp_path, monkeypatch):
    """Mutation: a target that is always read as the live tree (the commands ignore the tool's own
    paths) -> `test_a_scratch_target_prints_no_live_path`'s check finds live paths."""
    monkeypatch.setattr(V.Target, "kind", lambda self: "canonical")
    assert _live_named(V.rebuild_commands([V.Waiting.split_unbuilt], scratch(tmp_path)))


def test_a_half_live_target_prints_no_command(tmp_path):
    live = V.Target.canonical()
    half = scratch(tmp_path, build=live.build)                     # copies, but the live atlas
    assert half.kind() == "mixed"
    got = V.rebuild_commands([V.Waiting.split_unbuilt], half)
    assert len(got) == 1 and got[0].startswith("# no command: the tool serves some live artifacts")
    # a release root `deliver all --out-dir` would not write to: no command either
    off = scratch(tmp_path, release=tmp_path / "elsewhere")
    got = V.rebuild_commands([V.Waiting.reach_entry], off)
    assert len(got) == 1 and got[0].startswith("# no command: `pipeline.deliver all --out-dir")


class _Parsed(Exception):
    """Raised in place of the first real work a CLI does: its arguments parsed."""


def _parses(cmd: str, monkeypatch) -> None:
    """Run the printed command's CLI up to its first real work (patched to raise `_Parsed`): an
    unknown flag exits 2 before that."""
    words = shlex.split(cmd.split("  #", 1)[0])
    if words[0] == "scripts/heavy.sh":
        words = words[words.index("--") + 1:]
    assert words[:2] == ["python", "-m"], cmd
    mod, args = words[2], words[3:]

    def stop(*a, **k):
        raise _Parsed
    if mod == "pipeline.atlas.build":
        import pipeline.atlas.build as m
        monkeypatch.setattr(m, "FWADataAccessor", stop)
        monkeypatch.setattr(sys, "argv", [mod, *args])
        run = m.main
    elif mod == "pipeline.atlas.reach.cli":
        import pipeline.atlas.reach.cli as m
        monkeypatch.setattr(m, "load_registry", stop)
        monkeypatch.setattr(sys, "argv", [mod, *args])
        run = m.main
    elif mod == "pipeline.atlas.reach.binding_diff":
        import pipeline.atlas.reach.binding_diff as m
        monkeypatch.setattr(m, "diff_runs", stop)
        run = lambda: m.main(args)                                       # noqa: E731
    elif mod == "pipeline.atlas.promote":
        import pipeline.atlas.promote as m
        monkeypatch.setattr(m, "promote", stop)
        run = lambda: m.main(args)                                       # noqa: E731
    elif mod == "pipeline.deliver.tiles":
        import pipeline.deliver.tiles.__main__ as m
        from pipeline.deliver.tiles import tippe
        monkeypatch.setattr(tippe, "check", stop)
        monkeypatch.setattr(sys, "argv", [mod, *args])
        run = m.main
    elif mod == "pipeline.deliver":
        import pipeline.deliver.__main__ as m
        monkeypatch.setattr(m, "_all", stop)
        run = lambda: m.main(args)                                       # noqa: E731
    else:
        raise AssertionError(f"no parse check for {mod}")
    with pytest.raises(_Parsed):
        run()


def test_every_printed_scratch_command_parses_with_its_own_cli(tmp_path, monkeypatch):
    for cmd in V.rebuild_commands([V.Waiting.split_unbuilt], scratch(tmp_path)):
        with monkeypatch.context() as mp:
            _parses(cmd, mp)


def test_deliver_all_cuts_against_the_tiles_it_is_given(tmp_path, monkeypatch):
    """`--tiles` reaches the bundle step, the release step (with the tile release root this run
    writes to) and the vintage check — the three places a scratch rebuild read the live tiles."""
    import pipeline.deliver.__main__ as D
    from pipeline.common import vintage
    steps, checked = [], []
    monkeypatch.setattr(D, "_step", lambda args: steps.append(args))
    monkeypatch.setattr(vintage, "report_all", lambda tiles, arts, strict: (checked.append(tiles), (False, "x"))[1])
    tiles, out = tmp_path / "tiles", tmp_path / "deliver"
    assert D.main(["all", "--build", "B", "--reaches", "R", "--entries", "E", "--tiles", str(tiles),
                   "--out-dir", str(out)]) == 1
    assert steps[0][:3] == ["bundle", "--out", steps[0][2]] and steps[0][3:] == [
        "--build", "B", "--reaches", "R", "--entries", "E", "--tiles", str(tiles)]
    rel = next(s for s in steps if s[0] == "release")
    assert rel[-4:] == ["--tiles", str(tiles), "--tiles-release-root", str(out / "release")]
    assert checked == [tiles]


def test_promote_to_a_directory(tmp_path):
    from pipeline.atlas import promote as PR
    from pipeline.common.section_handles import FILENAME
    cand, target = tmp_path / "full_next", tmp_path / "full"
    for d, h in ((cand, "new"), (target, "old")):
        d.mkdir()
        (d / FILENAME).write_text(h)
        (d / "registry.json").write_text("{}")
    got = PR.promote(cand, target=target, tiles=tmp_path / "tiles", bundle=tmp_path / "b.sqlite",
                     log=lambda *_: None)
    assert (got["to"], got["prev"]) == (str(target.resolve()), str((tmp_path / "full.prev").resolve()))
    assert (target / FILENAME).read_text() == "new" and (tmp_path / "full.prev" / FILENAME).read_text() == "old"
    with pytest.raises(SystemExit, match="not both"):
        PR.promote(cand, "full", target=target)


# --------------------------------------------------------------------------------------------
# The false-stale measure
# --------------------------------------------------------------------------------------------

def _prints(text="t", meaning="m", binding="b", reach="r", answer="a", rules=("r1",)) -> M.Prints:
    recs = [M.RecordPrint(key="entry", meaning="e", binding="eb")]
    recs += [M.RecordPrint(key=f"rule:{r}", meaning=meaning, binding=binding, reach=reach,
                           answer=answer) for r in rules]
    return M.Prints(text=text, records=tuple(recs))


def _mark(target: str, prints: M.Prints, **kw) -> M.Mark:
    return M.Mark(scope="entry", target=target, verdict="verified",
                  answers={k: "y" for k in "TRPA"}, reviewer="tester", date="2026-10-23",
                  pdf_sha256=SHA, prints=prints, **kw)


MOVES = {   # what changed -> (moved parts, stale reasons)
    "nothing": (_prints(), set(), []),
    "text": (_prints(text="t2"), {"text"}, ["book text changed"]),
    "meaning": (_prints(meaning="m2"), {"records"}, ["rule changed"]),   # no pair: rule identity
    "binding": (_prints(binding="b2"), {"binding"}, ["binding moved"]),
    "reach": (_prints(reach="r2"), {"reach"}, ["binding moved"]),
    "answer": (_prints(answer="a2"), {"answer"}, ["answer changed"]),
    "unbuilt": (_prints(reach=None, answer=None), {"unbuilt"}, ["awaiting build"]),
    "new rule": (_prints(rules=("r1", "r2")), {"records"}, ["rule changed"]),
    "binding+reach": (_prints(binding="b2", reach="r2"), {"binding", "reach"}, ["binding moved"]),
}


def test_moved_is_the_one_comparison_and_stale_reasons_names_it():
    m = _mark("x", _prints())
    got = {k: (set(M.moved(m, now)), [r.value for r in M.stale_reasons(m, now)])
           for k, (now, _, _) in MOVES.items()}
    assert got == {k: (parts, reasons) for k, (_, parts, reasons) in MOVES.items()}
    assert (M.moved(m, None), M.stale_reasons(m, None)) == (frozenset({"records"}), [M.StaleReason.rule_changed])
    assert M.BUILT_PARTS == {"reach", "answer", "unbuilt"}


def _stale_world(monkeypatch):
    """Four marked entries and what each is NOW: one untouched, one edited (its binding: a split on
    its water), one whose bundle reach fluttered (a rebuild), one whose answer AND binding moved."""
    from pipeline.regs.review import fingerprint as F
    now = {"held": _prints(), "edited": _prints(binding="b2", reach="r2"),
           "flutter": _prints(reach="r2"), "both": _prints(binding="b2", answer="a2")}
    monkeypatch.setattr(F, "entry_fingerprints", lambda e, ctx, rule_id=None: now[e["entry_id"]])
    marks = [_mark(t, _prints()) for t in now]
    return marks, {t: {"entry_id": t} for t in now}


def test_stale_report_splits_an_edit_from_a_rebuild(monkeypatch):
    marks, entries = _stale_world(monkeypatch)
    row = lambda t, reasons, moved: {"scope": "entry", "target": t, "rule_id": None,  # noqa: E731
                                     "verdict": "verified", "reasons": reasons, "moved": moved}
    assert G.stale_report(marks, entries=entries, ctx=None) == {
        "marks": 4, "held": 1, "stale": 3,
        "edited": [row("edited", ["binding moved"], ["binding", "reach"]),
                   row("both", ["binding moved", "answer changed"], ["answer", "binding"])],
        "build_only": [row("flutter", ["binding moved"], ["reach"])],
        "false_stale_rate": 0.25}


def test_the_false_stale_split_is_pinned_by_mutation(monkeypatch):
    """Mutation: the bundle's `reach` not counted as a built part (a flutter read as an edit) ->
    `test_stale_report_splits_an_edit_from_a_rebuild` would see 0 false stales."""
    marks, entries = _stale_world(monkeypatch)
    monkeypatch.setattr(M, "BUILT_PARTS", frozenset({"answer", "unbuilt"}))
    got = G.stale_report(marks, entries=entries, ctx=None)
    assert got["build_only"] == [] and len(got["edited"]) == 3


# --------------------------------------------------------------------------------------------
# The parity gate
# --------------------------------------------------------------------------------------------

B = P.Binding


def test_compare_whole_values():
    s = frozenset({"1:0", "2:0"})
    assert P.compare(B("bound", None, s), B("bound", None, s)) is None
    assert P.compare(B("bound", None, s), B("bound", None, s | {"3:0"})) == {
        "outcome": ["bound", "bound"], "reason": [None, None], "gained": 1, "lost": 0,
        "sample_gained": ["3:0"], "sample_lost": [], "explained": None}
    # the app cannot answer a corpus fact alone: explained, by its cause
    assert P.compare(B("unresolved", "needs_corpus"), B("bound", None, s)) == {
        "outcome": ["unresolved", "bound"], "reason": ["needs_corpus", None], "gained": 2, "lost": 0,
        "sample_gained": ["1:0", "2:0"], "sample_lost": [], "explained": "needs_corpus"}
    # another unresolved reason is a defect
    assert P.compare(B("unresolved", "no_sections"), B("bound", None, s))["explained"] is None
    # a rule only one side has
    assert P.compare(None, B("unresolved", "no_extents"))["outcome"] == ["absent", "unresolved"]


def test_the_explanation_is_pinned_by_mutation(monkeypatch):
    """Mutation: an `explain` that explains any unresolved app answer -> `test_compare_whole_values`'s
    no_sections case would read explained."""
    monkeypatch.setattr(P, "explain", lambda app, run: app.reason if app.outcome == "unresolved" else None)
    assert P.compare(B("unresolved", "no_sections"), B("bound", None, frozenset({"1"})))["explained"]


def _write_run(tmp: Path, rows: list[dict], unresolved: list[dict]) -> Path:
    from pipeline.atlas.reach.io import _write_jsonl
    _write_jsonl(tmp / "rule_section.jsonl", rows)
    _write_jsonl(tmp / "rule_unresolved.jsonl", unresolved)
    _write_jsonl(tmp / "rule_diagnostic.jsonl", [])
    (tmp / "report.json").write_text("{}")
    return tmp


def _unresolved(d: Path):
    from pipeline.regs.review.runfacts import RunFacts
    return P.run_unresolved(RunFacts.read(d))


def test_the_streamed_run_reads_what_the_writer_wrote(tmp_path):
    rows = [{"entry_id": e, "rule_id": r, "section_id": s, "scope": sc}
            for e, r, s, sc in [("a", "r1", "1:0", "reach"), ("a", "r1", "2:0", "trib"),
                                ("a", "r2", "1:0", "reach"), ("b\"q", "r1", "9:0", "reach"),
                                ("c", "r1", "é:1", "reach")]]
    d = _write_run(tmp_path, rows, [{"entry_id": "a", "rule_id": "r3", "reason": "no_extents",
                                     "detail": ""}])
    got = [(e, list(rr)) for e, rr in P.run_sections(d / "rule_section.jsonl")]
    assert got == [("a", [("r1", frozenset({"1:0", "2:0"})), ("r2", frozenset({"1:0"}))]),
                   ('b"q', [("r1", frozenset({"9:0"}))]), ("c", [("r1", frozenset({"é:1"}))])]
    assert _unresolved(d) == {("a", "r3"): B("unresolved", "no_extents")}
    for line in (d / "rule_section.jsonl").read_text(encoding="utf-8").splitlines():
        j = json.loads(line)
        assert P._row(line + "\n") == (j["entry_id"], j["rule_id"], j["section_id"])
    (d / "bad.jsonl").write_text('{"entry_id":"b","rule_id":"r","scope":"reach","section_id":"1"}\n'
                                 '{"entry_id":"a","rule_id":"r","scope":"reach","section_id":"1"}\n')
    with pytest.raises(ValueError, match="out of order"):
        list(P.run_sections(d / "bad.jsonl"))


def test_compare_all_every_rule_once(tmp_path):
    rows = [{"entry_id": e, "rule_id": r, "section_id": s, "scope": "reach"}
            for e, r, s in [("a", "r1", "1"), ("a", "r2", "5"), ("ghost", "r1", "7"), ("z", "r1", "8")]]
    d = _write_run(tmp_path, rows, [{"entry_id": "a", "rule_id": "r3", "reason": "no_extents", "detail": ""}])
    app = {"a": {"r1": B("bound", None, frozenset({"1"})), "r2": B("bound", None, frozenset({"5", "6"})),
                 "r3": B("unresolved", "no_extents")},
           "z": {"r1": B("unresolved", "needs_corpus")}}
    asked = []
    diffs, n = P.compare_all({"a": ["r1", "r2", "r3"], "z": ["r1"]},
                             lambda e: (asked.append(e), app[e])[1],
                             P.run_sections(d / "rule_section.jsonl"),
                             _unresolved(d))
    assert (n, asked, sorted(diffs)) == (4, ["a", "z"], [("a", "r2"), ("ghost", "r1"), ("z", "r1")])
    assert (diffs[("a", "r2")]["lost"], diffs[("ghost", "r1")]["outcome"], diffs[("z", "r1")]["explained"]) \
        == (1, ["absent", "bound"], "needs_corpus")
    assert P.report(diffs, n) == {"rules": 4, "identical": 1, "explained": {"needs_corpus": 1},
                                  "unexplained": 2, "unexplained_rules": ["a#r2", "ghost#r1"],
                                  "parity": 25.0, "passes": False}


def test_the_run_must_be_of_this_corpus_and_atlas():
    rep = {"entries_digest": "c1", "handles": "h", "registry_digest": "g"}
    assert P.check_vintage(rep, "c1", "h", "g") == []
    assert len(P.check_vintage(rep, "c2", "h", "x")) == 2


# --------------------------------------------------------------------------------------------
# The pilot
# --------------------------------------------------------------------------------------------

def _rows():
    rows = []
    for tier, n in (("P1", 12), ("Z", 4), ("P2", 3), ("P3", 9), ("P4", 30), ("sweep", 5)):
        for i in range(n):
            rows.append({"entry_id": f"{tier}-{i}", "tier": tier, "name": f"{tier} {i}", "region": "1",
                         "verify": "unmarked", "vintage": "current"})
    rows[0]["verify"] = "verified"                     # P1-0: already marked
    rows[1]["vintage"] = "awaiting build"              # P1-1: cannot be finished now
    return rows


def _cards():
    return [{"split_id": f"s{i}", "label": f"L{i}", "waterbody": "W", "s0": True, "status": "unmarked",
             "vintage": {"state": "current"}} for i in range(8)]


def test_pilot_pick_spreads_each_tier_over_what_can_be_finished_now():
    got = G.pilot_pick(_rows(), _cards())
    ids = [t["id"] for t in got["targets"]]
    assert ids == ["s1", "s3", "s5", "s7",
                   "P1-2", "P1-4", "P1-6", "P1-7", "P1-9", "P1-11",
                   "Z-0", "Z-1", "Z-2", "Z-3",
                   "P2-0", "P2-1", "P2-2",
                   "P3-1", "P3-3", "P3-5", "P3-7",
                   "P4-2", "P4-7", "P4-12", "P4-17", "P4-22", "P4-27",
                   "sweep-1", "sweep-3"]
    assert got["short"] == {"P2": 1}
    assert got["targets"][0] == {"tier": "S0", "kind": "split", "id": "s1", "name": "W: L1", "hash": "split:s1"}
    assert sum(G.PILOT_QUOTAS.values()) == 30


def test_the_pilot_filter_is_pinned_by_mutation(monkeypatch):
    """Mutation: a pick that ignores the vintage (an entry awaiting its build is picked) ->
    `test_pilot_pick_spreads_each_tier_over_what_can_be_finished_now` changes."""
    rows = [dict(r, vintage="current") for r in _rows()]
    assert [t["id"] for t in G.pilot_pick(rows, _cards())["targets"]][4:10] != \
        ["P1-2", "P1-4", "P1-6", "P1-7", "P1-9", "P1-11"]


def test_pilot_report_reads_the_marks_store():
    targets = [{"tier": "P4", "kind": "entry", "id": "a"}, {"tier": "P4", "kind": "entry", "id": "b"},
               {"tier": "Z", "kind": "entry", "id": "z"}, {"tier": "P1", "kind": "entry", "id": "c"}]
    rule = lambda rid, s: M.Mark(scope="rule", target="z", rule_id=rid, verdict="verified",  # noqa: E731
                                 answers={k: "y" for k in "TRPA"}, reviewer="ann", date="2026-10-24",
                                 pdf_sha256=SHA, seconds=s,
                                 prints=M.Prints(text="t", records=(
                                     M.RecordPrint(key="entry", meaning="e", binding="b"),
                                     M.RecordPrint(key=f"rule:{rid}", meaning="m", binding="b"))))
    marks = [_mark("a", _prints(), seconds=40.0),
             M.Mark(scope="entry", target="b", verdict="wrong", answers={"T": "y", "R": "n", "P": "y", "A": "y"},
                    problem="quota", note="says 2, book says 1", reviewer="ann", date="2026-10-24",
                    pdf_sha256=SHA, prints=_prints(), seconds=95.5),
             rule("r1", 30.0), rule("r2", 12.5),
             M.Mark(scope="entry", target="a", verdict="verified", answers={k: "y" for k in "TRPA"},
                    reviewer="bot", date="2026-10-25", pdf_sha256=SHA, prints=_prints(),
                    carried_by="a migration", seconds=999.0),
             M.Mark(scope="entry", target="c", verdict="deferred", answers={}, reviewer="ann",
                    note="TOOL: the crop did not load", date="2026-10-24", pdf_sha256=SHA,
                    prints=_prints(), seconds=60.0)]
    got = G.pilot_report(targets, marks)
    assert got["overall"] == {"targets": 4, "marked": 4, "timed": 4, "median_s": 51.2, "mean_s": 59.5,
                              "defects": 1, "tool_blockers": 1}
    assert got["tiers"]["Z"] == {"targets": 1, "marked": 1, "timed": 1, "median_s": 42.5, "mean_s": 42.5,
                                 "defects": 0, "tool_blockers": 0}
    assert got["targets"][1]["defects"] == [{"verdict": "wrong", "problem": "quota", "note": "says 2, book says 1"}]
    assert got["targets"][0]["seconds"] == 40.0          # the carried copy is not the reviewer's time
    assert got["passes"] is False                          # a tool blocker
    md = G.pilot_markdown({"targets": [{"tier": "S0", "id": "s1", "name": "W: L", "hash": "split:s1"}]})
    assert md == ("| # | tier | target | open |\n|---|---|---|---|\n"
                  "| 1 | S0 | W: L (`s1`) | http://127.0.0.1:5173/#split%3As1 |\n")


def test_a_mark_records_the_reviewers_time():
    m = _mark("a", _prints(), seconds=12.3)
    assert json.loads(M.line(m))["seconds"] == 12.3
    assert _mark("a", _prints()).seconds is None
    with pytest.raises(ValueError):
        _mark("a", _prints(), seconds=-1)


# --------------------------------------------------------------------------------------------
# The ready signal
# --------------------------------------------------------------------------------------------

@pytest.fixture
def reuse_mod(monkeypatch):
    monkeypatch.syspath_prepend(str(BACKEND))
    for mod in ("reuse", "answer", "model_api", "writes"):
        sys.modules.pop(mod, None)
    import reuse
    yield reuse
    for mod in ("reuse", "answer", "model_api", "writes"):
        sys.modules.pop(mod, None)


def test_warm_reports_each_step_then_ready(reuse_mod, monkeypatch):
    seen = []
    monkeypatch.setattr(reuse_mod, "_one", lambda: seen.append(1), raising=False)
    monkeypatch.setattr(reuse_mod, "_two", lambda: seen.append(2), raising=False)
    monkeypatch.setattr(reuse_mod, "WARM_STEPS", (("one", "_one"), ("two", "_two")))
    assert reuse_mod.readiness()["ready"] is False
    got = reuse_mod.warm(reuse_mod.WARM_STEPS)
    assert seen == [1, 2]
    assert ([d["step"] for d in got["done"]], got["todo"], got["ready"], got["error"]) == (
        ["one", "two"], [], True, None)

    def boom():
        raise RuntimeError("no graph")
    monkeypatch.setattr(reuse_mod, "_two", boom, raising=False)
    got = reuse_mod.warm(reuse_mod.WARM_STEPS)
    assert (got["ready"], got["todo"], got["error"]) == (False, ["two"], "RuntimeError: no graph")
