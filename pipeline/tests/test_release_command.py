"""`python -m pipeline release` (T295/T297, T408): the orchestration, with every heavy step mocked — the order, stop on
a failure, resume, the binding-diff gate, and production refusing anything but a human. T666 (one release path):
preflight lists every failure, fast-fail is keyed on the tree's content, the follows stage refreshes what follows the
release on the candidate, the flags are remembered, the rehearsal walks every stage's checks."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline import build as B
from pipeline import release as R

LIVE, NEW = "r-live00000000", "r-new000000000"


def _release(root: Path, rid: str, reach: str, data: bytes = b"data", extra: dict | None = None,
             stamp: str | None = "0.5.0") -> None:
    d = root / rid
    d.mkdir(parents=True)
    (d / "water.bin.gz").write_bytes(data)
    (d / "manifest.json").write_text(json.dumps({
        "format": "app-manifest/2", "data_release": rid, **({"minAppVersion": stamp} if stamp else {}),
        "files": {"water": {"url": f"{rid}/water.bin.gz", "gz": len(data), "format": "water/1"}, **(extra or {})},
        "bundle": {"entries_digest": "e", "reach_digest": reach}}))


def _reach(d: Path, digest: str) -> None:
    d.mkdir(parents=True, exist_ok=True)
    (d / "report.json").write_text(json.dumps({"digest": digest}))
    (d / "rule_section.jsonl.gz").write_bytes(digest.encode())


#: (T555 item 4) the app-reads table these tests judge by: 0.3.0 was the first to read paint/5, 0.5.0 salmon/4
APPS = {"0.2.0": {"water": ["water/1"], "paint": ["paint/4"], "salmon": ["salmon/2"]},
        "0.3.0": {"water": ["water/1"], "paint": ["paint/5", "paint/4"], "salmon": ["salmon/2"]},
        "0.5.0": {"water": ["water/1"], "paint": ["paint/5", "paint/4"], "salmon": ["salmon/4"]}}


@pytest.fixture
def world(tmp_path: Path, monkeypatch):
    root, reaches = tmp_path / "release", tmp_path / "reaches" / "full"
    _release(root, LIVE, "reach-live")
    (root / "LATEST").write_text(LIVE + "\n")
    _reach(reaches, "reach-live")
    w = {"calls": [], "fail": set(), "dirty": [], "diffs": 0, "differs": False, "root": root, "reaches": reaches,
         "pushed": True, "fast_fail_rc": 0, "fast_fails": 0, "new_stamp": "0.5.0", "tree": "t1", "rc": {}}

    def run_cmd(cmd, env=None):
        name = "inseason" if "pipeline.regs.inseason.resolve" in cmd else \
            "recut" if any("regs-recut/sync.mjs" in c for c in cmd) else \
            "taps" if any("regs-recut/taps.py" in c for c in cmd) else \
            "follow-py" if "pytest" in cmd else "follow-worker" if "workers/feeds" in cmd else \
            "follow-app" if "vitest" in cmd else "app-gate" if cmd[:4] == ["pnpm", "-C", "app", "run"] else \
            "build" if "build" in cmd else \
            "upload" if any("upload.sh" in c for c in cmd) else "smoke" if any("smoke-staging" in c for c in cmd) else \
            "publish" if any("publish" in c for c in cmd) else " ".join(cmd)
        w["calls"].append((name, env))
        for c in cmd:                       # (T680) a check's report, as the real tool would write it
            if name in w.get("reports", {}) and c.startswith(("--outputFile.json=", "--junitxml=")):
                Path(c.split("=", 1)[1]).write_text(w["reports"][name])
        if name in w["rc"]:
            return w["rc"][name]
        if name in w["fail"]:
            return 3
        if name == "build" and not (root / NEW).exists():       # the build writes the release and CANDIDATE
            _reach(reaches, "reach-new")
            _release(root, NEW, "reach-new", extra=w.get("extra"), stamp=w["new_stamp"])
            (root / "CANDIDATE").write_text(NEW + "\n")
        return 0

    def diff(before, after):
        w["diffs"] += 1
        return {"before": str(before), "after": str(after), "rules_changed": int(w["differs"]),
                "unresolved_changed": 0, "licensing_changed": 0, "rules": {}, "unresolved": {}, "licensing": {}}

    monkeypatch.setattr(R, "release_root", lambda: root)
    monkeypatch.setattr(R, "reach_dir", lambda: reaches)
    monkeypatch.setattr(R, "HOME", tmp_path / "home")
    monkeypatch.setattr(R, "LEDGER", tmp_path / "home" / "ledger.json")
    monkeypatch.setattr(R, "git_dirty", lambda: list(w["dirty"]))
    monkeypatch.setattr(R, "git_head", lambda: "abc123")
    monkeypatch.setattr(R, "run_cmd", run_cmd)

    def fast_fail(base=None, env=None):
        w["fast_fails"] += 1
        w["fast_fail_base"], w["fast_fail_env"] = base, env
        return w["fast_fail_rc"], list(w.get("fast_fail_failed", ["a step that failed"] if w["fast_fail_rc"] else []))

    monkeypatch.setattr(R, "run_fast_fail", fast_fail)
    # (T666 item 5) the tree by content, what was uncommitted at a pass, and what differs from a commit
    monkeypatch.setattr(R, "tree_key", lambda: "key-" + w["tree"])
    w["dirt"], w["changed_since"] = [], set()
    monkeypatch.setattr(R, "dirty_paths", lambda: list(w["dirt"]))
    monkeypatch.setattr(R, "changed_since", lambda rev: set(w["changed_since"]))
    # (T666) the fresh-copy test run: a clean clone of HEAD, the no-data tier (heavy: stood in for)
    w["fresh_copies"], w["fresh_copy_rc"], w["committed_since"] = 0, 0, set()

    def fresh_copy():
        w["fresh_copies"] += 1
        return w["fresh_copy_rc"]
    monkeypatch.setattr(R, "run_fresh_copy", fresh_copy)
    monkeypatch.setattr(R, "head_tree", lambda: "committed-" + w["tree"])
    monkeypatch.setattr(R, "committed_since", lambda rev: set(w["committed_since"]))
    w["ci"] = (0, "ci-status: ok    app-ci.yml: green on abc123")
    w["ci_skips"] = []

    def ci(skip=None):
        w["ci_skips"].append(skip)
        return w["ci"]
    monkeypatch.setattr(R, "ci_status", ci)
    monkeypatch.setattr(R, "git_has_commit", lambda rev: rev in w.get("commits", ()))
    monkeypatch.setattr(R, "git_pushed", lambda: w["pushed"])
    w["landed"] = (True, "review marks: every mark in force has landed")
    monkeypatch.setattr(R, "marks_landed", lambda: w["landed"])
    w["atlas_behind"] = []
    monkeypatch.setattr(R, "atlas_behind", lambda: list(w["atlas_behind"]))
    monkeypatch.setattr(R, "binding_diff", diff)
    # (T555 item 4) what each app version reads: a small table in place of the committed one (pinned in test_app_reads.py),
    # so these releases' made-up formats are judged the same whatever the real apps read today
    from pipeline.deliver.release import app_gate as AG
    monkeypatch.setattr(AG, "table", lambda: APPS)
    # (T555 item 5) the ids gone with no alias: reads both releases' core files (pinned in test_release_gone_ids.py)
    w["gone"] = []

    def gone(before, after):
        rep = {"before": 3, "kept": 3 - len(w["gone"]), "aliased": [], "gone": list(w["gone"])}
        return rep, f"item ids, {before} -> {after}: {len(w['gone'])} gone with no alias"
    monkeypatch.setattr(R, "gone_ids", gone)
    monkeypatch.setattr(R, "diff_summary", lambda rep: f"{rep['rules_changed']} rule(s) bind different sections")
    # the manifest check's real body reads the size gate and publish.plan over real release files: pinned elsewhere
    monkeypatch.setattr(R, "manifest_problems", lambda rid: list(w.get("manifest_problems", [])))
    # the live staging site (network): `site_gate` is pinned by its own tests below
    w["site_min_apps"] = []

    def site(min_app):
        w["site_min_apps"].append(min_app)
        return list(w.get("site_problems", [])), "site serves app 9.9.9"
    monkeypatch.setattr(R, "site_problems", site)
    # (T537) the in-season resolve's file, its commit, and what staging serves (network)
    w["inseason_dirty"] = False
    monkeypatch.setattr(R, "inseason_dirty", lambda: w["inseason_dirty"])
    w["commits_made"] = []

    def commit_inseason(rid):
        w["commits_made"].append(rid)
        w["pushed"], w["tree"] = False, w["tree"] + "+matches"       # a new HEAD, not on GitHub yet
        w["unpushed"].append("c0mmit" + "0" * 34)
        return "c0mmit" + "0" * 34
    monkeypatch.setattr(R, "commit_inseason", commit_inseason)
    # (T666 items 1-2) the follows stage: the re-cut's changed paths, its commit, the tests found by rule, the push
    w["recut_changed"], w["recuts_committed"], w["unpushed"], w["pushes"] = [], [], [], 0
    monkeypatch.setattr(R, "recut_changed", lambda: list(w["recut_changed"]))
    monkeypatch.setattr(R, "recut_token", lambda paths: "tok-" + str(len(paths)))

    def commit_recut(rid, kind):
        w["recuts_committed"].append((rid, kind))
        w["recut_changed"], w["pushed"], w["tree"] = [], False, w["tree"] + "+fixtures"
        w["unpushed"].append("f1xture" + "0" * 33)
        return "f1xture" + "0" * 33
    monkeypatch.setattr(R, "commit_recut", commit_recut)
    monkeypatch.setattr(R, "follow_tests", lambda: (["pipeline/tests/test_x.py"], ["tools/x.release.test.ts"]))
    monkeypatch.setattr(R, "app_check_steps", lambda: ["typecheck"])      # (T680) one gate stands for check:fast's
    monkeypatch.setattr(R, "git_unpushed", lambda: list(w["unpushed"]))

    def push():
        w["pushes"] += 1
        w["pushed"], w["unpushed"] = True, []
        return 0
    monkeypatch.setattr(R, "git_push", push)
    w["staging_prechecks"] = []
    monkeypatch.setattr(R, "staging_prechecks", lambda rid: list(w["staging_prechecks"]))
    w["inseason_check"] = ("same", "in-season matches on r-x: unchanged")
    monkeypatch.setattr(R, "inseason_check", lambda rid: w["inseason_check"])
    w["goldens_paired"] = True
    monkeypatch.setattr(R, "goldens_paired", lambda d: (w["goldens_paired"], "said"))
    w["staging_serves"] = NEW
    monkeypatch.setattr(R, "staging_release", lambda: w["staging_serves"])
    monkeypatch.setattr(R, "after_chores", lambda rid: f"after the release: chores for {rid}")
    return w


def _staging(**kw) -> int:
    return R.main(["--staging", *sum(([f"--{k.replace('_', '-')}", str(v)] for k, v in kw.items()), [])])


def _latest(w) -> str:
    return (w["root"] / "LATEST").read_text().strip()


ALL = ["build", "inseason", "recut", "follow-py", "follow-app", "follow-worker", "app-gate", "upload", "smoke"]


def _env_of(w, name) -> dict:
    return [env for c, env in w["calls"] if c == name][0]


def test_the_stages_run_in_order_and_ship_the_candidate(world, capsys):
    assert _staging() == 0
    out = capsys.readouterr().out
    assert R.STAGES == ("preflight", "baseline", "build", "checks", "follows", "promote", "staging")
    marks = [out.index(f"[{i}/7] {s} ok") for i, s in enumerate(R.STAGES, 1)]
    assert marks == sorted(marks)
    assert [c for c, _ in world["calls"]] == ALL
    assert _env_of(world, "upload") == {"MIN_APP_VERSION": "0.5.0", "FAST_FAIL_DONE": "abc123", "SMOKE": "0"}   # live stamp
    assert _latest(world) == NEW and not (world["root"] / "CANDIDATE").exists()
    rec = json.loads(R.LEDGER.read_text())["releases"][NEW]
    assert rec["checks"]["binding_diff"] == "same" and rec["staged_at"] and rec["commit"] == "abc123"


def test_ids_gone_with_no_alias_are_listed_and_kept_and_never_stop_the_release(world, capsys):
    world["gone"] = [{"id": "wbk:3", "name": "Lost Pond"}]
    assert _staging() == 0
    out = capsys.readouterr().out
    assert f"item ids, {LIVE} -> {NEW}: 1 gone with no alias" in out and "1 item id(s) gone with no alias" in out
    assert json.loads(R.LEDGER.read_text())["releases"][NEW]["checks"]["gone_ids"] == 1
    kept = json.loads((R.HOME / "checks" / NEW / "gone_ids.json").read_text())
    assert kept["gone"] == [{"id": "wbk:3", "name": "Lost Pond"}]


def test_uncommitted_code_stops_before_anything_runs(world, capsys):
    world["dirty"] = ["pipeline/build.py"]
    assert _staging() == 1
    assert world["calls"] == [] and _latest(world) == LIVE
    err = capsys.readouterr().err
    assert "STOPPED at preflight" in err and "pipeline/build.py" in err


def test_an_atlas_behind_its_curated_inputs_stops_naming_the_file(world, capsys):
    """T547: a cut / name / area edited after the atlas build stops the release before anything runs, naming the file;
    --ship-stale-atlas REASON goes on and prints the reason."""
    world["atlas_behind"] = ["cuts.json (curated:waters.cuts): changed since the atlas was built — rebuild: x"]
    assert _staging() == 1
    err = capsys.readouterr().err
    assert "STOPPED at preflight" in err and "cuts.json (curated:waters.cuts)" in err and "--ship-stale-atlas" in err
    assert world["calls"] == [] and _latest(world) == LIVE
    with pytest.raises(SystemExit):
        _staging(ship_stale_atlas=" ")
    assert _staging(ship_stale_atlas="owner: names only, rebuild next week") == 0
    assert "SHIPPED ANYWAY (--ship-stale-atlas): owner: names only, rebuild next week" in capsys.readouterr().out


def test_the_dry_run_says_whether_the_atlas_is_behind(world, capsys):
    world["atlas_behind"] = ["slough_calls.json (curated:waters.slough_calls): changed since the atlas was built"]
    assert R.main(["--staging", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "WOULD STOP: the atlas is behind 1 input(s)" in out and "slough_calls.json" in out
    world["atlas_behind"] = []
    assert R.main(["--staging", "--dry-run"]) == 0
    assert "ok: the atlas is current with its curated inputs" in capsys.readouterr().out


def test_a_failed_build_leaves_latest_and_uploads_nothing(world, capsys):
    world["fail"] = {"build"}
    assert _staging() == 1
    assert [c for c, _ in world["calls"]] == ["build"] and _latest(world) == LIVE
    assert "STOPPED at build" in capsys.readouterr().err


def test_a_failed_check_leaves_latest_untouched(world, capsys):
    world["manifest_problems"] = ["files.water: missing"]
    assert _staging() == 1
    assert _latest(world) == LIVE and (world["root"] / "CANDIDATE").read_text().strip() == NEW
    assert [c for c, _ in world["calls"]] == ["build"]
    assert "STOPPED at checks" in capsys.readouterr().err


def test_the_app_gate_refuses_a_format_the_oldest_app_cannot_read(world, capsys):
    world["extra"] = {"salmon": {"url": "x", "format": "salmon/4"}}
    assert _staging(min_app_version="0.4.0") == 1
    err = capsys.readouterr().err
    assert "STOPPED at checks" in err and "salmon/4 needs app >= 0.5.0" in err
    assert _latest(world) == LIVE and [c for c, _ in world["calls"]] == ["build"]
    # (T666 item 4) the flag is remembered: a bare rerun asks the same thing again; a new value replaces it
    assert _staging() == 1 and "salmon/4 needs app >= 0.5.0" in capsys.readouterr().err
    assert _staging(min_app_version="0.5.0") == 0


def test_the_diff_gate_stops_and_only_its_own_json_lets_it_pass(world, tmp_path, capsys):
    world["differs"] = True
    assert _staging() == 1
    err = capsys.readouterr()
    assert "STOPPED at checks" in err.err and "--accept-diff" in err.err and "1 rule(s)" in err.out
    assert _latest(world) == LIVE and [c for c, _ in world["calls"]] == ["build"]
    js = R.HOME / "checks" / NEW / "binding_diff.json"
    assert js.is_file()
    stale = tmp_path / "stale.json"
    stale.write_text("{}")
    assert _staging(accept_diff=stale) == 1                  # another diff is not this one
    assert _latest(world) == LIVE
    assert _staging(accept_diff=js) == 0
    assert _latest(world) == NEW
    assert world["diffs"] == 1                                # the diff ran once; reruns read the ledger
    assert json.loads(R.LEDGER.read_text())["releases"][NEW]["checks"]["binding_diff"] == "accepted"


def test_a_rerun_resumes_where_it_stopped(world, capsys):
    world["fail"] = {"upload"}
    assert _staging() == 1
    assert "STOPPED at staging" in capsys.readouterr().err
    assert _latest(world) == NEW                              # checks passed and promoted before the upload failed
    world["fail"] = set()
    assert _staging() == 0
    assert [c for c, _ in world["calls"]] == ALL[:-1] + ["build", "upload", "smoke"]
    assert world["diffs"] == 1                                # checked once; the rerun read the ledger
    assert _staging() == 0                                    # done: the upload is not repeated
    assert [c for c, _ in world["calls"]][-1] == "build"
    assert "already on staging" in capsys.readouterr().out


def test_a_rerun_after_the_promote_keeps_the_first_runs_min_app(world, capsys):
    """T484 (r-1b4f54fb6ed9, 2026-11-07): the upload failed after the promote; the rerun took the default min app from
    LATEST — by then the new, unstamped release — so the gate saw "any" and refused paint/5 + salmon/4."""
    world["new_stamp"] = None                                 # the build does not stamp; the upload does
    world["extra"] = {"salmon": {"url": "x", "format": "salmon/4"}}
    world["fail"] = {"upload"}
    assert _staging() == 1
    assert "STOPPED at staging" in capsys.readouterr().err and _latest(world) == NEW
    rec = json.loads(R.LEDGER.read_text())["releases"][NEW]
    assert rec["checks"]["min_app_version"] == "0.5.0" and rec["live_before"] == LIVE
    world["fail"] = set()
    assert _staging() == 0, capsys.readouterr().err
    ups = [env for c, env in world["calls"] if c == "upload"]
    assert [u["MIN_APP_VERSION"] for u in ups] == ["0.5.0", "0.5.0"]


def test_a_rerun_with_no_checks_recorded_falls_back_to_the_release_before_the_promote(world):
    led = {"releases": {NEW: {"live_before": LIVE}}}
    assert R.resume_min_app(led, NEW, NEW) == "0.5.0"         # LIVE's stamp, not NEW's (LATEST now)
    led["releases"][NEW]["checks"] = {"min_app_version": "0.6.0"}
    assert R.resume_min_app(led, NEW, NEW) == "0.6.0"
    assert R.resume_min_app({"releases": {}}, None, LIVE) == "0.5.0"     # a first run: the live stamp


def test_fast_fail_runs_first_and_stops_the_release(world, capsys):
    world["fast_fail_rc"] = 1
    assert _staging() == 1
    err = capsys.readouterr().err
    assert "STOPPED at preflight" in err and "fast-fail.sh failed" in err
    assert world["calls"] == [] and _latest(world) == LIVE and world["fast_fails"] == 1


def test_preflight_runs_every_check_and_lists_every_failure_together(world, capsys):
    """T666 item 3 (build 27: six runs, each stopped by the next single failure). An instant failure is said in its
    first second (T535) and the rest still run — fast-fail too — so one stop carries the whole list."""
    world["dirty"], world["pushed"] = ["pipeline/build.py"], False
    world["site_problems"] = ["site 0.6.9 < 0.7.0"]
    world["landed"] = (False, "review marks: 1 NOT LANDED in the curated files:\n  would dfo dfo|2:some-river:x|: y")
    world["atlas_behind"] = ["cuts.json (curated:waters.cuts): changed since the atlas was built"]
    world["ci"] = (1, "ci-status: STOP  pipeline-ci.yml: RED on abc123 (failure): https://github.com/x/runs/7")
    world["fast_fail_rc"], world["fast_fail_failed"] = 1, ["app gates (pnpm check:fast)", "format pairs"]
    world["fresh_copy_rc"] = 1
    assert _staging() == 1
    io = capsys.readouterr()
    assert "[1/7] preflight FAILED: 8 problems — ALL of them" in io.err and "STOPPED at preflight" in io.err
    for said in ("pipeline/build.py", "cuts.json (curated:waters.cuts)", "dfo|2:some-river:x|", "not on origin/",
                 "CI is red", "site 0.6.9 < 0.7.0", "- app gates (pnpm check:fast)", "- format pairs",
                 "the fresh-copy test run failed"):
        assert said in io.err, said
    assert [f"  {k}. " in io.err for k in range(1, 9)] == [True] * 8 and io.err.count("     next: ") == 8
    assert "6 check(s) failed already" in io.out and world["fast_fails"] == 1      # it ran to complete the list
    assert world["fresh_copies"] == 1
    assert io.out.index("STOP: 1 uncommitted path(s)") < io.out.index("6 check(s) failed already")   # said at once
    assert world["calls"] == [] and _latest(world) == LIVE
    assert "fast_fail" not in json.loads(R.LEDGER.read_text())                      # a failed run records no pass


def test_one_failure_reads_as_it_always_did(world, capsys):
    world["pushed"] = False
    assert _staging() == 1
    err = capsys.readouterr().err
    assert "[1/7] preflight FAILED: HEAD abc123 is not on origin/redesign/stream-sections" in err
    assert "problems" not in err and "  next: git push origin HEAD:redesign/stream-sections" in err


def test_a_rerun_skips_fast_fail_on_the_same_tree_content_and_tells_heavy(world, capsys):
    """T535: fast-fail ran again on every resume. T666 item 5: the pass is keyed on the tree's CONTENT, not on HEAD —
    a commit that changes no content (or the same tree under a new HEAD) does not rerun it; any edit does."""
    world["fail"] = {"upload"}
    assert _staging() == 1 and world["fast_fails"] == 1
    assert world["calls"][0] == ("build", {"FAST_FAIL_DONE": "abc123"})
    world["fail"] = set()
    R.git_head = lambda: "def456"                             # a new HEAD over the same content: not rerun
    try:
        assert _staging() == 0
        assert world["fast_fails"] == 1 and "fast-fail passed for this tree's content (key-t1" in capsys.readouterr().out
        done = json.loads(R.LEDGER.read_text())["fast_fail"]
        assert (done["head"], done["key"]) == ("abc123", "key-t1")
        world["tree"] = "t2"                                  # an edit, committed or not: it runs again
        assert _staging() == 0 and world["fast_fails"] == 2
        assert json.loads(R.LEDGER.read_text())["fast_fail"]["key"] == "key-t2"
    finally:
        R.git_head = lambda: "abc123"


def test_after_a_pass_only_what_changed_since_is_checked(world, capsys):
    """T666 item 5 (build 27: the fixtures re-cut was a new commit and fast-fail ran its whole 8 minutes again): the
    base is the commit it last passed on, so a fixtures or docs commit reruns no test tier."""
    world["commits"] = {"abc123", "c0ffee"}
    R.save_ledger({"releases": {LIVE: {"commit": "c0ffee"}}})
    assert _staging() == 0 and world["fast_fail_base"] == "c0ffee"            # first pass: everything since the ship
    world["tree"], world["changed_since"] = "t2", {"app/fixtures/golden/taps.json"}
    capsys.readouterr()
    assert _staging() == 0 and world["fast_fail_base"] == "abc123"
    assert "fast-fail base: abc123, the commit it last passed on" in capsys.readouterr().out
    # a file that was uncommitted at that pass and is back at the commit's content was never checked there: ship base
    led = json.loads(R.LEDGER.read_text())
    led["fast_fail"]["dirty"] = ["app/x.ts"]
    R.LEDGER.write_text(json.dumps(led))
    world["tree"] = "t3"
    assert _staging() == 0 and world["fast_fail_base"] == "abc123"            # NEW is live now: the commit it was built from
    out = capsys.readouterr().out
    assert "1 file(s) uncommitted at its last pass are back at abc123's content (app/x.ts)" in out
    assert f"fast-fail base: abc123, the commit {NEW} was built from" in out
    world["tree"], world["changed_since"] = "t4", {"app/x.ts", "docs.md"}     # still different from that commit: usable
    led = json.loads(R.LEDGER.read_text())
    led["fast_fail"]["dirty"] = ["app/x.ts"]
    R.LEDGER.write_text(json.dumps(led))
    assert _staging() == 0 and world["fast_fail_base"] == "abc123"
    assert "the commit it last passed on" in capsys.readouterr().out


def test_red_ci_stops_at_preflight_and_the_escape_prints_its_reason(world, capsys):
    """T533: CI was red from 10-29 to 11-09 and no release asked GitHub."""
    world["ci"] = (1, "ci-status: STOP  pipeline-ci.yml: RED on abc123 (failure): https://github.com/x/runs/7")
    assert _staging() == 1
    io = capsys.readouterr()
    assert "STOPPED at preflight" in io.err and "CI is red" in io.err and "runs/7" in io.out
    assert world["calls"] == [] and world["fast_fails"] == 1      # (T666 item 3) the rest still ran
    world["ci"] = (2, "ci-status: STOP  app-ci.yml: CANNOT TELL — gh: not logged in")
    assert _staging() == 1 and "could not read CI" in capsys.readouterr().err
    world["ci"] = (0, "ci-status: SKIPPED (not asked): CI down for T999")
    with pytest.raises(SystemExit):
        _staging(no_ci_check=" ")
    assert _staging(no_ci_check="CI down for T999") == 0
    assert world["ci_skips"][-1] == "CI down for T999"
    assert _env_of(world, "upload")["CI_CHECK_SKIP"] == "release --no-ci-check: CI down for T999"


def test_fast_fail_checks_everything_since_the_commit_latest_was_built_from(world, capsys):
    """T533: with HEAD pushed, fast-fail's "changed" (not on origin) was nothing; the base is now LATEST's commit."""
    R.save_ledger({"releases": {LIVE: {"commit": "c0ffee"}}})
    world["commits"] = {"c0ffee"}
    assert _staging() == 0 and world["fast_fail_base"] == "c0ffee"
    assert "fast-fail base: c0ffee, the commit r-live00000000 was built from" in capsys.readouterr().out


def test_no_recorded_base_is_said_not_hidden(world, capsys):
    R.save_ledger({"releases": {LIVE: {"commit": "gone99"}}})          # recorded, but not in this clone
    assert _staging() == 0 and world["fast_fail_base"] is None
    assert "fast-fail base: no commit recorded for r-live00000000" in capsys.readouterr().out


def test_no_fast_fail_reaches_heavy_with_its_reason(world):
    assert _staging(no_fast_fail="tsc is down for the T999 refactor") == 0
    assert world["calls"][0] == ("build", {"HEAVY_NO_FAST_FAIL": "release --no-fast-fail: tsc is down for the T999 refactor"})


def test_the_dry_run_runs_the_site_gate_and_the_app_gate(world, capsys):
    """T535: the dry run omitted the site gate the real preflight stops on."""
    world["site_problems"] = ["the staging site is app 0.6.9, older than 0.7.0"]
    world["extra"] = {"salmon": {"url": "x", "format": "salmon/4"}}   # in the candidate the build's dry run names
    assert R.main(["--staging", "--dry-run", "--min-app-version", "0.7.0"]) == 0
    out = capsys.readouterr().out
    assert "WOULD STOP: the staging site is app 0.6.9" in out and world["site_min_apps"] == ["0.7.0"]
    assert f"app-version gate on {LIVE} (min app 0.7.0): ok" in out
    world["site_problems"] = []
    assert R.main(["--staging", "--dry-run", "--min-app-version", "0.4.0"]) == 0
    out = capsys.readouterr().out
    assert "ok: site serves app 9.9.9" in out and f"app-version gate on {NEW} (min app 0.4.0): WOULD STOP: salmon/4" in out


def test_no_fast_fail_needs_a_reason_and_prints_it(world, capsys):
    world["fast_fail_rc"] = 1
    with pytest.raises(SystemExit):
        _staging(no_fast_fail=" ")
    assert world["fast_fails"] == 0 and world["calls"] == []
    assert _staging(no_fast_fail="tsc is down for the T999 refactor") == 0
    assert "fast-fail SKIPPED (--no-fast-fail): tsc is down for the T999 refactor" in capsys.readouterr().out
    assert world["fast_fails"] == 0


def test_an_unpushed_head_stops_at_preflight_not_at_the_upload(world, capsys):
    """T484: found at stage 6 on 2026-11-07 (after the build, checks and promote); it belongs in preflight."""
    world["pushed"] = False
    assert _staging() == 1
    err = capsys.readouterr().err
    assert "STOPPED at preflight" in err and "not on origin/redesign/stream-sections" in err and "git push" in err
    assert world["calls"] == [] and _latest(world) == LIVE


def test_marks_that_have_not_landed_stop_at_preflight_even_without_fast_fail(world, capsys):
    """T545: two owner marks read "saved" and were in no curated file; no release looked."""
    world["landed"] = (False, "review marks: 1 NOT LANDED in the curated files:\n  would dfo dfo|2:some-river:x|: y")
    for kw in ({}, {"no_fast_fail": "fast-fail is down"}):
        assert _staging(**kw) == 1
        err = capsys.readouterr().err
        assert "STOPPED at preflight" in err and "dfo|2:some-river:x|" in err and "review.apply" in err
    assert world["calls"] == [] and _latest(world) == LIVE


def test_the_baseline_is_kept_before_the_build_overwrites_the_reach_run(world):
    world["differs"] = True
    assert _staging() == 1
    base = R.HOME / "baseline" / LIVE
    assert json.loads((base / "report.json").read_text())["digest"] == "reach-live"
    assert json.loads((world["reaches"] / "report.json").read_text())["digest"] == "reach-new"
    assert world["diffs"] == 1


def test_an_accepted_diff_is_compared_by_content_not_by_its_paths(world, tmp_path, capsys):
    """caught (2026-11-09): `--accept-diff` refused a diff identical in content because its `after` path was relative
    in one and absolute in the other (the file's bytes were compared)."""
    world["differs"] = True
    assert _staging() == 1
    js = R.HOME / "checks" / NEW / "binding_diff.json"
    rep = json.loads(js.read_text())
    same = tmp_path / "same.json"                             # the same diff, written with other path strings
    same.write_text(json.dumps({**rep, "before": "data/generated/release/baseline/x", "after": "reaches/full"}))
    other = tmp_path / "other.json"                           # another diff: one more rule moved
    other.write_text(json.dumps({**rep, "rules_changed": 2, "rules": {"e::r": {"gained": 1}}}))
    capsys.readouterr()
    assert _staging(accept_diff=other) == 1 and _latest(world) == LIVE
    assert "is not this diff" in capsys.readouterr().err
    assert _staging(accept_diff=same) == 0 and _latest(world) == NEW
    assert R.diff_content_sha(same) == R.diff_content_sha(js) != R.diff_content_sha(other)


def test_a_ledger_row_from_before_content_keys_still_takes_its_own_json(world):
    world["differs"] = True
    assert _staging() == 1
    led = json.loads(R.LEDGER.read_text())
    del led["releases"][NEW]["binding_diff"]["content"]       # written by the byte-compare version
    R.LEDGER.write_text(json.dumps(led))
    assert _staging(accept_diff=R.HOME / "checks" / NEW / "binding_diff.json") == 0


def test_an_old_staging_site_stops_at_preflight_not_after_the_upload(world, capsys):
    """caught (2026-11-09): a 0.7.0 release went up under a 0.6.9 staging site; the smoke test failed after upload."""
    world["site_problems"] = ["the staging site is app 0.6.9, older than 0.7.0 … deploy/staging/site.sh"]
    assert _staging(min_app_version="0.7.0") == 1
    err = capsys.readouterr().err
    assert "STOPPED at preflight" in err and "site.sh" in err
    assert world["calls"] == [] and _latest(world) == LIVE
    assert world["site_min_apps"] == ["0.7.0"]
    world["site_problems"] = []
    assert _staging() == 0 and world["site_min_apps"][-1] == "0.7.0"     # (T666 item 4) remembered from the first run


def test_the_live_stamp_is_the_min_app_when_none_was_ever_given(world):
    assert _staging() == 0 and world["site_min_apps"][-1] == "0.5.0"


def test_keep_baseline_copies_once_and_says_when_the_run_is_gone(world):
    kept, msg = R.keep_baseline(LIVE)
    assert kept and "copied" in msg
    assert json.loads((R.HOME / "baseline" / LIVE / "report.json").read_text())["digest"] == "reach-live"
    _reach(world["reaches"], "reach-other")                  # overwritten after the copy: the copy stands
    assert R.keep_baseline(LIVE) == (True, f"kept: {R.HOME / 'baseline' / LIVE} (reach reach-live)")
    import shutil
    shutil.rmtree(R.HOME / "baseline")
    kept, msg = R.keep_baseline(LIVE)
    assert not kept and "reach-other" in msg and "no copy" in msg


def test_no_baseline_and_a_moved_reach_run_stops_with_a_plain_message(world, capsys):
    _reach(world["reaches"], "reach-other")
    assert _staging() == 1
    err = capsys.readouterr().err
    assert "STOPPED at baseline" in err and "--baseline" in err and world["calls"] == []


# ------------------------------------------------------------------------------- T666: what follows the release

def test_a_renumber_only_recut_is_committed_with_the_release_id_and_pushed_when_asked(world, capsys):
    """T666 item 2 / T667 D5 (build 27: the goldens could only be re-cut AFTER the promote, by hand, as a new commit):
    the release re-cuts on the CANDIDATE and commits it; with --push it pushes its own commits and goes on."""
    world["recut_changed"] = ["app/fixtures/golden/taps.json", "app/packages/data/dev/regs-release/r-x/manifest.json",
                              "app/packages/data/dev/section-lines.json"]
    assert R.main(["--staging", "--push"]) == 0
    out = capsys.readouterr().out
    assert world["recuts_committed"] == [(NEW, "renumber only")] and world["pushes"] == 1
    assert "the re-cut changed 3 path(s): goldens 1, regs-fixture 2" in out
    assert "pushed the 1 commit(s) the release made (--push)" in out and _latest(world) == NEW
    names = [c for c, _ in world["calls"]]
    assert names.index("recut") < names.index("follow-py") < names.index("upload")   # before the promote and the upload
    rec = json.loads(R.LEDGER.read_text())["releases"][NEW]["follows"]
    assert rec["recut"]["kind"] == "renumber only" and rec["cut_for"] == NEW and rec["commits"] == ["f1xture" + "0" * 33]
    assert R.recut_cmd(NEW)[:3] == ["node", "app/tools/regs-recut/sync.mjs", "--release"]
    assert R.recut_cmd(NEW)[3] == str(world["root"] / NEW)                             # the candidate, never LATEST


def test_a_changed_golden_page_stops_uncommitted_until_its_own_token_is_accepted(world, capsys):
    """T667 D5: the release stops only when an ANSWER changed (a golden page's words)."""
    world["recut_changed"] = ["app/fixtures/golden/taps.json", "app/packages/ui-native/src/regs/fixtures/glenn_501.json"]
    assert _staging() == 1
    err = capsys.readouterr().err
    assert "STOPPED at follows" in err and "changed 1 golden page(s)" in err and "glenn_501.json" in err
    assert "nothing was committed" in err and "--accept-fixtures tok-2" in err
    assert world["recuts_committed"] == [] and _latest(world) == LIVE
    assert "follow-py" not in [c for c, _ in world["calls"]]                # no test ran on an unaccepted re-cut
    assert _staging(accept_fixtures="tok-9") == 1                           # another re-cut's token is not this one
    assert "--accept-fixtures tok-9 is not this re-cut" in capsys.readouterr().err and world["recuts_committed"] == []
    assert _staging(accept_fixtures="tok-2") == 1                           # accepted and committed: the push gate
    assert world["recuts_committed"] == [(NEW, "golden pages accepted")]
    assert "1 the release made" in capsys.readouterr().err
    world["pushed"], world["unpushed"] = True, []
    assert _staging() == 0 and _latest(world) == NEW


def test_push_never_pushes_a_commit_the_release_did_not_make(world, capsys):
    world["recut_changed"] = ["app/fixtures/golden/taps.json"]
    world["unpushed"] = ["0ther" + "0" * 35]                                 # another lane committed during the build
    assert R.main(["--staging", "--push"]) == 1
    err = capsys.readouterr().err
    assert world["pushes"] == 0 and "1 made by someone else since preflight (0ther0000000)" in err
    assert "--push pushes only the release's own commits" in err and _latest(world) == LIVE


def test_the_tests_that_ask_for_the_release_run_on_the_candidate_before_the_promote(world, capsys):
    """T666 item 2 (build 27: 13 tests compared with LATEST and were red from the build to the promote)."""
    world["rc"] = {"follow-py": 1, "follow-app": 1}
    assert _staging() == 1
    err = capsys.readouterr().err
    assert "STOPPED at follows" in err and "pipeline: rc=1" in err and "app: rc=1" in err and "LATEST was not moved" in err
    assert "2 failure(s) on the candidate" in err and "(no failed test in its report" in err   # no report: the rc stands
    assert _latest(world) == LIVE and "upload" not in [c for c, _ in world["calls"]]
    for name in ("follow-py", "follow-app", "follow-worker", "app-gate"):    # every check ran: one stop, all named
        env = _env_of(world, name)
        assert env["RELEASE_DIR"] == str(world["root"] / NEW) and env["REGS_STORE_REQUIRED"] == "1"
    assert "pipeline/tests/test_x.py" in [c for c in R.follow_tests_cmds(R.follow_tests()[0], R.HOME)[0][1]]
    with pytest.raises(SystemExit):
        _staging(no_follow_tests=" ")
    world["calls"].clear()
    assert _staging(no_follow_tests="perf suite on a loaded machine, T999") == 0
    assert "tests on the candidate SKIPPED (--no-follow-tests): perf suite on a loaded machine, T999" in capsys.readouterr().out
    assert "follow-py" not in [c for c, _ in world["calls"]]


def test_every_failure_on_the_candidate_is_listed_by_name_in_one_stop(world, capsys):
    """T680 (r-483b295de57c: the re-cut moved section 578 to 586 and changed a golden page's words; WaterMode.test.tsx
    and one-part-one-page.test.ts failed in CI one round at a time, after the push): after the re-cut the WHOLE app
    check runs on the candidate — the app suite, the worker suite, each check:fast gate — and the one stop names every
    failing test, from every suite, before anything is pushed."""
    app_json = json.dumps({"testResults": [
        {"name": str(R.REPO_ROOT / "app/packages/ui-native/src/WaterMode.test.tsx"), "status": "failed",
         "assertionResults": [{"status": "failed", "fullName": "one water screen > (T53) an unnamed reach"},
                              {"status": "passed", "fullName": "fine"}]},
        {"name": str(R.REPO_ROOT / "app/tools/one-part-one-page.test.ts"), "status": "failed",
         "assertionResults": [{"status": "failed", "fullName": "(T387) every tap on a part gives the same page"}]},
        {"name": str(R.REPO_ROOT / "app/tools/broken.test.ts"), "status": "failed", "message": "SyntaxError: x\nmore",
         "assertionResults": []}]})
    junit = ('<testsuites><testsuite><testcase classname="pipeline.tests.test_x" name="test_a"><failure/></testcase>'
             '<testcase classname="pipeline.tests.test_x" name="test_ok"/></testsuite></testsuites>')
    world["reports"] = {"follow-app": app_json, "follow-py": junit}
    world["rc"] = {"follow-py": 1, "follow-app": 1, "app-gate": 2}
    world["recut_changed"] = ["app/packages/data/dev/section-lines.json"]
    assert R.main(["--staging", "--push"]) == 1
    err = capsys.readouterr().err
    assert "5 failure(s) on the candidate" in err and "ALL of them" in err
    for line in ("pipeline: pipeline.tests.test_x::test_a",
                 "app: app/packages/ui-native/src/WaterMode.test.tsx › one water screen > (T53) an unnamed reach",
                 "app: app/tools/one-part-one-page.test.ts › (T387) every tap on a part gives the same page",
                 "app: app/tools/broken.test.ts: SyntaxError: x", "app gate typecheck: rc=2"):
        assert line in err
    assert "test_ok" not in err and "fine" not in err
    assert world["pushes"] == 0 and _latest(world) == LIVE                  # stopped before the push gate
    names = [c for c, _ in world["calls"]]
    assert names.index("recut") < names.index("follow-py") < names.index("follow-app") < names.index("follow-worker") \
        < names.index("app-gate")
    assert "tests" not in json.loads(R.LEDGER.read_text())["releases"][NEW]["follows"]   # not recorded as passed


def test_the_app_gates_are_read_from_check_fast_one_by_one():
    """T680: each check:fast gate runs on its own (the && chain stops at the first failure), read from its one home;
    the worker typecheck is one of them (it was in no check)."""
    from pipeline import release_follows as RF
    steps = RF.app_check_steps()
    assert steps[:2] == ["tickets", "anchors"] and "typecheck" in steps and "typecheck:worker" in steps
    assert RF.app_check_cmd("anchors") == ["pnpm", "-C", "app", "run", "-s", "anchors"]


def test_the_rehearsal_recuts_runs_everything_and_puts_the_fixtures_back(world, capsys, tmp_path, monkeypatch):
    """T680: `--dry-run --rehearse-follows` on a built candidate re-cuts in place, runs every check, lists every
    failure and the golden pages with the token the real run would take — and leaves the fixtures byte for byte."""
    repo = tmp_path / "repo"
    (repo / "app/fixtures/golden").mkdir(parents=True)
    (repo / "app/fixtures/golden/taps.json").write_text("before")
    pages = repo / "app/packages/ui-native/src/regs/fixtures"
    pages.mkdir(parents=True)
    (pages / "glenn_501.json").write_text("old words")
    monkeypatch.setattr(R, "REPO_ROOT", repo)
    real = R.run_cmd

    def run_cmd(cmd, env=None):
        rc = real(cmd, env)
        if any("regs-recut/sync.mjs" in c for c in cmd):               # the re-cut writes in place
            (repo / "app/fixtures/golden/taps.json").write_text("after")
            (pages / "glenn_501.json").write_text("new words")
            (pages / "new_1.json").write_text("+")
        return rc
    monkeypatch.setattr(R, "run_cmd", run_cmd)
    # what differs from "HEAD", read from the disk (so the put-back is seen)
    monkeypatch.setattr(R, "recut_changed", lambda: [p for p, head in (
        ("app/fixtures/golden/taps.json", "before"), ("app/packages/ui-native/src/regs/fixtures/glenn_501.json",
                                                     "old words")) if (repo / p).read_text() != head])
    assert R.main(["--staging"]) == 1 and "STOPPED at follows" in capsys.readouterr().err   # built: a CANDIDATE
    world["calls"].clear()
    (repo / "app/fixtures/golden/taps.json").write_text("before")      # the stopped real run's re-cut, undone
    (pages / "glenn_501.json").write_text("old words")
    (pages / "new_1.json").unlink()
    world["rc"] = {"follow-worker": 1}
    with pytest.raises(SystemExit):
        R.main(["--staging", "--rehearse-follows"])                   # a rehearsal only
    assert R.main(["--staging", "--dry-run", "--rehearse-follows"]) == 0
    out = capsys.readouterr().out
    assert "the re-cut would change 2 path(s)" in out and "1 golden page(s) change" in out and "--accept-fixtures tok-2" in out
    assert "1 failure(s) on the candidate" in out and "worker: rc=1" in out
    names = [c for c, _ in world["calls"]]
    assert names.count("recut") == 1 and {"follow-py", "follow-app", "follow-worker", "app-gate"} <= set(names)
    assert (repo / "app/fixtures/golden/taps.json").read_text() == "before"            # put back, byte for byte
    assert (pages / "glenn_501.json").read_text() == "old words" and not (pages / "new_1.json").exists()
    assert world["recuts_committed"] == [] and world["pushes"] == 0 and _latest(world) == LIVE
    assert "WARNING" not in out


def test_fast_fail_reads_the_candidate_once_the_fixtures_are_cut_for_it(world):
    """Between the re-cut and the promote the goldens pair with the CANDIDATE: a rerun's fast-fail is told so
    (RELEASE_DIR), and not before the re-cut (they still pair with LATEST then)."""
    world["rc"] = {"inseason": 3}
    assert _staging() == 1 and world["fast_fail_env"] == {}                  # no candidate yet
    world["tree"] = "t2"
    assert _staging() == 1 and world["fast_fail_env"] == {}                  # a candidate, fixtures not re-cut on it
    world["rc"], world["recut_changed"], world["tree"] = {}, ["app/fixtures/golden/taps.json"], "t3"
    assert _staging() == 1                                                   # re-cut + committed: the push gate
    world["pushed"], world["unpushed"] = True, []
    assert _staging() == 0
    assert world["fast_fail_env"] == {"RELEASE_DIR": str(world["root"] / NEW)}
    assert json.loads(R.LEDGER.read_text())["fast_fail"]["release_dir"] == str(world["root"] / NEW)


def test_the_command_remembers_its_flags_and_a_rerun_needs_none(world, tmp_path, capsys):
    """T666 item 4 (build 27: `--baseline` / `--accept-diff` had to be retyped, and were wrong after the promote)."""
    kept = tmp_path / "full_prev"
    _reach(kept, "reach-live")
    _reach(world["reaches"], "reach-other")                  # the live run was overwritten before this release began
    world["differs"] = True
    assert _staging(baseline=kept) == 1                      # the diff stops it
    assert json.loads((R.HOME / "baseline" / LIVE / "report.json").read_text())["digest"] == "reach-live"   # kept
    js = R.HOME / "checks" / NEW / "binding_diff.json"
    world["fail"] = {"upload"}
    assert _staging(accept_diff=js, min_app_version="0.6.0") == 1 and _latest(world) == NEW
    run = json.loads(R.LEDGER.read_text())["run"]
    assert run["promoted"] == NEW and run["flags"] == {"min_app": "0.6.0"}     # the promote spent the other two
    world["fail"] = set()
    capsys.readouterr()
    assert _staging() == 0                                   # the bare command
    out = capsys.readouterr().out
    assert "remembered from an earlier run of this release" in out and "--min-app-version 0.6.0" in out
    assert _env_of(world, "smoke") is None and [u["MIN_APP_VERSION"] for c, u in world["calls"] if c == "upload"] == ["0.6.0"] * 2
    assert "run" not in json.loads(R.LEDGER.read_text())     # on staging: the next release starts clean


def test_a_baseline_flag_left_over_from_before_the_promote_is_ignored_out_loud(world, tmp_path, capsys):
    kept = tmp_path / "full_prev"
    _reach(kept, "reach-live")
    world["fail"] = {"upload"}
    assert _staging(baseline=kept) == 1 and _latest(world) == NEW
    world["fail"] = set()
    capsys.readouterr()
    assert _staging(baseline=kept) == 0                      # the first run's command line, typed again
    assert "left over from before the promote, IGNORED" in capsys.readouterr().out
    world2 = tmp_path / "wrong"
    _reach(world2, "reach-wrong")
    R.save_ledger({"releases": {}})
    (world["root"] / "LATEST").write_text(LIVE + "\n")       # a first run with a wrong baseline is still refused
    _reach(world["reaches"], "reach-live")
    assert _staging(baseline=world2) == 1 and "STOPPED at baseline" in capsys.readouterr().err


def test_forget_flags_drops_what_was_remembered(world, capsys):
    world["extra"] = {"salmon": {"url": "x", "format": "salmon/4"}}
    assert _staging(min_app_version="0.4.0") == 1
    capsys.readouterr()
    assert R.main(["--staging", "--forget-flags"]) == 0      # back to the live stamp (0.5.0)
    assert "--forget-flags: dropped --min-app-version" in capsys.readouterr().out


def test_an_escape_is_never_remembered(world, capsys):
    world["fast_fail_rc"] = 1
    world["fail"] = {"upload"}
    assert _staging(no_fast_fail="tsc is down for the T999 refactor") == 1 and world["fast_fails"] == 0
    assert json.loads(R.LEDGER.read_text())["run"]["flags"] == {}
    assert _staging() == 1 and world["fast_fails"] == 1      # the bare rerun runs the guard again
    assert "STOPPED at preflight" in capsys.readouterr().err


def test_a_workflow_that_would_not_start_stops_saying_only_the_upload_is_left(world, capsys):
    """T666 item 6 (build 27: GitHub refused the start and the only retry was the whole command)."""
    world["rc"] = {"upload": R.RC_WORKFLOW_NOT_STARTED}
    assert _staging() == 1
    err = capsys.readouterr().err
    assert "STOPPED at staging" in err and "did not start the run-timing workflow" in err and "only the upload runs" in err
    assert _latest(world) == NEW and "uploaded_at" not in json.loads(R.LEDGER.read_text())["releases"][NEW]
    world["rc"] = {}
    world["calls"].clear()
    assert _staging() == 0
    assert [c for c, _ in world["calls"]] == ["build", "upload", "smoke"] and world["fast_fails"] == 1
    up = (Path(R.REPO_ROOT) / "deploy/staging/upload.sh").read_text()
    assert "scripts/start-workflow.sh" in up and "gh workflow run" not in up       # the one start, with its retry
    assert 'exit "$rc"' in up                                                      # its 75 reaches the release


def test_the_uploads_first_refusals_are_asked_before_it_starts(world, capsys):
    world["staging_prechecks"] = ["`gh auth status` failed (rc=1)", "the rclone remote `r2:` is not configured"]
    assert _staging() == 1
    err = capsys.readouterr().err
    assert "[7/7] staging FAILED: 2 problems" in err and "gh auth status" in err and "rclone remote" in err
    assert "upload" not in [c for c, _ in world["calls"]] and _latest(world) == NEW


def test_the_rehearsal_walks_every_stages_checks_and_lists_the_stops(world, capsys):
    """T666 item 6: `--dry-run` predicts the stops. It reads the SAME preflight table as the real run."""
    assert R.main(["--staging", "--dry-run"]) == 0           # before the build: preflight, baseline, the build's plan
    out = capsys.readouterr().out
    assert "REHEARSAL" in out and "rehearsal: no stop found before the build" in out
    assert world["fast_fails"] == 1 and "ok: fast-fail passed" in out          # fast-fail is a check: it ran
    assert json.loads(R.LEDGER.read_text())["fast_fail"]["key"] == "key-t1"     # … and the real run will skip it
    assert [c for c, _ in world["calls"]] == ["build"] and _latest(world) == LIVE   # the build's own --dry-run only
    assert "run" not in json.loads(R.LEDGER.read_text())                        # a rehearsal remembers no flag
    # the mock's build dry run left a CANDIDATE: now every later stage is rehearsed on it
    world["dirty"], world["pushed"] = ["pipeline/build.py"], False
    world["manifest_problems"] = ["files.water: missing"]
    world["inseason_check"] = ("unresolved", "in-season matches on r-new: WOULD CHANGE, 1 unresolved")
    world["goldens_paired"], world["rc"] = False, {"taps": 1}
    world["staging_prechecks"] = ["the rclone remote `r2:` is not configured"]
    world["calls"].clear()
    assert R.main(["--staging", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert world["fast_fails"] == 1                                             # same tree: not rerun
    for said in ("WOULD STOP: 1 uncommitted path(s) the build reads: pipeline/build.py",
                 "WOULD STOP: HEAD abc123 is not on origin/redesign/stream-sections",
                 f"manifest sanity on {NEW}: WOULD STOP: files.water: missing",
                 "WOULD STOP: reach run on disk is reach-new and no copy is kept (--baseline DIR)",
                 "binding diff: not rehearsed: no baseline copy is kept",
                 f"in-season matches: WOULD STOP: a match does not resolve on {NEW}",
                 f"fixtures: WOULD STOP: the re-cut's first step (taps.py) fails on {NEW}",
                 f"the upload's first refusals, on {NEW}: WOULD STOP: the rclone remote",
                 "rehearsal: the real run would STOP 7 time(s) as things are:"):
        assert said in out, said
    assert [c for c, _ in world["calls"]] == ["build", "taps"]                  # nothing re-cut, committed or sent
    assert _env_of(world, "taps") is None and _latest(world) == LIVE and world["recuts_committed"] == []
    assert "follows" not in json.loads(R.LEDGER.read_text()).get("releases", {}).get(NEW, {})


def test_the_rehearsal_and_the_run_read_one_preflight_table(world, capsys):
    """By construction: a check added to PREFLIGHT is in both. Every check's failure shows in the rehearsal with the
    words the real run stops with."""
    assert [n for n, _ in R.PREFLIGHT] == ["committed", "atlas", "marks", "pushed", "ci", "live", "site", "fast-fail",
                                           "fresh-copy"]
    world["pushed"], world["site_problems"], world["fast_fail_rc"] = False, ["site 0.6.9 < 0.7.0"], 1
    assert _staging() == 1
    real = capsys.readouterr()
    assert R.main(["--staging", "--dry-run"]) == 0
    rehearsed = capsys.readouterr().out
    for line in [ln.strip()[len("STOP: "):] for ln in real.out.splitlines() if ln.strip().startswith("STOP: ")]:
        assert "WOULD STOP: " + line in rehearsed, line
    assert real.out.count("    STOP: ") == 3 and "rehearsal: the real run would STOP 3 time(s)" in rehearsed


def test_the_fresh_copy_test_run_happens_once_in_the_release_pre_check(world, capsys):
    """T666, owner 2026-11-11: the clean-checkout run is the release pre-check's, once — not every quick check's. It
    is not repeated on a resume, nor after the release's own fixtures / matches commits; another commit runs it again."""
    world["fail"] = {"upload"}
    assert _staging() == 1 and world["fresh_copies"] == 1
    assert "ok: fresh-copy test run passed on a clean clone of HEAD abc123" in capsys.readouterr().out
    assert json.loads(R.LEDGER.read_text())["fresh_copy"] == {
        "tree": "committed-t1", "head": "abc123", "at": json.loads(R.LEDGER.read_text())["fresh_copy"]["at"]}
    assert _staging() == 1 and world["fresh_copies"] == 1                      # a resume: same tree
    world["commits"] = {"abc123"}
    world["tree"] = "t2"                                                       # the release's own re-cut commit
    world["committed_since"] = {"app/fixtures/golden/taps.json", "app/packages/ui-native/src/regs/fixtures/a.json",
                                "data/curated/inseason/notice_waters.json"}
    capsys.readouterr()
    assert _staging() == 1 and world["fresh_copies"] == 1
    assert "the commits since touch only what the release refreshes (fixtures, matches): not rerun" in capsys.readouterr().out
    world["committed_since"] |= {"pipeline/deliver/x.py"}                      # a code commit: it runs again
    assert _staging() == 1 and world["fresh_copies"] == 2
    assert "1 path(s) committed since its pass at abc123 (pipeline/deliver/x.py): it runs again" in capsys.readouterr().out


def test_a_failed_fresh_copy_stops_before_the_build_and_the_escape_prints_its_reason(world, capsys):
    world["fresh_copy_rc"] = 1
    assert _staging() == 1
    err = capsys.readouterr().err
    assert "STOPPED at preflight" in err and "on a clean clone of HEAD abc123" in err and "--no-fresh-copy REASON" in err
    assert world["calls"] == [] and "fresh_copy" not in json.loads(R.LEDGER.read_text())
    with pytest.raises(SystemExit):
        _staging(no_fresh_copy=" ")
    assert _staging(no_fresh_copy="GitHub CI ran the same tier green on this commit") == 0
    assert "fresh-copy test run SKIPPED (--no-fresh-copy): GitHub CI ran the same tier green" in capsys.readouterr().out
    assert world["fresh_copies"] == 1
    assert R.run_fresh_copy.__name__ == "fresh_copy"


def test_the_fresh_copy_runs_ci_s_tier_on_a_clone_through_heavy(monkeypatch):
    seen = []
    monkeypatch.setattr(R, "run_cmd", lambda cmd, env=None: seen.append(cmd) or 0)
    assert R.run_fresh_copy() == 0
    assert seen == [["bash", "scripts/heavy.sh", "release-fresh-copy", "30", "--", "bash", "scripts/fresh-copy-tests.sh"]]
    sh = (Path(R.REPO_ROOT) / "scripts/fresh-copy-tests.sh").read_text()
    assert "git clone -q --shared --no-checkout" in sh and "scripts/pytest-chunked.sh" in sh
    assert 'pipeline-ci.yml' in sh and "ci_no_build" not in sh                 # CI's own markers; no build to hide


# ------------------------------------------------------------------------------------------------------------- prod

@pytest.fixture
def staged(world, monkeypatch):
    assert _staging() == 0
    world["calls"].clear()
    for k in ("CLAUDECODE", "AI_AGENT", "CLAUDE_CODE_ENTRYPOINT", "CONFIRM_PROD"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(R, "is_tty", lambda: True)
    return world


def test_prod_refuses_without_the_human_confirmation(staged, capsys):
    assert R.prod(NEW, ask=lambda _: NEW) == 2
    err = capsys.readouterr().err
    assert "CONFIRM_PROD is not" in err and "deploy/prod/upload.sh" in err
    assert staged["calls"] == []


def test_prod_refuses_an_agent_even_with_the_confirmation(staged, monkeypatch, capsys):
    monkeypatch.setenv("CONFIRM_PROD", NEW)
    monkeypatch.setenv("CLAUDECODE", "1")
    assert R.prod(NEW, ask=lambda _: NEW) == 2
    assert "agent environment" in capsys.readouterr().err and staged["calls"] == []


def test_prod_refuses_without_a_terminal_or_the_typed_id(staged, monkeypatch):
    monkeypatch.setenv("CONFIRM_PROD", NEW)
    monkeypatch.setattr(R, "is_tty", lambda: False)
    assert R.prod(NEW, ask=lambda _: NEW) == 2
    monkeypatch.setattr(R, "is_tty", lambda: True)
    assert R.prod(NEW, ask=lambda _: "r-typo") == 2
    assert staged["calls"] == []


def test_prod_ships_only_a_checked_staged_unchanged_release(staged, monkeypatch):
    monkeypatch.setenv("CONFIRM_PROD", LIVE)
    assert R.prod(LIVE, ask=lambda _: LIVE) == 1              # never through release --staging
    monkeypatch.setenv("CONFIRM_PROD", NEW)
    (staged["root"] / NEW / "water.bin.gz").write_bytes(b"changed")
    assert R.prod(NEW, ask=lambda _: NEW) == 1                # not the artifact that was checked
    assert staged["calls"] == []


def test_prod_runs_the_prod_upload_for_a_human(staged, monkeypatch):
    monkeypatch.setenv("CONFIRM_PROD", NEW)
    # the stamp the staging upload adds does not change the artifact
    m = json.loads((staged["root"] / NEW / "manifest.json").read_text())
    (staged["root"] / NEW / "manifest.json").write_text(json.dumps({**m, "published": "2026-11-05"}))
    assert R.prod(NEW, ask=lambda _: NEW) == 0
    # (T404) the one production upload, deploy/prod/upload.sh, given its own confirmation only after release --prod's
    assert staged["calls"] == [("upload", {"CONFIRM_PROD": "canifishthis.ca"})]
    assert R.prod_cmd(NEW) == ["bash", "deploy/prod/upload.sh", NEW]


def test_prod_refuses_when_staging_serves_another_release(staged, monkeypatch, capsys):
    """T537: deploy/prod/upload.sh promotes what staging serves; a later release --staging replaced it silently."""
    monkeypatch.setenv("CONFIRM_PROD", NEW)
    staged["staging_serves"] = "r-other0000000"
    assert R.prod(NEW, ask=lambda _: NEW) == 1
    assert "staging serves r-other0000000, not r-new000000000" in capsys.readouterr().err and staged["calls"] == []
    assert R.main(["--prod", NEW, "--dry-run"]) == 0
    assert "WOULD STOP: staging serves r-other0000000" in capsys.readouterr().out

    def unreadable():
        raise OSError("offline")
    monkeypatch.setattr(R, "staging_release", unreadable)
    assert R.prod(NEW, ask=lambda _: NEW) == 1 and "cannot read what staging serves" in capsys.readouterr().err


def test_a_failed_inseason_resolve_stops_before_the_promote(world, capsys):
    """T537: the resolve ran mid-upload, after LATEST moved; a failed match now stops at the checks."""
    world["fail"] = {"inseason"}
    assert _staging() == 1
    err = capsys.readouterr().err
    assert "STOPPED at follows" in err and "do not all resolve" in err and "LATEST was not moved" in err
    assert _latest(world) == LIVE and "upload" not in [c for c, _ in world["calls"]]


def test_a_rewritten_inseason_file_is_committed_alone_and_the_run_stops_once_for_the_push(world, capsys):
    """T537, and T666: the stop comes at the END of the follows stage (after the re-cut and the tests), so the matches
    commit and the fixtures commit are one push, not two stops."""
    world["inseason_dirty"] = True
    world["recut_changed"] = ["app/fixtures/golden/taps.json"]
    assert _staging() == 1
    io = capsys.readouterr()
    assert world["commits_made"] == [NEW] and "committed c0mmit" in io.out
    assert world["recuts_committed"] == [(NEW, "renumber only")]
    assert "STOPPED at follows" in io.err and "2 commit(s) are not on origin/redesign/stream-sections: 2 the release made" in io.err
    assert "next: git push origin HEAD:redesign/stream-sections, then rerun" in io.err and "--push" in io.err
    assert _latest(world) == LIVE and [c for c, _ in world["calls"]] == ALL[:7]
    world["inseason_dirty"], world["pushed"], world["unpushed"] = False, True, []      # the human pushed
    assert _staging() == 0                                     # resolved, re-cut and tested for NEW: none run again
    names = [c for c, _ in world["calls"]]
    assert (names.count("inseason"), names.count("recut"), names.count("follow-py"), names.count("follow-app")) == (1, 1, 1, 1)
    assert world["pushes"] == 0 and _latest(world) == NEW


def test_a_smoke_failure_reruns_only_the_smoke_and_the_chores_are_named(world, capsys):
    world["fail"] = {"smoke"}
    assert _staging() == 1
    io = capsys.readouterr()
    assert "STOPPED at staging" in io.err and "only the smoke runs again" in io.err
    rec = json.loads(R.LEDGER.read_text())["releases"][NEW]
    assert rec["uploaded_at"] and "staged_at" not in rec
    world["fail"] = set()
    world["calls"].clear()
    assert _staging() == 0
    assert [c for c, _ in world["calls"]] == ["build", "smoke"]                  # no second upload
    out = capsys.readouterr().out
    assert "only the smoke runs" in out and f"after the release: chores for {NEW}" in out
    rec = json.loads(R.LEDGER.read_text())["releases"][NEW]
    assert rec["smoked_at"] == rec["staged_at"]


def test_replacing_a_release_handed_for_prod_is_said(world, capsys):
    R.save_ledger({"releases": {LIVE: {"staged_at": "2026-11-09T17:57:16", "commit": "x"}}})
    assert _staging() == 0
    assert f"this replaces {LIVE} on staging" in capsys.readouterr().out
    assert json.loads(R.LEDGER.read_text())["releases"][LIVE]["replaced_by"] == NEW


def test_after_chores_name_the_exact_commands():
    text = R.after_chores("r-x")
    assert "goldens:" in text and "CONFIRM_PROD=r-x python -m pipeline release --prod r-x" in text
    assert "deploy/staging/site-all.sh" in text and "deploy/staging/feeds.sh" in text


# ----------------------------------------------------------------------------------------------- the pieces it uses

def test_the_build_holds_latest_when_asked(monkeypatch, tmp_path):
    atlas = tmp_path / "atlas" / "full"                       # the build dir is only named in the commands
    monkeypatch.setattr(B, "HOLD_LATEST", True)
    assert B.commands(atlas)["deliver"][-1] == "--hold-latest"
    monkeypatch.setattr(B, "HOLD_LATEST", False)
    assert "--hold-latest" not in B.commands(atlas)["deliver"]
    assert R.build_cmd()[:5] == ["bash", "scripts/heavy.sh", "release", str(R.HEAVY_MIN_FREE), "--"]
    assert "--hold-latest" in R.build_cmd()


def test_prune_keeps_the_candidate(tmp_path):
    from pipeline.deliver.release import prune
    from pipeline.deliver.release.manifest import write_candidate, write_latest
    for i, rid in enumerate(("r-a", "r-b", "r-c", "r-d")):
        (tmp_path / rid).mkdir()
        (tmp_path / rid / "manifest.json").write_text("{}")
    write_latest(tmp_path, "r-d")
    write_candidate(tmp_path, "r-a")
    kept, _gone = prune.plan(tmp_path)
    assert "r-a" in {d.name for d in kept} and (tmp_path / "LATEST").read_text().strip() == "r-d"


def test_the_app_gate_is_the_uploads_rule():
    from pipeline.deliver.release.app_gate import problems
    m = {"files": {"salmon": {"format": "salmon/4"}, "paint": {"format": "paint/5"}}}
    assert problems(m, "0.5.0", APPS) == []
    assert problems(m, "0.4.0", APPS) == ["salmon/4 needs app >= 0.5.0 but the oldest app allowed is 0.4.0; "
                                          "rerun with MIN_APP_VERSION=0.5.0 (or newer)"]
    assert len(problems(m, None, APPS)) == 2                  # no stamp, no MIN_APP_VERSION: refused
    assert "app_gate" in (Path(R.REPO_ROOT) / "deploy/staging/upload.sh").read_text()


# ------------------------------------------------------------------------------- the staging site's version (T483)

def _site(index: str, bundle: str):
    def fetch(url, timeout=30):
        if url.endswith("/"):
            return index
        if "/_expo/static/js/web/index-" in url:
            return bundle
        raise AssertionError(url)
    return fetch


IDX = '<script src="/_expo/static/js/web/index-5524ce5c42e7529fe22800c74906f952.js" defer></script>'
JS = 'x;const b=(0,n.resolveAppVersion)({version:"0.6.9",build:"19",sha:"13b31d3f1ddb",date:"2026-11-08"});y'


def test_the_site_gate_reads_the_live_bundles_version_stamp():
    from pipeline.deliver.release import site_gate as SG
    v, how = SG.site_version("https://staging.example", _site(IDX, JS))
    assert v == "0.6.9" and "build 19" in how
    assert SG.site_version("https://s", _site("<html></html>", JS))[0] is None
    assert SG.site_version("https://s", _site(IDX, "no stamp here"))[0] is None

    def down(url, timeout=30):
        raise OSError("offline")
    v, how = SG.site_version("https://s", down)
    assert v is None and "offline" in how


def test_the_site_gate_refuses_a_site_older_than_the_release_and_names_the_redeploy(monkeypatch):
    from pipeline.deliver.release import site_gate as SG
    monkeypatch.delenv("SITE_CHECK_SKIP", raising=False)
    m = {"data_release": "r-x", "minAppVersion": "0.7.0", "files": {"salmon": {"format": "salmon/4"}}}
    bad, _ = SG.check(m, None, site="https://s", fetch=_site(IDX, JS), rid="r-x")
    assert len(bad) == 1 and "0.6.9, older than 0.7.0" in bad[0]
    assert "deploy/staging/site-all.sh" in bad[0]       # (T544) one command, not three typed from memory
    assert SG.check(m, None, site="https://s", fetch=_site(IDX, JS.replace("0.6.9", "0.7.0")))[0] == []
    assert SG.check(m, "0.6.0", site="https://s", fetch=_site(IDX, JS))[0] == []    # MIN_APP_VERSION wins
    # unstamped: the formats' own needs still hold (salmon/4 needs 0.5.0)
    assert SG.required({"files": {"salmon": {"format": "salmon/4"}}}, None) == "0.5.0"
    assert SG.check({"files": {}}, None, site="https://s", fetch=_site(IDX, JS.replace("0.6.9", "0.4.0")))[0] == []
    # an unreadable site is a refusal, never a pass; SITE_CHECK_SKIP says why it is skipped
    assert SG.check(m, None, site="https://s", fetch=_site("", ""))[0]
    monkeypatch.setenv("SITE_CHECK_SKIP", "site down for the T999 move")
    bad, said = SG.check(m, None, site="https://s", fetch=_site("", ""))
    assert bad == [] and "T999" in said


def test_the_upload_runs_the_site_gate_before_anything_is_sent():
    up = (Path(R.REPO_ROOT) / "deploy/staging/upload.sh").read_text()
    assert up.index("pipeline.deliver.release.site_gate") < up.index("rclone copy")


def test_heavy_runs_a_scripts_precheck_before_it_takes_the_lock(tmp_path):
    """caught (2026-11-09): `session.sh --build` with MLN_LOCAL_MAVEN and no TESTBED_APK_TAG failed only after waiting
    for the heavy lock; heavy.sh now runs a `heavy-precheck: yes` script's checks first, unlocked."""
    import os
    import subprocess
    lock = tmp_path / "heavy-job.lock"   # T536: only the two lane names exist; HEAVY_LANES_DIR keeps it off ~/.claude
    # (T534/T661) session.sh refuses an MLN_LOCAL_MAVEN that does not hold exactly one patched AAR: a real check (the precheck
    # finds it by name), so the experiment repo here holds one stand-in AAR
    aar = tmp_path / "m2/org/maplibre/gl/android-sdk-vulkan/0-test/android-sdk-vulkan-0-test.aar"
    aar.parent.mkdir(parents=True)
    aar.write_bytes(b"")
    env = {k: v for k, v in os.environ.items() if k not in ("TESTBED_APK_TAG", "HEAVY_HELD", "HEAVY_PRECHECK")}
    env.update(HEAVY_LOCK=str(lock), HEAVY_LANES_DIR=str(tmp_path), HEAVY_PAUSE=str(tmp_path / "no-pause"),
               MLN_LOCAL_MAVEN=str(tmp_path / "m2"))
    r = subprocess.run(["bash", "scripts/heavy.sh", "testbed", "30", "--", "bash", "app/tools/testbed/session.sh",
                        "--build", "--", "true"], cwd=R.REPO_ROOT, env=env, capture_output=True, text=True, timeout=60)
    assert r.returncode == 1, r.stdout + r.stderr
    assert "needs TESTBED_APK_TAG" in r.stdout and "the lock was not taken" in r.stderr
    assert not lock.exists()
    ok = subprocess.run(["bash", "app/tools/testbed/session.sh", "--build", "--", "false"], cwd=R.REPO_ROOT,
                        env={**env, "HEAVY_PRECHECK": "1", "TESTBED_APK_TAG": "t"}, capture_output=True, text=True,
                        timeout=60)
    assert ok.returncode == 0 and "precheck ok" in ok.stdout
