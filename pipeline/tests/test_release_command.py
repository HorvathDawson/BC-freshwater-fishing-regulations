"""`python -m pipeline release` (T295/T297, T408): the orchestration, with every heavy step mocked — the order, stop on
the first failure, resume, the binding-diff gate, and production refusing anything but a human."""
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


@pytest.fixture
def world(tmp_path: Path, monkeypatch):
    root, reaches = tmp_path / "release", tmp_path / "reaches" / "full"
    _release(root, LIVE, "reach-live")
    (root / "LATEST").write_text(LIVE + "\n")
    _reach(reaches, "reach-live")
    w = {"calls": [], "fail": set(), "dirty": [], "diffs": 0, "differs": False, "root": root, "reaches": reaches,
         "pushed": True, "fast_fail_rc": 0, "fast_fails": 0, "new_stamp": "0.5.0"}

    def run_cmd(cmd, env=None):
        name = "build" if "build" in cmd else "upload" if any("upload.sh" in c for c in cmd) else \
            "publish" if any("publish" in c for c in cmd) else " ".join(cmd)
        w["calls"].append((name, env))
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

    def fast_fail():
        w["fast_fails"] += 1
        return w["fast_fail_rc"]

    monkeypatch.setattr(R, "run_fast_fail", fast_fail)
    monkeypatch.setattr(R, "git_pushed", lambda: w["pushed"])
    monkeypatch.setattr(R, "binding_diff", diff)
    monkeypatch.setattr(R, "diff_summary", lambda rep: f"{rep['rules_changed']} rule(s) bind different sections")
    # the manifest check's real body reads the size gate and publish.plan over real release files: pinned elsewhere
    monkeypatch.setattr(R, "manifest_problems", lambda rid: list(w.get("manifest_problems", [])))
    return w


def _staging(**kw) -> int:
    return R.main(["--staging", *sum(([f"--{k.replace('_', '-')}", str(v)] for k, v in kw.items()), [])])


def _latest(w) -> str:
    return (w["root"] / "LATEST").read_text().strip()


def test_the_stages_run_in_order_and_ship_the_candidate(world, capsys):
    assert _staging() == 0
    out = capsys.readouterr().out
    marks = [out.index(f"[{i}/6] {s} ok") for i, s in enumerate(R.STAGES, 1)]
    assert marks == sorted(marks)
    assert [c for c, _ in world["calls"]] == ["build", "upload"]
    assert world["calls"][1][1] == {"MIN_APP_VERSION": "0.5.0"}       # the live release's stamp carries
    assert _latest(world) == NEW and not (world["root"] / "CANDIDATE").exists()
    rec = json.loads(R.LEDGER.read_text())["releases"][NEW]
    assert rec["checks"]["binding_diff"] == "same" and rec["staged_at"] and rec["commit"] == "abc123"


def test_uncommitted_code_stops_before_anything_runs(world, capsys):
    world["dirty"] = ["pipeline/build.py"]
    assert _staging() == 1
    assert world["calls"] == [] and _latest(world) == LIVE
    err = capsys.readouterr().err
    assert "STOPPED at preflight" in err and "pipeline/build.py" in err


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
    assert _staging() == 0                                  # the live stamp (0.5.0) reads it


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
    assert [c for c, _ in world["calls"]] == ["build", "upload", "build", "upload"]
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
    assert ups == [{"MIN_APP_VERSION": "0.5.0"}, {"MIN_APP_VERSION": "0.5.0"}]


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


def test_the_baseline_is_kept_before_the_build_overwrites_the_reach_run(world):
    world["differs"] = True
    assert _staging() == 1
    base = R.HOME / "baseline" / LIVE
    assert json.loads((base / "report.json").read_text())["digest"] == "reach-live"
    assert json.loads((world["reaches"] / "report.json").read_text())["digest"] == "reach-new"
    assert world["diffs"] == 1


def test_no_baseline_and_a_moved_reach_run_stops_with_a_plain_message(world, capsys):
    _reach(world["reaches"], "reach-other")
    assert _staging() == 1
    err = capsys.readouterr().err
    assert "STOPPED at baseline" in err and "--baseline" in err and world["calls"] == []


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
    assert "CONFIRM_PROD is not" in err and "pipeline.deliver.release.publish" in err
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


def test_prod_runs_the_existing_publish_for_a_human(staged, monkeypatch):
    monkeypatch.setenv("CONFIRM_PROD", NEW)
    # the stamp the staging upload adds does not change the artifact
    m = json.loads((staged["root"] / NEW / "manifest.json").read_text())
    (staged["root"] / NEW / "manifest.json").write_text(json.dumps({**m, "published": "2026-11-05"}))
    assert R.prod(NEW, ask=lambda _: NEW) == 0
    assert [c for c, _ in staged["calls"]] == ["publish"]
    assert R.prod_cmd(NEW)[-3:] == ["--release", str(staged["root"] / NEW), "--yes"]


# ----------------------------------------------------------------------------------------------- the pieces it uses

def test_the_build_holds_latest_when_asked(monkeypatch):
    monkeypatch.setattr(B, "HOLD_LATEST", True)
    assert B.commands(Path("data/generated/atlas/full"))["deliver"][-1] == "--hold-latest"
    monkeypatch.setattr(B, "HOLD_LATEST", False)
    assert "--hold-latest" not in B.commands(Path("data/generated/atlas/full"))["deliver"]
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
    assert problems(m, "0.5.0") == []
    assert len(problems(m, "0.4.0")) == 1 and "salmon/4 needs app >= 0.5.0" in problems(m, "0.4.0")[0]
    assert len(problems(m)) == 2                              # no stamp, no MIN_APP_VERSION: refused
    assert "app_gate" in (Path(R.REPO_ROOT) / "deploy/staging/upload.sh").read_text()
