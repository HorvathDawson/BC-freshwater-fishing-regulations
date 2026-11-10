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
    w["landed"] = (True, "review marks: every mark in force has landed")
    monkeypatch.setattr(R, "marks_landed", lambda: w["landed"])
    monkeypatch.setattr(R, "binding_diff", diff)
    monkeypatch.setattr(R, "diff_summary", lambda rep: f"{rep['rules_changed']} rule(s) bind different sections")
    # the manifest check's real body reads the size gate and publish.plan over real release files: pinned elsewhere
    monkeypatch.setattr(R, "manifest_problems", lambda rid: list(w.get("manifest_problems", [])))
    # the live staging site (network): `site_gate` is pinned by its own tests below
    w["site_min_apps"] = []

    def site(min_app):
        w["site_min_apps"].append(min_app)
        return list(w.get("site_problems", [])), "site serves app 9.9.9"
    monkeypatch.setattr(R, "site_problems", site)
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
    assert _staging() == 0 and world["site_min_apps"][-1] == "0.5.0"     # the live stamp when none is given


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
    for step in ("export:web:staging", "build-seo.mjs --release-dir data/release/r-x", "deploy/staging/site.sh"):
        assert step in bad[0]
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
    lock = tmp_path / "lock"
    env = {k: v for k, v in os.environ.items() if k not in ("TESTBED_APK_TAG", "HEAVY_HELD", "HEAVY_PRECHECK")}
    env.update(HEAVY_LOCK=str(lock), HEAVY_PAUSE=str(tmp_path / "no-pause"), MLN_LOCAL_MAVEN=str(tmp_path / "m2"))
    r = subprocess.run(["bash", "scripts/heavy.sh", "testbed", "30", "--", "bash", "app/tools/testbed/session.sh",
                        "--build", "--", "true"], cwd=R.REPO_ROOT, env=env, capture_output=True, text=True, timeout=60)
    assert r.returncode == 1, r.stdout + r.stderr
    assert "needs TESTBED_APK_TAG" in r.stdout and "the lock was not taken" in r.stderr
    assert not lock.exists()
    ok = subprocess.run(["bash", "app/tools/testbed/session.sh", "--build", "--", "false"], cwd=R.REPO_ROOT,
                        env={**env, "HEAVY_PRECHECK": "1", "TESTBED_APK_TAG": "t"}, capture_output=True, text=True,
                        timeout=60)
    assert ok.returncode == 0 and "precheck ok" in ok.stdout
