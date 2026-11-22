"""The CI workflows' pins (T543, T993, T994): every Python install takes constraints.txt, every job has a timeout, the
app workflows cancel a superseded run and watch the files their tests read, no workflow has a schedule (owner rule:
never a GitHub cron), every workflow declares least privilege and takes no input through shell text.

caught (workflow review 2026-11-09): four installs ignored constraints.txt (T416: a fresh venv got Pillow 13 /
shapely 2.2 and its release differed); app-ci had no timeout; a pipeline-only push ran no worker parity test.
caught (2026-11-21 audit, T993/T994): one failing app-ci step skipped the worker suite and the bundles; docs-only pushes
ran the whole app workflow; no workflow had `permissions:`; a dispatch input was spliced into a shell line."""
from __future__ import annotations

import re
from pathlib import Path

import yaml

WF = Path(__file__).resolve().parents[2] / ".github" / "workflows"
ACTIONS = WF.parent / "actions"


def _load(name: str) -> dict:
    return yaml.safe_load((WF / name).read_text())


def _on(doc: dict) -> dict:
    return doc.get("on", doc.get(True))      # YAML 1.1 reads a bare `on` as True


def test_every_pip_install_of_the_project_takes_constraints():
    bad = []
    for f in sorted([*WF.glob("*.yml"), *ACTIONS.glob("*/action.yml")]):
        for n, line in enumerate(f.read_text().splitlines(), 1):
            if re.search(r"pip install\b", line) and not line.lstrip().startswith("#") and "constraints.txt" not in line:
                bad.append(f"{f.name}:{n}: {line.strip()}")
    assert not bad, bad


def test_no_workflow_has_a_schedule():
    assert [f.name for f in WF.glob("*.yml") if "schedule" in _on(_load(f.name))] == []


def test_ci_jobs_have_timeouts_and_the_ci_workflows_cancel_superseded_runs():
    app, pipe = _load("app-ci.yml"), _load("pipeline-ci.yml")
    assert {j: d["timeout-minutes"] for j, d in app["jobs"].items()} == {"gates": 20, "vitest": 25, "bundles": 15}
    assert pipe["jobs"]["check"]["timeout-minutes"] == 20
    for name in ("app-ci.yml", "docs-ci.yml", "feeds-worker-ci.yml"):
        assert _load(name)["concurrency"]["cancel-in-progress"] is True, name
    for f in sorted(WF.glob("*.yml")):
        for j, d in _load(f.name)["jobs"].items():
            assert "timeout-minutes" in d, (f.name, j)


def test_ci_paths_cover_what_the_jobs_read():
    for ev in ("push", "pull_request"):
        for name in ("pipeline-ci.yml", "feeds-worker-ci.yml"):
            assert "constraints.txt" in _on(_load(name))[ev]["paths"], (name, ev)
        # (T993) the worker parity tests read the Python: their own workflow watches it (they left app-ci's one job)
        paths = _on(_load("feeds-worker-ci.yml"))[ev]["paths"]
        for p in ("pipeline/regs/dfo_salmon/**", "pipeline/gauges/**", "requirements.txt", "app/workers/feeds/**"):
            assert p in paths, (ev, p)
        # a docs-only push runs the ledger / docs guard alone, never the app's jobs
        assert not any(p.startswith("handoff") for p in _on(_load("app-ci.yml"))[ev]["paths"]), ev
        assert "handoff/**" in _on(_load("docs-ci.yml"))[ev]["paths"], ev


def test_pnpm_version_is_read_from_package_json_not_typed_here():
    text = (ACTIONS / "app-setup" / "action.yml").read_text()
    assert "package_json_file: app/package.json" in text and "version: 9" not in text
    assert "version:" not in (WF / "app-ci.yml").read_text().replace("python-version", "")


def test_app_ci_is_parallel_jobs_and_no_step_masks_another():
    """(T993) one failing step used to skip the worker suite and the bundles (a red `pnpm check` hid 0.8.5 stop 4 for
    20.9 h): the checks are jobs of their own, and the two bundle steps both run."""
    app = _load("app-ci.yml")["jobs"]
    assert set(app) == {"gates", "vitest", "bundles"}
    assert all("needs" not in d for d in app.values())
    runs = {j: [s.get("run", "") for s in d["steps"]] for j, d in app.items()}
    assert "pnpm run check:fast" in runs["gates"] and "pnpm run test" in runs["vitest"]
    native = [s for s in app["bundles"]["steps"] if "expo export" in s.get("run", "")]
    assert native and native[0]["if"] == "${{ !cancelled() }}"
    assert _load("feeds-worker-ci.yml")["jobs"]["worker"]["steps"][-1]["run"] == "npx vitest run --root ."


def test_every_workflow_has_least_privilege_and_no_input_is_spliced_into_shell():
    """(T994 6) `permissions:` on every workflow; a dispatch input / event field reaches a `run` only through env."""
    for f in sorted(WF.glob("*.yml")):
        doc = _load(f.name)
        assert doc.get("permissions") == {"contents": "read"}, f.name
        for j, d in doc["jobs"].items():
            for st in d["steps"]:
                assert not re.search(r"\$\{\{\s*(inputs|github\.event|github\.head_ref)", st.get("run", "")), \
                    (f.name, j, st.get("name"))


def test_no_register_only_push_trigger_and_actions_are_off_node_20():
    for name in ("feeds-hydat.yml", "feeds-runtiming.yml", "staging-smoke.yml", "staging-security.yml"):
        assert list(_on(_load(name))) == ["workflow_dispatch"], name
    old = re.compile(r"actions/(checkout|setup-node)@v[1-4]\b|actions/setup-python@v[1-5]\b|upload-artifact@v[1-5]\b"
                     r"|pnpm/action-setup@v[1-4]\b")
    bad = [f"{p.name}:{n}" for p in [*WF.glob("*.yml"), *ACTIONS.glob("*/action.yml")]
           for n, line in enumerate(p.read_text().splitlines(), 1) if old.search(line) and not line.lstrip().startswith("#")]
    assert not bad, bad


def test_hydat_checks_its_key_before_it_fetches_anything():
    steps = _load("feeds-hydat.yml")["jobs"]["hydat"]["steps"]
    assert "R2 keys are not set" in steps[0]["run"]


def test_the_run_timing_report_upload_cannot_fail_the_run_but_says_so():
    steps = _load("feeds-runtiming.yml")["jobs"]["runtiming"]["steps"]
    up = [s for s in steps if "upload-artifact" in s.get("uses", "")][0]
    assert up["continue-on-error"] is True and up["id"] == "report"
    said = steps[steps.index(up) + 1]
    assert said["if"] == "steps.report.outcome == 'failure'" and "::warning::" in said["run"]
