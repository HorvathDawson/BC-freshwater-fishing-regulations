"""The CI workflows' pins (T543): every Python install takes constraints.txt, the long jobs have timeouts, the app
workflow cancels a superseded run and watches the files its parity tests read, and no workflow has a schedule
(owner rule: never a GitHub cron).

caught (workflow review 2026-11-09): four installs ignored constraints.txt (T416: a fresh venv got Pillow 13 /
shapely 2.2 and its release differed); app-ci had no timeout; a pipeline-only push ran no worker parity test."""
from __future__ import annotations

import re
from pathlib import Path

import yaml

WF = Path(__file__).resolve().parents[2] / ".github" / "workflows"


def _load(name: str) -> dict:
    return yaml.safe_load((WF / name).read_text())


def _on(doc: dict) -> dict:
    return doc.get("on", doc.get(True))      # YAML 1.1 reads a bare `on` as True


def test_every_pip_install_of_the_project_takes_constraints():
    bad = []
    for f in sorted(WF.glob("*.yml")):
        for n, line in enumerate(f.read_text().splitlines(), 1):
            if re.search(r"pip install\b", line) and not line.lstrip().startswith("#") and "constraints.txt" not in line:
                bad.append(f"{f.name}:{n}: {line.strip()}")
    assert not bad, bad


def test_no_workflow_has_a_schedule():
    assert [f.name for f in WF.glob("*.yml") if "schedule" in _on(_load(f.name))] == []


def test_ci_jobs_have_timeouts_and_app_ci_cancels_superseded_runs():
    app, pipe = _load("app-ci.yml"), _load("pipeline-ci.yml")
    assert app["jobs"]["check"]["timeout-minutes"] == 25
    assert pipe["jobs"]["check"]["timeout-minutes"] == 20
    assert app["concurrency"]["cancel-in-progress"] is True


def test_ci_paths_cover_what_the_jobs_read():
    for name in ("app-ci.yml", "pipeline-ci.yml"):
        for ev in ("push", "pull_request"):
            assert "constraints.txt" in _on(_load(name))[ev]["paths"], (name, ev)
    for ev in ("push", "pull_request"):
        paths = _on(_load("app-ci.yml"))[ev]["paths"]
        for p in ("pipeline/regs/dfo_salmon/**", "pipeline/gauges/**", "requirements.txt"):
            assert p in paths, (ev, p)


def test_pnpm_version_is_read_from_package_json_not_typed_here():
    text = (WF / "app-ci.yml").read_text()
    assert "package_json_file: app/package.json" in text and "version: 9" not in text


def test_hydat_checks_its_key_before_it_fetches_anything():
    steps = _load("feeds-hydat.yml")["jobs"]["hydat"]["steps"]
    assert "R2 keys are not set" in steps[0]["run"]
