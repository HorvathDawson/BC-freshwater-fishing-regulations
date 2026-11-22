"""`scripts/before_push.py` (T993 item 7, T791 #2): one command before a push that asks what GitHub's runs ask — the
step list read from pipeline-ci.yml, every failure listed, the hook printed and never installed. No step is run here
(each is a real check of its own); the runner is stood in for."""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from pipeline.common.curated import REPO_ROOT

ROOT = Path(REPO_ROOT)
spec = importlib.util.spec_from_file_location("before_push", ROOT / "scripts" / "before_push.py")
BP = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = BP           # its dataclasses look their module up by name
spec.loader.exec_module(BP)


def test_every_pipeline_ci_run_step_is_run_or_named_with_its_reason():
    """Read from the workflow, as fast-fail's ci_step: a step added to pipeline-ci.yml joins by itself."""
    steps = BP.workflow_steps()
    names = {s.name for s in BP.plan()}
    for s in steps:
        if s.get("name") and "run" in s:
            assert f"CI: {s['name']}" in names or s["name"] in BP.SKIP or s["name"] == BP.TESTS, s["name"]
    assert set(BP.SKIP) <= {s.get("name") for s in steps}, "a SKIP entry names no workflow step"
    by_name = {s.name: s for s in BP.plan()}
    mini = by_name["CI: The committed mini release matches a rebuild"]
    assert mini.script == next(s["run"] for s in steps if s.get("name") == "The committed mini release matches a rebuild")


def test_a_step_added_to_the_workflow_joins_and_the_tier_reads_its_markers():
    steps = BP.workflow_steps() + [{"name": "A new guard", "run": "python -m pipeline.tools.new_guard --check"}]
    plan = BP.plan(steps=steps)
    assert any(s.name == "CI: A new guard" and "new_guard" in s.script for s in plan)
    tier = next(s for s in plan if s.name.startswith("CI: Tests"))
    assert "scripts/heavy.sh" in tier.cmd and "scripts/pytest-chunked.sh" in tier.cmd
    assert tier.cmd[tier.cmd.index("-m") + 1] == BP.tier_markers(steps) and "not needs_bundle" in BP.tier_markers(steps)


def test_it_asks_what_app_ci_asks_beyond_pnpm_check():
    plan = {s.name: s for s in BP.plan()}
    assert plan["app gates (pnpm check:fast)"].cmd[-1] == "check:fast"
    assert "workers/feeds" in plan["feeds worker tests"].cmd
    scanners = plan["app tree scanners (app/tools tests that read the tree)"].cmd
    assert "tools/failures.test.ts" in scanners and "tools/back-layers.test.ts" in scanners
    no_tier = {s.name: s for s in BP.plan(tier=False)}
    assert not any(n.startswith("CI: Tests") for n in no_tier)
    assert "pipeline/tests/test_liveness.py" in no_tier["the pipeline's source scanners (--no-tier)"].cmd


def test_every_failure_is_listed_and_the_exit_says_so(capsys):
    def runner(step):
        return 1 if step.name in ("app gates (pnpm check:fast)", "feeds worker tests") else 0
    assert BP.main([], runner=runner) == 1
    err = capsys.readouterr().err
    assert "2 of" in err and "- app gates (pnpm check:fast)" in err and "- feeds worker tests" in err
    assert "--hook prints how" in err
    assert BP.main(["--only", "feeds"], runner=lambda s: 0) == 0
    assert "PARTIAL (--only): 1 step(s) passed" in capsys.readouterr().out
    ran = []
    assert BP.main(["--only", "no-such-step"], runner=lambda s: ran.append(s) or 0) == 2 and ran == []
    assert "matches no step" in capsys.readouterr().err                   # never a pass on nothing run


def test_the_hook_is_printed_never_installed(capsys, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    ran = []
    assert BP.main(["--hook"], runner=lambda s: ran.append(s) or 0) == 0
    out = capsys.readouterr().out
    assert "\ngit config core.hooksPath .githooks\n" in out and "git push --no-verify" in out and ran == []
    assert not any(tmp_path.iterdir())


def _changed(monkeypatch, capsys, paths, argv=()):
    """(T995) `--changed` over these paths: the names of the steps it would run."""
    monkeypatch.delenv(BP.SKIP_ENV, raising=False)
    monkeypatch.setattr(BP, "changed_paths", lambda: ("b" * 40, list(paths)))
    ran = []
    assert BP.main(["--changed", "--no-tier", *argv], runner=lambda s: ran.append(s.name) or 0) == 0
    capsys.readouterr()
    return ran


def test_changed_runs_only_the_areas_the_branch_changed(monkeypatch, capsys):
    """(T995) the same areas as CI's `changes` jobs (scripts/ci_paths.py): a docs-only push is the ticket guard alone;
    a pipeline change adds the pipeline's steps; an app change runs the app gates (the ticket guard is inside them)."""
    guard = "ticket guard (node app/tools/check-tickets.mjs)"
    docs = "docs: the ticket guard's tests and the start-here page (docs-ci)"
    assert _changed(monkeypatch, capsys, ["handoff/PLAN/STATUS.md", "AGENTS.md"]) == [guard, docs]
    assert _changed(monkeypatch, capsys, ["README.md"]) == [guard]
    # (T995 review) what docs-ci runs on a handoff change, the hook runs: its vitest step, word for word
    import yaml
    ledger = yaml.safe_load((ROOT / ".github" / "workflows" / "docs-ci.yml").read_text())["jobs"]["ledger"]["steps"]
    assert f"pnpm exec vitest run {' '.join(BP.DOCS_TESTS)}" in [s.get("run") for s in ledger]
    ran = _changed(monkeypatch, capsys, ["pipeline/release.py"])
    assert ran[0] == guard and "the pipeline's source scanners (--no-tier)" in ran
    assert not any(n.startswith(("app", "feeds worker")) for n in ran)
    ran = _changed(monkeypatch, capsys, ["app/workers/feeds/src/index.ts"])
    assert guard not in ran and "app gates (pnpm check:fast)" in ran and "feeds worker tests" in ran
    assert not any(n.startswith("CI: ") for n in ran)
    full = [s.name for s in BP.plan(tier=False)]
    assert _changed(monkeypatch, capsys, ["pipeline/x.py", "app/x.ts", "app/workers/feeds/x.ts", "AGENTS.md"]) == full
    # every step names its area
    assert {BP.step_area(s) for s in BP.plan(app_suite=True)} == {"pipeline", "app", "feeds-worker", "docs"}


def test_the_release_commands_own_push_skips_saying_why(monkeypatch, capsys):
    monkeypatch.setenv(BP.SKIP_ENV, "the release command's own push")
    ran = []
    assert BP.main(["--changed"], runner=lambda s: ran.append(s) or 0) == 0 and ran == []
    assert "SKIPPED (nothing run): the release command's own push" in capsys.readouterr().out
    monkeypatch.setenv(BP.SKIP_ENV, " ")
    assert BP.main(["--changed"], runner=lambda s: ran.append(s) or 0) == 2 and ran == []
    assert "needs a reason" in capsys.readouterr().err


def test_the_versioned_hooks():
    """(T995) .githooks/pre-push runs `--changed` (seconds for a docs push); commit-msg is T998's stub (passes)."""
    import os
    import subprocess
    hooks = ROOT / ".githooks"
    pre = (hooks / "pre-push").read_text()
    assert 'exec "$py" scripts/before_push.py --changed' in pre and "git config core.hooksPath .githooks" in pre
    assert os.access(hooks / "pre-push", os.X_OK) and os.access(hooks / "commit-msg", os.X_OK)
    assert "T998" in (hooks / "commit-msg").read_text()
    assert subprocess.run(["sh", str(hooks / "commit-msg"), "/dev/null"]).returncode == 0
