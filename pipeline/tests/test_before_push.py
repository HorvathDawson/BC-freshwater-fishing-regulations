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
    assert "the owner's call" in out and "hooks/pre-push" in out and ran == []
    assert not any(tmp_path.iterdir())
