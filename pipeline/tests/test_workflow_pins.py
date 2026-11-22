"""The CI workflows' pins (T543, T993, T994): every Python install takes constraints.txt, every job has a timeout, the
app workflows cancel a superseded run and watch the files their tests read, no workflow has a schedule (owner rule:
never a GitHub cron), every workflow declares least privilege and takes no input through shell text.

caught (workflow review 2026-11-09): four installs ignored constraints.txt (T416: a fresh venv got Pillow 13 /
shapely 2.2 and its release differed); app-ci had no timeout; a pipeline-only push ran no worker parity test.
caught (2026-11-21 audit, T993/T994): one failing app-ci step skipped the worker suite and the bundles; docs-only pushes
ran the whole app workflow; no workflow had `permissions:`; a dispatch input was spliced into a shell line.
(T995) `main` is protected with required checks: a check from a path-filtered workflow that did not run stays pending
forever, so every CI workflow runs on every pull request and ends in one always-run `ok` job; the paths live once, in
scripts/ci_paths.py."""
from __future__ import annotations

import fnmatch
import functools
import os
import re
import subprocess
from pathlib import Path, PurePosixPath

import yaml

WF = Path(__file__).resolve().parents[2] / ".github" / "workflows"
ACTIONS = WF.parent / "actions"


def _ci_paths():
    import importlib.util
    spec = importlib.util.spec_from_file_location("ci_paths", WF.parents[1] / "scripts" / "ci_paths.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


CP = _ci_paths()


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
    assert {j: d["timeout-minutes"] for j, d in app["jobs"].items()} == {"changes": 5, "gates": 20, "vitest": 25,
                                                                         "bundles": 15, "ok": 5}
    assert pipe["jobs"]["check"]["timeout-minutes"] == 20
    for name in ("app-ci.yml", "docs-ci.yml", "feeds-worker-ci.yml"):
        assert _load(name)["concurrency"]["cancel-in-progress"] is True, name
    for f in sorted(WF.glob("*.yml")):
        for j, d in _load(f.name)["jobs"].items():
            assert "timeout-minutes" in d, (f.name, j)


def test_ci_paths_cover_what_the_jobs_read():
    for ev in ("push",):                    # (T995) a pull request has no path filter: its `changes` job reads the same
        for name in ("pipeline-ci.yml", "feeds-worker-ci.yml"):
            assert "constraints.txt" in _on(_load(name))[ev]["paths"], (name, ev)
        # (T993) the worker parity tests read the Python: their own workflow watches it (they left app-ci's one job)
        paths = _on(_load("feeds-worker-ci.yml"))[ev]["paths"]
        for p in ("pipeline/regs/dfo_salmon/**", "pipeline/gauges/**", "requirements.txt", "app/workers/feeds/**"):
            assert p in paths, (ev, p)
        # a docs-only push runs the ledger / docs guard alone, never the app's jobs — but for the one runbook an app
        # test reads (no-silent-skips.test.ts, T995 review): a named file, never a handoff folder
        handoff = [p for p in _on(_load("app-ci.yml"))[ev]["paths"] if p.startswith("handoff")]
        assert handoff == ["handoff/PLAN/runbooks/release.md"], ev
        assert "handoff/**" in _on(_load("docs-ci.yml"))[ev]["paths"], ev


def test_pnpm_version_is_read_from_package_json_not_typed_here():
    text = (ACTIONS / "app-setup" / "action.yml").read_text()
    assert "package_json_file: app/package.json" in text and "version: 9" not in text
    assert "version:" not in (WF / "app-ci.yml").read_text().replace("python-version", "")


def test_app_ci_is_parallel_jobs_and_no_step_masks_another():
    """(T993) one failing step used to skip the worker suite and the bundles (a red `pnpm check` hid 0.8.5 stop 4 for
    20.9 h): the checks are jobs of their own, and the two bundle steps both run."""
    app = _load("app-ci.yml")["jobs"]
    assert set(app) == {"changes", "gates", "vitest", "bundles", "ok"}
    assert all(d["needs"] == "changes" for j, d in app.items() if j not in ("changes", "ok"))   # no job waits on another
    app = {j: d for j, d in app.items() if j not in ("changes", "ok")}
    runs = {j: [s.get("run", "") for s in d["steps"]] for j, d in app.items()}
    assert "pnpm run check:fast" in runs["gates"]
    native = [s for s in app["bundles"]["steps"] if "expo export" in s.get("run", "")]
    assert native and native[0]["if"] == "${{ !cancelled() }}"
    assert _load("feeds-worker-ci.yml")["jobs"]["worker"]["steps"][-1]["run"] == "npx vitest run --root ."


def test_app_vitest_is_sharded_and_every_file_runs_in_exactly_one_leg():
    """(T866 S1) the app suite on N runners. The shard count is the matrix's length, typed once (`strategy.job-total`
    everywhere else): a `/4` typed beside a 3-leg matrix would leave a quarter of the files unrun with every leg green.
    No leg cancels another (each one's failures are listed); each leg proves the slices partition the file list before
    it runs, and that it ran its slice after (tools/vitest-shards.mjs, whose own tests plant the faults); `app ok` needs
    the job, whose result is every leg's (test_every_ci_workflow_runs_on_every_pr_and_ends_in_one_required_check)."""
    job = _load("app-ci.yml")["jobs"]["vitest"]
    legs = job["strategy"]["matrix"]["shard"]
    assert legs == list(range(1, len(legs) + 1)) and len(legs) >= 2, legs
    assert job["strategy"]["fail-fast"] is False and set(job["strategy"]) == {"fail-fast", "matrix"}
    assert set(job["strategy"]["matrix"]) == {"shard"}                     # no other axis multiplies the legs
    assert job["name"] == "vitest (${{ matrix.shard }}/${{ strategy.job-total }})"
    runs = [s["run"] for s in job["steps"] if "run" in s]
    n = "${{ strategy.job-total }}"
    assert runs == [f"node tools/vitest-shards.mjs {n}",
                    f"pnpm run test --shard=${{{{ matrix.shard }}}}/{n} --reporter=default --reporter=json"
                    " --outputFile.json=../vitest-shard.json",
                    f"node tools/vitest-shards.mjs {n} --ran ${{{{ matrix.shard }}}} ../vitest-shard.json"], runs
    assert job["steps"][-1]["if"] == "${{ !cancelled() }}"                  # it reports after a red test step too
    # (T866 review) a red leg is a red job: `continue-on-error` on the job or a step would make a failed leg "success"
    # in `needs`, and `app ok` would pass over it
    assert "continue-on-error" not in job and not [s for s in job["steps"] if "continue-on-error" in s]
    assert "vitest" in _load("app-ci.yml")["jobs"]["ok"]["needs"]


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
    for name in ("feeds-hydat.yml", "feeds-runtiming.yml"):
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


def _covered(path: str, patterns: list[str]) -> bool:
    import fnmatch
    return any(path == p or (p.endswith("/**") and path.startswith(p[:-2])) or fnmatch.fnmatch(path, p) for p in patterns)


def test_feeds_worker_ci_watches_everything_the_worker_tests_import_or_read():
    """(T993 review) a worker test that imports or reads a file outside the workflow's path filter lets a change to it
    through unrun. Derived from the test files themselves: relative imports resolved to repo paths, and quoted repo paths
    (app/ pipeline/ data/ deploy/ scripts/) in the sources."""
    root = WF.parents[1]
    w = root / "app" / "workers" / "feeds"
    paths = CP.AREAS["feeds-worker"]
    assert _on(_load("feeds-worker-ci.yml"))["push"]["paths"] == paths
    miss = set()
    for f in [*(w / "test").rglob("*.ts"), *(w / "parity").rglob("*.ts"), *(w / "src").rglob("*.ts")]:
        text = f.read_text()
        # a relative path in an import, or in any quoted string (a readFileSync / new URL / join of "../../..")
        for m in re.finditer(r"[\"'](\.{1,2}/[^\"'`$]+)[\"']", text):
            # LEXICAL, never .resolve(): a worktree's node_modules / .venv is often a symlink to the main checkout's,
            # and resolving it left the repo (ValueError) before the node_modules filter below could drop it
            target = Path(os.path.normpath(f.parent / m.group(1)))
            rel = target.relative_to(root).as_posix()
            real = any((root / (rel + sfx)).is_file() for sfx in ("", ".ts", ".tsx", ".mjs", ".js", "/index.ts"))
            if real and "node_modules" not in rel and not _covered(rel, paths) and not _covered(rel + ".ts", paths):
                miss.add(f"{f.relative_to(root)} imports {rel}")
        for m in re.finditer(r"[\"']((?:app|pipeline|data|deploy|scripts)/[\w./-]+)[\"']", text):
            if not _covered(m.group(1), paths):
                miss.add(f"{f.relative_to(root)} reads {m.group(1)}")
    assert not miss, "\n" + "\n".join(sorted(miss))


def test_every_ci_workflow_runs_on_every_pr_and_ends_in_one_required_check():
    """(T995) main's required checks are each workflow's `ok` job. Per workflow: a push to main / release/** is
    path-filtered by exactly its scripts/ci_paths.py area; a pull request is not filtered at all; the first job
    (`changes`) asks ci_paths whether the PR touched the area; every real job needs it and runs only on its `run`; the
    last job, named as in `REQUIRED`, always runs, needs every job, and gives the verdict."""
    assert set(CP.WORKFLOWS) == set(CP.AREAS) == set(CP.REQUIRED)
    assert len(set(CP.REQUIRED.values())) == 4                                # four distinct check names
    for area, name in CP.WORKFLOWS.items():
        doc = _load(name)
        on = _on(doc)
        assert on["push"]["branches"] == [CP.TRUNK, CP.RELEASE_PREFIX + "**"], name
        assert on["push"]["paths"] == CP.AREAS[area], name                   # the area, word for word
        assert on["pull_request"] is None, name                               # no filter: it always runs
        jobs = doc["jobs"]
        ch = jobs["changes"]
        assert "needs" not in ch and "if" not in ch and ch["outputs"] == {"run": "${{ steps.c.outputs.run }}"}
        use = [s for s in ch["steps"] if s.get("uses") == "./.github/actions/changes"]
        assert len(use) == 1 and use[0]["id"] == "c" and use[0]["with"] == {"area": area}, name
        assert ch["steps"][0]["with"]["fetch-depth"] == 0, name                # the PR's merge base is in the clone
        real = [j for j in jobs if j not in ("changes", "ok")]
        assert real, name
        for j in real:
            assert jobs[j]["needs"] == "changes" and jobs[j]["if"] == "needs.changes.outputs.run == 'true'", (name, j)
        ok = jobs["ok"]
        assert list(jobs)[-1] == "ok" and ok["name"] == CP.REQUIRED[area], name
        assert ok["if"] == "always()" and ok["needs"] == ["changes", *real], name
        last = ok["steps"][-1]
        assert last["run"] == "python3 scripts/ci_paths.py verdict" and last["env"] == {"NEEDS": "${{ toJSON(needs) }}"}


def test_the_changes_action_reads_ci_paths_and_takes_no_event_field_through_shell():
    doc = yaml.safe_load((ACTIONS / "changes" / "action.yml").read_text())
    step = doc["runs"]["steps"][0]
    assert step["run"] == 'python3 scripts/ci_paths.py github "$AREA"'
    assert step["env"]["BASE_SHA"] == "${{ github.event.pull_request.base.sha }}"
    assert step["env"]["HEAD_SHA"] == "${{ github.event.pull_request.head.sha }}"
    assert doc["outputs"]["run"]["value"] == "${{ steps.c.outputs.run }}"
    # no third-party action anywhere in it (T995: plain git diff)
    assert not [s for s in doc["runs"]["steps"] if "uses" in s]


# (T995 review) THE AREAS COVER WHAT THE TESTS READ. A PR touching only a file a test reads, outside the test's area,
# skipped every job and passed all four `ok` checks (feeds-hydat.yml, deploy/staging/upload.sh: false green). Derived
# from the test sources: quoted repo-relative paths, and `ROOT / "a" / "b"` / `join(REPO, "a", "b")` chains, that exist
# in the tree (a file, a folder, or a glob that matches). Only these may stay out, each with why:
_TOP = r"(?:\.github|\.githooks|app|data|deploy|scripts|curation-review|handoff|pipeline)"
_FILES = (r"(?:README\.md|AGENTS\.md|config\.yaml|project_config\.py|requirements\.txt|constraints\.txt|pytest\.ini"
          r"|conftest\.py|\.gitignore)")
_LIT = re.compile(rf"""["']({_TOP}/[\w./*{{}}-]*|{_FILES})["']""")
_CHAIN = re.compile(r"""(?:ROOT|REPO|REPO_ROOT|_ROOT|parents\[\d\])\s*((?:/\s*["'][^"'\n]+["']\s*)+)""")
_JOIN = re.compile(r"""join\(\s*(?:REPO|ROOT)\s*,\s*((?:["'][^"'\n]+["']\s*,?\s*)+)\)""")
NOT_READ = {
    ("test_before_push.py", "AGENTS.md"): "a made-up changed path (the --changed plan)",
    ("test_before_push.py", "handoff/PLAN/STATUS.md"): "a made-up changed path (the --changed plan)",
    ("test_ci_paths.py", "AGENTS.md"): "a made-up path the glob and the areas are matched against",
    ("test_ci_paths.py", "handoff/PLAN/STATUS.md"): "a made-up path the areas are matched against",
    ("test_release_command.py", "handoff/PLAN/STATUS.md"): "a made-up committed path (the rebuild decision)",
    ("test_ci_status.py", "handoff/PLAN/STATUS.md"): "a made-up changed path (the --strict area rule)",
    ("test_workflow_pins.py", "handoff/**"): "the docs filter's own entry, compared as text",
    ("test_workflow_pins.py", "AGENTS.md"): "a key of this table",
    ("test_workflow_pins.py", "handoff/PLAN/STATUS.md"): "a key of this table",
    ("check-test-anchors.test.ts", "data/curated/waters/cuts.json"): "the scanner's own input text, never read",
}
#: tests that docs-ci runs too: their reads are covered by the docs area as well as their own
DOCS_RUN = {"check-tickets.test.ts", "start-here.test.ts"}


@functools.lru_cache(maxsize=1)
def _tracked() -> tuple[frozenset[str], frozenset[str]]:
    """The TRACKED files and their parent dirs. A path a test reads that git ignores (data/release, the built bundle)
    is machine state: no PR can change it, CI never has it, so no area needs it. Judging by the disk instead made this
    pin pass in a fresh worktree and fail in a checkout holding a release (T995 merge, 2026-11-22)."""
    files = subprocess.run(["git", "ls-files", "-z"], cwd=WF.parents[1], capture_output=True, check=True,
                           text=True).stdout.split("\0")
    files = frozenset(f for f in files if f)
    dirs = frozenset(str(PurePosixPath(f).parents[i]) for f in files for i in range(len(PurePosixPath(f).parents) - 1))
    return files, dirs


def _is_tracked(x: str) -> bool:
    files, dirs = _tracked()
    if "*" in x:
        return any(fnmatch.fnmatch(f, x) for f in files)
    return x in files or x in dirs


def _repo_reads(files: list[Path]) -> set[tuple[str, str]]:
    out = set()
    for f in files:
        text = f.read_text(encoding="utf-8")
        found = {m.group(1) for m in _LIT.finditer(text)}
        for rx in (_CHAIN, _JOIN):
            for m in rx.finditer(text):
                found.add("/".join(re.findall(r"""["']([^"']+)["']""", m.group(1))))
        for x in found:
            x = x.rstrip("/")
            if not re.match(rf"^(?:{_TOP}(?:/|$)|{_FILES}$)", x):
                continue
            if _is_tracked(x):
                out.add((f.name, x))
    return out


def _uncovered(reads: set[tuple[str, str]], patterns_of) -> list[str]:
    miss = []
    for name, x in sorted(reads):
        probe = re.sub(r"\{[^}]*\}", "x", x).replace("**", "x").replace("*", "x")
        if (name, x) in NOT_READ or any(CP.matches(pr, patterns_of(name)) for pr in (probe, probe + "/x")):
            continue
        miss.append(f"{name} reads {x}")
    return miss


def test_the_pipeline_area_covers_what_the_pipeline_tests_read():
    root = WF.parents[1]
    reads = _repo_reads(sorted((root / "pipeline" / "tests").rglob("*.py")))
    assert ("test_deploy_gates.py", "deploy/staging/upload.sh") in reads            # the scan sees a real read
    miss = _uncovered(reads, lambda name: CP.AREAS["pipeline"])
    assert not miss, "add these to scripts/ci_paths.py AREAS['pipeline']:\n" + "\n".join(miss)


def test_the_app_area_covers_what_the_app_tools_tests_read():
    root = WF.parents[1]
    reads = _repo_reads(sorted((root / "app" / "tools").glob("*.test.ts")))
    miss = _uncovered(reads, lambda name: CP.AREAS["app"] + (CP.AREAS["docs"] if name in DOCS_RUN else []))
    assert not miss, "add these to scripts/ci_paths.py AREAS['app']:\n" + "\n".join(miss)


def test_the_area_pin_fails_on_a_read_its_area_misses(tmp_path):
    """Mutation: a test reading deploy/staging/upload.sh, judged against an area without deploy/**, is named."""
    f = tmp_path / "test_x.py"
    f.write_text('S = ROOT / "deploy" / "staging" / "upload.sh"\nW = ".github/workflows/feeds-hydat.yml"\n')
    reads = _repo_reads([f])
    assert reads == {("test_x.py", "deploy/staging/upload.sh"), ("test_x.py", ".github/workflows/feeds-hydat.yml")}
    assert _uncovered(reads, lambda name: ["pipeline/**"]) == [
        "test_x.py reads .github/workflows/feeds-hydat.yml", "test_x.py reads deploy/staging/upload.sh"]
    assert _uncovered(reads, lambda name: CP.AREAS["pipeline"]) == []



def test_run_timing_publish_refuses_when_the_release_moved_since_it_bound():
    """(T485 step 3) a run without a `tiles` input re-reads release/manifest.json before step 7 and fails, said, when the
    tiles moved since step 1, so run timing is never published for the old tiles after a release flipped."""
    steps = _load("feeds-runtiming.yml")["jobs"]["runtiming"]["steps"]
    up = [s for s in steps if s.get("name", "").startswith("7/7")][0]
    run = up["run"]
    assert 'if [ -z "$TILES_IN" ]' in run and "release/manifest.json" in run
    assert run.index('"$NOW" != "$TILES"') < run.index("feed.publish")
    assert "exit 1" in run[run.index('"$NOW" != "$TILES"'):run.index("feed.publish")]
    assert "the release moved" in run
