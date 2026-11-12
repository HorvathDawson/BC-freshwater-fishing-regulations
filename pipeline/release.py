"""ONE RELEASE COMMAND (T295/T297, T408 re-scope, owner 2026-11-05): build once, promote the same artifact.

    python -m pipeline release --staging                      # build -> checks -> follows -> promote LATEST -> upload staging
    python -m pipeline release --staging --dry-run            # the REHEARSAL: every stage's checks, nothing built / committed / sent
python -m pipeline release --staging --dry-run --rehearse-follows   # + (T680) re-cut + every test on the candidate, put back
    python -m pipeline release --staging --accept-diff FILE   # go on past a binding diff you have read (FILE = its json)
    python -m pipeline release --staging --push               # the release pushes the commits IT made (matches, fixtures)
    python -m pipeline release --prod <r-id>                  # HUMAN-ONLY: re-upload THAT checked release to production

ONE RELEASE PATH (T666, owner 2026-11-11, after build 27 took six runs): everything cut from the live release is
refreshed by the release itself, against the CANDIDATE, before the promote (stage 5, the table in
`pipeline/release_follows.py`); preflight runs ALL of its checks and stops once with every failure listed; the command
remembers its own flags (ledger `run`), so a rerun is the same bare command; `--dry-run` rehearses every stage's checks.

THE STAGES, in order; a stage that fails stops the run with a plain message (preflight: with EVERY failed check, each
with its next step), and a rerun of the same command picks up where it stopped (every stage below is skipped when it is
already done for this release):

  1 preflight  EVERY check runs and every failure is listed together (T666 item 3; the table is `PREFLIGHT`, which the
               dry run reads too). The instant checks first, then `scripts/fast-fail.sh` LAST (T483/T535, AGENTS 20a:
               the cheap checks, a minute or two, run with FAST_FAIL_ALL so it too lists every failed step;
               `--no-fast-fail REASON` skips it and prints the reason; the build's heavy.sh is told so,
               FAST_FAIL_DONE). (T666 item 5) Its pass is keyed on the TREE'S CONTENT, not on HEAD (ledger `fast_fail`
               `key`: HEAD's tree + what is uncommitted + untracked files): a rerun on the same content skips it, and
               after a commit it checks only what changed since the commit it last passed on (FAST_FAIL_BASE = that
               commit), so a docs or fixtures-only commit never reruns the whole no-data tier. After it, THE
               FRESH-COPY TEST RUN (owner 2026-11-11: once, in the release pre-check): HEAD cloned into a temp folder
               and the pipeline's no-data tier run there as CI runs it (`scripts/fresh-copy-tests.sh`, through
               heavy.sh), once per release — again only when a commit since its pass touched something other than
               what the release itself refreshes; `--no-fresh-copy REASON` skips it, printed. The instant ones:
               code and data/curated committed (the review log marks.jsonl
               and closed.json may be dirty: no stage reads them, `build.NOT_BUILD_INPUTS`); the atlas is not behind
               a curated input it was built from (T547, `atlas.inputs.behind`: a cut, name, area, added water, tidal
               limit or slough call edited after the atlas build is named, and the run stops unless
               `--ship-stale-atlas REASON`); every review mark in
               force landed in its curated file (T545, `review.apply --check`; not skipped by --no-fast-fail); HEAD pushed to
               origin/redesign/stream-sections (T484: the staging upload starts a GitHub workflow that runs GitHub's
               copy — found only at stage 6 on 2026-11-07); CI (`app-ci`, `pipeline-ci`) not red on HEAD, nor on the
               newest finished run when HEAD has none (T533, `scripts/ci_status.py`; running / no run = a warning;
               `--no-ci-check REASON`); fast-fail's "changed" base is the commit LATEST was built from (T533, ledger); the live staging SITE is at least the min app the release
               goes up with (`deliver.release.site_gate`; 2026-11-09 the smoke test found a 0.6.9 site under a 0.7.0
               release only after the upload); the HEAD commit is recorded with the release.
  2 baseline   a copy of the reach run the LIVE release (LATEST) was cut from, kept before the build can overwrite it,
               so the binding diff has its "before" (data/generated/release/baseline/<LATEST>/). `pipeline build` keeps
               the same copy before any reach rerun outside a release (`keep_baseline`, 2026-11-09).
  3 build      `python -m pipeline build --hold-latest` through `scripts/heavy.sh` (one heavy job at a time). The build
               caches its own stages (its manifest keys every stage by its inputs), so a rerun skips what is done.
               --hold-latest: the new release is written to data/release/<r-id>/ and named in data/release/CANDIDATE;
               LATEST does NOT move.
  4 checks     manifest sanity (every file the manifest names is there at its size, the size gate, the bundle block
               names the reach run on disk), the app-version gate (`deliver.release.app_gate`, the one the staging
               upload runs), and the reach BINDING DIFF against the live release (`atlas.reach.binding_diff`). A diff
               that is not empty STOPS the run: the summary is printed and written beside its json; a reviewer reads it
               and reruns with `--accept-diff <that json>` (its content must be the one written, so a stale accept never
               passes a new diff; content = the changes and counts, not the before/after path strings).
  5 follows    (T666 items 1-2) REFRESH WHAT FOLLOWS THE RELEASE, on the candidate, LATEST untouched — the table is
               `pipeline/release_follows.py` `FOLLOWS`: (a) the IN-SEASON MATCHES are resolved (T537,
               `regs.inseason.resolve`, through heavy.sh): a match that does not resolve stops here; a changed
               `data/curated/inseason/notice_waters.json` is committed (that path only). (b) the app's release-backed
               FIXTURES are re-cut (`node app/tools/regs-recut/sync.mjs --release <candidate>`): a change that only
               renumbers (ids, handles, the fixture release) is committed with the release id; a changed GOLDEN PAGE —
               what an angler is told — STOPS, nothing committed, until `--accept-fixtures <token>` (T667 D5). (c) the
               TESTS THAT ASK FOR THE RELEASE UNDER TEST (found by rule, both suites) run with RELEASE_DIR = the
               candidate, so nothing compares with LATEST before the promote, and (T680) THE WHOLE APP CHECK with
               them — the app suite, the feeds worker suite and each `check:fast` gate on its own — every one runs
               whatever the one before did, and the stop lists EVERY failing test by name, together (r-483b295de57c:
               two tests pinned on the old fixtures failed in CI one round at a time, after the push)
               (`--no-follow-tests REASON` skips them, printed; `--dry-run --rehearse-follows` rehearses the re-cut and
               all of them on a built candidate and puts the fixtures back). (d) THE PUSH GATE: the commits the release made must be on GitHub before the upload — it
               stops ONCE with the one push command, or pushes them itself with `--push` (only when every unpushed
               commit is its own). The upload's sync-inputs.sh only verifies the matches.
  6 promote    LATEST := the candidate (CANDIDATE removed). Only here does the live release pointer move, so a failed
               check leaves LATEST untouched.
  7 staging    the upload's own first refusals asked up front (`staging_prechecks`: the run-timing base names the
               release's tiles, `gh` is logged in, the rclone remote is there), then
               `deploy/staging/upload.sh <r-id>` (staging bucket only; MIN_APP_VERSION from --min-app-version, else the
               previous release's stamp — printed in the plan), with its smoke test run on its own after it (T537:
               ledger `uploaded_at` and `smoked_at`; a rerun after a smoke-only failure runs only the smoke). It says
               when it replaces a staged release the owner was handed a production command for. The success message
               lists the after-chores with their exact commands (goldens re-cut, site, feeds worker). On a RERUN (T484) the default is the min app this
               release's checks passed with (ledger `checks.min_app_version`), else the stamp of the release LATEST
               named before this one was promoted (ledger `live_before`) — never the new release's own stamp, which
               LATEST names once the promote is done. (T666 item 6) GitHub refusing to START the run-timing workflow
               is retried by the upload itself, bounded and said out loud (`scripts/start-workflow.sh`); when the
               bound is spent the stage says so (exit 75) and a rerun does only this stage's work.

THE COMMAND REMEMBERS ITS FLAGS (T666 item 4): `--baseline`, `--accept-diff`, `--accept-fixtures`, `--min-app-version`
and `--push` given on any run of a release are kept in the ledger (`run`) until it is on staging, and said when they
are used; `--baseline`'s reach run is also copied under baseline/. A flag left over from before the promote (a
`--baseline` for the release LATEST named then) is ignored out loud, never an error; `--forget-flags` drops them; a
flag given again replaces the remembered one. The ESCAPES (`--no-fast-fail`,
`--no-ci-check`, `--ship-stale-atlas`, `--no-follow-tests`, `--no-fresh-copy`) are NOT remembered: each run that skips a guard says why.

WHERE THINGS MOVE BEFORE THE CHECKS: the build writes the live working outputs (the reach run, tiles, bundle, export,
run-timing feed) as `pipeline build` always has — the review app reads them — and only the release POINTER waits. A
release whose checks failed is on disk under data/release/<r-id>/ and named by CANDIDATE; nothing ships it.

PROD (`--prod <r-id>`) builds nothing and checks nothing new but one thing (T537): staging must still serve that
release (its live `release/manifest.json`, read first, also in the dry run — `deploy/prod/upload.sh` promotes what
staging serves); otherwise: it re-uploads the SAME release folder that passed this
command's checks and reached staging (the ledger says so, and its content fingerprint must be unchanged), through
`deploy/prod/upload.sh <r-id>` (T404: it promotes what staging serves — the same manifest bytes — and keeps the previous
pointer for deploy/prod/rollback.sh; handoff/PLAN/release/PRODUCTION.md). It runs ONLY for a human:
CONFIRM_PROD=<r-id> in the environment, an interactive terminal where the id is typed again, and no agent environment
(CLAUDECODE / AI_AGENT). Otherwise it prints the exact commands and refuses (exit 2).

The ledger is data/generated/release/ledger.json: per release id, the commit, the checks, what followed it (`follows`),
when it was promoted, staged and published; `fast_fail` (the tree content it last passed on); `run` (the flags of the
release in flight).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

from pipeline import release_follows as RF
from pipeline.common.curated import CURATED, GENERATED, REPO_ROOT

#: everything release keeps of its own: the ledger, the baseline reach run, each candidate's check outputs
HOME = GENERATED.bundle.parent / "release"
LEDGER = HOME / "ledger.json"
#: the git paths a build reads: code + the curated tree (+ the scripts/deploy the release runs)
COMMITTED = ("pipeline", "data/curated", "scripts", "deploy", "project_config.py", "config.yaml")
#: the reach-run files the binding diff reads (`binding_diff.diff_runs`) + the report naming its digest
BASELINE_FILES = ("report.json", "rule_section.jsonl.gz", "rule_unresolved.jsonl", "licensing_section.jsonl",
                  # T546 item 8: DFO places, tidal / outside, the curator's diagnostics
                  "dfo_salmon.jsonl.gz", "dfo_salmon_diagnostic.jsonl", "rule_diagnostic.jsonl", "tidal.jsonl",
                  "outside_bc.jsonl")
#: min free memory % for heavy.sh to start the build (memory `heavy-jobs-one-at-a-time`)
HEAVY_MIN_FREE = 50
STAGES = ("preflight", "baseline", "build", "checks", "follows", "promote", "staging")
#: (T666 item 6) deploy/staging/upload.sh's exit when GitHub would not START the run-timing workflow
#: (`scripts/start-workflow.sh`, after its bounded retries): nothing that moves the pointer was sent
RC_WORKFLOW_NOT_STARTED = 75
#: (T666 item 4) the flags a rerun need not repeat (ledger `run`); the escapes are never remembered
REMEMBERED = ("baseline", "accept_diff", "accept_fixtures", "min_app", "push")
#: the branch GitHub's workflows run (the staging upload starts feeds-runtiming.yml there); HEAD must be on it
GH_REMOTE, GH_BRANCH = "origin", "redesign/stream-sections"
#: manifest keys stamped at upload time (`release/stamp.py`), outside every hash: not part of the fingerprint
STAMP_KEYS = ("published", "minAppVersion")


class Stop(Exception):
    """A stage failed: `what` failed, `todo` is what the human does next. `problems` is EVERY failure the stage found,
    each (what, todo) — one for most stages, the whole list for preflight (T666 item 3)."""

    def __init__(self, stage: str, what: str, todo: str, problems: list[tuple[str, str]] | None = None):
        super().__init__(f"{stage}: {what}")
        self.stage, self.what, self.todo = stage, what, todo
        self.problems = problems or [(what, todo)]


@dataclass
class Finding:
    """What one check found: `ok`, what it `said` (one line or a few), and — when not ok — `todo`, the next step."""
    ok: bool
    said: str
    todo: str = ""


# ---------------------------------------------------------------------------------------------------------- the world
# Every touch of disk, git and subprocesses goes through these few functions, so the tests swap them out.

def release_root() -> Path:
    return Path(GENERATED.release)


def reach_dir() -> Path:
    return Path(GENERATED.reaches) / GENERATED.build().name


def read_pointer(name: str) -> str | None:
    p = release_root() / name
    return p.read_text().strip() or None if p.is_file() else None


def git_dirty() -> list[str]:
    """The uncommitted paths a build reads (tracked changes and untracked files), minus the review log."""
    out = subprocess.run(["git", "status", "--porcelain", "--untracked-files=all", "--", *COMMITTED],
                         cwd=REPO_ROOT, capture_output=True, text=True, check=True).stdout
    allowed = {str(Path(p).resolve().relative_to(REPO_ROOT)) for p in
               (CURATED.review.marks, CURATED.review.closed)}
    paths = [ln[3:].split(" -> ")[-1].strip().strip('"') for ln in out.splitlines() if ln.strip()]
    return [p for p in paths if p not in allowed]


def marks_landed() -> tuple[bool, str]:
    """(T545) Has every review mark in force landed in its curated file (`review.apply --check`, read-only, ~2 s)?
    (ok, what it printed). fast-fail runs it too; preflight asks again itself so `--no-fast-fail` cannot skip it."""
    got = subprocess.run([sys.executable, "-m", "pipeline.regs.review.apply", "--check"], cwd=REPO_ROOT,
                         capture_output=True, text=True)
    if got.returncode not in (0, 1):
        raise RuntimeError(f"review.apply --check failed (rc={got.returncode}): {(got.stderr or got.stdout)[-800:]}")
    return got.returncode == 0, got.stdout.strip()


def atlas_behind() -> list[str]:
    """(T547) Each curated input the live atlas is behind (`pipeline.atlas.inputs.behind`), one line per file; [] when
    it is current. The release build reads this atlas (`pipeline build` rebuilds it only with --atlas)."""
    from pipeline.atlas.inputs import behind
    return behind(GENERATED.require_build())


def git_head() -> str:
    return subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, capture_output=True, text=True,
                          check=True).stdout.strip()


def git_pushed() -> bool:
    """HEAD is on origin/redesign/stream-sections after a fresh fetch. A failed fetch raises (never a silent pass)."""
    subprocess.run(["git", "fetch", "-q", GH_REMOTE, GH_BRANCH], cwd=REPO_ROOT, check=True)
    rc = subprocess.run(["git", "merge-base", "--is-ancestor", "HEAD", f"{GH_REMOTE}/{GH_BRANCH}"],
                        cwd=REPO_ROOT).returncode
    if rc not in (0, 1):
        raise RuntimeError(f"git merge-base --is-ancestor HEAD {GH_REMOTE}/{GH_BRANCH} failed (rc={rc})")
    return rc == 0


def run_fast_fail(base: str | None = None, env: dict | None = None) -> tuple[int, list[str]]:
    """`scripts/fast-fail.sh` (T483) on the console: (its return code, the name of EVERY step that failed). (T533)
    `base` = FAST_FAIL_BASE: what it checks as changed is everything since that commit, not only the unpushed commits.
    (T666 item 3) FAST_FAIL_ALL: it goes on past a failed step and reports them all (FAST_FAIL_REPORT). FAST_FAIL_ONLY
    is cleared: a partial run inherited from the caller's shell would be recorded as a pass."""
    import tempfile
    with tempfile.TemporaryDirectory(prefix="release-fast-fail-") as td:
        report = Path(td) / "failed.txt"
        rc = run_cmd(["bash", "scripts/fast-fail.sh"],
                     env={**(env or {}), "FAST_FAIL_BASE": base or "", "FAST_FAIL_ALL": "1", "FAST_FAIL_ONLY": "",
                          "FAST_FAIL_REPORT": str(report)})
        failed = [ln for ln in report.read_text(encoding="utf-8").splitlines() if ln.strip()] if report.is_file() else []
    return rc, failed


def _git(*args: str) -> bytes:
    return subprocess.run(["git", *args], cwd=REPO_ROOT, capture_output=True, check=True).stdout


def tree_key() -> str:
    """(T666 item 5) THE WORKING TREE BY CONTENT — what fast-fail reads: every file in it (tracked as it is on disk,
    and untracked ones git does not ignore) by path and git blob id. A new HEAD over the same files has the same key,
    committing an edit does not move it, any edit does. Read-only: nothing is added to the index or the object store
    (`git hash-object` without -w)."""
    blobs: dict[bytes, bytes] = {}
    for ent in _git("ls-files", "-s", "-z").split(b"\0"):          # `<mode> <blob> <stage>\t<path>`: the index
        if ent:
            meta, path = ent.split(b"\t", 1)
            blobs[path] = meta.split()[1]
    off = [x for x in _git("diff", "--name-only", "-z").split(b"\0") if x]            # on disk, not as indexed
    off += [x for x in _git("ls-files", "--others", "--exclude-standard", "-z").split(b"\0") if x]
    here = [x for x in off if (Path(REPO_ROOT) / os.fsdecode(x)).is_file()]
    for x in off:
        blobs.pop(x, None)                                          # deleted on disk: not in the tree
    if here:
        ids = subprocess.run(["git", "hash-object", "--stdin-paths"], cwd=REPO_ROOT, input=b"\n".join(here) + b"\n",
                             capture_output=True, check=True).stdout.split()
        if len(ids) != len(here):
            raise RuntimeError(f"git hash-object answered {len(ids)} ids for {len(here)} paths")
        blobs.update(zip(here, ids))
    h = hashlib.sha256()
    for path in sorted(blobs):
        h.update(path + b"\0" + blobs[path] + b"\n")
    return h.hexdigest()[:16]


def dirty_paths() -> list[str]:
    """Every tracked path whose working-tree content is not HEAD's."""
    return sorted(x for x in _git("diff", "HEAD", "--name-only").decode().splitlines() if x)


def changed_since(rev: str) -> set[str]:
    """Every tracked path whose working-tree content is not `rev`'s (what fast-fail's "changed" holds for that base)."""
    return {x for x in _git("diff", rev, "--name-only").decode().splitlines() if x}


def committed_since(rev: str) -> set[str]:
    """Every path a commit after `rev` touched (HEAD against it; the working tree is not asked)."""
    return {x for x in _git("diff", rev, "HEAD", "--name-only").decode().splitlines() if x}


def head_tree() -> str:
    return _git("rev-parse", "HEAD^{tree}").decode().strip()


def run_fresh_copy() -> int:
    """(T666) `scripts/fresh-copy-tests.sh` through heavy.sh: HEAD in a temp clone, the no-data tier as CI runs it."""
    return run_cmd(["bash", "scripts/heavy.sh", "release-fresh-copy", "30", "--", "bash", "scripts/fresh-copy-tests.sh"])


def git_unpushed() -> list[str]:
    """The commits on HEAD that origin's branch does not have (after `git_pushed`'s fetch), oldest first."""
    return _git("rev-list", "--reverse", f"{GH_REMOTE}/{GH_BRANCH}..HEAD").decode().split()


def git_push() -> int:
    """Push HEAD to the branch GitHub's workflows run; the return code."""
    return run_cmd(["git", "push", GH_REMOTE, f"HEAD:{GH_BRANCH}"])


def git_has_commit(rev: str) -> bool:
    return subprocess.run(["git", "cat-file", "-e", f"{rev}^{{commit}}"], cwd=REPO_ROOT,
                          capture_output=True).returncode == 0


def ci_status(skip: str | None = None) -> tuple[int, str]:
    """(T533) `scripts/ci_status.py`: is CI red on HEAD (or on the newest finished run when HEAD has none)? (rc, what it
    printed): 0 ok / warnings, 1 red, 2 cannot tell."""
    got = subprocess.run([sys.executable, "scripts/ci_status.py", *(["--skip", skip] if skip is not None else [])],
                         cwd=REPO_ROOT, capture_output=True, text=True)
    return got.returncode, (got.stdout + got.stderr).strip()


#: (T537) the in-season matches, resolved on the candidate before the promote
INSEASON = Path(CURATED.inseason.notice_waters)
#: where staging serves the release pointer the prod upload promotes (`deploy/prod/upload.sh`)
STAGING_MANIFEST_URL = "https://data-staging.canifishthis.ca/release/manifest.json"


def inseason_cmd(rid: str, check: bool = False) -> list[str]:
    """The in-season resolve on a release, through heavy.sh (`RF.refresh_cmd`: the table's command). `check`: resolve
    and compare, write nothing (the rehearsal)."""
    return ["bash", "scripts/heavy.sh", "inseason-resolve", "30", "--",
            *RF.refresh_cmd(RF.INSEASON, release_root() / rid), *(["--check"] if check else [])]


def inseason_check(rid: str) -> tuple[str, str]:
    """The rehearsal's question (`resolve --check`, nothing written): ("same" | "change" | "unresolved" | "error", its
    own line). "change": the real run rewrites and commits the file; "unresolved": the real run stops."""
    got = subprocess.run(inseason_cmd(rid, check=True), cwd=REPO_ROOT, capture_output=True, text=True)
    line = next((ln for ln in got.stdout.splitlines() if ln.startswith("in-season matches on")), "")
    if not line:
        return "error", f"rc={got.returncode}: {(got.stdout + got.stderr).strip()[-600:]}"
    return ("unresolved" if "unresolved" in line else "same" if got.returncode == 0 else "change"), line


def recut_cmd(rid: str) -> list[str]:
    """(T666 item 2) every release-backed app fixture, re-cut on that release (`RF.refresh_cmd`: the table's command)."""
    return RF.refresh_cmd(RF.RECUT, release_root() / rid)


def follow_tests() -> tuple[list[str], list[str]]:
    """The tests that ask for the release under test, found by rule: (pipeline files, app files relative to app/)."""
    return RF.pipeline_tests(), RF.app_tests()


def app_check_steps() -> list[str]:
    """(T680) the `pnpm check:fast` gates, one by one (`RF.app_check_steps`)."""
    return RF.app_check_steps()


def follow_tests_cmds(py: list[str], out: Path) -> list[tuple[str, list[str], Path | None]]:
    """(label, command, its report file or None) for every check run on the candidate after the re-cut: the pipeline
    tests found by rule, then (T680) the WHOLE app check — the app suite and the worker suite (through heavy.sh: they
    read the whole release) and each `check:fast` gate on its own — so every failure is found in one pass."""
    heavy = ["bash", "scripts/heavy.sh", "release-follow-tests", "30", "--"]
    return ([("pipeline", heavy + RF.pipeline_tests_cmd(py, out / "pipeline.xml"), out / "pipeline.xml")] if py else []) \
        + [("app", heavy + RF.app_suite_cmd(out / "app.json"), out / "app.json"),
           ("worker", heavy + RF.worker_suite_cmd(out / "worker.json"), out / "worker.json")] \
        + [(f"app gate {s}", RF.app_check_cmd(s), None) for s in app_check_steps()]


def candidate_failures(rid: str, env: dict) -> list[str]:
    """(T680) run every check of `follow_tests_cmds` on the candidate `rid` — each one, whatever the one before did —
    and return EVERY failure, one line each (a test by name where its report says so; else the check and its rc: a
    failure is never dropped for want of a report)."""
    out = HOME / "checks" / rid / "tests"
    shutil.rmtree(out, ignore_errors=True)       # last run's reports must not speak for this one
    out.mkdir(parents=True, exist_ok=True)
    py, _app = follow_tests()
    fails: list[str] = []
    for label, cmd, report in follow_tests_cmds(py, out):
        rc = run_cmd(cmd, env=env)
        if not rc:
            continue
        named: list[str] = []
        if report is not None and report.is_file():
            try:
                named = RF.report_failures(report)
            except (ValueError, OSError, SyntaxError) as e:     # json / xml unreadable: said, and the rc line stands
                print(f"    could not read {rel(report)}: {type(e).__name__}: {e}", flush=True)
        fails += [f"{label}: {x}" for x in named] or [
            f"{label}: rc={rc}" + ("" if report is None else " (no failed test in its report: read its output above)")]
    return fails


def recut_changed() -> list[str]:
    """The fixture paths the re-cut left different from HEAD."""
    return RF.changed(RF.RECUT)


def recut_token(paths: list[str]) -> str:
    return RF.content_token(paths)


def commit_recut(rid: str, kind: str) -> str:
    """Commit the re-cut fixtures ALONE (the table's paths); the new HEAD."""
    return RF.commit(RF.RECUT, rid, kind)


def goldens_paired(release_dir: Path) -> tuple[bool, str]:
    """Are the app's goldens cut from that release (`scripts/check-goldens-paired.py`, RELEASE_DIR)? (yes?, what it said)."""
    got = subprocess.run([sys.executable, "scripts/check-goldens-paired.py"], cwd=REPO_ROOT, capture_output=True,
                         text=True, env={**os.environ, RF.RELEASE_ENV: str(release_dir)})
    return got.returncode == 0, (got.stdout + got.stderr).strip()


def tool_says(cmd: list[str]) -> tuple[int, str]:
    """A tool's own word (rc, output), or (127, why) when it is not installed."""
    try:
        got = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.TimeoutExpired) as e:
        return 127, f"{type(e).__name__}: {e}"
    return got.returncode, (got.stdout + got.stderr).strip()


def staging_prechecks(rid: str) -> list[str]:
    """(T666 item 6) deploy/staging/upload.sh's own first refusals, asked before it starts and in the rehearsal: the
    run-timing base is the one for this release's tiles, `gh` can start the workflow, the rclone remote exists. Each
    is a problem line; [] when the upload can begin. (Its app, site and CI gates are checks of their own.)"""
    out = []
    tiles = (((manifest_of(rid).get("tiles") or {}).get("current")) or {}).get("id")
    base = Path(GENERATED.runtiming) / "feed" / "release" / "base.json"
    if not base.is_file():
        out.append(f"no run-timing base at {rel(base)} — run `python -m pipeline.deliver all` (the build's deliver stage)")
    else:
        tv = json.loads(base.read_text(encoding="utf-8")).get("tile_version")
        if tv != tiles:
            out.append(f"the run-timing base {rel(base)} is for tiles {tv}, the release's tiles are {tiles}")
    rc, said = tool_says(["gh", "auth", "status"])
    if rc:
        out.append(f"`gh auth status` failed (rc={rc}): the upload starts a GitHub workflow — {said.splitlines()[0] if said else 'no output'}")
    rc, said = tool_says(["rclone", "listremotes"])
    if rc or "r2:" not in said.split():
        out.append("the rclone remote `r2:` is not configured (`rclone listremotes`" + (f", rc={rc}" if rc else "") + ")")
    return out


def smoke_cmd() -> list[str]:
    return ["node", "app/tools/smoke-staging.mjs", "--shots", "app/dist/smoke-staging"]


def inseason_dirty() -> bool:
    """Is the in-season matches file changed in the working tree (the resolve wrote it)?"""
    return subprocess.run(["git", "diff", "--quiet", "--", str(INSEASON)], cwd=REPO_ROOT).returncode == 1


def commit_inseason(rid: str) -> str:
    """Commit the in-season matches file ALONE (explicit path); the new HEAD."""
    subprocess.run(["git", "commit", "-q", "-m", f"inseason: matches resolved on {rid} (pipeline release --staging, "
                    "T537)\n\nCo-Authored-By: Claude <noreply@anthropic.com>", "--", str(INSEASON)], cwd=REPO_ROOT,
                   check=True)
    return git_head()


def staging_release() -> str | None:
    """The release staging serves now (its live pointer's `data_release`). Raises when it cannot be read: a prod
    promote never goes ahead on an unread staging."""
    import urllib.request
    req = urllib.request.Request(STAGING_MANIFEST_URL, headers={"User-Agent": "bc-fishing-release",
                                                                "Cache-Control": "no-cache"})   # the default UA is 403
    with urllib.request.urlopen(req, timeout=30) as f:   # noqa: S310 — a fixed https URL
        return json.loads(f.read().decode("utf-8")).get("data_release")


def run_cmd(cmd: list[str], env: dict | None = None) -> int:
    """Run a command from the repo root on the console; its return code."""
    print(f"    $ {' '.join(rel(c) if c.startswith(str(REPO_ROOT)) else c for c in cmd)}", flush=True)
    return subprocess.run(cmd, cwd=REPO_ROOT, env={**os.environ, **(env or {})}).returncode


def build_cmd(dry_run: bool = False, jobs: int = 2) -> list[str]:
    inner = [sys.executable, "-m", "pipeline", "build", "--hold-latest", "--jobs", str(jobs)]
    if dry_run:
        return inner + ["--dry-run"]
    return ["bash", "scripts/heavy.sh", "release", str(HEAVY_MIN_FREE), "--", *inner]


def upload_cmd(rid: str) -> list[str]:
    return ["bash", "deploy/staging/upload.sh", rid]


def prod_cmd(rid: str) -> list[str]:
    """(T404) the ONE production upload: deploy/prod/upload.sh promotes the release staging serves (same manifest bytes,
    run timing, map assets, feed seeds, inputs; the pointer last, the previous one kept for deploy/prod/rollback.sh)."""
    return ["bash", "deploy/prod/upload.sh", rid]


# deploy/prod/upload.sh's own guard; release --prod has asked for CONFIRM_PROD=<id> and the typed id before it is set
PROD_UPLOAD_ENV = {"CONFIRM_PROD": "canifishthis.ca"}


def load_ledger() -> dict:
    return json.loads(LEDGER.read_text(encoding="utf-8")) if LEDGER.exists() else {"releases": {}}


def save_ledger(led: dict) -> None:
    LEDGER.parent.mkdir(parents=True, exist_ok=True)
    tmp = LEDGER.with_suffix(".tmp")
    tmp.write_text(json.dumps(led, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(LEDGER)


def rel(p) -> str:
    """A path as the human types it: relative to the repo root when it is under it."""
    try:
        return str(Path(p).relative_to(REPO_ROOT))
    except ValueError:
        return str(p)


def now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def manifest_of(rid: str) -> dict:
    return json.loads((release_root() / rid / "manifest.json").read_text(encoding="utf-8"))


def reach_digest_of(run_dir: Path) -> str | None:
    rep = Path(run_dir) / "report.json"
    return json.loads(rep.read_text(encoding="utf-8")).get("digest") if rep.exists() else None


def fingerprint(rid: str) -> str:
    """The release folder by content: every data file's bytes, and the manifest without its upload stamps — what
    `--prod` proves is still the artifact that passed the checks."""
    d = release_root() / rid
    h = hashlib.sha256()
    for p in sorted(x for x in d.rglob("*") if x.is_file()):
        h.update(str(p.relative_to(d)).encode())
        if p.name == "manifest.json":
            m = json.loads(p.read_text(encoding="utf-8"))
            h.update(json.dumps({k: v for k, v in m.items() if k not in STAMP_KEYS}, sort_keys=True).encode())
        else:
            h.update(p.read_bytes())
    return h.hexdigest()[:16]


def file_sha(p: Path) -> str:
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()[:16]


#: the binding diff's own words for WHERE it compared, not WHAT differs: left out of its content key
DIFF_PATH_KEYS = ("before", "after")


def diff_content_sha(p: Path) -> str | None:
    """A binding_diff.json by CONTENT — the rules / unresolved / licensing changes and their counts, not the before /
    after path strings (2026-11-09: a diff identical in content was refused because its `after` was relative in one
    and absolute in the other). None when the file is not a diff json."""
    try:
        rep = json.loads(Path(p).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(rep, dict):
        return None
    body = {k: v for k, v in rep.items() if k not in DIFF_PATH_KEYS}
    return hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()[:16]


# ------------------------------------------------------------------------------------------------------------ checks

def manifest_problems(rid: str) -> list[str]:
    """Manifest sanity: it is THIS release, every file it names is on disk at its size, the size gate holds, the
    tiles / basemap / synopsis it names exist (`publish.plan` — the upload's own list), and its bundle block names
    the reach run on disk."""
    from pipeline.deliver.release import build as RB
    from pipeline.deliver.release.manifest import FORMAT
    from pipeline.deliver.release.publish import plan as publish_plan
    root, out = release_root(), []
    try:
        m = manifest_of(rid)
    except (OSError, ValueError) as e:
        return [f"manifest.json unreadable: {e}"]
    if m.get("format") != FORMAT:
        out.append(f"format is {m.get('format')!r}, not {FORMAT}")
    if m.get("data_release") != rid:
        out.append(f"data_release is {m.get('data_release')!r}, the folder is {rid}")
    for name, e in sorted((m.get("files") or {}).items()):
        p = root / e["url"]
        if not p.is_file():
            out.append(f"files.{name}: {e['url']} is missing")
        elif e.get("gz") is not None and p.stat().st_size != e["gz"]:
            out.append(f"files.{name}: {e['url']} is {p.stat().st_size:,} B, the manifest says {e['gz']:,}")
    try:
        publish_plan(root / rid, root)
    except SystemExit as e:
        out.append(str(e))
    out += [f"size gate: {p}" for p in RB.gate_problems(root / rid)]
    b = m.get("bundle") or {}
    on_disk = reach_digest_of(reach_dir())
    if not b.get("reach_digest"):
        out.append("the manifest names no bundle reach_digest")
    elif b["reach_digest"] != on_disk:
        out.append(f"the manifest's reach_digest {b['reach_digest']} is not the reach run on disk ({on_disk}): "
                   "the build did not finish, or the reach run was rerun after it")
    return out


def app_gate_problems(rid: str, min_app: str | None) -> list[str]:
    from pipeline.deliver.release.app_gate import problems
    return problems(manifest_of(rid), min_app)


def site_problems(min_app: str | None) -> tuple[list[str], str]:
    """The live staging site against the oldest app the release will be stamped for (`deliver.release.site_gate`)."""
    from pipeline.deliver.release.site_gate import check
    return check(None, min_app)


def binding_diff(before: Path, after: Path) -> dict:
    from pipeline.atlas.reach.binding_diff import diff_runs
    return diff_runs(before, after)


def gone_ids(before_rid: str, after_rid: str) -> tuple[dict, str]:
    """(T555 item 5) The item ids of the live release that the candidate neither holds nor aliases, and the list as text."""
    from pipeline.deliver.release.gone_ids import compare, summary
    rep = compare(release_root() / before_rid, release_root() / after_rid)
    return rep, summary(rep, before_rid, after_rid)


def diff_summary(rep: dict) -> str:
    from pipeline.atlas.reach.binding_diff import summary
    return summary(rep)


# -------------------------------------------------------------------------------------------------------- the stages

@dataclass
class Ctx:
    accept_diff: Path | None = None
    no_fast_fail: str | None = None  # why scripts/fast-fail.sh is skipped (printed); None runs it
    stale_atlas: str | None = None   # (T547) why an atlas behind its curated inputs ships anyway (printed); None stops
    no_ci_check: str | None = None   # (T533) why CI's result is not asked (printed); None asks
    baseline: Path | None = None
    min_app: str | None = None
    jobs: int = 2
    reupload: bool = False
    accept_fixtures: str | None = None   # (T666) the token of the re-cut whose changed golden pages were read
    push: bool = False                   # (T666) the release pushes the commits it made itself
    no_follow_tests: str | None = None   # (T666) why the tests on the candidate are skipped (printed); None runs them
    rehearse_follows: bool = False       # (T680) --dry-run: re-cut + the whole check on the candidate, put back after
    no_fresh_copy: str | None = None     # (T666) why the fresh-copy test run is skipped (printed); None runs it
    forget: bool = False                 # (T666) drop the remembered flags of the release in flight
    heavy_env: dict = field(default_factory=dict)   # (T535) tells heavy.sh fast-fail ran: FAST_FAIL_DONE / HEAVY_NO_FAST_FAIL
    led: dict = field(default_factory=dict)
    rid: str | None = None          # the candidate, once the build named it
    live: str | None = None         # LATEST when the run started


def rec(ctx: Ctx, rid: str) -> dict:
    return ctx.led["releases"].setdefault(rid, {})


def baseline_dir(live: str) -> Path:
    return HOME / "baseline" / live


# ---- preflight: ONE table of checks (T666 item 3). The real run and the rehearsal both walk it, so the rehearsal
# predicts the run by construction; every check runs, and every failure is listed.

def check_committed(ctx: Ctx) -> Finding:
    dirty = git_dirty()
    if dirty:
        return Finding(False, f"{len(dirty)} uncommitted path(s) the build reads: " + ", ".join(dirty[:8])
                       + (" …" if len(dirty) > 8 else ""),
                       "commit them (a release is built from committed code and curated data only; marks.jsonl may "
                       "stay dirty)")
    return Finding(True, f"code + data/curated committed (HEAD {git_head()[:12]}; marks.jsonl / closed.json may be dirty)")


def check_atlas(ctx: Ctx) -> Finding:
    try:
        behind = atlas_behind()
    except (OSError, ValueError) as e:
        return Finding(False, f"cannot tell whether the atlas is current with its curated inputs: {e}",
                       "run `python -m pipeline.atlas.inputs` and fix what it says")
    if behind and ctx.stale_atlas is None:
        return Finding(False, f"the atlas is behind {len(behind)} input(s) it was built from (T547):\n    "
                       + "\n    ".join(behind),
                       "rebuild the atlas (`python -m pipeline build --atlas --promote`); or ship the old atlas with "
                       "--ship-stale-atlas REASON")
    if behind:
        return Finding(True, f"atlas behind {len(behind)} input(s), SHIPPED ANYWAY (--ship-stale-atlas): {ctx.stale_atlas}")
    return Finding(True, "the atlas is current with its curated inputs")


def check_marks(ctx: Ctx) -> Finding:
    try:
        landed, said = marks_landed()
    except RuntimeError as e:
        return Finding(False, str(e), "run `python -m pipeline.regs.review.apply --check` and fix what it says")
    if not landed:
        return Finding(False, "review marks in force have not landed in their curated files:\n" + said,
                       "land them (`python -m pipeline.regs.review.apply`, then commit a file it names), or put a mark "
                       "that must wait on NOT_LANDED_OK (pipeline/regs/review/apply.py)")
    return Finding(True, said.splitlines()[-1] if said else "review marks: every mark in force has landed")


def check_pushed(ctx: Ctx) -> Finding:
    try:
        pushed = git_pushed()
    except (subprocess.CalledProcessError, RuntimeError) as e:
        return Finding(False, f"could not tell whether HEAD is pushed: {e}",
                       f"check the network and `git fetch {GH_REMOTE} {GH_BRANCH}`")
    if not pushed:
        return Finding(False, f"HEAD {git_head()[:12]} is not on {GH_REMOTE}/{GH_BRANCH} (the staging upload starts "
                       "a GitHub workflow that runs GitHub's copy)",
                       f"git push {GH_REMOTE} HEAD:{GH_BRANCH} (after `build-apk.sh --play`, that pushes its app.json "
                       "commit)")
    return Finding(True, f"HEAD is on {GH_REMOTE}/{GH_BRANCH}")


def check_ci(ctx: Ctx) -> Finding:
    rc, said = ci_status(ctx.no_ci_check)
    if rc:
        return Finding(False, ("CI is red on HEAD (or its newest finished run)" if rc == 1 else "could not read CI (gh)")
                       + ":\n" + said,
                       "read the run named above and fix it (push the fix, let CI finish); --no-ci-check REASON goes "
                       "on and prints the reason")
    return Finding(True, said)


def check_live(ctx: Ctx) -> Finding:
    ctx.live = read_pointer("LATEST")
    if ctx.live is None:
        return Finding(False, f"no {release_root() / 'LATEST'}: there is no live release to diff against",
                       "build the first release with `python -m pipeline build` (it writes LATEST)")
    return Finding(True, f"live release {ctx.live}")


def check_site(ctx: Ctx) -> Finding:
    # (T483 fast-fail; caught 2026-11-09) the staging site older than the min app the release goes up with: found only by
    # the smoke test after the upload. Checked here, before the build; the upload checks again with the release's own
    # format needs (deploy/staging/upload.sh, `deliver.release.site_gate`).
    min_app = ctx.min_app or resume_min_app(ctx.led, read_pointer("CANDIDATE") or ctx.live, ctx.live)
    try:
        bad, said = site_problems(min_app)
    except Exception as e:  # noqa: BLE001 — shown as a stop: an unreadable site is never a pass (site_gate)
        return Finding(False, f"the staging site gate could not run ({type(e).__name__}: {e})",
                       "check the network; `python -m pipeline.deliver.release.site_gate <manifest>` asks alone")
    if bad:
        return Finding(False, "; ".join(bad), "redeploy the staging site (the commands above)")
    return Finding(True, said)


def follows_env(ctx: Ctx) -> dict:
    """(T666 item 2) RELEASE_DIR for the checks that ask for the release under test: the CANDIDATE once the fixtures
    were re-cut on it (ledger `follows.cut_for`) — from then until the promote the goldens pair with the candidate, not
    with LATEST. {} otherwise (LATEST)."""
    cand = read_pointer("CANDIDATE")
    cut_for = (((ctx.led.get("releases") or {}).get(cand or "") or {}).get("follows") or {}).get("cut_for")
    return {RF.RELEASE_ENV: str(release_root() / cand)} if cand and cut_for == cand else {}


def fast_fail_base(ctx: Ctx) -> str | None:
    """What fast-fail checks as "changed". (T666 item 5) the commit it LAST PASSED on, when one is recorded: that pass
    covered everything up to it, so only what changed since is checked (a fixtures or docs commit reruns no test tier).
    Not usable when a file that was uncommitted at that pass has since gone back to that commit's content (the commit's
    version was never checked). Else (T533) the commit the live release (LATEST) was built from (ledger `commit`):
    everything since the last ship. None (said) when neither is recorded."""
    done = ctx.led.get("fast_fail") or {}
    prev = done.get("head")
    if prev and done.get("key") and git_has_commit(prev):
        still = changed_since(prev)
        reverted = [x for x in done.get("dirty") or [] if x not in still]
        if not reverted:
            print(f"    fast-fail base: {prev[:12]}, the commit it last passed on ({done.get('at')}): only what changed "
                  f"since is checked ({len(still)} tracked path(s)) — that pass covered everything before it", flush=True)
            return prev
        print(f"    fast-fail: {len(reverted)} file(s) uncommitted at its last pass are back at {prev[:12]}'s content "
              f"({', '.join(reverted[:3])}{' …' if len(reverted) > 3 else ''}), which that pass never checked: the "
              "ship base instead", flush=True)
    base = ((ctx.led.get("releases") or {}).get(ctx.live or "") or {}).get("commit")
    if base and git_has_commit(base):
        print(f"    fast-fail base: {base[:12]}, the commit {ctx.live} was built from", flush=True)
        return base
    print(f"    fast-fail base: no commit recorded for {ctx.live} in {rel(LEDGER)}"
          + (f" ({base[:12]} is not in this clone)" if base else "") + ": its default (unpushed commits) — NOT since "
          "the last ship", flush=True)
    return None


def check_fast_fail(ctx: Ctx) -> Finding:
    """(T535) fast-fail, after the instant checks. (T666 item 5) keyed on the tree's content: a rerun on the same
    content skips it whatever HEAD is; the build's heavy.sh is told it ran (FAST_FAIL_DONE) or why not."""
    head = git_head()
    if ctx.no_fast_fail is not None:
        ctx.heavy_env = {"HEAVY_NO_FAST_FAIL": f"release --no-fast-fail: {ctx.no_fast_fail}"}
        return Finding(True, f"fast-fail SKIPPED (--no-fast-fail): {ctx.no_fast_fail}")
    env, key = follows_env(ctx), tree_key()
    under = env.get(RF.RELEASE_ENV)
    done = ctx.led.get("fast_fail") or {}
    ctx.heavy_env = {"FAST_FAIL_DONE": head}
    if done.get("key") == key and done.get("release_dir") == under:
        return Finding(True, f"fast-fail passed for this tree's content ({key}, at {done.get('head', '')[:12]}, "
                       f"{done.get('at')}): not rerun")
    if under:
        print(f"    fast-fail reads the release under test: {RF.RELEASE_ENV}={rel(under)} (the fixtures were re-cut "
              "on the candidate)", flush=True)
    rc, failed = run_fast_fail(fast_fail_base(ctx), env)
    if rc:
        ctx.heavy_env = {}
        steps = "".join(f"\n    - {f}" for f in failed) if failed else " (it stopped before its step list: read its output)"
        return Finding(False, f"scripts/fast-fail.sh failed (rc={rc}): {len(failed)} step(s){steps}",
                       "read each failed step's output above and fix it (one alone: FAST_FAIL_ONLY='<part of its "
                       "name>' scripts/fast-fail.sh); --no-fast-fail REASON skips it and prints the reason")
    ctx.led["fast_fail"] = {"head": head, "key": key, "at": now(), "dirty": dirty_paths(), "release_dir": under}
    save_ledger(ctx.led)
    return Finding(True, "fast-fail passed")


def check_fresh_copy(ctx: Ctx) -> Finding:
    """(T666; owner 2026-11-11: the clean-checkout test run happens once, in the RELEASE pre-check, not in every quick
    check.) HEAD cloned into a temp folder, the pipeline's no-data tier run there as CI runs it. Once per committed
    tree (ledger `fresh_copy`); after a pass it runs again only when a commit since touched a path that is NOT one the
    release itself refreshes (`release_follows.FOLLOWS`: its own fixtures and matches commits never rerun it)."""
    if ctx.no_fresh_copy is not None:
        return Finding(True, f"fresh-copy test run SKIPPED (--no-fresh-copy): {ctx.no_fresh_copy}")
    tree, head = head_tree(), git_head()
    done = ctx.led.get("fresh_copy") or {}
    if done.get("tree") == tree:
        return Finding(True, f"fresh-copy test run passed for this commit's tree ({done.get('at')}): not rerun")
    if done.get("head") and git_has_commit(done["head"]):
        other = sorted(x for x in committed_since(done["head"]) if RF.row_of(x) is None)
        if not other:
            return Finding(True, f"fresh-copy test run passed at {done['head'][:12]} ({done.get('at')}); the commits "
                           "since touch only what the release refreshes (fixtures, matches): not rerun")
        print(f"    fresh-copy: {len(other)} path(s) committed since its pass at {done['head'][:12]} "
              f"({', '.join(other[:3])}{' …' if len(other) > 3 else ''}): it runs again", flush=True)
    rc = run_fresh_copy()
    if rc:
        return Finding(False, f"the fresh-copy test run failed (rc={rc}): on a clean clone of HEAD {head[:12]} the "
                       "pipeline's no-data tests do not pass as they do here",
                       "read the failing tests above: each reads something this checkout has and git does not (a "
                       "build, a cache, an uncommitted file) — fix the test or commit the file; "
                       "`scripts/fresh-copy-tests.sh --only <regex>` reruns a few; --no-fresh-copy REASON skips it "
                       "and prints the reason")
    ctx.led["fresh_copy"] = {"tree": tree, "head": head, "at": now()}
    save_ledger(ctx.led)
    return Finding(True, f"fresh-copy test run passed on a clean clone of HEAD {head[:12]}")


#: every preflight check, in order: (name, check). The instant ones first; then the two that take minutes: fast-fail,
#: and last the fresh-copy test run (the whole no-data tier on a clean clone, once per release).
PREFLIGHT = (("committed", check_committed), ("atlas", check_atlas), ("marks", check_marks), ("pushed", check_pushed),
             ("ci", check_ci), ("live", check_live), ("site", check_site), ("fast-fail", check_fast_fail),
             ("fresh-copy", check_fresh_copy))


def preflight_findings(ctx: Ctx, indent: str = "    ", stop_word: str = "STOP") -> list[tuple[str, Finding]]:
    """Run EVERY preflight check and say each result as it is found (so an instant failure shows in its first second,
    while the slower checks still run to complete the list)."""
    out: list[tuple[str, Finding]] = []
    for name, check in PREFLIGHT:
        bad = [n for n, f in out if not f.ok]
        if name == "fast-fail" and bad and ctx.no_fast_fail is None:
            print(f"{indent}{len(bad)} check(s) failed already ({', '.join(bad)}); fast-fail still runs so the list "
                  "is complete (Ctrl-C to stop here)", flush=True)
        f = check(ctx)
        out.append((name, f))
        print(f"{indent}{'ok' if f.ok else stop_word}: " + f.said.replace("\n", "\n" + indent), flush=True)
    return out


def stage_preflight(ctx: Ctx) -> str:
    found = preflight_findings(ctx)
    bad = [(f.said, f.todo) for _, f in found if not f.ok]
    if bad:
        raise Stop("preflight", bad[0][0] if len(bad) == 1 else f"{len(bad)} checks failed", bad[0][1], problems=bad)
    return f"clean at {git_head()[:12]}; live release {ctx.live}; {dict(found)['fast-fail'].said}"


def keep_baseline(live: str, reach: Path | None = None) -> tuple[bool, str]:
    """Keep the reach run LATEST (`live`) was cut from under baseline/<live>/ — (kept?, what happened). Not kept when the
    run on disk is another one and no copy exists: the "before" is already gone. One copy of this rule: `stage_baseline`
    and (2026-11-09) `pipeline build`, which keeps it before a reach rerun outside a release overwrites the run."""
    reach = reach_dir() if reach is None else Path(reach)
    live_digest = (manifest_of(live).get("bundle") or {}).get("reach_digest")
    dest = baseline_dir(live)
    if reach_digest_of(dest) == live_digest:
        return True, f"kept: {dest} (reach {live_digest})"
    on_disk = reach_digest_of(reach)
    if on_disk != live_digest:
        # CANDIDATE from an earlier failed run, or a reach run done outside a release: the "before" is gone
        return False, (f"the reach run on disk ({on_disk}) is not the one the live release {live} was cut from "
                       f"({live_digest}), and no copy of that one was kept")
    tmp = dest.with_name(dest.name + ".tmp")
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir(parents=True)
    for n in BASELINE_FILES:
        if (reach / n).exists():
            shutil.copy2(reach / n, tmp / n)
    shutil.rmtree(dest, ignore_errors=True)
    tmp.replace(dest)
    return True, f"copied the live reach run ({live_digest}) to {dest}"


def stage_baseline(ctx: Ctx) -> str:
    """Keep the reach run LATEST was cut from, before the build can overwrite it (the binding diff's "before")."""
    live_digest = (manifest_of(ctx.live).get("bundle") or {}).get("reach_digest")
    if ctx.baseline is not None:
        got = reach_digest_of(ctx.baseline)
        if got == live_digest:
            # (T666 item 4) kept under baseline/<live>/ like any other, so a rerun needs no --baseline
            kept, msg = keep_baseline(ctx.live, reach=ctx.baseline)
            return f"--baseline {ctx.baseline} (reach {got}); {msg}"
        if (ctx.led.get("releases", {}).get(ctx.live) or {}).get("promoted_at"):
            # (T666 item 4; build 27) the flag of the run BEFORE the promote: LATEST is this command's own release now
            print(f"    --baseline {ctx.baseline} (reach {got}) is not for {ctx.live}, which this command promoted "
                  "already: left over from before the promote, IGNORED", flush=True)
            ctx.baseline = None
        else:
            raise Stop("baseline", f"--baseline {ctx.baseline} is reach run {got}, the live release {ctx.live} was "
                       f"cut from {live_digest}", "name the reach run the live release was built from")
    kept, msg = keep_baseline(ctx.live)
    if not kept:
        raise Stop("baseline", msg,
                   f"name a reach run whose report.json digest is {live_digest} with --baseline DIR "
                   f"(e.g. data/generated/reaches/full_prev), or rebuild LATEST's run")
    return msg


def stage_build(ctx: Ctx) -> str:
    rc = run_cmd(build_cmd(jobs=ctx.jobs), env=ctx.heavy_env or None)
    if rc:
        raise Stop("build", f"`pipeline build` failed (rc={rc})",
                   "read the build output above (`python -m pipeline build --status` lists each stage and its log), "
                   "fix it, commit, rerun: finished stages are skipped")
    ctx.rid = read_pointer("CANDIDATE") or read_pointer("LATEST")
    if not ctx.rid or not (release_root() / ctx.rid / "manifest.json").exists():
        raise Stop("build", "the build finished but names no release (CANDIDATE / LATEST)",
                   "look at data/release/ — `python -m pipeline build --status`")
    r = rec(ctx, ctx.rid)
    r.setdefault("built_at", now())
    r.setdefault("commit", git_head())
    # (T484) the release LATEST named before this one's promote: the min app default on a rerun after the promote
    r.setdefault("live_before", ctx.live)
    save_ledger(ctx.led)
    return f"release {ctx.rid}" + (" (already LATEST)" if ctx.rid == ctx.live else "")


def stage_checks(ctx: Ctx) -> str:
    rid, r = ctx.rid, rec(ctx, ctx.rid)
    base = ctx.baseline or baseline_dir(ctx.live)
    probs = manifest_problems(rid)
    if probs:
        raise Stop("checks", "manifest sanity: " + "; ".join(probs),
                   "the release folder is not what its manifest says: rebuild it (`python -m pipeline build --force "
                   "deliver --hold-latest`), then rerun")
    probs = app_gate_problems(rid, ctx.min_app)
    if probs:
        raise Stop("checks", "app-version gate: " + "; ".join(probs),
                   "rerun with --min-app-version X.Y.Z (the oldest app that reads these formats)")
    after_digest = (manifest_of(rid).get("bundle") or {}).get("reach_digest")
    before_digest = reach_digest_of(base) if rid != ctx.live else after_digest
    out_dir = HOME / "checks" / rid
    diff_key = f"{before_digest}->{after_digest}"
    done = r.get("binding_diff") or {}
    if done.get("key") != diff_key:
        if before_digest == after_digest:
            done = {"key": diff_key, "status": "same", "note": "same reach run"}
        else:
            rep = binding_diff(base, reach_dir())
            out_dir.mkdir(parents=True, exist_ok=True)
            js, txt = out_dir / "binding_diff.json", out_dir / "binding_diff.txt"
            js.write_text(json.dumps(rep, indent=1) + "\n", encoding="utf-8")
            txt.write_text(diff_summary(rep) + "\n", encoding="utf-8")
            from pipeline.atlas.reach.binding_diff import differs as _differs      # T546 item 8: + DFO, diagnostics
            differs = _differs(rep)
            done = {"key": diff_key, "status": "differs" if differs else "same", "json": str(js),
                    "sha": file_sha(js), "content": diff_content_sha(js), "summary": str(txt)}
        r["binding_diff"] = done
        save_ledger(ctx.led)
    if done["status"] == "differs":
        # (2026-11-09) by content, not bytes: the same diff written with another before/after path is the same diff.
        # A ledger row from before this has no "content": its own json's content stands in (as written, `sha`).
        want = done.get("content") or (diff_content_sha(Path(done["json"]))
                                       if Path(done["json"]).is_file() and file_sha(Path(done["json"])) == done["sha"]
                                       else None)
        if ctx.accept_diff is not None and Path(ctx.accept_diff).is_file() and want is not None \
                and diff_content_sha(Path(ctx.accept_diff)) == want:
            done.update(status="accepted", accepted_at=now())
            save_ledger(ctx.led)
        else:
            print(Path(done["summary"]).read_text(encoding="utf-8"))
            why = ("" if ctx.accept_diff is None else
                   f" (--accept-diff {ctx.accept_diff} is not this diff: its content differs from {done['json']})")
            raise Stop("checks", f"the rule bindings differ from the live release {ctx.live}{why}",
                       f"read {done['summary']} (full: {done['json']}); if every change is intended, rerun with\n"
                       f"    --accept-diff {done['json']}\n  otherwise fix the cause, commit, and rerun")
    # (T555 item 5) ids a reader may have kept (a spot's water, an alert's, a link) that this release neither holds nor
    # aliases: LISTED, never a stop (ids do go between builds; whether one needed an alias is read off the list)
    n_gone = 0
    if rid != ctx.live and ctx.live:
        rep, txt = gone_ids(ctx.live, rid)
        n_gone = len(rep["gone"])
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "gone_ids.json").write_text(json.dumps(rep, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
        (out_dir / "gone_ids.txt").write_text(txt + "\n", encoding="utf-8")
        print(txt if n_gone <= 40 else "\n".join(txt.splitlines()[:41]) + f"\n  … {n_gone - 40} more: {out_dir / 'gone_ids.txt'}")
    r["checks"] = {"manifest": "ok", "app_gate": "ok", "min_app_version": ctx.min_app,
                   "binding_diff": done["status"], "gone_ids": n_gone, "at": now()}
    r["fingerprint"] = fingerprint(rid)
    save_ledger(ctx.led)
    return (f"manifest ok, app gate ok (min app {ctx.min_app or 'manifest stamp'}), bindings {done['status']}, "
            f"{n_gone} item id(s) gone with no alias")


def stage_inseason(ctx: Ctx, rid: str, r: dict) -> str:
    """(T537) resolve the in-season matches on the candidate BEFORE the promote (was: mid-upload, after LATEST moved,
    and the rewritten file then dirtied the next preflight). Done once per release (ledger `inseason`). A rewritten
    file is committed alone; (T666) the run goes on, and the push gate at the end of the stage asks for it once."""
    if (r.get("inseason") or {}).get("release") == rid:
        return f"in-season resolved on {rid} ({r['inseason'].get('at')})"
    rc = run_cmd(inseason_cmd(rid), env=ctx.heavy_env or None)
    if rc:
        raise Stop("follows", f"the in-season matches do not all resolve on {rid} (rc={rc}); LATEST was not moved",
                   "re-match the unresolved notices in the review app's In-season tab (the output above names them), "
                   "commit data/curated/inseason/notice_waters.json, then rerun")
    if inseason_dirty():
        head = commit_inseason(rid)
        r["inseason"] = {"release": rid, "at": now(), "commit": head}
        made_commit(r, head)
        save_ledger(ctx.led)
        return f"in-season re-resolved on {rid}, committed {head[:12]} ({rel(INSEASON)} only)"
    r["inseason"] = {"release": rid, "at": now()}
    save_ledger(ctx.led)
    return f"in-season resolved on {rid}"


def made_commit(r: dict, head: str) -> None:
    """Record a commit THE RELEASE made (ledger `follows.commits`): what `--push` may push."""
    r.setdefault("follows", {}).setdefault("commits", []).append(head)


def stage_recut(ctx: Ctx, rid: str, r: dict) -> str:
    """(T666 item 2) re-cut the app's release-backed fixtures on the CANDIDATE and commit them with the release id.
    (T667 D5) a change that only renumbers is committed by itself; a changed golden page stops, nothing committed."""
    f = r.setdefault("follows", {})
    if (f.get("recut") or {}).get("release") == rid:
        return f"fixtures re-cut on {rid} ({f['recut'].get('kind')}, {f['recut'].get('at')})"
    rc = run_cmd(recut_cmd(rid), env=ctx.heavy_env or None)
    if rc:
        raise Stop("follows", f"the fixtures could not be re-cut on {rid} (rc={rc}); LATEST was not moved, nothing "
                   "was committed", "read the step that failed above (taps.py names each golden state the release "
                   "re-cut and the declaration it needs), fix it, commit, rerun")
    f["cut_for"] = rid          # from here to the promote the goldens pair with the candidate (`follows_env`)
    save_ledger(ctx.led)
    changed = recut_changed()
    if not changed:
        f["recut"] = {"release": rid, "at": now(), "kind": "unchanged", "paths": 0}
        save_ledger(ctx.led)
        return f"fixtures re-cut on {rid}: nothing changed"
    pages = RF.answer_paths(changed)
    token = recut_token(changed)
    print(f"    the re-cut changed {len(changed)} path(s): " + ", ".join(
        f"{row.name} {n}" for row in RF.FOLLOWS for n in [sum(RF.row_of(x) is row for x in changed)] if n), flush=True)
    if pages and token not in (ctx.accept_fixtures, f.get("accepted")):
        why = "" if ctx.accept_fixtures is None else f" (--accept-fixtures {ctx.accept_fixtures} is not this re-cut)"
        raise Stop("follows", f"the re-cut on {rid} changed {len(pages)} golden page(s) — what an angler is told "
                   f"changed{why}; nothing was committed:" + "".join(f"\n    {x}" for x in pages[:20])
                   + ("\n    …" if len(pages) > 20 else ""),
                   "read `git diff -- " + " ".join(x for row in RF.FOLLOWS if row.answers for x in row.paths)
                   + "`; if every change is intended, "
                   f"rerun with\n    --accept-fixtures {token}\n  otherwise fix the cause, commit, and rerun")
    kind = "golden pages accepted" if pages else "renumber only"
    if pages:
        f["accepted"] = token
    head = commit_recut(rid, kind)
    f["recut"] = {"release": rid, "at": now(), "kind": kind, "paths": len(changed), "commit": head}
    made_commit(r, head)
    save_ledger(ctx.led)
    return f"fixtures re-cut on {rid}: {len(changed)} path(s) committed {head[:12]} ({kind})"


def stage_follow_tests(ctx: Ctx, rid: str, r: dict) -> str:
    """(T666 item 2) the tests that ask for the release under test, run on the CANDIDATE (RELEASE_DIR), after the
    re-cut — so nothing compares with LATEST before the promote. Once per release and re-cut."""
    f = r.setdefault("follows", {})
    if ctx.no_follow_tests is not None:
        print(f"    tests on the candidate SKIPPED (--no-follow-tests): {ctx.no_follow_tests}", flush=True)
        return "tests on the candidate skipped (--no-follow-tests)"
    if (f.get("tests") or {}).get("release") == rid:
        return f"tests passed on {rid} ({f['tests'].get('at')})"
    py, _app = follow_tests()
    bad = candidate_failures(rid, {**ctx.heavy_env, **RF.test_env(release_root() / rid)})
    if bad:
        raise Stop("follows", failures_said(rid, bad) + "; LATEST was not moved, nothing was pushed",
                   f"fix them together, commit, rerun; one check alone: {RF.RELEASE_ENV}={rel(release_root() / rid)} "
                   "REGS_STORE_REQUIRED=1 in front of its command (above). --no-follow-tests REASON skips them and "
                   "prints the reason")
    f["tests"] = {"release": rid, "at": now(), "pipeline": len(py), "app": "all", "gates": len(app_check_steps())}
    save_ledger(ctx.led)
    return f"{len(py)} pipeline test file(s) + the whole app check passed on {rid}"


def failures_said(rid: str, bad: list[str], cap: int = 80) -> str:
    """(T680) every failure on the candidate, in one block."""
    return (f"{len(bad)} failure(s) on the candidate {rid} after the fixture re-cut — ALL of them:"
            + "".join(f"\n    {x}" for x in bad[:cap])
            + (f"\n    … and {len(bad) - cap} more (every one: the reports in {rel(HOME / 'checks' / rid / 'tests')})"
               if len(bad) > cap else ""))


def snapshot(roots: tuple[str, ...], into: Path) -> list[tuple[Path, Path | None]]:
    """(T680) a byte-for-byte copy of each repo path in `roots` (file or folder; None: it was not there)."""
    kept = []
    for i, r in enumerate(roots):
        src, dst = Path(REPO_ROOT) / r, into / str(i)
        if src.is_dir():
            shutil.copytree(src, dst, symlinks=True)
        elif src.exists():
            shutil.copy2(src, dst)
        kept.append((src, dst if src.exists() else None))
    return kept


def restore(kept: list[tuple[Path, Path | None]]) -> None:
    """Put each snapshotted path back exactly as it was (what the rehearsal's re-cut wrote is removed)."""
    for src, dst in kept:
        if src.is_dir() and not src.is_symlink():
            shutil.rmtree(src)
        elif src.exists() or src.is_symlink():
            src.unlink()
        if dst is not None:
            (shutil.copytree(dst, src, symlinks=True) if dst.is_dir() else shutil.copy2(dst, src))


def rehearse_follows(ctx: Ctx, cand: str) -> dict:
    """(T680) THE RE-CUT AND THE WHOLE CHECK, REHEARSED on a built candidate (`--dry-run --rehearse-follows`): the
    fixtures are re-cut IN PLACE on the candidate, every check of the follows stage runs on them, then every re-cut path
    is put back byte for byte — nothing committed, nothing kept. Says what the real run would: the changed paths, the
    golden pages and the token `--accept-fixtures` would take, and every failing test, all at once."""
    import tempfile
    before = recut_changed()
    with tempfile.TemporaryDirectory(prefix="release-rehearse-") as td:
        kept = snapshot(RF.paths_of(RF.RECUT), Path(td))
        try:
            rc = run_cmd(recut_cmd(cand), env=ctx.heavy_env or None)
            if rc:
                return {"recut_rc": rc}
            changed = recut_changed()
            got = {"recut_rc": 0, "changed": changed, "pages": RF.answer_paths(changed),
                   "token": recut_token(changed) if changed else None}
            got["failures"] = candidate_failures(cand, {**ctx.heavy_env, **RF.test_env(release_root() / cand)})
            return got
        finally:
            restore(kept)
            after = recut_changed()
            if after != before:                 # said, never passed over: the tree is not what it was
                print(f"    WARNING: the re-cut paths differ from before the rehearsal: {before} -> {after}; read "
                      f"`git status -- {' '.join(RF.paths_of(RF.RECUT))}`", flush=True)


def push_gate(ctx: Ctx, r: dict) -> str:
    """(T666) the upload's workflow runs GitHub's copy: every commit must be there before the promote. ONE stop with the
    one command — or, with --push, the release pushes, and only when every unpushed commit is one it made."""
    try:
        if git_pushed():
            return f"HEAD is on {GH_REMOTE}/{GH_BRANCH}"
    except (subprocess.CalledProcessError, RuntimeError) as e:
        raise Stop("follows", f"could not tell whether HEAD is pushed: {e}",
                   f"check the network and `git fetch {GH_REMOTE} {GH_BRANCH}`, then rerun")
    ours = set((r.get("follows") or {}).get("commits") or [])
    ahead = git_unpushed()
    theirs = [c for c in ahead if c not in ours]
    cmd = f"git push {GH_REMOTE} HEAD:{GH_BRANCH}"
    if ctx.push and not theirs:
        rc = git_push()
        if rc:
            raise Stop("follows", f"`{cmd}` failed (rc={rc})", f"fix what git said above, {cmd}, then rerun")
        return f"pushed the {len(ahead)} commit(s) the release made (--push)"
    what = (f"{len(ahead)} commit(s) are not on {GH_REMOTE}/{GH_BRANCH}: {len(ahead) - len(theirs)} the release made "
            "(in-season matches, re-cut fixtures)"
            + (f", {len(theirs)} made by someone else since preflight ({', '.join(c[:12] for c in theirs[:4])})"
               if theirs else ""))
    if (r.get("follows") or {}).get("recut", {}).get("commit"):
        what += (". Until the promote, the committed goldens are the candidate's: another lane's fast-fail reads them "
                 "as cut from another release than LATEST — finish this run")
    todo = (f"{cmd}, then rerun — only the promote and the upload are left"
            + (" (--push pushes only the release's own commits: these are not all its own)" if theirs
               else " (with --push the release pushes its own commits and goes on)"))
    raise Stop("follows", what, todo)


def stage_follows(ctx: Ctx) -> str:
    """(T666 items 1-2) REFRESH WHAT FOLLOWS THE RELEASE on the candidate, before the promote: the table is
    `pipeline/release_follows.py`. A release that is LATEST already (a rerun after the promote) refreshes the same way:
    every step is recorded per release id and skipped when done."""
    rid, r = ctx.rid, rec(ctx, ctx.rid)
    said = []
    for step in (stage_inseason, stage_recut, stage_follow_tests):
        said.append(step(ctx, rid, r))
        print(f"    {said[-1]}", flush=True)      # each as it finishes: a later stop must not hide what was done
    said.append(push_gate(ctx, r))
    return "; ".join(said)


def stage_promote(ctx: Ctx) -> str:
    from pipeline.deliver.release.manifest import write_latest
    rid = ctx.rid
    if read_pointer("LATEST") != rid:
        write_latest(release_root(), rid)
    if read_pointer("CANDIDATE") == rid:
        (release_root() / "CANDIDATE").unlink()
    rec(ctx, rid).setdefault("promoted_at", now())
    run = ctx.led.get("run")
    if run:                     # (T666 item 4) the flags about the release before this one are spent
        run["promoted"] = rid
        for k in ("baseline", "accept_diff", "accept_fixtures"):
            run.get("flags", {}).pop(k, None)
    save_ledger(ctx.led)
    # the old release's baseline is spent; the next release diffs against this one
    for d in (HOME / "baseline").glob("*") if (HOME / "baseline").is_dir() else ():
        if d.name != rid:
            shutil.rmtree(d, ignore_errors=True)
    return f"LATEST = {rid}" + (f" (was {ctx.live})" if ctx.live != rid else "")


def handed_for_prod(led: dict, rid: str) -> list[str]:
    """(T537) the releases on staging the owner was handed a production command for (staged, never published) that
    this upload replaces: their `release --prod` will refuse once staging serves another release."""
    return sorted(k for k, v in (led.get("releases") or {}).items()
                  if k != rid and v.get("staged_at") and not v.get("prod_at") and not v.get("replaced_by"))


def stage_staging(ctx: Ctx) -> str:
    rid, r = ctx.rid, rec(ctx, ctx.rid)
    if r.get("staged_at") and not ctx.reupload:
        return f"already on staging ({r['staged_at']}); --reupload to send it again"
    if r.get("fingerprint") != fingerprint(rid):
        raise Stop("staging", f"{rid} changed on disk after its checks (the fingerprint moved)",
                   "do not ship it: rebuild (`python -m pipeline build --force deliver --hold-latest`) and rerun")
    if r.get("uploaded_at") and not ctx.reupload:
        print(f"    uploaded at {r['uploaded_at']}; the smoke test failed after it: only the smoke runs", flush=True)
        return smoke(ctx, rid, r)
    probs = staging_prechecks(rid)
    if probs:
        raise Stop("staging", "the upload cannot begin: " + "; ".join(probs),
                   "fix what is named, then rerun — the build, checks and promote are done and are skipped",
                   problems=[(x, "fix it, then rerun") for x in probs] if len(probs) > 1 else None)
    for old in handed_for_prod(ctx.led, rid):
        print(f"    NOTE: this replaces {old} on staging, staged {ctx.led['releases'][old]['staged_at']} and never "
              f"published — the production command handed for it (release --prod {old}) will refuse once {rid} is "
              "up", flush=True)
    env = {**ctx.heavy_env, **({"MIN_APP_VERSION": ctx.min_app} if ctx.min_app else {}),
           **({"CI_CHECK_SKIP": f"release --no-ci-check: {ctx.no_ci_check}"} if ctx.no_ci_check is not None else {}),
           "SMOKE": "0"}                   # (T537) the smoke runs on its own below, so a smoke failure reruns only it
    rc = run_cmd(upload_cmd(rid), env=env)
    if rc == RC_WORKFLOW_NOT_STARTED:
        # (T666 item 6) only the workflow START failed, after the upload's own bounded retries; no pointer moved
        raise Stop("staging", f"GitHub did not start the run-timing workflow for {rid} (the upload retried the start "
                   "itself, each try above); the release pointer was not written",
                   "GitHub's side (build 27: it recovered in ~30 minutes): rerun when Actions answers — preflight's "
                   "fast-fail is skipped on the same tree, and the build, checks, follows and promote are done, so "
                   "only the upload runs; WF_START_TRIES / WF_START_WAIT widen the retry")
    if rc:
        raise Stop("staging", f"deploy/staging/upload.sh {rid} failed (rc={rc})",
                   "read its output above (rclone remote `r2:`, the run-timing feed); fix it and rerun "
                   "— the build, checks and promote are done and are skipped")
    r["uploaded_at"] = now()                 # the upload stamped the manifest; the fingerprint ignores the stamps
    for old in handed_for_prod(ctx.led, rid):
        ctx.led["releases"][old]["replaced_by"] = rid
    save_ledger(ctx.led)
    return smoke(ctx, rid, r)


def smoke(ctx: Ctx, rid: str, r: dict) -> str:
    rc = run_cmd(smoke_cmd())
    if rc:
        raise Stop("staging", f"the staging smoke test failed (rc={rc}) after {rid} was uploaded",
                   "read its FAIL lines and screenshots above; fix the cause and rerun — only the smoke runs again "
                   "(--reupload sends the release again)")
    r["smoked_at"] = r["staged_at"] = now()
    save_ledger(ctx.led)
    return f"{rid} uploaded to staging, smoke passed"


def after_chores(rid: str) -> str:
    """(T537) what is left after a release, as the exact commands (the goldens re-cut turns every lane's fast-fail red
    until it is done)."""
    goldens = subprocess.run([sys.executable, "scripts/check-goldens-paired.py"], cwd=REPO_ROOT, capture_output=True,
                             text=True)
    lines = ["after the release:"]
    lines.append("  goldens: " + ("cut from LATEST (the release re-cut them itself, stage 5)" if goldens.returncode == 0 else
                                  "NOT cut from LATEST — the follows stage should have (fast-fail is red in every lane "
                                  "until then):\n      node app/tools/regs-recut/sync.mjs && git diff --stat app/fixtures "
                                  "app/packages && git commit -- <the re-cut files>"))
    lines.append("  site:    the staging site serves an app at least the release's min app (checked in preflight); "
                 "after app changes: deploy/staging/site-all.sh")
    lines.append("  feeds:   the feeds worker goes with the app (memory feeds-worker-deploy-allowed): deploy/staging/feeds.sh")
    lines.append(f"  prod (the owner's): CONFIRM_PROD={rid} python -m pipeline release --prod {rid}")
    return "\n".join(lines)


RUNNERS = {"preflight": stage_preflight, "baseline": stage_baseline, "build": stage_build, "checks": stage_checks,
           "follows": stage_follows, "promote": stage_promote, "staging": stage_staging}


def default_min_app(live: str | None) -> str | None:
    """The previous release's stamp: the oldest app allowed does not move unless the human says so."""
    return manifest_of(live).get("minAppVersion") if live else None


def resume_min_app(led: dict, rid: str | None, live: str | None) -> str | None:
    """The min app version when none is given (T484). A rerun after the promote finds LATEST already naming this
    (unstamped) release, whose stamp is "any": so first the min app this release's checks passed with (ledger
    `checks.min_app_version`), else the stamp of the release LATEST named before the promote (`live_before`), else —
    a first run — the live release's stamp."""
    r = (led.get("releases") or {}).get(rid or "", {})
    got = (r.get("checks") or {}).get("min_app_version")
    if got:
        return got
    return default_min_app(r.get("live_before") or live)


def _flag(k: str) -> str:
    return "--" + {"min_app": "min-app-version"}.get(k, k.replace("_", "-"))


def remember_flags(ctx: Ctx, save: bool = True) -> None:
    """(T666 item 4) THE COMMAND REMEMBERS ITS OWN FLAGS: what was given on any run of the release in flight is kept in
    the ledger (`run`) and used, out loud, by a rerun that does not repeat it. The run in flight is the one that began
    from the same LATEST, or whose own release LATEST names now (a rerun after the promote). Only `REMEMBERED`: an
    escape that skips a guard is given, with its reason, on every run that uses it."""
    live = read_pointer("LATEST")
    run = json.loads(json.dumps(ctx.led.get("run") or {}))      # a copy: `save=False` leaves the ledger as it was
    if ctx.forget and run.get("flags"):
        print("    --forget-flags: dropped " + ", ".join(_flag(k) for k in sorted(run["flags"])), flush=True)
    if ctx.forget or not (run.get("live") == live or (run.get("promoted") and run.get("promoted") == live)):
        run = {"live": live, "flags": {}}
    flags = run.setdefault("flags", {})
    for k in REMEMBERED:
        v = getattr(ctx, k)
        if v not in (None, False):
            flags[k] = v if isinstance(v, bool) else str(v)
        elif k in flags:
            kept = flags[k]
            setattr(ctx, k, kept if isinstance(kept, bool) or k in ("min_app", "accept_fixtures") else Path(kept))
            print(f"    remembered from an earlier run of this release ({rel(LEDGER)}): {_flag(k)}"
                  + ("" if kept is True else f" {kept}"), flush=True)
    if save:                                 # the rehearsal recalls them and records nothing
        ctx.led["run"] = run
        save_ledger(ctx.led)


def print_stop(i: int, s: str, e: Stop) -> None:
    n = len(STAGES)
    if len(e.problems) == 1:
        print(f"\n[{i}/{n}] {s} FAILED: {e.what}\n\nrelease: STOPPED at {s}.\n  next: {e.todo}\n"
              f"  then run the same command again; what finished is skipped.", file=sys.stderr, flush=True)
        return
    lines = [f"\n[{i}/{n}] {s} FAILED: {len(e.problems)} problems — ALL of them, fix them together:"]
    for k, (what, todo) in enumerate(e.problems, 1):
        lines.append(f"  {k}. {what}\n     next: {todo}")
    lines.append(f"\nrelease: STOPPED at {s}.\n  then run the same command again (no flag needs repeating); what "
                 "finished is skipped.")
    print("\n".join(lines), file=sys.stderr, flush=True)


def staging(ctx: Ctx) -> int:
    ctx.led = load_ledger()
    remember_flags(ctx)
    t0 = time.time()
    n = len(STAGES)
    for i, s in enumerate(STAGES, 1):
        print(f"\n[{i}/{n}] {s} …", flush=True)
        t = time.time()
        if s == "checks" and ctx.min_app is None:
            ctx.min_app = resume_min_app(ctx.led, ctx.rid, ctx.live)
        try:
            msg = RUNNERS[s](ctx)
        except Stop as e:
            print_stop(i, s, e)
            return 1
        print(f"[{i}/{n}] {s} ok ({round(time.time() - t)}s): {msg}", flush=True)
    ctx.led.pop("run", None)                 # on staging: the next release starts with no remembered flag
    save_ledger(ctx.led)
    secs = round(time.time() - t0)
    print(f"\nrelease: {ctx.rid} is LATEST and on staging ({secs // 60}m{secs % 60:02d}s).\n{after_chores(ctx.rid)}")
    return 0


def dry_run(ctx: Ctx) -> int:
    """THE REHEARSAL (T666 item 6): every stage's CHECKS run — preflight's whole table (fast-fail and the fresh-copy
    test run too: a pass is recorded, so the real run skips it on the same tree), and, once a build has left a
    CANDIDATE, the checks of the stages after it. Nothing is built, committed, promoted or uploaded; the only thing
    written is the ledger's `fast_fail` / `fresh_copy` pass. It ends with every stop the real run would make now."""
    ctx.led = load_ledger()
    led = ctx.led
    P = "             "
    stops: list[str] = []

    def stop(what: str) -> str:
        stops.append(what.splitlines()[0])
        return "WOULD STOP: " + what

    n = len(STAGES)
    print("release --staging, REHEARSAL (--dry-run: every stage's checks; nothing is built, committed, promoted or "
          "uploaded)\n")
    remember_flags(ctx, save=False)
    print(f"[1/{n}] preflight  every check (the real run's table):")
    for name, f in preflight_findings(ctx, indent=P, stop_word="WOULD STOP"):
        if not f.ok:
            stops.append(f"preflight {name}: {f.said.splitlines()[0]}")
    live, cand = read_pointer("LATEST"), read_pointer("CANDIDATE")
    lock = Path(os.environ.get("HEAVY_LOCK", Path.home() / ".claude/heavy-job.lock")) / "owner"
    held = lock.read_text().splitlines()[0] if lock.exists() else None   # line 1 `<pid> <label> <date>` (T536)
    print(f"{P}heavy-job lock: {'held by ' + held + '  (scripts/heavy.sh --status)' if held else 'free'}")
    target = cand or live
    min_app = ctx.min_app or resume_min_app(led, target, live)
    r = led.get("releases", {}).get(target or "", {})

    live_digest = (manifest_of(live).get("bundle") or {}).get("reach_digest") if live else None
    kept = reach_digest_of(baseline_dir(live)) == live_digest if live else False
    on_disk = reach_digest_of(reach_dir())
    print(f"[2/{n}] baseline   live release {live} (reach {live_digest}): "
          + ("kept copy" if kept else f"would copy {rel(reach_dir())}" if on_disk == live_digest
             else f"--baseline {ctx.baseline} would be kept" if ctx.baseline is not None
             and reach_digest_of(ctx.baseline) == live_digest
             else stop(f"reach run on disk is {on_disk} and no copy is kept (--baseline DIR)")))
    print(f"[3/{n}] build      {' '.join(rel(c) if c.startswith(str(REPO_ROOT)) else c for c in build_cmd(jobs=ctx.jobs))}"
          f"\n{P}the build's plan:")
    sys.stdout.flush()
    rc = run_cmd(build_cmd(dry_run=True, jobs=ctx.jobs))
    if rc:
        print(f"{P}" + stop(f"the build's dry run returned rc={rc}"))
    print(f"{P}CANDIDATE now: {cand or '(none)'} — the build names its release there; LATEST stays {live}")

    # the stages after the build check the release it made: rehearsed on the CANDIDATE when there is one
    built = cand is not None and cand != live
    later = ("" if built else f"\n{P}no CANDIDATE yet: these are rehearsed on the release once the build has made it "
             "(run --dry-run again then); shown on " + str(target) + " where that says anything")
    print(f"[4/{n}] checks     manifest sanity; app gate (min app {min_app or 'any'}"
          + ("" if ctx.min_app else ", from the ledger, else the live release's stamp") + "); binding diff "
          f"{rel(baseline_dir(live)) if live else '?'} -> {rel(reach_dir())}{later}")
    if target:
        if built:
            probs = manifest_problems(target)
            print(f"{P}manifest sanity on {target}: " + (stop("; ".join(probs)) if probs else "ok"))
        probs = app_gate_problems(target, min_app)
        print(f"{P}app-version gate on {target} (min app {min_app or 'any'}): "
              + (stop("; ".join(probs)) if probs else "ok"))
    if built:
        after_digest = (manifest_of(cand).get("bundle") or {}).get("reach_digest")
        done = r.get("binding_diff") or {}
        if done.get("key") == f"{live_digest}->{after_digest}":
            said = done["status"] if done["status"] != "differs" else stop(
                f"the rule bindings differ from {live}: read {done.get('summary')}, then --accept-diff {done.get('json')}")
        elif live_digest == after_digest:
            said = "same reach run"
        elif not kept:
            said = "not rehearsed: no baseline copy is kept (the baseline stage's stop, above)"
        elif on_disk != after_digest:
            said = stop(f"the reach run on disk ({on_disk}) is not the candidate's ({after_digest}): the manifest "
                        "check stops on it (the reach run was rerun after the build)")
        else:
            from pipeline.atlas.reach.binding_diff import differs as _differs
            rep = binding_diff(baseline_dir(live), reach_dir())          # in memory: the rehearsal writes no diff file
            said = (stop(f"the rule bindings differ from {live} (the real run writes the diff and stops for "
                         f"--accept-diff):\n{P}" + diff_summary(rep).replace("\n", "\n" + P))
                    if _differs(rep) else "same bindings")
        print(f"{P}binding diff: {said}")
    if r.get("checks"):
        print(f"{P}ledger for {target}: {r.get('checks')}")

    f = r.get("follows") or {}
    py, app = follow_tests()
    print(f"[5/{n}] follows    refresh what follows the release on the candidate (python -m pipeline.release_follows "
          f"lists it){later}")
    rehearsed = None
    if built:
        if (r.get("inseason") or {}).get("release") == cand:
            print(f"{P}in-season matches: resolved on {cand} ({r['inseason'].get('at')})")
        else:
            how, line = inseason_check(cand)
            print(f"{P}in-season matches: " + (
                line if how == "same" else
                f"{line} — the real run rewrites and commits the file (one more commit for the push gate)"
                if how == "change" else
                stop(f"a match does not resolve on {cand}: {line} (re-match it in the review app's In-season tab)")
                if how == "unresolved" else stop(f"the in-season resolve could not be rehearsed on {cand}: {line}")))
        if (f.get("recut") or {}).get("release") == cand:
            print(f"{P}fixtures: re-cut on {cand} ({f['recut'].get('kind')})")
        elif ctx.rehearse_follows:
            print(f"{P}fixtures + tests REHEARSED (--rehearse-follows): the re-cut runs in place on {cand}, every check "
                  "after it runs, then the fixtures are put back byte for byte", flush=True)
            rehearsed = rehearse_follows(ctx, cand)
            if rehearsed["recut_rc"]:
                print(f"{P}fixtures: " + stop(f"the re-cut fails on {cand} (rc={rehearsed['recut_rc']}, above)"))
            else:
                ch, pages = rehearsed["changed"], rehearsed["pages"]
                print(f"{P}fixtures: the re-cut would change {len(ch)} path(s)" + (
                    "" if not pages else "; " + stop(
                        f"{len(pages)} golden page(s) change — what an angler is told:" + "".join(
                            f"\n{P}    {x}" for x in pages) + f"\n{P}  the real run stops on them with their diff; "
                        f"if every change is intended it takes --accept-fixtures {rehearsed['token']} (a token is "
                        "the re-cut's content: the same re-cut gives the same one)")))
                bad = rehearsed["failures"]
                print(f"{P}tests on the candidate: " + (stop(failures_said(cand, bad).replace("\n", "\n" + P))
                      if bad else "every check passed on the re-cut fixtures (the real run runs them again)"))
        else:
            paired, _said = goldens_paired(release_root() / cand)
            if paired:
                print(f"{P}fixtures: the goldens are cut from {cand} already")
            else:
                import tempfile
                with tempfile.TemporaryDirectory(prefix="release-rehearsal-") as td:
                    rc = run_cmd([sys.executable, "app/tools/regs-recut/taps.py", "--release", str(release_root() / cand),
                                  "--out", str(Path(td) / "taps.json")])
                print(f"{P}fixtures: " + (stop(f"the re-cut's first step (taps.py) fails on {cand} (rc={rc}, above)")
                      if rc else f"would be re-cut on {cand} and committed (taps.py rehearsed into a temp file: ok; a "
                      "changed golden page stops for --accept-fixtures — known only after the full re-cut)"))
        if rehearsed is None:
            print(f"{P}tests on the candidate: " + (f"passed on {cand} ({f['tests'].get('at')})"
                  if (f.get("tests") or {}).get("release") == cand else
                  f"SKIPPED (--no-follow-tests): {ctx.no_follow_tests}" if ctx.no_follow_tests is not None else
                  f"{len(py)} pipeline test file(s) + the whole app check run after the re-cut — NOT rehearsed: "
                  "add --rehearse-follows to re-cut in place, run them all and put the fixtures back (minutes)"))
    else:
        print(f"{P}{len(py)} pipeline + {len(app)} app test file(s) ask for the release under test")
    print(f"{P}push gate: the commits the release makes (matches, fixtures) must be pushed before the promote: "
          + ("--push given (it pushes its own)" if ctx.push else
             "it stops once with the push command (or pass --push)"))
    print(f"[6/{n}] promote    LATEST {live} -> the candidate (after every check passes)")
    print(f"[7/{n}] staging    MIN_APP_VERSION={min_app or ''} {' '.join(upload_cmd(cand if built else '<r-id>'))}"
          + (f"  ({target} staged {r['staged_at']})" if r.get("staged_at") else ""))
    if target:
        try:
            probs = staging_prechecks(target)
            print(f"{P}the upload's first refusals, on {target}: " + (stop("; ".join(probs)) if probs else "ok"))
        except (OSError, ValueError) as e:
            print(f"{P}" + stop(f"the upload's prechecks could not run on {target} ({type(e).__name__}: {e})"))
    # (T541) informational: what the review app's pending-build panel lists, read with no server — never a stop
    try:
        from pipeline.regs.review import pending as _pending
        print(f"review           {_pending.summary_line(_pending.pending_on_disk())}  "
              "(python -m pipeline.regs.review.pending lists them)")
    except Exception as e:  # noqa: BLE001 — shown, never swallowed: the dry run still prints the rest of the plan
        print(f"review           COULD NOT READ the pending review build ({type(e).__name__}: {e})")
    if stops:
        print(f"\nrehearsal: the real run would STOP {len(stops)} time(s) as things are"
              + ("" if built else " (before the build; the later stages are rehearsed once it has made a candidate)")
              + ":" + "".join(f"\n  {k}. {x}" for k, x in enumerate(stops, 1)))
    else:
        print("\nrehearsal: no stop found" + ("" if built else " before the build (the later stages are rehearsed once "
              "it has made a candidate)") + ". Not rehearsed: the build itself, "
              + ("" if rehearsed is not None else "a changed golden page, the tests on the candidate "
                 "(--rehearse-follows), ") + "the upload and its smoke test.")
    print("\nproduction (human-only, after staging): CONFIRM_PROD=<r-id> python -m pipeline release --prod <r-id>")
    return 0


# --------------------------------------------------------------------------------------------------------------- prod

def agent_env() -> str | None:
    """The name of an agent marker in the environment, or None (a human's terminal)."""
    return next((k for k in ("CLAUDECODE", "AI_AGENT", "CLAUDE_CODE_ENTRYPOINT") if os.environ.get(k)), None)


def is_tty() -> bool:
    return sys.stdin.isatty() and sys.stdout.isatty()


def prod_instructions(rid: str, r: dict) -> str:
    return (f"production is HUMAN-ONLY. To publish {rid} (checked {r.get('checks', {}).get('at')}, staged "
            f"{r.get('staged_at')}), in YOUR terminal:\n"
            f"    deploy/prod/upload.sh {rid} --bucket bc-fishing-regulations-staging --dry-run     # rehearse, nothing sent\n"
            f"    CONFIRM_PROD={rid} python -m pipeline release --prod {rid}\n"
            f"which runs (after you type the id again): CONFIRM_PROD=canifishthis.ca {' '.join(prod_cmd(rid))}\n"
            f"The rest of a production release (site, feeds worker, Play) is handoff/PLAN/release/PRODUCTION.md.")


def prod(rid: str, ask=input) -> int:
    led = load_ledger()
    r = led.get("releases", {}).get(rid)
    if not (release_root() / rid / "manifest.json").exists():
        print(f"release --prod: no release {rid} under {release_root()}", file=sys.stderr)
        return 1
    if not r or not r.get("checks") or not r.get("staged_at"):
        print(f"release --prod: {rid} never passed `python -m pipeline release --staging` (its checks and the staging "
              f"upload are not in {LEDGER}); production ships only a release that did", file=sys.stderr)
        return 1
    fp = fingerprint(rid)
    if fp != r.get("fingerprint"):
        print(f"release --prod: {rid} changed on disk since its checks (fingerprint {fp}, checked {r.get('fingerprint')}) "
              "— not the artifact that was checked; refused", file=sys.stderr)
        return 1
    try:
        on_staging = staging_release()
    except (OSError, ValueError) as e:
        print(f"release --prod: cannot read what staging serves ({STAGING_MANIFEST_URL}: {e}); refused",
              file=sys.stderr)
        return 1
    if on_staging != rid:
        print(f"release --prod: staging serves {on_staging}, not {rid} — deploy/prod/upload.sh promotes what staging "
              f"serves; refused (a later release --staging replaced it)", file=sys.stderr)
        return 1
    agent = agent_env()
    why = ([f"an agent environment ({agent})"] if agent else []) \
        + ([f"CONFIRM_PROD is not {rid}"] if os.environ.get("CONFIRM_PROD") != rid else []) \
        + (["no interactive terminal"] if not is_tty() else [])
    if why:
        print(f"release --prod: refused ({'; '.join(why)}).\n\n{prod_instructions(rid, r)}", file=sys.stderr)
        return 2
    if ask(f"type the release id to publish {rid} to PRODUCTION: ").strip() != rid:
        print("release --prod: the id did not match; nothing sent", file=sys.stderr)
        return 2
    rc = run_cmd(prod_cmd(rid), env=PROD_UPLOAD_ENV)
    if rc:
        print(f"release --prod: publish failed (rc={rc}); see above. Rerunning it re-sends the same files.",
              file=sys.stderr)
        return 1
    r["prod_at"] = now()
    save_ledger(led)
    print(f"release: {rid} published to production")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m pipeline release", description=__doc__.split("\n")[0],
                                 epilog="Runbook: handoff/PLAN/runbooks/release.md")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--staging", action="store_true", help="build -> checks -> promote LATEST -> upload staging")
    g.add_argument("--prod", metavar="R-ID", help="HUMAN-ONLY: re-upload this checked, staged release to production")
    ap.add_argument("--dry-run", action="store_true",
                    help="the rehearsal: every stage's checks; nothing is built, committed, promoted or uploaded")
    ap.add_argument("--rehearse-follows", action="store_true",
                    help="(T680) with --dry-run, once a CANDIDATE is built: re-cut the fixtures in place on it, run "
                         "every check the follows stage runs, list every failure, then put the fixtures back")
    ap.add_argument("--accept-fixtures", default=None, metavar="TOKEN",
                    help="(T666) the token of the fixture re-cut whose changed golden pages you read: commit exactly it")
    ap.add_argument("--push", action="store_true",
                    help="(T666) push the commits the release itself makes (in-season matches, re-cut fixtures); "
                         "never anyone else's")
    ap.add_argument("--no-fresh-copy", default=None, metavar="REASON",
                    help="(T666) skip the fresh-copy (clean clone) test run; the reason is required and printed")
    ap.add_argument("--forget-flags", action="store_true",
                    help="(T666) drop the flags remembered from earlier runs of this release (--baseline, "
                         "--accept-diff, --accept-fixtures, --min-app-version, --push); those given now still count")
    ap.add_argument("--no-follow-tests", default=None, metavar="REASON",
                    help="(T666) skip the tests run on the candidate; the reason is required and printed")
    ap.add_argument("--accept-diff", type=Path, default=None, metavar="FILE",
                    help="the binding_diff.json you read: go on past exactly that diff")
    ap.add_argument("--baseline", type=Path, default=None, metavar="DIR",
                    help="the reach run the live release was cut from, when no copy was kept")
    ap.add_argument("--min-app-version", default=None, metavar="X.Y.Z",
                    help="the oldest app allowed to apply the release (default: the live release's stamp)")
    ap.add_argument("--no-fast-fail", default=None, metavar="REASON",
                    help="skip scripts/fast-fail.sh (T483); the reason is required and printed")
    ap.add_argument("--no-ci-check", default=None, metavar="REASON",
                    help="(T533) do not ask GitHub whether CI is green on HEAD; the reason is required and printed")
    ap.add_argument("--ship-stale-atlas", default=None, metavar="REASON",
                    help="(T547) go on with an atlas behind its curated inputs; the reason is required and printed")
    ap.add_argument("--jobs", type=int, default=2, help="the build's --jobs (default 2, at most 4)")
    ap.add_argument("--reupload", action="store_true", help="upload to staging again even if the ledger says it is")
    a = ap.parse_args(argv)
    if a.prod:
        if a.dry_run:
            r = load_ledger().get("releases", {}).get(a.prod, {})
            try:
                on = staging_release()
                said = "ok" if on == a.prod else f"WOULD STOP: staging serves {on}, not {a.prod}"
            except (OSError, ValueError) as e:
                said = f"WOULD STOP: cannot read {STAGING_MANIFEST_URL} ({e})"
            print(f"staging serves: {said}\n{prod_instructions(a.prod, r)}")
            return 0
        return prod(a.prod)
    if a.no_fast_fail is not None and not a.no_fast_fail.strip():
        ap.error('--no-fast-fail needs a reason, which is printed: --no-fast-fail "<why the cheap checks are skipped>"')
    if a.no_ci_check is not None and not a.no_ci_check.strip():
        ap.error('--no-ci-check needs a reason, which is printed: --no-ci-check "<why CI is not asked>"')
    if a.ship_stale_atlas is not None and not a.ship_stale_atlas.strip():
        ap.error('--ship-stale-atlas needs a reason, which is printed: --ship-stale-atlas "<why the old atlas ships>"')
    if a.no_follow_tests is not None and not a.no_follow_tests.strip():
        ap.error('--no-follow-tests needs a reason, which is printed: --no-follow-tests "<why they are skipped>"')
    if a.no_fresh_copy is not None and not a.no_fresh_copy.strip():
        ap.error('--no-fresh-copy needs a reason, which is printed: --no-fresh-copy "<why it is skipped>"')
    if a.rehearse_follows and not a.dry_run:
        ap.error("--rehearse-follows is a rehearsal: give it with --dry-run (the real run re-cuts and checks anyway)")
    ctx = Ctx(rehearse_follows=a.rehearse_follows, accept_diff=a.accept_diff, baseline=a.baseline, min_app=a.min_app_version, jobs=a.jobs,
              reupload=a.reupload, no_fast_fail=a.no_fast_fail, stale_atlas=a.ship_stale_atlas,
              no_ci_check=a.no_ci_check, accept_fixtures=a.accept_fixtures, push=a.push,
              no_follow_tests=a.no_follow_tests, forget=a.forget_flags, no_fresh_copy=a.no_fresh_copy)
    return dry_run(ctx) if a.dry_run else staging(ctx)


if __name__ == "__main__":
    raise SystemExit(main())
