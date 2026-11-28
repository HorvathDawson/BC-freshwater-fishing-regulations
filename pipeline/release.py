"""ONE RELEASE COMMAND (T295/T297, T408 re-scope, owner 2026-11-05): build once, promote the same artifact.

    python -m pipeline release --staging                      # build -> checks -> follows -> promote LATEST -> upload staging
    python -m pipeline release --staging --dry-run            # the REHEARSAL: every stage's checks, nothing built / committed / sent
python -m pipeline release --staging --dry-run --rehearse-follows   # + (T680) re-cut + every test on the candidate, put back
    python -m pipeline release --staging --accept-diff FILE   # go on past a binding diff you have read (FILE = its json)
    python -m pipeline release --staging --push               # the release pushes the commits IT made (matches, fixtures)
    python -m pipeline release --cut                          # (T998) at scope lock: the version the commits compute,
                                                              # confirmed, then release/<x.y.z> from origin/main
    python -m pipeline release --cut <x.y.z>                  # the same, the version typed (refused if it disagrees)
    python -m pipeline release --cut --dry-run                # (T998) the computed version and its commits, (T1048) the
                                                              # app_reads row its version commit adds; nothing made
    python -m pipeline release --cut <x.y.z> --reads-moved WHY  # (T1048) the app's formats moved since the last row, meant
    python -m pipeline release --cut --data-only              # (T998) release/data-<YYYY-MM-DD>: data, no app change
    python -m pipeline release --prod <r-id>                  # HUMAN-ONLY: re-upload THAT checked release to production
    python -m pipeline release --final <x.y.z>                # (T1062) the final tag on the newest rc, pushed
    python -m pipeline release --rc-check                     # (T1062) HEAD is the newest rc (the store builds ask)

RELEASE BRANCHES AND TAGS (T995, owner 2026-11-21): `main` is the protected trunk. At scope lock `--cut <version>`
makes `release/<version>` from a fresh origin/main and pushes it (nothing else moves; it says what to switch to). The
release builds and ships from that branch: preflight requires HEAD on origin/release/<app.json's version>, the
release's own commits go there (`--push`), and the staging upload's workflow runs that branch's copy. A bug fix lands on
main first and is cherry-picked onto the release branch. AT SHIP (stage 7, once the staging upload and its smoke pass)
the release tags the shipped commit `data/<r-id>` and (T1062) the next RELEASE CANDIDATE `v<version>-rc.<N>` (a rerun
at a new HEAD tags the next rc; a data-only release tags only its data), pushes the tags, and prints the PR that merges
the release branch back into main. No tag is ever moved: `--final <version>` (on the owner's word, before production)
puts `v<version>` on the newest rc's commit. `--prod` refuses a release whose tags are missing or point elsewhere.

THE VERSION COMES FROM THE COMMITS (T998, owner 2026-11-21; `pipeline/release_version.py`): `--cut` reads the
Conventional Commits on origin/main since the last `v*` tag (first run: since the cutover, 0.8.5 the base), computes
the next version (feat -> minor, fix / perf -> patch, breaking -> major, minor before 1.0.0; only the app's commits
count), prints the commits that decided it and asks the owner to type it. The branch's first commit (made by the cut)
sets app.json + package.json, opens the anglers' changelog entry (empty: written by hand, check-changelog refuses it
until it has bullets) and writes the developer CHANGELOG.md. A version typed by hand that disagrees is refused, naming
both (1.0.0, the first open store release, is the owner's call). Preflight's `version` check holds app.json to the
same computation (the rehearsal prints it). A DATA-ONLY release (no app commit bumps since the last version) ships
from `release/data-<YYYY-MM-DD>` (`--cut --data-only`): preflight takes that branch, the ship tags only `data/<r-id>`,
and it merges back by PR like any release branch.

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
               `--no-fast-fail REASON` skips it and prints the reason; heavy.sh runs no fast-fail of its own,
               T661). (T666 item 5) Its pass is keyed on the TREE'S CONTENT, not on HEAD (ledger `fast_fail`
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
               limit or slough call edited after the atlas build is named — and (T1171) a module or code file it
               was built from, a package it imported, a config value it read, Python or SQLite,
               `atlas.inputs.code_behind`; an atlas built before it records no code and is rebuilt; every reason is
               printed, as `python -m pipeline.atlas.inputs` prints it — and the run stops unless
               `--ship-stale-atlas REASON`); every review mark in
               force landed in its curated file (T545, `review.apply --check`; not skipped by --no-fast-fail); no place's
               latest review save is "wrong" / "unsure" with no extent fix (T786, `review.wrong_saves`; only a dated
               WRONG_SAVE_OK row passes one; not skipped by --no-fast-fail); every classified water the book prints is
               in the corpus as printed — its classes, its tributaries, its glyph (T841, `regs.classified_waters`; not
               skipped by --no-fast-fail); HEAD pushed to
               origin/release/<app.json's version> (T995; T484: the staging upload starts a GitHub workflow that runs
               GitHub's copy — found only at stage 6 on 2026-11-07); CI (`app-ci`, `pipeline-ci`) not red on HEAD, nor on the
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
               upload runs), every book-classified water shipping classified with the Classified Waters Licence on its
               Licence tab, in its period, on every section it binds (T841, `regs.classified_waters --bundle`, on the
               candidate's bundle), and the reach BINDING DIFF against the live release (`atlas.reach.binding_diff`). A diff
               that is not empty STOPS the run: the summary is printed and written beside its json; a reviewer reads it
               and reruns with `--accept-diff <that json>` (its content must be the one written, so a stale accept never
               passes a new diff; content = the changes and counts, not the before/after path strings). LISTED, never
               a stop: the item ids gone with no alias, the live counts, and (T1047) each gauge level record the release
               moves onto its gauge's new zero, one plain line per station (`gauge_records`, checks/<r-id>/gauge_records.txt).
  5 follows    (T666 items 1-2) REFRESH WHAT FOLLOWS THE RELEASE, on the candidate, LATEST untouched — the table is
               `pipeline/release_follows.py` `FOLLOWS`: (a) the IN-SEASON MATCHES are resolved (T537,
               `regs.inseason.resolve`, through heavy.sh): a match that does not resolve stops here; the resolution is
               GENERATED (T679 #11: `data/generated/inseason/resolved.json`, never committed — the curated
               `notice_waters.json` holds only the owner's matches, and the resolve only reads it). (b) the app's release-backed
               FIXTURES are re-cut (`node app/tools/regs-recut/sync.mjs --release <candidate>`): a change that only
               renumbers (ids, handles, the fixture release) is committed with the release id; a changed GOLDEN PAGE —
               what an angler is told — STOPS, nothing committed, until `--accept-fixtures <token>` (T667 D5); the
               same re-cut records the release pins (T1135, row `pins`): a moved `answers: true` pin stops with the
               pages under the same token, a moved `answers: false` one is committed like a renumber. (c) the
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
               `deploy/staging/upload.sh <r-id>` (staging bucket only; MIN_APP_VERSION from --min-app-version, else (T993
               item 2) the previous release's stamp raised to what the code's formats need — derived and said in
               preflight, with the recorded apps a rise leaves behind), with its smoke test run on its own after it (T537:
               ledger `uploaded_at` and `smoked_at`; a rerun after a smoke-only failure runs only the smoke). It says
               when it replaces a staged release the owner was handed a production command for. (T995) Then the
               TAGS: `data/<r-id>` and (T1062) the next release candidate `v<version>-rc.N`, pushed, recorded
               (ledger `tags`, `ship_commit`), and the merge-back PR command printed. The success message
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
pointer for deploy/prod/rollback.sh; docs/runbooks/release-by-hand.md, hosts + rollback in docs/release/PRODUCTION.md). It runs ONLY for a human:
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
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

from pipeline import release_follows as RF
from pipeline import release_version as RV
from pipeline.common.curated import CURATED, GENERATED, REPO_ROOT, ROOTS

#: everything release keeps of its own: the ledger, the baseline reach run, each candidate's check outputs
HOME = GENERATED.bundle.parent / "release"
LEDGER = HOME / "ledger.json"
#: the git paths a build reads: code + the curated tree (+ the scripts/deploy the release runs)
COMMITTED = ("pipeline", "data/curated", "scripts", "deploy", "project_config.py", "config.yaml")  # path-label: git pathspecs, run with cwd=REPO_ROOT
#: the reach-run files the binding diff reads (`binding_diff.diff_runs`) + the report naming its digest
BASELINE_FILES = ("report.json", "rule_section.jsonl.gz", "rule_unresolved.jsonl", "licensing_section.jsonl",
                  # T546 item 8: DFO places, tidal / outside, the curator's diagnostics
                  "dfo_salmon.jsonl.gz", "dfo_salmon_diagnostic.jsonl", "rule_diagnostic.jsonl", "tidal.jsonl",
                  "outside_bc.jsonl")
#: min free memory % for heavy.sh to start the build (AGENTS 18, scripts/heavy.sh)
HEAVY_MIN_FREE = 50
STAGES = ("preflight", "baseline", "build", "checks", "follows", "promote", "staging")
#: (T666 item 6) deploy/staging/upload.sh's exit when GitHub would not START the run-timing workflow
#: (`scripts/start-workflow.sh`, after its bounded retries): nothing that moves the pointer was sent
RC_WORKFLOW_NOT_STARTED = 75
#: (T993 lane G3, `scripts/start-workflow.sh`) the dispatch was ACCEPTED but its run could not be read: it may be
#: running, so a rerun must not dispatch it again
RC_WORKFLOW_UNREADABLE = 76
#: (T666 item 4) the flags a rerun need not repeat (ledger `run`); the escapes are never remembered
REMEMBERED = ("baseline", "accept_diff", "accept_fixtures", "min_app", "push")


def _ci_paths():
    """(T995) scripts/ci_paths.py, where the trunk and the release-branch names are typed once (stdlib only)."""
    import importlib.util
    spec = importlib.util.spec_from_file_location("ci_paths", Path(REPO_ROOT) / "scripts" / "ci_paths.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


CP = _ci_paths()
#: (T995) the protected trunk a release branch is cut from, and the remote
GH_REMOTE, TRUNK = CP.REMOTE, CP.TRUNK
#: (T995) the release command's own pushes skip the pre-push hook's checks, saying why (scripts/before_push.py)
PUSH_ENV = {"BEFORE_PUSH_SKIP": "the release command's own push: its preflight ran fast-fail and the CI check"}
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
    return Path(ROOTS.release)


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


def wrong_saves() -> tuple[bool, str]:
    """(T786) Does any place's latest review save say "wrong" / "unsure" with no extent fix (`review.wrong_saves`,
    read-only, instant)? (ok, what it says). fast-fail runs it too; preflight asks again itself so `--no-fast-fail`
    cannot skip it. A store or allowance it cannot read raises: never a pass."""
    from pipeline.regs.review import wrong_saves as W
    rc, said = W.say(W.check())
    return rc == 0, said


def classified(bundle: Path | None = None) -> tuple[bool, str]:
    """(T841) Is every classified water the book prints in the corpus as printed (`regs.classified_waters`, instant) —
    and, given the candidate's `bundle`, shipped classified with the Classified Waters Licence on every section it binds
    (~15 s)? (ok, what it says). fast-fail runs the corpus half too; preflight asks again itself so `--no-fast-fail`
    cannot skip it. A corpus or bundle it cannot read raises: never a pass."""
    from pipeline.regs import classified_waters as CW
    rc, said = CW.say(CW.check(bundle=bundle))
    return rc == 0, said


def candidate_bundle(rid: str) -> Path:
    """(T841) The bundle the candidate was cut from — the build's, refused unless its digests are the ones the release's
    manifest names (a bundle rebuilt since is not this release's)."""
    import sqlite3
    b = Path(GENERATED.bundle) / "bundle.sqlite"
    if not b.is_file():
        raise RuntimeError(f"no bundle at {b}")
    want = manifest_of(rid).get("bundle") or {}
    db = sqlite3.connect(f"file:{b}?mode=ro", uri=True)
    try:
        meta = dict(db.execute("SELECT k, v FROM meta WHERE k IN ('entries_digest', 'reach_digest')"))
    finally:
        db.close()
    for k in ("entries_digest", "reach_digest"):
        if want.get(k) != meta.get(k):
            raise RuntimeError(f"{b} is not {rid}'s bundle: its {k} is {meta.get(k)}, the manifest says {want.get(k)}")
    return b


def atlas_behind() -> list[str]:
    """(T547) Each curated input the live atlas is behind (`pipeline.atlas.inputs.behind`), one line per file; [] when
    it is current. The release build reads this atlas (`pipeline build` rebuilds it only with --atlas)."""
    from pipeline.atlas.inputs import behind
    return behind(GENERATED.require_build())


def git_head() -> str:
    return subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, capture_output=True, text=True,
                          check=True).stdout.strip()


def app_version() -> str:
    """(T995) app/apps/mobile/app.json's expo.version: it names the release branch and the `v<version>` tag."""
    return CP.app_version()


def release_branch() -> str:
    """(T995) the branch this release ships from: release/<app.json's version>; (T998) or, checked out, a data-only
    release's release/data-<YYYY-MM-DD>."""
    return CP.shipping_branch(current_branch(), app_version())


def data_only_branch() -> bool:
    """(T998) this release ships from a data-only branch (release/data-<date>): no app change, only its data tag."""
    return bool(CP.DATA_BRANCH.fullmatch(release_branch()))


class NoReleaseBranch(LookupError):
    """(T995) origin has no release/<version>: it is cut at scope lock (`--cut`)."""


def remote_ref_exists(ref: str) -> bool:
    """(T995) origin has this ref (`refs/heads/…` / `refs/tags/…`). A git failure raises (never read as absent)."""
    rc = subprocess.run(["git", "ls-remote", "--exit-code", GH_REMOTE, ref], cwd=REPO_ROOT, capture_output=True).returncode
    if rc not in (0, 2):
        raise RuntimeError(f"git ls-remote --exit-code {GH_REMOTE} {ref} failed (rc={rc})")
    return rc == 0


class ReleaseBranchMoved(LookupError):
    """(T995 review) HEAD is behind origin/release/<version> or off it (a pre-cut main commit, a stale checkout): the
    release would build other code than the branch ships. `args[0]` = "behind" | "diverged"."""


def _ancestor(a: str, b: str) -> bool:
    rc = subprocess.run(["git", "merge-base", "--is-ancestor", a, b], cwd=REPO_ROOT).returncode
    if rc not in (0, 1):
        raise RuntimeError(f"git merge-base --is-ancestor {a} {b} failed (rc={rc})")
    return rc == 0


def git_pushed() -> bool:
    """(T995) HEAD IS the tip of origin/release/<version> after a fresh fetch: True. HEAD ahead of the tip (commits not
    pushed yet): False. HEAD behind it or off it raises ReleaseBranchMoved (an ancestor is not enough: a pre-cut main
    commit or a stale checkout is one); no such branch raises NoReleaseBranch; a failed fetch raises."""
    rb = release_branch()
    if not remote_ref_exists(f"refs/heads/{rb}"):
        raise NoReleaseBranch(rb)
    subprocess.run(["git", "fetch", "-q", GH_REMOTE, rb], cwd=REPO_ROOT, check=True)
    tip = f"{GH_REMOTE}/{rb}"
    head_on, tip_on = _ancestor("HEAD", tip), _ancestor(tip, "HEAD")
    if head_on and tip_on:
        return True
    if tip_on:
        return False
    raise ReleaseBranchMoved("behind" if head_on else "diverged")


def current_branch() -> str:
    """The branch checked out ("HEAD" when detached)."""
    return _git("rev-parse", "--abbrev-ref", "HEAD").decode().strip()


#: (T978) the variable that makes fast-fail run its data-state steps ("data: …"); only the data release sets it
DATA_STEPS = "FAST_FAIL_DATA"


def run_fast_fail(base: str | None = None, env: dict | None = None) -> tuple[int, list[str]]:
    """`scripts/fast-fail.sh` (T483) on the console: (its return code, the name of EVERY step that failed). (T533)
    `base` = FAST_FAIL_BASE: what it checks as changed is everything since that commit, not only the unpushed commits.
    (T666 item 3) FAST_FAIL_ALL: it goes on past a failed step and reports them all (FAST_FAIL_REPORT). FAST_FAIL_ONLY
    is cleared: a partial run inherited from the caller's shell would be recorded as a pass. (T978) FAST_FAIL_DATA: the
    DATA-STATE steps (review marks landed, wrong saves, override ids on the live atlas, goldens vs LATEST, /changes) run
    here, in the data release — an app build's or a lane's fast-fail leaves them out, so it never fails on this
    machine's data."""
    import tempfile
    with tempfile.TemporaryDirectory(prefix="release-fast-fail-") as td:
        report = Path(td) / "failed.txt"
        rc = run_cmd(["bash", "scripts/fast-fail.sh"],
                     env={**(env or {}), "FAST_FAIL_BASE": base or "", "FAST_FAIL_ALL": "1", "FAST_FAIL_ONLY": "",
                          DATA_STEPS: "1", "FAST_FAIL_REPORT": str(report)})
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
    """The commits on HEAD that origin's release branch does not have (after `git_pushed`'s fetch), oldest first."""
    return _git("rev-list", "--reverse", f"{GH_REMOTE}/{release_branch()}..HEAD").decode().split()


def git_push() -> int:
    """Push HEAD to the release branch (T995; the staging upload's workflow runs its copy); the return code."""
    return run_cmd(["git", "push", GH_REMOTE, f"HEAD:{release_branch()}"], env=PUSH_ENV)


def fetch_branch(branch: str) -> None:
    """(T1062) a fresh origin/<branch> (a failure raises)."""
    subprocess.run(["git", "fetch", "-q", GH_REMOTE, branch], cwd=REPO_ROOT, check=True)


def fetch_trunk() -> None:
    """(T995) a fresh origin/main (a failure raises)."""
    subprocess.run(["git", "fetch", "-q", GH_REMOTE, TRUNK], cwd=REPO_ROOT, check=True)


def trunk_decision() -> "RV.Decision":
    """(T998) the version the commits on origin/main compute (after `fetch_trunk`): the `v*` tags fetched from origin
    first (a local tag that differs is git's refusal, raised), then `release_version.decide_at`."""
    subprocess.run(["git", "fetch", "-q", GH_REMOTE, "refs/tags/v*:refs/tags/v*"], cwd=REPO_ROOT, check=True)
    return RV.decide_at(f"{GH_REMOTE}/{TRUNK}")


def version_decision() -> "RV.Decision":
    """(T998) what the commits since the last v* tag decide at HEAD (preflight's `version` check)."""
    return RV.decide_at("HEAD")


#: (T998) what the cut's first commit on a release branch changes
APP_JSON_REL, APP_PKG_REL = "app/apps/mobile/app.json", "app/apps/mobile/package.json"
CHANGELOG_TS, CHANGELOG_MD = "app/packages/core/src/changelog.ts", "CHANGELOG.md"
CHANGELOG_OPEN = "export const CHANGELOG: readonly ChangelogEntry[] = [\n"


def with_version(text: str, path: str, version: str) -> str:
    """`text` (app.json: expo.version; package.json: version) with the version set, every other byte kept. A file
    whose version is not written exactly once as `"version": "<x>"` raises, naming it (never a guess)."""
    data = json.loads(text)
    old = (data.get("expo") or {}).get("version") if path == APP_JSON_REL else data.get("version")
    needle = f'"version": "{old}"'
    if old is None or text.count(needle) != 1:
        raise RuntimeError(f"{path}: its version is not written once as {needle!r}: set it by hand")
    out = text.replace(needle, f'"version": "{version}"')
    got = json.loads(out)
    if ((got.get("expo") or {}).get("version") if path == APP_JSON_REL else got.get("version")) != version:
        raise RuntimeError(f"{path}: the version written is not the one read back: set it by hand")
    return out


def with_changelog_entry(text: str, version: str, day: str) -> str:
    """changelog.ts with an OPEN entry for `version` first (empty notes: the anglers' 3–5 bullets are written by hand,
    never commit subjects; scripts/check-changelog.mjs refuses an entry without one). An entry already there stays."""
    if re.search(r'version:\s*"' + re.escape(version) + '"', text):
        return text
    if text.count(CHANGELOG_OPEN) != 1:
        raise RuntimeError(f"{CHANGELOG_TS}: no single `{CHANGELOG_OPEN.strip()}` line to open the entry under")
    entry = ("  {\n    // (T998) opened by `python -m pipeline release --cut`: write 3-5 plain bullets for anglers (never "
             "commit\n    // subjects); check-changelog refuses the entry until it has one, and the owner clears `draft`\n"
             f'    version: "{version}", date: "{day}", draft: true, notes: [\n    ],\n  }},\n')
    return text.replace(CHANGELOG_OPEN, CHANGELOG_OPEN + entry)


def _show(rev: str, path: str) -> str | None:
    """`path` at `rev`, or None when `rev` has no such file (another git failure raises)."""
    if subprocess.run(["git", "cat-file", "-e", f"{rev}:{path}"], cwd=REPO_ROOT, capture_output=True).returncode:
        if subprocess.run(["git", "cat-file", "-e", f"{rev}^{{commit}}"], cwd=REPO_ROOT, capture_output=True).returncode:
            raise RuntimeError(f"git: no commit {rev}")
        return None
    return _git("show", f"{rev}:{path}").decode()


def app_reads_row(version: str, base: str | None = None, reads_moved: str | None = None) -> tuple[str, str, dict]:
    """(T1048) the version commit's app_reads.json on `base` (origin/main): (the table's text with `version`'s row,
    what was decided, the row). The row is what the app on `base` reads, which must be the newest recorded version's
    formats unless `reads_moved` says why not (`app_reads.for_cut`). READS ONLY (git rev-parse / cat-file / show: no
    ref, index or file moves), so `--cut --dry-run` prints it. A refusal raises `app_reads.CutRefused`, a tree this
    cannot read SystemExit, both naming the cause."""
    from pipeline.tools.emit import app_reads as AR
    base = base or f"{GH_REMOTE}/{TRUNK}"
    base_sha = _git("rev-parse", f"{base}^{{commit}}").decode().strip()
    table = _show(base_sha, AR.TABLE_REL)
    if table is None:
        raise AR.CutRefused(f"{base} has no {AR.TABLE_REL}")
    doc = json.loads(table)
    if not doc.get("_about"):
        raise AR.CutRefused(f"{base}'s {AR.TABLE_REL} has no `_about`: the table is not what "
                            "pipeline/tools/emit/app_reads.py writes; fix it on the trunk first")
    new, what = AR.for_cut(doc, version, AR.reads_in(lambda rel: _show(base_sha, rel)), reads_moved, shipped_versions())
    return AR.dump(new, about=doc["_about"]), what, new["apps"][version]


def shipped_versions() -> tuple[str, ...]:
    """(T1048 review) the app versions a `v<x.y.z>` tag says shipped (the local tags: the cut fetched origin's first)."""
    return tuple(t[1:] for t in _git("tag", "-l", "v*").decode().split() if re.fullmatch(r"v\d+\.\d+\.\d+", t))


def version_commit(version: str, d: "RV.Decision", base: str | None = None, day: str | None = None,
                   reads_moved: str | None = None) -> str:
    """(T998) THE RELEASE BRANCH'S FIRST COMMIT, made on `base` (origin/main) without a checkout (a temporary index):
    app.json + package.json at `version`, the anglers' changelog entry opened, the developer CHANGELOG.md entry
    written, and (T1048) `version`'s row in app_reads.json (`app_reads_row`; its refusal raises). Returns its id;
    nothing else moves."""
    import tempfile
    from pipeline.tools.emit import app_reads as AR
    base = base or f"{GH_REMOTE}/{TRUNK}"
    day = day or today()
    base_sha = _git("rev-parse", f"{base}^{{commit}}").decode().strip()
    table, reads_said, _ = app_reads_row(version, base_sha, reads_moved)
    files = {AR.TABLE_REL: table}
    for path in (APP_JSON_REL, APP_PKG_REL):
        text = _show(base_sha, path)
        if text is None:
            raise RuntimeError(f"{base} has no {path}")
        files[path] = with_version(text, path, version)
    ts = _show(base_sha, CHANGELOG_TS)
    if ts is None:
        raise RuntimeError(f"{base} has no {CHANGELOG_TS}")
    files[CHANGELOG_TS] = with_changelog_entry(ts, version, day)
    files[CHANGELOG_MD] = RV.changelog_with(_show(base_sha, CHANGELOG_MD), RV.changelog_section(version, day, d), version)
    with tempfile.TemporaryDirectory(prefix="release-cut-") as td:
        env = {**os.environ, "GIT_INDEX_FILE": str(Path(td) / "index")}

        def g(*args: str, data: str | None = None) -> str:
            return subprocess.run(["git", *args], cwd=REPO_ROOT, env=env, input=data, capture_output=True, text=True,
                                  check=True).stdout.strip()
        g("read-tree", base_sha)
        for path, text in files.items():
            listed = g("ls-tree", base_sha, "--", path)
            mode = listed.split()[0] if listed else "100644"
            blob = g("hash-object", "-w", "--stdin", data=text)
            g("update-index", "--add", "--cacheinfo", f"{mode},{blob},{path}")
        tree = g("write-tree")
        msg = (f"chore(release): {version}, its version in app.json and package.json, a draft changelog entry\n\n"
               f"Made by `python -m pipeline release --cut` (T998): {d.base} + {d.level} from the commits since "
               f"{d.since}. The anglers' notes in {CHANGELOG_TS} are written by hand; {CHANGELOG_MD} lists the commits.\n"
               f"{AR.TABLE_REL} (T1048): {reads_said}.\n")
        return g("commit-tree", tree, "-p", base_sha, "-m", msg)


def cut_branch(branch: str, commit: str | None = None) -> int:
    """(T995) make `branch` at `commit` (default origin/main; no switch: the checkout stays where it is) and push it;
    the return code."""
    rc = run_cmd(["git", "branch", "--no-track", branch, commit or f"{GH_REMOTE}/{TRUNK}"])
    return rc or run_cmd(["git", "push", "-u", GH_REMOTE, f"refs/heads/{branch}"], env=PUSH_ENV)


def remote_tag_commit(tag: str) -> str | None:
    """(T995 review) the commit a tag points at ON ORIGIN (`git ls-remote`, peeled), or None: a local tag is never
    taken as pushed. A git failure raises."""
    got = subprocess.run(["git", "ls-remote", GH_REMOTE, f"refs/tags/{tag}", f"refs/tags/{tag}^{{}}"], cwd=REPO_ROOT,
                         capture_output=True, text=True)
    if got.returncode:
        raise RuntimeError(f"git ls-remote {GH_REMOTE} refs/tags/{tag} failed (rc={got.returncode}): "
                           f"{got.stderr.strip()[-200:]}")
    refs = dict(reversed(ln.split("\t", 1)) for ln in got.stdout.splitlines() if "\t" in ln)
    return refs.get(f"refs/tags/{tag}^{{}}") or refs.get(f"refs/tags/{tag}")


def tag_commit(tag: str) -> str | None:
    """(T995) the commit a LOCAL tag points at, or None when there is no such tag."""
    got = subprocess.run(["git", "rev-parse", "-q", "--verify", f"refs/tags/{tag}^{{commit}}"], cwd=REPO_ROOT,
                         capture_output=True, text=True)
    return got.stdout.strip() if got.returncode == 0 else None


def is_ancestor(a: str, b: str) -> bool:
    """`a` is `b` or before it (False when git cannot tell, e.g. a commit this clone does not hold)."""
    return subprocess.run(["git", "merge-base", "--is-ancestor", a, b], cwd=REPO_ROOT, capture_output=True).returncode == 0


def make_tag(tag: str, commit: str, message: str) -> int:
    """(T995) an annotated tag; the return code."""
    return run_cmd(["git", "tag", "-a", tag, "-m", message, commit])


def push_tags(tags: list[str]) -> int:
    return run_cmd(["git", "push", GH_REMOTE, *(f"refs/tags/{x}" for x in tags)], env=PUSH_ENV)


def rc_tag(version: str, n: int) -> str:
    """(T1062) the name of `version`'s release candidate `n`: `v<x.y.z>-rc.<n>`."""
    return f"v{version}-rc.{n}"


def remote_rcs(version: str) -> dict[int, str]:
    """(T1062) `version`'s release candidates ON ORIGIN: {N: the commit it names (peeled)}. A local-only rc tag is not
    one; a git failure raises."""
    got = subprocess.run(["git", "ls-remote", GH_REMOTE, f"refs/tags/v{version}-rc.*"], cwd=REPO_ROOT,
                         capture_output=True, text=True)
    if got.returncode:
        raise RuntimeError(f"git ls-remote {GH_REMOTE} refs/tags/v{version}-rc.* failed (rc={got.returncode}): "
                           f"{got.stderr.strip()[-200:]}")
    plain, peeled = {}, {}
    for ln in got.stdout.splitlines():
        sha, _, ref = ln.partition("\t")
        n = rc_number(version, ref.removesuffix("^{}"))
        if n:
            (peeled if ref.endswith("^{}") else plain)[n] = sha
    return {**plain, **peeled}


def rc_number(version: str, ref: str) -> int | None:
    """(T1062) N when `ref` is `refs/tags/v<version>-rc.<N>` (N from 1, no leading zero), else None."""
    m = re.fullmatch(rf"refs/tags/v{re.escape(version)}-rc\.([1-9]\d*)", ref)
    return int(m[1]) if m else None


#: (T1062) app.json's store build numbers: a store build moves them (build-apk.sh --play commits the bump), never the
#: code a release candidate names
BUILD_NUMBERS = (("android", "versionCode"), ("ios", "buildNumber"))


def beyond_build_numbers(a: str, b: str) -> list[str]:
    """(T1062) what differs between commits `a` and `b` other than app.json's store build numbers: [] when `b` is `a`'s
    tree apart from them. A commit this clone does not hold is fetched from origin first; a git failure raises."""
    for rev in (a, b):
        if not git_has_commit(rev):
            subprocess.run(["git", "fetch", "-q", GH_REMOTE, rev], cwd=REPO_ROOT, check=True)
    names = [x for x in _git("diff", "--name-only", a, b).decode().splitlines() if x]
    out = [x for x in names if x != APP_JSON_REL]
    if APP_JSON_REL in names:
        def bare(rev: str) -> dict:
            doc = json.loads(_show(rev, APP_JSON_REL) or "{}")
            for platform, key in BUILD_NUMBERS:
                ((doc.get("expo") or {}).get(platform) or {}).pop(key, None)
            return doc
        if bare(a) != bare(b):
            out.append(f"{APP_JSON_REL} beyond its build numbers")
    return out


def merge_back_cmd(branch: str) -> str:
    """(T995) the PR that brings a release branch's commits (fixtures, matches, cherry-picks) back into main."""
    return (f"gh pr create --base {TRUNK} --head {branch} --title \"chore(release): merge {branch} back into {TRUNK}\" "
            f"--body \"The release's own commits (in-season matches, re-cut fixtures, cherry-picks) back on {TRUNK}.\"")


def git_has_commit(rev: str) -> bool:
    return subprocess.run(["git", "cat-file", "-e", f"{rev}^{{commit}}"], cwd=REPO_ROOT,
                          capture_output=True).returncode == 0


def ci_status(skip: str | None = None) -> tuple[int, str]:
    """(T533) `scripts/ci_status.py`: is CI red on HEAD (or on the newest finished run when HEAD has none)? (rc, what it
    printed): 0 ok / warnings, 1 red, 2 cannot tell."""
    got = subprocess.run([sys.executable, "scripts/ci_status.py", *(["--skip", skip] if skip is not None else [])],
                         cwd=REPO_ROOT, capture_output=True, text=True)
    return got.returncode, (got.stdout + got.stderr).strip()


#: where staging serves the release pointer the prod upload promotes (`deploy/prod/upload.sh`)
STAGING_MANIFEST_URL = "https://data-staging.canifishthis.ca/release/manifest.json"


def inseason_cmd(rid: str, check: bool = False) -> list[str]:
    """The in-season resolve on a release, through heavy.sh (`RF.refresh_cmd`: the table's command). `check`: resolve
    and compare, write nothing (the rehearsal)."""
    return ["bash", "scripts/heavy.sh", "inseason-resolve", "30", "--",
            *RF.refresh_cmd(RF.INSEASON, release_root() / rid), *(["--check"] if check else [])]


def inseason_check(rid: str) -> tuple[str, str]:
    """The rehearsal's question (`resolve --check`, nothing written): ("same" | "change" | "unresolved" | "error", its
    own line). "change": the real run rewrites the generated resolution; "unresolved": the real run stops."""
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


def data_tier_cmd() -> list[str]:
    """(T993 item 16) the pipeline's live-data tier in chunks (`RF.refresh_cmd(RF.DATA_TIER)`), RELEASE_DIR from the
    environment the follows stage sets (the candidate)."""
    return RF.data_tier_cmd(follow_tests()[0])


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
        + [(f"app gate {s}", RF.app_check_cmd(s), None) for s in app_check_steps()] \
        + [("pipeline data tier", heavy + data_tier_cmd(), None)]


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


def today() -> str:
    return time.strftime("%Y-%m-%d")


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


def run_content(run_dir: Path) -> str:
    """A reach run by the CONTENT of the files the binding diff reads (T631 item 1). The run's `digest` covers the rules
    only: 21 DFO places moved (Morice lost 11 sections) with the digest unchanged, and the check called that "same reach
    run" without diffing. A file the run does not hold counts as absent, never as empty."""
    h = hashlib.sha256()
    for n in BASELINE_FILES:
        if n == "report.json":
            continue
        p = Path(run_dir) / n
        h.update(n.encode() + b"\0")
        h.update(p.read_bytes() if p.is_file() else b"\0absent\0")
    return h.hexdigest()[:16]


def run_diff_key(base: Path, run: Path, before_digest: str | None, after_digest: str | None) -> tuple[str, bool]:
    """(the binding diff's ledger key, whether the two runs are the same run) — by digest AND files (T631 item 1)."""
    before_c, after_c = run_content(base), run_content(run)
    return (f"{before_digest}:{before_c}->{after_digest}:{after_c}",
            before_digest == after_digest and before_c == after_c)


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

def pack_problems_of(m: dict, root: Path) -> list[str]:
    """(T919 c) The map assets' pack the manifest names (`tiles.assets.pack`): on disk under `root`, at the manifest's
    bytes + sha256 and `unpacked` size, and still its folder's `assets.json` member for member (`assets.pack_problems`).
    [] when the manifest names no pack (a release built before it)."""
    from pipeline.deliver.tiles.assets import pack_path, pack_problems, unpacked_bytes
    a = (m.get("tiles") or {}).get("assets") or {}
    pk = a.get("pack")
    if not pk:
        return []
    p, folder = root / pk["url"], root / a["url"].rstrip("/")
    if not p.is_file():
        return [f"tiles.assets.pack: {pk['url']} is missing"]
    out = []
    if p.stat().st_size != pk.get("bytes"):
        out.append(f"tiles.assets.pack: {pk['url']} is {p.stat().st_size:,} B, the manifest says {pk.get('bytes')}")
    elif hashlib.sha256(p.read_bytes()).hexdigest() != pk.get("sha256"):
        out.append(f"tiles.assets.pack: {pk['url']} is not the sha256 the manifest names")
    if p != pack_path(folder):
        return out + [f"tiles.assets.pack: {pk['url']} is not the pack of the folder {a['url']}"]
    if not (folder / "assets.json").is_file():
        return out + [f"tiles.assets.pack: the folder {a['url']} has no assets.json"]
    out += [f"tiles.assets.pack: {x}" for x in pack_problems(folder)]
    if pk.get("unpacked") != unpacked_bytes(folder):
        out.append(f"tiles.assets.pack: unpacked is {pk.get('unpacked')}, the folder takes {unpacked_bytes(folder)} B")
    return out


def manifest_problems(rid: str) -> list[str]:
    """Manifest sanity: it is THIS release, every file it names is on disk at its size, the size gate holds, the
    tiles / basemap / synopsis it names exist (`publish.plan` — the upload's own list), (T919 c) the assets pack is its
    folder's (`pack_problems_of`), and its bundle block names the reach run on disk."""
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
    out += pack_problems_of(m, root)
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


def classified_problems(rid: str) -> str:
    """(T841) What the classified-waters gate says against the candidate's bundle; "" when it passes. A bundle it
    cannot read, or one not the candidate's, is a problem too (never a pass)."""
    try:
        ok, said = classified(candidate_bundle(rid))
    except Exception as e:  # noqa: BLE001 — a gate that could not read is a stop, never a pass
        return f"the gate could not run ({type(e).__name__}: {e})"
    return "" if ok else said


def app_gate_problems(rid: str, min_app: str | None) -> list[str]:
    from pipeline.deliver.release.app_gate import problems
    return problems(manifest_of(rid), min_app)


def site_problems(min_app: str | None, manifest: dict | None = None) -> tuple[list[str], str]:
    """The live staging site against the oldest app the release will be stamped for (`deliver.release.site_gate`),
    raised to what `manifest`'s formats need (T993 item 2: the candidate's, or before a build the code's)."""
    from pipeline.deliver.release.site_gate import check
    return check(manifest, min_app)


def binding_diff(before: Path, after: Path) -> dict:
    from pipeline.atlas.reach.binding_diff import diff_runs
    return diff_runs(before, after)


def gone_ids(before_rid: str, after_rid: str) -> tuple[dict, str]:
    """(T555 item 5) The item ids of the live release that the candidate neither holds nor aliases, and the list as text."""
    from pipeline.deliver.release.gone_ids import compare, summary
    rep = compare(release_root() / before_rid, release_root() / after_rid)
    return rep, summary(rep, before_rid, after_rid)


def live_counts(before_rid: str | None, after_rid: str, led: dict) -> tuple[dict, str]:
    """(T667 slice 6, D3) the counts tests used to pin, before -> after, as text: the candidate's measured now (its release
    folder, its reach run, the corpus and the registry), the live release's from its ledger record (a release recorded
    before this kept none: its release-folder counts are read, the review counts say "not recorded")."""
    from pipeline.deliver.release import live_counts as LC
    after = LC.release_counts(release_root() / after_rid) | LC.review_counts(
        reach_dir(), entries_dir=CURATED.regulations.synopsis, cuts=CURATED.waters.cuts, registry_json=GENERATED.registry())
    before = None
    if before_rid and before_rid != after_rid:
        before = ((led.get("releases", {}).get(before_rid) or {}).get("checks") or {}).get("counts")
        if before is None:
            before = LC.release_counts(release_root() / before_rid)
    return after, LC.diff_text(before, after, before_rid or "(no live release)", after_rid)


#: (T1047) the one line a release records when the gauge records could not be compared (said, never a stop)
GAUGE_UNCOMPARED = "could not be compared:"


def gauge_records(since: str | None) -> tuple[list[str], str]:
    """(T1047, owner 2026-11-24: "a clean well documented update ... which is easy to see") the gauge LEVEL RECORDS this
    release moves onto a new zero, one plain line per station (`datum.record_changes`): the datum change file as
    committed at `since` (the live release's commit) against the file now. No commit recorded (or not in this clone):
    every accepted change is listed, said so. Listed, never a stop: the file at the live commit that cannot be read (a
    git failure, a row its schema refuses) is SHOWN and RECORDED as the one line "could not be compared: <reason>"
    (W46: said, not hidden, and not a halt of stage 4). (lines, text)"""
    from pipeline.gauges.feed import datum
    now_rows = datum.load()
    if since and git_has_commit(since):
        try:
            then = datum.rows_at(since, Path(REPO_ROOT))
        except (subprocess.CalledProcessError, datum.DatumFileError, ValueError, OSError) as e:
            why = (e.stderr.decode(errors="replace").strip() if isinstance(e, subprocess.CalledProcessError) and e.stderr
                   else "") or str(e)
            line = f"{GAUGE_UNCOMPARED} the datum file at the live release's commit {since[:12]} ({type(e).__name__}: {why})"
            return [line], f"gauge level records (T1047 datum changes): {line}"
        basis = f"against the live release's commit {since[:12]}" + (" (no datum file then)" if then is None else "")
        lines = datum.record_changes(then or [], now_rows)
    else:
        basis = f"no commit in this clone for the live release ({since}): every accepted change listed"
        lines = datum.record_changes([], now_rows)
    head = f"gauge level records (T1047 datum changes), {basis}: " + (f"{len(lines)} moved" if lines else "none moved")
    return lines, head + "".join(f"\n  {x}" for x in lines)


#: (T1029) the one line a release records when the candidate's reach run gives no one-bank tally (said, never a stop)
ONE_BANK_UNCOUNTED = "could not be counted:"


def one_bank_unread() -> tuple[list[str], str]:
    """(T1029, owner 2026-11-27: "keep it closed (for trib rules i assume) but one thing to note we should add a thing that
    warns us if this is ever a thing") A ONE-BANK PLACE'S TRIBUTARY KEPT WITH NO SIDE READ, warned on at the release
    check: the candidate's reach run's report.json `dfo_one_bank` (`reach.dfo_salmon.one_bank_counts`), one WARNING line
    per place that holds one (`dfo_salmon.one_bank_unread_lines`). The tributary stays in the place under its rule:
    listed, never a stop. A run with no whole tally (a report.json that cannot be read, one from before T1029, a tally
    that is not a mapping or lacks a key) is SHOWN and RECORDED as the one line "could not be counted: <reason>" (W46:
    said, not read as none). (lines, text)"""
    from pipeline.atlas.reach import dfo_salmon as D
    rp = reach_dir() / "report.json"
    try:
        ob = json.loads(rp.read_text(encoding="utf-8"))["dfo_one_bank"]
        lines = D.one_bank_unread_lines(ob)
        none = (f"none ({ob['places']} place(s) with tributaries, {ob['kept']} kept, "
                f"{ob[D.TRIB_OTHER_BANK]} on the other bank)")
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as e:
        line = f"{ONE_BANK_UNCOUNTED} {rp} gives no whole `dfo_one_bank` tally ({type(e).__name__}: {e})"
        return [line], f"one-bank tributaries with no side read (T1029): {line}"
    head = ("one-bank tributaries with no side read (T1029): "
            + (f"{len(lines)} place(s) WARNED, kept in the place under its rule" if lines else none))
    return lines, head + "".join(f"\n  {x}" for x in lines)


def attribute_diff(js: Path, since: str | None) -> tuple[bool, list[str], list[str]]:
    """(T993 item 8) is every change of the binding diff `js` explained by a curated row changed in the commits since
    `since` (the commit LATEST was built from; `pipeline.release_attribute`)? (all explained, explained, not explained).
    No commit recorded, or not in this clone: nothing is explained (the diff stops, as it always did)."""
    if not since or not git_has_commit(since):
        return False, [], [f"no commit is recorded for the live release (or it is not in this clone: {since}): "
                           "nothing can be attributed"]
    from pipeline.release_attribute import attribute
    rep = json.loads(Path(js).read_text(encoding="utf-8"))
    moved = binding_moved(since, rep)
    if moved:
        return False, [], [f"NO auto-accept: {x}" for x in moved]
    return attribute(rep, since)


def reach_code_files() -> set[str]:
    """The files the last reach run imported (`build-manifest.json` `stages.reach.code_files`, T522): its code key."""
    from pipeline.build import load_manifest
    got = (((load_manifest().get("stages") or {}).get("reach") or {}).get("code_files"))
    if not got:
        raise RuntimeError("the build manifest records no reach code_files")
    return set(got)


def binding_moved(since: str, rep: dict) -> list[str]:
    """(T993 item 8, review 2026-11-21) why NO change of this diff may be auto-accepted: the binding CODE moved since
    `since` (a file the reach stage imports, or anything under pipeline/atlas/: the registry, the sectionizer, the
    reach), or the ATLAS did (the two runs' `handles` / `registry_digest` differ). [] when neither; a fact that cannot
    be read is a reason too (never a pass)."""
    out: list[str] = []
    try:
        code = reach_code_files()
    except Exception as e:  # noqa: BLE001 — unread is a reason, never a pass
        out.append(f"the reach stage's code files cannot be read ({type(e).__name__}: {e}): a code change cannot be "
                   "ruled out")
        code = set()
    moved = sorted(p for p in committed_since(since) if p in code or p.startswith("pipeline/atlas/"))
    if moved:
        out.append(f"the binding code changed since {since[:12]} ({len(moved)} file(s): {', '.join(moved[:4])}"
                   f"{' …' if len(moved) > 4 else ''}): read the diff")
    try:
        rb, ra = (json.loads((Path(rep[k]) / "report.json").read_text(encoding="utf-8")) for k in ("before", "after"))
    except (KeyError, OSError, ValueError) as e:
        return out + [f"the two reach runs' reports cannot be read ({type(e).__name__}: {e}): an atlas change cannot "
                      "be ruled out"]
    for k in ("handles", "registry_digest"):
        if rb.get(k) is None or rb.get(k) != ra.get(k):
            out.append(f"the atlas changed between the runs ({k} {rb.get(k)} -> {ra.get(k)}): read the diff")
    return out


def recut_renumbered(pages: list[str]) -> tuple[bool, str]:
    """(T993 item 8) do the changed golden pages differ from HEAD only in renumbered source indexes
    (`release_follows.renumbered_only`)?"""
    return RF.renumbered_only(pages)


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
    build: bool = False                  # (T993 item 4) --dry-run --build: build the candidate first (LATEST stays)
    rehearsal: bool = False              # the run is `--dry-run` (T993 item 15: what fast-fail leaves to the follows)
    no_fresh_copy: str | None = None     # (T666) why the fresh-copy test run is skipped (printed); None runs it
    forget: bool = False                 # (T666) drop the remembered flags of the release in flight
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
    except Exception as e:                          # noqa: BLE001 — (re-check of #286) any failure: a refusal
        return Finding(False, f"cannot tell whether the atlas needs rebuilding: {type(e).__name__}: {e}",
                       "run `python -m pipeline.atlas.inputs` and fix what it says")
    if behind and ctx.stale_atlas is None:
        return Finding(False, f"the atlas NEEDS REBUILDING: {len(behind)} reason(s), an input or its code moved since "
                       "it was built (T547, T1171):\n    " + "\n    ".join(behind),
                       "rebuild the atlas (`python -m pipeline build --atlas --promote`); or ship the old atlas with "
                       "--ship-stale-atlas REASON")
    if behind:
        # (third review of #286, M3) every reason it ships behind, not a count: the record of what was shipped stale
        return Finding(True, f"atlas behind {len(behind)} reason(s), SHIPPED ANYWAY (--ship-stale-atlas): "
                       f"{ctx.stale_atlas}:\n    " + "\n    ".join(behind))
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


def check_wrong_saves(ctx: Ctx) -> Finding:
    try:
        ok, said = wrong_saves()
    except Exception as e:  # noqa: BLE001 — a gate that could not read is a stop, never a pass
        return Finding(False, f"the wrong-saves gate could not run ({type(e).__name__}: {e})",
                       "run `python -m pipeline.regs.review.wrong_saves --check` and fix what it says")
    if not ok:
        return Finding(False, "places whose latest review save is wrong (T786: owner 2026-11-17, no \"wrong\" place "
                       "ships):\n" + said,
                       "fix and re-save each place in the review app; a save that must ship as it is goes on "
                       "WRONG_SAVE_OK (pipeline/regs/review/wrong_saves.py), dated, with its line and reason")
    return Finding(True, said.splitlines()[-1])


def check_classified(ctx: Ctx) -> Finding:
    try:
        ok, said = classified()
    except Exception as e:  # noqa: BLE001 — a gate that could not read is a stop, never a pass
        return Finding(False, f"the classified-waters gate could not run ({type(e).__name__}: {e})",
                       "run `python -m pipeline.regs.classified_waters --check` and fix what it says")
    if not ok:
        return Finding(False, "classified waters the book prints are not in the corpus as printed (T841):\n" + said,
                       "read each printed page; a parse defect is fixed in the corpus (the human re-runs the parser), "
                       "a changed book in BOOK (pipeline/regs/classified_waters.py)")
    return Finding(True, said.splitlines()[-1])


def check_pushed(ctx: Ctx) -> Finding:
    """(T484, T995) HEAD is on origin/release/<app.json's version>: the release ships from its release branch, and the
    staging upload starts a GitHub workflow that runs that branch's copy."""
    try:
        rb = release_branch()
        pushed = git_pushed()
    except SystemExit as e:                                          # app.json has no x.y.z version
        return Finding(False, f"cannot name the release branch: {e}", "fix expo.version in app/apps/mobile/app.json")
    except NoReleaseBranch:
        v = app_version()
        return Finding(False, f"there is no {rb} on {GH_REMOTE}: a release ships from its release branch, cut from "
                       f"{TRUNK} at scope lock (T995)",
                       f"python -m pipeline release --cut {v}, then build from it (git switch {rb})")
    except ReleaseBranchMoved as e:
        return Finding(False, f"HEAD {git_head()[:12]} is not the tip of {GH_REMOTE}/{rb} ({e.args[0]}: the release "
                       "would build other code than its branch ships)", f"git switch {rb} && git pull --ff-only")
    except (subprocess.CalledProcessError, RuntimeError) as e:
        return Finding(False, f"could not tell whether HEAD is pushed: {e}",
                       f"check the network and `git fetch {GH_REMOTE} {rb}`")
    if not pushed:
        on = current_branch()
        return Finding(False, f"HEAD {git_head()[:12]} is not on {GH_REMOTE}/{rb}: it has commits the branch does not "
                       "(the release ships from its release branch; the staging upload starts a GitHub workflow that "
                       "runs GitHub's copy)",
                       f"git push {GH_REMOTE} {rb} (after `build-apk.sh --play`, that pushes its app.json commit)"
                       if on == rb else
                       f"git switch {rb} && git pull --ff-only (HEAD is on {on}, not the release branch: build from it)")
    return Finding(True, f"HEAD is the tip of {GH_REMOTE}/{rb}")


def check_version(ctx: Ctx) -> Finding:
    """(T998) THE VERSION IS THE ONE THE COMMITS COMPUTE: app.json's expo.version against the Conventional Commits since
    the last v* tag (`pipeline/release_version.py`), said with the commits that decided it (the rehearsal prints it). A
    version set by hand that disagrees stops, naming both. On a data-only branch (release/data-<date>) no app commit may
    bump the app, and app.json names the shipped version."""
    try:
        v, rb = app_version(), release_branch()
        d = version_decision()
    except (SystemExit, RuntimeError, ValueError, subprocess.CalledProcessError) as e:
        return Finding(False, f"the version the commits compute could not be read ({type(e).__name__}: {e})",
                       "run `python -m pipeline.release_version` and fix what it says")
    said = d.said()
    if d.bad:
        return Finding(False, said, "a commit not in the format joined the history before the check: add its id to "
                       "app/tools/versioning.json `exempt` with why (history is never rewritten), then rerun")
    if CP.DATA_BRANCH.fullmatch(rb):
        if d.version is not None:
            return Finding(False, f"{rb} is a data-only release, but app commits since {d.since} bump the app to "
                           f"{d.version}:\n{said}",
                           f"ship them as an app release (python -m pipeline release --cut, from {TRUNK}); a data-only "
                           "branch carries no app change since the last version")
        if v != d.base:
            return Finding(False, f"{rb} is a data-only release, but app.json says {v}, not the shipped {d.base}",
                           "a data-only release ships no app change: app.json names the version that shipped")
        return Finding(True, f"a data-only release on {rb}, app {v} unchanged: {said}")
    if d.version is None and v == d.base:
        return Finding(True, f"app {v}, no app change since: {said}")
    ok, why = RV.accepts(v, d)
    if not ok:
        return Finding(False, f"app.json says {v}, the commits compute {d.version or d.base + ' (no bump)'}: {why}\n"
                       + said, "the version comes from the commits (T998): cut the computed one (python -m pipeline "
                       "release --cut), or land the commit that makes the change what it is (a feat, a `!`)")
    return Finding(True, f"{why}: {said}")


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


def built_candidate(ctx: Ctx) -> str | None:
    """The CANDIDATE when the build has made one that is not LATEST already; else None."""
    cand = read_pointer("CANDIDATE")
    return cand if cand and cand != ctx.live else None


def planned_min_app(ctx: Ctx) -> str | None:
    """The min app this run goes up with: the flag (given or remembered), else `resume_min_app` (a rerun after the
    promote keeps what its checks passed with; a first run: the live stamp raised to the code's formats)."""
    return ctx.min_app or resume_min_app(ctx.led, read_pointer("CANDIDATE") or ctx.live, ctx.live)


def check_min_app(ctx: Ctx) -> Finding:
    """(T993 item 2) THE MINIMUM APP, DERIVED BEFORE THE BUILD from the code's formats (`build.built_formats`,
    `app_reads.json`, `app_gate.oldest_reader`): said with what it was derived from and — when it rises above the live
    release's stamp — which recorded apps it leaves behind. Stops (before any build) on a format no recorded app reads,
    and on a `--min-app-version` below what the formats need: both are stops the checks stage would make 25 minutes
    later (0.8.5 stops 3 and 5)."""
    from pipeline.deliver.release.app_gate import needs, table, ver
    try:
        apps = table()
        need_by = needs(code_manifest(), apps)
    except Exception as e:  # noqa: BLE001 — a gate that could not read is a stop, never a pass
        return Finding(False, f"the minimum app cannot be derived from the code's formats ({type(e).__name__}: {e})",
                       "run `python -m pipeline.tools.emit.app_reads --check` and fix what it says")
    unread = sorted(f for f, v in need_by.items() if v is None)
    if unread:
        return Finding(False, f"the code writes {len(unread)} format(s) no recorded app reads: {', '.join(unread)} (the "
                       f"newest recorded app is {max(apps, key=ver)})",
                       "a new format ships after the app that reads it is recorded: bump the app version and run "
                       "`python -m pipeline.tools.emit.app_reads` (or keep the old format until then)")
    need = derived_min_app()
    stamp = live_stamp(ctx.live)
    if ctx.min_app and need and ver(ctx.min_app) < ver(need):
        top = sorted((f for f, v in need_by.items() if v == need))
        return Finding(False, f"--min-app-version {ctx.min_app} is older than the code's formats need: {need} "
                       f"({', '.join(top)}) — the app gate would stop the checks on it after the build",
                       f"drop --min-app-version (the default is derived: {need}), or give {need} or newer "
                       "(--forget-flags drops a remembered one)")
    chosen = planned_min_app(ctx)
    how = ("--min-app-version" if ctx.min_app else "derived: the live stamp raised to the code's formats")
    said = (f"min app {chosen or 'any'} ({how}; the code's formats need {need or 'any'}, the live release {ctx.live} "
            f"is stamped {stamp or 'none'})")
    gone = left_behind(stamp, chosen, list(apps))
    if gone:
        named = ", ".join(gone) if len(gone) <= 6 else f"{', '.join(gone[-5:])} and {len(gone) - 5} older"
        said += (f"\nRAISED from {stamp or 'none'} to {chosen}: recorded app(s) {named} stop applying new "
                 "releases — they keep the data they hold until they update")
    return Finding(True, said)


#: (T788 #2) the staging feeds worker the pieces gate asks — the same URL deploy/staging/upload.sh asks (held equal by
#: test_release_min_app.py)
STAGING_FEEDS_URL = "https://bc-fishing-feeds-staging.horvath-dawson.workers.dev"


def pieces_problems(rid: str) -> list[str]:
    """(T788 #2) the closed-area pieces gate (`deliver.release.pieces_gate`, the one the upload runs) on release `rid`
    against the staging feeds worker: [] when that worker serves every piece the release binds."""
    from pipeline.deliver import salmon as S
    from pipeline.deliver.release.pieces_gate import problems
    return problems(S.read_release(release_root() / rid), STAGING_FEEDS_URL)


def check_pieces(ctx: Ctx) -> Finding:
    """(T788 #2) the pieces gate asked BEFORE the build and the promote, not only inside the upload after the promote:
    on the candidate once there is one, else on LATEST (the checks stage asks again on the candidate)."""
    cand = built_candidate(ctx)
    target = cand or ctx.live
    if not target:
        return Finding(True, "pieces gate: no release to ask about yet")
    try:
        bad = pieces_problems(target)
    except (Exception, SystemExit) as e:  # noqa: BLE001 — SalmonError is a SystemExit; unread is a stop, never a pass
        return Finding(False, f"the pieces gate could not run on {target} ({type(e).__name__}: {e})",
                       f"`FEEDS_URL={STAGING_FEEDS_URL} python -m pipeline.deliver.release.pieces_gate "
                       f"data/release/{target}/manifest.json` asks alone")
    if bad:
        return Finding(False, f"pieces gate on {target}: " + "; ".join(bad),
                       "deploy the feeds worker to staging first (deploy/staging/feeds.sh), then rerun")
    return Finding(True, f"pieces gate on {target}: the staging feeds worker serves what it binds"
                   + ("" if cand else " (asked of LATEST before the build; the checks stage asks the candidate)"))


def check_site(ctx: Ctx) -> Finding:
    # (T483 fast-fail; caught 2026-11-09) the staging site older than the min app the release goes up with: found only by
    # the smoke test after the upload. Checked here, before the build; the upload checks again with the release's own
    # format needs (deploy/staging/upload.sh, `deliver.release.site_gate`). (T993 item 2) with the formats the release
    # will carry: the candidate's manifest once built, else the code's (0.8.5 stop 5: a 0.8.3 site under a 0.8.4 release)
    min_app = planned_min_app(ctx)
    cand = built_candidate(ctx)
    try:
        bad, said = site_problems(min_app, manifest_of(cand) if cand else code_manifest())
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


#: (T791) the variable fast-fail reads: the fresh-copy run follows in the same preflight (`scripts/fast-fail.sh`
#: `fresh_copy_next`), so its in-place whole tier and the two subsets of it are left to that run
FRESH_COPY_NEXT = "FAST_FAIL_FRESH_COPY_NEXT"


#: (T993 item 15) the variable fast-fail reads: the follows stage runs its app gates and the worker suite on the
#: candidate later in this release (`scripts/fast-fail.sh` `follows_next`)
FOLLOWS_NEXT = "FAST_FAIL_FOLLOWS_NEXT"


def follows_run_them(ctx: Ctx) -> bool:
    """(T993 item 15) will THIS run's follows stage run the app gates and the worker suite on a candidate? The real
    run: yes, unless `--no-follow-tests` or the candidate's tests passed already. The rehearsal: only with
    `--rehearse-follows` on a candidate (or one `--build` makes) — a bare rehearsal keeps them in fast-fail, so it
    still lists their failures before any build."""
    if ctx.no_follow_tests is not None:
        return False
    cand = built_candidate(ctx)
    if ctx.rehearsal:
        return ctx.rehearse_follows and (cand is not None or ctx.build)
    tests = ((((ctx.led.get("releases") or {}).get(cand or "") or {}).get("follows") or {}).get("tests") or {})
    return not (cand and tests.get("release") == cand)


def check_fast_fail(ctx: Ctx) -> Finding:
    """(T535) fast-fail, after the instant checks. (T666 item 5) keyed on the tree's content: a rerun on the same
    content skips it whatever HEAD is. (T661) heavy.sh runs none of its own, so nothing is handed to the build."""
    head = git_head()
    if ctx.no_fast_fail is not None:
        return Finding(True, f"fast-fail SKIPPED (--no-fast-fail): {ctx.no_fast_fail}")
    env, key = follows_env(ctx), tree_key()
    under = env.get(RF.RELEASE_ENV)
    done = ctx.led.get("fast_fail") or {}
    later = follows_run_them(ctx)
    scope = "follows-next" if later else "all"
    # a pass that left the app gates and the worker suite to the follows stage covers only a run that does the same
    if done.get("key") == key and done.get("release_dir") == under and done.get("scope", "all") in ("all", scope):
        return Finding(True, f"fast-fail passed for this tree's content ({key}, at {done.get('head', '')[:12]}, "
                       f"{done.get('at')}): not rerun")
    if later:
        # (T993 item 15) the follows stage runs every check:fast gate and the worker suite on the candidate, after the
        # re-cut, in this same release: they run once, there (FAST_FAIL_FOLLOWS_NEXT)
        print("    fast-fail leaves the app gates (check:fast) and the feeds worker suite to the follows stage, which "
              "runs them on the candidate in this release", flush=True)
        env = {**env, FOLLOWS_NEXT: "1"}
    if under:
        print(f"    fast-fail reads the release under test: {RF.RELEASE_ENV}={rel(under)} (the fixtures were re-cut "
              "on the candidate)", flush=True)
    if ctx.no_fresh_copy is None:
        # (T791 item 3, owner 2026-11-16 "Yes, as proposed"): the fresh copy runs the whole no-data tier next in this
        # preflight, so fast-fail does not run it in place too, nor its two subset steps (source scanners, review tests)
        env = {**env, FRESH_COPY_NEXT: "1"}
    rc, failed = run_fast_fail(fast_fail_base(ctx), env)
    if rc:
        steps = "".join(f"\n    - {f}" for f in failed) if failed else " (it stopped before its step list: read its output)"
        return Finding(False, f"scripts/fast-fail.sh failed (rc={rc}): {len(failed)} step(s){steps}",
                       "read each failed step's output above and fix it (one alone: FAST_FAIL_ONLY='<part of its "
                       "name>' scripts/fast-fail.sh); --no-fast-fail REASON skips it and prints the reason")
    ctx.led["fast_fail"] = {"head": head, "key": key, "at": now(), "dirty": dirty_paths(), "release_dir": under,
                            "scope": scope}
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
PREFLIGHT = (("committed", check_committed), ("atlas", check_atlas), ("marks", check_marks),
             ("wrong-saves", check_wrong_saves), ("classified", check_classified), ("pushed", check_pushed),
             ("version", check_version), ("ci", check_ci), ("live", check_live), ("min-app", check_min_app), ("site", check_site),
             ("pieces", check_pieces), ("fast-fail", check_fast_fail),
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


def built_by_this_release(ctx: Ctx) -> tuple[str, str] | None:
    """(T788 #10) (the candidate, what was built) when this release built it already and nothing a build reads
    (`COMMITTED`) was committed or left dirty since its commit — only the release's own fixtures, docs, the app. A
    rerun then does not rebuild: r-741815c6090d's own in-season step reran deliver for 9 minutes for the same release
    id (the generated in-season resolution is a deliver input, and the follows stage rewrites it on the candidate)."""
    cand = built_candidate(ctx)
    r = ((ctx.led.get("releases") or {}).get(cand or "") or {})
    c = r.get("commit")
    if not (cand and r.get("built_at") and c and git_has_commit(c)):
        return None
    if git_dirty() or any(RF.under(x, COMMITTED) for x in committed_since(c)):
        return None
    return cand, f"built {r['built_at']} at {c[:12]}"


def stage_build(ctx: Ctx) -> str:
    done = built_by_this_release(ctx)
    if done:
        ctx.rid = done[0]
        return (f"release {ctx.rid} ({done[1]}): nothing a build reads was committed since — not rebuilt (T788 #10; "
                "`python -m pipeline build --hold-latest` rebuilds by hand)")
    rc = run_cmd(build_cmd(jobs=ctx.jobs))
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
        raise Stop("checks", "app-version gate: " + "; ".join(probs)
                   + f" (the code's formats said {derived_min_app() or 'no app'} before the build: a candidate that "
                   "needs more is a gap in `build.built_formats` — fix it there too)",
                   "rerun with --min-app-version X.Y.Z (the oldest app that reads these formats)")
    try:
        probs = pieces_problems(rid)
    except (Exception, SystemExit) as e:  # noqa: BLE001 — SalmonError is a SystemExit; unread is a stop, never a pass
        probs = [f"the gate could not run ({type(e).__name__}: {e})"]
    if probs:
        raise Stop("checks", f"pieces gate (T788 #2) on {rid}: " + "; ".join(probs),
                   "deploy the feeds worker to staging first (deploy/staging/feeds.sh), then rerun — LATEST was not "
                   "moved")
    probs = classified_problems(rid)
    if probs:
        raise Stop("checks", "classified waters: the candidate ships a book-classified water without its Classified "
                   "Waters Licence (T841):\n" + probs,
                   "fix the cause it names (the corpus, the reach run or the licence reader), rebuild, and rerun; "
                   "`python -m pipeline.regs.classified_waters --check --bundle` says it again")
    after_digest = (manifest_of(rid).get("bundle") or {}).get("reach_digest")
    before_digest = reach_digest_of(base) if rid != ctx.live else after_digest
    out_dir = HOME / "checks" / rid
    # (T631 item 1) keyed by the run files' content too: a DFO-only change leaves the rules digest as it was
    if rid == ctx.live:
        diff_key, same_run = f"{before_digest}->{after_digest}", True
    else:
        diff_key, same_run = run_diff_key(base, reach_dir(), before_digest, after_digest)
    done = r.get("binding_diff") or {}
    # (T788 #8) a rerun AFTER the promote compares the release with itself: the record its checks made before the
    # promote (an accepted diff, its json) stands — r-741815c6090d's accept of Lost Lagoon +1 was overwritten with
    # "same reach run" this way
    keep = rid == ctx.live and bool(done.get("status"))
    if keep:
        print(f"    binding diff: as checked before the promote ({done['status']}"
              + (f", {done['json']}" if done.get("json") else "") + ")", flush=True)
    if done.get("key") != diff_key and not keep:
        if same_run:
            done = {"key": diff_key, "status": "same", "note": "same reach run (digest and files)"}
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
        since = ((ctx.led.get("releases") or {}).get(ctx.live or "") or {}).get("commit")
        if ctx.accept_diff is not None and Path(ctx.accept_diff).is_file() and want is not None \
                and diff_content_sha(Path(ctx.accept_diff)) == want:
            done.update(status="accepted", accepted_at=now())
            save_ledger(ctx.led)
        else:
            all_ok, explained, unexplained = attribute_diff(Path(done["json"]), since)
            if all_ok:
                # (T993 item 8) every change names a curated row the batch changed since LATEST's commit: it passes,
                # said, and the ledger keeps what explained it
                print(f"    binding diff AUTO-ACCEPTED (T993 item 8): every change is explained by a curated row "
                      f"changed since {since[:12]} ({ctx.live}'s commit):" + "".join(f"\n      {x}" for x in explained),
                      flush=True)
                done.update(status="accepted", accepted_at=now(), auto="attributable", explained=explained)
                save_ledger(ctx.led)
            else:
                print(Path(done["summary"]).read_text(encoding="utf-8"))
                if unexplained:
                    print("  not explained by a curated row changed since LATEST's commit (T993 item 8):"
                          + "".join(f"\n    {x}" for x in unexplained[:40])
                          + (f"\n    … {len(unexplained) - 40} more" if len(unexplained) > 40 else ""), flush=True)
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
    # (T667 slice 6, D3) the live counts: LISTED, never a stop; stored so the next release diffs against them
    counts, txt = live_counts(ctx.live, rid, ctx.led)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "live_counts.txt").write_text(txt + "\n", encoding="utf-8")
    print(txt)
    # (T1047) a gauge level record moved onto its gauge's new zero: its own plain line per station, LISTED, never a stop
    recs, txt = gauge_records(((ctx.led.get("releases") or {}).get(ctx.live or "") or {}).get("commit"))
    (out_dir / "gauge_records.txt").write_text(txt + "\n", encoding="utf-8")
    print(txt)
    # (T1029, owner 2026-11-27) a one-bank place's tributary kept with no side read: WARNED, its own line per place, never
    # a stop (the tributary stays in the place under its rule)
    banks, txt = one_bank_unread()
    (out_dir / "one_bank_unread.txt").write_text(txt + "\n", encoding="utf-8")
    print(txt)
    r["checks"] = {"manifest": "ok", "app_gate": "ok", "classified": "ok", "min_app_version": ctx.min_app,
                   "binding_diff": done["status"], "gone_ids": n_gone, "counts": counts, "gauge_records": recs,
                   "one_bank_unread": banks, "at": now()}
    r["fingerprint"] = fingerprint(rid)
    save_ledger(ctx.led)
    return (f"manifest ok, app gate ok (min app {ctx.min_app or 'manifest stamp'}), bindings {done['status']}, "
            f"{n_gone} item id(s) gone with no alias, "
            + ("gauge level records NOT compared (see above)" if recs and recs[0].startswith(GAUGE_UNCOMPARED)
               else f"{len(recs)} gauge level record(s) moved")
            + ("; one-bank tributaries NOT counted (see above)" if banks and banks[0].startswith(ONE_BANK_UNCOUNTED)
               else f"; WARNING: {len(banks)} one-bank place(s) keep a tributary with no side read (T1029, see above)"
               if banks else ""))


def stage_inseason(ctx: Ctx, rid: str, r: dict) -> str:
    """(T537) resolve the in-season matches on the candidate BEFORE the promote (was: mid-upload, after LATEST moved,
    and the rewritten file then dirtied the next preflight). Done once per release (ledger `inseason`). (T679 #11)
    The resolve writes the GENERATED resolution only: nothing to commit."""
    if (r.get("inseason") or {}).get("release") == rid:
        return f"in-season resolved on {rid} ({r['inseason'].get('at')})"
    rc = run_cmd(inseason_cmd(rid))
    if rc:
        raise Stop("follows", f"the in-season matches do not all resolve on {rid} (rc={rc}); LATEST was not moved",
                   "re-match the unresolved notices in the review app's In-season tab (the output above names them), "
                   "commit data/curated/inseason/notice_waters.json, then rerun")
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
    rc = run_cmd(recut_cmd(rid))
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
    try:
        pages = RF.answer_paths(changed)
    except RF.PinsFileError as e:        # (T1135) a broken pins file: a stop naming it, never a pass or an accept
        raise Stop("follows", f"the re-cut on {rid} left a release-pins file that is not one: {e}; nothing was "
                   "committed", f"fix the file (re-cut: {RF.REFRESH_PINS} --release {rel(release_root() / rid)}), "
                   "then rerun") from None
    token = recut_token(changed)
    print(f"    the re-cut changed {len(changed)} path(s): " + ", ".join(
        f"{row.name} {n}" for row in RF.FOLLOWS for n in [sum(RF.row_of(x) is row for x in changed)] if n), flush=True)
    pins = [x for x in pages if RF.is_pins(x)]      # (T1135) a moved answer pin is never a renumber
    golden = [x for x in pages if x not in pins]
    renumbered, note = recut_renumbered(golden) if golden else (False, "")
    if renumbered:
        # (T993 item 8; 0.8.5 stop 8: nine pages whose source indexes only renumbered) the same page, its sources
        # renumbered one for one: nothing an angler reads changed — committed, said, never a stop
        print(f"    golden pages: {note} — AUTO-ACCEPTED as a renumber (T993 item 8)", flush=True)
        f["renumbered"] = note
        pages = pins
    if pages and token not in (ctx.accept_fixtures, f.get("accepted")):
        why = "" if ctx.accept_fixtures is None else f" (--accept-fixtures {ctx.accept_fixtures} is not this re-cut)"
        raise Stop("follows", f"the re-cut on {rid} changed {len(pages)} golden page(s) — what an angler is told "
                   f"changed{why}; nothing was committed:" + "".join(f"\n    {x}" for x in RF.said(pages[:20]))
                   + ("\n    …" if len(pages) > 20 else ""),
                   "read `git diff -- " + " ".join(RF.answer_roots())
                   + "`; if every change is intended, "
                   f"rerun with\n    --accept-fixtures {token}\n  otherwise fix the cause, commit, and rerun")
    kind = "golden pages accepted" if pages else "renumber only (source indexes)" if renumbered else "renumber only"
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
    bad = candidate_failures(rid, RF.test_env(release_root() / rid))
    if bad:
        raise Stop("follows", failures_said(rid, bad) + "; LATEST was not moved, nothing was pushed",
                   f"fix them together, commit, rerun; one check alone: {RF.RELEASE_ENV}={rel(release_root() / rid)} "
                   "REGS_STORE_REQUIRED=1 in front of its command (above). --no-follow-tests REASON skips them and "
                   "prints the reason")
    f["tests"] = {"release": rid, "at": now(), "pipeline": len(py), "app": "all", "gates": len(app_check_steps()),
                  "data_tier": True}
    save_ledger(ctx.led)
    return f"{len(py)} pipeline test file(s) + the whole app check + the pipeline data tier passed on {rid}"


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
            rc = run_cmd(recut_cmd(cand))
            if rc:
                return {"recut_rc": rc}
            changed = recut_changed()
            try:
                pages = RF.answer_paths(changed)
            except RF.PinsFileError as e:    # (T1135) the real run stops on it: listed as a stop, the tests still run
                pages, broken = [], [f"a release-pins file that is not one: {e}"]
            else:
                broken = []
            pins = [x for x in pages if RF.is_pins(x)]
            golden = [x for x in pages if x not in pins]
            renumbered, note = recut_renumbered(golden) if golden else (False, "")
            # (T1135) said here, while the re-cut is on disk: a pins file names its moved pins
            got = {"recut_rc": 0, "changed": changed,
                   "pages": broken + RF.said(pins if renumbered else pages), "renumbered": note,
                   "token": recut_token(changed) if changed else None}
            got["failures"] = candidate_failures(cand, RF.test_env(release_root() / cand))
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
    rb = release_branch()
    try:
        if git_pushed():
            return f"HEAD is the tip of {GH_REMOTE}/{rb}"
    except ReleaseBranchMoved as e:
        raise Stop("follows", f"{GH_REMOTE}/{rb} moved since preflight ({e.args[0]}): HEAD is no longer on its tip",
                   f"read what landed (git log HEAD..{GH_REMOTE}/{rb}); git pull --ff-only, then rerun") from None
    except (subprocess.CalledProcessError, RuntimeError, NoReleaseBranch) as e:
        raise Stop("follows", f"could not tell whether HEAD is pushed to {rb}: {type(e).__name__}: {e}",
                   f"check the network and `git fetch {GH_REMOTE} {rb}`, then rerun")
    ours = set((r.get("follows") or {}).get("commits") or [])
    ahead = git_unpushed()
    theirs = [c for c in ahead if c not in ours]
    cmd = f"git push {GH_REMOTE} HEAD:{rb}"
    if ctx.push and not theirs:
        rc = git_push()
        if rc:
            raise Stop("follows", f"`{cmd}` failed (rc={rc})", f"fix what git said above, {cmd}, then rerun")
        return f"pushed the {len(ahead)} commit(s) the release made (--push)"
    what = (f"{len(ahead)} commit(s) are not on {GH_REMOTE}/{rb}: {len(ahead) - len(theirs)} the release made "
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


def tag_release(ctx: Ctx, rid: str, r: dict) -> str:
    """(T995, T1062) AT SHIP: tag the commit THE UPLOAD SHIPPED (ledger `uploaded_commit`, recorded when `uploaded_at`
    is) `data/<r-id>` and, for an app release, the next RELEASE CANDIDATE `v<version>-rc.<N>` (annotated, N = one past
    the newest on origin); push them; record them (ledger `tags`, `ship_commit`, `app_version`). No tag is ever moved:
    the final `v<version>` is put on the newest rc by `release --final`, and a late fix is a new rc. A rerun at a new
    HEAD (a fix cherry-picked, the r-id unchanged) tags HEAD the next rc; one whose HEAD is the newest rc's tree apart
    from the store build numbers (`beyond_build_numbers`: a Play build's versionCode commit) tags nothing new. A
    version tagged final already gets no rc, said (0.10.0 and before: the old code tagged `v<version>` at ship).
    Tags are judged ON ORIGIN (`remote_tag_commit`, `remote_rcs`): a local-only tag at the commit it names is pushed,
    one elsewhere stops; a data tag on origin at another commit stops (an id names one release)."""
    data_only = data_only_branch()
    tags = dict(r.get("tags") or {})
    if tags and data_only:
        return "tagged already: " + ", ".join(f"{k} at {v[:12]}" for k, v in sorted(tags.items()))
    ship = r.get("uploaded_commit")
    if not ship:
        raise Stop("staging", f"{rid} has no recorded upload commit (it was staged before T995 recorded one): the "
                   "commit to tag is not known", f"tag it by hand at the commit you uploaded: git tag -a data/{rid} "
                   f"-m 'data release {rid}' <sha> && git push {GH_REMOTE} refs/tags/data/{rid}")
    version = app_version()
    data_tag, final = f"data/{rid}", f"v{version}"  # path-label: a git tag name (data/<r-id>), not a path
    try:
        remote = {data_tag: remote_tag_commit(data_tag)}
        final_at, rcs = (None, {}) if data_only else (remote_tag_commit(final), remote_rcs(version))
    except RuntimeError as e:
        raise Stop("staging", f"could not read the tags on {GH_REMOTE} ({e})",
                   "check the network, then rerun: the upload is done and is skipped; only the tags are left")
    want, said = {}, []                      # tag -> the commit it names
    if data_tag not in tags:
        if remote[data_tag] not in (None, ship):
            raise Stop("staging", f"the tag {data_tag} exists on {GH_REMOTE} already at {remote[data_tag][:12]}, not at "
                       f"the uploaded commit {ship[:12]}",
                       f"read why (git ls-remote {GH_REMOTE} refs/tags/{data_tag}): a release id names one release")
        want[data_tag] = ship
    if data_only:
        said.append(f"{final} not tagged here: a data-only release ({release_branch()}), app {version} unchanged")
    else:
        at = ship if not tags else git_head()    # the first tagging names the upload; a rerun, the pushed HEAD
        newest = max(rcs, default=0)
        local_final = None if final_at else tag_commit(final)
        if local_final:
            raise Stop("staging", f"a local-only tag {final} points at {local_final[:12]} (a `release --final` whose "
                       "push failed, or one made by hand): the version is final here but not on GitHub",
                       f"if {final} is meant: python -m pipeline release --final {version} (it pushes it when it is on "
                       f"the newest rc); if it is stale: git tag -d {final} (a local-only tag), then rerun")
        try:
            same = bool(newest) and not final_at and not beyond_build_numbers(rcs[newest], at)
        except (RuntimeError, subprocess.CalledProcessError) as e:
            raise Stop("staging", f"could not compare {at[:12]} with {rc_tag(version, newest)} ({e})",
                       "check the network, then rerun: the upload is done and is skipped; only the tags are left")
        if final_at:
            said.append(f"no release candidate made: {final} is tagged final already at {final_at[:12]} ("
                        + ("its newest rc" if rcs else "made before release candidates, T1062")
                        + "); a change after it ships as the next version")
        elif same:
            tags.setdefault(rc_tag(version, newest), rcs[newest])
            said.append(f"{rc_tag(version, newest)} ({rcs[newest][:12]}) is {at[:12]} apart from the build numbers: "
                        "no new rc")
        else:
            want[rc_tag(version, newest + 1)] = at
    make, send = [], []
    for tag, c in want.items():
        if remote.get(tag) == c:
            continue
        local = tag_commit(tag)
        if local not in (None, c):
            raise Stop("staging", f"a local-only tag {tag} points at {local[:12]}, not at {c[:12]}, the commit it tags",
                       f"read it (git show {tag}); if it is stale: git tag -d {tag}, then rerun")
        if local is None:
            make.append(tag)
        send.append(tag)
    for tag in make:
        what = f"data release {rid}" if tag == data_tag else f"app {version}, release candidate {tag.rsplit('.', 1)[1]}"
        rc = make_tag(tag, want[tag], f"{what} (python -m pipeline release, {rid} on staging)")
        if rc:
            raise Stop("staging", f"`git tag {tag}` failed (rc={rc})", "fix what git said above, then rerun")
    if send:
        rc = push_tags(send)
        if rc:
            raise Stop("staging", f"pushing the tag(s) {', '.join(send)} failed (rc={rc})",
                       f"fix what git said above (git push {GH_REMOTE} " + " ".join(f"refs/tags/{x}" for x in send)
                       + "), then rerun: only the tags are left")
    tags.update(want)
    r["tags"] = tags
    r["ship_commit"], r["app_version"] = ship, version
    if data_only:
        r["data_only"] = True
    save_ledger(ctx.led)
    return "; ".join([("tagged " + ", ".join(f"{t} at {want[t][:12]}" for t in make) if make else "tagged nothing new")
                      + f", pushed {', '.join(send) or 'nothing (on origin already)'}", *said])


def stage_staging(ctx: Ctx) -> str:
    rid, r = ctx.rid, rec(ctx, ctx.rid)
    if r.get("staged_at") and not ctx.reupload:
        return f"already on staging ({r['staged_at']}); --reupload to send it again; " + tag_release(ctx, rid, r)
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
    env = {**({"MIN_APP_VERSION": ctx.min_app} if ctx.min_app else {}),
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
    if rc == RC_WORKFLOW_UNREADABLE:
        # (T993 lane G3) GitHub ACCEPTED the dispatch but its run could not be read: it may be running — a rerun that
        # dispatches again would start a second one
        raise Stop("staging", f"the run-timing workflow for {rid} was dispatched (GitHub accepted it) but its run could "
                   "not be read: it may be running; the release pointer was not written",
                   "check it, don't re-dispatch: `gh run list --workflow feeds-runtiming.yml` (or the Actions page); "
                   "once that run has finished and published, rerun — only the upload's remaining steps run")
    if rc:
        raise Stop("staging", f"deploy/staging/upload.sh {rid} failed (rc={rc})",
                   "read its output above (rclone remote `r2:`, the run-timing feed); fix it and rerun "
                   "— the build, checks and promote are done and are skipped")
    r["uploaded_at"] = now()                 # the upload stamped the manifest; the fingerprint ignores the stamps
    r["uploaded_commit"] = git_head()        # (T995 review) what the upload shipped: the tags name this commit
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
    return f"{rid} uploaded to staging, smoke passed; " + tag_release(ctx, rid, r)


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
    lines.append("  feeds:   the feeds worker goes with the app (staging any time: deploy/staging/feeds.sh; the production worker is the owner's, release-by-hand.md)")
    lines.append(f"  prod (the owner's): CONFIRM_PROD={rid} python -m pipeline release --prod {rid}")
    return "\n".join(lines)


RUNNERS = {"preflight": stage_preflight, "baseline": stage_baseline, "build": stage_build, "checks": stage_checks,
           "follows": stage_follows, "promote": stage_promote, "staging": stage_staging}


def code_manifest() -> dict:
    """(T993 item 2) the manifest's `files` as a release built from THIS code will write them (`build.built_formats`):
    what the app gate asks before a candidate exists."""
    from pipeline.deliver.release.build import built_formats
    return {"files": {k: {"format": f} for k, f in built_formats().items()}}


def derived_min_app() -> str | None:
    """(T993 item 2) the oldest recorded app that reads every format the code writes (`app_gate.oldest_reader` over
    `code_manifest` and `app_reads.json`); None when the newest recorded app cannot read one of them."""
    from pipeline.deliver.release.app_gate import oldest_reader
    return oldest_reader(code_manifest())


def live_stamp(live: str | None) -> str | None:
    return manifest_of(live).get("minAppVersion") if live else None


def default_min_app(live: str | None) -> str | None:
    """(T993 item 2) THE MINIMUM APP IS DERIVED, never a flag to remember (0.8.5 stop 3: `--min-app-version 0.8.4` was
    not passed though T686 always needed it): the live release's stamp, raised to what the code's formats need
    (`derived_min_app`). It never goes down by itself; `--min-app-version` still sets it (the preflight refuses one
    below what the formats need). `check_min_app` says when it rises and which recorded apps that leaves behind."""
    from pipeline.deliver.release.app_gate import ver
    got = [v for v in (live_stamp(live), derived_min_app()) if v]
    return max(got, key=ver) if got else None


def left_behind(old: str | None, new: str | None, apps: list[str] | None = None) -> list[str]:
    """(T993 item 2) the recorded app versions a rise of the minimum app from `old` to `new` leaves behind: those at
    least `old` (the ones that apply releases today) and older than `new` — they keep the data they hold and apply no
    newer release until they update."""
    from pipeline.deliver.release.app_gate import table, ver
    if not new or (old and ver(new) <= ver(old)):
        return []
    return sorted((v for v in (table() if apps is None else apps) if ver(v) < ver(new) and (not old or ver(v) >= ver(old))),
                  key=ver)


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
    print(f"\nrelease: {ctx.rid} is LATEST and on staging ({secs // 60}m{secs % 60:02d}s).\n{after_chores(ctx.rid)}"
          f"\n  merge back (T995): the release branch's commits go back into {TRUNK} by a PR:\n      "
          f"{merge_back_cmd(release_branch())}")
    return 0


def size_headroom(rid: str) -> list[tuple]:
    """(T993 item 9) every gated size of release `rid` against its gate, tightest first (`build.headroom`)."""
    from pipeline.deliver.release.build import headroom
    return headroom(release_root() / rid)


def headroom_said(rid: str, rows: list[tuple], indent: str, show: int = 6) -> list[str]:
    """(T993 item 9) the rehearsal's size lines: the `show` tightest, and EVERY one under `build.HEADROOM_WARN` marked
    WARN (a new column on it would stop the checks after a 25-minute build: 0.8.5 stop 2)."""
    from pipeline.deliver.release.build import HEADROOM_WARN
    warn = [r for r in rows if r[3] < HEADROOM_WARN]
    keep = rows[:max(show, len(warn))]
    out = [f"{indent}size headroom on {rid} (gzip vs its gate, tightest first; WARN under {HEADROOM_WARN:.0%}):"]
    for what, n, gate, room in keep:
        out.append(f"{indent}  {what:22} {n:>12,} / {gate:>12,}  {room:6.1%}" + ("  WARN" if room < HEADROOM_WARN else ""))
    if warn:
        out.append(f"{indent}  {len(warn)} under {HEADROOM_WARN:.0%}: a lane adding a column to one of them measures "
                   "its gz delta first (raise the gate with a measurement comment, build.SIZE_GATES, or shrink)")
    return out


#: (T993 item 4) what a rehearsal with no CANDIDATE cannot see, and the step that lets it
NOT_SEEN_BEFORE_A_BUILD = (
    "the size gates on the new files (LATEST's headroom is shown instead), the app and pieces gates on the real "
    "manifest (preflight asked the code's formats and LATEST), the classified waters on the new bundle, the binding "
    "diff, the in-season resolve, the fixture re-cut (golden pages) and the tests on the candidate")


def build_step_said(indent: str) -> str:
    return (f"{indent}to see them now: python -m pipeline release --staging --dry-run --build --rehearse-follows "
            "(builds the candidate the real run would; LATEST stays)\n"
            f"{indent}  or the three steps: 1. {' '.join(rel(c) if c.startswith(str(REPO_ROOT)) else c for c in build_cmd())}"
            f"\n{indent}                      2. python -m pipeline release --staging --dry-run --rehearse-follows"
            f"\n{indent}                      3. python -m pipeline release --staging")


def dry_run(ctx: Ctx) -> int:
    """THE REHEARSAL (T666 item 6): every stage's CHECKS run — preflight's whole table (fast-fail and the fresh-copy
    test run too: a pass is recorded, so the real run skips it on the same tree), and, once a build has left a
    CANDIDATE, the checks of the stages after it. Nothing is built, committed, promoted or uploaded; the only thing
    written is the ledger's `fast_fail` / `fresh_copy` pass. It ends with every stop the real run would make now.
    (T993 item 4) Before a build it says plainly what it cannot see (`NOT_SEEN_BEFORE_A_BUILD`) and the step that
    shows it: `--build` runs stage 3's own build first (the baseline kept as stage 2 does; LATEST stays, nothing is
    committed, promoted or uploaded), so one rehearsal covers every stage."""
    ctx.led = load_ledger()
    ctx.rehearsal = True
    led = ctx.led
    P = "             "
    stops: list[str] = []

    def stop(what: str) -> str:
        stops.append(what.splitlines()[0])
        return "WOULD STOP: " + what

    n = len(STAGES)
    print("release --staging, REHEARSAL (--dry-run: every stage's checks; "
          + ("--build: the candidate is built first, LATEST stays; " if ctx.build else "nothing is built, ")
          + "nothing is committed, promoted or uploaded)\n")
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
          + (f"\n{P}--build: the build RUNS now (stage 3's own build: LATEST stays, the candidate waits in CANDIDATE) so "
             "the stages after it are rehearsed on what it makes" if ctx.build else f"\n{P}the build's plan:"))
    sys.stdout.flush()
    if ctx.build:
        # (T993 item 4) the baseline first, as stage 2 does: the build's reach run overwrites the diff's "before"
        kept_now, msg = (keep_baseline(live, reach=ctx.baseline) if ctx.baseline is not None else keep_baseline(live)) \
            if live else (False, "no live release")
        print(f"{P}baseline: {msg}")
        if not kept_now:
            print(f"{P}" + stop(f"--build: the live reach run's copy is not kept ({msg}): nothing was built"))
        else:
            kept = True
            rc = run_cmd(build_cmd(jobs=ctx.jobs))
            if rc:
                print(f"{P}" + stop(f"--build: `pipeline build` failed (rc={rc}): read its output above "
                                    "(`python -m pipeline build --status`)"))
            cand, on_disk = read_pointer("CANDIDATE"), reach_digest_of(reach_dir())
            target = cand or live
            min_app = ctx.min_app or resume_min_app(led, target, live)
            r = led.get("releases", {}).get(target or "", {})
    else:
        rc = run_cmd(build_cmd(dry_run=True, jobs=ctx.jobs))
        if rc:
            print(f"{P}" + stop(f"the build's dry run returned rc={rc}"))
    print(f"{P}CANDIDATE now: {cand or '(none)'} — the build names its release there; LATEST stays {live}")

    # the stages after the build check the release it made: rehearsed on the CANDIDATE when there is one
    built = cand is not None and cand != live
    later = ("" if built else f"\n{P}NOT SEEN — no CANDIDATE yet (T993 item 4): {NOT_SEEN_BEFORE_A_BUILD}. Shown on "
             f"{target} where that says anything.\n" + build_step_said(P))
    print(f"[4/{n}] checks     manifest sanity; app gate (min app {min_app or 'any'}"
          + ("" if ctx.min_app else ", from the ledger, else the live stamp raised to the code's formats")
          + "); pieces gate; binding diff "
          f"{rel(baseline_dir(live)) if live else '?'} -> {rel(reach_dir())}{later}")
    if target:
        if built:
            probs = manifest_problems(target)
            print(f"{P}manifest sanity on {target}: " + (stop("; ".join(probs)) if probs else "ok"))
        try:
            print("\n".join(headroom_said(target, size_headroom(target), P)))
        except (OSError, ValueError, KeyError) as e:
            print(f"{P}size headroom on {target}: COULD NOT READ ({type(e).__name__}: {e})")
        probs = app_gate_problems(target, min_app)
        print(f"{P}app-version gate on {target} (min app {min_app or 'any'}): "
              + (stop("; ".join(probs)) if probs else "ok"))
    if built:
        said = classified_problems(cand)
        print(f"{P}classified waters on {cand}'s bundle (T841): " + (stop(said) if said else "ok"))
    if built:
        after_digest = (manifest_of(cand).get("bundle") or {}).get("reach_digest")
        done = r.get("binding_diff") or {}
        key, same_run = (run_diff_key(baseline_dir(live), reach_dir(), live_digest, after_digest) if kept
                         else (None, False))
        since_live = ((led.get("releases") or {}).get(live or "") or {}).get("commit")
        if key is not None and done.get("key") == key:
            said = done["status"]
            if done["status"] == "differs":
                all_ok, explained, _not = (attribute_diff(Path(done["json"]), since_live)
                                           if Path(done.get("json") or "").is_file() else (False, [], []))
                said = ("differs, every change explained by a curated row changed since LATEST's commit — the real run "
                        "AUTO-ACCEPTS it (T993 item 8)" if all_ok else stop(
                            f"the rule bindings differ from {live}: read {done.get('summary')}, then --accept-diff "
                            f"{done.get('json')}"))
        elif not kept:
            said = "not rehearsed: no baseline copy is kept (the baseline stage's stop, above)"
        elif same_run:
            said = "same reach run (digest and files)"
        elif on_disk != after_digest:
            said = stop(f"the reach run on disk ({on_disk}) is not the candidate's ({after_digest}): the manifest "
                        "check stops on it (the reach run was rerun after the build)")
        else:
            from pipeline.atlas.reach.binding_diff import differs as _differs
            rep = binding_diff(baseline_dir(live), reach_dir())          # in memory: the rehearsal writes no diff file
            said = "same bindings"
            if _differs(rep):
                import tempfile
                with tempfile.TemporaryDirectory(prefix="release-rehearsal-diff-") as td:
                    js = Path(td) / "binding_diff.json"
                    js.write_text(json.dumps(rep), encoding="utf-8")
                    all_ok, explained, unexplained = attribute_diff(
                        js, since_live)
                said = (f"differs, every change explained by a curated row changed since {live}'s commit — the real "
                        "run AUTO-ACCEPTS it with this note (T993 item 8):" + "".join(f"\n{P}  {x}" for x in explained)
                        if all_ok else
                        stop(f"the rule bindings differ from {live} (the real run writes the diff and stops for "
                             f"--accept-diff):\n{P}" + diff_summary(rep).replace("\n", "\n" + P)
                             + "".join(f"\n{P}  not explained: {x}" for x in unexplained[:20])))
        print(f"{P}binding diff: {said}")
    if r.get("checks"):
        print(f"{P}ledger for {target}: {r.get('checks')}")

    f = r.get("follows") or {}
    py, app = follow_tests()
    print(f"[5/{n}] follows    refresh what follows the release on the candidate (python -m pipeline.release_follows "
          f"lists it)" + ("" if built else f"\n{P}NOT SEEN — no CANDIDATE yet: see [4/{n}] (the step that shows it)"))
    rehearsed = None
    if built:
        if (r.get("inseason") or {}).get("release") == cand:
            print(f"{P}in-season matches: resolved on {cand} ({r['inseason'].get('at')})")
        else:
            how, line = inseason_check(cand)
            print(f"{P}in-season matches: " + (
                line if how == "same" else
                f"{line} — the real run rewrites the generated resolution (nothing to commit)"
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
                if rehearsed.get("renumbered"):
                    print(f"{P}golden pages: {rehearsed['renumbered']} — the real run commits them as a renumber, "
                          "with this note (T993 item 8)")
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
    try:
        rb, ver = release_branch(), app_version()
        tags = (f"data/<r-id> only (a data-only release, app {ver} unchanged)" if data_only_branch() else  # path-label: a git tag name (data/<r-id>), not a path
                f"data/<r-id> + v{ver}-rc.N, the next release candidate (T1062: none when HEAD is the newest rc)")  # path-label: a git tag name (data/<r-id>), not a path
        print(f"{P}then (T995) tags {tags} on the pushed HEAD, pushes them, and prints the merge-back PR: "
              f"{merge_back_cmd(rb)}")
    except SystemExit as e:
        print(f"{P}" + stop(f"the release branch cannot be named: {e}"))
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
              + ("" if built else " (before the build)")
              + ":" + "".join(f"\n  {k}. {x}" for k, x in enumerate(stops, 1)))
    else:
        print("\nrehearsal: no stop found" + ("" if built else " before the build") + ". Not rehearsed: "
              + ("" if ctx.build else "the build itself, ")
              + ("" if rehearsed is not None else "a changed golden page, the tests on the candidate "
                 "(--rehearse-follows), ") + "the upload and its smoke test.")
    if not built:
        # (T993 item 4) said plainly, never implied: a pre-build rehearsal cannot promise the real run's stops
        print(f"NOT SEEN before a build: {NOT_SEEN_BEFORE_A_BUILD}.\n" + build_step_said("  "))
    print("\nproduction (human-only, after staging): CONFIRM_PROD=<r-id> python -m pipeline release --prod <r-id>")
    return 0


# ---------------------------------------------------------------------------------------------------------------- cut

def _cut_row(version: str, reads_moved: str | None) -> bool:
    """(T1048) print the app_reads row the version commit adds (dry run or not); False (printed why) when the cut
    stops on it."""
    from pipeline.tools.emit import app_reads as AR
    try:
        _, what, row = app_reads_row(version, reads_moved=reads_moved)
    except (AR.CutRefused, SystemExit, subprocess.CalledProcessError, RuntimeError) as e:
        print(f"release --cut: refused: {AR.TABLE_REL}: {e}", file=sys.stderr)
        return False
    print(f"release --cut: {AR.TABLE_REL} (T1048): {what}; the version commit adds\n"
          f"  {json.dumps(version)}: {json.dumps(row, ensure_ascii=False, sort_keys=True)}")
    return True


def cut(version: str | None, data_only: bool = False, dry_run: bool = False, ask=input,
        reads_moved: str | None = None) -> int:
    """(T995, T998) AT SCOPE LOCK: the version the commits on origin/main compute (printed with the commits that decided
    it), confirmed by the owner — typed at the prompt, or given as `version` (refused when it disagrees, naming both) —
    then `release/<version>` made from a fresh origin/main with ONE commit (`version_commit`: app.json + package.json,
    the anglers' changelog entry opened, CHANGELOG.md), and pushed. `data_only`: `release/data-<today>` at origin/main
    itself, refused when an app commit bumps the app. `dry_run`: the decision and (T1048) the app_reads row the version
    commit would add, nothing made (it fetches origin/main and the v* tags, as the real cut does, and reads git: no
    branch, commit object, tag or push). The cut stops when what the app reads moved since the newest recorded version,
    naming the files, unless `reads_moved` says why. The checkout is not switched; it says what to switch to."""
    if version is not None and not re.fullmatch(r"\d+\.\d+\.\d+", version):
        print(f"release --cut: {version!r} is not x.y.z", file=sys.stderr)
        return 2
    try:
        fetch_trunk()
        d = trunk_decision()
    except (subprocess.CalledProcessError, RuntimeError, ValueError) as e:
        print(f"release --cut: cannot read {GH_REMOTE} ({e}): check the network; nothing was made", file=sys.stderr)
        return 1
    print(f"release --cut: the commits on {GH_REMOTE}/{TRUNK} (T998):\n  " + d.said().replace("\n", "\n  "))
    if d.bad:
        print("release --cut: refused: a commit not in the format joined the history before the check; add its id to "
              "app/tools/versioning.json `exempt` with why (history is never rewritten)", file=sys.stderr)
        return 1
    if data_only:
        if d.version is not None:
            print(f"release --cut --data-only: refused: app commits since {d.since} bump the app to {d.version} "
                  f"(above): cut the app release (python -m pipeline release --cut)", file=sys.stderr)
            return 1
        branch = CP.data_branch(today())
    else:
        if version is None:
            if d.version is None:
                print(f"release --cut: no app release: nothing since {d.since} bumps the app (docs, CI, tests, data "
                      "only). A data release: python -m pipeline release --cut --data-only", file=sys.stderr)
                return 1
            if dry_run:
                if not _cut_row(d.version, reads_moved):
                    return 1
                print(f"release --cut --dry-run: would cut release/{d.version} once you confirm it; nothing was made")
                return 0
            if not is_tty():
                print(f"release --cut: the owner confirms the version (T998): in a terminal, type it at the prompt, or "
                      f"run: python -m pipeline release --cut {d.version}", file=sys.stderr)
                return 2
            if ask(f"type {d.version} to cut release/{d.version} with it: ").strip() != d.version:
                print("release --cut: the version did not match; nothing was made", file=sys.stderr)
                return 2
            version = d.version
        else:
            ok, why = RV.accepts(version, d)
            if not ok:
                print(f"release --cut: refused: {why}", file=sys.stderr)
                return 1
            print(f"release --cut: {why}")
        branch = CP.release_branch(version)
        if not _cut_row(version, reads_moved):
            return 1
    if dry_run:
        print(f"release --cut --dry-run: would cut {branch} from {GH_REMOTE}/{TRUNK}; nothing was made")
        return 0
    try:
        exists = remote_ref_exists(f"refs/heads/{branch}")
        tagged = False if data_only else remote_ref_exists(f"refs/tags/v{version}")
    except (subprocess.CalledProcessError, RuntimeError) as e:
        print(f"release --cut: cannot read {GH_REMOTE} ({e}): check the network; nothing was made", file=sys.stderr)
        return 1
    if exists:
        print(f"release --cut: {branch} exists on {GH_REMOTE} already: work on it (git switch {branch}); a fix lands on "
              f"{TRUNK} first and is cherry-picked onto it", file=sys.stderr)
        return 1
    if tagged:
        print(f"release --cut: v{version} is tagged already (that version shipped): cut the next version",
              file=sys.stderr)
        return 1
    try:
        commit = None if data_only else version_commit(version, d, reads_moved=reads_moved)
    except (subprocess.CalledProcessError, RuntimeError, ValueError, SystemExit) as e:
        print(f"release --cut: the version commit could not be made ({e}); nothing was pushed", file=sys.stderr)
        return 1
    rc = cut_branch(branch, commit)
    if rc:
        print(f"release --cut: making or pushing {branch} failed (rc={rc}, above); if the local branch was made and "
              f"the push failed: git push -u {GH_REMOTE} refs/heads/{branch}", file=sys.stderr)
        return 1
    print(f"release: {branch} cut from {GH_REMOTE}/{TRUNK} and pushed. Next: git switch {branch} (or "
          f"git worktree add <dir> {branch}); the release builds and ships from it.")
    if data_only:
        print(f"  a data-only release: app.json stays {d.base}; the ship tags only data/<r-id>")
    else:
        print(f"  its first commit ({commit[:12]}) sets expo.version {version} (app.json + package.json), opens the "
              f"anglers' entry in {CHANGELOG_TS} (EMPTY: write 3-5 plain bullets and commit them; check-changelog and "
              f"the app build refuse it until then) and writes {CHANGELOG_MD}")
    return 0


# --------------------------------------------------------------------------------------------------------------- prod

def agent_env() -> str | None:
    """The name of an agent marker in the environment, or None (a human's terminal)."""
    return next((k for k in ("CLAUDECODE", "AI_AGENT", "CLAUDE_CODE_ENTRYPOINT") if os.environ.get(k)), None)


def is_tty() -> bool:
    return sys.stdin.isatty() and sys.stdout.isatty()


def is_macos() -> bool:
    return sys.platform == "darwin"


def pmset_batt() -> str:
    """`pmset -g batt` (macOS), raising SystemExit when it cannot be read: never a guess about the power source."""
    try:
        p = subprocess.run(["pmset", "-g", "batt"], capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.SubprocessError) as e:
        raise SystemExit(f"release: cannot read the power source (pmset -g batt: {e}); refused") from e
    if p.returncode != 0:
        raise SystemExit(f"release: cannot read the power source (pmset -g batt exit {p.returncode}: "
                         f"{(p.stderr or p.stdout).strip()[:200]}); refused")
    return p.stdout


def start_caffeinate(pid: int) -> None:
    """`caffeinate -i -s -w <pid>`: no idle sleep, and no system sleep on power, until that process ends."""
    try:
        subprocess.Popen(["caffeinate", "-i", "-s", "-w", str(pid)], stdin=subprocess.DEVNULL,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except OSError as e:
        raise SystemExit(f"release: cannot start caffeinate ({e}): the Mac could sleep mid-release; refused") from e


def stay_awake(on_battery: str | None = None) -> str:
    """(T1161) macOS: the Mac stays awake (caffeinate, bound to this process) while the release runs. An UNATTENDED run
    (no terminal) on battery is refused unless `--on-battery REASON` (printed); an attended one prints a warning. A
    `pmset` that fails or prints something unparseable is a refusal, never a pass. Not macOS: nothing to keep awake.
    Closed lid: caffeinate does not cover it on a MacBook without an external display (see the runbook): lid open."""
    if not is_macos():
        return "not macOS: nothing to keep awake"
    first = (pmset_batt().splitlines() or [""])[0]
    if "AC Power" in first:
        battery = False
    elif "Battery Power" in first:
        battery = True
    else:
        raise SystemExit(f"release: cannot read the power source from pmset -g batt (first line: {first!r}); refused")
    note = ""
    if battery:
        if on_battery:
            note = f"; ON BATTERY, goes on (--on-battery): {on_battery}"
        elif not is_tty():
            raise SystemExit("release: refused on battery for an unattended run (it could drain and stop mid-release): "
                             'plug the Mac in, or give --on-battery "<why it runs on battery>"')
        else:
            note = "; WARNING: on battery, plug the Mac in"
    start_caffeinate(os.getpid())
    return f"keeping the Mac awake while this runs (caffeinate -i -s): keep the lid open, on power{note}"


def prod_instructions(rid: str, r: dict) -> str:
    return (f"production is HUMAN-ONLY. To publish {rid} (checked {r.get('checks', {}).get('at')}, staged "
            f"{r.get('staged_at')}), in YOUR terminal:\n"
            f"    deploy/prod/upload.sh {rid} --bucket bc-fishing-regulations-staging --dry-run     # rehearse, nothing sent\n"
            f"    CONFIRM_PROD={rid} python -m pipeline release --prod {rid}\n"
            f"which runs (after you type the id again): CONFIRM_PROD=canifishthis.ca {' '.join(prod_cmd(rid))}\n"
            f"The whole release by hand (site, feeds worker, Play) is docs/runbooks/release-by-hand.md; hosts and rollback "
            f"are docs/release/PRODUCTION.md.")


def tag_problems(rid: str, r: dict) -> list[str]:
    """(T995, T1062) production ships only what its tags name: `data/<r-id>` exists and points at the commit the
    release shipped from (ledger `ship_commit`); for an app release with release candidates, the final `v<version>`
    is at the NEWEST rc's commit (`release --final` put it there) and `data/<r-id>` is at that commit or before it.
    A version with no rc on origin (tagged before T1062, or a data-only release) keeps the old rule: `v<version>` at
    the ship commit or before it. [] when all hold."""
    ship = r.get("ship_commit")
    if not ship:
        return [f"{rid} was never tagged at ship (no `ship_commit` in the ledger): rerun `python -m pipeline release "
                "--staging` (it tags a staged release and skips everything done)"]
    version = r.get("app_version") or app_version()
    out = []
    data_tag, app_tag = f"data/{rid}", f"v{version}"  # path-label: a git tag name (data/<r-id>), not a path
    try:
        at, v = remote_tag_commit(data_tag), remote_tag_commit(app_tag)
        rcs = {} if r.get("data_only") else remote_rcs(version)
    except RuntimeError as e:
        return [f"could not read the tags on {GH_REMOTE} ({e}): check the network"]
    if at is None:
        out.append(f"no tag {data_tag} on {GH_REMOTE}"
                   + (" (only a local one: git push origin refs/tags/" + data_tag + ")" if tag_commit(data_tag) else
                      f": git tag -a {data_tag} -m 'data release {rid}' {ship} && git push {GH_REMOTE} "
                      f"refs/tags/{data_tag}"))
    elif at != ship:
        out.append(f"{data_tag} points at {at[:12]}, not at the commit {rid} shipped from ({ship[:12]})")
    if rcs:                                  # (T1062) the final tag is on the newest rc, which holds this data
        name, c = rc_tag(version, max(rcs)), rcs[max(rcs)]
        if v is None:
            out.append(f"no tag {app_tag} on {GH_REMOTE}: the final tag goes on the newest release candidate {name} "
                       f"({c[:12]}) once the owner passed it: python -m pipeline release --final {version}")
        elif v != c:
            out.append(f"{app_tag} points at {v[:12]}, not at the newest release candidate {name} ({c[:12]})")
        try:                                 # a ship from a versionCode commit after the rc is the rc's code
            held = ship == c or is_ancestor(ship, c) or not beyond_build_numbers(c, ship)
        except (RuntimeError, subprocess.CalledProcessError) as e:
            return out + [f"could not compare {rid}'s commit {ship[:12]} with {name} ({e}): check the network"]
        if not held:
            out.append(f"{rid} shipped from {ship[:12]}, which is not {name} ({c[:12]}), nor before it, nor it apart "
                       "from the build numbers: the newest rc does not hold the code this data release shipped with")
    elif v is None:
        if not r.get("data_only"):         # (T998) a data-only release tags only its data (the app shipped before)
            out.append(f"no tag {app_tag} and no release candidate of it on {GH_REMOTE}: rerun `python -m pipeline "
                       "release --staging` (the staging ship tags the rc), then `python -m pipeline release --final "
                       f"{version}`")
    elif v != ship and not is_ancestor(v, ship):  # (T1062) no rc: a version tagged before release candidates
        out.append(f"{app_tag} points at {v[:12]}, which is not {ship[:12]} nor before it")
    return out


def newest_rc(version: str) -> tuple[str, str] | None:
    """(T1062) (name, commit) of `version`'s newest release candidate on origin, or None. A git failure raises."""
    rcs = remote_rcs(version)
    return (rc_tag(version, max(rcs)), rcs[max(rcs)]) if rcs else None


def final(version: str) -> int:
    """(T1062) THE FINAL TAG: annotated `v<version>` on the SAME commit as the newest release candidate, pushed — run
    by Claude on the owner's word (after his look at staging and the chosen Internal build), before production data.
    Refused, saying why, unless `version` is app.json's, the newest rc exists on origin, HEAD and origin's release
    branch are that rc's tree apart from the store build numbers, and `v<version>` exists nowhere (on origin or
    here). It never moves or deletes a tag."""
    try:
        here, rb = app_version(), release_branch()
    except SystemExit as e:
        print(f"release --final: refused: the release branch cannot be named ({e})", file=sys.stderr)
        return 1
    if version != here:
        print(f"release --final: refused: {version} is not app.json's version ({here}, on {current_branch()}): name "
              "the version being released, from its release branch", file=sys.stderr)
        return 1
    if data_only_branch():
        print(f"release --final: refused: {rb} is a data-only release: it tags only data/<r-id>, there is no app "
              "version to tag", file=sys.stderr)
        return 1
    tag = f"v{version}"
    try:
        on_origin, rc = remote_tag_commit(tag), newest_rc(version)
        fetch_branch(rb)
    except (RuntimeError, subprocess.CalledProcessError) as e:
        print(f"release --final: cannot read {GH_REMOTE} ({e}): check the network; nothing was tagged", file=sys.stderr)
        return 1
    if on_origin:
        print(f"release --final: refused: {tag} exists already (on {GH_REMOTE} at {on_origin[:12]}): a tag is never "
              "moved or made twice (T1062)", file=sys.stderr)
        return 1
    local = tag_commit(tag)
    if rc is None:
        print(f"release --final: refused: no release candidate of {version} on {GH_REMOTE}: the staging ship tags "
              "one (python -m pipeline release --staging --push)", file=sys.stderr)
        return 1
    name, c = rc
    try:
        off = {rev: beyond_build_numbers(c, rev) for rev in ("HEAD", f"{GH_REMOTE}/{rb}")}
    except (RuntimeError, subprocess.CalledProcessError) as e:
        print(f"release --final: cannot compare with {name} ({e}); nothing was tagged", file=sys.stderr)
        return 1
    bad = {rev: x for rev, x in off.items() if x}
    if bad:
        print(f"release --final: refused: {' and '.join(bad)} not {name} ({c[:12]}) apart from the build numbers ("
              + "; ".join(f"{rev}: {', '.join(x[:6])}" for rev, x in bad.items()) + "): tag a new rc first (python "
              f"-m pipeline release --staging --push), look at it, then --final {version}", file=sys.stderr)
        return 1
    if local not in (None, c):               # (T1062 review) a local-only final, not on the newest rc
        print(f"release --final: refused: a local-only tag {tag} points at {local[:12]}, not at the newest release "
              f"candidate {name} ({c[:12]}): read it (git show {tag}); it is not on {GH_REMOTE}, so: git tag -d {tag}, "
              f"then --final {version} again", file=sys.stderr)
        return 1
    rc_ = 0 if local else make_tag(tag, c, f"app {version}, final: {name} (python -m pipeline release --final)")
    if rc_:
        print(f"release --final: `git tag {tag}` failed (rc={rc_}, above); nothing was pushed", file=sys.stderr)
        return 1
    rc_ = push_tags([tag])
    if rc_:
        print(f"release --final: {tag} was made here but the push failed (rc={rc_}, above): git push {GH_REMOTE} "
              f"refs/tags/{tag}, or rerun --final {version}", file=sys.stderr)
        return 1
    print(f"release --final: {tag} at {c[:12]} (= {name}), " + ("made before, " if local else "") + "pushed. Next, the owner's: production data, "
          "CONFIRM_PROD=<r-id> python -m pipeline release --prod <r-id>")
    return 0


def rc_check() -> int:
    """(T1062) THE STORE BUILDS' PRECHECK (deploy/lib/build-links.sh `build_check_rc`): HEAD is the newest release
    candidate's tree apart from the store build numbers. A version tagged final before release candidates is
    compared with that tag. One line: ok (0), or what differs and the command that tags a new rc (1)."""
    try:
        version = app_version()
        rc, final_at = newest_rc(version), remote_tag_commit(f"v{version}")
    except (SystemExit, RuntimeError) as e:
        print(f"release candidate: cannot read the tags on {GH_REMOTE} ({e}): check the network")
        return 1
    if rc is None and final_at:
        rc = (f"v{version}", final_at)           # (T1062) tagged final before release candidates
    if rc is None:
        print(f"no release candidate of {version} on {GH_REMOTE}: tag one first, the staging ship does: python -m "
              "pipeline release --staging --push")
        return 1
    name, c = rc
    try:
        off = beyond_build_numbers(c, "HEAD")
    except (RuntimeError, subprocess.CalledProcessError) as e:
        print(f"release candidate: cannot compare HEAD with {name} ({e})")
        return 1
    if off:
        print(f"HEAD is not {name} ({c[:12]}) apart from the build numbers ({', '.join(off[:6])}"
              + (" …" if len(off) > 6 else "") + "): "
              + (f"v{version} is final already: a change after it ships as the next version" if final_at else
                 "tag a new rc first: python -m pipeline release --staging --push"))
        return 1
    print(f"HEAD is {name} ({c[:12]}) apart from the build numbers")
    return 0


def prod(rid: str, ask=input, on_battery: str | None = None) -> int:
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
    bad = tag_problems(rid, r)
    if bad:
        print(f"release --prod: {rid}'s release tags are not right (T995) — refused:\n" + "\n".join(f"  - {x}" for x in bad),
              file=sys.stderr)
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
    print(f"release: {stay_awake(on_battery)}", flush=True)   # (T1161) after every human-only check, before the upload
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
                                 epilog="Runbook: docs/runbooks/release.md")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--staging", action="store_true", help="build -> checks -> promote LATEST -> upload staging")
    g.add_argument("--prod", metavar="R-ID", help="HUMAN-ONLY: re-upload this checked, staged release to production")
    g.add_argument("--cut", nargs="?", const="", metavar="X.Y.Z",
                   help="(T995, T998) at scope lock: the version the commits compute (confirmed; a typed X.Y.Z must be "
                        "it), then release/X.Y.Z from origin/main with its version commit, pushed")
    g.add_argument("--final", metavar="X.Y.Z",
                   help="(T1062) on the owner's word, before production data: the final vX.Y.Z on the newest release "
                        "candidate's commit, pushed (refused unless HEAD is that rc apart from the build numbers)")
    g.add_argument("--rc-check", action="store_true",
                   help="(T1062) the store builds' precheck: HEAD is the newest release candidate apart from the build "
                        "numbers")
    ap.add_argument("--dry-run", action="store_true",
                    help="the rehearsal: every stage's checks; nothing is built, committed, promoted or uploaded")
    ap.add_argument("--rehearse-follows", action="store_true",
                    help="(T680) with --dry-run, once a CANDIDATE is built: re-cut the fixtures in place on it, run "
                         "every check the follows stage runs, list every failure, then put the fixtures back")
    ap.add_argument("--build", action="store_true",
                    help="(T993 item 4) with --dry-run: build the candidate first (stage 3's build: LATEST stays), so "
                         "the checks after the build are rehearsed on it too")
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
                    help="the oldest app allowed to apply the release (default, T993: the live release's stamp raised to "
                         "what the code's formats need, derived before the build)")
    ap.add_argument("--no-fast-fail", default=None, metavar="REASON",
                    help="skip scripts/fast-fail.sh (T483); the reason is required and printed")
    ap.add_argument("--no-ci-check", default=None, metavar="REASON",
                    help="(T533) do not ask GitHub whether CI is green on HEAD; the reason is required and printed")
    ap.add_argument("--ship-stale-atlas", default=None, metavar="REASON",
                    help="(T547) go on with an atlas behind its curated inputs; the reason is required and printed")
    ap.add_argument("--jobs", type=int, default=2, help="the build's --jobs (default 2, at most 4)")
    ap.add_argument("--reupload", action="store_true", help="upload to staging again even if the ledger says it is")
    ap.add_argument("--reads-moved", default=None, metavar="WHY",
                    help="(T1048) with --cut: what the app reads moved since the newest recorded app version, and that "
                         "is meant (the owner's call): the version commit records the new formats; the reason is printed")
    ap.add_argument("--data-only", action="store_true",
                    help="(T998) with --cut: release/data-<YYYY-MM-DD> from origin/main, a data release with no app "
                         "change (refused when an app commit bumps the app)")
    ap.add_argument("--on-battery", default=None, metavar="REASON",
                    help="(T1161) an unattended run on battery goes on instead of being refused; the reason is printed")
    a = ap.parse_args(argv)
    if a.on_battery is not None and not a.on_battery.strip():
        ap.error('--on-battery needs a reason, which is printed: --on-battery "<why it runs on battery>"')
    if a.data_only and (a.cut is None or a.cut):
        ap.error("--data-only goes with a bare --cut (the version stays the shipped one)")
    if a.reads_moved is not None and (a.cut is None or a.data_only or not a.reads_moved.strip()):
        ap.error("--reads-moved WHY goes with an app --cut, and needs the reason")
    if a.final is not None:
        if a.dry_run:
            ap.error("--final has no --dry-run (it would tag): `python -m pipeline release --prod <r-id> --dry-run` "
                     "says what production's tag check wants")
        return final(a.final)
    if a.rc_check:
        return rc_check()
    if a.cut is not None:
        return cut(a.cut or None, data_only=a.data_only, dry_run=a.dry_run, reads_moved=a.reads_moved)
    if a.prod:
        if a.dry_run:
            r = load_ledger().get("releases", {}).get(a.prod, {})
            try:
                on = staging_release()
                said = "ok" if on == a.prod else f"WOULD STOP: staging serves {on}, not {a.prod}"
            except (OSError, ValueError) as e:
                said = f"WOULD STOP: cannot read {STAGING_MANIFEST_URL} ({e})"
            tags = tag_problems(a.prod, r) if r else ["no ledger record"]
            print(f"staging serves: {said}\ntags (T995): " + ("ok" if not tags else "WOULD STOP: " + "; ".join(tags))
                  + f"\n{prod_instructions(a.prod, r)}")
            return 0
        return prod(a.prod, on_battery=a.on_battery)
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
    if a.build and not a.dry_run:
        ap.error("--build is the rehearsal's build step: give it with --dry-run (the real run builds anyway)")
    ctx = Ctx(rehearse_follows=a.rehearse_follows, build=a.build, accept_diff=a.accept_diff, baseline=a.baseline, min_app=a.min_app_version, jobs=a.jobs,
              reupload=a.reupload, no_fast_fail=a.no_fast_fail, stale_atlas=a.ship_stale_atlas,
              no_ci_check=a.no_ci_check, accept_fixtures=a.accept_fixtures, push=a.push,
              no_follow_tests=a.no_follow_tests, forget=a.forget_flags, no_fresh_copy=a.no_fresh_copy)
    # (T1161) the long runs keep the Mac awake: --staging, --prod (in prod(), after its checks), and a rehearsal that builds (--build) or
    # re-cuts and checks a candidate (--rehearse-follows). A bare --dry-run is the cheap checks: no hold.
    if not a.dry_run or a.build or a.rehearse_follows:
        print(f"release: {stay_awake(a.on_battery)}", flush=True)
    return dry_run(ctx) if a.dry_run else staging(ctx)


if __name__ == "__main__":
    raise SystemExit(main())
