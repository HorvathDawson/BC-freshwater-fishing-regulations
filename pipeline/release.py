"""ONE RELEASE COMMAND (T295/T297, T408 re-scope, owner 2026-11-05): build once, promote the same artifact.

    python -m pipeline release --staging                      # build -> checks -> promote LATEST -> upload staging
    python -m pipeline release --staging --dry-run            # the plan: each stage and what it would do; nothing runs
    python -m pipeline release --staging --accept-diff FILE   # go on past a binding diff you have read (FILE = its json)
    python -m pipeline release --prod <r-id>                  # HUMAN-ONLY: re-upload THAT checked release to production

THE STAGES, in order; the run stops at the first failure with a plain message, and a rerun of the same command picks up
where it stopped (every stage below is skipped when it is already done for this release):

  1 preflight  the instant checks first, then `scripts/fast-fail.sh` LAST (T483/T535, AGENTS 20a: the cheap checks, a
               minute or two; `--no-fast-fail REASON` skips it and prints the reason; a rerun skips it when it passed
               for this HEAD, ledger `fast_fail`; the build's heavy.sh is told so, FAST_FAIL_DONE). The instant ones:
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
               passes a new diff; content = the changes and counts, not the before/after path strings). (T537) Then
               the IN-SEASON MATCHES are resolved on the candidate (`regs.inseason.resolve`, through heavy.sh) BEFORE
               the promote: a match that does not resolve stops here, LATEST untouched; a changed
               `data/curated/inseason/notice_waters.json` is committed (that path only) and the run stops for the
               push. The upload's sync-inputs.sh only verifies them.
  5 promote    LATEST := the candidate (CANDIDATE removed). Only here does the live release pointer move, so a failed
               check leaves LATEST untouched.
  6 staging    `deploy/staging/upload.sh <r-id>` (staging bucket only; MIN_APP_VERSION from --min-app-version, else the
               previous release's stamp — printed in the plan), with its smoke test run on its own after it (T537:
               ledger `uploaded_at` and `smoked_at`; a rerun after a smoke-only failure runs only the smoke). It says
               when it replaces a staged release the owner was handed a production command for. The success message
               lists the after-chores with their exact commands (goldens re-cut, site, feeds worker). On a RERUN (T484) the default is the min app this
               release's checks passed with (ledger `checks.min_app_version`), else the stamp of the release LATEST
               named before this one was promoted (ledger `live_before`) — never the new release's own stamp, which
               LATEST names once the promote is done.

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

The ledger is data/generated/release/ledger.json: per release id, the commit, the checks, when it was promoted, staged
and published.
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
STAGES = ("preflight", "baseline", "build", "checks", "promote", "staging")
#: the branch GitHub's workflows run (the staging upload starts feeds-runtiming.yml there); HEAD must be on it
GH_REMOTE, GH_BRANCH = "origin", "redesign/stream-sections"
#: manifest keys stamped at upload time (`release/stamp.py`), outside every hash: not part of the fingerprint
STAMP_KEYS = ("published", "minAppVersion")


class Stop(Exception):
    """A stage failed: `what` failed, `todo` is what the human does next."""

    def __init__(self, stage: str, what: str, todo: str):
        super().__init__(f"{stage}: {what}")
        self.stage, self.what, self.todo = stage, what, todo


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


def run_fast_fail(base: str | None = None) -> int:
    """`scripts/fast-fail.sh` (T483) on the console; its return code. (T533) `base` = FAST_FAIL_BASE: what it checks as
    changed is everything since that commit (the one LATEST was built from), not only the unpushed commits."""
    return run_cmd(["bash", "scripts/fast-fail.sh"], env={"FAST_FAIL_BASE": base} if base else None)


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


def inseason_cmd(rid: str) -> list[str]:
    return ["bash", "scripts/heavy.sh", "inseason-resolve", "30", "--", sys.executable, "-m",
            "pipeline.regs.inseason.resolve", "--release", str(release_root() / rid)]


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
    heavy_env: dict = field(default_factory=dict)   # (T535) tells heavy.sh fast-fail ran: FAST_FAIL_DONE / HEAVY_NO_FAST_FAIL
    led: dict = field(default_factory=dict)
    rid: str | None = None          # the candidate, once the build named it
    live: str | None = None         # LATEST when the run started


def rec(ctx: Ctx, rid: str) -> dict:
    return ctx.led["releases"].setdefault(rid, {})


def baseline_dir(live: str) -> Path:
    return HOME / "baseline" / live


def stage_preflight(ctx: Ctx) -> str:
    # (T535) the instant checks first; the minute-or-two fast-fail last (`preflight_fast_fail`)
    dirty = git_dirty()
    if dirty:
        raise Stop("preflight", f"{len(dirty)} uncommitted path(s) the build reads: " + ", ".join(dirty[:8])
                   + (" …" if len(dirty) > 8 else ""),
                   "commit them (a release is built from committed code and curated data only; marks.jsonl may "
                   "stay dirty), then rerun")
    try:
        behind = atlas_behind()
    except (OSError, ValueError) as e:
        raise Stop("preflight", f"cannot tell whether the atlas is current with its curated inputs: {e}",
                   "run `python -m pipeline.atlas.inputs` and fix what it says, then rerun")
    if behind and ctx.stale_atlas is None:
        raise Stop("preflight", f"the atlas is behind {len(behind)} curated input(s) it was built from (T547):\n    "
                   + "\n    ".join(behind),
                   "rebuild the atlas (`python -m pipeline build --atlas --promote`), then rerun; or ship the old atlas "
                   "with --ship-stale-atlas REASON")
    if behind:
        print(f"    atlas behind {len(behind)} input(s), SHIPPED ANYWAY (--ship-stale-atlas): {ctx.stale_atlas}",
              flush=True)
    try:
        landed, said = marks_landed()
    except RuntimeError as e:
        raise Stop("preflight", str(e), "run `python -m pipeline.regs.review.apply --check` and fix what it says")
    if not landed:
        raise Stop("preflight", "review marks in force have not landed in their curated files:\n" + said,
                   "land them (`python -m pipeline.regs.review.apply`, then commit a file it names), or put a mark "
                   "that must wait on NOT_LANDED_OK (pipeline/regs/review/apply.py), then rerun")
    try:
        pushed = git_pushed()
    except (subprocess.CalledProcessError, RuntimeError) as e:
        raise Stop("preflight", f"could not tell whether HEAD is pushed: {e}",
                   f"check the network and `git fetch {GH_REMOTE} {GH_BRANCH}`, then rerun")
    if not pushed:
        raise Stop("preflight", f"HEAD {git_head()[:12]} is not on {GH_REMOTE}/{GH_BRANCH} (the staging upload starts "
                   "a GitHub workflow that runs GitHub's copy)",
                   f"git push {GH_REMOTE} HEAD:{GH_BRANCH} (after `build-apk.sh --play`, that pushes its app.json "
                   "commit), then rerun")
    rc, said = ci_status(ctx.no_ci_check)
    print("    " + said.replace("\n", "\n    "), flush=True)
    if rc:
        raise Stop("preflight", "CI is red on HEAD (or its newest finished run)" if rc == 1 else "could not read CI (gh)",
                   "read the run named above and fix it (push the fix, let CI finish), then rerun; "
                   "--no-ci-check REASON goes on and prints the reason")
    ctx.live = read_pointer("LATEST")
    if ctx.live is None:
        raise Stop("preflight", f"no {release_root() / 'LATEST'}: there is no live release to diff against",
                   "build the first release with `python -m pipeline build` (it writes LATEST), then rerun")
    # (T483 fast-fail; caught 2026-11-09) the staging site older than the min app the release goes up with: found only by
    # the smoke test after the upload. Checked here, before the build; the upload checks again with the release's own
    # format needs (deploy/staging/upload.sh, `deliver.release.site_gate`).
    min_app = ctx.min_app or resume_min_app(ctx.led, read_pointer("CANDIDATE") or ctx.live, ctx.live)
    bad, said = site_problems(min_app)
    if bad:
        raise Stop("preflight", "; ".join(bad), "redeploy the staging site (the commands above), then rerun")
    return f"clean at {git_head()[:12]}; live release {ctx.live}; {said}; {preflight_fast_fail(ctx)}"


def fast_fail_base(ctx: Ctx) -> str | None:
    """(T533) the commit the live release (LATEST) was built from (ledger `commit`), so fast-fail's changed-file checks
    cover everything since the last ship — with HEAD pushed, "not on origin" is nothing. None (said) when not recorded."""
    base = ((ctx.led.get("releases") or {}).get(ctx.live or "") or {}).get("commit")
    if base and git_has_commit(base):
        print(f"    fast-fail base: {base[:12]}, the commit {ctx.live} was built from", flush=True)
        return base
    print(f"    fast-fail base: no commit recorded for {ctx.live} in {rel(LEDGER)}"
          + (f" ({base[:12]} is not in this clone)" if base else "") + ": its default (unpushed commits) — NOT since "
          "the last ship", flush=True)
    return None


def preflight_fast_fail(ctx: Ctx) -> str:
    """(T535) fast-fail, after the instant checks. A rerun skips it when it passed for this HEAD (the tree is clean: the
    dirty check above stopped otherwise); the build's heavy.sh is told it ran (FAST_FAIL_DONE) or why not."""
    head = git_head()
    if ctx.no_fast_fail is not None:
        print(f"    fast-fail SKIPPED (--no-fast-fail): {ctx.no_fast_fail}", flush=True)
        ctx.heavy_env = {"HEAVY_NO_FAST_FAIL": f"release --no-fast-fail: {ctx.no_fast_fail}"}
        return "fast-fail skipped (--no-fast-fail)"
    done = ctx.led.get("fast_fail") or {}
    if done.get("head") == head:
        msg = f"fast-fail passed for {head[:12]} at {done.get('at')}: not rerun"
        print(f"    {msg}", flush=True)
    else:
        rc = run_fast_fail(fast_fail_base(ctx))
        if rc:
            raise Stop("preflight", f"scripts/fast-fail.sh failed (rc={rc})",
                       "read its output above (the last check printed is the one that failed), fix it, rerun; "
                       "--no-fast-fail REASON skips it and prints the reason")
        ctx.led["fast_fail"] = {"head": head, "at": now()}
        save_ledger(ctx.led)
        msg = "fast-fail passed"
    ctx.heavy_env = {"FAST_FAIL_DONE": head}
    return msg


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
        if got != live_digest:
            raise Stop("baseline", f"--baseline {ctx.baseline} is reach run {got}, the live release {ctx.live} was "
                       f"cut from {live_digest}", "name the reach run the live release was built from")
        return f"--baseline {ctx.baseline} (reach {got})"
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
    inseason = stage_inseason(ctx, rid, r)
    r["checks"] = {"manifest": "ok", "app_gate": "ok", "min_app_version": ctx.min_app,
                   "binding_diff": done["status"], "inseason": inseason, "at": now()}
    r["fingerprint"] = fingerprint(rid)
    save_ledger(ctx.led)
    return (f"manifest ok, app gate ok (min app {ctx.min_app or 'manifest stamp'}), bindings {done['status']}, "
            f"in-season {inseason}")


def stage_inseason(ctx: Ctx, rid: str, r: dict) -> str:
    """(T537) resolve the in-season matches on the candidate BEFORE the promote (was: mid-upload, after LATEST moved,
    and the rewritten file then dirtied the next preflight). Done once per release (ledger `inseason`)."""
    if (r.get("inseason") or {}).get("release") == rid:
        return f"resolved on {rid} ({r['inseason'].get('at')})"
    rc = run_cmd(inseason_cmd(rid), env=ctx.heavy_env or None)
    if rc:
        raise Stop("checks", f"the in-season matches do not all resolve on {rid} (rc={rc}); LATEST was not moved",
                   "re-match the unresolved notices in the review app's In-season tab (the output above names them), "
                   "commit data/curated/inseason/notice_waters.json, then rerun")
    if inseason_dirty():
        head = commit_inseason(rid)
        r["inseason"] = {"release": rid, "at": now(), "commit": head}
        save_ledger(ctx.led)
        raise Stop("checks", f"the in-season matches were re-resolved on {rid} and committed ({head[:12]}, "
                   f"{rel(INSEASON)} only)",
                   f"git push {GH_REMOTE} HEAD:{GH_BRANCH} (the upload's workflow runs GitHub's copy), then rerun")
    r["inseason"] = {"release": rid, "at": now()}
    save_ledger(ctx.led)
    return f"resolved on {rid}"


def stage_promote(ctx: Ctx) -> str:
    from pipeline.deliver.release.manifest import write_latest
    rid = ctx.rid
    if read_pointer("LATEST") != rid:
        write_latest(release_root(), rid)
    if read_pointer("CANDIDATE") == rid:
        (release_root() / "CANDIDATE").unlink()
    rec(ctx, rid).setdefault("promoted_at", now())
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
    for old in handed_for_prod(ctx.led, rid):
        print(f"    NOTE: this replaces {old} on staging, staged {ctx.led['releases'][old]['staged_at']} and never "
              f"published — the production command handed for it (release --prod {old}) will refuse once {rid} is "
              "up", flush=True)
    env = {**ctx.heavy_env, **({"MIN_APP_VERSION": ctx.min_app} if ctx.min_app else {}),
           **({"CI_CHECK_SKIP": f"release --no-ci-check: {ctx.no_ci_check}"} if ctx.no_ci_check is not None else {}),
           "SMOKE": "0"}                   # (T537) the smoke runs on its own below, so a smoke failure reruns only it
    rc = run_cmd(upload_cmd(rid), env=env)
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
    lines.append("  goldens: " + ("cut from LATEST already" if goldens.returncode == 0 else
                                  "RE-CUT NEEDED (fast-fail is red in every lane until then):\n"
                                  "      node app/tools/regs-recut/sync.mjs && git diff --stat app/fixtures app/packages "
                                  "&& git commit -- <the re-cut files>"))
    lines.append("  site:    the staging site serves an app at least the release's min app (checked in preflight); "
                 "after app changes: deploy/staging/site-all.sh")
    lines.append("  feeds:   the feeds worker goes with the app (memory feeds-worker-deploy-allowed): deploy/staging/feeds.sh")
    lines.append(f"  prod (the owner's): CONFIRM_PROD={rid} python -m pipeline release --prod {rid}")
    return "\n".join(lines)


RUNNERS = {"preflight": stage_preflight, "baseline": stage_baseline, "build": stage_build, "checks": stage_checks,
           "promote": stage_promote, "staging": stage_staging}


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


def staging(ctx: Ctx) -> int:
    ctx.led = load_ledger()
    t0 = time.time()
    for i, s in enumerate(STAGES, 1):
        print(f"\n[{i}/{len(STAGES)}] {s} …", flush=True)
        t = time.time()
        if s == "checks" and ctx.min_app is None:
            ctx.min_app = resume_min_app(ctx.led, ctx.rid, ctx.live)
        try:
            msg = RUNNERS[s](ctx)
        except Stop as e:
            print(f"\n[{i}/{len(STAGES)}] {s} FAILED: {e.what}\n\nrelease: STOPPED at {s}.\n  next: {e.todo}\n"
                  f"  then run the same command again; what finished is skipped.", file=sys.stderr, flush=True)
            return 1
        print(f"[{i}/{len(STAGES)}] {s} ok ({round(time.time() - t)}s): {msg}", flush=True)
    secs = round(time.time() - t0)
    print(f"\nrelease: {ctx.rid} is LATEST and on staging ({secs // 60}m{secs % 60:02d}s).\n{after_chores(ctx.rid)}")
    return 0


def dry_run(ctx: Ctx) -> int:
    """The plan: what each stage would do now. Runs nothing but the build's own --dry-run."""
    led = load_ledger()
    live, cand = read_pointer("LATEST"), read_pointer("CANDIDATE")
    print("release --staging, DRY RUN (nothing is built, promoted or uploaded)\n")
    dirty = git_dirty()
    ff = led.get("fast_fail") or {}
    print("[1/6] preflight  " + (f"fast-fail SKIPPED: {ctx.no_fast_fail}" if ctx.no_fast_fail is not None
                                   else f"fast-fail passed for HEAD at {ff.get('at')}: a rerun skips it"
                                   if ff.get("head") == git_head() else
                                   "bash scripts/fast-fail.sh, last (not run in a dry run)"))
    print("             " + ("ok: code + data/curated committed" if not dirty else
                             f"WOULD STOP: {len(dirty)} uncommitted path(s): {', '.join(dirty[:8])}")
          + f"  (HEAD {git_head()[:12]}; marks.jsonl / closed.json may be dirty)")
    try:
        behind = atlas_behind()
        print("             " + ("ok: the atlas is current with its curated inputs" if not behind else
                                 ("SHIPS ANYWAY (--ship-stale-atlas)" if ctx.stale_atlas is not None else "WOULD STOP")
                                 + f": the atlas is behind {len(behind)} input(s):\n               "
                                 + "\n               ".join(behind)))
    except (OSError, ValueError) as e:
        print(f"             WOULD STOP: cannot tell whether the atlas is current ({e})")
    try:
        landed, said = marks_landed()
        print("             " + ("ok: " + said.splitlines()[-1] if landed else "WOULD STOP: " + next(
            (ln for ln in said.splitlines() if "NOT LANDED" in ln), said)
            + "  (python -m pipeline.regs.review.apply --check lists them)"))
    except RuntimeError as e:
        print(f"             WOULD STOP: {e}")
    try:
        pushed = "ok: HEAD is on" if git_pushed() else "WOULD STOP: HEAD is not on"
    except (subprocess.CalledProcessError, RuntimeError) as e:
        pushed = f"WOULD STOP ({e}): cannot tell whether HEAD is on"
    print(f"             {pushed} {GH_REMOTE}/{GH_BRANCH}")
    rc, said = ci_status(ctx.no_ci_check)
    print("             " + ("" if rc == 0 else "WOULD STOP: ") + said.replace("\n", "\n             "))
    base = ((led.get("releases") or {}).get(live or "") or {}).get("commit")
    print(f"             fast-fail base: {base[:12] + ', the commit ' + str(live) + ' was built from' if base else 'none recorded for ' + str(live) + ' (its default)'}")
    # (T535) the gates the real preflight and checks stop on: the staging site and the app version
    target = cand or live
    min_app = ctx.min_app or resume_min_app(led, target, live)
    try:
        bad, said = site_problems(min_app)
        print("             " + (f"WOULD STOP: {'; '.join(bad)}" if bad else f"ok: {said}"))
    except Exception as e:  # noqa: BLE001 — shown: an unreadable site is a stop in the real run (site_gate)
        print(f"             WOULD STOP: the staging site gate could not run ({type(e).__name__}: {e})")
    if target:
        probs = app_gate_problems(target, min_app)
        print(f"             app-version gate on {target} (min app {min_app or 'any'}): "
              + (f"WOULD STOP: {'; '.join(probs)}" if probs else "ok"))
    lock = Path(os.environ.get("HEAVY_LOCK", Path.home() / ".claude/heavy-job.lock")) / "owner"
    held = lock.read_text().splitlines()[0] if lock.exists() else None   # line 1 `<pid> <label> <date>` (T536)
    print(f"             heavy-job lock: {'held by ' + held + '  (scripts/heavy.sh --status)' if held else 'free'}")
    live_digest = (manifest_of(live).get("bundle") or {}).get("reach_digest") if live else None
    kept = reach_digest_of(baseline_dir(live)) == live_digest if live else False
    on_disk = reach_digest_of(reach_dir())
    print(f"[2/6] baseline   live release {live} (reach {live_digest}): "
          + ("kept copy" if kept else f"would copy {rel(reach_dir())}" if on_disk == live_digest
             else f"WOULD STOP: reach run on disk is {on_disk} and no copy is kept (--baseline DIR)"))
    print(f"[3/6] build      {' '.join(rel(c) if c.startswith(str(REPO_ROOT)) else c for c in build_cmd(jobs=ctx.jobs))}"
          "\n             the build's plan:")
    sys.stdout.flush()
    rc = run_cmd(build_cmd(dry_run=True, jobs=ctx.jobs))
    if rc:
        print(f"             (the build's dry run returned rc={rc})")
    print(f"             CANDIDATE now: {cand or '(none)'} — the build names its release there; LATEST stays {live}")
    r = led.get("releases", {}).get(target or "", {})
    print(f"[4/6] checks     manifest sanity; app gate (min app {min_app or 'any'}"
          + ("" if ctx.min_app else ", from the ledger, else the live release's stamp") + "); binding diff "
          f"{rel(baseline_dir(live)) if live else '?'} -> {rel(reach_dir())}"
          + (f"\n             ledger for {target}: {r.get('checks')}" if r.get("checks") else ""))
    print(f"[5/6] promote    LATEST {live} -> the candidate (after every check passes)")
    print(f"[6/6] staging    MIN_APP_VERSION={min_app or ''} {' '.join(upload_cmd('<r-id>'))}"
          + (f"  ({target} staged {r['staged_at']})" if r.get("staged_at") else ""))
    # (T541) informational: what the review app's pending-build panel lists, read with no server — never a stop
    try:
        from pipeline.regs.review import pending as _pending
        print(f"review           {_pending.summary_line(_pending.pending_on_disk())}  "
              "(python -m pipeline.regs.review.pending lists them)")
    except Exception as e:  # noqa: BLE001 — shown, never swallowed: the dry run still prints the rest of the plan
        print(f"review           COULD NOT READ the pending review build ({type(e).__name__}: {e})")
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
    ap.add_argument("--dry-run", action="store_true", help="print the plan; run nothing")
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
    ctx = Ctx(accept_diff=a.accept_diff, baseline=a.baseline, min_app=a.min_app_version, jobs=a.jobs,
              reupload=a.reupload, no_fast_fail=a.no_fast_fail, stale_atlas=a.ship_stale_atlas,
              no_ci_check=a.no_ci_check)
    return dry_run(ctx) if a.dry_run else staging(ctx)


if __name__ == "__main__":
    raise SystemExit(main())
