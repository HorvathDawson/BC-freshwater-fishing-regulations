"""ONE RELEASE COMMAND (T295/T297, T408 re-scope, owner 2026-11-05): build once, promote the same artifact.

    python -m pipeline release --staging                      # build -> checks -> promote LATEST -> upload staging
    python -m pipeline release --staging --dry-run            # the plan: each stage and what it would do; nothing runs
    python -m pipeline release --staging --accept-diff FILE   # go on past a binding diff you have read (FILE = its json)
    python -m pipeline release --prod <r-id>                  # HUMAN-ONLY: re-upload THAT checked release to production

THE STAGES, in order; the run stops at the first failure with a plain message, and a rerun of the same command picks up
where it stopped (every stage below is skipped when it is already done for this release):

  1 preflight  code and data/curated committed (the review log marks.jsonl and closed.json may be dirty: no stage reads
               them, `build.NOT_BUILD_INPUTS`); the HEAD commit is recorded with the release.
  2 baseline   a copy of the reach run the LIVE release (LATEST) was cut from, kept before the build can overwrite it,
               so the binding diff has its "before" (data/generated/release/baseline/<LATEST>/).
  3 build      `python -m pipeline build --hold-latest` through `scripts/heavy.sh` (one heavy job at a time). The build
               caches its own stages (its manifest keys every stage by its inputs), so a rerun skips what is done.
               --hold-latest: the new release is written to data/release/<r-id>/ and named in data/release/CANDIDATE;
               LATEST does NOT move.
  4 checks     manifest sanity (every file the manifest names is there at its size, the size gate, the bundle block
               names the reach run on disk), the app-version gate (`deliver.release.app_gate`, the one the staging
               upload runs), and the reach BINDING DIFF against the live release (`atlas.reach.binding_diff`). A diff
               that is not empty STOPS the run: the summary is printed and written beside its json; a reviewer reads it
               and reruns with `--accept-diff <that json>` (its content must be the one written, so a stale accept never
               passes a new diff).
  5 promote    LATEST := the candidate (CANDIDATE removed). Only here does the live release pointer move, so a failed
               check leaves LATEST untouched.
  6 staging    `deploy/staging/upload.sh <r-id>` (staging bucket only; MIN_APP_VERSION from --min-app-version, else the
               previous release's stamp — printed in the plan).

WHERE THINGS MOVE BEFORE THE CHECKS: the build writes the live working outputs (the reach run, tiles, bundle, export,
run-timing feed) as `pipeline build` always has — the review app reads them — and only the release POINTER waits. A
release whose checks failed is on disk under data/release/<r-id>/ and named by CANDIDATE; nothing ships it.

PROD (`--prod <r-id>`) builds nothing and checks nothing new: it re-uploads the SAME release folder that passed this
command's checks and reached staging (the ledger says so, and its content fingerprint must be unchanged), through the
existing `python -m pipeline.deliver.release.publish --release data/release/<r-id> --yes`. It runs ONLY for a human:
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
BASELINE_FILES = ("report.json", "rule_section.jsonl.gz", "rule_unresolved.jsonl", "licensing_section.jsonl")
#: min free memory % for heavy.sh to start the build (memory `heavy-jobs-one-at-a-time`)
HEAVY_MIN_FREE = 50
STAGES = ("preflight", "baseline", "build", "checks", "promote", "staging")
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
               (CURATED.regulations.review.marks, CURATED.regulations.review.closed)}
    paths = [ln[3:].split(" -> ")[-1].strip().strip('"') for ln in out.splitlines() if ln.strip()]
    return [p for p in paths if p not in allowed]


def git_head() -> str:
    return subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, capture_output=True, text=True,
                          check=True).stdout.strip()


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
    return [sys.executable, "-m", "pipeline.deliver.release.publish", "--release", str(release_root() / rid), "--yes"]


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
    baseline: Path | None = None
    min_app: str | None = None
    jobs: int = 2
    reupload: bool = False
    led: dict = field(default_factory=dict)
    rid: str | None = None          # the candidate, once the build named it
    live: str | None = None         # LATEST when the run started


def rec(ctx: Ctx, rid: str) -> dict:
    return ctx.led["releases"].setdefault(rid, {})


def baseline_dir(live: str) -> Path:
    return HOME / "baseline" / live


def stage_preflight(ctx: Ctx) -> str:
    dirty = git_dirty()
    if dirty:
        raise Stop("preflight", f"{len(dirty)} uncommitted path(s) the build reads: " + ", ".join(dirty[:8])
                   + (" …" if len(dirty) > 8 else ""),
                   "commit them (a release is built from committed code and curated data only; marks.jsonl may "
                   "stay dirty), then rerun")
    ctx.live = read_pointer("LATEST")
    if ctx.live is None:
        raise Stop("preflight", f"no {release_root() / 'LATEST'}: there is no live release to diff against",
                   "build the first release with `python -m pipeline build` (it writes LATEST), then rerun")
    return f"clean at {git_head()[:12]}; live release {ctx.live}"


def stage_baseline(ctx: Ctx) -> str:
    """Keep the reach run LATEST was cut from, before the build can overwrite it (the binding diff's "before")."""
    live_digest = (manifest_of(ctx.live).get("bundle") or {}).get("reach_digest")
    dest = baseline_dir(ctx.live)
    if ctx.baseline is not None:
        got = reach_digest_of(ctx.baseline)
        if got != live_digest:
            raise Stop("baseline", f"--baseline {ctx.baseline} is reach run {got}, the live release {ctx.live} was "
                       f"cut from {live_digest}", "name the reach run the live release was built from")
        return f"--baseline {ctx.baseline} (reach {got})"
    if reach_digest_of(dest) == live_digest:
        return f"kept: {dest} (reach {live_digest})"
    on_disk = reach_digest_of(reach_dir())
    if on_disk != live_digest:
        # CANDIDATE from an earlier failed run, or a reach run done outside a release: the "before" is gone
        raise Stop("baseline", f"the reach run on disk ({on_disk}) is not the one the live release {ctx.live} was cut "
                   f"from ({live_digest}), and no copy of that one was kept",
                   f"name a reach run whose report.json digest is {live_digest} with --baseline DIR "
                   f"(e.g. data/generated/reaches/full_prev), or rebuild LATEST's run")
    tmp = dest.with_name(dest.name + ".tmp")
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir(parents=True)
    for n in BASELINE_FILES:
        if (reach_dir() / n).exists():
            shutil.copy2(reach_dir() / n, tmp / n)
    shutil.rmtree(dest, ignore_errors=True)
    tmp.replace(dest)
    return f"copied the live reach run ({live_digest}) to {dest}"


def stage_build(ctx: Ctx) -> str:
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
            differs = rep["rules_changed"] or rep["unresolved_changed"] or rep["licensing_changed"]
            done = {"key": diff_key, "status": "differs" if differs else "same", "json": str(js),
                    "sha": file_sha(js), "summary": str(txt)}
        r["binding_diff"] = done
        save_ledger(ctx.led)
    if done["status"] == "differs":
        if ctx.accept_diff is not None and Path(ctx.accept_diff).is_file() \
                and file_sha(Path(ctx.accept_diff)) == done["sha"]:
            done.update(status="accepted", accepted_at=now())
            save_ledger(ctx.led)
        else:
            print(Path(done["summary"]).read_text(encoding="utf-8"))
            why = ("" if ctx.accept_diff is None else
                   f" (--accept-diff {ctx.accept_diff} is not this diff: its content differs from {done['json']})")
            raise Stop("checks", f"the rule bindings differ from the live release {ctx.live}{why}",
                       f"read {done['summary']} (full: {done['json']}); if every change is intended, rerun with\n"
                       f"    --accept-diff {done['json']}\n  otherwise fix the cause, commit, and rerun")
    r["checks"] = {"manifest": "ok", "app_gate": "ok", "min_app_version": ctx.min_app,
                   "binding_diff": done["status"], "at": now()}
    r["fingerprint"] = fingerprint(rid)
    save_ledger(ctx.led)
    return f"manifest ok, app gate ok (min app {ctx.min_app or 'manifest stamp'}), bindings {done['status']}"


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


def stage_staging(ctx: Ctx) -> str:
    rid, r = ctx.rid, rec(ctx, ctx.rid)
    if r.get("staged_at") and not ctx.reupload:
        return f"already on staging ({r['staged_at']}); --reupload to send it again"
    if r.get("fingerprint") != fingerprint(rid):
        raise Stop("staging", f"{rid} changed on disk after its checks (the fingerprint moved)",
                   "do not ship it: rebuild (`python -m pipeline build --force deliver --hold-latest`) and rerun")
    env = {"MIN_APP_VERSION": ctx.min_app} if ctx.min_app else {}
    rc = run_cmd(upload_cmd(rid), env=env)
    if rc:
        raise Stop("staging", f"deploy/staging/upload.sh {rid} failed (rc={rc})",
                   "read its output above (rclone remote `r2:`, the run-timing feed, the smoke test); fix it and rerun "
                   "— the build, checks and promote are done and are skipped")
    r["staged_at"] = now()                   # the upload stamped the manifest; the fingerprint ignores the stamps
    save_ledger(ctx.led)
    return f"{rid} uploaded to staging"


RUNNERS = {"preflight": stage_preflight, "baseline": stage_baseline, "build": stage_build, "checks": stage_checks,
           "promote": stage_promote, "staging": stage_staging}


def default_min_app(live: str | None) -> str | None:
    """The previous release's stamp: the oldest app allowed does not move unless the human says so."""
    return manifest_of(live).get("minAppVersion") if live else None


def staging(ctx: Ctx) -> int:
    ctx.led = load_ledger()
    t0 = time.time()
    for i, s in enumerate(STAGES, 1):
        print(f"\n[{i}/{len(STAGES)}] {s} …", flush=True)
        t = time.time()
        if s == "checks" and ctx.min_app is None:
            ctx.min_app = default_min_app(ctx.live)
        try:
            msg = RUNNERS[s](ctx)
        except Stop as e:
            print(f"\n[{i}/{len(STAGES)}] {s} FAILED: {e.what}\n\nrelease: STOPPED at {s}.\n  next: {e.todo}\n"
                  f"  then run the same command again; what finished is skipped.", file=sys.stderr, flush=True)
            return 1
        print(f"[{i}/{len(STAGES)}] {s} ok ({round(time.time() - t)}s): {msg}", flush=True)
    secs = round(time.time() - t0)
    print(f"\nrelease: {ctx.rid} is LATEST and on staging ({secs // 60}m{secs % 60:02d}s). Production is the owner's:\n"
          f"    CONFIRM_PROD={ctx.rid} python -m pipeline release --prod {ctx.rid}")
    return 0


def dry_run(ctx: Ctx) -> int:
    """The plan: what each stage would do now. Runs nothing but the build's own --dry-run."""
    led = load_ledger()
    live, cand = read_pointer("LATEST"), read_pointer("CANDIDATE")
    print("release --staging, DRY RUN (nothing is built, promoted or uploaded)\n")
    dirty = git_dirty()
    print("[1/6] preflight  " + ("ok: code + data/curated committed" if not dirty else
                                   f"WOULD STOP: {len(dirty)} uncommitted path(s): {', '.join(dirty[:8])}")
          + f"  (HEAD {git_head()[:12]}; marks.jsonl / closed.json may be dirty)")
    lock = Path(os.environ.get("HEAVY_LOCK", Path.home() / ".claude/heavy-job.lock")) / "owner"
    print(f"             heavy-job lock: {'held by ' + lock.read_text().strip() if lock.exists() else 'free'}")
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
    min_app = ctx.min_app or default_min_app(live)
    target = cand or live
    r = led.get("releases", {}).get(target or "", {})
    print(f"[4/6] checks     manifest sanity; app gate (min app {min_app or 'any'}"
          + ("" if ctx.min_app else ", from the live release's stamp") + "); binding diff "
          f"{rel(baseline_dir(live)) if live else '?'} -> {rel(reach_dir())}"
          + (f"\n             ledger for {target}: {r.get('checks')}" if r.get("checks") else ""))
    print(f"[5/6] promote    LATEST {live} -> the candidate (after every check passes)")
    print(f"[6/6] staging    MIN_APP_VERSION={min_app or ''} {' '.join(upload_cmd('<r-id>'))}"
          + (f"  ({target} staged {r['staged_at']})" if r.get("staged_at") else ""))
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
            f"    export R2_ACCOUNT_ID=… R2_ACCESS_KEY_ID=… R2_SECRET_ACCESS_KEY=… R2_BUCKET=<the production bucket>\n"
            f"    python -m pipeline.deliver.release.publish --release {release_root() / rid}     # the plan, nothing sent\n"
            f"    CONFIRM_PROD={rid} python -m pipeline release --prod {rid}\n"
            f"which runs (after you type the id again): {' '.join(prod_cmd(rid))}\n"
            f"The production site, if it must follow, is separate: deploy/prod/site.sh (CONFIRM_PROD=canifishthis.ca).")


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
    rc = run_cmd(prod_cmd(rid))
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
    ap.add_argument("--jobs", type=int, default=2, help="the build's --jobs (default 2, at most 4)")
    ap.add_argument("--reupload", action="store_true", help="upload to staging again even if the ledger says it is")
    a = ap.parse_args(argv)
    if a.prod:
        if a.dry_run:
            r = load_ledger().get("releases", {}).get(a.prod, {})
            print(prod_instructions(a.prod, r))
            return 0
        return prod(a.prod)
    ctx = Ctx(accept_diff=a.accept_diff, baseline=a.baseline, min_app=a.min_app_version, jobs=a.jobs,
              reupload=a.reupload)
    return dry_run(ctx) if a.dry_run else staging(ctx)


if __name__ == "__main__":
    raise SystemExit(main())
