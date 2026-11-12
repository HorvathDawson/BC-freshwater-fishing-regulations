"""T433 (audit 2026-11-05): the staging scripts fail safe.

  upload        the bucket is cleared AFTER the pointer flip, sparing what the new release names (a failure above it
                once left staging with no manifest)
  sync-inputs   a failed rclone copy stops the script (run, against a stub rclone); it once printed "in sync"
  hydat         fetch_data's hydat layer downloads through the checked scripts/hydat-fetch.sh and raises on an
                unreadable listing
  check-public  a failed secret read is not swallowed (a secret it cannot read is one it cannot look for)
  testflight    --production needs CONFIRM_PROD, before the heavy queue (run)
"""
from __future__ import annotations

import os
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def _exe(p: Path, text: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)
    p.chmod(p.stat().st_mode | stat.S_IXUSR)


def test_upload_clears_after_the_pointer_and_spares_the_release():
    lines = (ROOT / "deploy/staging/upload.sh").read_text().splitlines()
    run = [ln for ln in lines if not ln.lstrip().startswith("#")]
    pointer = next(i for i, ln in enumerate(run) if 'cp_ "$M" release/manifest.json' in ln)
    clear = [i for i, ln in enumerate(run) if ln.startswith("rclone delete")]
    assert clear and all(i > pointer for i in clear), "the clear must come after the pointer flip"
    cmd = " ".join(run[clear[0]:clear[0] + 3])
    for spared in ('"release/manifest.json"', '"release/tiles/$TILES/**"', '"release/$REL/**"', '"release/$BM"',
                   '"release/$SYN"', '"feeds/**"', '"private/**"', '"runtiming/**"', '"test/**"'):
        assert f"--exclude {spared}" in cmd, spared


PY_STUB = """#!/usr/bin/env bash
# pipeline.deliver.sync_inputs --out <dir>: two inputs
out=$4; mkdir -p "$out/feeds/inseason" "$out/dfo"
echo '{"notices": {"a": 1, "b": 2}}' > "$out/feeds/inseason/matches.json"; echo '{}' > "$out/dfo/a.json"
printf 'dfo/a.json\\t-\\nfeeds/inseason/matches.json\\tno-cache\\n'
"""


@pytest.fixture
def sync_tree(tmp_path):
    (tmp_path / "deploy/staging").mkdir(parents=True)
    shutil.copy(ROOT / "deploy/staging/sync-inputs.sh", tmp_path / "deploy/staging/sync-inputs.sh")
    (tmp_path / "deploy/lib").mkdir()
    shutil.copy(ROOT / "deploy/lib/input-formats.sh", tmp_path / "deploy/lib/input-formats.sh")   # the shared format check
    _exe(tmp_path / ".venv/bin/python", PY_STUB)
    # (T520) the in-season matches already resolved on LATEST: no resolve step
    (tmp_path / "data/release").mkdir(parents=True)
    (tmp_path / "data/release/LATEST").write_text("r-1\n")
    (tmp_path / "data/curated/inseason").mkdir(parents=True)
    (tmp_path / "data/curated/inseason/notice_waters.json").write_text('{"release": "r-1"}')
    return tmp_path


def _sync(tree: Path, rclone_rc: int) -> subprocess.CompletedProcess:
    _exe(tree / "bin/rclone", f'#!/usr/bin/env bash\necho "$*" >> "{tree}/rclone.log"\n'
                              f'echo "ERROR : boom" >&2\nexit {rclone_rc}\n' if rclone_rc else
         f'#!/usr/bin/env bash\necho "$*" >> "{tree}/rclone.log"\necho "INFO  : x: Copied (new)" >&2\n')
    env = {**os.environ, "PATH": f"{tree / 'bin'}:{os.environ['PATH']}"}
    return subprocess.run(["bash", str(tree / "deploy/staging/sync-inputs.sh")], env=env, capture_output=True,
                          text=True)


def test_sync_inputs_stops_on_a_failed_copy(sync_tree):
    r = _sync(sync_tree, 1)
    assert r.returncode != 0
    assert "inputs in sync" not in r.stdout
    assert "sync-inputs: dfo/a.json failed" in r.stderr
    assert len((sync_tree / "rclone.log").read_text().splitlines()) == 1     # nothing after the failed one


def test_sync_inputs_only_verifies_matches_on_another_release(sync_tree):
    """(T537) matches on another release than LATEST are refused with the resolve + commit commands; nothing is put
    and nothing is re-resolved here (`pipeline release` resolves them on the candidate before its promote)."""
    (sync_tree / "data/release/LATEST").write_text("r-2\n")
    _exe(sync_tree / "scripts/heavy.sh", f'#!/usr/bin/env bash\necho ran >> "{sync_tree}/heavy.log"\n')
    r = _sync(sync_tree, 0)
    assert r.returncode != 0 and "LATEST is r-2 — nothing was put" in r.stderr
    assert "pipeline.regs.inseason.resolve --release data/release/r-2" in r.stderr and "git commit" in r.stderr
    assert not (sync_tree / "rclone.log").exists() and not (sync_tree / "heavy.log").exists()


def test_sync_inputs_from_the_review_app_never_rewrites_the_matches(sync_tree):
    """(T520) from the review app, matches on another release are REFUSED (re-resolve there, then commit) — the
    curated file is never rewritten outside its writes.commit."""
    (sync_tree / "data/release/LATEST").write_text("r-2\n")
    _exe(sync_tree / "scripts/heavy.sh", f'#!/usr/bin/env bash\necho ran >> "{sync_tree}/heavy.log"\n')
    env_tree = sync_tree
    r = subprocess.run(["bash", str(env_tree / "deploy/staging/sync-inputs.sh")], capture_output=True, text=True,
                       env={**os.environ, "SYNC_INPUTS_FROM_REVIEW": "1",
                            "PATH": f"{env_tree / 'bin'}:{os.environ['PATH']}"})
    assert r.returncode != 0 and "re-resolve in the review app, then commit" in r.stderr
    assert not (sync_tree / "heavy.log").exists()


def test_sync_inputs_puts_the_inputs_that_staged_and_names_the_one_that_did_not(sync_tree):
    _exe(sync_tree / ".venv/bin/python", PY_STUB + 'echo "FAILED inseason-matches: refused — is-a" >&2\nexit 1\n')
    r = _sync(sync_tree, 0)
    assert r.returncode != 0
    assert "FAILED inseason-matches: refused — is-a" in r.stderr
    assert len((sync_tree / "rclone.log").read_text().splitlines()) == 2      # the others still went up


def test_sync_inputs_waits_for_another_sync(sync_tree):
    """(T520) one at a time across processes: a live holder of the lock is waited for (here: refused after it)."""
    import time
    lock = sync_tree / "data/generated/.sync-inputs.lock"
    lock.mkdir(parents=True)
    holder = subprocess.Popen(["sleep", "30"])
    try:
        (lock / "pid").write_text(str(holder.pid))
        _exe(sync_tree / "bin/seq", "#!/usr/bin/env bash\necho 1\necho 2\n")      # two tries, not 600
        _exe(sync_tree / "bin/sleep", "#!/usr/bin/env bash\nexit 0\n")
        t0 = time.monotonic()
        r = _sync(sync_tree, 0)
        assert r.returncode != 0 and "another sync held the lock" in r.stderr and time.monotonic() - t0 < 20
        assert not (sync_tree / "rclone.log").exists()
    finally:
        holder.kill()


def test_sync_inputs_reports_a_clean_copy(sync_tree):
    r = _sync(sync_tree, 0)
    assert r.returncode == 0, r.stderr
    assert "inputs in sync: r2:bc-fishing-regulations-staging (2 objects, in-season matches 2)" in r.stdout


@pytest.fixture
def fetch_data(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "data"))
    import fetch_data as F
    return F


def test_hydat_layer_uses_the_checked_downloader(fetch_data, tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(fetch_data, "hydat_latest_release", lambda: "20261020")
    monkeypatch.setattr(subprocess, "run", lambda cmd, **k: calls.append((cmd, k)))
    fetch_data.fetch_hydat("hydat", tmp_path / "hydat.sqlite3")
    assert calls == [(["bash", str(ROOT / "scripts/hydat-fetch.sh"), "20261020", str(tmp_path / "hydat-download"),
                       str(tmp_path / "hydat.sqlite3")], {"check": True})]


def test_hydat_layer_raises_on_an_unreadable_listing(fetch_data, tmp_path, monkeypatch):
    monkeypatch.setattr(fetch_data, "hydat_latest_release", lambda: None)
    with pytest.raises(RuntimeError, match="could not read the HYDAT listing"):
        fetch_data.fetch_hydat("hydat", tmp_path / "hydat.sqlite3")


def test_check_public_does_not_swallow_a_failed_secret_read():
    src = (ROOT / "deploy/staging/check-public.mjs").read_text()
    read = src[src.index("function readPrivate"):src.index("/** the secret VALUES")]
    assert "catch" not in read, "readPrivate swallows a failed read"
    dump = src[src.index("'config', 'dump'") - 200:src.index("'config', 'dump'")]
    assert "try {" not in dump.splitlines()[-1], "a failed rclone config dump is swallowed"
    assert "e.code !== 'ENOENT'" in src                        # only a missing .run-token is a skip


def test_testflight_production_needs_confirm_prod():
    env = {k: v for k, v in os.environ.items() if k not in ("CONFIRM_PROD", "BUILD_IOS_INNER")}
    # --dry-run: without the guard this only prints its plan (exit 0); never a build, never the heavy queue
    r = subprocess.run(["bash", str(ROOT / "deploy/staging/build-ios-testflight.sh"), "--production", "--dry-run"], env=env,
                       capture_output=True, text=True, timeout=30)
    assert r.returncode == 2
    assert "set CONFIRM_PROD=canifishthis.ca" in r.stderr


def test_upload_puts_the_pointer_coupled_group_last_and_a_failed_test_reset_stops():
    """(T537) run timing done -> inputs (sync-inputs.sh) -> the pointer, as one last group; the test/ reset was a WARNING."""
    up = (ROOT / "deploy/staging/upload.sh").read_text()
    code = "\n".join(ln for ln in up.splitlines() if not ln.lstrip().startswith("#"))
    assert code.index('gh run watch "$RT_RUN"') < code.index("deploy/staging/sync-inputs.sh") \
        < code.index("cp_ \"$M\" release/manifest.json")
    assert code.count("deploy/staging/sync-inputs.sh") == 1
    reset = next(ln for ln in code.splitlines() if "test-updates.mjs reset" in ln)
    assert "exit 1" in reset and "WARNING" not in reset


def test_build_apk_checks_before_the_lock_and_commits_the_bump_after_the_artifact():
    """(T538) the precheck (changelog, keystores, Play, wrangler, token, readelf, AAR, app.json, uncommitted app/) runs
    before `exec scripts/heavy.sh`; Play notes come from the changelog, never commit subjects; the versionCode bump is
    committed only after the AAB's MapLibre check; one build-id check serves both paths."""
    sh = (ROOT / "deploy/staging/build-apk.sh").read_text()
    code = "\n".join(ln for ln in sh.splitlines() if not ln.lstrip().startswith("#"))
    lock = code.index('exec scripts/heavy.sh build-apk')
    for probe in ("scripts/check-changelog.mjs", "upload_certificate.pem", "--max-version-code", "whoami",
                  "$TOKF", "llvm-readelf", "git status --porcelain --untracked-files=all -- app/"):
        assert code.index(probe) < lock, probe
    assert "--format=%s" not in code                                   # no commit subjects as notes
    assert code.index('mln_check "$AAB"') < code.index("commit_bump\n") < code.index("play-upload.py\" --aab")
    assert 'mln_check "$AAB"' in code and 'mln_check "$APKF"' in code and code.count("mln_check() {") == 1


def _formatted(tree: Path, fmt: str, served: str | None) -> subprocess.CompletedProcess:
    """(T597 item 2) the staged matches name `fmt`; the worker's /status.json serves `served` (None: unreachable)."""
    old = """'{"notices": {"a": 1, "b": 2}}'"""
    assert old in PY_STUB
    _exe(tree / ".venv/bin/python", PY_STUB.replace(old, f"""'{{"format": "{fmt}", "notices": {{"a": 1}}}}'"""))
    _exe(tree / "bin/curl", "#!/usr/bin/env bash\nexit 7\n" if served is None else
         f"#!/usr/bin/env bash\necho '{served}'\n")
    return _sync(tree, 0)


def test_sync_inputs_refuses_a_format_the_deployed_worker_does_not_read(sync_tree):
    """(T597 item 2, 2026-11-09: matches.json went to inseason-links/3 before the worker that reads it): refused with a
    plain message and not put; the input with no format still goes up."""
    r = _formatted(sync_tree, "inseason-links/3", '{"inputs": {"feeds/inseason/matches.json": ["inseason-links/2"]}}')
    assert r.returncode != 0 and "inputs in sync" not in r.stdout
    assert ("FAILED feeds/inseason/matches.json: inseason-links/3, but the deployed worker reads inseason-links/2: "
            "not put") in r.stderr
    log = (sync_tree / "rclone.log").read_text().splitlines()
    assert len(log) == 1 and "dfo/a.json" in log[0]


def test_sync_inputs_puts_a_format_the_deployed_worker_reads(sync_tree):
    r = _formatted(sync_tree, "inseason-links/3", '{"inputs": {"feeds/inseason/matches.json": ["inseason-links/3"]}}')
    assert r.returncode == 0, r.stderr
    assert len((sync_tree / "rclone.log").read_text().splitlines()) == 2


def test_sync_inputs_refuses_a_format_when_the_worker_cannot_be_asked(sync_tree):
    for served in (None, '{"feeds": {}}'):            # unreachable; a worker from before the list
        r = _formatted(sync_tree, "inseason-links/3", served)
        assert r.returncode != 0
        assert "does not say which input formats the worker reads" in r.stderr
        assert "matches.json" not in (sync_tree / "rclone.log").read_text()
        (sync_tree / "rclone.log").unlink()


# ── T597: the PRODUCTION twin of the format check (run against stubs in a temp tree: nothing here reaches a bucket) ────

PROD_URL = "https://feeds.canifishthis.ca"
PLACEHOLDER = "feeds (production) is not live yet"


@pytest.fixture
def prod_tree(tmp_path):
    for rel in ("deploy/prod/sync-inputs.sh", "deploy/lib/input-formats.sh"):
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(ROOT / rel, tmp_path / rel)
    return tmp_path


def _prod(tree: Path, fmt: str | None, served: str | None, *args: str, **env: str) -> subprocess.CompletedProcess:
    """deploy/prod/sync-inputs.sh on stubs: the staged matches name `fmt` (None: no format); the worker's /status.json
    answers `served` (None: no HTTP answer). The curl stub logs the URL it was asked; rclone logs every copy."""
    old = """'{"notices": {"a": 1, "b": 2}}'"""
    assert old in PY_STUB
    _exe(tree / ".venv/bin/python", PY_STUB if fmt is None else
         PY_STUB.replace(old, f"""'{{"format": "{fmt}", "notices": {{"a": 1}}}}'"""))
    _exe(tree / "bin/curl", f'#!/usr/bin/env bash\nfor a in "$@"; do case $a in http*) echo "$a" >> "{tree}/curl.log";; esac; done\n'
         + ("exit 7\n" if served is None else f"echo '{served}'\n"))
    _exe(tree / "bin/rclone", f'#!/usr/bin/env bash\necho "$*" >> "{tree}/rclone.log"\necho "INFO  : x: Copied (new)" >&2\n')
    for f in ("rclone.log", "curl.log"):
        (tree / f).unlink(missing_ok=True)
    full = {k: v for k, v in os.environ.items() if k not in ("CONFIRM_PROD", "FEEDS_URL", "FEEDS_WORKER_NOT_LIVE")}
    full.update(PATH=f"{tree / 'bin'}:{os.environ['PATH']}", **env)
    return subprocess.run(["bash", str(tree / "deploy/prod/sync-inputs.sh"), *args], env=full, capture_output=True, text=True)


def _log(tree: Path, name: str) -> list[str]:
    p = tree / name
    return p.read_text().splitlines() if p.exists() else []


LISTS_2 = '{"inputs": {"feeds/inseason/matches.json": ["inseason-links/2"]}}'
LISTS_3 = '{"inputs": {"feeds/inseason/matches.json": ["inseason-links/3"]}}'


def test_prod_sync_refuses_a_format_the_production_worker_does_not_read(prod_tree):
    """The staging failure of 2026-11-09, on production: refused with a plain message, not put; the input with no format
    still goes up; exit 1. The PRODUCTION worker is the one asked, whatever FEEDS_URL the shell carries."""
    r = _prod(prod_tree, "inseason-links/3", LISTS_2, CONFIRM_PROD="canifishthis.ca",
              FEEDS_URL="https://bc-fishing-feeds-staging.horvath-dawson.workers.dev")
    assert r.returncode == 1 and "inputs in sync" not in r.stdout
    assert ("FAILED feeds/inseason/matches.json: inseason-links/3, but the deployed worker reads inseason-links/2: not put. "
            "Deploy the worker that reads inseason-links/3 first (CONFIRM_PROD=bc-fishing-feeds deploy/prod/feeds.sh), "
            "then sync again") in r.stderr
    assert "sync-inputs (prod): 1 object(s) put; these inputs were NOT:" in r.stderr
    assert _log(prod_tree, "curl.log") == [f"{PROD_URL}/status.json"]
    log = _log(prod_tree, "rclone.log")
    assert len(log) == 1 and "r2:bc-fishing-regulations/dfo/a.json" in log[0]


def test_prod_sync_puts_a_format_the_production_worker_reads(prod_tree):
    r = _prod(prod_tree, "inseason-links/3", LISTS_3, CONFIRM_PROD="canifishthis.ca")
    assert r.returncode == 0, r.stderr
    assert "inputs in sync: r2:bc-fishing-regulations (2 objects, in-season matches 1)" in r.stdout
    assert len(_log(prod_tree, "rclone.log")) == 2


def test_prod_sync_refuses_a_format_when_the_worker_cannot_be_asked(prod_tree):
    """No answer, the placeholder, or a worker from before the list: every formatted input is refused — and the claim
    FEEDS_WORKER_NOT_LIVE is what it takes to seed before the first deploy (next test), never the default."""
    for served in (None, PLACEHOLDER, '{"feeds": {}}'):
        r = _prod(prod_tree, "inseason-links/3", served, CONFIRM_PROD="canifishthis.ca")
        assert r.returncode == 1, served
        assert "does not say which input formats the worker reads" in r.stderr
        assert not any("matches.json" in ln for ln in _log(prod_tree, "rclone.log"))


def test_prod_first_seed_before_any_worker_is_an_explicit_claim_that_a_live_worker_overrules(prod_tree):
    # no answer / the placeholder + the claim: put, and said
    for served, how in ((None, "no answer"), (PLACEHOLDER, "the placeholder")):
        r = _prod(prod_tree, "inseason-links/3", served, CONFIRM_PROD="canifishthis.ca", FEEDS_WORKER_NOT_LIVE="1")
        assert r.returncode == 0, r.stderr
        assert f"input formats: NOT CHECKED — no feeds worker reads this bucket yet ({PROD_URL}: {how})" in r.stderr
        assert len(_log(prod_tree, "rclone.log")) == 2
    # a real worker answers: its list decides, the claim does nothing
    r = _prod(prod_tree, "inseason-links/3", LISTS_2, CONFIRM_PROD="canifishthis.ca", FEEDS_WORKER_NOT_LIVE="1")
    assert r.returncode == 1 and "but the deployed worker reads inseason-links/2" in r.stderr
    # a worker from before the list answers (not the placeholder): refused, and the claim is said to be wrong
    r = _prod(prod_tree, "inseason-links/3", '{"feeds": {}}', CONFIRM_PROD="canifishthis.ca", FEEDS_WORKER_NOT_LIVE="1")
    assert r.returncode == 1 and "a worker is deployed there, so its list decides" in r.stderr
    assert not any("matches.json" in ln for ln in _log(prod_tree, "rclone.log"))


def test_the_placeholder_line_the_check_knows_is_the_placeholder_workers(prod_tree):
    assert PLACEHOLDER in (ROOT / "deploy/prod/feeds-placeholder/worker.js").read_text()
    assert f'PLACEHOLDER_SAYS="{PLACEHOLDER}"' in (ROOT / "deploy/lib/input-formats.sh").read_text()


def test_prod_sync_still_needs_its_confirmation_and_asks_nothing_without_it(prod_tree):
    r = _prod(prod_tree, "inseason-links/3", LISTS_3)
    assert r.returncode == 2 and "set CONFIRM_PROD=canifishthis.ca" in r.stderr
    assert _log(prod_tree, "curl.log") == [] and _log(prod_tree, "rclone.log") == []


def test_prod_rehearsal_asks_the_worker_it_names_and_refuses_formats_when_it_names_none(prod_tree):
    r = _prod(prod_tree, "inseason-links/3", LISTS_3, "--bucket", "scratch", "--dry-run")
    assert r.returncode == 1
    assert "FAILED feeds/inseason/matches.json: no feeds worker was named for this bucket" in r.stderr
    assert _log(prod_tree, "curl.log") == []
    log = _log(prod_tree, "rclone.log")
    assert len(log) == 1 and "r2:scratch/dfo/a.json" in log[0] and "--dry-run" in log[0]
    r = _prod(prod_tree, "inseason-links/3", LISTS_3, "--bucket", "scratch", "--dry-run", FEEDS_URL="https://w.example")
    assert r.returncode == 0, r.stderr
    assert _log(prod_tree, "curl.log") == ["https://w.example/status.json"]
    assert "inputs in sync (dry run): r2:scratch (2 objects" in r.stdout


def test_prod_sync_does_not_ask_the_worker_when_no_input_names_a_format(prod_tree):
    r = _prod(prod_tree, None, None, CONFIRM_PROD="canifishthis.ca")
    assert r.returncode == 0, r.stderr
    assert _log(prod_tree, "curl.log") == [] and len(_log(prod_tree, "rclone.log")) == 2


def test_both_uploaders_run_the_one_format_check():
    """One copy (deploy/lib/input-formats.sh), sourced by both: the twins cannot drift."""
    for rel in ("deploy/staging/sync-inputs.sh", "deploy/prod/sync-inputs.sh"):
        body = "\n".join(ln for ln in (ROOT / rel).read_text().splitlines() if not ln.lstrip().startswith("#"))
        assert "source deploy/lib/input-formats.sh" in body and 'input_formats_filter "$T/manifest.tsv" "$T/out" "$T/put.tsv"' in body, rel
        assert "status.json" not in body, rel                       # no second copy of the question
    staging = (ROOT / "deploy/staging/sync-inputs.sh").read_text()
    assert "FEEDS_WORKER_NOT_LIVE" not in staging and "\nNO_WORKER_YET=\n" in staging   # staging never claims it
