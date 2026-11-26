"""T433 (audit 2026-11-05): the staging scripts fail safe.

  upload        the bucket is cleared AFTER the pointer flip, sparing what the new release names (a failure above it
                once left staging with no manifest)
  sync-inputs   a failed rclone copy stops the script (run, against a stub rclone); it once printed "in sync"
  hydat         fetch_data's hydat layer downloads through the checked scripts/hydat-fetch.sh and raises on an
                unreadable listing
  check-public  a failed secret read is not swallowed (a secret it cannot read is one it cannot look for)
  testflight    --production needs CONFIRM_PROD, before the heavy queue (run); (T894) the version files go to the
                bucket of the build's environment (staging by default, production only with --production, run as a
                dry run), one per version, and a failed put stops with the redo hint
  play          (T894) a Play build's own app/android/play/<versionCode>.json goes up before play.json, both stopping on a
                failed put
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


def _pin() -> dict[str, str]:
    """app/tools/maplibre-android/pin.env (T978): MLN_COMMIT, MLN_VERSION."""
    return dict(line.split("=", 1) for line in (ROOT / "app/tools/maplibre-android/pin.env").read_text().splitlines()
                if line and not line.startswith("#"))


def _pinned_repo(parent: Path, so: str = "x", commit: str | None = None) -> Path:
    """A stand-in of build.sh's output under `parent`: <parent>/<MLN_VERSION>/ with the AAR (an arm64 libmaplibre.so
    holding `so`) and SOURCE.txt naming `commit` (default: the pin's). Returns the repo folder."""
    import zipfile
    p = _pin()
    repo = parent / p["MLN_VERSION"]
    aar = repo / f"org/maplibre/gl/android-sdk-vulkan/{p['MLN_VERSION']}/android-sdk-vulkan-{p['MLN_VERSION']}.aar"
    aar.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(aar, "w") as z:
        z.writestr("jni/arm64-v8a/libmaplibre.so", so)
    (repo / "SOURCE.txt").write_text(f"maplibre-native {commit or p['MLN_COMMIT']} (upstream, no patch), version "
                                     f"{p['MLN_VERSION']}, built 2026-11-22T00:00:00Z from /src\n")
    return repo


def test_upload_clears_after_the_pointer_and_spares_the_release():
    lines = (ROOT / "deploy/staging/upload.sh").read_text().splitlines()
    run = [ln for ln in lines if not ln.lstrip().startswith("#")]
    pointer = next(i for i, ln in enumerate(run) if 'cp_ "$M" release/manifest.json' in ln)
    clear = [i for i, ln in enumerate(run) if ln.startswith("rclone delete")]
    assert clear and all(i > pointer for i in clear), "the clear must come after the pointer flip"
    cmd = " ".join(run[clear[0]:clear[0] + 3])
    for spared in ('"release/manifest.json"', '"release/tiles/$TILES/**"', '"release/$REL/**"', '"release/$BM"',
                   '"release/$SYN"', '"feeds/**"', '"private/**"', '"runtiming/**"', '"test/**"', '"app/android/**"',
                   '"app/ios/**"'):   # (T894 review) the iPhone's version files survive a staging release
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
    (tmp_path / "data/generated/inseason").mkdir(parents=True)        # (T679 #11) the generated resolution
    (tmp_path / "data/generated/inseason/resolved.json").write_text('{"release": "r-1"}')
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
    """(T537) matches on another release than LATEST are refused with the resolve command; nothing is put and nothing
    is re-resolved here (`pipeline release` resolves them on the candidate before its promote). (T679 #11) the
    resolution is generated: no commit is asked for, and a missing one is refused the same way."""
    (sync_tree / "data/release/LATEST").write_text("r-2\n")
    _exe(sync_tree / "scripts/heavy.sh", f'#!/usr/bin/env bash\necho ran >> "{sync_tree}/heavy.log"\n')
    r = _sync(sync_tree, 0)
    assert r.returncode != 0 and "LATEST is r-2 — nothing was put" in r.stderr
    assert "pipeline.regs.inseason.resolve --release data/release/r-2" in r.stderr and "git commit" not in r.stderr
    assert not (sync_tree / "rclone.log").exists() and not (sync_tree / "heavy.log").exists()
    (sync_tree / "data/generated/inseason/resolved.json").unlink()
    r = _sync(sync_tree, 0)
    assert r.returncode != 0 and "resolved on nothing (no data/generated/inseason/resolved.json)" in r.stderr
    assert not (sync_tree / "rclone.log").exists()


def test_sync_inputs_from_the_review_app_never_rewrites_the_matches(sync_tree):
    """(T520) from the review app, matches on another release are REFUSED (re-resolve there; T679 #11: no commit) — the
    curated file is never rewritten outside its writes.commit."""
    (sync_tree / "data/release/LATEST").write_text("r-2\n")
    _exe(sync_tree / "scripts/heavy.sh", f'#!/usr/bin/env bash\necho ran >> "{sync_tree}/heavy.log"\n')
    env_tree = sync_tree
    r = subprocess.run(["bash", str(env_tree / "deploy/staging/sync-inputs.sh")], capture_output=True, text=True,
                       env={**os.environ, "SYNC_INPUTS_FROM_REVIEW": "1",
                            "PATH": f"{env_tree / 'bin'}:{os.environ['PATH']}"})
    assert r.returncode != 0 and "re-resolve in the review app" in r.stderr and "commit" not in r.stderr
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


def _no_comments(rel: str) -> str:
    return "\n".join(ln for ln in (ROOT / rel).read_text().splitlines() if not ln.lstrip().startswith("#"))


def test_testflight_dry_run_puts_the_version_files_in_its_environments_bucket():
    """(T894) staging by default; bc-fishing-regulations only with --production (and its confirmation)."""
    import json
    ver = json.loads((ROOT / "app/apps/mobile/app.json").read_text())["expo"]["version"]
    env = {k: v for k, v in os.environ.items() if k not in ("CONFIRM_PROD", "BUILD_IOS_INNER")}
    script = str(ROOT / "deploy/staging/build-ios-testflight.sh")
    # (T993 plan 5) the dry run asks CI too: told not to here (no network in a test)
    r = subprocess.run(["bash", script, "--dry-run", "--no-ci-check=a test"], env=env, capture_output=True, text=True,
                       timeout=60)
    assert r.returncode == 0, r.stderr
    put = next(ln for ln in r.stdout.splitlines() if "after the upload: put" in ln)
    assert put.startswith(f"[dry-run] after the upload: put bc-fishing-regulations-staging/app/ios/{ver}.json, then "
                          "bc-fishing-regulations-staging/app/ios/appstore.json, with notes ["), put
    assert "bc-fishing-regulations/" not in r.stdout
    r = subprocess.run(["bash", script, "--production", "--dry-run", "--no-ci-check=a test"],
                       env={**env, "CONFIRM_PROD": "canifishthis.ca"}, capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stderr
    put = next(ln for ln in r.stdout.splitlines() if "after the upload: put" in ln)
    assert put.startswith(f"[dry-run] after the upload: put bc-fishing-regulations/app/ios/{ver}.json, then "
                          "bc-fishing-regulations/app/ios/appstore.json, with notes ["), put
    assert "bc-fishing-regulations-staging" not in r.stdout


def test_testflight_publishes_one_file_per_version_and_a_failed_put_stops():
    code = _no_comments("deploy/staging/build-ios-testflight.sh")
    loop = code[code.index('for f in "$VER.json" appstore.json; do'):code.index("done", code.index('for f in "$VER.json"'))]
    assert 'put --remote "$BUCKET/app/ios/$f" --file "$OUT/appstore.json"' in loop
    assert "was NOT published (the build IS uploaded): redo the puts in the header" in loop
    assert "testflight-distribute.mjs" in loop                     # T918: the redo hint names the distribute step too
    assert '\" >&2; exit 1; }' in loop                             # ... and a failed put still stops the script
    assert "app/ios/appstore.json\" --file" not in code           # appstore.json only through the loop, after <ver>.json


def test_play_build_publishes_its_own_versioncode_file_before_play_json():
    code = _no_comments("deploy/staging/build-apk.sh")
    own = code.index('put --remote "$BUCKET/app/android/play/$CODE.json" --file "$OUT/play.json"')
    last = code.index('put --remote "$BUCKET/app/android/play.json" --file "$OUT/play.json"')
    assert own < last
    for at in (own, last):
        tail = code[at:code.index("\n", code.index("|| {", at))]
        assert "the build IS on Play): fix wrangler, then build-apk.sh --play --publish-only\" >&2; exit 1; }" in tail
    # --publish-only redoes both, the build's own file first
    assert code.index('put --remote "$BUCKET/app/android/play/$LOCALB.json" --file "$PJ"') \
        < code.index('put --remote "$BUCKET/app/android/play.json" --file "$PJ"')


@pytest.mark.parametrize("mode", [[], ["--play"], ["--play", "--production"]])
def test_no_apk_build_takes_the_stock_maplibre(mode):
    """(T893, owner 2026-11-19: "It has to use patched one"; T978 review, the owner's rule "every Android build uses the
    pinned build") build-apk.sh refuses --stock-maplibre in every mode — the staging page, Play and production all reach
    a phone — and names the test bed's escape; a bare flag too."""
    env = {k: v for k, v in os.environ.items() if k not in ("CONFIRM_PROD", "MLN_LOCAL_MAVEN")}
    for flag in ("--stock-maplibre=a bisect", "--stock-maplibre"):
        r = subprocess.run(["bash", str(ROOT / "deploy/staging/build-apk.sh"), *mode, "--dry-run", flag], env=env,
                           capture_output=True, text=True, timeout=30, cwd=ROOT)
        assert r.returncode == 2, (mode, flag, r.stdout + r.stderr)
        assert "build-apk: --stock-maplibre is refused: every build here ships the pinned upstream MapLibre" in r.stderr
        assert "app/tools/testbed/session.sh --stock-maplibre=" in r.stderr
    assert "STOCK_MLN" not in _no_comments("deploy/staging/build-apk.sh")   # no stock path left to reach


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
    for probe in ("build_check_changelog", "upload_certificate.pem", "--max-version-code", "whoami",
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
    for rel in ("deploy/prod/sync-inputs.sh", "deploy/lib/input-formats.sh", "deploy/lib/bucket.sh"):
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


def test_publish_only_refuses_a_build_of_the_other_environment():
    """(T894 review) a staging build's play.json never goes to the production bucket by --publish-only, nor the reverse:
    the build writes its environment into the file, and --publish-only refuses a file of another (or no) environment
    before any read of the published file or any put."""
    sh = (ROOT / "deploy/staging/build-apk.sh").read_text()
    assert 'v["env"]=sys.argv[3]' in sh and '"$MLN_GOT" "$ENVWORD" > "$OUT/play.json"' in sh
    block = sh[sh.index("if [[ -n $PUBLISH_ONLY ]]; then"):]
    block = block[:block.index("\nfi\n")]
    refuse = block.index('[[ $PJENV == "$ENVWORD" ]] || {')
    assert refuse < block.index('PUBB=$(curl') < block.index("r2 object put")


def test_build_apk_lists_a_missing_maplibre_repo_and_never_exits_silently(tmp_path):
    """(T978, 0.8.4 production build from a worktree) no cache/ made the AAR `find` fail and `set -e` end build-apk.sh
    with NO message. Now the precheck lists it with its fix beside every other failure and stops before the lock."""
    env = {k: v for k, v in os.environ.items() if k not in ("CONFIRM_PROD", "BUILD_APK_INNER", "MLN_LOCAL_MAVEN")}
    gone = tmp_path / "no-such-maven"
    r = subprocess.run(["bash", str(ROOT / "deploy/staging/build-apk.sh"), "--precheck", "--no-upload"],
                       env={**env, "MLN_LOCAL_MAVEN": str(gone)}, capture_output=True, text=True, timeout=60, cwd=ROOT)
    assert r.returncode == 1, r.stdout + r.stderr
    assert f"FAIL no pinned MapLibre repo at {gone}" in r.stdout and "maplibre-android/build.sh" in r.stdout
    assert "precheck FAILED" in r.stderr and "nothing was built" in r.stderr
    # --check-maplibre with no AAR to compare is refused, never read as "stock"
    r = subprocess.run(["bash", str(ROOT / "deploy/staging/build-apk.sh"), f"--check-maplibre={tmp_path / 'x.apk'}"],
                       env={**env, "MLN_LOCAL_MAVEN": str(gone)}, capture_output=True, text=True, timeout=60, cwd=ROOT)
    assert r.returncode == 1 and "no pinned MapLibre AAR" in r.stderr


def test_check_maplibre_proves_the_archive_against_the_pinned_aar(tmp_path):
    """(T512, T978 review 2) the build-id proof itself, run: an archive carrying the pinned AAR's libmaplibre.so passes and
    names the pin; another build id (the stock SDK slipping in: Play builds 20, 21) is refused; an AAR with the pin's
    SOURCE.txt but another file name is no pinned AAR; and the inner run stops when the AAR went away after the precheck.
    Each was a guard no test ran (a mutation that made it always pass went green)."""
    import zipfile
    env = {k: v for k, v in os.environ.items() if k not in ("CONFIRM_PROD", "BUILD_APK_INNER", "MLN_LOCAL_MAVEN")}
    sdk = tmp_path / "sdk"
    # llvm-readelf stand-in: the "build id" is the .so's text (mln_id runs `llvm-readelf -n <so>`)
    _exe(sdk / "ndk/0/toolchains/llvm/prebuilt/darwin-x86_64/bin/llvm-readelf",
         '#!/usr/bin/env bash\necho "    Build ID: $(cat "$2")"\n')
    repo = _pinned_repo(tmp_path / "m2", "beefcafe")
    ver = _pin()["MLN_VERSION"]

    def apk(name: str, so: str) -> Path:
        p = tmp_path / name
        with zipfile.ZipFile(p, "w") as z:
            z.writestr("lib/arm64-v8a/libmaplibre.so", so)
        return p

    def check(archive: Path, mvn: Path) -> subprocess.CompletedProcess:
        return subprocess.run(["bash", str(ROOT / "deploy/staging/build-apk.sh"), f"--check-maplibre={archive}"],
                              env={**env, "ANDROID_HOME": str(sdk), "MLN_LOCAL_MAVEN": str(mvn)}, capture_output=True,
                              text=True, timeout=60, cwd=ROOT)

    r = check(apk("good.apk", "beefcafe"), repo)
    assert r.returncode == 0, r.stdout + r.stderr
    assert f"libmaplibre.so build id beefcafe (pinned upstream {ver})" in r.stdout
    r = check(apk("stock.apk", "aab47813"), repo)
    assert r.returncode == 1, r.stdout + r.stderr
    assert "stock.apk's libmaplibre.so is build id 'aab47813', not the pinned AAR's 'beefcafe'" in r.stderr
    # the pin's SOURCE.txt beside an AAR of another name: not the pinned build (mln_pinned_aar's file-name half)
    aar = next(repo.rglob("*.aar"))
    aar.rename(aar.with_name("android-sdk-vulkan-13.7.1-exp.aar"))
    r = check(apk("good2.apk", "beefcafe"), repo)
    assert r.returncode == 1 and "no pinned MapLibre AAR" in r.stderr, r.stdout + r.stderr
    # the inner run (after the precheck, under the lock) stops before anything when the pinned AAR is gone
    r = subprocess.run(["bash", str(ROOT / "deploy/staging/build-apk.sh"), "--dry-run", "--no-upload"],
                       env={**env, "BUILD_APK_INNER": "1", "MLN_LOCAL_MAVEN": str(tmp_path / "gone")},
                       capture_output=True, text=True, timeout=60, cwd=ROOT)
    assert r.returncode == 1, r.stdout + r.stderr
    assert "no pinned MapLibre AAR in MLN_LOCAL_MAVEN=" in r.stderr and "nothing was built" in r.stderr


def test_build_apk_never_ships_an_unpinned_maplibre(tmp_path):
    """(T978, owner 2026-11-22: "Pinned upstream main (Recommended)") an AAR that is not pin.env's version — the old lean
    patch's, an experiment — is never proved against or shipped: the precheck lists it, --check-maplibre refuses."""
    env = {k: v for k, v in os.environ.items() if k not in ("CONFIRM_PROD", "BUILD_APK_INNER", "MLN_LOCAL_MAVEN")}
    lean = tmp_path / "m2/org/maplibre/gl/android-sdk-vulkan/13.6.1-fslean/android-sdk-vulkan-13.6.1-fslean.aar"
    lean.parent.mkdir(parents=True)
    lean.write_bytes(b"")
    r = subprocess.run(["bash", str(ROOT / "deploy/staging/build-apk.sh"), "--precheck", "--no-upload"],
                       env={**env, "MLN_LOCAL_MAVEN": str(tmp_path / "m2")}, capture_output=True, text=True, timeout=60,
                       cwd=ROOT)
    assert r.returncode == 1, r.stdout + r.stderr
    assert f"FAIL {lean} is not the pinned MapLibre" in r.stdout and "pinned upstream MapLibre" not in r.stdout
    r = subprocess.run(["bash", str(ROOT / "deploy/staging/build-apk.sh"), f"--check-maplibre={tmp_path / 'x.apk'}"],
                       env={**env, "MLN_LOCAL_MAVEN": str(tmp_path / "m2")}, capture_output=True, text=True, timeout=60,
                       cwd=ROOT)
    assert r.returncode == 1 and "no pinned MapLibre AAR" in r.stderr
    # the default repo is the pinned one, named from pin.env (no MLN_LOCAL_MAVEN)
    r = subprocess.run(["bash", str(ROOT / "deploy/staging/build-apk.sh"), "--precheck", "--no-upload"],
                       env={**env, "HOME": str(tmp_path)}, capture_output=True, text=True, timeout=60, cwd=ROOT)
    ver = next(line.split("=", 1)[1] for line in (ROOT / "app/tools/maplibre-android/pin.env").read_text().splitlines()
               if line.startswith("MLN_VERSION="))
    assert f"cache/maplibre/{ver}" in r.stdout, r.stdout


def _links(cwd: Path, body: str) -> subprocess.CompletedProcess:
    """deploy/lib/build-links.sh sourced in `cwd`, with a bad/ok pair that prints, then `body`."""
    script = (f"set -u; bad() {{ echo \"FAIL $*\"; }}; ok() {{ echo \"ok $*\"; }}; "
              f"source {ROOT / 'deploy/lib/build-links.sh'}; {body}")
    return subprocess.run(["bash", "-c", script], cwd=cwd, capture_output=True, text=True, timeout=30)


def test_build_links_fix_a_worktree_by_linking_the_main_checkout(tmp_path):
    """(T978) every missing link is said with its exact fix: from a worktree, a link (or a copy, for a file) of the main
    checkout's own; in the main checkout, the command that makes it."""
    main = tmp_path / "main"
    main.mkdir()
    git = ["git", "-c", "user.email=t@t", "-c", "user.name=t"]
    subprocess.run([*git, "init", "-q"], cwd=main, check=True)
    (main / "f").write_text("x")
    subprocess.run([*git, "add", "f"], cwd=main, check=True)
    subprocess.run([*git, "commit", "-qm", "one"], cwd=main, check=True)
    subprocess.run([*git, "worktree", "add", "-q", "--detach", str(tmp_path / "wt")], cwd=main, check=True)
    pin = dict(line.split("=", 1) for line in (ROOT / "app/tools/maplibre-android/pin.env").read_text().splitlines()
               if line and not line.startswith("#"))
    ver, sha9 = pin["MLN_VERSION"], pin["MLN_COMMIT"][:9]
    repo = f"cache/maplibre/{ver}"
    (main / ".venv").mkdir()
    (main / repo).mkdir(parents=True)
    (main / "app/workers/feeds").mkdir(parents=True)
    (main / "app/workers/feeds/.run-token").write_text("t")
    wt, m = (tmp_path / "wt").resolve(), main.resolve()
    got = _links(wt, f"build_links_venv; build_links_node; build_links_mln {wt}/{repo}; "
                     "build_links_file app/workers/feeds/.run-token 'the email' rotate.sh '--no-email goes on'")
    assert got.returncode == 0, got.stderr
    assert got.stdout.splitlines() == [
        f"FAIL no runnable .venv (fast-fail, the CI status, the Play API read it): ln -s {m}/.venv .venv",
        "FAIL no app/node_modules (fast-fail's app gates, wrangler): (cd app && pnpm install --offline --frozen-lockfile)",
        f"FAIL no pinned MapLibre repo at {wt}/{repo} (T978, upstream {sha9}): mkdir -p cache/maplibre && "
        f"ln -s {m}/{repo} {repo}",
        f"FAIL no app/workers/feeds/.run-token (the email): mkdir -p app/workers/feeds && cp "
        f"{m}/app/workers/feeds/.run-token app/workers/feeds/.run-token; or --no-email goes on"], got.stdout
    # the main checkout itself: the command that makes each, never a link to itself
    (main / "app/workers/feeds/.run-token").unlink()
    got = _links(m, "build_links_venv; build_links_file app/workers/feeds/.run-token 'the email' rotate.sh")
    assert "scripts/bootstrap.sh" in got.stdout and "FAIL no app/workers/feeds/.run-token (the email): rotate.sh" in got.stdout
    assert "ln -s" not in got.stdout
    # with no repo at all in the main checkout: the build command, pinned (no patch named)
    got = _links(m, f"build_links_mln {m}/cache/maplibre/nowhere")
    assert "scripts/heavy.sh mln-aar 30 -- bash app/tools/maplibre-android/build.sh <work dir>" in got.stdout
    assert "patch" not in got.stdout.replace("no patch", "")
    # (T978) the pinned AAR: ok, MLN_AAR names it; an AAR of another version (the old lean one): refused; two: refused
    aar = main / f"{repo}/org/maplibre/gl/android-sdk-vulkan/{ver}/android-sdk-vulkan-{ver}.aar"
    aar.parent.mkdir(parents=True)
    aar.write_text("a")
    # (T978 review) the right file name is not enough: build.sh's SOURCE.txt must name the pinned commit
    got = _links(m, f"build_links_mln {m}/{repo}; echo \"AAR=[$MLN_AAR]\"")
    assert f"{m}/{repo}/SOURCE.txt names no commit, not the pinned {pin['MLN_COMMIT']}" in got.stdout, got.stdout
    assert "AAR=[]" in got.stdout
    (main / repo / "SOURCE.txt").write_text(f"maplibre-native {'0' * 40} (upstream, no patch), version {ver}\n")
    got = _links(m, f"build_links_mln {m}/{repo}; echo \"AAR=[$MLN_AAR]\"")
    assert f"SOURCE.txt names {'0' * 40}, not the pinned" in got.stdout and "AAR=[]" in got.stdout
    (main / repo / "SOURCE.txt").write_text(f"maplibre-native {pin['MLN_COMMIT']} (upstream, no patch), version {ver}\n")
    got = _links(m, f"build_links_mln {m}/{repo}; echo \"AAR=$MLN_AAR\"")
    assert f"AAR={aar}" in got.stdout and "FAIL" not in got.stdout and f"upstream {sha9}, no patch" in got.stdout
    got = _links(m, f"echo \"[$(mln_pinned_aar {m}/{repo})]\"")   # the silent twin build-apk.sh uses: the same answer
    assert got.stdout.strip() == f"[{aar}]"
    # (T978) the fix it prints works: the worktree's LINK to the main checkout's repo is followed (a bare `find` did
    # not descend into a symlinked folder, so the advised link was still "found: none")
    (wt / "cache/maplibre").mkdir(parents=True)
    (wt / repo).symlink_to(m / repo)
    got = _links(wt, f"build_links_mln {wt}/{repo}; echo \"AAR=$MLN_AAR\"")
    assert f"AAR={wt}/{repo}/org/" in got.stdout and "FAIL" not in got.stdout, got.stdout
    lean = main / "cache/maplibre/fslean-m2/org/maplibre/gl/android-sdk-vulkan/13.6.1-fslean/android-sdk-vulkan-13.6.1-fslean.aar"
    lean.parent.mkdir(parents=True)
    lean.write_text("l")
    got = _links(m, f"build_links_mln {m}/cache/maplibre/fslean-m2; echo \"AAR=[$MLN_AAR]\"")
    assert f"is not the pinned MapLibre ({ver}" in got.stdout and "AAR=[]" in got.stdout
    (aar.parent / "android-sdk-vulkan-2.aar").write_text("b")
    got = _links(m, f"build_links_mln {m}/{repo}; echo \"AAR=[$MLN_AAR]\"")
    assert "must hold exactly one android-sdk-vulkan AAR" in got.stdout and "AAR=[]" in got.stdout


def test_build_links_frontend_fails_on_a_git_error_never_skips(tmp_path):
    """(T978 review, lane G3) a failed `git merge-base` / `git diff` once returned quietly, skipping the check: now each
    is a failed item naming its cause; a real frontend change with no node_modules asks for `npm ci`; none, nothing."""
    repo = tmp_path / "r"
    (repo / "curation-review/frontend").mkdir(parents=True)
    git = ["git", "-c", "user.email=t@t", "-c", "user.name=t"]
    subprocess.run([*git, "init", "-q"], cwd=repo, check=True)
    (repo / "f").write_text("x")
    subprocess.run([*git, "add", "f"], cwd=repo, check=True)
    subprocess.run([*git, "commit", "-qm", "one"], cwd=repo, check=True)
    one = subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True, check=True).stdout.strip()
    got = _links(repo, 'build_links_frontend ""')                   # no origin/main (the trunk, T995) here
    assert "FAIL cannot tell whether fast-fail typechecks the review app: git merge-base HEAD origin/main" in got.stdout, \
        got.stdout
    got = _links(repo, "build_links_frontend deadbeefdeadbeef")
    assert "FAIL cannot tell whether fast-fail typechecks the review app: git diff --name-only deadbeef" in got.stdout
    assert _links(repo, f"build_links_frontend {one}").stdout == ""   # nothing of the frontend changed
    (repo / "curation-review/frontend/a.ts").write_text("x")
    subprocess.run([*git, "add", "."], cwd=repo, check=True)
    subprocess.run([*git, "commit", "-qm", "two"], cwd=repo, check=True)
    got = _links(repo, f"build_links_frontend {one}")
    assert "FAIL no curation-review/frontend/node_modules" in got.stdout and "npm ci" in got.stdout


def test_testflight_precheck_lists_every_missing_secret_before_the_lock(tmp_path):
    """(T978) the first missing secret ended the TestFlight build after the lock wait; now every one is listed at once,
    before `exec scripts/heavy.sh`."""
    env = {k: v for k, v in os.environ.items() if k not in ("CONFIRM_PROD", "BUILD_IOS_INNER")}
    # stand-ins for the tools (a CI runner has no Xcode, CocoaPods or Node): the two secrets are missing here, and (T993
    # plan 5) on a runner with no .venv / app/node_modules those links too — every failure is counted, whichever
    for tool in ("xcodebuild", "pod", "node"):
        _exe(tmp_path / "bin" / tool, "#!/usr/bin/env bash\nexit 0\n")
    env["PATH"] = f"{tmp_path / 'bin'}{os.pathsep}{env.get('PATH', '')}"
    script = str(ROOT / "deploy/staging/build-ios-testflight.sh")
    r = subprocess.run(["bash", script, "--precheck", "--no-upload"],
                       env={**env, "HOME": str(tmp_path)}, capture_output=True, text=True, timeout=60, cwd=ROOT)
    assert r.returncode == 1, r.stdout + r.stderr
    assert f"FAIL no {tmp_path}/.canifishthis/ios-team-id" in r.stdout
    fails = r.stdout.count("  FAIL ")
    assert f"FAIL no {tmp_path}/.canifishthis/asc-api-key.json" in r.stdout and f"precheck FAILED ({fails})" in r.stderr
    # ... and a missing tool is listed beside them, not instead of them
    (tmp_path / "bin" / "pod").unlink()
    r = subprocess.run(["bash", script, "--precheck", "--no-upload"], env={**env, "HOME": str(tmp_path),
                       "PATH": f"{tmp_path / 'bin'}{os.pathsep}/usr/bin{os.pathsep}/bin"},
                       capture_output=True, text=True, timeout=60, cwd=ROOT)
    assert "FAIL no pod on PATH" in r.stdout and f"precheck FAILED ({fails + 1})" in r.stderr, r.stdout + r.stderr
    code = _no_comments("deploy/staging/build-ios-testflight.sh")
    assert code.index("precheck (before the heavy lock)") < code.index("exec scripts/heavy.sh build-ios-testflight")


# ── (T993 plan 5, T661 item 3) both store builds run the same gates before the heavy lock ──────────────────────────────
#: build -> (script, the flags that need no account, no Play, no MapLibre repo, no upload, where it records its last sha)
GATED = {"build-apk": ("deploy/staging/build-apk.sh", ["--no-upload"], "app/dist/apk"),
         "build-ios-testflight": ("deploy/staging/build-ios-testflight.sh", ["--no-upload"], "app/dist/ios")}


@pytest.fixture
def gate_tree(tmp_path):
    """A scratch repo holding the two build scripts and build-links.sh as they are, with every tool behind them a stub
    that logs to gates.log: CI (`.venv/bin/python scripts/ci_status.py`, rc CI_RC), fast-fail (its FAST_FAIL_BASE, rc
    FF_RC), heavy.sh (the lock: nothing past it runs), node (the changelog check, rc CL_RC), Xcode and CocoaPods."""
    t = tmp_path / "repo"
    for rel in ("deploy/staging/build-apk.sh", "deploy/staging/build-ios-testflight.sh", "deploy/lib/build-links.sh",
                "deploy/lib/gradle-watch.sh",
                "scripts/ci_paths.py", "app/tools/maplibre-android/pin.env"):
        (t / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / rel, t / rel)
    # (T978 review) build-apk has no stock escape any more: a stand-in of the pinned repo (an AAR holding an arm64
    # libmaplibre.so, build.sh's SOURCE.txt) and an llvm-readelf stub that names its build id
    _pinned_repo(t / "cache/maplibre", "beefcafe")
    _exe(tmp_path / "sdk/ndk/0/toolchains/llvm/prebuilt/darwin-x86_64/bin/llvm-readelf",
         '#!/usr/bin/env bash\necho "    Build ID: beefcafe"\n')
    log = tmp_path / "gates.log"
    _exe(t / "scripts/fast-fail.sh", f'#!/usr/bin/env bash\necho "fast-fail base=${{FAST_FAIL_BASE:-}}" >> "{log}"\n'
                                     'exit ${FF_RC:-0}\n')
    _exe(t / "scripts/heavy.sh", f'#!/usr/bin/env bash\necho "heavy $1" >> "{log}"\n')
    _exe(t / ".venv/bin/python", f'#!/usr/bin/env bash\n[[ $1 == -c ]] && exit 0\n'
                                 # (T1062) the release-candidate precheck: what it says (RC_SAID), rc RC_RC
                                 f'[[ $3 == release ]] && {{ echo "rc-check" >> "{log}"; echo "$RC_SAID"; '
                                 'exit ${RC_RC:-0}; }\n'
                                 f'echo "ci $*" >> "{log}"\nexit ${{CI_RC:-0}}\n')
    _exe(tmp_path / "bin/node", "#!/usr/bin/env bash\ncase $1 in\n"
         '  -p) if [[ $2 == *key_path* ]]; then echo "$HOME/key.p8"; else echo 9.9.9; fi ;;\n'
         '  scripts/check-changelog.mjs) [[ ${CL_RC:-0} == 1 ]] && { echo "changelog: 9.9.9 has no entry" >&2; exit 1; }\n'
         '    echo "changelog: 9.9.9 has 1 bullet(s) and is the newest entry" ;;\nesac\n')
    for tool in ("xcodebuild", "pod"):
        _exe(tmp_path / "bin" / tool, "#!/usr/bin/env bash\nexit 0\n")
    (t / "app/node_modules").mkdir(parents=True)
    (t / "app/node_modules/.modules.yaml").write_text("x")
    (t / "app/apps/mobile/android/app").mkdir(parents=True)
    (t / "app/apps/mobile/android/app/debug.keystore").write_text("k")
    (t / "app/apps/mobile/app.json").write_text('{"expo": {"version": "9.9.9"}}\n')
    (t / ".gitignore").write_text("/.venv\n/cache\napp/**/dist/\n")
    secrets = tmp_path / "home/.canifishthis"
    secrets.mkdir(parents=True)
    (secrets / "ios-team-id").write_text("TEAM")
    (secrets / "asc-api-key.json").write_text("{}")
    (tmp_path / "home/key.p8").write_text("p8")
    for d in ("jdk", "sdk"):
        (tmp_path / d).mkdir(exist_ok=True)
    git = ["git", "-c", "user.email=t@t", "-c", "user.name=t"]
    subprocess.run([*git, "init", "-q"], cwd=t, check=True)
    subprocess.run([*git, "add", "-A"], cwd=t, check=True)
    subprocess.run([*git, "commit", "-qm", "one"], cwd=t, check=True)
    subprocess.run(["git", "update-ref", "refs/remotes/origin/main", "HEAD"], cwd=t, check=True)
    env = {k: v for k, v in os.environ.items()
           if k not in ("CONFIRM_PROD", "BUILD_APK_INNER", "BUILD_IOS_INNER", "MLN_LOCAL_MAVEN", "FAST_FAIL_BASE",
                        "CI_CHECK_SKIP", "CANIFISHTHIS_R8", "CANIFISHTHIS_NO_R8", "HEAVY_HELD")}
    env.update(HOME=str(tmp_path / "home"), JAVA_HOME=str(tmp_path / "jdk"), ANDROID_HOME=str(tmp_path / "sdk"),
               PATH=f"{tmp_path / 'bin'}{os.pathsep}{env.get('PATH', '')}")

    def run(build: str, *extra: str, **over: str) -> tuple[subprocess.CompletedProcess, list[str]]:
        log.unlink(missing_ok=True)
        script, flags, _ = GATED[build]
        r = subprocess.run(["bash", str(t / script), *flags, *extra], env={**env, **over}, capture_output=True,
                           text=True, timeout=60, cwd=t)
        return r, (log.read_text().splitlines() if log.exists() else [])
    run.tree = t
    return run


@pytest.mark.parametrize("build", sorted(GATED))
def test_a_store_build_asks_ci_then_fast_fail_before_the_lock(gate_tree, build):
    """(T993 plan 5) the iPhone build ran no fast-fail, no CI check and no changelog check (build-apk ran all three), and
    five places said heavy.sh ran fast-fail for it: it never did (T661). Both builds now run the shared gates
    (deploy/lib/build-links.sh) in one order — precheck, CI, fast-fail, then the lock — and each stop is before it."""
    r, log = gate_tree(build)
    assert r.returncode == 0, r.stdout + r.stderr
    assert log == ["ci scripts/ci_status.py", "fast-fail base=", f"heavy {build}"], log
    assert "  ok   changelog: 9.9.9 has 1 bullet(s)" in r.stdout and "precheck ok" in r.stdout
    # a red fast-fail: nothing past it, said
    r, log = gate_tree(build, FF_RC="1")
    assert r.returncode == 1 and f"{build}: scripts/fast-fail.sh failed (above) — nothing was built" in r.stderr
    assert log == ["ci scripts/ci_status.py", "fast-fail base="], log
    # a red CI: not even fast-fail
    r, log = gate_tree(build, CI_RC="1")
    assert r.returncode == 1 and f"{build}: CI is red or unreadable (above) — nothing was built" in r.stderr
    assert log == ["ci scripts/ci_status.py"], log
    # no changelog entry: the precheck stops, before CI
    r, log = gate_tree(build, CL_RC="1")
    assert r.returncode == 1 and "FAIL changelog: 9.9.9 has no entry (--no-notes=<reason> goes on)" in r.stdout
    assert "precheck FAILED (1)" in r.stderr and log == [], log


@pytest.mark.parametrize("build", sorted(GATED))
def test_a_store_builds_escapes_print_their_reason_and_a_bare_one_is_refused(gate_tree, build):
    r, log = gate_tree(build, "--no-fast-fail=tsc is down", "--no-ci-check=GitHub is down", "--no-notes=a draft",
                       FF_RC="1", CI_RC="1", CL_RC="1")
    assert r.returncode == 0, r.stdout + r.stderr
    assert f"{build}: fast-fail SKIPPED (--no-fast-fail): tsc is down" in r.stdout
    assert f"{build}: CI check SKIPPED (--no-ci-check): GitHub is down" in r.stdout
    assert "changelog: SKIPPED (--no-notes): a draft" in r.stdout and log == [f"heavy {build}"], log
    for bare in ("--no-fast-fail", "--no-ci-check", "--no-notes", "--no-fast-fail= "):
        r, log = gate_tree(build, bare)
        assert r.returncode == 2 and "needs a reason" in r.stderr and log == [], (bare, r.stderr, log)


@pytest.mark.parametrize("build", sorted(GATED))
def test_a_store_build_on_a_release_branch_is_the_newest_release_candidate(gate_tree, build):
    """(T1062) on release/x.y.z the precheck asks `release --rc-check` (HEAD is the newest rc apart from the build
    numbers): a refusal is a precheck failure naming its command, `--no-rc-check=<why>` goes on and prints why, a bare
    one is refused; on any other branch it says none is asked. Mutation: drop build_check_rc from either script, or
    its `bad`: this fails."""
    r, log = gate_tree(build)
    assert r.returncode == 0 and "rc-check" not in log
    assert "  ok   release candidate: not asked on " in r.stdout and "(only on an app release branch, T1062)" in r.stdout
    subprocess.run(["git", "checkout", "-q", "-b", "release/9.9.9"], cwd=gate_tree.tree, check=True)
    said = "HEAD is v9.9.9-rc.2 (abc) apart from the build numbers"
    r, log = gate_tree(build, RC_SAID=said)
    assert r.returncode == 0 and log[0] == "rc-check" and f"  ok   release candidate: {said}" in r.stdout, r.stdout
    said = "HEAD is not v9.9.9-rc.2 (abc) apart from the build numbers (x.ts): tag a new rc first: python -m x"
    r, log = gate_tree(build, RC_SAID=said, RC_RC="1")
    assert r.returncode == 1 and f"  FAIL release candidate: {said} (--no-rc-check=<reason> goes on)" in r.stdout
    assert "precheck FAILED (1)" in r.stderr and log == ["rc-check"], log
    r, log = gate_tree(build, "--no-rc-check=the owner's hotfix", RC_SAID=said, RC_RC="1")
    assert r.returncode == 0 and f"release candidate: SKIPPED (--no-rc-check): the owner's hotfix — {said}" in r.stdout
    r, log = gate_tree(build, "--no-rc-check")
    assert r.returncode == 2 and "needs a reason" in r.stderr and log == []


def test_the_inner_build_refuses_a_head_other_than_the_prechecked_one(gate_tree):
    """(T1062 review) the precheck (the rc check) runs before the heavy lock, the build after it: HEAD may move while the
    lock is waited for. The precheck exports BUILD_PRECHECK_SHA and the inner run refuses any other HEAD, before a
    worktree is made; both scripts call it there. Mutation: drop the build_same_head call: this fails."""
    t = gate_tree.tree
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=t, capture_output=True, text=True, check=True).stdout.strip()
    for sha, want in (("0" * 40, f"HEAD is {head[:12]}, not 000000000000 the precheck checked"),
                      ("", "no BUILD_PRECHECK_SHA: the inner run was started by hand")):
        r, log = gate_tree("build-apk", BUILD_APK_INNER="1", BUILD_PRECHECK_SHA=sha)
        assert r.returncode == 1 and want in r.stderr and "building committed HEAD" not in r.stdout, r.stdout + r.stderr
    for script in ("deploy/staging/build-apk.sh", "deploy/staging/build-ios-testflight.sh"):
        code = _no_comments(script)
        at = code.index("SHA_FULL=$(git rev-parse HEAD)")
        assert at < code.index("build_same_head", at) < code.index("git worktree add", at), script
        assert "BUILD_PRECHECK_SHA=$(git rev-parse HEAD)" in code[:code.index("exec scripts/heavy.sh")], script


@pytest.mark.parametrize("build", sorted(GATED))
def test_a_store_builds_fast_fail_checks_what_changed_since_its_last_build(gate_tree, build):
    """(T533) fast-fail's "changed" = since the commit the last build of this kind was made from, said; (T993 plan 5)
    the iPhone build records its own (app/dist/ios/last-sha, written after an upload)."""
    t, dist = gate_tree.tree, GATED[build][2]
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=t, capture_output=True, text=True, check=True).stdout.strip()
    (t / dist).mkdir(parents=True)
    (t / dist / "last-sha").write_text(head + "\n")
    r, log = gate_tree(build)
    assert r.returncode == 0 and f"fast-fail base={head}" in log, (r.stdout + r.stderr, log)
    assert f"{build}: fast-fail base {head[:12]}" in r.stdout
    (t / dist / "last-sha").write_text("deadbeef" * 5 + "\n")      # not a commit here: the default, said
    r, log = gate_tree(build)
    assert "fast-fail base=" in log and "fast-fail base: none found for" in r.stdout, (r.stdout, log)
    if build == "build-ios-testflight":
        code = _no_comments(GATED[build][0])
        upload = code.index("if [[ $DEST == upload ]]; then")
        assert upload < code.index('echo "$SHA_FULL" > "$OUT/last-sha"') < code.index('else\n  echo "exported', upload)


def test_a_production_iphone_build_asks_ci_strictly_and_an_unsigned_one_skips_the_changelog(gate_tree):
    r, log = gate_tree("build-ios-testflight", "--production", CONFIRM_PROD="canifishthis.ca")
    assert r.returncode == 0 and log[0] == "ci scripts/ci_status.py --strict", (r.stdout + r.stderr, log)
    r, log = gate_tree("build-ios-testflight", "--unsigned", CL_RC="1")
    assert r.returncode == 0 and "changelog: not asked (--unsigned: nothing is uploaded)" in r.stdout, r.stdout
    assert log == ["ci scripts/ci_status.py", "fast-fail base=", "heavy build-ios-testflight"], log


def test_nothing_hands_heavy_a_fast_fail_word():
    """(T661, 2026-11-22) heavy.sh runs no fast-fail (the gate parked in T535 was dropped, not installed), so no script
    tells it fast-fail ran (FAST_FAIL_DONE) or why it was skipped (HEAVY_NO_FAST_FAIL): five places once did, and the
    iPhone build was taken to be gated by it."""
    hits = subprocess.run(["git", "grep", "-n", "-E", "FAST_FAIL_DONE|HEAVY_NO_FAST_FAIL", "--", "scripts", "deploy",
                           "pipeline/*.py", "app/tools", ".githooks", ":!pipeline/tests"], cwd=ROOT, capture_output=True,
                          text=True)
    assert hits.returncode == 1 and hits.stdout == "", hits.stdout + hits.stderr
    assert "scripts/fast-fail.sh" not in (ROOT / "scripts/heavy.sh").read_text()
