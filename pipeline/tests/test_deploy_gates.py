"""T993 lane G3: THE DEPLOY GATES SAY THEIR CAUSE AND FIRE WHEN THEY SHOULD (T788 #1 #4 #5, T994 #9 #10 #18 #19 #28).

Run against stubs in temp trees (rclone, curl, npx/wrangler stand-ins on PATH): nothing here reaches a bucket, a
worker or a site. Each test names the mutation it catches.

  formats       deploy/lib/input-formats.sh fails closed: a staged file jq cannot read is refused (it was put as
                "no format"); no jq stops the sync; a worker that cannot be asked is refused WITH its cause
  sync args     deploy/staging/sync-inputs.sh reads --dry-run in any place, refuses an unknown argument, asks its own
                worker whatever FEEDS_URL the shell carries, and takes a dead holder's lock over once
  site stamp    both site.sh refuse a dirty app/ (staging's did not on its own) and stamp the EXPORT's commit (read
                from its bundle), not HEAD at deploy time; a bundle-less export is said, not a bare exit 1
  hub rewrite   staging's "Open the app" rewrite is checked afterwards (it was `|| true`)
  r2 read       deploy/lib/r2-object.sh tells "no object" from "rclone failed"
  prod upload   production asks the site-version gate (staging's), and both uploads check HYDAT before any copy
  release root  --no-basemap / --no-synopsis are refused for the live release root
  W46 scan      every `|| true` / `2>/dev/null` left in the deploy scripts is one of the justified ones below
"""
from __future__ import annotations

import gzip
import json
import os
import re
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
STAGING_WORKER = "https://bc-fishing-feeds-staging.horvath-dawson.workers.dev"


def _exe(p: Path, text: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)
    p.chmod(p.stat().st_mode | stat.S_IXUSR)


def _copy(tree: Path, *rels: str) -> None:
    for rel in rels:
        (tree / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(ROOT / rel, tree / rel)


def _env(tree: Path, **extra: str) -> dict:
    env = {k: v for k, v in os.environ.items()
           if k not in ("FEEDS_URL", "CONFIRM_PROD", "SITE_DIRTY_OK", "SITE_DIRTY", "SITE_COMMIT", "SITE_RELEASE_ANY")}
    env.update(PATH=f"{tree / 'bin'}:{os.environ['PATH']}", **extra)
    return env


def _log(tree: Path, name: str) -> list[str]:
    p = tree / name
    return p.read_text().splitlines() if p.exists() else []


# ── formats + sync args: deploy/staging/sync-inputs.sh on stubs ─────────────────────────────────────────────────────

def _stage_stub(matches: str) -> str:
    """pipeline.deliver.sync_inputs --out <dir>: two inputs, the matches file holding `matches` (raw text)"""
    return ("#!/usr/bin/env bash\nout=$4; mkdir -p \"$out/feeds/inseason\" \"$out/dfo\"\n"
            f"cat > \"$out/feeds/inseason/matches.json\" <<'J'\n{matches}\nJ\n"
            "echo '{}' > \"$out/dfo/a.json\"\nprintf 'dfo/a.json\\t-\\nfeeds/inseason/matches.json\\tno-cache\\n'\n")


@pytest.fixture
def sync_tree(tmp_path):
    _copy(tmp_path, "deploy/staging/sync-inputs.sh", "deploy/lib/input-formats.sh")
    _exe(tmp_path / ".venv/bin/python", _stage_stub('{"notices": {"a": 1}}'))
    (tmp_path / "data/release").mkdir(parents=True)
    (tmp_path / "data/release/LATEST").write_text("r-1\n")
    (tmp_path / "data/generated/inseason").mkdir(parents=True)
    (tmp_path / "data/generated/inseason/resolved.json").write_text('{"release": "r-1"}')
    _exe(tmp_path / "bin/rclone", f'#!/usr/bin/env bash\necho "$*" >> "{tmp_path}/rclone.log"\n'
                                  'echo "INFO  : x: Copied (new)" >&2\n')
    return tmp_path


def _curl(tree: Path, answer: str | None) -> None:
    """the worker's /status.json: `answer` (None: no HTTP answer, curl exit 6 with its words); the URL asked is logged"""
    _exe(tree / "bin/curl", f'#!/usr/bin/env bash\nfor a in "$@"; do case $a in http*) echo "$a" >> "{tree}/curl.log";; esac; done\n'
         + ('echo "curl: (6) Could not resolve host: w.example" >&2\nexit 6\n' if answer is None else
            f"cat <<'J'\n{answer}\nJ\n"))


def _sync(tree: Path, *args: str, **env: str) -> subprocess.CompletedProcess:
    return subprocess.run(["bash", str(tree / "deploy/staging/sync-inputs.sh"), *args], env=_env(tree, **env),
                          capture_output=True, text=True)


def test_a_staged_file_jq_cannot_read_is_refused_not_put(sync_tree):
    """(T994 #9) `fmt=$(jq … 2>/dev/null || true)`: a corrupt staged file read as "no format" and went to R2 unasked.
    Mutation: put the `|| true` back — the corrupt file is put and the sync reports success."""
    _exe(sync_tree / ".venv/bin/python", _stage_stub('{"format": "inseason-links/3", "notices": '))   # cut short
    _curl(sync_tree, '{"inputs": {"feeds/inseason/matches.json": ["inseason-links/3"]}}')
    r = _sync(sync_tree)
    assert r.returncode == 1 and "inputs in sync" not in r.stdout
    assert ("FAILED feeds/inseason/matches.json: the staged file cannot be read as JSON, so its format is not known (jq: "
            in r.stderr)
    log = _log(sync_tree, "rclone.log")
    assert len(log) == 1 and "dfo/a.json" in log[0]                    # the readable input still went up


def test_the_format_gate_stops_without_jq():
    """(T994 #9) a missing jq read every file as "no format". Mutation: drop the `command -v jq` guard — the filter
    reads on and puts every key."""
    script = ('refuse() { echo "FAILED $1"; }; source deploy/lib/input-formats.sh; PATH=/nonexistent-dir; '
              'input_formats_filter m o p; echo "rc=$?"')
    r = subprocess.run(["bash", "-c", script], cwd=ROOT, capture_output=True, text=True)
    assert "rc=1" in r.stdout and "jq is not installed" in r.stderr
    for rel in ("deploy/staging/sync-inputs.sh", "deploy/prod/sync-inputs.sh"):   # and both scripts look before staging
        body = "\n".join(ln for ln in (ROOT / rel).read_text().splitlines() if not ln.lstrip().startswith("#"))
        assert body.index("command -v jq") < body.index("-m pipeline.deliver.sync_inputs"), rel


@pytest.mark.parametrize("answer,cause", [
    (None, "(no answer: curl exit 6, curl: (6) Could not resolve host: w.example"),
    ("<html>502 Bad Gateway</html>", "(its answer is not a JSON object (it begins: <html>502 Bad Gateway</html>"),
    ('{"feeds": {}}', "(its answer lists no inputs: a worker deployed before T597)")])
def test_a_worker_that_cannot_be_asked_is_refused_with_its_cause(sync_tree, answer, cause):
    """(T788 #1's twin, input-formats.sh line 35) `curl … 2>/dev/null | jq … 2>/dev/null || true` made a DNS failure, a
    gateway page and an old worker read alike. Mutation: hide the cause again — the refusal loses the words."""
    _exe(sync_tree / ".venv/bin/python", _stage_stub('{"format": "inseason-links/3", "notices": {}}'))
    _curl(sync_tree, answer)
    r = _sync(sync_tree)
    assert r.returncode == 1
    assert "does not say which input formats the worker reads " + cause in r.stderr
    assert not any("matches.json" in ln for ln in _log(sync_tree, "rclone.log"))


def test_staging_sync_reads_dry_run_in_any_place_and_refuses_an_unknown_argument(sync_tree):
    """(T994 #18) `[ "${1:-}" = "--dry-run" ]`: --dry-run in second place, or a typo, did a REAL put. Mutation: read $1
    only — the misspelt run puts both inputs for real."""
    _curl(sync_tree, None)
    r = _sync(sync_tree, "--dryrun")
    assert r.returncode == 2 and "unknown argument --dryrun" in r.stderr and _log(sync_tree, "rclone.log") == []
    r = _sync(sync_tree, "--dry-run", "--dry-run")                       # read in second place too
    assert r.returncode == 0, r.stderr
    log = _log(sync_tree, "rclone.log")
    assert len(log) == 2 and all("--dry-run" in ln for ln in log)


def test_staging_sync_asks_its_own_worker_whatever_feeds_url_the_shell_carries(sync_tree):
    """(T788 #1) `FEEDS_URL=${FEEDS_URL:-…}` in both staging scripts: a stray value from another session asked another
    worker. Mutation: take the shell's value again — curl is asked https://elsewhere.example."""
    _exe(sync_tree / ".venv/bin/python", _stage_stub('{"format": "inseason-links/3", "notices": {}}'))
    _curl(sync_tree, '{"inputs": {"feeds/inseason/matches.json": ["inseason-links/3"]}}')
    r = _sync(sync_tree, FEEDS_URL="https://elsewhere.example")
    assert r.returncode == 0, r.stderr
    assert _log(sync_tree, "curl.log") == [f"{STAGING_WORKER}/status.json"]


def test_staging_sync_takes_a_dead_holders_lock_over_once(sync_tree):
    """(T994 #18) a dead holder's lock was `rm -rf`ed by every waiter, so a second could delete the lock the first had
    just taken. Now: under a mutex, re-checked, mv then delete (heavy.sh's way), said."""
    lock = sync_tree / "data/generated/.sync-inputs.lock"
    lock.mkdir(parents=True)
    dead = subprocess.run(["bash", "-c", "echo $$"], capture_output=True, text=True).stdout.strip()
    (lock / "pid").write_text(dead)
    _curl(sync_tree, None)
    r = _sync(sync_tree)
    assert r.returncode == 0, r.stderr
    assert f"took over the lock of a sync that is gone (pid {dead})" in r.stderr
    assert not lock.exists() and not (sync_tree / "data/generated/.sync-inputs.lock.takeover").exists()


# ── r2 read: deploy/lib/r2-object.sh ───────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("stat_answer,want", [
    ('[{"Path":"index.json","IsDir": false}]', "rc=0 body=hello"),
    ('{"Path":"","IsDir": true}', "rc=1 body="),          # R2's answer for a missing key
    (None, "rc=2 body=")])                                 # rclone failed (credentials, network)
def test_r2_read_tells_no_object_from_a_failed_rclone(tmp_path, stat_answer, want):
    """(T994 #19) staging's upload read run timing with `rclone cat … 2>/dev/null || true`: a dead rclone read as
    "staging has no run timing". Mutation: make r2_read fold rc 2 into 1 — the failure reads as absent."""
    _exe(tmp_path / "bin/rclone", '#!/usr/bin/env bash\ncase $1 in\n'
         + ('  lsjson) echo "Failed to lsjson: AccessDenied" >&2; exit 1 ;;\n' if stat_answer is None else
            f"  lsjson) echo '{stat_answer}' ;;\n")
         + '  cat) echo hello ;;\nesac\n')
    script = (f'source {ROOT}/deploy/lib/r2-object.sh; rc=0; r2_read r2:b/runtiming/index.json "{tmp_path}/out" || rc=$?; '
              f'echo "rc=$rc body=$(cat "{tmp_path}/out")"')
    r = subprocess.run(["bash", "-c", script], env=_env(tmp_path), capture_output=True, text=True)
    assert r.stdout.strip() == want, r.stderr


def test_staging_upload_reads_run_timing_through_r2_read():
    body = (ROOT / "deploy/staging/upload.sh").read_text()
    run = "\n".join(ln for ln in body.splitlines() if not ln.lstrip().startswith("#"))
    assert "source deploy/lib/r2-object.sh" in run and 'r2_read "$D/runtiming/index.json"' in run
    assert 'r2_read "$D/$RT_KEY"' in run
    assert "2>/dev/null" not in run and "|| true" not in run


# ── site stamp + hub rewrite: deploy/staging/site.sh and deploy/prod/site.sh on a fixture tree ──────────────────────

def _git(tree: Path, *args: str) -> str:
    env = {**os.environ, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@example", "GIT_COMMITTER_NAME": "t",
           "GIT_COMMITTER_EMAIL": "t@example"}
    return subprocess.run(["git", "-C", str(tree), *args], env=env, check=True, capture_output=True,
                          text=True).stdout.strip()


HUB = '<a class="app" href="https://canifishthis.ca/">'


@pytest.fixture
def site_tree(tmp_path):
    """a git tree whose app/ is committed (commit A), with a staging and a production web export built at A, their SEO
    pages, a release, and stand-ins for npx (wrangler); HEAD then moves on with a docs-only commit B"""
    t = tmp_path
    _copy(t, "deploy/staging/site.sh", "deploy/prod/site.sh", "deploy/lib/site-stamp.sh")
    (t / "app").mkdir()
    (t / "app/x.txt").write_text("app\n")
    (t / ".gitignore").write_text("app/dist/\nbin/\nnpx.log\n")
    _git(t, "init", "-q")
    _git(t, "add", "-A")
    _git(t, "commit", "-qm", "A")
    a = _git(t, "rev-parse", "HEAD")
    js = "/_expo/static/js/web/index-0123abcd.js"
    for w, s, prod in (("web-staging", "seo", False), ("web-prod", "seo-prod", True)):
        W, S = t / "app/dist" / w, t / "app/dist" / s
        (W / "_expo/static/js/web").mkdir(parents=True)
        (W / "index.html").write_text(f'<link rel=preload href="{js}"><script src="{js}"></script>')
        (W / js.lstrip("/")).write_text(f'x;resolveAppVersion({{version:"0.8.5",build:"40",sha:"{a}",date:"d"}});')
        (S / "water/vedder").mkdir(parents=True)
        (S / "water/vedder/index.html.gz").write_bytes(gzip.compress(
            f'<script src="{js}"></script><meta name="cift:region" content="2">'.encode()))
        (S / "hub").mkdir()
        (S / "hub/index.html").write_text(f"{HUB}Open the app</a>")
        (S / "gauge/08MH001").mkdir(parents=True)
        (S / "gauge/08MH001/index.html").write_text('"https://data.canifishthis.ca/x"')
        (S / "gauge/stations.json").write_text("[]")
        (S / "share").mkdir()
        (S / "share/gauge.png").write_bytes(b"png")
        (S / "_worker.js").write_text("worker")
        (S / "host-worker.js").write_text("host")
        (S / "snapshot.json").write_text('{"release": "r-1"}')
        (S / "_headers").write_text("/*\n  X-Frame-Options: DENY\n")
        (S / "robots.txt").write_text("User-agent: *\nAllow: /\nSitemap: https://canifishthis.ca/sitemap.xml\n")
    (t / "data/release/r-1").mkdir(parents=True)
    (t / "data/release/LATEST").write_text("r-1\n")
    (t / "data/release/r-1/manifest.json").write_text('{"minAppVersion": "0.8.0"}')
    _exe(t / "bin/npx", f'#!/usr/bin/env bash\necho "$*" >> "{t}/npx.log"\n')
    (t / "docs.md").write_text("docs\n")
    _git(t, "add", "docs.md")
    _git(t, "commit", "-qm", "B")
    return t, a, _git(t, "rev-parse", "HEAD")


def _site(t: Path, which: str, **env: str) -> subprocess.CompletedProcess:
    args = ["--dry-run"] if which == "prod" else []
    return subprocess.run(["bash", str(t / f"deploy/{which}/site.sh"), *args], env=_env(t, SMOKE="0", **env),
                          capture_output=True, text=True)


@pytest.mark.parametrize("which,out", [("staging", "staging-site"), ("prod", "prod-site")])
def test_the_site_stamps_the_exports_commit_not_head(site_tree, which, out):
    """(T788 #4) production stamped `git rev-parse HEAD` at deploy time, so a site exported at A and deployed at B said
    B. Mutation: stamp HEAD again — site-release.json names B."""
    t, a, b = site_tree
    r = _site(t, which)
    assert r.returncode == 0, r.stderr
    got = json.loads((t / f"app/dist/{out}/site-release.json").read_text())
    assert got["commit"] == a and got["dirty"] is False
    assert f"the export was built at {a}; HEAD is {b} (app/ is the same in both)" in r.stdout
    assert _log(t, "npx.log"), "deployed (the stand-in)"
    (t / "app/x.txt").write_text("app v2\n")                      # app/ moves on in a commit after the export
    _git(t, "commit", "-qam", "C")
    r = _site(t, which)
    assert r.returncode == 0 and "app/ DIFFERS between them" in r.stdout
    assert json.loads((t / f"app/dist/{out}/site-release.json").read_text())["commit"] == a


@pytest.mark.parametrize("which", ["staging", "prod"])
def test_the_site_refuses_a_dirty_app_on_its_own(site_tree, which):
    """(T788 #4) staging's site.sh did not ask (only site-all.sh did): run on its own it deployed a dirty app/.
    Mutation: drop site_stamp's dirty check — the deploy goes ahead."""
    t, _, _ = site_tree
    (t / "app/x.txt").write_text("someone else's edit\n")
    r = _site(t, which)
    assert r.returncode == 1 and "app/ has uncommitted changes" in r.stderr and "app/x.txt" in r.stderr
    assert _log(t, "npx.log") == []
    r = _site(t, which, SITE_DIRTY_OK="T999 hotfix test")
    assert r.returncode == 0, r.stderr
    assert "SITE_DIRTY_OK: 1 uncommitted app/ path(s) - T999 hotfix test" in r.stdout


@pytest.mark.parametrize("which", ["staging", "prod"])
def test_an_export_that_names_no_bundle_or_commit_is_said(site_tree, which):
    """(T994 #19) `js=$(grep -o … | sort -u)` exited 1 with no word when index.html named no bundle."""
    t, _, _ = site_tree
    w = t / "app/dist" / ("web-prod" if which == "prod" else "web-staging")
    js = next((w / "_expo/static/js/web").iterdir())
    js.write_text("x;no stamp here;")
    r = _site(t, which)
    assert r.returncode == 1 and 'carries no commit (sha:"<40 hex>", version-stamp.js)' in r.stderr
    (w / "index.html").write_text("<html></html>")
    r = _site(t, which)
    assert r.returncode == 1 and "index.html names no app bundle" in r.stderr
    assert _log(t, "npx.log") == []


def test_the_staging_hub_rewrite_is_checked(site_tree):
    """(T994 #19) `grep -rl … | xargs perl … || true`: a failed rewrite left production's "Open the app" on staging's
    hubs and nothing looked. Mutation: a perl that rewrites nothing, with the `|| true` back — the deploy goes on."""
    t, _, _ = site_tree
    r = _site(t, "staging")
    assert r.returncode == 0, r.stderr
    assert (t / "app/dist/staging-site/hub/index.html").read_text() == '<a class="app" href="/">Open the app</a>'
    _exe(t / "bin/perl", "#!/usr/bin/env bash\nexit 0\n")              # rewrites nothing, says nothing
    (t / "npx.log").unlink()
    r = _site(t, "staging")
    assert r.returncode == 1 and "a hub page still opens production's app after the staging rewrite" in r.stderr
    assert _log(t, "npx.log") == []


# ── production upload: the site gate (T994 #10) and HYDAT before any copy (T994 #28) ────────────────────────────────

def _first_copy(body: str) -> int:
    run = re.search(r'^\s*(?:cpr|cp_|rclone copy|rclone copyto) ', body, re.M)
    assert run
    return run.start()


def test_production_upload_asks_the_site_gate_before_anything_is_sent():
    """(T994 #10) production ran app_gate and pieces_gate only: a release needing a newer app than the production site
    would make its web app refuse the data. Mutation: drop the call — no site_gate in the script."""
    body = (ROOT / "deploy/prod/upload.sh").read_text()
    call = body.index('.venv/bin/python -m pipeline.deliver.release.site_gate "$M" --site "$SITE"')
    assert call < _first_copy(body)
    assert 'if [[ $BUCKET == "$PROD_BUCKET" ]]; then SITE=https://canifishthis.ca;' in body
    # its own escape: a SITE_CHECK_SKIP left in the shell from a staging run never skips production's gate
    assert "SITE_CHECK_SKIP=${PROD_SITE_CHECK_SKIP:-} MIN_APP_VERSION= .venv/bin/python -m pipeline.deliver.release.site_gate" in body


def test_the_site_gate_names_the_production_site_and_its_steps(monkeypatch):
    from pipeline.deliver.release import site_gate as SG
    monkeypatch.delenv("SITE_CHECK_SKIP", raising=False)
    idx = '<script src="/_expo/static/js/web/index-5524ce5c.js"></script>'
    js = 'resolveAppVersion)({version:"0.8.4",build:"39",sha:"x"});'
    fetch = lambda url, timeout=30: idx if url.endswith("/") else js       # noqa: E731
    bad, _ = SG.check({"minAppVersion": "0.8.5"}, None, site=SG.PROD_SITE, fetch=fetch)
    assert len(bad) == 1 and bad[0].startswith("the production site is app 0.8.4, older than 0.8.5")
    assert "PRODUCTION.md step 6" in bad[0] and "deploy/prod/site.sh" in bad[0] and "site-all" not in bad[0]
    (unknown,) = SG.check({"minAppVersion": "0.8.5"}, None, site=SG.PROD_SITE, fetch=lambda u, timeout=30: "")[0]
    assert "PROD_SITE_CHECK_SKIP=<reason>" in unknown
    staged, _ = SG.check({"minAppVersion": "0.8.5"}, None, site=SG.STAGING_SITE, fetch=fetch)
    assert staged[0].startswith("the staging site is app 0.8.4") and "deploy/staging/site-all.sh" in staged[0]


@pytest.mark.parametrize("rel", ["deploy/staging/upload.sh", "deploy/prod/upload.sh"])
def test_both_uploads_check_hydat_before_any_copy(rel):
    """(T994 #28) production checked hydat_daily after its tiles and data had gone up; staging skipped it in silence
    (`if [[ -d … ]]`). Mutation: move the check back below the copies, or make it optional again."""
    body = (ROOT / rel).read_text()
    check = body.index("[[ -d data/generated/gauges/hydat_daily ]] || {")
    assert check < _first_copy(body)
    assert "if [[ -d data/generated/gauges/hydat_daily ]]" not in body


# ── release root (T788 #5) ──────────────────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("flag", ["--no-basemap", "--no-synopsis"])
def test_no_ground_or_no_book_is_refused_for_the_live_release_root(tmp_path, capsys, flag):
    """(T788 #5) both flags were accepted with the default (live) roots: a release a device installs could ship no
    basemap or no synopsis. Mutation: drop the check — the release is built (the stand-in below is reached)."""
    from pipeline.deliver.release import __main__ as CLI
    from pipeline.deliver.release import manifest as MAN
    args = ["--bundle", str(tmp_path / "b.sqlite"), "--store", str(tmp_path / "s.sqlite"), flag]
    with pytest.raises(SystemExit) as e:
        CLI.main(args)                                  # the defaults: both roots are the live one
    assert e.value.code == 2
    err = capsys.readouterr().err
    assert f"{flag} is only for a release no device installs" in err and "is the live release root" in err
    assert "--out-root" in err and "--tiles-release-root" in err
    # `deliver all` cuts into a staging folder and names the live root for the tiles: that is live too
    with pytest.raises(SystemExit):
        CLI.main(args + ["--out-root", str(tmp_path / "staging")])
    assert "--tiles-release-root" in capsys.readouterr().err
    # roots of its own (the mini release): past the check, to the next one (--source is read next)
    own = ["--out-root", str(tmp_path / "out"), "--tiles-release-root", str(tmp_path / "out"),
           "--source", str(tmp_path / "no-source")]
    with pytest.raises(SystemExit):
        CLI.main(args + own)
    assert "is not a directory" in capsys.readouterr().err
    assert MAN  # the module the release reads its blocks from is importable (the check runs before any of them)


# ── feeds + restore: the reads that hid a failure (T994 #19) ──────────────────────────────────────────────────────

@pytest.mark.parametrize("rel", ["deploy/staging/feeds.sh", "deploy/prod/feeds.sh"])
def test_the_feeds_deploy_reads_its_secret_list_once_and_checks_it(rel):
    """`wrangler secret list 2>/dev/null | grep -q REPORT_SALT`: an expired login read as "not set" (and grep -q could
    SIGPIPE wrangler). Mutation: pipe it into grep again."""
    run = "\n".join(ln for ln in (ROOT / rel).read_text().splitlines() if not ln.lstrip().startswith("#"))
    assert run.count("wrangler secret list") == 1
    assert re.search(r'^SECRETS=\$\(npx --prefix app wrangler secret list -c [^)]+\) \|\| \{', run, re.M)
    assert "secret list" not in "".join(ln for ln in run.splitlines() if "| grep" in ln)


def test_staging_feeds_shows_a_failed_deploy_whole():
    run = (ROOT / "deploy/staging/feeds.sh").read_text()
    assert "wrangler deploy -c app/workers/feeds/wrangler.toml 2>&1 | grep" not in run
    assert 'wrangler deploy -c app/workers/feeds/wrangler.toml > "$DEPLOY_LOG" 2>&1' in run


def test_the_restore_test_never_reads_a_failed_check_as_no_difference():
    body = (ROOT / "deploy/staging/restore-test.sh").read_text()
    assert '|| echo "0 differences found"' not in body
    assert body.count('rclone check "$tmp/state" "$SRC"') == 1
    assert "printed no count (rclone exit $crc)" in body


# ── the W46 scan: what is left of `|| true` / `2>/dev/null` in the deploy scripts is justified ──────────────────────

#: (file, a fragment of the line) -> why it hides nothing. The build scripts are lane T978's (build-apk.sh, the iOS ones).
JUSTIFIED: dict[tuple[str, str], str] = {
    ("deploy/staging/sync-inputs.sh", 'mkdir "$LOCK" 2>/dev/null'): "the lock: losing the mkdir race is the answer",
    ("deploy/staging/sync-inputs.sh", 'holder=$(cat "$LOCK/pid" 2>/dev/null)'): "the lock: a pid file not yet written",
    ("deploy/staging/sync-inputs.sh", 'kill -0 "$holder" 2>/dev/null'): "the lock: probing a pid that may be dead",
    ("deploy/staging/sync-inputs.sh", 'mkdir "$LOCK.takeover" 2>/dev/null'): "the takeover mutex: losing is the answer",
    ("deploy/staging/sync-inputs.sh", 'mv "$LOCK" "$LOCK.stale.$$" 2>/dev/null'): "the takeover: another waiter won",
    ("deploy/staging/sync-inputs.sh", 'stat -f %m "$LOCK.takeover" 2>/dev/null'): "the mutex vanished: age 0",
    ("deploy/staging/sync-inputs.sh", 'rmdir "$LOCK.takeover" 2>/dev/null'): "a stale mutex another waiter removed",
    ("deploy/staging/sync-inputs.sh", "trap 'rm -rf \"$T\"; [[ \"$(cat \"$LOCK/pid\" 2>/dev/null)\""): "the exit trap: only the owner frees the lock",
    ("deploy/staging/sync-inputs.sh", 'grep -E "Copied|Skipped|ERROR" "$T/log" || true'): "a log filter after a checked copy",
    ("deploy/staging/sync-inputs.sh", "grep '^FAILED ' \"$T/stage.err\" >&2 || true"): "the stage's stderr was printed whole above",
    ("deploy/prod/sync-inputs.sh", 'grep -E "Copied|Skipped|ERROR|NOTICE" "$T/log" || true'): "a log filter after a checked copy",
    ("deploy/staging/feeds.sh", '--s3-no-check-bucket > "$LIVE_M" || : > "$LIVE_M"'): "rclone says why; empty = the gate's refusal",
    ("deploy/prod/feeds.sh", '--s3-no-check-bucket > "$LIVE_M" || : > "$LIVE_M"'): "rclone says why; empty = the gate's refusal",
    ("deploy/lib/site-stamp.sh", 'git cat-file -e "$sha^{commit}" 2>/dev/null'): "a commit not here is said, never a pass",
    # (T978 lane, deploy/lib/build-links.sh) the search of a folder checked to exist: an unreadable part finds fewer
    # AARs, which is refused ("must hold exactly one"): it fails closed
    ("deploy/lib/build-links.sh", "found=$(find -L \"$dir\" -name 'android-sdk-vulkan-*.aar' 2>/dev/null)"):
        "fails closed: fewer AARs found is the refusal",
}
SCANNED_OUT = ("deploy/staging/build-apk.sh", "deploy/staging/build-ios-testflight.sh", "deploy/staging/build-ios-sim.sh")
HIDES = re.compile(r"\|\| true\b|2>/dev/null|\|\| :")


def test_every_hiding_left_in_the_deploy_scripts_is_justified():
    """(T994 #19, W46) a new `|| true` / `2>/dev/null` / `|| :` in a deploy script fails here until it is either removed
    or added above with why it hides no tool failure; an allowance whose line is gone fails too (stale)."""
    seen, bad = set(), []
    for p in sorted((ROOT / "deploy").rglob("*.sh")):
        rel = p.relative_to(ROOT).as_posix()
        if rel in SCANNED_OUT:
            continue
        for n, ln in enumerate(p.read_text().splitlines(), 1):
            if ln.lstrip().startswith("#") or not HIDES.search(ln.split(" # ")[0]):
                continue
            hit = [k for k in JUSTIFIED if k[0] == rel and k[1] in ln]
            if hit:
                seen.update(hit)
            else:
                bad.append(f"{rel}:{n}: {ln.strip()}")
    assert not bad, "unjustified (remove it, or add it to JUSTIFIED with why):\n" + "\n".join(bad)
    # stale: an allowance whose line is gone from a file that is here (a lane's file may land after this one)
    stale = sorted(k for k in set(JUSTIFIED) - seen if (ROOT / k[0]).exists())
    assert not stale, f"stale allowances: {stale}"
