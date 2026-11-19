"""T865: the production bucket prunes itself after a promote (pipeline.deliver.release.prod_prune).

No network, no rclone: a bucket is a folder (`Folder`, rclone's include rules for the patterns the module uses) and the
site a string. Old releases: live + previous kept, older ones deleted unless they went up under 24 h ago, nothing
outside release/r-*, release/tiles/*, release/basemap/* ever touched, refused on a pointer it cannot read or a named
file that is missing, a delete that stops part way says what went. v1: kept without the new site's marker, deleted
with it, only by deploy/prod/drop-v1.sh. The wiring: upload.sh prunes releases after the pointer; every deploy/prod
script compares --bucket bare (run on stubs: nothing here reaches a bucket).
"""
from __future__ import annotations

import json
import os
import re
import shutil
import stat
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipeline.deliver.release import prod_prune as pp

ROOT = Path(__file__).resolve().parents[2]

OLD_SITE = ('<!doctype html>\n<!-- build: webapp UI fixes (ungazetted disambiguation, overlay panel, mobile panel, report '
            'button) -->\n<html><script type="module" crossorigin src="/assets/index-BpMjuwd5.js"></script></html>')
NEW_SITE = '<!DOCTYPE html><html><script src="/_expo/static/js/web/index-e4aabba7d7857df495d615b5ad7ea919.js" defer>'
#: keys no prune may ever delete: the feeds worker's, the run-timing feed's, fixed keys, the app's, user reports
NEVER = ("feeds/live/index.json", "feeds/dfo/region-1.json", "feeds/hydat/08MF005.json", "private/reports/a.json",
         "private/runtiming/base/T0.json.gz", "runtiming/index.json", "runtiming/runs-abc.json", "app/android/play.json",
         "play.json", "map-assets/v1/sprite.png", "sql-wasm.wasm", "feedback/2026-07-01/x.json", "test/release/manifest.json",
         "feeds/release/r-0/core.gz", "private/release/tiles/T0/atlas.pmtiles")
V1 = ("tier0.json", "data_version.json", "bc.pmtiles", "freshwater_atlas.pmtiles", "layer_manifest.json",
      "cron/hydro/stations.json", "cron/hydro/gauges.geojson", "cron/hydro/recent/08MF005.json",
      "cron/hydro/history/08MF005.json", "cron/hydro/climatology/08MF005.json", "cron/in-season/in_season.json",
      "cron/stocking/records/1.json", "shards/v2/fids/abc.json", "mobile/v2/regulations.sqlite.gz")


class Folder:
    """A bucket on disk, read the way rclone reads a remote with `--include` rules (`/key`, `/prefix/**`, `*`)."""

    def __init__(self, root: Path, unreadable: tuple = (), fail_after: int | None = None):
        self.root, self.unreadable, self.deleted, self.fail_after = root, set(unreadable), [], fail_after

    @staticmethod
    def _rx(pattern: str):
        assert pattern.startswith("/"), pattern
        body = re.escape(pattern[1:]).replace(r"\*\*", "\0").replace(r"\*", "[^/]*").replace("\0", ".*")
        return re.compile(f"^{body}$")

    def list(self, includes):
        rx = [self._rx(i) for i in includes]
        keys = (p.relative_to(self.root).as_posix() for p in self.root.rglob("*") if p.is_file())
        return {k: pp.Obj((self.root / k).stat().st_size,
                          datetime.fromtimestamp((self.root / k).stat().st_mtime, timezone.utc))
                for k in keys if any(r.match(k) for r in rx)}

    def cat(self, key):
        if key in self.unreadable:
            raise pp.Refused("rclone cat failed (exit 3): connection reset")
        p = self.root / key
        return p.read_bytes() if p.is_file() else None

    def delete(self, keys, on_deleted):
        for k in keys:
            if self.fail_after is not None and len(self.deleted) == self.fail_after:
                raise pp.PartialDelete(list(self.deleted), "rclone delete failed (exit 1): 503 Service Unavailable")
            (self.root / k).unlink()
            self.deleted.append(k)
            on_deleted(k)

    def keys(self):
        return {p.relative_to(self.root).as_posix() for p in self.root.rglob("*") if p.is_file()}


def _put(root: Path, key: str, body: str = "x") -> None:
    (root / key).parent.mkdir(parents=True, exist_ok=True)
    (root / key).write_text(body)


def _manifest(rel: str, tiles: str, basemap: str = "basemap-a") -> dict:
    return {"data_release": rel, "format": "app-manifest/2",
            "files": {"core": {"url": f"{rel}/core-{rel}.sqlite.gz"}, "regs-1": {"url": f"{rel}/regs-1-{rel}.sqlite.gz"}},
            "synopsis": {"url": f"{rel}/synopsis-{rel}.pdf"},
            "basemap": {"url": f"basemap/{basemap}/{basemap}.pmtiles"},
            "tiles": {"current": {"url": f"tiles/{tiles}/atlas-{tiles}.pmtiles"},
                      "mask": {"url": f"tiles/{tiles}/bc_outside.geojson"},
                      "assets": {"url": f"tiles/{tiles}/map-assets-1/"}, "compatible": [f"{tiles.split('.')[0]}.*"]}}


def _release(root: Path, m: dict) -> None:
    for k in pp.named(m):
        _put(root, k + "font.pbf" if k.endswith("/") else k, "y" * 10)


@pytest.fixture
def bucket(tmp_path):
    """Three releases on disk: r-new (live), r-old (previous), r-older (pruned; its own tiles and basemap)."""
    root = tmp_path / "bucket"
    live, prev, older = _manifest("r-new", "T2.b"), _manifest("r-old", "T2.a"), _manifest("r-older", "T1.a", "basemap-0")
    for m in (live, prev, older):
        _release(root, m)
    _put(root, pp.LIVE, json.dumps(live))
    _put(root, pp.PREVIOUS, json.dumps(prev))
    for k in NEVER + V1:
        _put(root, k)
    _age(root, "", NOW - timedelta(days=3))
    return Folder(root)


NOW = datetime(2026, 11, 18, 12, 0, tzinfo=timezone.utc)


def _age(root: Path, prefix: str, when: datetime) -> None:
    for p in (root / prefix).rglob("*"):
        if p.is_file():
            os.utime(p, (when.timestamp(), when.timestamp()))


def _run(b, *argv, site=OLD_SITE):
    return pp.main(list(argv), bucket=b, fetch=lambda url: site, now=NOW)


def test_live_and_previous_are_kept_and_an_older_release_is_deleted(bucket, capsys):
    before = bucket.keys()
    assert _run(bucket, "releases") == 0
    gone = before - bucket.keys()
    assert gone == {k for k in before if "r-older" in k or "/T1.a/" in k or "/basemap-0/" in k}
    assert gone and all(k.startswith(("release/r-older/", "release/tiles/T1.a/", "release/basemap/basemap-0/")) for k in gone)
    for rel in ("r-new", "r-old"):
        assert all(k in bucket.keys() for k in pp.named(_manifest(rel, "T2.b" if rel == "r-new" else "T2.a")) if
                   not k.endswith("/"))
    out = capsys.readouterr().out
    assert "keep r-new, r-old (live + previous)" in out
    assert f"old releases: {len(gone)} object(s), {10 * len(gone):,} B" in out and "freed" in out
    assert "deleted release/r-older/core-r-older.sqlite.gz  (10 B)" in out
    assert out.count("\ndeleted ") + out.startswith("deleted ") == len(gone)


def test_a_folder_that_went_up_under_a_day_ago_is_kept_and_said_why(bucket, capsys):
    """A web tab opened on r-older's day (it was live this morning) still reads its region files (app boot.ts)."""
    _age(bucket.root, "release/r-older", NOW - timedelta(hours=23))
    _age(bucket.root, "release/tiles/T1.a", NOW - timedelta(hours=25))
    assert _run(bucket, "releases") == 0
    left, out = bucket.keys(), capsys.readouterr().out
    assert any(k.startswith("release/r-older/") for k in left)
    assert not any(k.startswith("release/tiles/T1.a/") for k in left)
    assert "keep release/r-older/: no pointer names it, but it went up 2026-11-17 13:00 UTC, under 24 h ago" in out


def test_a_delete_that_stops_part_way_says_what_went(bucket, capsys):
    bucket.fail_after = 2
    assert _run(bucket, "releases") == 1
    out, err = capsys.readouterr()
    assert out.count("deleted release/") == 2 and len(bucket.deleted) == 2
    assert "nothing deleted" not in err
    assert "prune FAILED: old releases: the delete stopped part way, 2 of " in err and "20 B (0.00 GB) deleted" in err


def test_rclone_s_delete_log_names_each_key_deleted():
    line = ('{"time":"2026-11-18T18:43:45.99-07:00","level":"info","msg":"Deleted","object":"release/r-1/a.gz",'
            '"objectType":"*s3.Object","source":"operations/operations.go:579"}')
    assert pp.deleted_key(line) == "release/r-1/a.gz"
    assert pp.deleted_key('{"level":"error","msg":"Failed to delete: 503","object":"release/r-1/b.gz"}') is None
    assert pp.deleted_key("2026/11/18 ERROR : not json") is None


def test_rclone_s_times_are_read_as_utc():
    assert pp._when("2026-11-18T18:43:45.992369123-07:00") == datetime(2026, 11, 19, 1, 43, 45, 992369, timezone.utc)
    assert pp._when("2026-11-19T01:43:45Z") == datetime(2026, 11, 19, 1, 43, 45, tzinfo=timezone.utc)


def test_nothing_outside_the_release_scoped_prefixes_is_ever_touched(bucket):
    assert _run(bucket, "releases") == 0
    assert _run(bucket, "v1", site=NEW_SITE) == 0
    left = bucket.keys()
    assert all(k in left for k in NEVER), sorted(set(NEVER) - left)
    assert pp.LIVE in left and pp.PREVIOUS in left


def test_a_dry_run_deletes_nothing_and_says_what_would_go(bucket, capsys):
    before = bucket.keys()
    assert _run(bucket, "releases", "--dry-run") == 0
    assert bucket.keys() == before
    assert "would delete release/r-older/core-r-older.sqlite.gz" in capsys.readouterr().out


def test_the_dry_run_of_a_promote_prunes_as_the_pointer_step_would_leave_it(bucket, tmp_path, capsys):
    """upload.sh --dry-run wrote no pointer: the pending release is live, the bucket's live one the previous."""
    pending = tmp_path / "manifest.json"
    pending.write_text(json.dumps(_manifest("r-next", "T3.a")))
    assert _run(bucket, "releases", "--dry-run", "--pending", str(pending)) == 0
    out = capsys.readouterr().out
    assert "keep r-next, r-new (live + previous)" in out
    assert "would delete release/r-old/core-r-old.sqlite.gz" in out and "would delete release/r-new/" not in out


@pytest.mark.parametrize("case", ["unreadable", "unparseable", "no data_release", "a named file missing", "same release"])
@pytest.mark.parametrize("which", [pp.LIVE, pp.PREVIOUS])
def test_the_prune_is_refused_and_deletes_nothing(bucket, capsys, case, which):
    before = bucket.keys()
    if case == "unreadable":
        bucket.unreadable.add(which)
    elif case == "unparseable":
        (bucket.root / which).write_text('{"data_release": "r-new", "files": {')
    elif case == "no data_release":
        (bucket.root / which).write_text('{"files": {}}')
    elif case == "a named file missing":
        rel = json.loads((bucket.root / which).read_text())["data_release"]
        (bucket.root / f"release/{rel}/regs-1-{rel}.sqlite.gz").unlink()
        before = bucket.keys()
    else:
        other = pp.PREVIOUS if which == pp.LIVE else pp.LIVE
        (bucket.root / which).write_text((bucket.root / other).read_text())
    assert _run(bucket, "releases") == 1
    assert bucket.keys() == before and not bucket.deleted
    err = capsys.readouterr().err
    assert "prune REFUSED, nothing deleted" in err


def test_no_previous_pointer_yet_prunes_nothing_and_says_so(bucket, capsys):
    (bucket.root / pp.PREVIOUS).unlink()
    before = bucket.keys()
    assert _run(bucket, "releases") == 0
    assert bucket.keys() == before
    assert "no previous pointer" in capsys.readouterr().out


@pytest.mark.parametrize("site, why", [(OLD_SITE, "still serves the old Vite site"),
                                       ("<html><script src=/assets/index-x.js></script></html>",
                                        "does not load the new web export"),
                                       (NEW_SITE + OLD_SITE, "still serves the old Vite site")])
def test_v1_is_kept_without_the_new_site_marker(bucket, capsys, site, why):
    """Fails closed: drop-v1.sh is run by hand at the switch, so a missing marker is an error, not a quiet skip."""
    before = bucket.keys()
    assert _run(bucket, "v1", site=site) == 1
    assert bucket.keys() == before
    assert f"v1 kept, nothing deleted: https://canifishthis.ca/ {why}" in capsys.readouterr().err


def test_v1_is_deleted_once_the_new_site_is_live(bucket, capsys):
    assert _run(bucket, "v1", site=NEW_SITE) == 0
    left = bucket.keys()
    assert not set(V1) & left, sorted(set(V1) & left)
    assert all(k in left for k in NEVER) and any(k.startswith("release/r-older/") for k in left)   # releases untouched
    assert f"v1 layout: {len(V1)} object(s), {len(V1):,} B" in capsys.readouterr().out


def test_a_site_that_cannot_be_read_keeps_v1_and_fails(bucket, capsys):
    def down(url):
        raise OSError("HTTP 522")
    before = bucket.keys()
    assert pp.main(["v1"], bucket=bucket, fetch=down) == 1
    assert bucket.keys() == before
    assert "v1 kept: https://canifishthis.ca/ could not be read (OSError: HTTP 522)" in capsys.readouterr().err


def test_the_v1_list_never_names_the_new_layout():
    new = ("release/", "feeds/", "private/", "runtiming/", "map-assets/", "app/", "test/", "feedback/")
    for inc in pp.V1_INCLUDES:
        assert not inc.lstrip("/").startswith(new), inc
        assert not inc.endswith(".wasm") and inc != "/play.json", inc


def test_upload_prunes_releases_after_the_pointer_and_never_drops_v1():
    run = [ln for ln in (ROOT / "deploy/prod/upload.sh").read_text().splitlines() if not ln.lstrip().startswith("#")]
    pointer = next(i for i, ln in enumerate(run) if 'cp_ "$M" release/manifest.json' in ln)
    prunes = [i for i, ln in enumerate(run) if "pipeline.deliver.release.prod_prune" in ln]
    assert [run[i].split("prod_prune ")[1].split()[0] for i in prunes] == ["releases"]
    assert all(i > pointer for i in prunes), "the prune must come after the pointer moves"
    assert not any(re.match(r"\s*rclone (delete|purge|sync)\b", ln) for ln in run), "deletes go through prod_prune only"
    for script in ("deploy/prod/site.sh", "deploy/staging/upload.sh"):
        assert "prod_prune" not in (ROOT / script).read_text(), f"{script} must not drop v1 (a site rollback needs it)"
    drop = (ROOT / "deploy/prod/drop-v1.sh").read_text()
    assert drop.index("CONFIRM_PROD") < drop.index("prod_prune v1"), "drop-v1.sh asks CONFIRM_PROD first"


# ── --bucket is compared bare (T865 review): a slash or a remote prefix must not slip past CONFIRM_PROD ─────────────

PROD_SCRIPTS = ("deploy/prod/upload.sh", "deploy/prod/rollback.sh", "deploy/prod/sync-inputs.sh", "deploy/prod/drop-v1.sh")


@pytest.fixture
def stub_tree(tmp_path):
    """The prod scripts in a temp tree; rclone, the venv's python and curl are stubs that log and fail."""
    for rel in PROD_SCRIPTS + ("deploy/lib/bucket.sh", "deploy/lib/input-formats.sh"):
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(ROOT / rel, tmp_path / rel)
    for tool in ("bin/rclone", "bin/curl", ".venv/bin/python"):
        p = tmp_path / tool
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(f'#!/usr/bin/env bash\necho "{tool} $*" >> "{tmp_path}/called.log"\nexit 99\n')
        p.chmod(p.stat().st_mode | stat.S_IXUSR)
    return tmp_path


def _script(tree: Path, script: str, *args: str) -> subprocess.CompletedProcess:
    env = {k: v for k, v in os.environ.items() if k not in ("CONFIRM_PROD", "RCLONE_CONFIG")}
    env.update(PATH=f"{tree / 'bin'}:/usr/bin:/bin", RCLONE_CONFIG="/dev/null")
    return subprocess.run(["bash", str(tree / script), *args], env=env, capture_output=True, text=True, cwd=tree)


@pytest.mark.parametrize("script", PROD_SCRIPTS)
@pytest.mark.parametrize("typed", ["bc-fishing-regulations/", "/bc-fishing-regulations", "r2:bc-fishing-regulations",
                                   "r2:bc-fishing-regulations//", "BC-Fishing-Regulations"])
def test_the_production_bucket_however_typed_needs_confirm_prod(stub_tree, script, typed):
    r = _script(stub_tree, script, "--bucket", typed, "--dry-run")
    assert r.returncode == 2 and "CONFIRM_PROD=canifishthis.ca" in r.stderr, (r.returncode, r.stderr)
    assert not (stub_tree / "called.log").exists(), "nothing may run before the guard"


@pytest.mark.parametrize("script", PROD_SCRIPTS)
@pytest.mark.parametrize("typed", ["bc-fishing-regulations/release", "", "r2:", "/"])
def test_a_bucket_path_or_empty_name_is_refused(stub_tree, script, typed):
    r = _script(stub_tree, script, "--bucket", typed, "--dry-run")
    assert r.returncode == 2 and not (stub_tree / "called.log").exists(), (r.returncode, r.stderr)


def test_a_rehearsal_bucket_passes_the_guard(stub_tree):
    r = _script(stub_tree, "deploy/prod/drop-v1.sh", "--bucket", "r2:bc-rehearsal/", "--dry-run")
    assert r.returncode == 99
    assert (stub_tree / "called.log").read_text().split("\n")[0] == (
        ".venv/bin/python -m pipeline.deliver.release.prod_prune v1 --remote r2:bc-rehearsal --site "
        "https://canifishthis.ca/ --dry-run")
