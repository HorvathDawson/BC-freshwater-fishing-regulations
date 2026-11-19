"""T865: the production bucket prunes itself after a promote (pipeline.deliver.release.prod_prune).

No network, no rclone: a bucket is a folder (`Folder`, rclone's include rules for the patterns the module uses) and the
site a string. Old releases: live + previous kept, older ones deleted, nothing outside release/r-*, release/tiles/*,
release/basemap/* ever touched, refused on a pointer it cannot read or a named file that is missing. v1: kept without
the new site's marker, deleted with it. The wiring: upload.sh prunes after the pointer, site.sh after the deploy.
"""
from __future__ import annotations

import json
import re
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

    def __init__(self, root: Path, unreadable: tuple = ()):
        self.root, self.unreadable, self.deleted = root, set(unreadable), []

    @staticmethod
    def _rx(pattern: str):
        assert pattern.startswith("/"), pattern
        body = re.escape(pattern[1:]).replace(r"\*\*", "\0").replace(r"\*", "[^/]*").replace("\0", ".*")
        return re.compile(f"^{body}$")

    def list(self, includes):
        rx = [self._rx(i) for i in includes]
        keys = (p.relative_to(self.root).as_posix() for p in self.root.rglob("*") if p.is_file())
        return {k: (self.root / k).stat().st_size for k in keys if any(r.match(k) for r in rx)}

    def cat(self, key):
        if key in self.unreadable:
            raise pp.Refused("rclone cat failed (exit 3): connection reset")
        p = self.root / key
        return p.read_bytes() if p.is_file() else None

    def delete(self, keys, dry):
        if not dry:
            for k in keys:
                (self.root / k).unlink()
                self.deleted.append(k)

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
    return Folder(root)


def _run(b, *argv, site=OLD_SITE):
    return pp.main(list(argv), bucket=b, fetch=lambda url: site)


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
    assert "delete release/r-older/core-r-older.sqlite.gz  (10 B)" in out


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
    before = bucket.keys()
    assert _run(bucket, "v1", site=site) == 0
    assert bucket.keys() == before
    assert f"v1 kept: https://canifishthis.ca/ {why}" in capsys.readouterr().out


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


def test_upload_prunes_after_the_pointer_and_site_after_the_deploy():
    run = [ln for ln in (ROOT / "deploy/prod/upload.sh").read_text().splitlines() if not ln.lstrip().startswith("#")]
    pointer = next(i for i, ln in enumerate(run) if 'cp_ "$M" release/manifest.json' in ln)
    prunes = [i for i, ln in enumerate(run) if "pipeline.deliver.release.prod_prune" in ln]
    assert [run[i].split("prod_prune ")[1].split()[0] for i in prunes] == ["releases", "v1"]
    assert all(i > pointer for i in prunes), "the prune must come after the pointer moves"
    assert not any(re.match(r"\s*rclone (delete|purge|sync)\b", ln) for ln in run), "deletes go through prod_prune only"
    site = [ln for ln in (ROOT / "deploy/prod/site.sh").read_text().splitlines() if not ln.lstrip().startswith("#")]
    deploy = next(i for i, ln in enumerate(site) if "wrangler deploy -c deploy/prod/site.wrangler.toml" in ln)
    v1 = next(i for i, ln in enumerate(site) if "prod_prune v1" in ln)
    assert v1 > deploy and "[[ -n $DRY ]]" in site[v1] + site[v1 - 1], "site.sh runs v1 after a real deploy only"
