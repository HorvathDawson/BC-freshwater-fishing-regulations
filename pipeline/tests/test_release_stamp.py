"""(T251) The publish step stamps `published` and `minAppVersion` into the manifest — and nothing else moves: no file,
no tiles block, no release id, so a stamp never forces a tile rebuild or re-upload."""
from __future__ import annotations

import datetime as dt
import json

import pytest

from pipeline.deliver.release import publish as PUB
from pipeline.deliver.release.stamp import stamp, stamp_file

M = {"format": "app-manifest/2", "data_release": "r-abc", "files": {"core": {"url": "r-abc/core.gz", "sha256": "11"}},
     "tiles": {"compatible": ["h.*"], "current": {"id": "h.t1", "url": "tiles/h.t1/a.pmtiles"}}, "basemap": None}
D1, D2 = dt.date(2026, 10, 28), dt.date(2026, 11, 2)


def test_published_is_stamped_once_and_kept():
    a = stamp(M, today=D1)
    assert a["published"] == "2026-10-28" and "published" not in M
    assert stamp(a, today=D2)["published"] == "2026-10-28"            # staging then production: the first day stays
    assert stamp(a, today=D2, restamp=True)["published"] == "2026-11-02"


def test_min_app_version_set_removed_and_checked():
    a = stamp(M, today=D1, min_app_version="0.2.0")
    assert a["minAppVersion"] == "0.2.0"
    assert "minAppVersion" not in stamp(a, today=D1, min_app_version="")
    assert stamp(a, today=D1)["minAppVersion"] == "0.2.0"              # not asked: left as it is
    with pytest.raises(SystemExit):
        stamp(M, today=D1, min_app_version="2")


def test_a_stamp_moves_nothing_the_app_compares():
    a = stamp(M, today=D1, min_app_version="9.9.9")
    for k in ("data_release", "files", "tiles", "basemap", "format"):
        assert a[k] == M[k]


def test_stamp_file_writes_the_builds_format(tmp_path):
    p = tmp_path / "manifest.json"
    p.write_text(json.dumps(M, indent=1, sort_keys=True) + "\n")
    stamp_file(p, today=D1)
    assert p.read_text() == json.dumps({**M, "published": "2026-10-28"}, indent=1, sort_keys=True) + "\n"
    before = p.read_text()
    stamp_file(p, today=D2)                                            # nothing to change: not rewritten
    assert p.read_text() == before


def test_publish_plan_does_not_stamp(tmp_path, monkeypatch):
    r = tmp_path / "r-x"
    r.mkdir()
    (r / "core.gz").write_bytes(b"c")
    (r / "manifest.json").write_text(json.dumps({"files": {"core": {"url": "r-x/core.gz"}}, "tiles": {}}))
    monkeypatch.setattr(PUB, "GENERATED", type("G", (), {"release": tmp_path})())
    assert PUB.main(["--release", str(r), "--min-app-version", "0.2.0"]) == 0
    assert "published" not in json.loads((r / "manifest.json").read_text())
