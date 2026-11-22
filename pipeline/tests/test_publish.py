"""P10: the upload plan puts the tiles first, the data next and the manifest LAST; (T994 #1) it sends nothing, ever."""
from __future__ import annotations

import json
from pathlib import Path

from pipeline.deliver.release import publish as PUB


def _release(root: Path) -> Path:
    v = root / "tiles" / "d768d6e135e62b99.aaaaaaaaaaaa"
    (v / "map-assets-1" / "fonts").mkdir(parents=True)
    (v / "atlas-d768d6e135e62b99.aaaaaaaaaaaa.pmtiles").write_bytes(b"pm")
    (v / "bc_outside.geojson").write_text("{}")
    (v / "map-assets-1" / "fonts" / "0-255.pbf").write_bytes(b"g")
    r = root / "r-x"
    r.mkdir()
    (r / "core-abc.sqlite.gz").write_bytes(b"c")
    (r / "manifest.json").write_text(json.dumps({
        "tiles": {"current": {"id": "d768d6e135e62b99.aaaaaaaaaaaa"}},
        "files": {"core": {"url": "r-x/core-abc.sqlite.gz"}}}))
    return r


def test_the_manifest_goes_last_and_tiles_first(tmp_path):
    puts = PUB.plan(_release(tmp_path), tmp_path)
    keys = [p.key for p in puts]
    assert keys[-1] == "manifest.json" and puts[-1].cache == "no-cache"
    first_data = keys.index("r-x/core-abc.sqlite.gz")
    assert all(k.startswith("tiles/") for k in keys[:first_data]) and len(keys[:first_data]) == 3
    assert puts[first_data].encoding == "gzip" and "immutable" in puts[first_data].cache


def test_without_yes_nothing_is_sent(tmp_path, monkeypatch, capsys):
    r = _release(tmp_path)
    monkeypatch.setattr(PUB, "GENERATED", type("G", (), {"release": tmp_path})())
    assert PUB.main(["--release", str(r)]) == 0
    assert "nothing sent" in capsys.readouterr().err


def test_t994_yes_is_refused_and_nothing_is_sent_or_stamped(tmp_path, monkeypatch, capsys):
    """(T994 #1) `--yes` sent the plan to whatever R2_BUCKET named — production included — past every deploy gate, and
    re-stamped the local manifest. It is refused with the one release path; no client is made, nothing is written.
    Mutation: put the boto3 upload back behind --yes -> fails (a client is made, PUBLISHED written, manifest stamped)."""
    import sys
    r = _release(tmp_path)
    before = (r / "manifest.json").read_bytes()
    monkeypatch.setattr(PUB, "GENERATED", type("G", (), {"release": tmp_path})())
    made = []
    monkeypatch.setitem(sys.modules, "boto3", type("B", (), {"client": staticmethod(lambda *a, **k: made.append(a))}))
    for k in ("R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY"):
        monkeypatch.setenv(k, "x")
    monkeypatch.setenv("R2_BUCKET", "bc-fishing-regulations")
    for argv in (["--yes"], ["--yes", "--min-app-version", "0.2.0"], ["--min-app-version", "0.2.0"]):
        assert PUB.main(["--release", str(r), *argv]) == 2
        err = capsys.readouterr().err
        assert "uploads nothing (T994 #1)" in err and "python -m pipeline release --staging" in err
    assert made == []
    assert not (tmp_path / "PUBLISHED").exists()
    assert (r / "manifest.json").read_bytes() == before
