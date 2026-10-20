"""P10: publish uploads the tiles first, the data next and the manifest LAST, and sends nothing without --yes."""
from __future__ import annotations

import json
from pathlib import Path

from pipeline.tools import publish as PUB


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
