"""The release on disk, read the way the app reads it (T853).

Everything a map says comes from the release's own files, each proved against the sha256 its manifest names:

  core     item names, kinds, aliases, the sections of each water (`item_section`), its regions (`item_area`)
  water    parts (`part`, `part_section`, `part_line`), their break marks (`part_mark`), which water each flows into
           (`water_exit`) and goes on as (`water_continue`)
  geo      each part's box and middle (`part_geo`)
  regs     the answers and the export subset, reassembled by `pipeline.deliver.store.release.province_store` — the
           page's own lookup is `pipeline.deliver.answers.encode.tap` over them
  salmon   the DFO answers per chain and day (`pipeline.deliver.salmon.decode_files`, the reference reader)

Nothing here decides a rule; it only opens files.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import sqlite3
import tempfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple


class ReleaseError(SystemExit):
    """A release (or tile layer set) this tool cannot read as asked: says what is missing."""


def release_dir(path: Optional[Path]) -> Path:
    """A release folder (holds manifest.json), or a release ROOT whose `LATEST` names one; default the repo's
    `data/release` (its LATEST)."""
    from pipeline.deliver.release.manifest import latest
    if path is None:
        from pipeline.common.curated import GENERATED
        path = GENERATED.release
    p = Path(path)
    if (p / "manifest.json").is_file():
        return p
    if p.is_file() and p.name == "LATEST":
        p = p.parent
    d = latest(p) if p.is_dir() else None
    if d is None:
        raise ReleaseError(f"water_map: no release at {p} (no manifest.json, no LATEST)")
    return d


class Release:
    """One release folder. Files are decompressed once into a temp folder (kept until `close`)."""

    def __init__(self, path: Optional[Path] = None):
        self.dir = release_dir(path)
        self.manifest = json.loads((self.dir / "manifest.json").read_text(encoding="utf-8"))
        self.id = self.manifest.get("data_release") or self.dir.name
        self._tmp = tempfile.TemporaryDirectory(prefix="water-map-")
        self._con: Dict[str, sqlite3.Connection] = {}
        self._answers: Optional[Tuple[dict, dict]] = None
        self._salmon = None

    def close(self) -> None:
        for c in self._con.values():
            c.close()
        self._con.clear()
        self._tmp.cleanup()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ---- files
    def has(self, name: str) -> bool:
        return name in (self.manifest.get("files") or {})

    def raw(self, name: str) -> bytes:
        f = (self.manifest.get("files") or {}).get(name)
        if f is None:
            raise ReleaseError(f"water_map: release {self.id} has no `{name}` file")
        blob = (self.dir.parent / f["url"]).read_bytes()
        raw = gzip.decompress(blob) if f["url"].endswith(".gz") else blob
        if hashlib.sha256(raw).hexdigest() != f["sha256"]:
            raise ReleaseError(f"water_map: {f['url']} is not the file release {self.id} names (sha256)")
        return raw

    def db(self, name: str) -> sqlite3.Connection:
        """A release SQLite file (`core`, `water`, `geo`), read-only."""
        if name not in self._con:
            p = Path(self._tmp.name) / f"{name}.sqlite"
            p.write_bytes(self.raw(name))
            self._con[name] = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
        return self._con[name]

    # ---- the regs answers (BC)
    def answers(self) -> Tuple[dict, dict]:
        """(wire, data): the answers file and the export subset the release was cut from — the inputs of the page's
        lookup (`encode.tap`)."""
        if self._answers is None:
            from pipeline.deliver.store.release import province_store
            st = province_store(self.dir)
            try:
                self._answers = (st.answers(), st.export_subset())
            finally:
                st.close()
        return self._answers

    # ---- DFO salmon
    def salmon(self) -> Optional[Tuple[dict, Dict[int, int]]]:
        """(payload, {section handle: chain}) or None when the release ships no salmon file (the committed regs
        fixture). A section in no run is chain `payload.default` (UNLISTED)."""
        if self._salmon is None:
            if not self.has("salmon"):
                self._salmon = False
            else:
                from pipeline.deliver import salmon as SA
                files = {k: self.raw(k) for k in self.manifest["files"] if k == "salmon" or k.startswith("salmon-")}
                _, payload, secs = SA.decode_files(files)
                self._salmon = (payload, dict(secs))
        return self._salmon or None

    # ---- tiles
    def tile_version(self) -> Optional[str]:
        cur = (self.manifest.get("tiles") or {}).get("current") or {}
        return cur.get("id")

    def handles(self) -> Optional[str]:
        comp = (self.manifest.get("tiles") or {}).get("compatible") or []
        return comp[0].split(".")[0] if comp else None


def layers_dir(path: Optional[Path], release: Release) -> Path:
    """The tile build's `layers/*.geojsonl` (default `data/generated/tiles/layers`). Refuses a tile build of another
    section-handle space than the release's (`tiles-bundle-same-vintage`): its section ids would name other sections."""
    if path is None:
        from pipeline.common.curated import GENERATED
        path = Path(GENERATED.tiles) / "layers"
    d = Path(path)
    if not (d / "stream.geojsonl").is_file():
        raise ReleaseError(f"water_map: no tile layers at {d} (stream.geojsonl) — build the tiles "
                           "(python -m pipeline build) or pass --layers")
    meta = d.parent / "atlas.meta.json"
    if meta.is_file():
        m = json.loads(meta.read_text(encoding="utf-8"))
        want = release.handles()
        if want and m.get("section_handles") != want:
            raise ReleaseError(f"water_map: tile layers {d} are section handles {m.get('section_handles')}, release "
                               f"{release.id} is {want} — a tile build of another atlas")
    return d


def tile_vintage_note(path: Path, release: Release) -> List[str]:
    """Words for the image's footer when the layers are another tile build of the same atlas than the release names."""
    meta = Path(path).parent / "atlas.meta.json"
    if not meta.is_file():
        return ["tile layers carry no atlas.meta.json (vintage unchecked)"]
    v = json.loads(meta.read_text(encoding="utf-8")).get("tile_version")
    want = release.tile_version()
    if want and v != want:
        return [f"lines from tile build {v}; the release names {want}"]
    return []
