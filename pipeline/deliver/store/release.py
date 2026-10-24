"""THE PROVINCE STORE, READ FROM THE RELEASE (P20, T61 2.4): no reader needs `regs.sqlite`.

The release ships the regs store whole, only split by reader: `water` holds its common tables, `regs-<region>` the
`cell` rows and frames of the akeys in that region (a row two regions reach is in both), and `regs-sid` the section
maps. `province_store(release_dir)` puts those rows back into the store's own schema (`schema.sql`), in memory, and
hands back the reference decoder (`decode.Store`) over them — the same tables `pipeline.deliver store` wrote, row for
row (`test_release.py` pins it).

The store's own `meta` rides in the water file as `store_meta` (JSON) from release format 2026-10-23 on, so the
reassembled store carries its `store_digest` and is held to it (any missing or altered row refuses). An older release
has no `store_meta`: its meta is rebuilt from the water file's own keys (format, section_handles, reach_digest,
rule_ids_sha256, answers_sha256, layout, blob_codec, about_bundle, card_check) and there is no digest to verify;
`Store.answers_bytes()` still proves the decoded answers against `answers_sha256`.

    from pipeline.deliver.store.release import province_store
    st = province_store()                       # data/release/LATEST
    st = province_store(Path("data/release/r-…"))
"""
from __future__ import annotations

import gzip
import hashlib
import json
import sqlite3
import tempfile
from pathlib import Path
from typing import Optional

from pipeline.deliver.store import FORMAT
from pipeline.deliver.store.common import StoreError, store_digest
from pipeline.deliver.store.decode import Store

#: the water file's meta keys an older release (no `store_meta`) carries over from the store's
WATER_STORE_KEYS = ("about_bundle", "answers_sha256", "blob_codec", "card_check", "layout", "reach_digest",
                    "rule_ids_sha256", "section_handles")


def release_dir(release: Optional[Path] = None) -> Path:
    """`release`, or the release `data/release/LATEST` names (`manifest.latest`); refuses when there is none."""
    from pipeline.deliver.release.manifest import latest
    if release is not None:
        d = Path(release)
        if not (d / "manifest.json").is_file():
            raise FileNotFoundError(f"no release at {d} (no manifest.json)")
        return d
    from pipeline.common.curated import GENERATED
    d = latest(GENERATED.release)
    if d is None:
        raise FileNotFoundError(f"no release under {GENERATED.release} — make one with: python -m pipeline.deliver all")
    return d


def _store_files(manifest: dict) -> list:
    """The release files that hold the store's rows: water, every regs-<region>, regs-sid (manifest order)."""
    names = [n for n in manifest["files"] if n == "water" or n.startswith("regs-")]
    if "water" not in names or "regs-sid" not in names or len(names) < 3:
        raise StoreError(f"release: the manifest lists no regs store files ({sorted(manifest['files'])})")
    return names


def province_store(release: Optional[Path] = None, *, verify: bool = True) -> Store:
    """The regs store `release` (default: the live release) was cut from, reassembled in memory (module doc)."""
    d = release_dir(release)
    manifest = json.loads((d / "manifest.json").read_text(encoding="utf-8"))
    from pipeline.deliver.store.build import SCHEMA
    con = sqlite3.connect(":memory:")
    con.executescript(SCHEMA.read_text())
    tables = [t for (t,) in con.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name <> 'meta'")]
    water_meta: dict = {}
    with tempfile.TemporaryDirectory(prefix="province-store-") as tmp:
        for name in _store_files(manifest):
            f = manifest["files"][name]
            raw = gzip.decompress((d.parent / f["url"]).read_bytes())
            if hashlib.sha256(raw).hexdigest() != f["sha256"]:
                raise StoreError(f"release: {name} is not the file its manifest names (sha256)")
            p = Path(tmp) / f"{name}.sqlite"
            p.write_bytes(raw)
            con.execute("ATTACH ? AS src", (str(p),))
            have = {t for (t,) in con.execute("SELECT name FROM src.sqlite_master WHERE type = 'table'")}
            for t in tables:
                if t in have:
                    cols = ", ".join(c[1] for c in con.execute(f"PRAGMA main.table_info({t})"))
                    con.execute(f"INSERT OR IGNORE INTO main.{t} ({cols}) SELECT {cols} FROM src.{t}")
            if name == "water":
                water_meta = dict(con.execute("SELECT k, v FROM src.meta"))
            con.commit()
            con.execute("DETACH src")
    if "store_meta" in water_meta:
        meta = json.loads(water_meta["store_meta"])
    else:
        meta = {"format": FORMAT, **{k: water_meta[k] for k in WATER_STORE_KEYS if k in water_meta}}
    con.executemany("INSERT INTO meta (k, v) VALUES (?, ?)", sorted(meta.items()))
    con.commit()
    if verify and "store_digest" in meta and store_digest(con) != meta["store_digest"]:
        raise StoreError(f"release {d.name}: the reassembled regs store is not store {meta['store_digest']} "
                         "(a row is missing or altered)")
    return Store(d, verify=False, con=con)
