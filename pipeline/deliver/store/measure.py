"""S0's measurements: a store's size per table (SQLite's `dbstat`) and whole-file compressed for
download (gzip -9 in Python; brotli -q 11 and zstd -19 through their command-line tools where
installed — the venv has neither library), and `section_akey` alone. Nothing here writes a store
(`__main__ measure --variants DIR` builds the codec variants into DIR first)."""
from __future__ import annotations

import gzip
import shutil
import sqlite3
import subprocess
from pathlib import Path
from typing import Dict, Optional


def compressed(b: bytes) -> Dict[str, Optional[int]]:
    out: Dict[str, Optional[int]] = {"raw": len(b), "gzip9": len(gzip.compress(b, 9, mtime=0))}
    for name, cmd in (("br11", ["brotli", "-c", "-q", "11", "-"]),
                      ("zstd19", ["zstd", "-19", "-c", "-q", "-"])):
        exe = shutil.which(cmd[0])
        out[name] = (len(subprocess.run([exe, *cmd[1:]], input=b, capture_output=True,
                                        check=True).stdout) if exe else None)
    return out


def tables(path: Path) -> Dict[str, int]:
    """Bytes on disk per table (pages, overflow included), largest first."""
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        rows = con.execute("SELECT name, SUM(pgsize) FROM dbstat GROUP BY name "
                           "ORDER BY 2 DESC").fetchall()
    finally:
        con.close()
    return dict(rows)


def section_akey(path: Path) -> Dict[str, Optional[int]]:
    """section_akey's blocks, concatenated in block order, compressed (what shipping it alone,
    beside the index, would cost) — plus how many blocks it holds."""
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        rows = con.execute("SELECT block, b FROM section_akey ORDER BY block").fetchall()
    finally:
        con.close()
    out = compressed(b"".join(b for _, b in rows))
    out["blocks"] = len(rows)
    return out


def measure(path: Path) -> dict:
    path = Path(path)
    return {"file": str(path), "whole": compressed(path.read_bytes()), "tables": tables(path),
            "section_akey": section_akey(path)}
