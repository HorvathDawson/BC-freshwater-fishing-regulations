"""Artifact IO + partial-rerun cache detection (05).

Pickle + a sibling *.meta.json for the first validatable build (GeoParquet per docs/09 is a
later optimization). Visual inspection is via the GPKG exporter (export_gpkg.py), not GeoJSON.
"""

from __future__ import annotations

import json
import pickle
import time
from pathlib import Path
from typing import Any


def write_artifact(obj: Any, path: str, input_hashes: dict[str, str] | None = None) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "wb") as f:
        pickle.dump(obj, f, protocol=pickle.HIGHEST_PROTOCOL)
    meta = {"input_hashes": input_hashes or {}, "build_ts": time.time()}
    p.with_suffix(p.suffix + ".meta.json").write_text(json.dumps(meta, indent=2))


def read_artifact(path: str) -> Any:
    with open(path, "rb") as f:
        return pickle.load(f)


def is_stale(path: str, input_hashes: dict[str, str]) -> bool:
    """True if the artifact is missing or its recorded input hashes differ."""
    meta_path = Path(path).with_suffix(Path(path).suffix + ".meta.json")
    if not Path(path).exists() or not meta_path.exists():
        return True
    recorded = json.loads(meta_path.read_text()).get("input_hashes", {})
    return recorded != input_hashes
