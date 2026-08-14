#!/usr/bin/env python3
"""gauge_matches.py — build the static gauge→reach matching artifact.

Produced ONCE during the full pipeline run (enrich), NOT by the cron. The cron
only fetches + shapes per-station data keyed by ``station_id``; this decouples
the (atlas-time, geopandas) match from the (frequent, stateless) fetch.

Reads ``gauge_fwa_match`` (written by match_fwa.py into the local hydro.db) and
resolves each stream gauge's nearest segment ``fid`` → ``reach_id`` against the
deploy fid shards (``shards/v{N}/fids/{sha3}.json``; sha3 = first 3 hex chars of
SHA-256(fid), 4096 buckets). Emits::

    gauge_matches.json
    {
      "reach":     { "<reach_id>": ["<station_id>", ...], ... },   # stream gauges
      "waterbody": { "<waterbody_key>": ["<station_id>", ...], ... } # lake/polygon gauges
    }

The webapp loads this small file and, on info-panel open, maps a reach_id (and/or
waterbody_key) → station_ids, then fetches the per-station cron artifacts.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Optional

STREAM_LAYERS = ("streams", "under_lake_streams")


class _FidReachResolver:
    """fid → reach_id via ``shards/v{N}/fids/{sha3}.json`` (cached per bucket)."""

    def __init__(self, shard_root: Path):
        self._fids_dir = shard_root / "fids"
        self._cache: dict[str, dict] = {}

    @property
    def available(self) -> bool:
        return self._fids_dir.is_dir()

    @staticmethod
    def _prefix(fid: str) -> str:
        return hashlib.sha256(fid.encode()).hexdigest()[:3]

    def reach_id(self, fid: str) -> Optional[str]:
        prefix = self._prefix(fid)
        if prefix not in self._cache:
            path = self._fids_dir / f"{prefix}.json"
            self._cache[prefix] = (
                json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
            )
        return self._cache[prefix].get(fid)


def build_gauge_matches(hydro_db: Path, shard_root: Optional[Path]) -> dict:
    """Resolve gauge_fwa_match (primary matches) into the reach/waterbody index.

    Stream gauges resolve their nearest segment fid → reach_id against the deploy
    shards; polygon/lake gauges join by waterbody_key. Returns the artifact dict
    (empty buckets if gauge_fwa_match is absent).
    """
    reach: dict[str, list] = defaultdict(list)
    waterbody: dict[str, list] = defaultdict(list)

    conn = sqlite3.connect(f"file:{hydro_db}?mode=ro", uri=True)
    try:
        has = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='gauge_fwa_match'"
        ).fetchone()
        if not has:
            return {"reach": {}, "waterbody": {}}

        resolver = _FidReachResolver(shard_root) if shard_root else None
        if resolver and not resolver.available:
            print(f"WARNING: fid shards not found under {shard_root} — "
                  "stream gauges will be unresolved")
            resolver = None

        n_stream = n_reach = 0
        for sid, layer, fwa_id, wbk in conn.execute(
            """SELECT station_id, layer, fwa_id, waterbody_key
               FROM gauge_fwa_match WHERE is_primary=1"""
        ):
            if layer in STREAM_LAYERS:
                n_stream += 1
                rid = resolver.reach_id(fwa_id) if (resolver and fwa_id) else None
                if rid is not None:
                    reach[rid].append(sid)
                    n_reach += 1
            elif wbk:
                waterbody[str(wbk)].append(sid)
        print(f"gauge_matches: {n_reach}/{n_stream} stream gauges resolved to a reach, "
              f"{len(waterbody)} waterbody keys")
    finally:
        conn.close()

    return {
        "reach": {k: sorted(set(v)) for k, v in reach.items()},
        "waterbody": {k: sorted(set(v)) for k, v in waterbody.items()},
    }


def write_gauge_matches(hydro_db: Path, shard_root: Optional[Path], out_path: Path) -> dict:
    artifact = build_gauge_matches(hydro_db, shard_root)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(artifact, separators=(",", ":")), encoding="utf-8")
    return artifact
