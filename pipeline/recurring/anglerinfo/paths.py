"""
paths.py — single source of truth for this package's on-disk artifacts.

Both live under the pipeline *output* tree (see config.yaml
``output.pipeline.anglerinfo`` / ``output.pipeline.anglerinfo_matches``),
not committed to git:

- ``DB_PATH`` (``anglerinfo.db``) — a regenerable fetch+match artifact
  (built by :mod:`pipeline.recurring.anglerinfo.build_db`, run at data-fetch
  time alongside the other external downloads).
- ``MATCHES_PATH`` (``anglerinfo_matches.json``) — derived from the db by
  :mod:`pipeline.recurring.anglerinfo.export`, regenerated as part of the
  regulations build and read by
  :mod:`pipeline.enrichment.anglerinfo_accessor`.

Previously each fetcher defined its own ``DB_PATH = Path(__file__).parent /
"anglerinfo.db"`` independently (three copies) — centralized here so the
location is defined exactly once and driven by config.
"""

from __future__ import annotations

from pathlib import Path

_FALLBACK_DIR = Path(__file__).resolve().parents[3] / "output" / "pipeline" / "anglerinfo"


def _resolve(config_key: str, filename: str) -> Path:
    """Resolve an output path from config.yaml, falling back to the default
    output/pipeline/anglerinfo/ location if config is unavailable."""
    try:
        from project_config import ProjectConfig

        cfg = ProjectConfig()
        value = cfg.config.get("output", {}).get("pipeline", {}).get(config_key)
        if value:
            p = Path(value)
            return p if p.is_absolute() else cfg.project_root / p
    except Exception:
        pass
    return _FALLBACK_DIR / filename


DB_PATH: Path = _resolve("anglerinfo", "anglerinfo.db")
MATCHES_PATH: Path = _resolve("anglerinfo_matches", "anglerinfo_matches.json")
