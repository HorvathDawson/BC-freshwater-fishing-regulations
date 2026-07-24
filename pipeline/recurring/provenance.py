"""Shared provenance block for recurring/cron artifacts.

Every cron output (in-season, stocking, hydro) carries a uniform top-level
provenance block so consumers can tell what produced a file, from where, when,
and under what terms. Keeps attribution and traceability consistent across the
`deploy/cron/` subtree.

Usage:
    from pipeline.recurring.provenance import provenance
    doc = {**provenance(generator="in_season_resolver",
                         source="BC fishing regulations — in-season changes",
                         source_url=SOURCE_URL,
                         attribution=ATTRIB_STRING),
           "changes": ...,
           "stats": ...}
"""

from __future__ import annotations

import os
import subprocess
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, Optional


@lru_cache(maxsize=1)
def _git_sha() -> str:
    """Short git sha of the checkout, or '' outside a repo (e.g. a container)."""
    sha = os.environ.get("GIT_SHA") or os.environ.get("GITHUB_SHA")
    if sha:
        return sha[:12]
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return out.stdout.strip() if out.returncode == 0 else ""
    except Exception:
        return ""


def provenance(
    *,
    generator: str,
    source: str,
    source_url: Optional[str] = None,
    attribution: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """A standard provenance block to spread into a cron artifact's top level.

    Args:
        generator: the module/job that produced the file (e.g. "export_hydro").
        source: human-readable upstream data source.
        source_url: canonical upstream URL, if any.
        attribution: verbatim required attribution/licence string, if any.
        extra: any additional job-specific provenance fields.

    Returns a dict with: generated_at (UTC ISO 8601), generator (module + git sha),
    source, source_url, deploy_env, attribution, plus `extra`.
    """
    block: Dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generator": f"{generator}@{_git_sha()}" if _git_sha() else generator,
        "source": source,
        "deploy_env": os.environ.get("DEPLOY_ENV", "local"),
    }
    if source_url is not None:
        block["source_url"] = source_url
    if attribution is not None:
        block["attribution"] = attribution
    if extra:
        block.update(extra)
    return block
