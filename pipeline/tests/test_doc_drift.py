"""DOC DRIFT (T561 items 4, 5, 6, 7): claims in pipeline comments and docs that the code proved false, held so they do
not come back. Each is a grep over the source, seconds, no data."""
from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _grep(pattern: str, *paths: str) -> set:
    r = subprocess.run(["git", "grep", "-l", "-i", "-E", pattern, "--", *paths], cwd=ROOT, capture_output=True,
                       text=True)
    assert r.returncode in (0, 1), r.stderr
    return set(r.stdout.split())


#: files another lane owns that still carry the claim — a ratchet: remove a line when its file is fixed
NOT_DETERMINISTIC_PENDING = {"pipeline/regs/review/gate.py"}


def test_no_comment_says_the_atlas_build_is_not_deterministic():
    """Item 4: T65 (N12, 2026-11-04) proved two atlas builds identical but for timings, build_ts and pickled set order.
    A promoted atlas is immutable because its derivatives are keyed by its handle digest, not because the build is
    random."""
    hits = _grep(r"(atlas|build)[^.]{0,40}not deterministic|atlas-build-not-deterministic", "pipeline",
                 ":!pipeline/tests")
    assert hits == NOT_DETERMINISTIC_PENDING, (
        f"stale 'not deterministic' claims: {sorted(hits - NOT_DETERMINISTIC_PENDING)}; "
        f"fixed (drop from the pending list): {sorted(NOT_DETERMINISTIC_PENDING - hits)}")


def test_reach_builder_doc_lists_the_modules_and_reasons_that_exist():
    """Item 5: REACH-BUILDER.md opened '**Not built.**', listed a cache as load-bearing and a reason set of its own.
    Its Parts tree is the package's modules, its reason block is `models.Reason`, both exactly."""
    import re

    from pipeline.atlas.reach.models import Reason
    doc = (ROOT / "pipeline/docs/REACH-BUILDER.md").read_text(encoding="utf-8")
    assert "Not built" not in doc
    parts = doc.split("## Parts", 1)[1].split("```")[1]
    listed = set(re.findall(r"^\s+(\w+\.py)\b", parts, re.M))
    have = {p.name for p in (ROOT / "pipeline/atlas/reach").glob("*.py") if p.name != "__init__.py"}
    assert listed == have, f"doc lists {sorted(listed - have)} that do not exist; misses {sorted(have - listed)}"
    block = doc.split("## Reason vocabulary", 1)[1].split("```")[1]
    assert set(block.split()) == {r.value for r in Reason}


def test_no_gauge_doc_names_the_retired_paths_or_the_old_feed():
    """Item 6: the frozen match is `data/curated/gauges/matches.json` (never `pipeline/gauge_match.json`), the roster
    is under `data/source/`, and the live-data doc describes the feeds Worker, not a Python "hydro job" holding a
    325 KB envelope (clim.json is 8.9 MB for 1,959 stations)."""
    hits = _grep(r"pipeline/gauge_match\.json|[^/]data/bc_hydrometric_stations\.json|hydro job|about 325 KB",
                 "pipeline", ":!pipeline/tests")
    assert not hits, f"stale gauge paths or feed description in {sorted(hits)}"
    assert not (ROOT / "pipeline/atlas/gauges").exists()

