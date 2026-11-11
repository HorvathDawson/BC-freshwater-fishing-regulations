"""DOC DRIFT (T561 items 4, 6, 7): claims in pipeline comments and docs that the code proved false, held so they do
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
