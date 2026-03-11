#!/usr/bin/env python
"""
Overnight pipeline runner — runs metadata_builder then regulation_pipeline
in sequence, with aggressive garbage collection between stages to avoid OOM.

Usage:
    conda activate fish
    python scripts/run_pipeline.py [--include-zones] [--upload] [--verbose]

Stages:
    1. metadata_builder   — reads GPKG + graph, writes metadata pickle
    2. regulation_pipeline — reads metadata + regulations, exports GPKG/PMTiles/JSON

Each stage runs as a subprocess so all memory is fully reclaimed between them.
"""

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# Always run from project root (parent of scripts/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
os.chdir(PROJECT_ROOT)


def _banner(msg: str) -> None:
    print(f"\n{'=' * 80}")
    print(f"  {msg}")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 80}\n")


def _run_stage(label: str, cmd: list[str]) -> bool:
    """Run a pipeline stage as a subprocess. Returns True on success."""
    _banner(f"STAGE: {label}")
    print(f"  Command: {' '.join(cmd)}\n")
    t0 = time.monotonic()
    result = subprocess.run(cmd)
    elapsed = time.monotonic() - t0
    mins, secs = divmod(int(elapsed), 60)
    hours, mins = divmod(mins, 60)
    time_str = f"{hours}h {mins}m {secs}s" if hours else f"{mins}m {secs}s"

    if result.returncode != 0:
        print(f"\n  ❌ {label} FAILED (exit code {result.returncode}) after {time_str}")
        return False
    print(f"\n  ✅ {label} completed in {time_str}")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the full BC fishing regulations pipeline overnight.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--include-zones",
        action="store_true",
        help="Include zone-level default regulations (passed to regulation_pipeline)",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Upload results to R2 after completion (passed to regulation_pipeline)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show detailed statistics (passed to regulation_pipeline)",
    )
    parser.add_argument(
        "--skip-metadata",
        action="store_true",
        help="Skip metadata_builder (assume pickle is already fresh)",
    )
    args = parser.parse_args()

    overall_t0 = time.monotonic()
    _banner("OVERNIGHT PIPELINE RUNNER — START")

    python = sys.executable  # use the same interpreter (conda env)

    # ── Stage 1: Metadata Builder ──────────────────────────────────
    if not args.skip_metadata:
        ok = _run_stage(
            "Metadata Builder",
            [python, "-m", "fwa_pipeline.metadata_builder"],
        )
        if not ok:
            return 1
    else:
        print("  ⏭  Skipping metadata_builder (--skip-metadata)")

    # ── Stage 2: Regulation Pipeline ───────────────────────────────
    reg_cmd = [python, "-m", "regulation_mapping.regulation_pipeline"]
    if args.include_zones:
        reg_cmd.append("--include-zones")
    if args.upload:
        reg_cmd.append("--upload")
    if args.verbose:
        reg_cmd.append("--verbose")

    ok = _run_stage("Regulation Pipeline", reg_cmd)
    if not ok:
        return 1

    # ── Summary ────────────────────────────────────────────────────
    elapsed = time.monotonic() - overall_t0
    mins, secs = divmod(int(elapsed), 60)
    hours, mins = divmod(mins, 60)
    time_str = f"{hours}h {mins}m {secs}s" if hours else f"{mins}m {secs}s"
    _banner(f"ALL STAGES COMPLETE — Total: {time_str}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
