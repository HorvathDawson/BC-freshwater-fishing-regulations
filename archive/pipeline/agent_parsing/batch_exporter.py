"""Export pending synopsis rows into batch files for agent parsing.

Reads the shared ``session_state.json`` checkpoint, selects rows that are not
yet ``success`` (i.e. pending *or* failed — matching the Gemini resume logic),
and writes one JSON batch file plus a ready-to-use rendered prompt per batch.
A ``manifest.json`` records the batch layout and a content digest so ingest can
detect data drift.

Usage
-----
    python -m pipeline.agent_parsing.batch_exporter                 # export all pending
    python -m pipeline.agent_parsing.batch_exporter --batch-size 40
    python -m pipeline.agent_parsing.batch_exporter --status        # show progress
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from pipeline.parsing.rows import load_synopsis_rows

from .common import (
    compute_rows_digest,
    default_agent_dir,
    load_session_state,
    resolve_parsing_dir,
)
from .prompt_render import render_batch_prompt


def build_batches(pending: List[int], batch_size: int) -> List[List[int]]:
    """Chunk pending indices into batches of at most ``batch_size``."""
    if batch_size < 1:
        raise ValueError(f"batch_size must be >= 1, got {batch_size}")
    return [
        pending[i : i + batch_size] for i in range(0, len(pending), batch_size)
    ]


def _batch_item(index: int, row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "index": index,
        "water": row.get("water", ""),
        "raw_regs": row.get("raw_regs", ""),
        "symbols": row.get("symbols", []),
        "region": row.get("region"),
    }


def export(
    rows: List[Dict[str, Any]],
    status: List[str],
    out_dir: Path,
    batch_size: int,
) -> Dict[str, Any]:
    """Write batch files + rendered prompts + manifest.  Returns the manifest."""
    batches_dir = out_dir / "batches"
    batches_dir.mkdir(parents=True, exist_ok=True)

    # Clear stale batch files so a re-export (e.g. at a different batch size)
    # never leaves orphaned batches that desync from the manifest.
    for stale in batches_dir.glob("batch_*"):
        stale.unlink()

    digest = compute_rows_digest(rows)

    # Not-yet-success rows.  Rows with empty raw_regs can never satisfy the
    # verbatim echo check, so they are surfaced loudly and excluded rather than
    # silently shipped to an agent that cannot parse them.
    pending: List[int] = []
    excluded_empty: List[int] = []
    for i, s in enumerate(status):
        if s == "success":
            continue
        if not rows[i].get("raw_regs", "").strip():
            excluded_empty.append(i)
        else:
            pending.append(i)

    if excluded_empty:
        print(
            f"  WARNING: {len(excluded_empty)} pending rows have empty raw_regs "
            f"and were excluded (indices: {excluded_empty[:20]}"
            f"{'...' if len(excluded_empty) > 20 else ''})"
        )

    batch_index_groups = build_batches(pending, batch_size)
    manifest_batches: List[Dict[str, Any]] = []

    for b, indices in enumerate(batch_index_groups):
        items = [_batch_item(i, rows[i]) for i in indices]
        batch_file = batches_dir / f"batch_{b:03d}.json"
        prompt_file = batches_dir / f"batch_{b:03d}.prompt.txt"

        with open(batch_file, "w", encoding="utf-8") as f:
            json.dump(
                {"batch": b, "rows_digest": digest, "indices": indices, "items": items},
                f,
                ensure_ascii=False,
                indent=2,
            )
        prompt_file.write_text(render_batch_prompt(items), encoding="utf-8")

        manifest_batches.append(
            {
                "id": b,
                "file": str(batch_file.relative_to(out_dir)),
                "prompt": str(prompt_file.relative_to(out_dir)),
                "indices": indices,
                "count": len(indices),
                "state": "exported",
            }
        )

    manifest = {
        "created_at": datetime.now().isoformat(),
        "total_rows": len(rows),
        "rows_digest": digest,
        "batch_size": batch_size,
        "pending_count": len(pending),
        "excluded_empty": excluded_empty,
        "batches": manifest_batches,
    }
    with open(out_dir / "manifest.json", "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    return manifest


def _print_status(out_dir: Path, status: List[str]) -> None:
    manifest_file = out_dir / "manifest.json"
    if not manifest_file.exists():
        print(f"No manifest at {manifest_file}. Run the exporter first.")
        return
    with open(manifest_file, encoding="utf-8") as f:
        manifest = json.load(f)

    print(f"Manifest: {manifest_file}")
    print(f"  created_at : {manifest['created_at']}")
    print(f"  batch_size : {manifest['batch_size']}")
    print(f"  pending    : {manifest['pending_count']} rows across "
          f"{len(manifest['batches'])} batches")
    print(f"  {'batch':<8}{'rows':<7}{'done':<7}{'state':<12}")
    for b in manifest["batches"]:
        done = sum(1 for i in b["indices"] if status[i] == "success")
        print(f"  {b['id']:<8}{b['count']:<7}{done:<7}{b['state']:<12}")
    overall = {
        "success": status.count("success"),
        "failed": status.count("failed"),
        "pending": status.count("pending"),
        "total": len(status),
    }
    print(f"  session: {overall}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export pending synopsis rows into agent-parsing batches."
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=40,
        help="Rows per batch (default 40; raise with large-context models).",
    )
    parser.add_argument("--raw", help="Path to synopsis_raw_data.json (optional).")
    parser.add_argument(
        "--session-dir",
        help="Session checkpoint dir (default: the shared parsing output dir). "
        "Point at an isolated dir to run without touching the live session.",
    )
    parser.add_argument(
        "--out-dir",
        help="Agent-parsing working dir (default: output/pipeline/agent_parsing).",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Print batch/session progress instead of exporting.",
    )
    args = parser.parse_args()

    raw_path = Path(args.raw) if args.raw else None
    rows = load_synopsis_rows(raw_path)

    session_dir = Path(args.session_dir) if args.session_dir else resolve_parsing_dir()
    state = load_session_state(session_dir, expected_total=len(rows))
    status: List[str] = state["status"]

    out_dir = Path(args.out_dir) if args.out_dir else default_agent_dir()
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.status:
        _print_status(out_dir, status)
        return

    manifest = export(rows, status, out_dir, args.batch_size)
    print(
        f"Exported {manifest['pending_count']} pending rows into "
        f"{len(manifest['batches'])} batches → {out_dir / 'batches'}"
    )
    print(f"Manifest: {out_dir / 'manifest.json'}")


if __name__ == "__main__":
    main()
