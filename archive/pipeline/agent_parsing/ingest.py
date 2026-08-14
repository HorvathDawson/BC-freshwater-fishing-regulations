"""Ingest agent responses back into the shared session checkpoint.

Takes the JSON produced by subagents for one or more exported batches,
validates every entry through the *same* ``ParsedEntry`` schema and verbatim
echo check the Gemini parser uses, and applies the successes to the shared
``session_state.json``.  Nothing is applied unless it validates; failures are
reported loudly and left pending for a re-run.

Usage
-----
    python -m pipeline.agent_parsing.ingest responses/batch_000.json --dry-run
    python -m pipeline.agent_parsing.ingest responses/batch_000.json
    python -m pipeline.agent_parsing.ingest responses/*.json --finalize
"""

from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pipeline.parsing.models import ParsedEntry, validate_batch
from pipeline.parsing.rows import load_synopsis_rows
from pipeline.parsing.session import ParsingSession

from .common import (
    compute_rows_digest,
    default_agent_dir,
    load_session_state,
    resolve_parsing_dir,
)


def parse_response_text(text: str) -> List[Dict[str, Any]]:
    """Parse an agent response into a list of ``{index, entry}`` objects.

    Strips a single optional Markdown code fence deterministically, then does a
    strict ``json.loads``.  No silent repair — malformed JSON raises so the
    operator sees exactly what failed.
    """
    stripped = text.strip()
    if stripped.startswith("```"):
        # Drop the opening fence line (```json / ```) and the trailing fence.
        lines = stripped.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        stripped = "\n".join(lines).strip()

    data = json.loads(stripped)
    if not isinstance(data, list):
        raise ValueError(
            f"Expected a JSON array of {{index, entry}} objects, got "
            f"{type(data).__name__}."
        )
    return data


def validate_entry(
    index: int, entry: Dict[str, Any], row: Dict[str, Any]
) -> Tuple[Optional[ParsedEntry], List[str]]:
    """Validate one entry against the row. Returns (ParsedEntry|None, errors)."""
    errors: List[str] = []

    raw_regs = row.get("raw_regs", "")
    if entry.get("regs_verbatim") != raw_regs:
        errors.append(
            f"index {index}: regs_verbatim does not exactly echo raw_regs"
        )
        return None, errors

    try:
        parsed = ParsedEntry.model_validate(entry)
    except Exception as exc:  # pydantic ValidationError and friends
        errors.append(f"index {index}: schema validation failed: {exc}")
        return None, errors

    # Warn-level structural checks (symbol/tributary consistency).
    batch_errors = validate_batch([parsed], [row])
    for err in batch_errors:
        print(f"  WARN index {index}: {err}")

    return parsed, errors


def ingest_responses(
    response_texts: List[str],
    rows: List[Dict[str, Any]],
    status: List[str],
    exported_indices: Optional[set],
    force: bool,
) -> Tuple[Dict[int, ParsedEntry], Dict[str, Any]]:
    """Validate all responses. Returns (results_map, report).

    ``results_map`` contains only successfully validated entries (index →
    ParsedEntry).  Failures are recorded in ``report`` and NOT applied.
    """
    report: Dict[str, Any] = {
        "accepted": [],
        "failed": [],
        "skipped_success": [],
        "duplicates": [],
        "out_of_range": [],
        "not_exported": [],
    }
    results_map: Dict[int, ParsedEntry] = {}
    seen: set = set()

    for text in response_texts:
        objects = parse_response_text(text)
        for obj in objects:
            index = obj.get("index")
            entry = obj.get("entry")
            if not isinstance(index, int):
                report["failed"].append(
                    {"index": index, "error": "missing/invalid integer 'index'"}
                )
                continue
            if index < 0 or index >= len(rows):
                report["out_of_range"].append(index)
                continue
            if index in seen:
                report["duplicates"].append(index)
                continue
            seen.add(index)

            if exported_indices is not None and index not in exported_indices:
                report["not_exported"].append(index)
                # still allow, but flag — operator may be ingesting a re-run
            if status[index] == "success" and not force:
                report["skipped_success"].append(index)
                continue

            parsed, errors = validate_entry(index, entry, rows[index])
            if parsed is None:
                report["failed"].append({"index": index, "error": errors})
                continue
            results_map[index] = parsed
            report["accepted"].append(index)

    return results_map, report


def _load_manifest_digest(out_dir: Path) -> Tuple[Optional[str], Optional[set]]:
    manifest_file = out_dir / "manifest.json"
    if not manifest_file.exists():
        return None, None
    with open(manifest_file, encoding="utf-8") as f:
        manifest = json.load(f)
    exported: set = set()
    for b in manifest.get("batches", []):
        exported.update(b.get("indices", []))
    return manifest.get("rows_digest"), exported


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest agent responses into the shared session checkpoint."
    )
    parser.add_argument("responses", nargs="+", help="Response JSON file(s).")
    parser.add_argument("--raw", help="Path to synopsis_raw_data.json (optional).")
    parser.add_argument(
        "--session-dir",
        help="Session checkpoint dir (default: the shared parsing output dir). "
        "Point at an isolated dir to run without touching the live session.",
    )
    parser.add_argument(
        "--out-dir",
        help="Agent-parsing working dir holding manifest.json (default: "
        "output/pipeline/agent_parsing).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and report only; write nothing.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite rows already marked success (default: skip them).",
    )
    parser.add_argument(
        "--finalize",
        action="store_true",
        help="After applying, write synopsis_parsed.json.",
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="Skip backing up session_state.json before writing.",
    )
    args = parser.parse_args()

    raw_path = Path(args.raw) if args.raw else None
    rows = load_synopsis_rows(raw_path)

    out_dir = Path(args.out_dir) if args.out_dir else default_agent_dir()

    # Data-drift guard: the manifest digest must still match the current rows.
    manifest_digest, exported_indices = _load_manifest_digest(out_dir)
    current_digest = compute_rows_digest(rows)
    if manifest_digest is None:
        print(
            f"  WARNING: no manifest.json in {out_dir}; cannot verify data "
            f"drift or exported-index set."
        )
    elif manifest_digest != current_digest:
        raise SystemExit(
            "ABORT: synopsis data changed since export (rows_digest mismatch). "
            "Re-export batches before ingesting."
        )

    session_dir = Path(args.session_dir) if args.session_dir else resolve_parsing_dir()
    state = load_session_state(session_dir, expected_total=len(rows))
    status: List[str] = state["status"]

    response_texts = [
        Path(p).read_text(encoding="utf-8") for p in args.responses
    ]

    results_map, report = ingest_responses(
        response_texts, rows, status, exported_indices, args.force
    )

    print("Ingest summary:")
    print(f"  accepted        : {len(report['accepted'])}")
    print(f"  failed          : {len(report['failed'])}")
    print(f"  skipped success : {len(report['skipped_success'])}")
    if report["duplicates"]:
        print(f"  DUPLICATE index : {report['duplicates']}")
    if report["out_of_range"]:
        print(f"  OUT OF RANGE    : {report['out_of_range']}")
    if report["not_exported"]:
        print(f"  not in manifest : {report['not_exported']}")
    for f in report["failed"]:
        print(f"    FAILED {f['index']}: {f['error']}")

    if args.dry_run:
        print("Dry run — nothing written.")
        return

    if not results_map:
        print("No valid entries to apply.")
        return

    session_file = session_dir / "session_state.json"
    if not args.no_backup:
        ts = datetime.now().strftime("%Y%m%dT%H%M%S")
        backup = session_dir / f"session_state.backup-{ts}.json"
        shutil.copy2(session_file, backup)
        print(f"  Backed up session → {backup}")

    session = ParsingSession(session_dir=session_dir, total_rows=len(rows))
    session.record_indexed(results_map)
    print(f"  Applied {len(results_map)} entries. Session: {session.summary()}")

    if args.finalize:
        out_path = session_dir / "synopsis_parsed.json"
        session.finalize(out_path)
        print(f"  Finalized → {out_path}")


if __name__ == "__main__":
    main()
