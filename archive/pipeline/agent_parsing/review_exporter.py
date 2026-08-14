"""Render a second-pass REVIEW prompt for a batch's agent output.

The parsing workflow dispatches each ``batch_NNN.prompt.txt`` to a subagent and
saves its reply to ``responses/batch_NNN.json``.  This tool pairs that reply
with the batch's rows and emits ``batches/batch_NNN.review.prompt.txt`` — a
self-contained prompt for an independent *reviewer* subagent that audits the
candidate output for structural validity (the same ``.validate()`` gate ingest
runs), content correctness (the spirit of the validators), and cross-row
consistency, then reports issues for the parsing agent to fix.

Nothing here re-encodes the parsing rules: the reviewer prompt is built from the
same canonical ``prompt.txt`` + ``examples.json`` as the parse prompt, so the
two passes can never drift.

Usage
-----
    python -m pipeline.agent_parsing.review_exporter \\
        output/pipeline/agent_parsing/responses/batch_000.json
    python -m pipeline.agent_parsing.review_exporter \\
        output/pipeline/agent_parsing/responses/*.json --out-dir <dir>
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List

from .common import default_agent_dir
from .ingest import parse_response_text
from .prompt_render import render_review_prompt


def _load_batch_items(batch_file: Path) -> List[Dict[str, Any]]:
    """Load the ``items`` list a batch file was exported with."""
    with open(batch_file, encoding="utf-8") as f:
        batch = json.load(f)
    items = batch.get("items")
    if not isinstance(items, list) or not items:
        raise ValueError(f"{batch_file} has no 'items' array to review")
    return items


def render_review_for_response(
    response_file: Path, out_dir: Path
) -> Path:
    """Render one review prompt for ``response_file``.

    Locates the sibling ``batches/<stem>.json`` for the row inputs, pairs it
    with the candidate output, and writes ``batches/<stem>.review.prompt.txt``.
    Returns the path written.
    """
    stem = response_file.stem  # e.g. "batch_000"
    batch_file = out_dir / "batches" / f"{stem}.json"
    if not batch_file.exists():
        raise FileNotFoundError(
            f"No batch file for {response_file.name} at {batch_file} — "
            f"review needs the original rows. Re-export or fix --out-dir."
        )

    items = _load_batch_items(batch_file)
    results = parse_response_text(response_file.read_text(encoding="utf-8"))

    prompt = render_review_prompt(items, results)
    review_file = out_dir / "batches" / f"{stem}.review.prompt.txt"
    review_file.write_text(prompt, encoding="utf-8")
    return review_file


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Render review prompts for agent-parsing batch responses."
    )
    parser.add_argument(
        "responses",
        nargs="+",
        help="Response JSON file(s) (responses/batch_NNN.json).",
    )
    parser.add_argument(
        "--out-dir",
        help="Agent-parsing working dir holding batches/ and responses/ "
        "(default: output/pipeline/agent_parsing).",
    )
    args = parser.parse_args()

    out_dir = Path(args.out_dir) if args.out_dir else default_agent_dir()

    written: List[Path] = []
    for r in args.responses:
        review_file = render_review_for_response(Path(r), out_dir)
        written.append(review_file)
        print(f"  {review_file}")

    print(
        f"Rendered {len(written)} review prompt(s). Dispatch each to a reviewer "
        f"subagent; apply its reported fixes, then re-ingest."
    )


if __name__ == "__main__":
    main()
