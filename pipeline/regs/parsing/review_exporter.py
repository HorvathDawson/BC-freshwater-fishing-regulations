"""Render an independent-review prompt for a parsed batch — the second-pass reviewer subagent.

The prompt is CATALOGUE_REVIEW_PROMPT.md (the checklist), then, per row: the item's bindable
boundaries and printed regs (from the batch file) and the entry the parser produced (from the
response), then the output envelope. The reviewer reports findings; it does not rewrite.
Self-contained (batch + response only).

ONE OUTPUT CONTRACT, and it is `_ENVELOPE` below: one object per BATCH, each issue naming its row
by `index`. `dispatch` stores it as `reviews/batch_NNN.review.json`, and
`io.read_review_findings` joins each `index` back to the batch item's `entry_id`. The checklist
itself says nothing about output, so the two cannot disagree again."""

from __future__ import annotations

import json
from pathlib import Path

from pipeline.regs.parsing.parse_context import render_boundary_menu

_REVIEW_PROMPT = Path(__file__).resolve().parent / "prompts" / "CATALOGUE_REVIEW_PROMPT.md"

_ENVELOPE = """\
---

# OUTPUT (STRICT)

Return ONLY this JSON object (no prose, no fences):

    { "verdict": "pass" | "changes_requested",
      "issues": [ { "index": <the row's index, unchanged>,
                    "severity": "high" | "medium" | "low",
                    "problem": "<what is wrong — quote the printed text you compared against>",
                    "fix": "<the concrete correction>" } ] }

- `high`: a Fatal check failed. `medium`: a Serious one. Both send the row to a stronger model
  for a re-parse, with your `problem` and `fix` as its instructions. `low`: a nit, not re-parsed.
- `changes_requested` if any issue is `high` or `medium`; otherwise `pass`.
- A clean batch is `{"verdict": "pass", "issues": []}`. Do not invent findings to look thorough.
- Judge from the material shown — do not call any tools."""


def render_review_prompt(batch_items: list[dict], results_by_index: dict[int, dict]) -> str:
    """`batch_items` = the batch file's items; `results_by_index` = index -> produced entry dict."""
    parts = [_REVIEW_PROMPT.read_text(encoding="utf-8"),
             "\n\n---\n\n# REVIEW THESE ROWS\n"]
    for it in batch_items:
        idx = it["index"]
        entry = results_by_index.get(idx, {})
        menu = "\n".join(render_boundary_menu(
            it["boundaries"], list((it.get("bindable_by_item") or {}).items()) or None))
        parts.append(
            f"\n## ITEM index={idx} — {it.get('name', '')}\n"
            f"### Bindable boundaries (the ids an extent may bind)\n{menu}\n\n"
            f"Printed symbols: {', '.join(it.get('symbols') or []) or '(none)'}\n"
            f"Regs:\n{it.get('raw_regs', '')}\n\n"
            f"Produced entry (check every rule's extents, `when`, species, and the entry's "
            f"`includes_tributaries` and `licensing`):\n"
            f"```json\n{json.dumps(entry, ensure_ascii=False, indent=2)}\n```\n"
        )
    parts.append(_ENVELOPE)
    return "\n".join(parts)


def render_from_files(batch_path: str | Path, response_path: str | Path) -> str:
    """The review prompt for one batch and the response `dispatch` wrote for it."""
    from pipeline.regs.parsing.ingest_catalogue import response_rows
    batch = json.loads(Path(batch_path).read_text(encoding="utf-8"))
    by_index = {row.pop("_batch_index"): row for row in response_rows(response_path)}
    return render_review_prompt(batch["items"], by_index)


def main() -> None:
    import argparse
    ap = argparse.ArgumentParser(description="Render a review prompt for a parsed batch.")
    ap.add_argument("batch", help="batch_NNN.json")
    ap.add_argument("response", help="responses/batch_NNN.json")
    ap.add_argument("--out", help="write the prompt here (default: alongside the response, .review.prompt.txt)")
    args = ap.parse_args()
    prompt = render_from_files(args.batch, args.response)
    out = Path(args.out) if args.out else Path(args.response).with_suffix(".review.prompt.txt")
    out.write_text(prompt, encoding="utf-8")
    print(f"Review prompt -> {out}")


if __name__ == "__main__":
    main()
