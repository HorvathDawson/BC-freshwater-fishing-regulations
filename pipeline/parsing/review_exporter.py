"""Render an independent-review prompt for a parsed batch — the second-pass reviewer subagent.

Reuses the canonical REVIEW_PROMPT.md checklist and gives the reviewer, per row: the item's bindable
boundaries + regs (from the batch file) and the entry another agent produced (from the response). The
reviewer reports `{verdict, issues}`; it does not rewrite. Self-contained (batch + response only)."""

from __future__ import annotations

import json
from pathlib import Path

_REVIEW_PROMPT = Path(__file__).resolve().parent / "prompts" / "REVIEW_PROMPT.md"

_ENVELOPE = """\
---

# OUTPUT (STRICT)

Return ONLY this JSON object (no prose, no fences):

    { "verdict": "pass" | "changes_requested",
      "issues": [ { "index": <row>, "severity": "high"|"medium"|"low",
                    "problem": "<what is wrong>", "fix": "<the concrete correction>" } ] }

Empty issues + verdict "pass" = every row is correct. You may run the repo's Python helpers to check
(e.g. `python -m pipeline.parsing.validate <batch.json> <response.json>`)."""


def render_review_prompt(batch_items: list[dict], results_by_index: dict[int, dict]) -> str:
    """`batch_items` = the batch file's items; `results_by_index` = index -> produced entry dict."""
    parts = [_REVIEW_PROMPT.read_text(encoding="utf-8"),
             "\n\n---\n\n# REVIEW THESE ROWS\n"]
    for it in batch_items:
        idx = it["index"]
        entry = results_by_index.get(idx, {})
        parts.append(
            f"\n## ITEM index={idx} — {it.get('name','')}\n"
            f"Bindable boundaries: {it.get('bindable_ids', [])}\n"
            f"Regs: {it.get('raw_regs','')}\n\n"
            f"Produced entry:\n```json\n{json.dumps(entry, ensure_ascii=False, indent=2)}\n```\n"
        )
    parts.append(_ENVELOPE)
    return "\n".join(parts)


def render_from_files(batch_path: str | Path, response_path: str | Path) -> str:
    batch = json.loads(Path(batch_path).read_text(encoding="utf-8"))
    resp = json.loads(Path(response_path).read_text(encoding="utf-8"))
    if isinstance(resp, dict) and "entry" in resp:
        resp = [resp]
    by_index = {o["index"]: o.get("entry", {}) for o in resp if isinstance(o, dict) and "index" in o}
    return render_review_prompt(batch.get("items", []), by_index)


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
