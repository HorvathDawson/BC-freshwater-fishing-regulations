"""Render a self-contained parsing prompt for a subagent batch.

The parsing *rules* live in exactly one place — ``pipeline/parsing/prompt.txt``
and ``pipeline/parsing/examples.json`` (the same files the Gemini parser uses).
This module reads those canonical files at render time and appends only a thin
output envelope describing the ``{index, entry}`` contract the ingest step
expects.  Nothing here re-encodes the parsing rules, so the two engines can
never drift.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

# Canonical prompt sources (single source of truth — do NOT copy their text).
_PARSING_DIR = Path(__file__).resolve().parent.parent / "parsing"
_PROMPT_PATH = _PARSING_DIR / "prompt.txt"
_EXAMPLES_PATH = _PARSING_DIR / "examples.json"

_OUTPUT_ENVELOPE = """
---

OUTPUT FORMAT (STRICT — read carefully):

Return ONLY a JSON array, one object per input item, in this exact shape:

    [
      {"index": <the input item's index, unchanged>, "entry": { ...ParsedEntry... }},
      ...
    ]

Rules for the wrapper:
- Copy each input item's "index" value verbatim into your output object.
  The index is how results are mapped back — never renumber or reorder-key it.
- "entry" is the full ParsedEntry object described above for that item.
- "entry.regs_verbatim" MUST be a character-for-character copy of that item's
  "raw_regs" (same whitespace, same punctuation, same ** bold markers).
- Return one object for every input item. If an item is genuinely unparseable,
  still return it with your best-effort entry and note the issue in
  "entry.audit_log"; do not omit items.
- Do NOT wrap the response in Markdown code fences.
- Do NOT add any prose before or after the JSON array.
"""

_REVIEW_HEADER = """
---

YOU ARE A REVIEW AGENT — NOT A PARSER.

Everything above is the exact parsing specification (rules + worked examples)
and the INPUT ROWS for this batch. Below is the CANDIDATE OUTPUT that another
agent produced for those same rows. Your job is to audit that output for BOTH
structural validity and content correctness, and to check consistency across
the whole batch. You do NOT re-author the entries — you report issues so the
parsing agent can fix them.

CANDIDATE OUTPUT (one object per row, keyed by the same integer "index"):
"""

_REVIEW_RUBRIC = """
---

REVIEW CHECKLIST — apply to EVERY row:

A. STRUCTURAL VALIDITY — the ".validate()" gate. The candidate must pass the
   SAME validators the ingest step runs (ParsedEntry schema + verbatim echo +
   keyword coverage). Confirm it by running, from the repo root:

       PYTHONPATH="$PWD" .venv/bin/python -m pipeline.agent_parsing.ingest \\
         <candidate-response.json> --session-dir <dir> --out-dir <dir> --dry-run

   It MUST report `failed : 0` with `accepted` equal to the row count. Any
   failure is a BLOCKING issue — report it with the exact validator message.

B. CONTENT CORRECTNESS — the SPIRIT of the validators, not just the letter.
   The schema can pass while the content is wrong; catch that here:
   - Coverage: every restriction / closure / quota / gear / licensing / note
     clause in raw_regs is represented by a rule. Nothing dropped, nothing
     invented.
   - `restriction_type` is the correct category for each rule.
   - `details` faithfully and concisely summarises its `rule_text` — no meaning
     changed, no information lost.
   - `location_text`, `dates`, and `exception` are correct, complete verbatim
     slices (no missed date / location / "except" clause).
   - Per-rule `includes_tributaries` is null unless THIS rule's own text
     overrides the entry scope; true/false only when the rule text explicitly
     differs from the entry-wide scope.
   - Entry `includes_tributaries` / `tributary_only` match the entry-wide
     markers (symbols, "[Includes Tributaries]", title "'S TRIBUTARIES").
   - Map/page references: an EMBEDDED pointer stays inside its parent rule and
     is surfaced in that rule's `details` (NO duplicate note rule); a STANDALONE
     reference sentence is its own `note` rule.
   - No duplicated rules (two rules with the same rule_text AND the same
     meaning).

C. CROSS-ROW CONSISTENCY — you can see the whole batch at once; use it:
   - The same raw phrasing is parsed the same way across rows (same rule
     splitting, same `restriction_type`, same `details` style).
   - `restriction_type` vocabulary is used consistently.
   - Tributary and exception handling is applied uniformly for equivalent text.

OUTPUT — return ONLY this JSON object (no prose, no Markdown fences):

    {
      "verdict": "pass" | "changes_requested",
      "issues": [
        {
          "index": <row index>,
          "severity": "blocking" | "consistency" | "nit",
          "problem": "<what is wrong>",
          "fix": "<the concrete correction the parsing agent should make>"
        }
      ]
    }

If every row is correct, return {"verdict": "pass", "issues": []}. Do NOT rewrite
the entries yourself — describe each fix so the parsing agent can apply it, then
it re-runs the dry-run until `failed : 0` and re-submits.
"""


def _format_examples(examples: List[Dict[str, Any]]) -> str:
    """Format example input/output pairs (mirrors the Gemini parser format)."""
    parts: List[str] = []
    for ex in examples:
        parts.append(
            f"INPUT:\n```json\n{json.dumps(ex['input'], indent=2)}\n```\n\n"
            f"OUTPUT:\n```json\n{json.dumps(ex['output'], indent=2)}\n```"
        )
    return "\n\n---\n\n".join(parts)


def _render_spec_body(items: List[Dict[str, Any]]) -> str:
    """Render the canonical spec (rules + examples) with the batch rows spliced
    into the ``{batch_inputs}`` slot.  Shared by the parse and review prompts so
    both always see the identical, single-source-of-truth instructions."""
    template = _PROMPT_PATH.read_text(encoding="utf-8")
    examples = json.loads(_EXAMPLES_PATH.read_text(encoding="utf-8"))

    batch_inputs = json.dumps(
        [
            {
                "index": it["index"],
                "water": it["water"],
                "raw_regs": it["raw_regs"],
                "symbols": it.get("symbols", []),
            }
            for it in items
        ],
        indent=2,
        ensure_ascii=False,
    )

    return (
        template.replace("{num_items}", str(len(items)))
        .replace("{examples}", _format_examples(examples))
        .replace("{batch_inputs}", batch_inputs)
    )


def render_batch_prompt(items: List[Dict[str, Any]]) -> str:
    """Render the full self-contained prompt for one batch of indexed rows.

    ``items`` is a list of ``{index, water, raw_regs, symbols}`` dicts (extra
    keys are ignored).  The returned string is safe to hand directly to a
    subagent.
    """
    return _render_spec_body(items) + "\n" + _OUTPUT_ENVELOPE


def render_review_prompt(
    items: List[Dict[str, Any]], results: List[Dict[str, Any]]
) -> str:
    """Render a self-contained REVIEW prompt for a second-pass reviewer agent.

    The reviewer sees the identical canonical spec + rows as the parser (via
    ``_render_spec_body``) plus the ``results`` a parsing agent produced, and is
    asked to audit them for structural validity, content correctness, and
    cross-row consistency — returning a structured issues report.

    ``items``  — the batch rows (same shape as ``render_batch_prompt``).
    ``results``— the candidate output: a list of ``{"index", "entry"}`` objects
                 (the parsed ``responses/batch_NNN.json`` payload).
    """
    candidate = json.dumps(results, indent=2, ensure_ascii=False)
    return (
        _render_spec_body(items)
        + _REVIEW_HEADER
        + "\n```json\n"
        + candidate
        + "\n```\n"
        + _REVIEW_RUBRIC
    )

