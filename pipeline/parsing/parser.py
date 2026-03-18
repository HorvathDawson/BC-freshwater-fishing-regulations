"""
V2 Synopsis Parser — lightweight LLM-based regulation parsing.

Sends batches of synopsis rows to Gemini, validates the structured JSON
output against the Pydantic models, and writes results.

Usage
-----
    python -m pipeline.parsing.parser
    python -m pipeline.parsing.parser --dry-run
    python -m pipeline.parsing.parser --batch-size 30
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from google.genai import types

from .api_manager import APIKeyManager
from .models import ParsedBatch, ParsedEntry, validate_batch
from .session import ParsingSession

logger = logging.getLogger(__name__)

# Graceful shutdown flag
_interrupted = False


def _signal_handler(sig: int, frame: Any) -> None:
    global _interrupted
    if _interrupted:
        print("\nForce exit.")
        sys.exit(1)
    _interrupted = True
    print("\nInterrupt received — finishing current batch then stopping.")


# ---------------------------------------------------------------------------
# Prompt builder
# ---------------------------------------------------------------------------

_PROMPT_PATH = Path(__file__).resolve().parent / "prompt.txt"
_EXAMPLES_PATH = Path(__file__).resolve().parent / "examples.json"


def _format_examples(examples: List[Dict[str, Any]]) -> str:
    """Format example pairs for the prompt."""
    parts: List[str] = []
    for ex in examples:
        parts.append(
            f"INPUT:\n```json\n{json.dumps(ex['input'], indent=2)}\n```\n\n"
            f"OUTPUT:\n```json\n{json.dumps(ex['output'], indent=2)}\n```"
        )
    return "\n\n---\n\n".join(parts)


def build_prompt(rows: List[Dict[str, Any]]) -> str:
    """Build the full prompt from template, examples, and input batch."""
    template = _PROMPT_PATH.read_text(encoding="utf-8")
    examples = json.loads(_EXAMPLES_PATH.read_text(encoding="utf-8"))

    batch_inputs = json.dumps(
        [
            {
                "water": r["water"],
                "raw_regs": r["raw_regs"],
                "symbols": r.get("symbols", []),
            }
            for r in rows
        ],
        indent=2,
        ensure_ascii=False,
    )

    return (
        template.replace("{num_items}", str(len(rows)))
        .replace("{examples}", _format_examples(examples))
        .replace("{batch_inputs}", batch_inputs)
    )


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------


class SynopsisParser:
    """Lightweight batch parser using Gemini structured output."""

    def __init__(
        self,
        api: APIKeyManager,
        model: str = "gemini-2.5-flash",
        max_retries: int = 3,
    ) -> None:
        self.api = api
        self.model = model
        self.max_retries = max_retries

    def parse_batch(
        self, rows: List[Dict[str, Any]], dry_run: bool = False
    ) -> List[Optional[ParsedEntry]]:
        """Parse a batch of synopsis rows.  Returns list parallel to rows.

        Each element is a ParsedEntry on success or None on failure.
        """
        prompt = build_prompt(rows)
        if dry_run:
            print(f"  [DRY RUN] {len(rows)} items, prompt {len(prompt)} chars")
            return [None] * len(rows)

        # Call API
        raw = self._call_api(prompt)

        # Parse into Pydantic models
        entries: List[Optional[ParsedEntry]] = []
        parsed_list = (
            raw
            if isinstance(raw, list)
            else raw.get("entries", []) if isinstance(raw, dict) else []
        )

        # Pad if LLM returned fewer entries
        while len(parsed_list) < len(rows):
            parsed_list.append(None)

        for i, item in enumerate(parsed_list):
            if not isinstance(item, dict) or not item:
                entries.append(None)
                logger.warning(
                    "Row %d (%s): empty/invalid response", i, rows[i].get("water", "?")
                )
                continue
            try:
                entry = ParsedEntry.model_validate(item)
                entries.append(entry)
            except Exception as exc:
                entries.append(None)
                logger.warning(
                    "Row %d (%s): validation failed: %s",
                    i,
                    rows[i].get("water", "?"),
                    exc,
                )

        # Batch-level validation (regs_verbatim echo, symbol checks)
        batch_errors = validate_batch(
            [e for e in entries if e is not None],
            [rows[i] for i, e in enumerate(entries) if e is not None],
        )
        if batch_errors:
            for err in batch_errors:
                idx = err["index"]
                logger.warning("Batch validation: %s — %s", err["water"], err["errors"])

        return entries

    def _call_api(self, prompt: str) -> Any:
        """Call Gemini with retry and key rotation."""
        errors: List[str] = []
        for attempt in range(self.max_retries):
            try:
                resp = self.api.current_client.models.generate_content(
                    model=self.model,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                    ),
                )
                if not resp.text:
                    raise ValueError("Empty response from API")
                self.api.record_success()
                return json.loads(resp.text)
            except Exception as exc:
                err_str = str(exc).lower()
                errors.append(
                    f"Attempt {attempt + 1}: {type(exc).__name__}: {str(exc)[:200]}"
                )

                if "429" in err_str or "rate limit" in err_str:
                    if not self.api.record_rate_limit():
                        raise RuntimeError(
                            "All API keys rate-limited. Errors: " + "; ".join(errors)
                        )
                elif "503" in err_str:
                    time.sleep((2**attempt) * 2)
                else:
                    self.api.record_failure()
                    raise

        raise RuntimeError(
            f"Max retries ({self.max_retries}) exceeded. Errors: " + "; ".join(errors)
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    from project_config import get_config, get_api_keys, load_config

    signal.signal(signal.SIGINT, _signal_handler)

    parser = argparse.ArgumentParser(description="V2 Synopsis Parser")
    parser.add_argument("--raw", help="Path to synopsis_raw_data.json")
    parser.add_argument("--output", help="Output directory")
    parser.add_argument("--batch-size", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume a previous run, skipping already-parsed rows",
    )
    args = parser.parse_args()

    config = get_config()
    llm_cfg = load_config()["llm"]

    raw_path = Path(args.raw) if args.raw else config.synopsis_raw_data_path
    batch_size = args.batch_size or llm_cfg.get("batch_size", 45)
    model = llm_cfg.get("model", "gemini-2.5-flash")

    # Output directory: output/pipeline/parsing/
    if args.output:
        out_dir = Path(args.output)
    else:
        import yaml

        with open(config.project_root / "config.yaml") as f:
            cfg = yaml.safe_load(f)
        out_dir = config.project_root / cfg["output"]["pipeline"]["parsing"]
    out_dir.mkdir(parents=True, exist_ok=True)

    # Load raw rows
    print(f"Loading synopsis rows from: {raw_path}")
    with open(raw_path, encoding="utf-8") as f:
        pages = json.load(f)
    rows: List[Dict[str, Any]] = []
    for page in pages:
        region = page.get("context", {}).get("region")
        for row in page.get("rows", []):
            row_dict = dict(row)
            if region and not row_dict.get("region"):
                row_dict["region"] = region
            rows.append(row_dict)
    print(f"  {len(rows)} rows loaded")

    # Setup API
    api_keys = get_api_keys()
    api = APIKeyManager(api_keys, max_failures=3, max_rate_limit_hits=2)
    parser_engine = SynopsisParser(
        api=api, model=model, max_retries=llm_cfg.get("max_retries", 3)
    )

    # Session for checkpoint/resume
    if not args.resume:
        # Fresh run — remove any stale session
        session_file = out_dir / "session_state.json"
        if session_file.exists():
            session_file.unlink()
    session = ParsingSession(session_dir=out_dir, total_rows=len(rows))
    if args.resume:
        reset_count = session.revalidate(rows)
        if reset_count:
            print(f"  Re-validation reset {reset_count} entries for re-parsing")
    pending = session.pending_indices()
    if args.resume and len(pending) < len(rows):
        s = session.summary()
        print(
            f"  Resuming: {s['success']} done, {s['failed']} failed, {s['pending']} pending"
        )

    # Process batches (only pending rows)
    success = session.summary()["success"]
    failed = session.summary()["failed"]

    # Build batches from pending indices
    for batch_start_pos in range(0, len(pending), batch_size):
        if _interrupted:
            print("\nStopping after interrupt. Resume with --resume.")
            break

        batch_indices = pending[batch_start_pos : batch_start_pos + batch_size]
        batch_rows = [rows[i] for i in batch_indices]
        batch_num = batch_start_pos // batch_size + 1
        total_batches = (len(pending) + batch_size - 1) // batch_size

        print(f"\nBatch {batch_num}/{total_batches} ({len(batch_rows)} items)...")
        print(f"  API keys: {api.status()}")

        try:
            entries = parser_engine.parse_batch(batch_rows, dry_run=args.dry_run)
        except RuntimeError as exc:
            print(f"  FATAL: {exc}")
            break

        # Record into session (indices may not be contiguous on resume)
        for j, entry in enumerate(entries):
            idx = batch_indices[j]
            session.results[idx] = entry.model_dump(mode="json") if entry else None
            session.status[idx] = "success" if entry else "failed"
            if entry is not None:
                success += 1
            else:
                failed += 1
        session._save()

    # Write final output
    out_path = out_dir / "synopsis_parsed.json"
    session.finalize(out_path)

    s = session.summary()
    print(f"\n{'=' * 50}")
    print(f"  Parsed:  {s['success']}/{s['total']}")
    print(f"  Failed:  {s['failed']}/{s['total']}")
    print(f"  Pending: {s['pending']}/{s['total']}")
    print(f"  Output:  {out_path}")
    if s["failed"] > 0 or s["pending"] > 0:
        print(f"  Restart with --resume to retry failed/pending rows.")
    print(f"{'=' * 50}")


if __name__ == "__main__":
    main()
