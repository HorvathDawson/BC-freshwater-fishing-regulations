"""
Post-parsing reviewer for BC freshwater fishing regulations.

Sends batches of parsed results to an LLM reviewer personality that verifies
completeness and accuracy against the original regulation text.

Usage:
    python -m synopsis_pipeline.review_parsed [--file FILE] [--output OUTPUT] [--batch-size N]
"""

import json
import os
import argparse
import time
import signal
import sys
from datetime import datetime
from pathlib import Path
from types import FrameType
from typing import List, Dict, Any, Optional, Tuple

from google import genai
from google.genai import types

from .parse_synopsis import APIKeyManager, _ensure_config
from project_config import get_config, get_api_keys, load_config

# ── Globals ──────────────────────────────────────────────────────────────────

interruption_requested = False


def _signal_handler(sig: int, frame: Optional[FrameType]) -> None:
    global interruption_requested
    if not interruption_requested:
        interruption_requested = True
        print("\n\n🛑 Interruption requested. Finishing current batch and saving...")
    else:
        print("\n💀 Forcing exit!")
        sys.exit(1)


signal.signal(signal.SIGINT, _signal_handler)


# ── Prompt Builder ───────────────────────────────────────────────────────────


def get_review_prompt_path() -> Path:
    return Path(__file__).parent / "prompts" / "review_prompt.txt"


def build_review_prompt(entries: List[Dict[str, Any]]) -> str:
    """Build the reviewer prompt for a batch of parsed entries.

    Each entry must have: water, regs_verbatim, and the parsed rules.
    The prompt asks the LLM to verify completeness and accuracy.
    """
    batch_inputs = []
    for entry in entries:
        batch_inputs.append(
            {
                "water": entry.get("identity", {}).get("name_verbatim", "UNKNOWN"),
                "regs_verbatim": entry.get("regs_verbatim", ""),
                "parsed_rules": entry.get("rules", []),
            }
        )

    template_path = get_review_prompt_path()
    with open(template_path, "r", encoding="utf-8") as f:
        template = f.read()

    return template.format(
        num_items=len(entries),
        batch_inputs=json.dumps(batch_inputs, indent=2, ensure_ascii=False),
    )


# ── Review Session State ─────────────────────────────────────────────────────


class ReviewSession:
    """Tracks review progress so interrupted runs can be resumed."""

    def __init__(
        self,
        total_items: int,
        verdicts: Optional[List[Optional[Dict]]] = None,
        processed: Optional[List[int]] = None,
        created_at: Optional[str] = None,
    ):
        self.total_items = total_items
        self.verdicts: List[Optional[Dict]] = verdicts or [None] * total_items
        self.processed: List[int] = processed or []
        self.created_at = created_at or datetime.now().isoformat()
        self.completed_at: Optional[str] = None

    def save(self, filepath: str) -> None:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "total_items": self.total_items,
                    "verdicts": self.verdicts,
                    "processed": self.processed,
                    "created_at": self.created_at,
                    "completed_at": self.completed_at,
                },
                f,
                indent=2,
                ensure_ascii=False,
            )

    @classmethod
    def load(cls, filepath: str) -> Optional["ReviewSession"]:
        if not os.path.exists(filepath):
            return None
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        session = cls(
            total_items=data["total_items"],
            verdicts=data.get("verdicts"),
            processed=data.get("processed", []),
            created_at=data.get("created_at"),
        )
        session.completed_at = data.get("completed_at")
        return session


# ── Batch Reviewer ───────────────────────────────────────────────────────────


class BatchReviewer:
    """Sends batches of parsed results to the LLM reviewer and collects verdicts."""

    def __init__(self, session: ReviewSession, session_file: str, api: APIKeyManager):
        self.session = session
        self.session_file = session_file
        self.api = api

    def review_batch(self, indices: List[int], entries: List[Dict[str, Any]]) -> bool:
        """Review a batch. Returns True on success, False on fatal rate-limit stop."""
        if not indices:
            return True

        batch_entries = [entries[i] for i in indices]
        prompt = build_review_prompt(batch_entries)

        # API call
        try:
            raw = self._call_api(prompt)
        except Exception as e:
            print(f"  ✗ Review batch failed: {e}")
            # Mark these as not reviewed — they'll be retried on resume
            return "RATE_LIMIT" not in str(e)

        # Parse response
        verdict_list = raw if isinstance(raw, list) else []

        # Pad if short
        while len(verdict_list) < len(indices):
            verdict_list.append(
                {
                    "water": (
                        batch_entries[len(verdict_list)]
                        .get("identity", {})
                        .get("name_verbatim", "UNKNOWN")
                        if len(verdict_list) < len(batch_entries)
                        else "UNKNOWN"
                    ),
                    "verdict": "ERROR",
                    "issues": [
                        {
                            "severity": "CRITICAL",
                            "category": "missing_rule",
                            "description": "Reviewer returned no verdict for this entry",
                        }
                    ],
                    "rule_count_expected": 0,
                    "rule_count_actual": 0,
                    "summary": "No reviewer response received",
                }
            )

        # Store verdicts
        success = 0
        for i, verdict in enumerate(verdict_list):
            if i >= len(indices):
                break
            idx = indices[i]
            self.session.verdicts[idx] = verdict
            if idx not in self.session.processed:
                self.session.processed.append(idx)
            success += 1

        fail_count = sum(
            1
            for v in verdict_list[: len(indices)]
            if isinstance(v, dict) and v.get("verdict") == "FAIL"
        )
        pass_count = sum(
            1
            for v in verdict_list[: len(indices)]
            if isinstance(v, dict) and v.get("verdict") == "PASS"
        )

        print(
            f"  ✓ {success}/{len(indices)} reviewed ({pass_count} PASS, {fail_count} FAIL)"
        )
        self.session.save(self.session_file)
        return True

    def _call_api(self, prompt: str) -> Any:
        _ensure_config()
        from .parse_synopsis import CONFIG

        retries = 0
        max_retries = CONFIG["synopsis_pipeline"]["llm"]["max_retries"]

        while retries < max_retries:
            try:
                resp = self.api.current_client.models.generate_content(
                    model=CONFIG["synopsis_pipeline"]["llm"]["model"],
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json"
                    ),
                )
                if not resp.text:
                    raise ValueError("Empty response")
                self.api.record_success()
                return json.loads(resp.text)
            except Exception as e:
                err = str(e).lower()
                if "429" in err or "rate limit" in err:
                    if not self.api.record_rate_limit():
                        raise RuntimeError("RATE_LIMIT_EXHAUSTED")
                elif "503" in err:
                    time.sleep((2**retries) * 2)
                    retries += 1
                else:
                    self.api.record_failure()
                    raise
        raise RuntimeError(f"Max retries ({max_retries}) exceeded")


# ── Report Generator ─────────────────────────────────────────────────────────


def generate_report(session: ReviewSession, entries: List[Dict]) -> Dict[str, Any]:
    """Build a summary report from review verdicts."""
    failures = []
    warnings = []
    pass_count = 0
    fail_count = 0
    error_count = 0

    for idx, verdict in enumerate(session.verdicts):
        if verdict is None:
            error_count += 1
            continue

        v = verdict.get("verdict", "ERROR")
        if v == "PASS":
            pass_count += 1
            # Still collect warnings even for PASS verdicts
            for issue in verdict.get("issues", []):
                if issue.get("severity") == "WARNING":
                    warnings.append(
                        {
                            "index": idx,
                            "water": verdict.get("water", "UNKNOWN"),
                            **issue,
                        }
                    )
        elif v == "FAIL":
            fail_count += 1
            failures.append(
                {
                    "index": idx,
                    "water": verdict.get("water", "UNKNOWN"),
                    "issues": verdict.get("issues", []),
                    "rule_count_expected": verdict.get("rule_count_expected"),
                    "rule_count_actual": verdict.get("rule_count_actual"),
                    "summary": verdict.get("summary", ""),
                }
            )
        else:
            error_count += 1

    return {
        "generated_at": datetime.now().isoformat(),
        "total_reviewed": session.total_items,
        "pass": pass_count,
        "fail": fail_count,
        "error": error_count,
        "warnings": len(warnings),
        "pass_rate": f"{pass_count / max(session.total_items, 1) * 100:.1f}%",
        "failures": failures,
        "warning_details": warnings,
    }


# ── CLI ──────────────────────────────────────────────────────────────────────


def main() -> None:
    _ensure_config()
    config = get_config()

    default_input = str(config.synopsis_parsed_results_path)
    default_output_dir = str(Path(config.synopsis_parsed_results_path).parent)

    parser = argparse.ArgumentParser(
        description="Review parsed fishing regulations for completeness and accuracy"
    )
    parser.add_argument(
        "--file",
        default=default_input,
        help=f"Parsed results JSON to review (default: {default_input})",
    )
    parser.add_argument(
        "--output",
        default=os.path.join(default_output_dir, "review_report.json"),
        help="Output review report path",
    )
    parser.add_argument(
        "--session-file",
        default=os.path.join(default_output_dir, "review_session.json"),
        help="Review session state file (for resuming)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=15,
        help="Entries per review batch (default: 15, smaller = more thorough)",
    )
    parser.add_argument("--resume", action="store_true", help="Resume previous review")
    parser.add_argument(
        "--report-only",
        action="store_true",
        help="Generate report from existing review session without calling LLM",
    )

    args = parser.parse_args()

    # Load parsed results
    with open(args.file, "r", encoding="utf-8") as f:
        entries = json.load(f)

    # Skip error placeholders (entries with lookup_name=FAILED)
    reviewable_indices = [
        i
        for i, e in enumerate(entries)
        if e.get("identity", {}).get("lookup_name") != "FAILED"
        and len(e.get("rules", [])) > 0
    ]

    print("=" * 80)
    print("BC FRESHWATER FISHING REGULATIONS - POST-PARSE REVIEW")
    print("=" * 80)
    print(f"\n📝 Input: {args.file}")
    print(f"   Total entries: {len(entries)}")
    print(f"   Reviewable entries: {len(reviewable_indices)}")
    print(f"   Skipped (failed/empty): {len(entries) - len(reviewable_indices)}")
    print(f"\n💾 Output: {args.output}")
    print(f"   Session: {args.session_file}")
    print(f"   Batch size: {args.batch_size}")

    # Report-only mode
    if args.report_only:
        session = ReviewSession.load(args.session_file)
        if not session:
            print("Error: No review session found.")
            return
        report = generate_report(session, entries)
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        _print_summary(report)
        return

    # Load or create session
    session = None
    if args.resume:
        session = ReviewSession.load(args.session_file)
        if session:
            print(
                f"\n✓ Resuming review: {len(session.processed)}/{session.total_items} done"
            )

    if not session:
        session = ReviewSession(total_items=len(entries))

    # Build pending list
    pending = [i for i in reviewable_indices if i not in session.processed]
    print(f"\nWork Queue: {len(pending)} entries to review")

    if not pending:
        print("✓ All entries already reviewed.")
    else:
        # Initialize API
        api_keys = get_api_keys()
        api = APIKeyManager(api_keys)
        reviewer = BatchReviewer(session, args.session_file, api)

        for batch_start in range(0, len(pending), args.batch_size):
            if interruption_requested:
                break

            batch = pending[batch_start : batch_start + args.batch_size]
            pct = len(session.processed) / len(reviewable_indices) * 100
            print(
                f"\nReview Batch {batch_start // args.batch_size + 1} | "
                f"Progress: {pct:.1f}% | API: {api.get_status()}"
            )

            if not reviewer.review_batch(batch, entries):
                print("Stopping due to API limits.")
                break

            time.sleep(1)

    # Finalize
    if len(session.processed) >= len(reviewable_indices):
        session.completed_at = datetime.now().isoformat()
    session.save(args.session_file)

    # Generate report
    report = generate_report(session, entries)
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    _print_summary(report)


def _print_summary(report: Dict[str, Any]) -> None:
    """Print a human-readable summary of the review report."""
    print(f"\n{'=' * 60}")
    print("Review Summary:")
    print(f"  Total reviewed: {report['total_reviewed']}")
    print(f"  ✓ Pass: {report['pass']}")
    print(f"  ✗ Fail: {report['fail']}")
    print(f"  ⚠ Warnings: {report['warnings']}")
    if report["error"] > 0:
        print(f"  ? Errors: {report['error']}")
    print(f"  Pass rate: {report['pass_rate']}")

    if report["failures"]:
        print(f"\n  Top Failures:")
        for fail in report["failures"][:10]:
            print(f"    [{fail['index']}] {fail['water']}: {fail['summary']}")
            for issue in fail["issues"][:3]:
                print(f"       {issue['severity']}: {issue['description']}")
        if len(report["failures"]) > 10:
            print(f"    ... and {len(report['failures']) - 10} more")

    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
