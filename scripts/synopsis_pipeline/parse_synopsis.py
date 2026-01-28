import json
import os
import argparse
import time
import signal
import shutil
import sys
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

from google import genai
from google.genai import types

# Import shared models and utilities
from .models import (
    WaterbodyRow,
    ExtractionResults,
    ParsedWaterbody,
    SessionState,
)
from .prompt_builder import build_prompt
from .config import load_config, get_api_keys

# --- GLOBALS & CONFIG ---
try:
    CONFIG = load_config()
    API_KEYS = get_api_keys(CONFIG)
except FileNotFoundError:
    print("Error: config.yaml not found.")
    sys.exit(1)

# Global flag for graceful shutdown
interruption_requested = False


def signal_handler(sig, frame):
    """
    Catches Ctrl+C.
    First press: Sets flag to stop AFTER current batch.
    Second press: Forces immediate exit.
    """
    global interruption_requested
    if not interruption_requested:
        interruption_requested = True
        print(
            "\n\n🛑 Interruption requested (Ctrl+C). Finishing current batch and saving..."
        )
        print("   (Press Ctrl+C again to force quit immediately)")
    else:
        print("\n💀 Forcing exit!")
        sys.exit(1)


signal.signal(signal.SIGINT, signal_handler)

# --- CLASSES ---


class APIKeyManager:
    """Manages API keys, rotation, and limits."""

    def __init__(
        self, api_keys: List[Dict[str, str]], max_failures=3, max_rate_limit_hits=2
    ):
        self.api_keys = api_keys
        self.clients = {k["id"]: genai.Client(api_key=k["key"]) for k in api_keys}
        self.stats = {k["id"]: {"fails": 0, "rate_limits": 0} for k in api_keys}
        self.current_idx = 0
        self.max_failures = max_failures
        self.max_rate_limit_hits = max_rate_limit_hits

    @property
    def current_id(self) -> str:
        return self.api_keys[self.current_idx]["id"]

    @property
    def current_client(self) -> genai.Client:
        return self.clients[self.current_id]

    def rotate(self) -> bool:
        start = self.current_idx
        while True:
            self.current_idx = (self.current_idx + 1) % len(self.api_keys)
            k = self.current_id

            # Check if key is usable
            is_usable = (
                self.stats[k]["fails"] < self.max_failures
                and self.stats[k]["rate_limits"] < self.max_rate_limit_hits
            )

            if is_usable:
                print(f"  ↻  Switched to API key '{k}'")
                return True

            # If we looped all the way back to start, all keys are dead
            if self.current_idx == start:
                return False

    def record_success(self):
        self.stats[self.current_id]["fails"] = 0

    def record_failure(self):
        self.stats[self.current_id]["fails"] += 1
        return self.rotate()

    def record_rate_limit(self):
        self.stats[self.current_id]["rate_limits"] += 1
        print(f"  ⚠  Rate limit on '{self.current_id}'")
        return self.rotate()

    def get_status(self) -> str:
        """Returns a detailed status string for the user."""
        active_count = sum(
            1
            for k in self.api_keys
            if self.stats[k["id"]]["fails"] < self.max_failures
            and self.stats[k["id"]]["rate_limits"] < self.max_rate_limit_hits
        )

        total = len(self.api_keys)
        details = " | ".join(
            [f"{k}: {v['fails']}F/{v['rate_limits']}RL" for k, v in self.stats.items()]
        )
        return f"Active Keys: {active_count}/{total} [{details}]"


class FailureLogger:
    """Handles detailed logging of failed items to a separate JSON/Text file."""

    def __init__(self, log_path: str):
        self.log_path = log_path
        self.summary_path = log_path.replace(".json", "_summary.txt")
        os.makedirs(os.path.dirname(log_path), exist_ok=True)

    def log_batch_errors(
        self,
        batch_indices: List[int],
        item_errors: List[Dict],
        rows: List[WaterbodyRow],
        results: List[Dict],
    ):
        if not item_errors:
            return

        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "batch_indices": batch_indices,
            "failures": [],
        }

        # Structure failures
        for err in item_errors:
            b_idx = err["batch_index"]
            real_idx = batch_indices[b_idx]
            row = rows[b_idx]  # Use batch index, not real index

            detail = {
                "index": real_idx,
                "waterbody": row.water,
                "raw_regs": row.raw_regs,
                "error": err["error"],
                "error_type": err.get("error_type", "unknown"),
            }
            if "model_output" in err:
                detail["model_output"] = err["model_output"]
            if "all_errors" in err:
                detail["validation_errors"] = err["all_errors"]

            log_entry["failures"].append(detail)

        # Append to JSON
        with open(self.log_path, "a", encoding="utf-8") as f:
            json.dump(log_entry, f, indent=2, ensure_ascii=False)
            f.write(",\n")

        # Append to Summary Text
        with open(self.summary_path, "a", encoding="utf-8") as f:
            f.write(f"\n{'='*80}\nBatch Failures {log_entry['timestamp']}\n{'='*80}\n")
            for fail in log_entry["failures"]:
                f.write(
                    f"[{fail['index']}] {fail['waterbody']}\nError: {fail['error']}\n"
                )
                if "validation_errors" in fail:
                    for ve in fail["validation_errors"]:
                        f.write(f" - {ve}\n")
                f.write("-" * 40 + "\n")


class SessionManager:
    """Handles loading, saving, revalidating, and archiving sessions."""

    def __init__(self, filepath: str):
        self.filepath = filepath

    def load_or_create(
        self, rows: List[WaterbodyRow], resume: bool
    ) -> Tuple[SessionState, bool]:
        session = SessionState.load(self.filepath)
        if session and len(session.processed_items) > 0:
            if resume:
                print(f"✓ Resuming session: {len(session.processed_items)} processed")
                return session, True
            else:
                # Interactive prompt check
                print(
                    f"⚠ Found existing session ({len(session.processed_items)} processed)."
                )
                ans = input("Resume? [Y/n]: ").strip().lower()
                if ans in ("", "y", "yes"):
                    return session, True
                print("Starting fresh...")
                if os.path.exists(self.filepath):
                    os.remove(self.filepath)

        return SessionState.create_new(rows), False

    def revalidate_existing_results(self, session: SessionState) -> List[int]:
        """Check previously processed items against CURRENT validation rules."""
        print(f"Revalidating {len(session.processed_items)} items...")
        failed_indices = []

        for idx in session.processed_items:
            # Sanity checks
            if idx >= len(session.results) or not session.results[idx]:
                failed_indices.append(idx)
                continue

            # Run validation
            res = session.results[idx]
            row = session.input_rows[idx]

            # Since ParsedWaterbody.validate takes strings, we check strict logic manually here for revalidation
            errors = res.validate(row.water, row.raw_regs)

            # Header Consistency Check (Re-check)
            symbols = getattr(row, "symbols", []) or []
            header_includes = any("incl. tribs" in str(s).lower() for s in symbols)
            if header_includes and not res.identity.global_scope.includes_tributaries:
                errors.append(
                    "Header has 'Incl. Tribs' but output global_scope.includes_tributaries is False"
                )

            if errors:
                print(f"✗ Item {idx} failed revalidation: {errors[0]}...")
                failed_indices.append(idx)

        if failed_indices:
            print(
                f"⚠ {len(failed_indices)} items failed revalidation and will be retried."
            )
            # Clean session state for these items
            session.processed_items = [
                i for i in session.processed_items if i not in failed_indices
            ]
            for i in failed_indices:
                session.results[i] = None
                if i in session.retry_counts:
                    del session.retry_counts[i]

            session.save(self.filepath)
        else:
            print("✓ All processed items valid.")

        return failed_indices

    def archive(self, session: SessionState, results_file: str, force_incomplete: bool = False):
        """
        Archive a session to completed_sessions or partial_sessions folder.
        
        Args:
            session: Session state to archive
            results_file: Path to parsed results file
            force_incomplete: If True, archive to partial_sessions even if not complete
        """
        is_complete = session.completed_at is not None
        
        # Don't archive incomplete sessions unless explicitly requested
        if not is_complete and not force_incomplete:
            return

        ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        
        # Choose folder based on completion status
        folder_name = "completed_sessions" if is_complete else "partial_sessions"
        folder = os.path.join(os.path.dirname(self.filepath), folder_name, ts)
        os.makedirs(folder, exist_ok=True)

        # Copy core files
        shutil.copy2(self.filepath, os.path.join(folder, "session.json"))
        if os.path.exists(results_file):
            shutil.copy2(results_file, os.path.join(folder, "parsed_results.json"))

        # Manifest
        manifest = {
            "timestamp": ts,
            "total": session.total_items,
            "success": len(session.processed_items),
            "failed": len(session.failed_items),
            "validation_failures": len(session.validation_failures),
            "completion_status": "complete" if is_complete else "partial",
            "completion_percentage": (len(session.processed_items) / session.total_items * 100) if session.total_items > 0 else 0,
        }
        with open(os.path.join(folder, "manifest.json"), "w") as f:
            json.dump(manifest, f, indent=2)

        archive_type = "completed" if is_complete else "partial"
        print(f"✓ Session archived to {archive_type}_sessions: {folder}")


class BatchProcessor:
    """Core LLM processing engine."""

    def __init__(self, session: SessionState, session_file: str, api: APIKeyManager):
        self.session = session
        self.session_file = session_file
        self.api = api
        self.logger = FailureLogger(
            os.path.join(os.path.dirname(session_file), "failure_log.json")
        )

    def run_batch(self, indices: List[int], dry_run=False) -> bool:
        """Process a list of indices. Returns True if successful, False if fatal stop."""
        if not indices:
            return True

        rows = [self.session.input_rows[i] for i in indices]

        # 1. Build Prompt
        prompt = build_prompt(rows)
        if dry_run:
            print(f"  [DRY RUN] {len(rows)} items. Prompt len: {len(prompt)}")
            return True

        # 2. API Call with Retries
        try:
            raw_response = self._call_api(prompt)
        except Exception as e:
            print(f"  ✗ Batch Failed: {e}")
            print(f"     Affected waterbodies ({len(indices)}):")
            for idx in indices[:5]:  # Show first 5
                wb = self.session.input_rows[idx].water
                print(f"       [{idx}] {wb}")
            if len(indices) > 5:
                print(f"       ... and {len(indices) - 5} more")
            self._handle_total_failure(indices, str(e))
            return "RATE_LIMIT" not in str(e)  # Stop if rate limited

        # 3. Parse & Validate
        parsed_list = raw_response if isinstance(raw_response, list) else []
        item_errors = []
        valid_results_dicts = []

        # Pad response if short (API error case)
        while len(parsed_list) < len(rows):
            parsed_list.append({})

        # Use models.py validate_batch to check everything (including header logic)
        batch_validation_errors = ParsedWaterbody.validate_batch(parsed_list, rows)

        for i, entry_dict in enumerate(parsed_list):
            idx = indices[i]
            error = None

            # Structural Check
            if not isinstance(entry_dict, dict) or not entry_dict:
                error = "Empty/Invalid JSON for item"

            # Check for validation errors specific to this item from the batch validation
            if not error:
                specific_errors = [
                    e for e in batch_validation_errors if e.startswith(f"Item {i}:")
                ]
                if specific_errors:
                    error = "; ".join(specific_errors)

            if error:
                item_errors.append(
                    {
                        "batch_index": i,
                        "error": error,
                        "error_type": "validation",
                        "model_output": entry_dict,
                        "all_errors": [error],
                    }
                )
                valid_results_dicts.append(None)
            else:
                valid_results_dicts.append(entry_dict)

        # 4. Save Results to Session (Strict: No auto-correction)
        success_count = 0

        # FIX: Track failure index using iterator to avoid IndexError
        error_iterator = iter(item_errors)

        for i, res_dict in enumerate(valid_results_dicts):
            idx = indices[i]
            if res_dict:
                # Convert back to object for storage
                self.session.results[idx] = ParsedWaterbody.from_dict(res_dict)
                if idx not in self.session.processed_items:
                    self.session.processed_items.append(idx)

                # Clear failures
                if idx in self.session.retry_counts:
                    del self.session.retry_counts[idx]
                self.session.failed_items = [
                    f for f in self.session.failed_items if f["index"] != idx
                ]
                self.session.validation_failures = [
                    f for f in self.session.validation_failures if f["index"] != idx
                ]

                success_count += 1
            else:
                # Handle Failure safely
                try:
                    err_info = next(error_iterator)
                    error_msg = err_info["error"]
                except StopIteration:
                    error_msg = "Unknown validation error (Iterator mismatch)"

                self._mark_failed(idx, error_msg)

        # 5. Log
        self.logger.log_batch_errors(indices, item_errors, rows, parsed_list)
        if item_errors:
            print(f"  ✓ {success_count}/{len(indices)} OK, {len(item_errors)} failed")
            # Show details of first few failures
            for err in item_errors[:3]:
                idx = indices[err["batch_index"]]
                wb = rows[err["batch_index"]].water
                print(f"     [{idx}] {wb}: {err['error'][:80]}...")
            if len(item_errors) > 3:
                print(f"     ... and {len(item_errors) - 3} more failures")
        else:
            print(f"  ✓ {success_count}/{len(indices)} OK")
        self.session.save(self.session_file)
        return True

    def _call_api(self, prompt):
        retries = 0
        max_retries = CONFIG["parsing"]["max_retries"]
        error_log = []  # Track all errors encountered

        while retries < max_retries:
            try:
                resp = self.api.current_client.models.generate_content(
                    model=CONFIG["parsing"]["model"],
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
                error_log.append(
                    f"Retry {retries + 1}: {type(e).__name__}: {str(e)[:200]}"
                )

                if "429" in err or "rate limit" in err:
                    if not self.api.record_rate_limit():
                        detailed_msg = "RATE_LIMIT_EXHAUSTED. Errors: " + "; ".join(
                            error_log
                        )
                        raise RuntimeError(detailed_msg)
                elif "503" in err:
                    time.sleep((2**retries) * 2)
                    retries += 1
                else:
                    self.api.record_failure()
                    raise e

        # Max retries exceeded - include all error details
        detailed_msg = (
            f"Max retries ({max_retries}) exceeded. Errors encountered: "
            + "; ".join(error_log)
        )
        raise RuntimeError(detailed_msg)

    def _handle_total_failure(self, indices, error):
        """Mark entire batch as failed and log to failure log."""
        rows = [self.session.input_rows[i] for i in indices]

        # Create error entries for logging
        item_errors = []
        for i, idx in enumerate(indices):
            self._mark_failed(idx, f"Batch API Error: {error}")
            item_errors.append(
                {
                    "batch_index": i,
                    "error": f"Batch API Error: {error}",
                    "error_type": "api_failure",
                    "model_output": None,
                }
            )

        # Log to failure file
        self.logger.log_batch_errors(indices, item_errors, rows, [])
        self.session.save(self.session_file)

    def _mark_failed(self, idx, msg):
        self.session.retry_counts[idx] = self.session.retry_counts.get(idx, 0) + 1
        record = {
            "index": idx,
            "waterbody": self.session.input_rows[idx].water,
            "error": msg,
        }

        if self.session.retry_counts[idx] >= 3:
            # Permanent failure
            if idx not in [f["index"] for f in self.session.failed_items]:
                self.session.failed_items.append(record)
        else:
            # Transient/Validation failure (retryable)
            exists = next(
                (x for x in self.session.validation_failures if x["index"] == idx), None
            )
            if not exists:
                self.session.validation_failures.append(record)


# --- CLI FUNCTIONS ---


def export_session(session_file: str, output_file: str):
    """Exports session to flat list JSON, preserving exact order."""
    session = SessionState.load(session_file)
    if not session:
        print("Session not found.")
        return

    final_results = []
    for idx in range(session.total_items):
        input_row = session.input_rows[idx]

        if session.results[idx]:
            # Success: Convert to dict
            out = session.results[idx].to_dict()
        else:
            # Failure: Create error placeholder
            fail = next((f for f in session.failed_items if f["index"] == idx), None)
            err = fail["error"] if fail else "Not processed"
            out = {
                "identity": {
                    "name_verbatim": input_row.water,
                    "waterbody_key": "FAILED",
                },
                "regs_verbatim": input_row.raw_regs,
                "rules": [],
                "error": err,
            }

        # Merge Metadata (Order Preserved)
        if hasattr(input_row, "mu"):
            out["mu"] = input_row.mu
        if hasattr(input_row, "symbols"):
            out["symbols"] = input_row.symbols
        if hasattr(input_row, "page"):
            out["page"] = input_row.page
        if hasattr(input_row, "image"):
            out["image"] = input_row.image
        if hasattr(input_row, "region"):
            out["region"] = input_row.region

        final_results.append(out)

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(final_results, f, indent=2, ensure_ascii=False)

    print(f"✓ Exported {len(final_results)} items to {output_file}")


def main():
    parser = argparse.ArgumentParser(description="Strict Fishing Regs Parser")
    parser.add_argument("--file", help="Input raw JSON")
    parser.add_argument("--session-file", default="output/llm_parser/session.json")
    parser.add_argument("--output", default="output/llm_parser/llm_parsed_results.json")
    parser.add_argument(
        "--batch-size", type=int, default=CONFIG["parsing"]["batch_size"]
    )
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--export-session", action="store_true")
    parser.add_argument(
        "--archive-current", 
        action="store_true",
        help="Archive the current session state to partial_sessions folder (for incomplete sessions)"
    )

    args = parser.parse_args()

    if args.export_session:
        export_session(args.session_file, args.output)
        return
    
    if args.archive_current:
        # Load current session and archive it
        session = SessionState.load(args.session_file)
        if session:
            mgr = SessionManager(args.session_file)
            mgr.archive(session, args.output, force_incomplete=True)
            print(f"\nCurrent session archived successfully.")
            print(f"  Items processed: {len(session.processed_items)}/{session.total_items}")
            print(f"  Failed items: {len(session.failed_items)}")
            print(f"  Validation failures: {len(session.validation_failures)}")
        else:
            print(f"Error: No session found at {args.session_file}")
        return

    # --- MAIN PARSING FLOW ---

    # 1. Load Data
    waterbody_rows = []
    if args.file:
        with open(args.file, "r", encoding="utf-8") as f:
            raw = json.load(f)
            waterbody_rows = [
                row
                for page in ExtractionResults.from_dict(raw).pages
                for row in page.rows
            ]

    # 2. Session Setup
    mgr = SessionManager(args.session_file)
    if not waterbody_rows and not args.resume:
        print("Error: Provide --file or --resume")
        return

    if args.resume and not waterbody_rows:
        temp_sess = SessionState.load(args.session_file)
        if temp_sess:
            waterbody_rows = temp_sess.input_rows

    session, is_resuming = mgr.load_or_create(waterbody_rows, args.resume)

    # 3. Revalidation (Catch bad data from previous runs)
    retry_indices = []
    if is_resuming:
        # Check already processed items against NEW strict rules
        retry_indices.extend(mgr.revalidate_existing_results(session))
        retry_indices.extend([f["index"] for f in session.failed_items])
        retry_indices.extend([f["index"] for f in session.validation_failures])
        retry_indices = list(set(retry_indices))

        session.processed_items = [
            i for i in session.processed_items if i not in retry_indices
        ]

    # 4. Processing
    api_manager = APIKeyManager(API_KEYS)
    processor = BatchProcessor(session, args.session_file, api_manager)

    pending_indices = [
        i for i in range(session.total_items) if i not in session.processed_items
    ]
    pending_indices.sort(key=lambda x: 0 if x in retry_indices else 1)  # Retries first

    print(f"\nWork Queue: {len(pending_indices)} items ({len(retry_indices)} retries)")

    for i in range(0, len(pending_indices), args.batch_size):
        if interruption_requested:
            break

        batch = pending_indices[i : i + args.batch_size]

        pct = (len(session.processed_items) / session.total_items) * 100
        print(f"\nBatch {i//args.batch_size + 1} | Progress: {pct:.1f}%")
        print(f"API Status: {api_manager.get_status()}")  # Display API status

        if not processor.run_batch(batch, args.dry_run):
            print("Stopping due to API limits or errors.")
            break

        time.sleep(1)

    # 5. Finalize
    if len(session.processed_items) == session.total_items:
        session.completed_at = datetime.now().isoformat()
        session.save(args.session_file)
        mgr.archive(session, args.output)

    export_session(args.session_file, args.output)

    # Summary
    print(f"\n{'='*60}")
    print(f"Final Summary:")
    print(f"  Total Items: {session.total_items}")
    print(
        f"  Processed: {len(session.processed_items)} ({len(session.processed_items)/session.total_items*100:.1f}%)"
    )
    print(f"  Failed (permanent): {len(session.failed_items)}")
    print(f"  Failed (validation/retry): {len(session.validation_failures)}")

    if session.failed_items:
        print(f"\n  Permanent Failures (max retries exceeded):")
        for fail in session.failed_items[:10]:  # Show first 10
            print(f"    [{fail['index']}] {fail['waterbody']}: {fail['error']}")
        if len(session.failed_items) > 10:
            print(f"    ... and {len(session.failed_items) - 10} more")

    print(f"={'='*60}")
    print("\nDone.")


if __name__ == "__main__":
    main()
