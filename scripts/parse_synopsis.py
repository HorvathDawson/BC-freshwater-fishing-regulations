import json
import os
import argparse
import time
import signal
from datetime import datetime
from google import genai
from google.genai import types
from typing import List, Dict, Any, Optional

# Import shared models and utilities
from synopsis_pipeline.models import (
    WaterbodyRow,
    ExtractionResults,
    ParsedRule,
    ParsedGeographicGroup,
    ParsedWaterbody,
    SessionState,
)
from synopsis_pipeline.prompt_builder import build_prompt
from synopsis_pipeline.config import load_config, get_api_keys
from synopsis_pipeline.waterbody_cleaner import (
    process_and_group_results,
    add_cleaned_names,
)

# Global flag for graceful shutdown
interruption_requested = False

# Load configuration from config.yaml
try:
    CONFIG = load_config()
    API_KEYS = get_api_keys(CONFIG)
except FileNotFoundError:
    print("Warning: config.yaml not found, using default API keys")
    # Fallback to hardcoded keys if config file missing
    API_KEYS = [
        {
            "id": "horvath.dawson",
            "key": os.environ.get(
                "GOOGLE_API_KEY", "AIzaSyBPZigLsxFIU7JOFSux8ZqS03p9-E878VE"
            ),
        },
        {
            "id": "darcy.turin",
            "key": os.environ.get(
                "GOOGLE_API_KEY_2", "AIzaSyC9C-PueILJLJ32bWpUzAV7sQ3R-VXpSUA"
            ),
        },
    ]


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully by finishing current batch before exiting."""
    global interruption_requested
    if not interruption_requested:
        interruption_requested = True
        print("\n\n⚠  Interruption requested (Ctrl+C detected)")
        print("   Will finish current batch and save progress before exiting...\n")
    else:
        print("\n⚠  Second interrupt detected - forcing immediate exit!")
        exit(1)


class APIKeyManager:
    """Manages multiple API keys with automatic rotation on failures."""

    def __init__(
        self,
        api_keys: List[Dict[str, str]],
        max_failures: int = 3,
        max_rate_limit_hits: int = 2,
    ):
        self.api_keys = api_keys
        self.max_failures = max_failures
        self.max_rate_limit_hits = max_rate_limit_hits
        self.current_index = 0
        self.failure_counts = {key["id"]: 0 for key in api_keys}
        self.rate_limit_counts = {
            key["id"]: 0 for key in api_keys
        }  # Track 429 errors separately
        self.clients = {key["id"]: genai.Client(api_key=key["key"]) for key in api_keys}

    def get_current_client(self) -> genai.Client:
        """Get the current active API client."""
        current_key_id = self.api_keys[self.current_index]["id"]
        return self.clients[current_key_id]

    def get_current_key_id(self) -> str:
        """Get the current active API key identifier."""
        return self.api_keys[self.current_index]["id"]

    def record_success(self):
        """Record a successful API call - resets failure count for current key."""
        current_key_id = self.get_current_key_id()
        self.failure_counts[current_key_id] = 0

    def record_rate_limit(self) -> bool:
        """Record a 429 rate limit error and rotate to next key.

        Returns:
            True if we should continue, False if all keys are rate limited
        """
        current_key_id = self.get_current_key_id()
        self.rate_limit_counts[current_key_id] += 1

        print(
            f"  ⚠  Rate limit hit on '{current_key_id}' ({self.rate_limit_counts[current_key_id]}/{self.max_rate_limit_hits})"
        )

        # Immediately rotate to next key
        start_index = self.current_index
        for _ in range(len(self.api_keys)):
            self.current_index = (self.current_index + 1) % len(self.api_keys)
            next_key_id = self.get_current_key_id()

            # Use a key that hasn't hit max rate limits yet
            if self.rate_limit_counts[next_key_id] < self.max_rate_limit_hits:
                print(f"  ↻  Switched to API key '{next_key_id}'")
                return True

            # If we've checked all keys and we're back to start
            if self.current_index == start_index:
                break

        # All keys have hit rate limit max times
        return False

    def record_failure(self) -> bool:
        """Record a failure for current key and rotate if needed.

        Returns:
            True if we should continue (another key available), False if all keys exhausted
        """
        current_key_id = self.get_current_key_id()
        self.failure_counts[current_key_id] += 1

        # Check if current key has hit max failures
        if self.failure_counts[current_key_id] >= self.max_failures:
            print(
                f"  ⚠  API key '{current_key_id}' failed {self.max_failures} times, rotating..."
            )

            # Try to find a key that hasn't failed max times
            start_index = self.current_index
            for _ in range(len(self.api_keys)):
                self.current_index = (self.current_index + 1) % len(self.api_keys)
                next_key_id = self.get_current_key_id()

                if self.failure_counts[next_key_id] < self.max_failures:
                    print(f"  ↻  Switched to API key '{next_key_id}'")
                    return True

                # If we've checked all keys and we're back to start
                if self.current_index == start_index:
                    break

            # All keys have failed max times
            return False

        return True

    def all_keys_exhausted(self) -> bool:
        """Check if all API keys have reached max failures."""
        return all(count >= self.max_failures for count in self.failure_counts.values())

    def all_keys_rate_limited(self) -> bool:
        """Check if all API keys have been rate limited."""
        return all(
            count >= self.max_rate_limit_hits
            for count in self.rate_limit_counts.values()
        )

    def get_status(self) -> str:
        """Get a status string showing failure counts for all keys."""
        status_parts = []
        for key in self.api_keys:
            key_id = key["id"]
            failures = self.failure_counts[key_id]
            rate_limits = self.rate_limit_counts[key_id]
            current = "*" if key_id == self.get_current_key_id() else " "
            status_parts.append(
                f"{current}{key_id}: {failures}/{self.max_failures} fails, {rate_limits}/{self.max_rate_limit_hits} rate-limited"
            )
        return " | ".join(status_parts)


# Initialize API key manager
api_key_manager = APIKeyManager(API_KEYS, max_failures=3, max_rate_limit_hits=2)


class ValidationError(Exception):
    """Custom exception for validation failures."""

    pass


def validate_input_rows(rows: List) -> List[str]:
    """Validate input rows have required fields.

    Args:
        rows: List of waterbody row objects to validate

    Returns:
        List of error messages (empty if valid)
    """
    errors = []

    if not rows or len(rows) == 0:
        errors.append("Input rows list is empty")
        return errors

    for idx, row in enumerate(rows):
        # Check required attributes exist
        if not hasattr(row, "water"):
            errors.append(f"Row {idx}: missing 'water' attribute")
        elif not row.water or not str(row.water).strip():
            errors.append(f"Row {idx}: 'water' is empty")

        if not hasattr(row, "raw_regs"):
            errors.append(f"Row {idx}: missing 'raw_regs' attribute")
        elif not row.raw_regs or not str(row.raw_regs).strip():
            errors.append(f"Row {idx}: 'raw_regs' is empty")

    return errors


def validate_partial_json(
    json_path: str, input_rows: Optional[List] = None
) -> Dict[str, Any]:
    """Validate a session file or parsed results JSON file.

    Automatically detects file type:
    - Session file: has 'input_rows', 'results', 'processed_items', etc.
    - Parsed results: list of waterbody objects

    Args:
        json_path: Path to JSON file to validate
        input_rows: Optional list of input rows for name validation (ignored for session files)

    Returns:
        Dict with 'valid', 'errors', 'warnings', 'file_type', and 'items_checked' keys
    """
    if not os.path.exists(json_path):
        return {
            "valid": False,
            "errors": [f"File not found: {json_path}"],
            "warnings": [],
            "file_type": "unknown",
        }

    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        return {
            "valid": False,
            "errors": [f"Invalid JSON: {e}"],
            "warnings": [],
            "file_type": "unknown",
        }

    all_errors = []
    all_warnings = []
    file_type = "unknown"

    # Detect file type
    if (
        isinstance(data, dict)
        and "input_rows" in data
        and "results" in data
        and "processed_items" in data
    ):
        # This is a session file
        file_type = "session"

        try:
            session = SessionState.from_dict(data)

            # Use input_rows from session itself
            session_input_rows = session.input_rows

            # Validate session structure
            if session.total_items != len(session.input_rows):
                all_errors.append(
                    f"Session total_items ({session.total_items}) doesn't match input_rows count ({len(session.input_rows)})"
                )

            if len(session.results) != session.total_items:
                all_errors.append(
                    f"Session results array length ({len(session.results)}) doesn't match total_items ({session.total_items})"
                )

            # Validate each processed result
            items_checked = 0
            for idx in session.processed_items:
                if idx >= len(session.results):
                    all_errors.append(
                        f"Processed item index {idx} out of bounds (results length: {len(session.results)})"
                    )
                    continue

                result = session.results[idx]
                if result is None:
                    all_errors.append(
                        f"Item {idx}: Marked as processed but result is None"
                    )
                    continue

                # Validate the parsed waterbody
                try:
                    expected_name = (
                        session_input_rows[idx].water
                        if idx < len(session_input_rows)
                        else None
                    )
                    expected_raw_text = (
                        session_input_rows[idx].raw_regs
                        if idx < len(session_input_rows)
                        else None
                    )
                    errors = result.validate(expected_name, expected_raw_text)

                    if errors:
                        all_errors.extend(
                            [
                                f"Item {idx} ({result.waterbody_name}): {err}"
                                for err in errors
                            ]
                        )
                    items_checked += 1
                except Exception as e:
                    all_errors.append(f"Item {idx}: Validation failed - {e}")

            # Report on unprocessed items
            unprocessed_count = (
                session.total_items
                - len(session.processed_items)
                - len(session.failed_items)
                - len(session.validation_failures)
            )
            if unprocessed_count > 0:
                all_warnings.append(f"{unprocessed_count} items not yet processed")

            # Report on failed items
            if session.failed_items:
                all_warnings.append(
                    f"{len(session.failed_items)} items permanently failed"
                )
                for failed in session.failed_items[:5]:
                    all_warnings.append(
                        f"  - Index {failed['index']}: {failed.get('waterbody', 'unknown')}"
                    )

            # Report on validation failures
            if session.validation_failures:
                all_warnings.append(
                    f"{len(session.validation_failures)} items failed validation (will retry on --resume)"
                )
                for failed in session.validation_failures[:5]:
                    all_warnings.append(
                        f"  - Index {failed['index']}: {failed.get('waterbody', 'unknown')}"
                    )

            return {
                "valid": len(all_errors) == 0,
                "errors": all_errors,
                "warnings": all_warnings,
                "file_type": file_type,
                "items_checked": items_checked,
                "session_info": {
                    "total_items": session.total_items,
                    "processed": len(session.processed_items),
                    "failed": len(session.failed_items),
                    "validation_failed": len(session.validation_failures),
                    "created_at": session.created_at,
                    "last_updated": session.last_updated,
                    "completed_at": session.completed_at,
                },
            }

        except Exception as e:
            all_errors.append(f"Failed to load session: {e}")
            return {
                "valid": False,
                "errors": all_errors,
                "warnings": all_warnings,
                "file_type": file_type,
            }

    elif isinstance(data, list):
        # This is a parsed results file
        file_type = "parsed_results"

        for idx, item in enumerate(data):
            # Check for error placeholders
            if isinstance(item, dict) and "error" in item:
                all_warnings.append(
                    f"Item {idx} ({item.get('waterbody_name', 'unknown')}): {item['error']}"
                )
                continue

            try:
                # Convert to dataclass and validate
                parsed = ParsedWaterbody.from_dict(item)
                expected_name = (
                    input_rows[idx].water
                    if (input_rows and idx < len(input_rows))
                    else None
                )
                expected_raw_text = (
                    input_rows[idx].raw_regs
                    if (input_rows and idx < len(input_rows))
                    else None
                )
                errors = parsed.validate(expected_name, expected_raw_text)

                if errors:
                    all_errors.extend(
                        [
                            f"Item {idx} ({parsed.waterbody_name}): {err}"
                            for err in errors
                        ]
                    )
            except Exception as e:
                all_errors.append(f"Item {idx}: Failed to parse - {e}")

        return {
            "valid": len(all_errors) == 0,
            "errors": all_errors,
            "warnings": all_warnings,
            "file_type": file_type,
            "items_checked": len(data),
        }

    else:
        return {
            "valid": False,
            "errors": [
                "JSON must be either a session object or a list of waterbody results"
            ],
            "warnings": all_warnings,
            "file_type": "unknown",
        }


def revalidate_session_results(session: "SessionState") -> List[int]:
    """Revalidate all processed items in session with current validation rules.

    This allows validation improvements to catch previously-processed items that
    now fail validation. Returns list of indices that need reprocessing.

    Args:
        session: SessionState with processed results

    Returns:
        List of indices that failed revalidation
    """
    failed_indices = []

    print(f"\n{'='*80}")
    print(
        f"Revalidating {len(session.processed_items)} processed items with current validation rules..."
    )
    print(f"{'='*80}")

    for idx in session.processed_items:
        if idx >= len(session.results) or session.results[idx] is None:
            print(f"⚠ Item {idx}: Result is None, marking for reprocessing")
            failed_indices.append(idx)
            continue

        result = session.results[idx]
        expected_name = session.input_rows[idx].water
        expected_raw_text = session.input_rows[idx].raw_regs

        # Validate with current rules
        errors = result.validate(expected_name, expected_raw_text)

        if errors:
            print(f"✗ Item {idx} ({result.waterbody_name}): Failed revalidation")
            for err in errors[:3]:
                print(f"    - {err}")
            if len(errors) > 3:
                print(f"    ... and {len(errors) - 3} more errors")
            failed_indices.append(idx)

    if failed_indices:
        print(
            f"\n⚠ {len(failed_indices)} items failed revalidation and will be reprocessed"
        )
        print(
            f"Indices: {failed_indices[:20]}{'...' if len(failed_indices) > 20 else ''}"
        )
    else:
        print(
            f"\n✓ All {len(session.processed_items)} processed items passed revalidation"
        )

    return failed_indices


class SynopsisParser:
    """Parser for fishing regulation synopsis data using LLM."""

    @staticmethod
    def get_prompt(waterbody_rows: List):
        """Get the LLM prompt for parsing regulations.

        Args:
            waterbody_rows: List of WaterbodyRow objects with water and raw_regs attributes
        """
        return build_prompt(waterbody_rows)

    @classmethod
    def parse_synopsis_batch(
        cls,
        waterbody_rows: List,
        api_manager: APIKeyManager = None,
        dry_run: bool = False,
    ):
        """
        Parse a list of WaterbodyRow objects with API key rotation.

        Args:
            waterbody_rows: List of WaterbodyRow objects with water and raw_regs attributes
            api_manager: APIKeyManager instance for handling multiple keys
            dry_run: If True, skip the actual API call and return early
        """
        if api_manager is None:
            api_manager = api_key_manager

        try:
            prompt = cls.get_prompt(waterbody_rows)

            # Dry run mode - stop here before making API call
            if dry_run:
                print("\n[DRY RUN] Stopping before LLM API call")
                print(f"  Would process batch of {len(waterbody_rows)} items")
                print(f"  Prompt length: {len(prompt)} characters")
                return {"dry_run": True, "batch_size": len(waterbody_rows)}

            current_client = api_manager.get_current_client()

            response = current_client.models.generate_content(
                # model='gemini-2.5-flash-lite', # Updated to the latest stable flash
                # model='gemini-2.5-flash',
                model="gemini-3-flash-preview",
                # model='gemini-2.0-flash',
                # model='gemma-3-27b-it',
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    # temperature=0.1,
                    cached_content=None,
                ),
            )

            if response.text:
                # Parse JSON - if malformed, this will raise JSONDecodeError
                try:
                    parsed_result = json.loads(response.text)
                except json.JSONDecodeError as e:
                    api_manager.record_failure()
                    return {"error": f"Malformed JSON from model: {e}"}

                # Validate batch structure (count and basic format)
                if not isinstance(parsed_result, list):
                    return {
                        "error": f"Result is not a list, got {type(parsed_result).__name__}"
                    }

                if len(parsed_result) != len(waterbody_rows):
                    return {
                        "error": f"Expected {len(waterbody_rows)} items, got {len(parsed_result)}"
                    }

                # Validate each item individually and collect results
                validated_results = []
                item_errors = []

                for idx, entry in enumerate(parsed_result):
                    try:
                        # Validate this specific item
                        input_row = waterbody_rows[idx]

                        # Check name matches (ORDER + VERBATIM VALIDATION)
                        if entry.get("waterbody_name") != input_row.water:
                            error_msg = f"Name/order mismatch - expected '{input_row.water}', got '{entry.get('waterbody_name')}'"
                            item_errors.append(
                                {
                                    "batch_index": idx,
                                    "waterbody": input_row.water,
                                    "error": error_msg,
                                    "error_type": "name_mismatch",
                                }
                            )
                            validated_results.append(None)
                            continue

                        # Convert to dataclass and validate structure
                        parsed = ParsedWaterbody.from_dict(entry)
                        validation_errors = parsed.validate(
                            input_row.water, input_row.raw_regs
                        )

                        if validation_errors:
                            error_msg = "; ".join(validation_errors[:3]) + (
                                "..." if len(validation_errors) > 3 else ""
                            )
                            item_errors.append(
                                {
                                    "batch_index": idx,
                                    "waterbody": input_row.water,
                                    "error": error_msg,
                                    "error_type": "validation_error",
                                    "all_errors": validation_errors,
                                    "model_output": entry,  # Store the original output before setting to None
                                }
                            )
                            validated_results.append(None)
                        else:
                            # Item passed validation
                            validated_results.append(entry)

                    except Exception as e:
                        item_errors.append(
                            {
                                "batch_index": idx,
                                "waterbody": waterbody_rows[idx].water,
                                "error": f"Failed to parse - {str(e)}",
                                "error_type": "parse_error",
                            }
                        )
                        validated_results.append(None)

                # Return results with partial success info
                api_manager.record_success()
                return {
                    "results": validated_results,
                    "item_errors": item_errors,
                    "success_count": len(
                        [r for r in validated_results if r is not None]
                    ),
                    "failed_count": len(item_errors),
                }
            else:
                api_manager.record_failure()
                return {"error": "Empty response from model"}
        except ValidationError as e:
            # Validation errors should trigger retries (model didn't follow instructions)
            # But don't count as API failures
            return {"error": str(e)}
        except Exception as e:
            error_msg = str(e)

            # Check for 429 rate limit errors
            if (
                "429" in error_msg
                or "rate limit" in error_msg.lower()
                or "quota" in error_msg.lower()
            ):
                # 429 Too Many Requests - immediately rotate key
                can_continue = api_manager.record_rate_limit()

                if can_continue:
                    # Retry immediately with next key
                    return cls.parse_synopsis_batch(waterbody_rows, api_manager)
                else:
                    # All keys are rate limited - save progress and exit
                    return {
                        "error": f"RATE_LIMIT_EXHAUSTED: All API keys have been rate limited. Status: {api_manager.get_status()}"
                    }

            # Check for 503 service unavailable errors
            elif (
                "503" in error_msg
                or "service unavailable" in error_msg.lower()
                or "temporarily unavailable" in error_msg.lower()
            ):
                # 503 Service Unavailable - retry with exponential backoff (3 attempts)
                max_retries = 3
                for retry in range(max_retries):
                    wait_time = (2**retry) * 2  # 2s, 4s, 8s
                    print(
                        f"  ⚠  Service unavailable (503), retry {retry + 1}/{max_retries} after {wait_time}s..."
                    )
                    time.sleep(wait_time)

                    try:
                        # Retry the same request
                        current_client = api_manager.get_current_client()
                        response = current_client.models.generate_content(
                            model="gemini-2.5-flash-lite",
                            contents=cls.get_prompt(waterbody_rows),
                            config=types.GenerateContentConfig(
                                response_mime_type="application/json",
                                temperature=0.1,
                                cached_content=None,
                            ),
                        )

                        if response.text:
                            # Success - process as normal
                            try:
                                parsed_result = json.loads(response.text)
                            except json.JSONDecodeError as e:
                                api_manager.record_failure()
                                return {"error": f"Malformed JSON from model: {e}"}

                            # Validate batch structure
                            if not isinstance(parsed_result, list):
                                return {
                                    "error": f"Result is not a list, got {type(parsed_result).__name__}"
                                }

                            if len(parsed_result) != len(waterbody_rows):
                                return {
                                    "error": f"Expected {len(waterbody_rows)} items, got {len(parsed_result)}"
                                }

                            # Continue with validation as in original code
                            validated_results = []
                            item_errors = []

                            for idx, entry in enumerate(parsed_result):
                                try:
                                    input_row = waterbody_rows[idx]

                                    if entry.get("waterbody_name") != input_row.water:
                                        error_msg = f"Name/order mismatch - expected '{input_row.water}', got '{entry.get('waterbody_name')}'"
                                        item_errors.append(
                                            {
                                                "batch_index": idx,
                                                "waterbody": input_row.water,
                                                "error": error_msg,
                                                "error_type": "name_mismatch",
                                            }
                                        )
                                        validated_results.append(None)
                                        continue

                                    parsed = ParsedWaterbody.from_dict(entry)
                                    validation_errors = parsed.validate(
                                        input_row.water, input_row.raw_regs
                                    )

                                    if validation_errors:
                                        error_msg = "; ".join(validation_errors[:3]) + (
                                            "..." if len(validation_errors) > 3 else ""
                                        )
                                        item_errors.append(
                                            {
                                                "batch_index": idx,
                                                "waterbody": input_row.water,
                                                "error": error_msg,
                                                "error_type": "validation_error",
                                                "all_errors": validation_errors,
                                                "model_output": entry,
                                            }
                                        )
                                        validated_results.append(None)
                                    else:
                                        validated_results.append(parsed)
                                        api_manager.record_success()

                                except Exception as e:
                                    item_errors.append(
                                        {
                                            "batch_index": idx,
                                            "waterbody": (
                                                waterbody_rows[idx].water
                                                if idx < len(waterbody_rows)
                                                else "unknown"
                                            ),
                                            "error": str(e),
                                            "error_type": "parse_error",
                                        }
                                    )
                                    validated_results.append(None)

                            if item_errors:
                                return {
                                    "results": validated_results,
                                    "errors": item_errors,
                                    "raw_response": parsed_result,
                                }

                            return {"results": validated_results}

                    except Exception as retry_error:
                        retry_msg = str(retry_error)
                        # If still 503, continue to next retry
                        if (
                            "503" in retry_msg
                            or "service unavailable" in retry_msg.lower()
                        ):
                            if retry < max_retries - 1:
                                continue  # Try next retry
                            else:
                                return {
                                    "error": f"Service unavailable (503) after {max_retries} retries"
                                }
                        else:
                            # Different error occurred
                            return {"error": f"Error during retry: {retry_msg}"}

                # All retries exhausted
                return {
                    "error": f"Service unavailable (503) after {max_retries} retries with exponential backoff"
                }

            # Other errors - record as regular failure
            else:
                api_manager.record_failure()
                return {"error": error_msg}


# --- FAILURE LOGGING ---


def log_failure_details(
    failure_log_file: str,
    batch_indices: List[int],
    item_errors: List[Dict],
    waterbody_rows: List,
    parsed_result: List = None,
):
    """
    Log detailed failure information to a file for analysis and prompt improvement.

    Args:
        failure_log_file: Path to the failure log file
        batch_indices: Indices of items in the batch
        item_errors: List of error dictionaries with keys: batch_index, waterbody, error, error_type
        waterbody_rows: Full list of input rows
        parsed_result: The full parsed result from LLM (optional, for debugging)
    """
    os.makedirs(os.path.dirname(failure_log_file), exist_ok=True)

    # Prepare log entry
    log_entry = {
        "timestamp": datetime.now().isoformat(),
        "batch_indices": batch_indices,
        "failures": [],
    }

    for error_info in item_errors:
        batch_idx = error_info["batch_index"]
        actual_idx = batch_indices[batch_idx]
        input_row = waterbody_rows[actual_idx]

        failure_detail = {
            "index": actual_idx,
            "waterbody_name": input_row.water,
            "raw_regs": input_row.raw_regs,
            "error_type": error_info["error_type"],
            "error": error_info["error"],
        }

        # Add full model output - prefer from error_info (has original output), fallback to parsed_result
        if "model_output" in error_info:
            failure_detail["model_output"] = error_info["model_output"]
        elif parsed_result and batch_idx < len(parsed_result):
            failure_detail["model_output"] = parsed_result[batch_idx]

        # Add all validation errors if available
        if "all_errors" in error_info:
            failure_detail["all_validation_errors"] = error_info["all_errors"]

        log_entry["failures"].append(failure_detail)

    # Append to log file
    file_exists = os.path.exists(failure_log_file)
    with open(failure_log_file, "a", encoding="utf-8") as f:
        if file_exists:
            f.write(",\n")
        else:
            f.write("[\n")
        json.dump(log_entry, f, indent=2, ensure_ascii=False)

    # Create a summary file for easier review
    summary_file = failure_log_file.replace(".json", "_summary.txt")
    with open(summary_file, "a", encoding="utf-8") as f:
        f.write(f"\n{'='*80}\n")
        f.write(f"Batch Failures at {log_entry['timestamp']}\n")
        f.write(f"{'='*80}\n\n")
        for failure in log_entry["failures"]:
            f.write(f"[{failure['index']}] {failure['waterbody_name']}\n")
            f.write(f"{'-'*80}\n")
            f.write(f"Error Type: {failure['error_type']}\n")
            f.write(f"Error: {failure['error']}\n\n")

            f.write(f"Full Input (raw_regs):\n")
            f.write(f"{failure['raw_regs']}\n\n")

            if "model_output" in failure and failure["model_output"]:
                f.write(f"Model Output:\n")
                f.write(
                    json.dumps(failure["model_output"], indent=2, ensure_ascii=False)
                )
                f.write("\n\n")

            if "all_validation_errors" in failure:
                f.write(f"All Validation Errors:\n")
                for err in failure["all_validation_errors"]:
                    f.write(f"  - {err}\n")
                f.write("\n")


# --- BATCH DEBUG RUNNER ---


def run_llm_parsing(
    waterbody_rows: Optional[List] = None,
    output_file="output/llm_parser/llm_parsed_results.json",
    batch_size=10,
    session_file="output/llm_parser/session.json",
    resume=False,
    dry_run=False,
):
    """
    Run LLM parsing with batching support and progress tracking.

    Args:
        waterbody_rows: List of waterbody objects to parse (optional if resuming)
        output_file: Final output file path
        batch_size: Number of items to process per batch (smaller = more consistent, less rate limiting)
        session_file: Path to save/load session state (JSON file)
        resume: Whether to resume from previous session
        dry_run: If True, stop before making API calls
    """
    # Set up signal handler for graceful Ctrl+C
    global interruption_requested
    interruption_requested = False
    signal.signal(signal.SIGINT, signal_handler)

    parser = SynopsisParser()
    print(f"\n{'='*80}\nRunning LLM Batch Parsing...\n{'='*80}")

    # Validate input rows if provided (before loading session)
    if waterbody_rows is not None:
        input_errors = validate_input_rows(waterbody_rows)
        if input_errors:
            print(f"\n✗ Input validation failed:")
            for err in input_errors[:10]:
                print(f"  - {err}")
            if len(input_errors) > 10:
                print(f"  ... and {len(input_errors) - 10} more errors")
            print(f"\nFix input data before running parser.")
            exit(1)
        print(f"✓ Input validation passed ({len(waterbody_rows)} rows)")

    # Load or create session state
    session = None
    is_resuming = (
        resume  # Track if we're resuming (either explicit flag or user choice)
    )

    # Check if session file exists
    existing_session = SessionState.load(session_file)

    if existing_session and len(existing_session.processed_items) > 0:
        # Session file exists with completed items
        if resume:
            # User explicitly requested resume
            session = existing_session
            waterbody_rows = session.input_rows  # Load from session
            print(
                f"✓ Resumed from session file: {len(session.processed_items)}/{session.total_items} items completed"
            )
            print(f"   Session created: {session.created_at}")
            print(f"   Last updated: {session.last_updated}")
        else:
            # Ask user if they want to resume
            print(
                f"\n⚠ Found existing session: {len(existing_session.processed_items)}/{existing_session.total_items} items completed"
            )
            print(f"   Session file: {session_file}")
            print(f"   Created: {existing_session.created_at}")

            response = (
                input("\nDo you want to resume from this session? [Y/n]: ")
                .strip()
                .lower()
            )

            if response in ("", "y", "yes"):
                session = existing_session
                waterbody_rows = session.input_rows  # Load from session
                is_resuming = True  # User chose to resume
                print(f"✓ Resuming from existing session...")
            else:
                print(f"✓ Starting fresh (old session will be overwritten)")
                # Delete old session file
                if os.path.exists(session_file):
                    os.remove(session_file)
    elif resume:
        print("⚠ --resume flag provided but no session file found")
        if waterbody_rows is None:
            print(
                "✗ Error: Cannot resume without session file and no input data provided"
            )
            print("   Either provide --file or use an existing session")
            exit(1)

    # Check if we have input data
    if waterbody_rows is None:
        print("✗ Error: No input data provided. Use --file to specify input data.")
        exit(1)

    if session is None:
        session = SessionState.create_new(waterbody_rows)

    total_items = session.total_items
    print(f"Total items to process: {total_items}")
    print(f"Batch size: {batch_size}")
    print(
        f"API keys available: {len(API_KEYS)} ({', '.join(k['id'] for k in API_KEYS)})"
    )

    # If resuming, include permanently failed items and validation failures for retry
    # Revalidate ALL processed items to catch any that now fail with updated validation rules
    revalidation_failed = []
    if is_resuming and (
        len(session.processed_items) > 0
        or len(session.failed_items) > 0
        or len(session.validation_failures) > 0
    ):
        # Revalidate all processed items with current validation rules
        revalidation_failed = revalidate_session_results(session)

        # Add permanently failed items for retry on resume
        permanently_failed_indices = [f["index"] for f in session.failed_items]
        if permanently_failed_indices:
            print(
                f"⚠  Retrying {len(permanently_failed_indices)} previously failed items"
            )
            # Add to revalidation_failed list (avoid duplicates)
            for idx in permanently_failed_indices:
                if idx not in revalidation_failed:
                    revalidation_failed.append(idx)

        # ALWAYS retry validation failures on resume (these are likely prompt/validation issues)
        validation_failed_indices = [f["index"] for f in session.validation_failures]
        if validation_failed_indices:
            print(f"⚠  Retrying {len(validation_failed_indices)} validation failures")
            # Add to revalidation_failed list (avoid duplicates)
            for idx in validation_failed_indices:
                if idx not in revalidation_failed:
                    revalidation_failed.append(idx)

            # Clear validation_failures list - they're being retried
            session.validation_failures = []

        if revalidation_failed:
            # Remove failed items from processed_items list
            session.processed_items = [
                i for i in session.processed_items if i not in revalidation_failed
            ]

            # Clear their results so they get reprocessed
            for idx in revalidation_failed:
                session.results[idx] = None
                # Reset retry count for fresh attempts
                if idx in session.retry_counts:
                    del session.retry_counts[idx]

            # Remove from failed_items if they were there
            session.failed_items = [
                f for f in session.failed_items if f["index"] not in revalidation_failed
            ]

            # Save updated session
            session.save(session_file)
            print(
                f"✓  Session updated: {len(revalidation_failed)} items marked for reprocessing\n"
            )

            # Reprocess failed items in batches BEFORE continuing with unprocessed items
            print(f"{'─'*80}")
            print(f"REVALIDATION ({len(revalidation_failed)} items)")
            print(f"{'─'*80}")

            revalidation_start_time = datetime.now()

            for batch_start in range(0, len(revalidation_failed), batch_size):
                batch_indices = revalidation_failed[
                    batch_start : batch_start + batch_size
                ]
                batch_rows = [waterbody_rows[i] for i in batch_indices]

                batch_num = batch_start // batch_size + 1
                total_batches = (
                    len(revalidation_failed) + batch_size - 1
                ) // batch_size

                # Calculate progress
                revalidation_completed = batch_start
                revalidation_progress_pct = (
                    revalidation_completed / len(revalidation_failed)
                ) * 100
                total_session_completed = len(session.processed_items)
                total_session_progress_pct = (
                    total_session_completed / total_items
                ) * 100

                print(
                    f"\n[Batch {batch_num}/{total_batches}] Indices {batch_indices[0]}-{batch_indices[-1]} | "
                    f"Revalidation: {revalidation_progress_pct:.0f}% | Overall: {total_session_progress_pct:.0f}%"
                )
                print(f"  API Keys: {api_key_manager.get_status()}")

                # Check for interruption before starting revalidation batch
                if interruption_requested:
                    print(
                        f"\n⚠  Interruption detected - stopping before revalidation batch {batch_num}"
                    )
                    print(
                        f"   Saving session with {len(session.processed_items)}/{total_items} items completed..."
                    )
                    session.save(session_file)
                    print(f"   ✓ Session saved to: {session_file}")
                    print(f"\n   Run with --resume to continue from this point.")
                    return None

                # Parse batch
                batch_results = parser.parse_synopsis_batch(batch_rows, dry_run=dry_run)

                # Handle dry run mode
                if isinstance(batch_results, dict) and batch_results.get("dry_run"):
                    print(f"\n{'='*80}")
                    print("DRY RUN COMPLETE")
                    print(f"{'='*80}")
                    print(f"✓ All setup and validation completed successfully")
                    print(
                        f"✓ Would have processed {batch_results.get('batch_size', 0)} items in first batch"
                    )
                    print(f"✓ Session ready at: {session_file}")
                    print(f"\nRun without --dry-run to begin actual processing")
                    return None

                # Check for complete batch errors (API errors, etc.)
                if (
                    isinstance(batch_results, dict)
                    and "error" in batch_results
                    and "results" not in batch_results
                ):
                    # Complete batch failure (API error, malformed JSON, etc.)
                    error_msg = batch_results["error"]
                    print(f"  ✗ Batch Failed: {error_msg}")

                    # Check if all API keys are rate limited
                    if error_msg.startswith("RATE_LIMIT_EXHAUSTED"):
                        print(f"\n⚠  All API keys have been rate limited!")
                        print(f"   Key status: {api_key_manager.get_status()}")
                        print(f"   Session saved to: {session_file}")
                        print(
                            f"\n   Wait for rate limits to reset and run with --resume to continue."
                        )
                        session.save(session_file)
                        return None

                    # Check if all API keys are exhausted
                    if api_key_manager.all_keys_exhausted():
                        print(f"\n⚠  All API keys exhausted!")
                        print(f"   Key status: {api_key_manager.get_status()}")
                        print(f"   Session saved to: {session_file}")
                        print(
                            f"\n   Wait for quota reset and run with --resume to continue."
                        )
                        session.save(session_file)
                        return None

                    # Track retry counts for entire batch
                    max_retries = 3
                    for idx in batch_indices:
                        retry_count = session.retry_counts.get(idx, 0)
                        session.retry_counts[idx] = retry_count + 1

                        if session.retry_counts[idx] >= max_retries:
                            # Mark as permanently failed
                            if idx not in [f["index"] for f in session.failed_items]:
                                session.failed_items.append(
                                    {
                                        "index": idx,
                                        "waterbody": waterbody_rows[idx].water,
                                        "error": f"Revalidation batch error: {error_msg}",
                                        "retries": retry_count + 1,
                                    }
                                )
                            print(
                                f"    ✗ Item {idx} permanently failed after {max_retries} retries"
                            )

                    session.save(session_file)
                    continue

                # Handle partial batch success (new format)
                if isinstance(batch_results, dict) and "results" in batch_results:
                    results_list = batch_results["results"]
                    item_errors = batch_results.get("item_errors", [])
                    success_count = batch_results.get("success_count", 0)
                    failed_count = batch_results.get("failed_count", 0)

                    # Process successful items - preserve order by mapping batch index to actual index
                    for i, result_dict in enumerate(results_list):
                        if result_dict is not None and i < len(batch_indices):
                            idx = batch_indices[
                                i
                            ]  # Map batch position to actual dataset position
                            # Convert dict to ParsedWaterbody instance
                            parsed_waterbody = ParsedWaterbody.from_dict(result_dict)
                            # Store in correct position - session.results[idx] maintains input order
                            session.results[idx] = parsed_waterbody
                            if idx not in session.processed_items:
                                session.processed_items.append(idx)
                            # Reset retry count on success
                            if idx in session.retry_counts:
                                del session.retry_counts[idx]

                    # Log failed items for analysis
                    if item_errors:
                        failure_log_file = "output/llm_parser/failure_log.json"
                        log_failure_details(
                            failure_log_file,
                            batch_indices,
                            item_errors,
                            waterbody_rows,
                            results_list,
                        )

                        # Track individual item failures
                        for error_info in item_errors:
                            batch_idx = error_info[
                                "batch_index"
                            ]  # Index within the batch (0-9)
                            actual_idx = batch_indices[
                                batch_idx
                            ]  # Actual index in full dataset
                            retry_count = session.retry_counts.get(actual_idx, 0)
                            session.retry_counts[actual_idx] = retry_count + 1

                    if success_count == len(batch_indices):
                        print(
                            f"  ✓ Success: {success_count}/{len(batch_indices)} items succeeded"
                        )
                    else:
                        print(
                            f"  ✓ Partial Success: {success_count}/{len(batch_indices)} items succeeded"
                        )

                    if failed_count > 0:
                        print(f"    Failed items logged to failure_log.json")

                # Check for interruption after completing revalidation batch
                if interruption_requested:
                    print(
                        f"\n⚠  Interruption detected - revalidation batch {batch_num} completed"
                    )
                    print(
                        f"   Saving session with {len(session.processed_items)}/{total_items} items completed..."
                    )
                    session.save(session_file)
                    print(f"   ✓ Session saved to: {session_file}")
                    print(f"\n   Run with --resume to continue from this point.")
                    return None

                # Handle old format (full list of dicts) for backwards compatibility
                elif isinstance(batch_results, list):
                    for i, result_dict in enumerate(batch_results):
                        if i < len(batch_indices):
                            idx = batch_indices[
                                i
                            ]  # Map batch position to actual dataset position
                            # Convert dict to ParsedWaterbody instance
                            parsed_waterbody = ParsedWaterbody.from_dict(result_dict)
                            # Store in correct position - session.results[idx] maintains input order
                            session.results[idx] = parsed_waterbody
                            if idx not in session.processed_items:
                                session.processed_items.append(idx)

                    print(f"  ✓ Success")
                else:
                    # Unexpected format - treat as complete batch failure and retry
                    print(f"  ✗ Unexpected result format: {type(batch_results)}")

                    # Track retry counts for entire batch
                    max_retries = 3
                    for idx in batch_indices:
                        retry_count = session.retry_counts.get(idx, 0)
                        session.retry_counts[idx] = retry_count + 1

                        if session.retry_counts[idx] >= max_retries:
                            # Mark as permanently failed
                            if idx not in [f["index"] for f in session.failed_items]:
                                session.failed_items.append(
                                    {
                                        "index": idx,
                                        "waterbody": waterbody_rows[idx].water,
                                        "error": f"Revalidation batch error: Unexpected result format {type(batch_results)}",
                                        "retries": retry_count + 1,
                                    }
                                )
                            print(
                                f"    ✗ Item {idx} permanently failed after {max_retries} retries"
                            )

                    session.save(session_file)
                    continue

                # Save after each batch
                session.save(session_file)

                # Small delay between batches
                if batch_start + batch_size < len(revalidation_failed):
                    time.sleep(1)

            # Summary of revalidation reprocessing
            revalidation_elapsed = (
                datetime.now() - revalidation_start_time
            ).total_seconds()
            revalidation_success = len(
                [i for i in revalidation_failed if i in session.processed_items]
            )
            revalidation_failed_count = len(
                [i for i in revalidation_failed if i not in session.processed_items]
            )

            print(f"\n{'─'*80}")
            if revalidation_failed_count > 0:
                print(
                    f"Revalidation complete: {revalidation_success}/{len(revalidation_failed)} succeeded ({int(revalidation_elapsed)}s)"
                )
            else:
                print(
                    f"Revalidation complete: All {revalidation_success} items succeeded ({int(revalidation_elapsed)}s)"
                )
            print(f"{'─'*80}\n")

    # Determine which items need processing
    # Only exclude successfully processed items
    # Failed items will be retried when user manually resumes (after deleting session file)
    items_to_process = [
        i for i in range(total_items) if i not in session.processed_items
    ]

    if not items_to_process:
        print("✓  All items already processed!")
        # Compile final results from parsed class instances - maintain order
        final_results = []
        for idx in range(total_items):
            input_row = waterbody_rows[idx]

            if session.results[idx] is not None:
                # Convert to dict and merge with input metadata
                result_dict = session.results[idx].to_dict()

                # Add metadata from input row
                if hasattr(input_row, "mu"):
                    result_dict["mu"] = input_row.mu
                if hasattr(input_row, "symbols"):
                    result_dict["symbols"] = input_row.symbols
                if hasattr(input_row, "page"):
                    result_dict["page"] = input_row.page
                if hasattr(input_row, "image"):
                    result_dict["image"] = input_row.image
                if hasattr(input_row, "region"):
                    result_dict["region"] = input_row.region

                final_results.append(result_dict)
            else:
                # Include error placeholder for failed items to maintain order
                failed_info = next(
                    (f for f in session.failed_items if f["index"] == idx), None
                )
                if not failed_info:
                    failed_info = next(
                        (f for f in session.validation_failures if f["index"] == idx),
                        None,
                    )
                error_msg = failed_info["error"] if failed_info else "Not processed"
                placeholder = {
                    "waterbody_name": input_row.water,
                    "error": f"FAILED_TO_PARSE: {error_msg}",
                    "raw_text": input_row.raw_regs,
                    "cleaned_text": "",
                    "geographic_groups": [],
                }
                # Add metadata fields
                if hasattr(input_row, "mu"):
                    placeholder["mu"] = input_row.mu
                if hasattr(input_row, "symbols"):
                    placeholder["symbols"] = input_row.symbols
                if hasattr(input_row, "page"):
                    placeholder["page"] = input_row.page
                if hasattr(input_row, "image"):
                    placeholder["image"] = input_row.image
                if hasattr(input_row, "region"):
                    placeholder["region"] = input_row.region
                final_results.append(placeholder)

        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(final_results, f, indent=2, ensure_ascii=False)
        print(f"✓  Saved final results to: {output_file}")

        # Mark session as complete and archive if fully successful
        if (
            len(session.processed_items) == total_items
            and not session.failed_items
            and not session.validation_failures
        ):
            if session.completed_at is None:
                session.completed_at = datetime.now().isoformat()
                session.save(session_file)
                print(f"✓  Session marked as complete at {session.completed_at}")

            # Archive completed session
            archive_completed_session(session_file, output_file, session)

        return session.results

    # Track timing for progress estimates
    start_time = datetime.now()

    # Process in batches
    print(f"{'─'*80}")
    print(f"PROCESSING ({len(items_to_process)} items remaining)")
    print(f"{'─'*80}")
    print("(Press Ctrl+C to gracefully stop after current batch)\n")

    for batch_start in range(0, len(items_to_process), batch_size):
        batch_indices = items_to_process[batch_start : batch_start + batch_size]
        batch_rows = [waterbody_rows[i] for i in batch_indices]

        batch_num = batch_start // batch_size + 1
        total_batches = (len(items_to_process) + batch_size - 1) // batch_size

        # Calculate progress
        normal_completed_so_far = batch_start
        completed_so_far = len(session.processed_items)
        progress_pct = (completed_so_far / total_items) * 100

        # Estimate time remaining
        time_str = ""
        if normal_completed_so_far > 0:
            elapsed = (datetime.now() - start_time).total_seconds()
            items_per_second = normal_completed_so_far / elapsed
            remaining_items = len(items_to_process) - normal_completed_so_far
            est_seconds = (
                remaining_items / items_per_second if items_per_second > 0 else 0
            )

            if est_seconds < 60:
                time_str = f" | ETA: {int(est_seconds)}s"
            elif est_seconds < 3600:
                time_str = f" | ETA: {int(est_seconds / 60)}m"
            else:
                time_str = f" | ETA: {int(est_seconds / 3600)}h {int((est_seconds % 3600) / 60)}m"

        print(
            f"\n[Batch {batch_num}/{total_batches}] Indices {batch_indices[0]}-{batch_indices[-1]} | "
            f"Overall: {progress_pct:.0f}%{time_str}"
        )
        print(f"  API Keys: {api_key_manager.get_status()}")

        # Check for interruption before starting batch
        if interruption_requested:
            print(f"\n⚠  Interruption detected - stopping before batch {batch_num}")
            print(
                f"   Saving session with {len(session.processed_items)}/{total_items} items completed..."
            )
            session.save(session_file)
            print(f"   ✓ Session saved to: {session_file}")
            print(f"\n   Run with --resume to continue from this point.")
            return None

        # Parse batch
        batch_results = parser.parse_synopsis_batch(batch_rows, dry_run=dry_run)

        # Handle dry run mode
        if isinstance(batch_results, dict) and batch_results.get("dry_run"):
            print(f"\n{'='*80}")
            print("DRY RUN COMPLETE")
            print(f"{'='*80}")
            print(f"✓ All setup and validation completed successfully")
            print(
                f"✓ Would have processed {batch_results.get('batch_size', 0)} items in first batch"
            )
            print(f"✓ Session ready at: {session_file}")
            print(f"\nRun without --dry-run to begin actual processing")
            return None

        # Check for complete batch errors (API errors, malformed JSON, etc.)
        if (
            isinstance(batch_results, dict)
            and "error" in batch_results
            and "results" not in batch_results
        ):
            # Complete batch failure
            error_msg = batch_results["error"]
            print(f"  ✗ Batch Failed: {error_msg}")

            # Check if all API keys are rate limited
            if error_msg.startswith("RATE_LIMIT_EXHAUSTED"):
                print(f"\n⚠  All API keys have been rate limited!")
                print(f"   Key status: {api_key_manager.get_status()}")
                print(f"   Session saved to: {session_file}")
                print(
                    f"\n   Wait for rate limits to reset and run with --resume to continue."
                )
                session.save(session_file)
                return None

            # Check if all API keys are exhausted
            if api_key_manager.all_keys_exhausted():
                print(f"\n⚠  All API keys exhausted!")
                print(f"   Key status: {api_key_manager.get_status()}")
                print(f"   Session saved to: {session_file}")
                print(f"\n   Wait for quota reset and run with --resume to continue.")
                session.save(session_file)
                return None

            # Track retry counts for entire batch
            max_retries = 3
            for idx in batch_indices:
                retry_count = session.retry_counts.get(idx, 0)
                session.retry_counts[idx] = retry_count + 1

            # Apply exponential backoff before retrying
            retry_attempt = max([session.retry_counts.get(i, 0) for i in batch_indices])
            if retry_attempt > 0 and retry_attempt < max_retries:
                backoff_time = (2 ** (retry_attempt - 1)) * 20  # 20s, 40s, 80s
                print(f"  ⏳ Retry {retry_attempt}/{max_retries} in {backoff_time}s...")
                time.sleep(backoff_time)

            session.save(session_file)
            continue

        # Handle partial batch success (new format)
        if isinstance(batch_results, dict) and "results" in batch_results:
            results_list = batch_results["results"]
            item_errors = batch_results.get("item_errors", [])
            success_count = batch_results.get("success_count", 0)
            failed_count = batch_results.get("failed_count", 0)

            # Process successful items - preserve order by mapping batch index to actual index
            for i, result_dict in enumerate(results_list):
                if result_dict is not None and i < len(batch_indices):
                    idx = batch_indices[
                        i
                    ]  # Map batch position to actual dataset position
                    # Convert dict to ParsedWaterbody instance
                    parsed_waterbody = ParsedWaterbody.from_dict(result_dict)
                    # Store in correct position - session.results[idx] maintains input order
                    session.results[idx] = parsed_waterbody
                    if idx not in session.processed_items:
                        session.processed_items.append(idx)
                    # Reset retry count on success
                    if idx in session.retry_counts:
                        del session.retry_counts[idx]

            # Log failed items for analysis
            if item_errors:
                failure_log_file = "output/llm_parser/failure_log.json"
                log_failure_details(
                    failure_log_file,
                    batch_indices,
                    item_errors,
                    waterbody_rows,
                    results_list,
                )

                # Track individual item failures - these will be retried at the end
                for error_info in item_errors:
                    batch_idx = error_info[
                        "batch_index"
                    ]  # Index within the batch (0-9)
                    actual_idx = batch_indices[
                        batch_idx
                    ]  # Actual index in full dataset
                    retry_count = session.retry_counts.get(actual_idx, 0)
                    session.retry_counts[actual_idx] = retry_count + 1

            # Show running summary
            total_success = len(session.processed_items)
            total_pending = (
                session.total_items - total_success - len(session.failed_items)
            )

            if success_count == len(batch_indices):
                print(
                    f"  ✓ Success: {success_count}/{len(batch_indices)} items succeeded | Total: {total_success} OK, {total_pending} pending"
                )
            else:
                print(
                    f"  ✓ Partial Success: {success_count}/{len(batch_indices)} items succeeded | Total: {total_success} OK, {total_pending} pending"
                )

            if failed_count > 0:
                print(
                    f"    {failed_count} items will be retried at end (logged to failure_log.json)"
                )

        # Handle old format (full list of dicts) for backwards compatibility
        elif isinstance(batch_results, list):
            for i, result_dict in enumerate(batch_results):
                if i < len(batch_indices):
                    idx = batch_indices[
                        i
                    ]  # Map batch position to actual dataset position
                    # Convert dict to ParsedWaterbody instance
                    parsed_waterbody = ParsedWaterbody.from_dict(result_dict)
                    # Store in correct position - session.results[idx] maintains input order
                    session.results[idx] = parsed_waterbody
                    if idx not in session.processed_items:
                        session.processed_items.append(idx)

            # Show running summary
            success_count = len(session.processed_items)
            fail_count = len(session.failed_items)
            validation_fail_count = len(session.validation_failures)
            print(
                f"  ✓ Success | Total: {success_count} OK, {fail_count + validation_fail_count} failed"
            )
        else:
            # Unexpected format - treat as complete batch failure and retry
            print(f"  ✗ Unexpected result format: {type(batch_results)}")

            # Track retry counts for entire batch
            max_retries = 3
            for idx in batch_indices:
                retry_count = session.retry_counts.get(idx, 0)
                session.retry_counts[idx] = retry_count + 1

            # Apply exponential backoff before retrying
            retry_attempt = max([session.retry_counts.get(i, 0) for i in batch_indices])
            if retry_attempt > 0 and retry_attempt < max_retries:
                backoff_time = (2 ** (retry_attempt - 1)) * 20  # 20s, 40s, 80s
                print(f"  ⏳ Retry {retry_attempt}/{max_retries} in {backoff_time}s...")
                time.sleep(backoff_time)

            session.save(session_file)
            continue

        # Check for interruption after completing batch
        if interruption_requested:
            print(f"\n⚠  Interruption detected - batch {batch_num} completed")
            print(
                f"   Saving session with {len(session.processed_items)}/{total_items} items completed..."
            )
            session.save(session_file)
            print(f"   ✓ Session saved to: {session_file}")
            print(f"\n   Run with --resume to continue from this point.")
            return None

        # Save session after each batch - results are in order by index
        session.save(session_file)

        # Small delay between batches to avoid rate limiting
        if batch_start + batch_size < len(items_to_process):
            time.sleep(1)

    # After main processing, retry failed items (those with retry_counts > 0 but not yet processed)
    failed_to_retry = [
        i
        for i in range(total_items)
        if i not in session.processed_items
        and session.retry_counts.get(i, 0) > 0
        and session.retry_counts.get(i, 0) < 3
    ]

    if failed_to_retry:
        print(f"\n{'─'*80}")
        print(f"RETRYING FAILED ITEMS ({len(failed_to_retry)} items)")
        print(f"{'─'*80}")

        retry_start_time = datetime.now()
        max_retries = 3

        for batch_start in range(0, len(failed_to_retry), batch_size):
            batch_indices = failed_to_retry[batch_start : batch_start + batch_size]
            batch_rows = [waterbody_rows[i] for i in batch_indices]

            batch_num = batch_start // batch_size + 1
            total_retry_batches = (len(failed_to_retry) + batch_size - 1) // batch_size

            retry_progress = (batch_start / len(failed_to_retry)) * 100

            print(
                f"\n[Retry Batch {batch_num}/{total_retry_batches}] Indices {batch_indices[0]}-{batch_indices[-1]} | "
                f"Retry Progress: {retry_progress:.0f}%"
            )
            print(f"  API Keys: {api_key_manager.get_status()}")

            # Check for interruption before starting retry batch
            if interruption_requested:
                print(
                    f"\n⚠  Interruption detected - stopping before retry batch {batch_num}"
                )
                print(
                    f"   Saving session with {len(session.processed_items)}/{total_items} items completed..."
                )
                session.save(session_file)
                print(f"   ✓ Session saved to: {session_file}")
                print(f"\n   Run with --resume to continue from this point.")
                return None

            # Parse batch
            batch_results = parser.parse_synopsis_batch(batch_rows, dry_run=dry_run)

            # Handle dry run mode
            if isinstance(batch_results, dict) and batch_results.get("dry_run"):
                print(f"\n{'='*80}")
                print("DRY RUN COMPLETE")
                print(f"{'='*80}")
                print(f"✓ All setup and validation completed successfully")
                print(
                    f"✓ Would have processed {batch_results.get('batch_size', 0)} items in first retry batch"
                )
                print(f"✓ Session ready at: {session_file}")
                print(f"\nRun without --dry-run to begin actual processing")
                return None

            # Check for complete batch errors
            if (
                isinstance(batch_results, dict)
                and "error" in batch_results
                and "results" not in batch_results
            ):
                error_msg = batch_results["error"]
                print(f"  ✗ Batch Failed: {error_msg}")

                # Check if all API keys are rate limited
                if error_msg.startswith("RATE_LIMIT_EXHAUSTED"):
                    print(f"\n⚠  All API keys have been rate limited during retries!")
                    print(f"   Key status: {api_key_manager.get_status()}")
                    session.save(session_file)
                    break

                if api_key_manager.all_keys_exhausted():
                    print(f"\n⚠  All API keys exhausted during retries!")
                    print(f"   Key status: {api_key_manager.get_status()}")
                    session.save(session_file)
                    break

                # Mark as permanently failed if max retries reached
                for idx in batch_indices:
                    if session.retry_counts[idx] >= max_retries:
                        if idx not in [f["index"] for f in session.failed_items]:
                            session.failed_items.append(
                                {
                                    "index": idx,
                                    "waterbody": waterbody_rows[idx].water,
                                    "error": f"Retry failed: {error_msg}",
                                    "retries": session.retry_counts[idx],
                                }
                            )
                session.save(session_file)
                continue

            # Handle partial batch success
            if isinstance(batch_results, dict) and "results" in batch_results:
                results_list = batch_results["results"]
                item_errors = batch_results.get("item_errors", [])
                success_count = batch_results.get("success_count", 0)

                # Process successful items - preserve order by mapping batch index to actual index
                for i, result_dict in enumerate(results_list):
                    if result_dict is not None and i < len(batch_indices):
                        idx = batch_indices[
                            i
                        ]  # Map batch position to actual dataset position
                        parsed_waterbody = ParsedWaterbody.from_dict(result_dict)
                        # Store in correct position - session.results[idx] maintains input order
                        session.results[idx] = parsed_waterbody
                        if idx not in session.processed_items:
                            session.processed_items.append(idx)
                        # Clear retry count on success
                        if idx in session.retry_counts:
                            del session.retry_counts[idx]

                # Log failures and mark as permanently failed if max retries reached
                if item_errors:
                    failure_log_file = "output/llm_parser/failure_log.json"
                    log_failure_details(
                        failure_log_file,
                        batch_indices,
                        item_errors,
                        waterbody_rows,
                        results_list,
                    )

                    for error_info in item_errors:
                        batch_idx = error_info[
                            "batch_index"
                        ]  # Index within the batch (0-9)
                        actual_idx = batch_indices[
                            batch_idx
                        ]  # Actual index in full dataset

                        if session.retry_counts[actual_idx] >= max_retries:
                            # Permanently failed
                            if actual_idx not in [
                                f["index"] for f in session.failed_items
                            ]:
                                session.failed_items.append(
                                    {
                                        "index": actual_idx,
                                        "waterbody": waterbody_rows[actual_idx].water,
                                        "error": error_info["error"],
                                        "retries": session.retry_counts[actual_idx],
                                        "error_type": error_info["error_type"],
                                    }
                                )

                print(
                    f"  ✓ Retry Result: {success_count}/{len(batch_indices)} items succeeded"
                )

            # Check for interruption after completing retry batch
            if interruption_requested:
                print(f"\n⚠  Interruption detected - retry batch {batch_num} completed")
                print(
                    f"   Saving session with {len(session.processed_items)}/{total_items} items completed..."
                )
                session.save(session_file)
                print(f"   ✓ Session saved to: {session_file}")
                print(f"\n   Run with --resume to continue from this point.")
                return None

            session.save(session_file)

            # Small delay between retry batches
            if batch_start + batch_size < len(failed_to_retry):
                time.sleep(1)

        retry_elapsed = (datetime.now() - retry_start_time).total_seconds()
        retry_succeeded = len(
            [i for i in failed_to_retry if i in session.processed_items]
        )
        print(f"\n{'─'*80}")
        print(
            f"Retry phase complete: {retry_succeeded}/{len(failed_to_retry)} items recovered ({int(retry_elapsed)}s)"
        )
        print(f"{'─'*80}\n")

    # Check if all items were processed
    unprocessed_indices = [
        i for i in range(total_items) if i not in session.processed_items
    ]

    # Report on failed items and validation failures
    if session.failed_items:
        print(f"\n⚠  {len(session.failed_items)} items permanently failed:")
        for failed in session.failed_items[:5]:
            error_preview = (
                failed["error"][:80] + "..."
                if len(failed["error"]) > 80
                else failed["error"]
            )
            print(f"  [{failed['index']}] {failed['waterbody']}: {error_preview}")
        if len(session.failed_items) > 5:
            print(f"  ... and {len(session.failed_items) - 5} more")

    if session.validation_failures:
        print(
            f"\n⚠  {len(session.validation_failures)} validation failures (retry with --resume):"
        )
        for failed in session.validation_failures[:5]:
            error_preview = (
                failed["error"][:80] + "..."
                if len(failed["error"]) > 80
                else failed["error"]
            )
            print(f"  [{failed['index']}] {failed['waterbody']}: {error_preview}")
        if len(session.validation_failures) > 5:
            print(f"  ... and {len(session.validation_failures) - 5} more")

    # Compile final results - maintain order, include all items
    # Convert ParsedWaterbody instances to dicts for JSON output
    # For failed items, include error placeholder
    final_results_dicts = []
    for idx in range(total_items):
        input_row = waterbody_rows[idx]

        if session.results[idx] is not None:
            # Convert class instance to dict and merge with input metadata
            result_dict = session.results[idx].to_dict()

            # Add metadata from input row
            if hasattr(input_row, "mu"):
                result_dict["mu"] = input_row.mu
            if hasattr(input_row, "symbols"):
                result_dict["symbols"] = input_row.symbols
            if hasattr(input_row, "page"):
                result_dict["page"] = input_row.page
            if hasattr(input_row, "image"):
                result_dict["image"] = input_row.image
            if hasattr(input_row, "region"):
                result_dict["region"] = input_row.region

            final_results_dicts.append(result_dict)
        else:
            # Item failed - create error placeholder to maintain order
            failed_info = next(
                (f for f in session.failed_items if f["index"] == idx), None
            )
            if not failed_info:
                failed_info = next(
                    (f for f in session.validation_failures if f["index"] == idx), None
                )
            error_msg = failed_info["error"] if failed_info else "Unknown error"
            placeholder = {
                "waterbody_name": input_row.water,
                "error": f"FAILED_TO_PARSE: {error_msg}",
                "raw_text": input_row.raw_regs,
                "cleaned_text": "",
                "geographic_groups": [],
            }
            # Add metadata fields
            if hasattr(input_row, "mu"):
                placeholder["mu"] = input_row.mu
            if hasattr(input_row, "symbols"):
                placeholder["symbols"] = input_row.symbols
            if hasattr(input_row, "page"):
                placeholder["page"] = input_row.page
            if hasattr(input_row, "image"):
                placeholder["image"] = input_row.image
            if hasattr(input_row, "region"):
                placeholder["region"] = input_row.region
            final_results_dicts.append(placeholder)

    # Save final output as JSON
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(final_results_dicts, f, indent=2, ensure_ascii=False)

    success_count = len(session.processed_items)
    failed_count = len(session.failed_items)
    validation_fail_count = len(session.validation_failures)
    total_failures = failed_count + validation_fail_count

    print(f"\n{'─'*80}")
    print(f"✓  Completed! Saved to: {output_file}")
    print(f"   {success_count} succeeded, {total_failures} failed")
    if validation_fail_count > 0:
        print(f"   ({validation_fail_count} validation failures - retry with --resume)")
    if failed_count > 0:
        print(f"   ({failed_count} other failures)")

    if unprocessed_indices:
        print(f"\n⚠  WARNING: {len(unprocessed_indices)} items were never processed")
        print(f"   Indices: {unprocessed_indices[:10]}...")

    # Mark session as complete and archive if fully successful
    if (
        len(session.processed_items) == total_items
        and not session.failed_items
        and not session.validation_failures
    ):
        if session.completed_at is None:
            session.completed_at = datetime.now().isoformat()
            session.save(session_file)
            print(f"✓  Session marked as complete at {session.completed_at}")

        # Archive completed session
        archive_completed_session(session_file, output_file, session)
    print(f"{'─'*80}")

    return session.results  # Return class instances, not dicts


def archive_completed_session(
    session_file: str, results_file: str, session: "SessionState"
):
    """
    Archive a completed session to a timestamped folder with manifest.

    Creates folder structure:
        completed_sessions/YYYY-MM-DD_HHmmss/
            session.json
            parsed_results.json
            manifest.json

    Args:
        session_file: Path to current session file
        results_file: Path to current results file
        session: SessionState instance
    """
    if not session.completed_at:
        return

    # Parse completion timestamp for folder name
    try:
        completed_dt = datetime.fromisoformat(session.completed_at)
        folder_name = completed_dt.strftime("%Y-%m-%d_%H%M%S")
    except:
        folder_name = datetime.now().strftime("%Y-%m-%d_%H%M%S")

    # Create archive folder
    archive_base = os.path.join(os.path.dirname(session_file), "completed_sessions")
    archive_folder = os.path.join(archive_base, folder_name)
    os.makedirs(archive_folder, exist_ok=True)

    # Copy session file
    session_dest = os.path.join(archive_folder, "session.json")
    if os.path.exists(session_file):
        import shutil

        shutil.copy2(session_file, session_dest)

    # Copy results file
    results_dest = os.path.join(archive_folder, "parsed_results.json")
    if os.path.exists(results_file):
        import shutil

        shutil.copy2(results_file, results_dest)

    # Generate cleaned results (flat structure with cleaned_waterbody_name field)
    cleaned_dest = os.path.join(archive_folder, "cleaned_results.json")
    cleaned_stats = None
    if os.path.exists(results_dest):
        try:
            with open(results_dest, "r", encoding="utf-8") as f:
                parsed_results = json.load(f)

            # Add cleaned names
            results_with_cleaned = add_cleaned_names(parsed_results)

            # Calculate statistics
            cleaned_count = sum(
                1
                for r in results_with_cleaned
                if r.get("cleaned_waterbody_name") != r.get("waterbody_name")
            )
            cleaned_stats = {
                "total_waterbodies": len(results_with_cleaned),
                "names_modified": cleaned_count,
                "names_unchanged": len(results_with_cleaned) - cleaned_count,
            }

            # Save cleaned results
            with open(cleaned_dest, "w", encoding="utf-8") as f:
                json.dump(results_with_cleaned, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"   ⚠ Warning: Failed to generate cleaned results: {e}")
            cleaned_stats = None

    # Generate grouped results (nested structure by region and cleaned name)
    grouped_dest = os.path.join(archive_folder, "grouped_results.json")
    grouped_stats = None
    if os.path.exists(results_dest):
        try:
            with open(results_dest, "r", encoding="utf-8") as f:
                parsed_results = json.load(f)

            # Process and group
            result = process_and_group_results(parsed_results)
            grouped_stats = result["stats"]

            # Save grouped results
            with open(grouped_dest, "w", encoding="utf-8") as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"   ⚠ Warning: Failed to generate grouped results: {e}")
            grouped_stats = None

    # Create manifest
    manifest = {
        "created_at": session.created_at,
        "completed_at": session.completed_at,
        "last_updated": session.last_updated,
        "total_items": session.total_items,
        "processed_items": len(session.processed_items),
        "failed_items": len(session.failed_items),
        "validation_failures": len(session.validation_failures),
        "success_rate": f"{(len(session.processed_items) / session.total_items * 100):.1f}%",
        "files": {
            "session": "session.json",
            "results": "parsed_results.json",
            "cleaned_results": "cleaned_results.json" if cleaned_stats else None,
            "grouped_results": "grouped_results.json" if grouped_stats else None,
        },
        "summary": {
            "total_waterbodies": session.total_items,
            "successfully_parsed": len(session.processed_items),
            "failures": len(session.failed_items) + len(session.validation_failures),
        },
    }

    # Add cleaning statistics if available
    if cleaned_stats:
        manifest["cleaning"] = cleaned_stats

    # Add grouping statistics if available
    if grouped_stats:
        manifest["grouping"] = grouped_stats

    manifest_path = os.path.join(archive_folder, "manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    print(f"✓  Archived to: {archive_folder}")
    print(f"   - session.json")
    print(f"   - parsed_results.json")
    if cleaned_stats:
        print(
            f"   - cleaned_results.json ({cleaned_stats['names_modified']} names modified)"
        )
    if grouped_stats:
        print(
            f"   - grouped_results.json ({grouped_stats['regions']} regions, {grouped_stats['total_unique_cleaned_names']} unique names)"
        )
    print(f"   - manifest.json")


def clean_and_group_results(
    input_file: str, output_file: str, group_only: bool = False
):
    """
    Clean waterbody names and optionally group by region and cleaned name.

    Args:
        input_file: Path to parsed results JSON file
        output_file: Path to save cleaned/grouped results
        group_only: If True, also group by region and cleaned name
    """
    print(f"\n{'='*80}\nCleaning Waterbody Names...\n{'='*80}")

    # Load parsed results
    if not os.path.exists(input_file):
        print(f"✗ Error: Input file not found: {input_file}")
        exit(1)

    with open(input_file, "r", encoding="utf-8") as f:
        parsed_results = json.load(f)

    print(f"Loaded {len(parsed_results)} waterbodies from {input_file}")

    if group_only:
        # Process and group results
        result = process_and_group_results(parsed_results)

        # Show statistics
        print(f"\nGrouping Statistics:")
        print(f"  Total waterbodies: {result['stats']['total_waterbodies']}")
        print(f"  Regions: {result['stats']['regions']}")
        print(
            f"  Total unique cleaned names: {result['stats']['total_unique_cleaned_names']}"
        )
        print(f"\n  Cleaned names per region:")
        for region, count in result["stats"]["cleaned_names_by_region"].items():
            print(f"    {region}: {count}")

        # Save grouped results
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)

        print(f"\n✓ Grouped results saved to: {output_file}")
    else:
        # Just add cleaned names without grouping
        results_with_cleaned = add_cleaned_names(parsed_results)

        # Count how many names were cleaned (different from original)
        cleaned_count = sum(
            1
            for r in results_with_cleaned
            if r.get("cleaned_waterbody_name") != r.get("waterbody_name")
        )

        print(f"\nCleaning Statistics:")
        print(f"  Total waterbodies: {len(results_with_cleaned)}")
        print(f"  Names modified: {cleaned_count}")
        print(f"  Names unchanged: {len(results_with_cleaned) - cleaned_count}")

        # Save results with cleaned names
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(results_with_cleaned, f, indent=2, ensure_ascii=False)

        print(f"\n✓ Cleaned results saved to: {output_file}")


def update_completed_session_grouped_results(session_dir: str):
    """
    Update the grouped_results.json for a completed session.

    This re-runs the cleaning and grouping logic on the parsed_results.json
    from a completed session directory, useful for applying updated cleaning
    rules (like new part suffix stripping) to existing session data.

    Args:
        session_dir: Path to completed session directory (e.g., completed_sessions/2024-01-15_123456)
    """
    print(f"\n{'='*80}\nUpdating Grouped Results for Completed Session...\n{'='*80}")

    # Validate session directory exists
    if not os.path.isdir(session_dir):
        print(f"✗ Error: Session directory not found: {session_dir}")
        exit(1)

    # Check for required files
    parsed_results_path = os.path.join(session_dir, "parsed_results.json")
    if not os.path.exists(parsed_results_path):
        print(f"✗ Error: parsed_results.json not found in session directory")
        exit(1)

    manifest_path = os.path.join(session_dir, "manifest.json")

    print(f"Session directory: {session_dir}")

    # Load parsed results
    with open(parsed_results_path, "r", encoding="utf-8") as f:
        parsed_results = json.load(f)

    print(f"Loaded {len(parsed_results)} waterbodies from parsed_results.json")

    # Re-process and group results with current cleaning rules
    result = process_and_group_results(parsed_results)
    grouped_stats = result["stats"]

    # Show statistics
    print(f"\nGrouping Statistics:")
    print(f"  Total waterbodies: {grouped_stats['total_waterbodies']}")
    print(f"  Regions: {grouped_stats['regions']}")
    print(
        f"  Total unique cleaned names: {grouped_stats['total_unique_cleaned_names']}"
    )
    print(f"\n  Cleaned names per region:")
    for region, count in grouped_stats["cleaned_names_by_region"].items():
        print(f"    {region}: {count}")

    # Save updated grouped results
    grouped_results_path = os.path.join(session_dir, "grouped_results.json")
    with open(grouped_results_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    print(f"\n✓ Updated grouped_results.json")

    # Update manifest if it exists
    if os.path.exists(manifest_path):
        try:
            with open(manifest_path, "r", encoding="utf-8") as f:
                manifest = json.load(f)

            # Update grouping statistics
            manifest["grouping"] = grouped_stats
            manifest["files"]["grouped_results"] = "grouped_results.json"

            # Add update timestamp
            manifest["grouped_results_updated_at"] = datetime.now().isoformat()

            with open(manifest_path, "w", encoding="utf-8") as f:
                json.dump(manifest, f, indent=2, ensure_ascii=False)

            print(f"✓ Updated manifest.json with new grouping statistics")
        except Exception as e:
            print(f"⚠ Warning: Failed to update manifest.json: {e}")

    print(f"\n{'='*80}")
    print(f"✓ Grouped results successfully updated!")
    print(f"{'='*80}")


def export_session(session_file: str, output_file: str):
    """
    Export current session results to JSON output file.

    Args:
        session_file: Path to session file to export
        output_file: Path to save exported results
    """
    print(f"\n{'='*80}\nExporting Session to JSON...\n{'='*80}")

    # Load session
    session = SessionState.load(session_file)
    if session is None:
        print(f"✗ Error: Session file not found: {session_file}")
        exit(1)

    print(f"Session info:")
    print(f"  Created: {session.created_at}")
    print(f"  Last updated: {session.last_updated}")
    print(f"  Total items: {session.total_items}")
    print(f"  Processed: {len(session.processed_items)}")
    print(f"  Failed: {len(session.failed_items)}")

    # Convert results to dicts, maintaining order
    final_results_dicts = []
    for idx in range(session.total_items):
        input_row = session.input_rows[idx]

        if session.results[idx] is not None:
            # Convert ParsedWaterbody instance to dict and merge with input metadata
            result_dict = session.results[idx].to_dict()

            # Add metadata from input row
            if hasattr(input_row, "mu"):
                result_dict["mu"] = input_row.mu
            if hasattr(input_row, "symbols"):
                result_dict["symbols"] = input_row.symbols
            if hasattr(input_row, "page"):
                result_dict["page"] = input_row.page
            if hasattr(input_row, "image"):
                result_dict["image"] = input_row.image
            if hasattr(input_row, "region"):
                result_dict["region"] = input_row.region

            final_results_dicts.append(result_dict)
        else:
            # Item not yet processed or failed
            if idx in session.processed_items:
                # This shouldn't happen but handle it
                print(f"  ⚠ Warning: Item {idx} marked as processed but result is None")

            # Check if it's a failed item
            failed_info = next(
                (f for f in session.failed_items if f["index"] == idx), None
            )
            if failed_info:
                # Include error placeholder with metadata
                error_msg = failed_info.get("error", "Unknown error")
                placeholder = {
                    "waterbody_name": input_row.water,
                    "error": f"FAILED_TO_PARSE: {error_msg}",
                    "raw_text": input_row.raw_regs,
                    "cleaned_text": "",
                    "geographic_groups": [],
                }
                # Add metadata fields
                if hasattr(input_row, "mu"):
                    placeholder["mu"] = input_row.mu
                if hasattr(input_row, "symbols"):
                    placeholder["symbols"] = input_row.symbols
                if hasattr(input_row, "page"):
                    placeholder["page"] = input_row.page
                if hasattr(input_row, "image"):
                    placeholder["image"] = input_row.image
                if hasattr(input_row, "region"):
                    placeholder["region"] = input_row.region
                final_results_dicts.append(placeholder)
            else:
                # Not processed yet - include placeholder with metadata
                placeholder = {
                    "waterbody_name": input_row.water,
                    "error": "NOT_YET_PROCESSED",
                    "raw_text": input_row.raw_regs,
                    "cleaned_text": "",
                    "geographic_groups": [],
                }
                # Add metadata fields
                if hasattr(input_row, "mu"):
                    placeholder["mu"] = input_row.mu
                if hasattr(input_row, "symbols"):
                    placeholder["symbols"] = input_row.symbols
                if hasattr(input_row, "page"):
                    placeholder["page"] = input_row.page
                if hasattr(input_row, "image"):
                    placeholder["image"] = input_row.image
                if hasattr(input_row, "region"):
                    placeholder["region"] = input_row.region
                final_results_dicts.append(placeholder)

    # Save to output file
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(final_results_dicts, f, indent=2, ensure_ascii=False)

    print(f"\n✓ Exported {len(final_results_dicts)} items to: {output_file}")
    print(
        f"  Successfully parsed: {len([r for r in session.results if r is not None])}"
    )
    print(f"  Failed: {len(session.failed_items)}")
    print(
        f"  Not yet processed: {session.total_items - len(session.processed_items) - len(session.failed_items)}"
    )


def print_prompt(waterbody_rows: List):
    """Print the prompt that would be sent to the LLM."""
    parser = SynopsisParser()

    prompt = parser.get_prompt(waterbody_rows)
    print(prompt)

    # Save prompt to file
    prompt_file = "output/llm_parser/prompt.txt"
    os.makedirs(os.path.dirname(prompt_file), exist_ok=True)
    with open(prompt_file, "w", encoding="utf-8") as f:
        f.write(prompt)
    print(f"\n✓ Prompt saved to: {prompt_file}")


def load_waterbody_rows_from_file(file_path):
    """Load WaterbodyRow objects from a synopsis_raw_data.json file."""
    if not os.path.exists(file_path):
        print(f"Error: File not found: {file_path}")
        return None

    print(f"Loading waterbody rows from: {file_path}")
    with open(file_path, "r", encoding="utf-8") as f:
        json_data = json.load(f)

    # Reconstruct ExtractionResults from JSON
    extraction_results = ExtractionResults.from_dict(json_data)

    # Extract all WaterbodyRow objects from all pages
    all_rows = []
    for page_result in extraction_results.pages:
        all_rows.extend(page_result.rows)

    print(
        f"Loaded {len(all_rows)} waterbody rows from {len(extraction_results.pages)} pages"
    )
    return all_rows


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Parse fishing regulations using LLM with batch processing and validation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
WORKFLOW EXAMPLES:

1. Start New Parsing Job
   python parse_synopsis.py --file scripts/output/extract_synopsis/synopsis_raw_data.json
   
   - Processes in batches (default 10 items)
   - Saves progress to session.json after each batch
   - Shows time estimates and progress percentage
   
2. Check What the LLM Will See
   python parse_synopsis.py --file synopsis_raw_data.json --prompt
   
   - Displays the full prompt without making API calls
   - Useful for debugging or understanding the parsing instructions
   
3. If Processing is Interrupted (rate limit, error, Ctrl+C)
   python parse_synopsis.py --resume
   
   - No --file needed! Session contains all input data
   - Continues from where it left off
   - Retries failed items (max 3 attempts)
   
4. Check Current Progress
   python parse_synopsis.py --export-session
   
   - Exports current session state to JSON output
   - Shows completed, failed, and pending items
   - Useful for inspecting partial results
   
5. Validate Results (auto-detects session or parsed results)
   
   a) Validate session file (uses embedded input data)
      python parse_synopsis.py --validate output/llm_parser/session.json
      
      - Auto-detects session format
      - Shows progress (processed/failed/pending)
      - No --file needed (session contains input)
   
   b) Validate parsed results with input comparison
      python parse_synopsis.py --validate output/llm_parser/llm_parsed_results.json --file synopsis_raw_data.json
      
      - Checks names and raw_text match input exactly
      - Validates all structure and content
   
   c) Validate parsed results (structure only)
      python parse_synopsis.py --validate output/llm_parser/llm_parsed_results.json
      
      - Checks data structure without input comparison

6. Update Grouped Results for Completed Session
   python parse_synopsis.py --update-grouped output/llm_parser/completed_sessions/2024-01-15_123456
   
   - Re-runs cleaning and grouping logic on existing parsed_results.json
   - Useful for applying updated cleaning rules (e.g., new part suffix stripping)
   - Updates grouped_results.json and manifest.json in the session directory
   - Does not re-parse or modify parsed_results.json

COMPLETE WORKFLOW:
  
  Step 1: Start parsing
    $ python parse_synopsis.py --file synopsis_raw_data.json --batch-size 5
    
  Step 2: If interrupted, resume
    $ python parse_synopsis.py --resume
    
  Step 3: Check progress anytime
    $ python parse_synopsis.py --export-session --output progress_check.json
    
  Step 4: Validate session or final output
    $ python parse_synopsis.py --validate output/llm_parser/session.json
    $ python parse_synopsis.py --validate output/llm_parser/llm_parsed_results.json --file synopsis_raw_data.json

CUSTOM PATHS:
  
  # Use custom session and output files
  python parse_synopsis.py --file data.json --session-file my_session.json --output my_results.json
  
  # Resume from custom session
  python parse_synopsis.py --resume --session-file my_session.json

TROUBLESHOOTING:

  - If items fail permanently (after 3 retries):
    1. Script exits with error details
    2. Review errors printed to console
    3. Fix input data if needed
    4. Delete session file: rm output/llm_parser/session.json
    5. Run again from start
    
  - To change batch size (if hitting rate limits):
    python parse_synopsis.py --file data.json --batch-size 3
    
  - Session file is human-readable JSON - you can inspect it:
    cat output/llm_parser/session.json
        """,
    )

    # Input/Output arguments
    io_group = parser.add_argument_group("Input/Output")
    io_group.add_argument(
        "--file",
        type=str,
        metavar="PATH",
        help="Path to synopsis_raw_data.json file to parse (not required if resuming)",
    )
    io_group.add_argument(
        "--output",
        default="output/llm_parser/llm_parsed_results.json",
        metavar="PATH",
        help="Path to save parsed results (default: output/llm_parser/llm_parsed_results.json)",
    )
    io_group.add_argument(
        "--session-file",
        default="output/llm_parser/session.json",
        metavar="PATH",
        help="Path to session file for resuming (default: output/llm_parser/session.json)",
    )

    # Processing arguments
    proc_group = parser.add_argument_group("Processing")
    proc_group.add_argument(
        "--batch-size",
        type=int,
        default=45,
        metavar="N",
        help="Number of items per batch (default: 10, smaller = safer)",
    )
    proc_group.add_argument(
        "--resume", action="store_true", help="Resume from previous progress file"
    )
    proc_group.add_argument(
        "--dry-run",
        action="store_true",
        help="Run all setup and validation but stop before making any LLM API calls",
    )

    # Action arguments (mutually exclusive)
    action_group = parser.add_argument_group("Actions")
    action_group.add_argument(
        "--validate",
        type=str,
        metavar="PATH",
        help="Validate a session or parsed results JSON file (auto-detects type)",
    )
    action_group.add_argument(
        "--prompt",
        action="store_true",
        help="Print the LLM prompt without making API calls",
    )
    action_group.add_argument(
        "--export-session",
        action="store_true",
        help="Export current session results to output JSON file",
    )
    action_group.add_argument(
        "--clean-names",
        type=str,
        metavar="INPUT_JSON",
        help="Add cleaned_waterbody_name field to parsed results (removes quotes and parentheses)",
    )
    action_group.add_argument(
        "--group-by-region",
        type=str,
        metavar="INPUT_JSON",
        help="Clean names and group waterbodies by region and cleaned name",
    )
    action_group.add_argument(
        "--update-grouped",
        type=str,
        metavar="SESSION_DIR",
        help="Update grouped_results.json for a completed session directory (e.g., completed_sessions/2024-01-15_123456)",
    )

    args = parser.parse_args(argv)

    # Handle update grouped results mode
    if args.update_grouped:
        update_completed_session_grouped_results(args.update_grouped)
        exit(0)

    # Handle clean names mode
    if args.clean_names:
        clean_and_group_results(args.clean_names, args.output, group_only=False)
        exit(0)

    # Handle group by region mode
    if args.group_by_region:
        clean_and_group_results(args.group_by_region, args.output, group_only=True)
        exit(0)

    # Handle export session mode
    if args.export_session:
        export_session(args.session_file, args.output)
        exit(0)

    # Handle validation mode
    if args.validate:
        print(f"\n{'='*80}\nValidating JSON file...\n{'='*80}")

        # Load input rows if --file provided for name matching (only used for non-session files)
        input_rows = None
        if args.file:
            input_rows = load_waterbody_rows_from_file(args.file)

        result = validate_partial_json(args.validate, input_rows)

        print(f"\nValidation Results:")
        print(f"  File type: {result.get('file_type', 'unknown')}")
        print(f"  Items checked: {result.get('items_checked', 0)}")
        print(f"  Valid: {result['valid']}")

        # Show session info if available
        if result.get("file_type") == "session" and "session_info" in result:
            info = result["session_info"]
            print(f"\nSession Info:")
            print(f"  Total items: {info['total_items']}")
            print(f"  Processed: {info['processed']}")
            print(f"  Failed: {info['failed']}")
            print(f"  Created: {info['created_at']}")
            print(f"  Last updated: {info['last_updated']}")
            if info.get("completed_at"):
                print(f"  Completed: {info['completed_at']}")

        if result["errors"]:
            print(f"\n  Errors ({len(result['errors'])}):")
            for err in result["errors"][:20]:
                print(f"    - {err}")
            if len(result["errors"]) > 20:
                print(f"    ... and {len(result['errors']) - 20} more")

        if result["warnings"]:
            print(f"\n  Warnings ({len(result['warnings'])}):")
            for warn in result["warnings"][:10]:
                print(f"    - {warn}")
            if len(result["warnings"]) > 10:
                print(f"    ... and {len(result['warnings']) - 10} more")

        if result["valid"]:
            print("\n✓ All checks passed!")
        else:
            print("\n✗ Validation failed")
            exit(1)
        exit(0)

    # Load waterbody rows if --file provided
    waterbody_rows = None
    if args.file:
        waterbody_rows = load_waterbody_rows_from_file(args.file)
        if waterbody_rows is None:
            exit(1)
    elif not args.resume:
        # If not resuming and no file, error
        parser.error("--file is required (unless using --validate or --resume)")

    # Handle prompt mode
    if args.prompt:
        if waterbody_rows is None:
            print("Error: --prompt requires --file")
            exit(1)
        print_prompt(waterbody_rows)
        exit(0)

    # Run LLM parsing (waterbody_rows can be None if resuming)
    run_llm_parsing(
        waterbody_rows=waterbody_rows,
        output_file=args.output,
        batch_size=args.batch_size,
        session_file=args.session_file,
        resume=args.resume,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
