import json
import os
import argparse
import time
from datetime import datetime
from google import genai
from google.genai import types
from extract_synopsis import WaterbodyRow, ExtractionResults
from attrs import define
from typing import List, Dict, Any, Optional

# Configuration - Multiple API keys for rotation
# To add more API keys, uncomment the lines below and add your keys.
# The system will automatically rotate through keys when one fails 3 times.
# This helps avoid rate limits by distributing load across multiple keys.
API_KEYS = [
    {"id": "horvath.dawson", "key": os.environ.get("GOOGLE_API_KEY", "AIzaSyBPZigLsxFIU7JOFSux8ZqS03p9-E878VE")},
    {"id": "darcy.turin", "key": os.environ.get("GOOGLE_API_KEY_2", "AIzaSyC9C-PueILJLJ32bWpUzAV7sQ3R-VXpSUA")},
    {"id": "datswrite", "key": os.environ.get("GOOGLE_API_KEY_3", "AIzaSyA83jZlnUeswsdnljezcSld6UQE2hPys-M")},
    {"id": "helpfulhints116", "key": os.environ.get("GOOGLE_API_KEY_4", "AIzaSyD4jJOcWpml2ATn9Jo_6iY-B3sdh68jOi0")},
]

class APIKeyManager:
    """Manages multiple API keys with automatic rotation on failures."""
    
    def __init__(self, api_keys: List[Dict[str, str]], max_failures: int = 3):
        self.api_keys = api_keys
        self.max_failures = max_failures
        self.current_index = 0
        self.failure_counts = {key["id"]: 0 for key in api_keys}
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
    
    def record_failure(self) -> bool:
        """Record a failure for current key and rotate if needed.
        
        Returns:
            True if we should continue (another key available), False if all keys exhausted
        """
        current_key_id = self.get_current_key_id()
        self.failure_counts[current_key_id] += 1
        
        # Check if current key has hit max failures
        if self.failure_counts[current_key_id] >= self.max_failures:
            print(f"  ⚠  API key '{current_key_id}' failed {self.max_failures} times, rotating...")
            
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
    
    def get_status(self) -> str:
        """Get a status string showing failure counts for all keys."""
        status_parts = []
        for key in self.api_keys:
            key_id = key["id"]
            failures = self.failure_counts[key_id]
            current = "*" if key_id == self.get_current_key_id() else " "
            status_parts.append(f"{current}{key_id}: {failures}/{self.max_failures}")
        return " | ".join(status_parts)

# Initialize API key manager
api_key_manager = APIKeyManager(API_KEYS, max_failures=3)

@define(frozen=True, cache_hash=True)
class ParsedRule:
    """A single parsed fishing regulation rule."""
    verbatim_text: str
    rule: str
    type: str
    dates: Optional[List[str]]
    species: Optional[List[str]]
    
    def validate(self, parent_text: str) -> List[str]:
        """Validate this rule. Returns list of error messages.
        
        Args:
            parent_text: The geographic group's raw_text that should contain this rule's verbatim_text
        """
        errors = []
        
        # Check required fields are non-empty
        if not self.verbatim_text or not self.verbatim_text.strip():
            errors.append("verbatim_text is empty")
        if not self.rule or not self.rule.strip():
            errors.append("rule is empty")
        
        # Validate verbatim_text actually appears in parent (geographic group's raw_text)
        if self.verbatim_text and parent_text:
            # Normalize for comparison (remove extra whitespace, case insensitive)
            verbatim_normalized = ' '.join(self.verbatim_text.replace('\n', ' ').split()).lower()
            parent_normalized = ' '.join(parent_text.replace('\n', ' ').split()).lower()
            
            if verbatim_normalized not in parent_normalized:
                errors.append(f"verbatim_text not found in geographic group's raw_text")
        
        # Validate rule type
        valid_types = {'closure', 'harvest', 'gear_restriction', 'restriction', 'licensing', 'access', 'note'}
        if self.type not in valid_types:
            errors.append(f"Invalid rule type '{self.type}', must be one of {valid_types}")
        
        # Validate dates/species are list or None
        if self.dates is not None and not isinstance(self.dates, list):
            errors.append(f"dates must be list or None, got {type(self.dates).__name__}")
        if self.species is not None and not isinstance(self.species, list):
            errors.append(f"species must be list or None, got {type(self.species).__name__}")
        
        # Validate dates appear in verbatim_text only (not parent text)
        if self.dates and isinstance(self.dates, list) and self.verbatim_text:
            for date in self.dates:
                # Normalize for comparison (remove spaces, case insensitive)
                date_normalized = date.replace(' ', '').replace('-', '').lower()
                verbatim_normalized = self.verbatim_text.replace(' ', '').replace('\n', '').replace('-', '').lower()
                
                # Check if date appears in verbatim_text
                if date_normalized not in verbatim_normalized:
                    errors.append(f"Date '{date}' not found in verbatim_text")
        
        return errors
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ParsedRule':
        """Create ParsedRule from dictionary, handling 'null' strings."""
        dates = data.get('dates')
        if dates == 'null' or dates == "null":
            dates = None
        species = data.get('species')
        if species == 'null' or species == "null":
            species = None
        
        return cls(
            verbatim_text=data.get('verbatim_text', ''),
            rule=data.get('rule', ''),
            type=data.get('type', ''),
            dates=dates,
            species=species
        )

@define(frozen=True, cache_hash=True)
class ParsedGeographicGroup:
    """A geographic subdivision of regulations for a waterbody."""
    location: str
    raw_text: str
    cleaned_text: str
    rules: List[ParsedRule]
    
    def validate(self, waterbody_name: str) -> List[str]:
        """Validate this geographic group. Returns list of error messages."""
        errors = []
        
        # Check rules array not empty
        if not self.rules or len(self.rules) == 0:
            errors.append(f"Geographic group '{self.location}' has no rules")
        
        # Validate each rule
        for idx, rule in enumerate(self.rules):
            rule_errors = rule.validate(self.raw_text)
            for err in rule_errors:
                errors.append(f"Rule {idx}: {err}")
        
        return errors
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ParsedGeographicGroup':
        """Create ParsedGeographicGroup from dictionary."""
        rules = [ParsedRule.from_dict(r) for r in data.get('rules', [])]
        return cls(
            location=data.get('location', ''),
            raw_text=data.get('raw_text', ''),
            cleaned_text=data.get('cleaned_text', ''),
            rules=rules
        )

@define(frozen=True, cache_hash=True)
class ParsedWaterbody:
    """Complete parsed result for a single waterbody."""
    waterbody_name: str
    raw_text: str
    cleaned_text: str
    geographic_groups: List[ParsedGeographicGroup]
    
    def validate(self, expected_name: str, expected_raw_text: str = None) -> List[str]:
        """Validate this waterbody result. Returns list of error messages.
        
        Args:
            expected_name: Required expected name from input - must always be provided
            expected_raw_text: Expected raw regulation text from input - should match exactly
        """
        errors = []
        
        # Check required fields
        if not self.waterbody_name or not self.waterbody_name.strip():
            errors.append("waterbody_name is empty")
        
        # Check name matches expected (REQUIRED - no optional)
        if not expected_name:
            errors.append("expected_name not provided to validate()")
        elif self.waterbody_name.strip() != expected_name.strip():
            errors.append(f"Name mismatch: expected '{expected_name}', got '{self.waterbody_name}'")
        
        # Check raw_text matches input raw_regs exactly
        if expected_raw_text is not None:
            if self.raw_text != expected_raw_text:
                # Show a preview of the difference
                preview_len = 100
                expected_preview = expected_raw_text[:preview_len] + ('...' if len(expected_raw_text) > preview_len else '')
                actual_preview = self.raw_text[:preview_len] + ('...' if len(self.raw_text) > preview_len else '')
                errors.append(f"raw_text doesn't match input raw_regs exactly. Expected: '{expected_preview}', Got: '{actual_preview}'")
        
        # Validate geographic groups
        if not self.geographic_groups or len(self.geographic_groups) == 0:
            errors.append("No geographic groups found")
        else:
            for idx, group in enumerate(self.geographic_groups):
                group_errors = group.validate(self.waterbody_name)
                for err in group_errors:
                    errors.append(f"Group {idx} ({group.location}): {err}")
        
        return errors
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'waterbody_name': self.waterbody_name,
            'raw_text': self.raw_text,
            'cleaned_text': self.cleaned_text,
            'geographic_groups': [
                {
                    'location': g.location,
                    'raw_text': g.raw_text,
                    'cleaned_text': g.cleaned_text,
                    'rules': [
                        {
                            'verbatim_text': r.verbatim_text,
                            'rule': r.rule,
                            'type': r.type,
                            'dates': r.dates if r.dates is not None else 'null',
                            'species': r.species if r.species is not None else 'null'
                        } for r in g.rules
                    ]
                } for g in self.geographic_groups
            ]
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ParsedWaterbody':
        """Create ParsedWaterbody from dictionary."""
        groups = [ParsedGeographicGroup.from_dict(g) for g in data.get('geographic_groups', [])]
        return cls(
            waterbody_name=data.get('waterbody_name', ''),
            raw_text=data.get('raw_text', ''),
            cleaned_text=data.get('cleaned_text', ''),
            geographic_groups=groups
        )
    
    @classmethod
    def validate_batch(cls, parsed_batch: List[Dict[str, Any]], input_rows: List) -> List[str]:
        """Validate a batch of parsed results matches input order and content.
        
        This is the critical validation that ensures:
        1. Output has same count as input
        2. Output[idx] corresponds to input[idx] (order preservation)
        3. Names are copied verbatim
        4. All individual waterbody validations pass
        
        Args:
            parsed_batch: List of parsed waterbody dicts from LLM
            input_rows: List of input WaterbodyRow objects
        
        Returns:
            List of error messages (empty if valid)
        """
        validation_errors = []
        
        # Check basic structure
        if not isinstance(parsed_batch, list):
            validation_errors.append(f"Result is not a list, got {type(parsed_batch).__name__}")
            return validation_errors
        
        # Check count matches (ORDER VALIDATION: must have same number of items)
        if len(parsed_batch) != len(input_rows):
            validation_errors.append(f"Expected {len(input_rows)} items, got {len(parsed_batch)}")
            return validation_errors
        
        # Validate each item at its position (ORDER VALIDATION: position idx must match)
        for idx, entry in enumerate(parsed_batch):
            try:
                # ORDER + VERBATIM VALIDATION: output[idx].name must exactly match input[idx].name
                # This single check validates both correct ordering AND verbatim copying
                if entry.get('waterbody_name') != input_rows[idx].water:
                    validation_errors.append(
                        f"Item {idx}: Name/order mismatch - expected '{input_rows[idx].water}', "
                        f"got '{entry.get('waterbody_name')}'"
                    )
                
                # Convert to dataclass and validate structure
                parsed = cls.from_dict(entry)
                expected_name = input_rows[idx].water
                expected_raw_text = input_rows[idx].raw_regs
                item_errors = parsed.validate(expected_name, expected_raw_text)
                validation_errors.extend([f"Item {idx}: {err}" for err in item_errors])
            except Exception as e:
                validation_errors.append(f"Item {idx}: Failed to parse - {e}")
        
        return validation_errors

@define
class SessionState:
    """Complete session state for resumable parsing.
    
    CRITICAL: Order preservation is maintained throughout:
    - input_rows: Original input order (index 0 is first input, etc.)
    - results: Indexed array matching input_rows (results[i] corresponds to input_rows[i])
    - Final output iterates through range(total_items) to preserve exact order
    """
    input_rows: List[WaterbodyRow]  # Full input data in original order
    results: List[Optional[ParsedWaterbody]]  # Parsed results indexed by position - results[i] = parsed input_rows[i]
    processed_items: List[int]  # Indices of successfully processed items
    failed_items: List[Dict[str, Any]]  # Items that failed with error info
    validation_failures: List[Dict[str, Any]]  # Items that failed validation (reset on resume for retry)
    retry_counts: Dict[int, int]  # Track retry attempts per item index
    total_items: int
    created_at: str  # ISO timestamp
    last_updated: str  # ISO timestamp
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert session state to dictionary for JSON serialization."""
        # Convert input_rows (WaterbodyRow instances) to dicts
        input_rows_dicts = []
        for row in self.input_rows:
            if hasattr(row, 'to_dict'):
                input_rows_dicts.append(row.to_dict())
            else:
                # Fallback for simple objects with __dict__
                input_rows_dicts.append({'water': row.water, 'raw_regs': row.raw_regs})
        
        # Convert results (ParsedWaterbody instances or None) to dicts
        results_dicts = []
        for result in self.results:
            if result is not None:
                results_dicts.append(result.to_dict())
            else:
                results_dicts.append(None)
        
        # Convert retry_counts keys to strings (JSON requires string keys)
        retry_counts_str = {str(k): v for k, v in self.retry_counts.items()}
        
        return {
            'input_rows': input_rows_dicts,
            'results': results_dicts,
            'processed_items': self.processed_items,
            'failed_items': self.failed_items,
            'validation_failures': self.validation_failures,
            'retry_counts': retry_counts_str,
            'total_items': self.total_items,
            'created_at': self.created_at,
            'last_updated': self.last_updated
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SessionState':
        """Create SessionState from dictionary loaded from JSON."""
        # Convert input_rows dicts back to WaterbodyRow instances
        input_rows = []
        for row_dict in data['input_rows']:
            if 'water' in row_dict and 'raw_regs' in row_dict:
                # If WaterbodyRow has from_dict, use it
                if hasattr(WaterbodyRow, 'from_dict'):
                    input_rows.append(WaterbodyRow.from_dict(row_dict))
                else:
                    # Create simple object with required attributes
                    input_rows.append(type('WaterbodyRow', (), row_dict)())
            else:
                input_rows.append(row_dict)
        
        # Convert results dicts back to ParsedWaterbody instances
        results = []
        for result_dict in data['results']:
            if result_dict is not None:
                results.append(ParsedWaterbody.from_dict(result_dict))
            else:
                results.append(None)
        
        # Convert retry_counts keys back to integers
        retry_counts = {int(k): v for k, v in data.get('retry_counts', {}).items()}
        
        return cls(
            input_rows=input_rows,
            results=results,
            processed_items=data['processed_items'],
            failed_items=data['failed_items'],
            validation_failures=data.get('validation_failures', []),  # Default to empty list for old sessions
            retry_counts=retry_counts,
            total_items=data['total_items'],
            created_at=data['created_at'],
            last_updated=data['last_updated']
        )
    
    def save(self, filepath: str):
        """Save session state to JSON file."""
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        self.last_updated = datetime.now().isoformat()
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(self.to_dict(), f, indent=2, ensure_ascii=False)
    
    @classmethod
    def load(cls, filepath: str) -> Optional['SessionState']:
        """Load session state from JSON file."""
        if not os.path.exists(filepath):
            return None
        with open(filepath, 'r', encoding='utf-8') as f:
            return cls.from_dict(json.load(f))
    
    @classmethod
    def create_new(cls, input_rows: List[WaterbodyRow]) -> 'SessionState':
        """Create new session state."""
        total = len(input_rows)
        now = datetime.now().isoformat()
        return cls(
            input_rows=input_rows,
            results=[None] * total,
            processed_items=[],
            failed_items=[],
            validation_failures=[],
            retry_counts={},
            total_items=total,
            created_at=now,
            last_updated=now
        )

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
        if not hasattr(row, 'water'):
            errors.append(f"Row {idx}: missing 'water' attribute")
        elif not row.water or not str(row.water).strip():
            errors.append(f"Row {idx}: 'water' is empty")
        
        if not hasattr(row, 'raw_regs'):
            errors.append(f"Row {idx}: missing 'raw_regs' attribute")
        elif not row.raw_regs or not str(row.raw_regs).strip():
            errors.append(f"Row {idx}: 'raw_regs' is empty")
    
    return errors

def validate_partial_json(json_path: str, input_rows: Optional[List] = None) -> Dict[str, Any]:
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
        return {'valid': False, 'errors': [f"File not found: {json_path}"], 'warnings': [], 'file_type': 'unknown'}
    
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        return {'valid': False, 'errors': [f"Invalid JSON: {e}"], 'warnings': [], 'file_type': 'unknown'}
    
    all_errors = []
    all_warnings = []
    file_type = 'unknown'
    
    # Detect file type
    if isinstance(data, dict) and 'input_rows' in data and 'results' in data and 'processed_items' in data:
        # This is a session file
        file_type = 'session'
        
        try:
            session = SessionState.from_dict(data)
            
            # Use input_rows from session itself
            session_input_rows = session.input_rows
            
            # Validate session structure
            if session.total_items != len(session.input_rows):
                all_errors.append(f"Session total_items ({session.total_items}) doesn't match input_rows count ({len(session.input_rows)})")
            
            if len(session.results) != session.total_items:
                all_errors.append(f"Session results array length ({len(session.results)}) doesn't match total_items ({session.total_items})")
            
            # Validate each processed result
            items_checked = 0
            for idx in session.processed_items:
                if idx >= len(session.results):
                    all_errors.append(f"Processed item index {idx} out of bounds (results length: {len(session.results)})")
                    continue
                
                result = session.results[idx]
                if result is None:
                    all_errors.append(f"Item {idx}: Marked as processed but result is None")
                    continue
                
                # Validate the parsed waterbody
                try:
                    expected_name = session_input_rows[idx].water if idx < len(session_input_rows) else None
                    expected_raw_text = session_input_rows[idx].raw_regs if idx < len(session_input_rows) else None
                    errors = result.validate(expected_name, expected_raw_text)
                    
                    if errors:
                        all_errors.extend([f"Item {idx} ({result.waterbody_name}): {err}" for err in errors])
                    items_checked += 1
                except Exception as e:
                    all_errors.append(f"Item {idx}: Validation failed - {e}")
            
            # Report on unprocessed items
            unprocessed_count = session.total_items - len(session.processed_items) - len(session.failed_items) - len(session.validation_failures)
            if unprocessed_count > 0:
                all_warnings.append(f"{unprocessed_count} items not yet processed")
            
            # Report on failed items
            if session.failed_items:
                all_warnings.append(f"{len(session.failed_items)} items permanently failed")
                for failed in session.failed_items[:5]:
                    all_warnings.append(f"  - Index {failed['index']}: {failed.get('waterbody', 'unknown')}")
            
            # Report on validation failures
            if session.validation_failures:
                all_warnings.append(f"{len(session.validation_failures)} items failed validation (will retry on --resume)")
                for failed in session.validation_failures[:5]:
                    all_warnings.append(f"  - Index {failed['index']}: {failed.get('waterbody', 'unknown')}")
            
            return {
                'valid': len(all_errors) == 0,
                'errors': all_errors,
                'warnings': all_warnings,
                'file_type': file_type,
                'items_checked': items_checked,
                'session_info': {
                    'total_items': session.total_items,
                    'processed': len(session.processed_items),
                    'failed': len(session.failed_items),
                    'validation_failed': len(session.validation_failures),
                    'created_at': session.created_at,
                    'last_updated': session.last_updated
                }
            }
            
        except Exception as e:
            all_errors.append(f"Failed to load session: {e}")
            return {
                'valid': False,
                'errors': all_errors,
                'warnings': all_warnings,
                'file_type': file_type
            }
    
    elif isinstance(data, list):
        # This is a parsed results file
        file_type = 'parsed_results'
        
        for idx, item in enumerate(data):
            # Check for error placeholders
            if isinstance(item, dict) and 'error' in item:
                all_warnings.append(f"Item {idx} ({item.get('waterbody_name', 'unknown')}): {item['error']}")
                continue
            
            try:
                # Convert to dataclass and validate
                parsed = ParsedWaterbody.from_dict(item)
                expected_name = input_rows[idx].water if (input_rows and idx < len(input_rows)) else None
                expected_raw_text = input_rows[idx].raw_regs if (input_rows and idx < len(input_rows)) else None
                errors = parsed.validate(expected_name, expected_raw_text)
                
                if errors:
                    all_errors.extend([f"Item {idx} ({parsed.waterbody_name}): {err}" for err in errors])
            except Exception as e:
                all_errors.append(f"Item {idx}: Failed to parse - {e}")
        
        return {
            'valid': len(all_errors) == 0,
            'errors': all_errors,
            'warnings': all_warnings,
            'file_type': file_type,
            'items_checked': len(data)
        }
    
    else:
        return {
            'valid': False,
            'errors': ["JSON must be either a session object or a list of waterbody results"],
            'warnings': all_warnings,
            'file_type': 'unknown'
        }

def revalidate_session_results(session: 'SessionState') -> List[int]:
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
    print(f"Revalidating {len(session.processed_items)} processed items with current validation rules...")
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
        print(f"\n⚠ {len(failed_indices)} items failed revalidation and will be reprocessed")
        print(f"Indices: {failed_indices[:20]}{'...' if len(failed_indices) > 20 else ''}")
    else:
        print(f"\n✓ All {len(session.processed_items)} processed items passed revalidation")
    
    return failed_indices

class SynopsisParser:
    """Parser for fishing regulation synopsis data using LLM."""
    
    @staticmethod
    def get_prompt(waterbody_rows: List):
        """
        Enforces a hierarchical subject-predicate relationship while preserving
        full block context and individual rules.
        
        Args:
            waterbody_rows: List of WaterbodyRow objects with water and raw_regs attributes
        """
        # Format inputs from WaterbodyRow objects
        batch_inputs = [f"Waterbody Name: {row.water} | Regulation Block: {row.raw_regs}" for row in waterbody_rows]
        
        return f"""
            You are a legal data architect. Parse this list of fishing regulation blocks into a JSON array. 
            All information must be preserved. All verbatim text must keep original punctuation and line breaks.
            Each object in the array corresponds to a waterbody with its regulations. 
            Rules must exist within the regulation block they are extracted from.
            
            DIRECTIONS:
            1. HIERARCHY: Map the input text into 'geographic_groups'.
            2. CONTEXT: For each group, provide 'raw_text' (verbatim from input) and 'cleaned_text' (fixed word-breaks, collapsed hyphens, single line).
            3. RULES: Split the 'cleaned_text' of that group into individual rule objects in the 'rules' array.
            4. LISTS: Split nested lists (a, b, c) into individual rule objects that are referencing the proper location.
            5. VERBATIM: Do not summarize. Every word of the original text must exist within the 'geographic_groups'.
            6. TYPES: Classify each rule into one of: closure, harvest, gear_restriction, restriction, licensing, access, note.
            7. DATES & SPECIES: Extract date ranges and species into arrays, or null if none found. Dates should be in the exact format as found in the text.
            8. FORMATTING: Ensure valid JSON output.
            9. RULES EXTRACTION: Extract all rules, even if they overlap in meaning. One rule per object. Multiple rules can exist in one block of text.
            10. MAKE SURE ALL ENTRIES ARE FILLED OUT AS PER THE SCHEMA BELOW. DO NOT LEAVE ANYTHING BLANK. DO NOT SKIP ANY RULES OR WATERBODIES.
            
            CRITICAL REQUIREMENTS - ORDER AND VERBATIM COPYING:
            1. Return EXACTLY {len(waterbody_rows)} items in the EXACT SAME ORDER as input
            2. Output array index MUST match input array index (output[0] = input[0], output[1] = input[1], etc.)
            3. Copy "waterbody_name" VERBATIM from "Waterbody Name:" in input - CHARACTER-FOR-CHARACTER, byte-for-byte copy
            4. Copy "raw_text" VERBATIM from "Regulation Block:" in input - CHARACTER-FOR-CHARACTER, byte-for-byte copy
            5. Process ALL items completely - do not skip any
            
            CRITICAL: DO NOT "FIX" OR "CORRECT" NAMES AND RAW TEXT:
            ⚠️ WRONG EXAMPLES - DO NOT DO THIS:
            - Input: "CAYCUSE RIVER" → waterbody_name: "CAYUSE RIVER" ❌ WRONG - You changed spelling
            - Input: '"PETE\'S POND" Unnamed lake at the head of San Juan River' → waterbody_name: "PETE'S POND" ❌ WRONG - You removed text
            - Input: "COQUIHALLA RIVER" → waterbody_name: "Coquihalla River" ❌ WRONG - You changed capitalization
            - Input: "Lake   St. Mary" → waterbody_name: "Lake St. Mary" ❌ WRONG - You fixed spacing
            
            ✓ CORRECT EXAMPLES:
            - Input: "CAYCUSE RIVER" → waterbody_name: "CAYCUSE RIVER" ✓ (even if it looks like a typo)
            - Input: '"PETE\'S POND" Unnamed lake at the head of San Juan River' → waterbody_name: '"PETE\'S POND" Unnamed lake at the head of San Juan River' ✓ (copy ALL of it)
            - Input: "COQUIHALLA RIVER" → waterbody_name: "COQUIHALLA RIVER" ✓ (preserve exact capitalization)
            - Input: "Lake   St. Mary" → waterbody_name: "Lake   St. Mary" ✓ (preserve spacing, even if weird)
            
            IMPORTANT: WHEN TO BE A DUMB COPIER VS WHEN TO USE INTELLIGENCE:
            
            VERBATIM COPYING REQUIRED (no modification allowed):
            - "waterbody_name" field → Copy EXACTLY from input "Waterbody Name:" - character for character
            - "raw_text" field (at waterbody level) → Copy EXACTLY from input "Regulation Block:" - character for character
            - "raw_text" field (in geographic groups) → Copy EXACTLY from parent waterbody raw_text - must be exact substring
            - "verbatim_text" field (in rules) → Copy EXACTLY from geographic group's raw_text - must be exact substring
            
            INTELLIGENCE AND PROCESSING REQUIRED (use your understanding):
            - "location" field → Extract and describe the geographic area (e.g., "upstream of dam", "tributaries", "downstream of bridge [Including Tributaries]")
            - "cleaned_text" field → Fix word-breaks, remove newlines, correct punctuation, make readable, coherent and easy to understand the actual requirements while preserving the original meaning completely. 
            - "rule" field → Extract and normalize the specific rule (e.g., "No Fishing", "Bait ban")
            - "type" field → Classify the rule type (closure|harvest|gear_restriction|restriction|licensing|access|note)
            - "species" field → Identify fish species mentioned (use knowledge of fish names)
            - "dates" field → Extract date ranges in their original format
            - Splitting regulations into geographic_groups → Use understanding of geographic context
            - Splitting text into individual rules → Use understanding of regulatory structure
            
            REMEMBER: You are a LEGAL DATA ARCHITECT with expertise in fishing regulations.
            - Be a photocopier for names and raw source text
            - Be an intelligent parser for extracting meaning, structure, species, locations, and rule types
            
            
            CRITICAL: VERBATIM_TEXT REQUIREMENTS
            - The "verbatim_text" field in each rule MUST be an EXACT substring copied character-for-character from the geographic group's "raw_text"
            - DO NOT rephrase, rewrite, paraphrase, or reconstruct the text
            - DO NOT add words that aren't in the original (like repeating "No Fishing" for each date range)
            - DO NOT skip connecting words like "and", "or", "from"
            - Include ALL context necessary to understand the rule, even if it means the verbatim_text is longer and/or contains other rules
            - The verbatim_text must be findable in raw_text using exact string matching (case-insensitive, whitespace-normalized)
            
            EXAMPLES OF CORRECT VERBATIM_TEXT:
                CORRECT: If raw_text is "No Fishing Dec 1-May 31 and July 15-Aug 31"
                    Rule 1 verbatim_text: "No Fishing Dec 1-May 31"
                    Rule 2 verbatim_text: "No Fishing Dec 1-May 31 and July 15-Aug 31"
                
                WRONG: If raw_text is "No Fishing Dec 1-May 31 and July 15-Aug 31"
                    Rule 2 verbatim_text: "No Fishing July 15-Aug 31" (WRONG - "No Fishing July" doesn't appear in raw_text)
                
                CORRECT: If raw_text is "Trout/char catch and release, bait ban"
                    Rule 1 verbatim_text: "Trout/char catch and release, bait ban" (full context)
                    Rule 2 verbatim_text: "bait ban" (substring that appears in raw_text in correct location)
                
                WRONG: If raw_text is "Trout/char catch and release, bait ban"
                    Rule 1 verbatim_text: "Trout/char catch and release only" (WRONG - "only" not in raw_text)
            
            JSON SCHEMA:
            List of objects:
            {{
                "waterbody_name": "EXACT VERBATIM name from 'Waterbody Name:' field - copy exactly, do not modify",
                "raw_text": "EXACT VERBATIM full regulation block from the 'Regulation Block:' field. Does not include name only the text. - copy exactly, do not modify",
                "cleaned_text": "The block of text with repaired word-breaks and newlines. Mains full context. Has fixed Punctuation.",
                "geographic_groups": [
                    {{
                        "location": "Location anchor (if any), blank assumes the whole waterbody. E.g., 'upstream of the dam', 'tributaries', 'from the bridge to the lake'",
                        "raw_text": "The verbatim block of text for the context of this location (including newlines/hyphens)",
                        "cleaned_text": "The block of text with repaired word-breaks, no newlines, and corrected punctuation. Maintains full context.",
                        "rules": [
                            {{
                                "verbatim_text": "EXACT substring from geographic group's raw_text - must be findable via exact string matching. Include full context needed to understand the rule. DO NOT paraphrase or add words not in the original.",
                                "rule": "Specific rule extracted, normalized. E.g., 'No Fishing', 'Trout catch and release'. One rule per object. Multiple rules can exist in one block of text.",
                                "type": "closure|harvest|gear_restriction|restriction|licensing|access|note",
                                "dates": ["Date ranges found, or null"],
                                "species": ["Fish types found, or null"]
                            }}
                        ]
                    }}
                ]
            }}
            
            A Complete example with the format desired is shown below:
            ---
            INPUT:
            Waterbody Name: "Coquihalla River" | Regulation Block: "No Fishing upstream of the northern entrance to the upper most railway tunnel, Nov 1-June 30 (see map on page 24)\\nFly fishing only; bait ban upstream of the northern entrance to the upper most railway tunnel, Jul 1-Oct 31\\nNo Fishing downstream of the southern entrance to the lower most railway tunnel, Apr 1-Oct 31\\nNo Fishing at Othello Tunnels from the northern entrance to the upper most railway tunnel to the southern entrance of the lower most tunnel;\\napproximately 700 m length\\nTrout/char (including steelhead) catch and release, bait ban, downstream of the southern entrance to the lower most railway tunnel, Nov\\n1-Mar 31"
            
            OUTPUT:
            {{
                "waterbody_name": "Coquihalla River",
                "raw_text": "No Fishing upstream of the northern entrance to the upper most railway tunnel, Nov 1-June 30 (see map on page 24)\\nFly fishing only; bait ban upstream of the northern entrance to the upper most railway tunnel, Jul 1-Oct 31\\nNo Fishing downstream of the southern entrance to the lower most railway tunnel, Apr 1-Oct 31\\nNo Fishing at Othello Tunnels from the northern entrance to the upper most railway tunnel to the southern entrance of the lower most tunnel;\\napproximately 700 m length\\nTrout/char (including steelhead) catch and release, bait ban, downstream of the southern entrance to the lower most railway tunnel, Nov\\n1-Mar 31",
                "cleaned_text": "No Fishing upstream of the northern entrance to the upper most railway tunnel, Nov 1-June 30 (see map on page 24). Fly fishing only; bait ban upstream of the northern entrance to the upper most railway tunnel, Jul 1-Oct 31. No Fishing downstream of the southern entrance to the lower most railway tunnel, Apr 1-Oct 31. No Fishing at Othello Tunnels from the northern entrance to the upper most railway tunnel to the southern entrance of the lower most tunnel; approximately 700 m length. Trout/char (including steelhead) catch and release, bait ban, downstream of the southern entrance to the lower most railway tunnel, Nov 1-Mar 31.",
                "geographic_groups": [
                {{
                    "location": "upstream of the northern entrance to the upper most railway tunnel",
                    "raw_text": "No Fishing upstream of the northern entrance to the upper most railway tunnel, Nov 1-June 30 (see map on page 24)\\nFly fishing only; bait ban upstream of the northern entrance to the upper most railway tunnel, Jul 1-Oct 31",
                    "cleaned_text": "No Fishing upstream of the northern entrance to the upper most railway tunnel, Nov 1-June 30 (see map on page 24). Fly fishing only and bait ban upstream of the northern entrance to the upper most railway tunnel, Jul 1-Oct 31.",
                    "rules": [
                        {{
                            "verbatim_text": "No Fishing upstream of the northern entrance to the upper most railway tunnel, Nov 1-June 30 (see map on page 24)",
                            "rule": "No Fishing",
                            "type": "closure",
                            "dates": [
                                "Nov 1-June 30"
                            ],
                            "species": "null"
                        }},
                        {{
                            "verbatim_text": "Fly fishing only; bait ban upstream of the northern entrance to the upper most railway tunnel, Jul 1-Oct 31",
                            "rule": "Fly fishing only",
                            "type": "gear_restriction",
                            "dates": [
                                "Jul 1-Oct 31"
                            ],
                            "species": "null"
                        }},
                        {{
                            "verbatim_text": "bait ban upstream of the northern entrance to the upper most railway tunnel, Jul 1-Oct 31",
                            "rule": "Bait ban",
                            "type": "gear_restriction",
                            "dates": [
                                "Jul 1-Oct 31"
                            ],
                            "species": "null"
                        }}
                    ]
                }},
                {{
                    "location": "downstream of the southern entrance to the lower most railway tunnel",
                    "raw_text": "No Fishing downstream of the southern entrance to the lower most railway tunnel, Apr 1-Oct 31\\nTrout/char (including steelhead) catch and release, bait ban, downstream of the southern entrance to the lower most railway tunnel, Nov\\n1-Mar 31",
                    "cleaned_text": "No Fishing downstream of the southern entrance to the lower most railway tunnel, Apr 1-Oct 31. Trout/char (including steelhead) catch and release, bait ban, downstream of the southern entrance to the lower most railway tunnel, Nov 1-Mar 31.",
                    "rules": [
                        {{
                            "verbatim_text": "No Fishing downstream of the southern entrance to the lower most railway tunnel, Apr 1-Oct 31",
                            "rule": "No Fishing",
                            "type": "closure",
                            "dates": [
                                "Apr 1-Oct 31"
                            ],
                            "species": "null"
                        }},
                        {{
                            "verbatim_text": "Trout/char (including steelhead) catch and release, bait ban, downstream of the southern entrance to the lower most railway tunnel, Nov\\n1-Mar 31",
                            "rule": "Trout/char (including steelhead) catch and release",
                            "type": "harvest",
                            "dates": [
                                "Nov 1-Mar 31"
                            ],
                            "species": [
                                "trout",
                                "char",
                                "steelhead"
                            ]
                        }},
                        {{
                            "verbatim_text": "bait ban, downstream of the southern entrance to the lower most railway tunnel, Nov 1-Mar 31",
                            "rule": "Bait ban",
                            "type": "gear_restriction",
                            "dates": [
                                "Nov 1-Mar 31"
                            ],
                            "species": "null"
                        }}
                    ]
                }},
                {{
                    "location": "Othello Tunnels from the northern entrance to the upper most railway tunnel to the southern entrance of the lower most tunnel",
                    "raw_text": "No Fishing at Othello Tunnels from the northern entrance to the upper most railway tunnel to the southern entrance of the lower most tunnel;\\napproximately 700 m length",
                    "cleaned_text": "No Fishing at Othello Tunnels from the northern entrance to the upper most railway tunnel (see map on page 24) to the southern entrance of the lower most tunnel; approximately 700 m length.",
                    "rules": [
                        {{
                            "verbatim_text": "No Fishing at Othello Tunnels from the northern entrance to the upper most railway tunnel to the southern entrance of the lower most tunnel; approximately 700 m length",
                            "rule": "No Fishing",
                            "type": "closure",
                            "dates": "null",
                            "species": "null"
                        }}
                    ]
                }}
                ]
            }},

            INPUT DATA:
            {json.dumps(batch_inputs)}
            """

    @classmethod
    def parse_synopsis_batch(cls, waterbody_rows: List, api_manager: APIKeyManager = None):
        """
        Parse a list of WaterbodyRow objects with API key rotation.
        
        Args:
            waterbody_rows: List of WaterbodyRow objects with water and raw_regs attributes
            api_manager: APIKeyManager instance for handling multiple keys
        """
        if api_manager is None:
            api_manager = api_key_manager
        
        try:
            prompt = cls.get_prompt(waterbody_rows)
            current_client = api_manager.get_current_client()
            
            response = current_client.models.generate_content(
                model='gemini-2.5-flash-lite', # Updated to the latest stable flash
                # model='gemini-2.0-flash',
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type='application/json',
                    temperature=0.1,
                    cached_content=None
                )
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
                    return {"error": f"Result is not a list, got {type(parsed_result).__name__}"}
                
                if len(parsed_result) != len(waterbody_rows):
                    return {"error": f"Expected {len(waterbody_rows)} items, got {len(parsed_result)}"}
                
                # Validate each item individually and collect results
                validated_results = []
                item_errors = []
                
                for idx, entry in enumerate(parsed_result):
                    try:
                        # Validate this specific item
                        input_row = waterbody_rows[idx]
                        
                        # Check name matches (ORDER + VERBATIM VALIDATION)
                        if entry.get('waterbody_name') != input_row.water:
                            error_msg = f"Name/order mismatch - expected '{input_row.water}', got '{entry.get('waterbody_name')}'"
                            item_errors.append({
                                'batch_index': idx,
                                'waterbody': input_row.water,
                                'error': error_msg,
                                'error_type': 'name_mismatch'
                            })
                            validated_results.append(None)
                            continue
                        
                        # Convert to dataclass and validate structure
                        parsed = ParsedWaterbody.from_dict(entry)
                        validation_errors = parsed.validate(input_row.water, input_row.raw_regs)
                        
                        if validation_errors:
                            error_msg = '; '.join(validation_errors[:3]) + ('...' if len(validation_errors) > 3 else '')
                            item_errors.append({
                                'batch_index': idx,
                                'waterbody': input_row.water,
                                'error': error_msg,
                                'error_type': 'validation_error',
                                'all_errors': validation_errors
                            })
                            validated_results.append(None)
                        else:
                            # Item passed validation
                            validated_results.append(entry)
                    
                    except Exception as e:
                        item_errors.append({
                            'batch_index': idx,
                            'waterbody': waterbody_rows[idx].water,
                            'error': f"Failed to parse - {str(e)}",
                            'error_type': 'parse_error'
                        })
                        validated_results.append(None)
                
                # Return results with partial success info
                api_manager.record_success()
                return {
                    'results': validated_results,
                    'item_errors': item_errors,
                    'success_count': len([r for r in validated_results if r is not None]),
                    'failed_count': len(item_errors)
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
            
            # Record failure and check if we should rotate keys
            can_continue = api_manager.record_failure()
            
            # Check for rate limiting or API errors
            if 'rate limit' in error_msg.lower() or '429' in error_msg or 'quota' in error_msg.lower():
                if can_continue and not api_manager.all_keys_exhausted():
                    # Try again with the rotated key
                    wait_time = 2  # Short wait before trying next key
                    time.sleep(wait_time)
                    return cls.parse_synopsis_batch(waterbody_rows, api_manager)
                else:
                    return {"error": f"All API keys exhausted. Key status: {api_manager.get_status()}"}
            
            return {"error": error_msg}


# --- FAILURE LOGGING ---

def log_failure_details(failure_log_file: str, batch_indices: List[int], item_errors: List[Dict], waterbody_rows: List):
    """
    Log detailed failure information to a file for analysis and prompt improvement.
    
    Args:
        failure_log_file: Path to the failure log file
        batch_indices: Indices of items in the batch
        item_errors: List of error dictionaries with keys: batch_index, waterbody, error, error_type
        waterbody_rows: Full list of input rows
    """
    os.makedirs(os.path.dirname(failure_log_file), exist_ok=True)
    
    # Prepare log entry
    log_entry = {
        'timestamp': datetime.now().isoformat(),
        'batch_indices': batch_indices,
        'failures': []
    }
    
    for error_info in item_errors:
        batch_idx = error_info['batch_index']
        actual_idx = batch_indices[batch_idx]
        input_row = waterbody_rows[actual_idx]
        
        failure_detail = {
            'index': actual_idx,
            'waterbody_name': input_row.water,
            'raw_regs': input_row.raw_regs,
            'error_type': error_info['error_type'],
            'error': error_info['error']
        }
        
        # Add all validation errors if available
        if 'all_errors' in error_info:
            failure_detail['all_validation_errors'] = error_info['all_errors']
        
        log_entry['failures'].append(failure_detail)
    
    # Append to log file
    file_exists = os.path.exists(failure_log_file)
    with open(failure_log_file, 'a', encoding='utf-8') as f:
        if file_exists:
            f.write(',\n')
        else:
            f.write('[\n')
        json.dump(log_entry, f, indent=2, ensure_ascii=False)
    
    # Create a summary file for easier review
    summary_file = failure_log_file.replace('.json', '_summary.txt')
    with open(summary_file, 'a', encoding='utf-8') as f:
        f.write(f"\n{'='*80}\n")
        f.write(f"Batch Failures at {log_entry['timestamp']}\n")
        f.write(f"{'='*80}\n")
        for failure in log_entry['failures']:
            f.write(f"\n[{failure['index']}] {failure['waterbody_name']}\n")
            f.write(f"Error Type: {failure['error_type']}\n")
            f.write(f"Error: {failure['error']}\n")
            f.write(f"Input: {failure['raw_regs'][:200]}...\n")
            if 'all_validation_errors' in failure:
                f.write(f"All Errors:\n")
                for err in failure['all_validation_errors']:
                    f.write(f"  - {err}\n")


# --- BATCH DEBUG RUNNER ---

def run_llm_parsing(waterbody_rows: Optional[List] = None, output_file='output/llm_parser/llm_parsed_results.json', 
                    batch_size=10, session_file='output/llm_parser/session.json', resume=False):
    """
    Run LLM parsing with batching support and progress tracking.
    
    Args:
        waterbody_rows: List of waterbody objects to parse (optional if resuming)
        output_file: Final output file path
        batch_size: Number of items to process per batch (smaller = more consistent, less rate limiting)
        session_file: Path to save/load session state (JSON file)
        resume: Whether to resume from previous session
    """
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
    
    # Check if session file exists
    existing_session = SessionState.load(session_file)
    
    if existing_session and len(existing_session.processed_items) > 0:
        # Session file exists with completed items
        if resume:
            # User explicitly requested resume
            session = existing_session
            waterbody_rows = session.input_rows  # Load from session
            print(f"✓ Resumed from session file: {len(session.processed_items)}/{session.total_items} items completed")
            print(f"   Session created: {session.created_at}")
            print(f"   Last updated: {session.last_updated}")
        else:
            # Ask user if they want to resume
            print(f"\n⚠ Found existing session: {len(existing_session.processed_items)}/{existing_session.total_items} items completed")
            print(f"   Session file: {session_file}")
            print(f"   Created: {existing_session.created_at}")
            
            response = input("\nDo you want to resume from this session? [Y/n]: ").strip().lower()
            
            if response in ('', 'y', 'yes'):
                session = existing_session
                waterbody_rows = session.input_rows  # Load from session
                print(f"✓ Resuming from existing session...")
            else:
                print(f"✓ Starting fresh (old session will be overwritten)")
                # Delete old session file
                if os.path.exists(session_file):
                    os.remove(session_file)
    elif resume:
        print("⚠ --resume flag provided but no session file found")
        if waterbody_rows is None:
            print("✗ Error: Cannot resume without session file and no input data provided")
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
    print(f"API keys available: {len(API_KEYS)} ({', '.join(k['id'] for k in API_KEYS)})")
    
    # If resuming, revalidate processed items with current validation rules
    # AND include permanently failed items for retry
    revalidation_failed = []
    if resume and (len(session.processed_items) > 0 or len(session.failed_items) > 0 or len(session.validation_failures) > 0):
        revalidation_failed = revalidate_session_results(session)
        
        # Add all null/None results for reprocessing
        null_results = [i for i in range(session.total_items) if session.results[i] is None]
        if null_results:
            print(f"\n⚠  Found {len(null_results)} null results, will reprocess")
            for idx in null_results:
                if idx not in revalidation_failed:
                    revalidation_failed.append(idx)
        
        # Add permanently failed items for retry on resume
        permanently_failed_indices = [f['index'] for f in session.failed_items]
        if permanently_failed_indices:
            print(f"⚠  Retrying {len(permanently_failed_indices)} previously failed items")
            # Add to revalidation_failed list (avoid duplicates)
            for idx in permanently_failed_indices:
                if idx not in revalidation_failed:
                    revalidation_failed.append(idx)
        
        # ALWAYS retry validation failures on resume (these are likely prompt/validation issues)
        validation_failed_indices = [f['index'] for f in session.validation_failures]
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
            session.processed_items = [i for i in session.processed_items if i not in revalidation_failed]
            
            # Clear their results so they get reprocessed
            for idx in revalidation_failed:
                session.results[idx] = None
                # Reset retry count for fresh attempts
                if idx in session.retry_counts:
                    del session.retry_counts[idx]
            
            # Remove from failed_items if they were there
            session.failed_items = [f for f in session.failed_items if f['index'] not in revalidation_failed]
            
            # Save updated session
            session.save(session_file)
            print(f"✓  Session updated: {len(revalidation_failed)} items marked for reprocessing\n")
            
            # Reprocess failed items in batches BEFORE continuing with unprocessed items
            print(f"{'─'*80}")
            print(f"REVALIDATION ({len(revalidation_failed)} items)")
            print(f"{'─'*80}")
            
            revalidation_start_time = datetime.now()
            
            for batch_start in range(0, len(revalidation_failed), batch_size):
                batch_indices = revalidation_failed[batch_start:batch_start + batch_size]
                batch_rows = [waterbody_rows[i] for i in batch_indices]
                
                batch_num = batch_start // batch_size + 1
                total_batches = (len(revalidation_failed) + batch_size - 1) // batch_size
                
                # Calculate progress
                revalidation_completed = batch_start
                revalidation_progress_pct = (revalidation_completed / len(revalidation_failed)) * 100
                total_session_completed = len(session.processed_items)
                total_session_progress_pct = (total_session_completed / total_items) * 100
                
                print(f"\n[Batch {batch_num}/{total_batches}] Indices {batch_indices[0]}-{batch_indices[-1]} | "
                      f"Revalidation: {revalidation_progress_pct:.0f}% | Overall: {total_session_progress_pct:.0f}%")
                print(f"  API Keys: {api_key_manager.get_status()}")
                
                # Parse batch
                batch_results = parser.parse_synopsis_batch(batch_rows)
                
                # Check for complete batch errors (API errors, etc.)
                if isinstance(batch_results, dict) and "error" in batch_results and "results" not in batch_results:
                    # Complete batch failure (API error, malformed JSON, etc.)
                    error_msg = batch_results['error']
                    print(f"  ✗ Batch Failed: {error_msg}")
                    
                    # Check if all API keys are exhausted
                    if api_key_manager.all_keys_exhausted():
                        print(f"\n⚠  All API keys exhausted!")
                        print(f"   Key status: {api_key_manager.get_status()}")
                        print(f"   Session saved to: {session_file}")
                        print(f"\n   Wait for quota reset and run with --resume to continue.")
                        return None
                    
                    # Track retry counts for entire batch
                    max_retries = 3
                    for idx in batch_indices:
                        retry_count = session.retry_counts.get(idx, 0)
                        session.retry_counts[idx] = retry_count + 1
                        
                        if session.retry_counts[idx] >= max_retries:
                            # Mark as permanently failed
                            if idx not in [f['index'] for f in session.failed_items]:
                                session.failed_items.append({
                                    'index': idx,
                                    'waterbody': waterbody_rows[idx].water,
                                    'error': f"Revalidation batch error: {error_msg}",
                                    'retries': retry_count + 1
                                })
                            print(f"    ✗ Item {idx} permanently failed after {max_retries} retries")
                    
                    session.save(session_file)
                    continue
                
                # Handle partial batch success (new format)
                if isinstance(batch_results, dict) and "results" in batch_results:
                    results_list = batch_results['results']
                    item_errors = batch_results.get('item_errors', [])
                    success_count = batch_results.get('success_count', 0)
                    failed_count = batch_results.get('failed_count', 0)
                    
                    # Process successful items - preserve order by mapping batch index to actual index
                    for i, result_dict in enumerate(results_list):
                        if result_dict is not None and i < len(batch_indices):
                            idx = batch_indices[i]  # Map batch position to actual dataset position
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
                        failure_log_file = 'output/llm_parser/failure_log.json'
                        log_failure_details(failure_log_file, batch_indices, item_errors, waterbody_rows)
                        
                        # Track individual item failures
                        for error_info in item_errors:
                            batch_idx = error_info['batch_index']  # Index within the batch (0-9)
                            actual_idx = batch_indices[batch_idx]  # Actual index in full dataset
                            retry_count = session.retry_counts.get(actual_idx, 0)
                            session.retry_counts[actual_idx] = retry_count + 1
                    
                    print(f"  ✓ Partial Success: {success_count}/{len(batch_indices)} items succeeded")
                    if failed_count > 0:
                        print(f"    Failed items logged to failure_log.json")
                    
                # Handle old format (full list of dicts) for backwards compatibility
                elif isinstance(batch_results, list):
                    for i, result_dict in enumerate(batch_results):
                        if i < len(batch_indices):
                            idx = batch_indices[i]  # Map batch position to actual dataset position
                            # Convert dict to ParsedWaterbody instance
                            parsed_waterbody = ParsedWaterbody.from_dict(result_dict)
                            # Store in correct position - session.results[idx] maintains input order
                            session.results[idx] = parsed_waterbody
                            if idx not in session.processed_items:
                                session.processed_items.append(idx)
                    
                    print(f"  ✓ Success")
                else:
                    print(f"  ✗ Unexpected result format: {type(batch_results)}")
                
                # Save after each batch
                session.save(session_file)
                
                # Small delay between batches
                if batch_start + batch_size < len(revalidation_failed):
                    time.sleep(1)
            
            # Summary of revalidation reprocessing
            revalidation_elapsed = (datetime.now() - revalidation_start_time).total_seconds()
            revalidation_success = len([i for i in revalidation_failed if i in session.processed_items])
            revalidation_failed_count = len([i for i in revalidation_failed if i not in session.processed_items])
            
            print(f"\n{'─'*80}")
            if revalidation_failed_count > 0:
                print(f"Revalidation complete: {revalidation_success}/{len(revalidation_failed)} succeeded ({int(revalidation_elapsed)}s)")
            else:
                print(f"Revalidation complete: All {revalidation_success} items succeeded ({int(revalidation_elapsed)}s)")
            print(f"{'─'*80}\n")
    
    # Determine which items need processing
    # Only exclude successfully processed items
    # Failed items will be retried when user manually resumes (after deleting session file)
    items_to_process = [i for i in range(total_items) if i not in session.processed_items]
    
    if not items_to_process:
        print("✓  All items already processed!")
        # Compile final results from parsed class instances - maintain order
        final_results = []
        for idx in range(total_items):
            if session.results[idx] is not None:
                final_results.append(session.results[idx].to_dict())
            else:
                # Include error placeholder for failed items to maintain order
                failed_info = next((f for f in session.failed_items if f['index'] == idx), None)
                if not failed_info:
                    failed_info = next((f for f in session.validation_failures if f['index'] == idx), None)
                error_msg = failed_info['error'] if failed_info else 'Not processed'
                final_results.append({
                    'waterbody_name': waterbody_rows[idx].water,
                    'error': f"FAILED_TO_PARSE: {error_msg}",
                    'raw_text': waterbody_rows[idx].raw_regs,
                    'cleaned_text': '',
                    'geographic_groups': []
                })
        
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(final_results, f, indent=2, ensure_ascii=False)
        print(f"✓  Saved final results to: {output_file}")
        return session.results
    
    # Track timing for progress estimates
    start_time = datetime.now()
    
    # Process in batches
    print(f"{'─'*80}")
    print(f"PROCESSING ({len(items_to_process)} items remaining)")
    print(f"{'─'*80}")
    
    for batch_start in range(0, len(items_to_process), batch_size):
        batch_indices = items_to_process[batch_start:batch_start + batch_size]
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
            est_seconds = remaining_items / items_per_second if items_per_second > 0 else 0
            
            if est_seconds < 60:
                time_str = f" | ETA: {int(est_seconds)}s"
            elif est_seconds < 3600:
                time_str = f" | ETA: {int(est_seconds / 60)}m"
            else:
                time_str = f" | ETA: {int(est_seconds / 3600)}h {int((est_seconds % 3600) / 60)}m"
        
        print(f"\n[Batch {batch_num}/{total_batches}] Indices {batch_indices[0]}-{batch_indices[-1]} | "
              f"Overall: {progress_pct:.0f}%{time_str}")
        print(f"  API Keys: {api_key_manager.get_status()}")
        
        # Parse batch
        batch_results = parser.parse_synopsis_batch(batch_rows)
        
        # Check for complete batch errors (API errors, malformed JSON, etc.)
        if isinstance(batch_results, dict) and "error" in batch_results and "results" not in batch_results:
            # Complete batch failure
            error_msg = batch_results['error']
            print(f"  ✗ Batch Failed: {error_msg}")
            
            # Check if all API keys are exhausted
            if api_key_manager.all_keys_exhausted():
                print(f"\n⚠  All API keys exhausted!")
                print(f"   Key status: {api_key_manager.get_status()}")
                print(f"   Session saved to: {session_file}")
                print(f"\n   Wait for quota reset and run with --resume to continue.")
                return None
            
            # Track retry counts for entire batch
            max_retries = 3
            for idx in batch_indices:
                retry_count = session.retry_counts.get(idx, 0)
                session.retry_counts[idx] = retry_count + 1
            
            # Apply exponential backoff before retrying
            retry_attempt = max([session.retry_counts.get(i, 0) for i in batch_indices])
            if retry_attempt > 0 and retry_attempt < max_retries:
                backoff_time = (2 ** (retry_attempt - 1)) * 5  # 5s, 10s, 20s
                print(f"  ⏳ Retry {retry_attempt}/{max_retries} in {backoff_time}s...")
                time.sleep(backoff_time)
            
            session.save(session_file)
            continue
        
        # Handle partial batch success (new format)
        if isinstance(batch_results, dict) and "results" in batch_results:
            results_list = batch_results['results']
            item_errors = batch_results.get('item_errors', [])
            success_count = batch_results.get('success_count', 0)
            failed_count = batch_results.get('failed_count', 0)
            
            # Process successful items - preserve order by mapping batch index to actual index
            for i, result_dict in enumerate(results_list):
                if result_dict is not None and i < len(batch_indices):
                    idx = batch_indices[i]  # Map batch position to actual dataset position
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
                failure_log_file = 'output/llm_parser/failure_log.json'
                log_failure_details(failure_log_file, batch_indices, item_errors, waterbody_rows)
                
                # Track individual item failures - these will be retried at the end
                for error_info in item_errors:
                    batch_idx = error_info['batch_index']  # Index within the batch (0-9)
                    actual_idx = batch_indices[batch_idx]  # Actual index in full dataset
                    retry_count = session.retry_counts.get(actual_idx, 0)
                    session.retry_counts[actual_idx] = retry_count + 1
            
            # Show running summary
            total_success = len(session.processed_items)
            total_pending = session.total_items - total_success - len(session.failed_items)
            print(f"  ✓ Partial Success: {success_count}/{len(batch_indices)} items succeeded | Total: {total_success} OK, {total_pending} pending")
            if failed_count > 0:
                print(f"    {failed_count} items will be retried at end (logged to failure_log.json)")
        
        # Handle old format (full list of dicts) for backwards compatibility
        elif isinstance(batch_results, list):
            for i, result_dict in enumerate(batch_results):
                if i < len(batch_indices):
                    idx = batch_indices[i]  # Map batch position to actual dataset position
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
            print(f"  ✓ Success | Total: {success_count} OK, {fail_count + validation_fail_count} failed")
        else:
            print(f"  ✗ Unexpected result format: {type(batch_results)}")
        
        # Save session after each batch - results are in order by index
        session.save(session_file)
        
        # Small delay between batches to avoid rate limiting
        if batch_start + batch_size < len(items_to_process):
            time.sleep(1)
    
    # After main processing, retry failed items (those with retry_counts > 0 but not yet processed)
    failed_to_retry = [i for i in range(total_items) 
                       if i not in session.processed_items 
                       and session.retry_counts.get(i, 0) > 0
                       and session.retry_counts.get(i, 0) < 3]
    
    if failed_to_retry:
        print(f"\n{'─'*80}")
        print(f"RETRYING FAILED ITEMS ({len(failed_to_retry)} items)")
        print(f"{'─'*80}")
        
        retry_start_time = datetime.now()
        max_retries = 3
        
        for batch_start in range(0, len(failed_to_retry), batch_size):
            batch_indices = failed_to_retry[batch_start:batch_start + batch_size]
            batch_rows = [waterbody_rows[i] for i in batch_indices]
            
            batch_num = batch_start // batch_size + 1
            total_retry_batches = (len(failed_to_retry) + batch_size - 1) // batch_size
            
            retry_progress = (batch_start / len(failed_to_retry)) * 100
            
            print(f"\n[Retry Batch {batch_num}/{total_retry_batches}] Indices {batch_indices[0]}-{batch_indices[-1]} | "
                  f"Retry Progress: {retry_progress:.0f}%")
            print(f"  API Keys: {api_key_manager.get_status()}")
            
            # Parse batch
            batch_results = parser.parse_synopsis_batch(batch_rows)
            
            # Check for complete batch errors
            if isinstance(batch_results, dict) and "error" in batch_results and "results" not in batch_results:
                error_msg = batch_results['error']
                print(f"  ✗ Batch Failed: {error_msg}")
                
                if api_key_manager.all_keys_exhausted():
                    print(f"\n⚠  All API keys exhausted during retries!")
                    print(f"   Key status: {api_key_manager.get_status()}")
                    session.save(session_file)
                    break
                
                # Mark as permanently failed if max retries reached
                for idx in batch_indices:
                    if session.retry_counts[idx] >= max_retries:
                        if idx not in [f['index'] for f in session.failed_items]:
                            session.failed_items.append({
                                'index': idx,
                                'waterbody': waterbody_rows[idx].water,
                                'error': f"Retry failed: {error_msg}",
                                'retries': session.retry_counts[idx]
                            })
                session.save(session_file)
                continue
            
            # Handle partial batch success
            if isinstance(batch_results, dict) and "results" in batch_results:
                results_list = batch_results['results']
                item_errors = batch_results.get('item_errors', [])
                success_count = batch_results.get('success_count', 0)
                
                # Process successful items - preserve order by mapping batch index to actual index
                for i, result_dict in enumerate(results_list):
                    if result_dict is not None and i < len(batch_indices):
                        idx = batch_indices[i]  # Map batch position to actual dataset position
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
                    failure_log_file = 'output/llm_parser/failure_log.json'
                    log_failure_details(failure_log_file, batch_indices, item_errors, waterbody_rows)
                    
                    for error_info in item_errors:
                        batch_idx = error_info['batch_index']  # Index within the batch (0-9)
                        actual_idx = batch_indices[batch_idx]  # Actual index in full dataset
                        
                        if session.retry_counts[actual_idx] >= max_retries:
                            # Permanently failed
                            if actual_idx not in [f['index'] for f in session.failed_items]:
                                session.failed_items.append({
                                    'index': actual_idx,
                                    'waterbody': waterbody_rows[actual_idx].water,
                                    'error': error_info['error'],
                                    'retries': session.retry_counts[actual_idx],
                                    'error_type': error_info['error_type']
                                })
                
                print(f"  ✓ Retry Result: {success_count}/{len(batch_indices)} items succeeded")
            
            session.save(session_file)
            
            # Small delay between retry batches
            if batch_start + batch_size < len(failed_to_retry):
                time.sleep(1)
        
        retry_elapsed = (datetime.now() - retry_start_time).total_seconds()
        retry_succeeded = len([i for i in failed_to_retry if i in session.processed_items])
        print(f"\n{'─'*80}")
        print(f"Retry phase complete: {retry_succeeded}/{len(failed_to_retry)} items recovered ({int(retry_elapsed)}s)")
        print(f"{'─'*80}\n")
    
    # Check if all items were processed
    unprocessed_indices = [i for i in range(total_items) if i not in session.processed_items]
    
    # Report on failed items and validation failures
    if session.failed_items:
        print(f"\n⚠  {len(session.failed_items)} items permanently failed:")
        for failed in session.failed_items[:5]:
            error_preview = failed['error'][:80] + '...' if len(failed['error']) > 80 else failed['error']
            print(f"  [{failed['index']}] {failed['waterbody']}: {error_preview}")
        if len(session.failed_items) > 5:
            print(f"  ... and {len(session.failed_items) - 5} more")
    
    if session.validation_failures:
        print(f"\n⚠  {len(session.validation_failures)} validation failures (retry with --resume):")
        for failed in session.validation_failures[:5]:
            error_preview = failed['error'][:80] + '...' if len(failed['error']) > 80 else failed['error']
            print(f"  [{failed['index']}] {failed['waterbody']}: {error_preview}")
        if len(session.validation_failures) > 5:
            print(f"  ... and {len(session.validation_failures) - 5} more")
    
    # Compile final results - maintain order, include all items
    # Convert ParsedWaterbody instances to dicts for JSON output
    # For failed items, include error placeholder
    final_results_dicts = []
    for idx in range(total_items):
        if session.results[idx] is not None:
            # Convert class instance to dict
            final_results_dicts.append(session.results[idx].to_dict())
        else:
            # Item failed - create error placeholder to maintain order
            failed_info = next((f for f in session.failed_items if f['index'] == idx), None)
            if not failed_info:
                failed_info = next((f for f in session.validation_failures if f['index'] == idx), None)
            error_msg = failed_info['error'] if failed_info else 'Unknown error'
            final_results_dicts.append({
                'waterbody_name': waterbody_rows[idx].water,
                'error': f"FAILED_TO_PARSE: {error_msg}",
                'raw_text': waterbody_rows[idx].raw_regs,
                'cleaned_text': '',
                'geographic_groups': []
            })
    
    # Save final output as JSON
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w', encoding='utf-8') as f:
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
    
    # Clean up session file if fully successful
    if len(session.processed_items) == total_items and not session.failed_items and not session.validation_failures:
        if os.path.exists(session_file):
            os.remove(session_file)
            print(f"✓  Removed session file (all successful)")
    print(f"{'─'*80}")
    
    return session.results  # Return class instances, not dicts

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
        if session.results[idx] is not None:
            # Convert ParsedWaterbody instance to dict
            final_results_dicts.append(session.results[idx].to_dict())
        else:
            # Item not yet processed or failed
            if idx in session.processed_items:
                # This shouldn't happen but handle it
                print(f"  ⚠ Warning: Item {idx} marked as processed but result is None")
            
            # Check if it's a failed item
            failed_info = next((f for f in session.failed_items if f['index'] == idx), None)
            if failed_info:
                # Include error placeholder
                error_msg = failed_info.get('error', 'Unknown error')
                final_results_dicts.append({
                    'waterbody_name': session.input_rows[idx].water,
                    'error': f"FAILED_TO_PARSE: {error_msg}",
                    'raw_text': session.input_rows[idx].raw_regs,
                    'cleaned_text': '',
                    'geographic_groups': []
                })
            else:
                # Not processed yet - include placeholder
                final_results_dicts.append({
                    'waterbody_name': session.input_rows[idx].water,
                    'error': 'NOT_YET_PROCESSED',
                    'raw_text': session.input_rows[idx].raw_regs,
                    'cleaned_text': '',
                    'geographic_groups': []
                })
    
    # Save to output file
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(final_results_dicts, f, indent=2, ensure_ascii=False)
    
    print(f"\n✓ Exported {len(final_results_dicts)} items to: {output_file}")
    print(f"  Successfully parsed: {len([r for r in session.results if r is not None])}")
    print(f"  Failed: {len(session.failed_items)}")
    print(f"  Not yet processed: {session.total_items - len(session.processed_items) - len(session.failed_items)}")

def print_prompt(waterbody_rows: List):
    """Print the prompt that would be sent to the LLM."""
    parser = SynopsisParser()

    prompt = parser.get_prompt(waterbody_rows)
    print(prompt)
    
    # Save prompt to file
    prompt_file = 'output/llm_parser/prompt.txt'
    os.makedirs(os.path.dirname(prompt_file), exist_ok=True)
    with open(prompt_file, 'w', encoding='utf-8') as f:
        f.write(prompt)
    print(f"\n✓ Prompt saved to: {prompt_file}")

def load_waterbody_rows_from_file(file_path):
    """Load WaterbodyRow objects from a synopsis_raw_data.json file."""
    if not os.path.exists(file_path):
        print(f"Error: File not found: {file_path}")
        return None
    
    print(f"Loading waterbody rows from: {file_path}")
    with open(file_path, 'r', encoding='utf-8') as f:
        json_data = json.load(f)
    
    # Reconstruct ExtractionResults from JSON
    extraction_results = ExtractionResults.from_dict(json_data)
    
    # Extract all WaterbodyRow objects from all pages
    all_rows = []
    for page_result in extraction_results.pages:
        all_rows.extend(page_result.rows)
    
    print(f"Loaded {len(all_rows)} waterbody rows from {len(extraction_results.pages)} pages")
    return all_rows

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Parse fishing regulations using LLM with batch processing and validation',
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
        """
    )
    
    # Input/Output arguments
    io_group = parser.add_argument_group('Input/Output')
    io_group.add_argument('--file', type=str, metavar='PATH',
                         help='Path to synopsis_raw_data.json file to parse (not required if resuming)')
    io_group.add_argument('--output', default='output/llm_parser/llm_parsed_results.json', metavar='PATH',
                         help='Path to save parsed results (default: output/llm_parser/llm_parsed_results.json)')
    io_group.add_argument('--session-file', default='output/llm_parser/session.json', metavar='PATH',
                         help='Path to session file for resuming (default: output/llm_parser/session.json)')
    
    # Processing arguments
    proc_group = parser.add_argument_group('Processing')
    proc_group.add_argument('--batch-size', type=int, default=10, metavar='N',
                           help='Number of items per batch (default: 10, smaller = safer)')
    proc_group.add_argument('--resume', action='store_true',
                           help='Resume from previous progress file')
    
    # Action arguments (mutually exclusive)
    action_group = parser.add_argument_group('Actions')
    action_group.add_argument('--validate', type=str, metavar='PATH',
                             help='Validate a session or parsed results JSON file (auto-detects type)')
    action_group.add_argument('--prompt', action='store_true',
                             help='Print the LLM prompt without making API calls')
    action_group.add_argument('--export-session', action='store_true',
                             help='Export current session results to output JSON file')
    
    args = parser.parse_args()
    
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
        if result.get('file_type') == 'session' and 'session_info' in result:
            info = result['session_info']
            print(f"\nSession Info:")
            print(f"  Total items: {info['total_items']}")
            print(f"  Processed: {info['processed']}")
            print(f"  Failed: {info['failed']}")
            print(f"  Created: {info['created_at']}")
            print(f"  Last updated: {info['last_updated']}")
        
        if result['errors']:
            print(f"\n  Errors ({len(result['errors'])}):")
            for err in result['errors'][:20]:
                print(f"    - {err}")
            if len(result['errors']) > 20:
                print(f"    ... and {len(result['errors']) - 20} more")
        
        if result['warnings']:
            print(f"\n  Warnings ({len(result['warnings'])}):")
            for warn in result['warnings'][:10]:
                print(f"    - {warn}")
            if len(result['warnings']) > 10:
                print(f"    ... and {len(result['warnings']) - 10} more")
        
        if result['valid']:
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
        resume=args.resume
    )