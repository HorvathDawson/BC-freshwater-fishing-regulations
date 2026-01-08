import json
import os
import argparse
import time
from google import genai
from google.genai import types
from extract_synopsis import WaterbodyRow, ExtractionResults
from attrs import define
from typing import List, Dict, Any, Optional

# Configuration
API_KEY = os.environ.get("GOOGLE_API_KEY", "AIzaSyBPZigLsxFIU7JOFSux8ZqS03p9-E878VE")
client = genai.Client(api_key=API_KEY)

@define(frozen=True)
class TestRow:
    """Simplified row for testing with only water name and raw regulations."""
    water: str
    raw_regs: str

@define(frozen=True)
class ParsedRule:
    """A single parsed fishing regulation rule."""
    verbatim_text: str
    rule: str
    type: str
    dates: Optional[List[str]]
    species: Optional[List[str]]
    
    def validate(self, parent_text: str) -> List[str]:
        """Validate this rule. Returns list of error messages."""
        errors = []
        
        # Check required fields are non-empty
        if not self.verbatim_text or not self.verbatim_text.strip():
            errors.append("verbatim_text is empty")
        if not self.rule or not self.rule.strip():
            errors.append("rule is empty")
        
        # Validate rule type
        valid_types = {'closure', 'harvest', 'gear_restriction', 'restriction', 'licensing', 'access', 'note'}
        if self.type not in valid_types:
            errors.append(f"Invalid rule type '{self.type}', must be one of {valid_types}")
        
        # Validate dates/species are list or None
        if self.dates is not None and not isinstance(self.dates, list):
            errors.append(f"dates must be list or None, got {type(self.dates).__name__}")
        if self.species is not None and not isinstance(self.species, list):
            errors.append(f"species must be list or None, got {type(self.species).__name__}")
        
        # Validate dates appear in source text
        if self.dates and isinstance(self.dates, list):
            for date in self.dates:
                # Normalize for comparison (remove spaces, case insensitive)
                date_normalized = date.replace(' ', '').replace('-', '').lower()
                parent_normalized = parent_text.replace(' ', '').replace('\n', '').replace('-', '').lower()
                
                # Check if date appears in parent text (with some flexibility)
                if date_normalized not in parent_normalized:
                    # Try checking verbatim_text instead
                    verbatim_normalized = self.verbatim_text.replace(' ', '').replace('\n', '').replace('-', '').lower()
                    if date_normalized not in verbatim_normalized:
                        errors.append(f"Date '{date}' not found in source text")
        
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

@define(frozen=True)
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

@define(frozen=True)
class ParsedWaterbody:
    """Complete parsed result for a single waterbody."""
    waterbody_name: str
    raw_text: str
    cleaned_text: str
    geographic_groups: List[ParsedGeographicGroup]
    
    def validate(self, expected_name: Optional[str] = None) -> List[str]:
        """Validate this waterbody result. Returns list of error messages."""
        errors = []
        
        # Check required fields
        if not self.waterbody_name or not self.waterbody_name.strip():
            errors.append("waterbody_name is empty")
        
        # Check name matches expected
        if expected_name and self.waterbody_name.strip() != expected_name.strip():
            errors.append(f"Name mismatch: expected '{expected_name}', got '{self.waterbody_name}'")
        
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

@define
class BatchProgress:
    """Track progress of batch processing."""
    total_items: int
    processed_items: List[int]  # Indices of successfully processed items
    failed_items: List[Dict[str, Any]]  # Items that failed validation with error info
    results: List[Optional[Dict[str, Any]]]  # Parsed results indexed by original position
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'total_items': self.total_items,
            'processed_items': self.processed_items,
            'failed_items': self.failed_items,
            'results': self.results
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BatchProgress':
        return cls(
            total_items=data['total_items'],
            processed_items=data['processed_items'],
            failed_items=data['failed_items'],
            results=data['results']
        )
    
    def save(self, filepath: str):
        """Save progress to file."""
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(self.to_dict(), f, indent=2, ensure_ascii=False)
    
    @classmethod
    def load(cls, filepath: str) -> Optional['BatchProgress']:
        """Load progress from file."""
        if not os.path.exists(filepath):
            return None
        with open(filepath, 'r', encoding='utf-8') as f:
            return cls.from_dict(json.load(f))

class ValidationError(Exception):
    """Custom exception for validation failures."""
    pass

def validate_partial_json(json_path: str, input_rows: Optional[List] = None) -> Dict[str, Any]:
    """Validate a partial or complete LLM output JSON file.
    
    Args:
        json_path: Path to JSON file to validate
        input_rows: Optional list of input rows for name validation
    
    Returns:
        Dict with 'valid', 'errors', and 'warnings' keys
    """
    if not os.path.exists(json_path):
        return {'valid': False, 'errors': [f"File not found: {json_path}"], 'warnings': []}
    
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        return {'valid': False, 'errors': [f"Invalid JSON: {e}"], 'warnings': []}
    
    if not isinstance(data, list):
        return {'valid': False, 'errors': ["JSON must be a list of waterbody results"], 'warnings': []}
    
    all_errors = []
    all_warnings = []
    
    for idx, item in enumerate(data):
        try:
            # Convert to dataclass and validate
            parsed = ParsedWaterbody.from_dict(item)
            expected_name = input_rows[idx].water if (input_rows and idx < len(input_rows)) else None
            errors = parsed.validate(expected_name)
            
            if errors:
                all_errors.extend([f"Item {idx} ({parsed.waterbody_name}): {err}" for err in errors])
        except Exception as e:
            all_errors.append(f"Item {idx}: Failed to parse - {e}")
    
    return {
        'valid': len(all_errors) == 0,
        'errors': all_errors,
        'warnings': all_warnings,
        'items_checked': len(data)
    }

class FishingSynopsisParser:
    class LLMBatchParser:
        @staticmethod
        def get_prompt(waterbody_rows: List):
            """
            Enforces a hierarchical subject-predicate relationship while preserving
            full block context and individual rules.
            
            Args:
                waterbody_rows: List of WaterbodyRow or TestRow objects with water and raw_regs attributes
            """
            # Format inputs from WaterbodyRow objects
            batch_inputs = [f"Waterbody Name: {row.water} | Regulation Block: {row.raw_regs}" for row in waterbody_rows]
            
            return f"""
            You are a legal data architect. Parse this list of fishing regulation blocks into a JSON array. 
            All information must be preserved. All verbatim text must keep original punctuation and line breaks.
            Each object in the array corresponds to a waterbody with its regulations. 
            Rules must exist within the regulation block they are extracted from.
            
            CRITICAL: Return EXACTLY {len(waterbody_rows)} items in the same order as input. Process ALL items completely.
            
            DIRECTIONS:
            1. HIERARCHY: Map the input text into 'geographic_groups'.
            2. CONTEXT: For each group, provide 'raw_text' (verbatim from input) and 'cleaned_text' (fixed word-breaks, collapsed hyphens, single line).
            3. RULES: Split the 'cleaned_text' of that group into individual rule objects in the 'rules' array.
            4. LISTS: Split nested lists (a, b, c) into individual rule objects.
            5. VERBATIM: Do not summarize. Every word of the original text must exist within the 'geographic_groups'.
            6. TYPES: Classify each rule into one of: closure, harvest, gear_restriction, restriction, licensing, access, note.
            7. DATES & SPECIES: Extract date ranges and species into arrays, or null if none found.
            8. FORMATTING: Ensure valid JSON output.
            9. RULES EXTRACTION: Extract all rules, even if they overlap in meaning. One rule per object. Multiple rules can exist in one block of text.
            10. MAKE SURE ALL ENTRIES ARE FILLED OUT AS PER THE SCHEMA BELOW. DO NOT LEAVE ANYTHING BLANK. DO NOT SKIP ANY RULES OR WATERBODIES.
            11. ALL ITEMS SHOULD BE PROCESSED IN THE ORDER THEY APPEAR IN THE INPUT.
            
            JSON SCHEMA:
            List of objects:
            {{
                "waterbody_name": "Name from input",
                "raw_text": "The full verbatim regulation block. Does not include name only text.",
                "cleaned_text": "The block of text with repaired word-breaks and newlines. Mains full context. Has fixed Punctuation.",
                "geographic_groups": [
                    {{
                        "location": "Location anchor (if any), blank assumes the whole waterbody. E.g., 'upstream of the dam', 'tributaries', 'from the bridge to the lake'",
                        "raw_text": "The verbatim block of text for the context of this location (including newlines/hyphens)",
                        "cleaned_text": "The block of text with repaired word-breaks, no newlines, and corrected punctuation. Maintains full context.",
                        "rules": [
                            {{
                                "verbatim_text": "The complete context for the specific legal instruction",
                                "rule": "Specific rule extracted, normalized. E.g., 'No Fishing', 'Trout catch and release'. One rule per object. Multiple rules can exist in one block of text.",
                                "type": "closure|harvest|gear_restriction|restriction|licensing|access|note",
                                "dates": ["Date ranges found, or null"],
                                "species": ["Fish types found, or null"]
                            }}
                        ]
                    }}
                ]
            }}
            
            A few examples:
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
        def parse_synopsis_batch(cls, waterbody_rows: List, retry_count: int = 0, max_retries: int = 3):
            """
            Parse a list of WaterbodyRow or TestRow objects with retry logic.
            
            Args:
                waterbody_rows: List of objects with water and raw_regs attributes
                retry_count: Current retry attempt
                max_retries: Maximum number of retries for rate limiting
            """
            try:
                prompt = cls.get_prompt(waterbody_rows)
                
                response = client.models.generate_content(
                    model='gemini-2.5-flash-lite', # Updated to the latest stable flash
                    # model='gemini-2.0-flash',
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type='application/json',
                        temperature=0.1
                    )
                )
                
                if response.text:
                    parsed_result = json.loads(response.text)
                    
                    # Validate the result using dataclasses
                    validation_errors = []
                    
                    # Check basic structure
                    if not isinstance(parsed_result, list):
                        validation_errors.append(f"Result is not a list, got {type(parsed_result).__name__}")
                    elif len(parsed_result) != len(waterbody_rows):
                        validation_errors.append(f"Expected {len(waterbody_rows)} items, got {len(parsed_result)}")
                    else:
                        # Validate each item using dataclass
                        for idx, entry in enumerate(parsed_result):
                            try:
                                parsed = ParsedWaterbody.from_dict(entry)
                                expected_name = waterbody_rows[idx].water if idx < len(waterbody_rows) else None
                                item_errors = parsed.validate(expected_name)
                                validation_errors.extend([f"Item {idx}: {err}" for err in item_errors])
                            except Exception as e:
                                validation_errors.append(f"Item {idx}: Failed to parse - {e}")
                    
                    if validation_errors:
                        raise ValidationError(f"Validation failed: {'; '.join(validation_errors[:5])}...")  # Show first 5
                    
                    return parsed_result
                else:
                    return {"error": "Empty response from model"}
            except Exception as e:
                error_msg = str(e)
                
                # Check for rate limiting
                if 'rate limit' in error_msg.lower() or '429' in error_msg:
                    if retry_count < max_retries:
                        wait_time = (2 ** retry_count) * 5  # Exponential backoff: 5s, 10s, 20s
                        print(f"⚠ Rate limited. Waiting {wait_time}s before retry {retry_count + 1}/{max_retries}...")
                        time.sleep(wait_time)
                        return cls.parse_synopsis_batch(waterbody_rows, retry_count + 1, max_retries)
                    else:
                        return {"error": f"Rate limit exceeded after {max_retries} retries"}
                
                return {"error": error_msg}


# --- BATCH DEBUG RUNNER ---

def run_llm_parsing(waterbody_rows: List, output_file='output/llm_parser/llm_parsed_results.json', 
                    batch_size=10, progress_file='output/llm_parser/progress.json', resume=False):
    """
    Run LLM parsing with batching support and progress tracking.
    
    Args:
        waterbody_rows: List of waterbody objects to parse (required)
        output_file: Final output file path
        batch_size: Number of items to process per batch (smaller = more consistent, less rate limiting)
        progress_file: Path to save/load progress
        resume: Whether to resume from previous progress
    """
    parser = FishingSynopsisParser.LLMBatchParser()
    print(f"\n{'='*80}\nRunning LLM Batch Parsing...\n{'='*80}")
    
    total_items = len(waterbody_rows)
    print(f"Total items to process: {total_items}")
    print(f"Batch size: {batch_size}")
    
    # Load or create progress
    progress = None
    
    # Check if progress file exists
    existing_progress = BatchProgress.load(progress_file)
    
    if existing_progress and len(existing_progress.processed_items) > 0:
        # Progress file exists with completed items
        if resume:
            # User explicitly requested resume
            progress = existing_progress
            print(f"✓ Resumed from progress file: {len(progress.processed_items)}/{total_items} items completed")
        else:
            # Ask user if they want to resume
            print(f"\n⚠ Found existing progress: {len(existing_progress.processed_items)}/{existing_progress.total_items} items completed")
            print(f"   Progress file: {progress_file}")
            
            response = input("\nDo you want to resume from this progress? [Y/n]: ").strip().lower()
            
            if response in ('', 'y', 'yes'):
                progress = existing_progress
                print(f"✓ Resuming from existing progress...")
            else:
                print(f"✓ Starting fresh (old progress will be overwritten)")
                # Delete old progress file
                if os.path.exists(progress_file):
                    os.remove(progress_file)
    elif resume:
        print("⚠ --resume flag provided but no progress file found, starting from beginning")
    
    if progress is None:
        progress = BatchProgress(
            total_items=total_items,
            processed_items=[],
            failed_items=[],
            results=[None] * total_items
        )
    
    # Determine which items need processing
    items_to_process = [i for i in range(total_items) if i not in progress.processed_items]
    
    if not items_to_process:
        print("✓ All items already processed!")
        # Compile final results
        final_results = [r for r in progress.results if r is not None]
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(final_results, f, indent=2, ensure_ascii=False)
        print(f"✓ Saved final results to: {output_file}")
        return final_results
    
    print(f"Items remaining to process: {len(items_to_process)}")
    
    # Process in batches
    for batch_start in range(0, len(items_to_process), batch_size):
        batch_indices = items_to_process[batch_start:batch_start + batch_size]
        batch_rows = [waterbody_rows[i] for i in batch_indices]
        
        print(f"\n--- Batch {batch_start // batch_size + 1}/{(len(items_to_process) + batch_size - 1) // batch_size} ---")
        print(f"Processing indices: {batch_indices[0]}-{batch_indices[-1]} ({len(batch_indices)} items)")
        
        # Parse batch
        batch_results = parser.parse_synopsis_batch(batch_rows)
        
        # Check for errors
        if isinstance(batch_results, dict) and "error" in batch_results:
            error_msg = batch_results['error']
            print(f"✗ Batch failed: {error_msg}")
            
            # Save failed items for manual review
            for idx in batch_indices:
                progress.failed_items.append({
                    'index': idx,
                    'waterbody': waterbody_rows[idx].water,
                    'error': error_msg
                })
            
            # Save progress and stop if it's a rate limit error
            progress.save(progress_file)
            if 'rate limit' in error_msg.lower() or '429' in error_msg:
                print(f"\n⚠ Rate limited. Progress saved to: {progress_file}")
                print(f"Run with --resume to continue from where you left off")
                return None
            continue
        
        # Store results
        if isinstance(batch_results, list):
            for i, result in enumerate(batch_results):
                if i < len(batch_indices):
                    idx = batch_indices[i]
                    progress.results[idx] = result
                    progress.processed_items.append(idx)
            
            print(f"✓ Batch completed: {len(batch_results)} items parsed")
        else:
            print(f"✗ Unexpected result format: {type(batch_results)}")
        
        # Save progress after each batch
        progress.save(progress_file)
        print(f"✓ Progress saved ({len(progress.processed_items)}/{total_items} completed)")
        
        # Small delay between batches to avoid rate limiting
        if batch_start + batch_size < len(items_to_process):
            time.sleep(1)
    
    # Compile final results
    final_results = [r for r in progress.results if r is not None]
    
    # Report on any failed items
    if progress.failed_items:
        print(f"\n⚠ {len(progress.failed_items)} items failed validation:")
        for failed in progress.failed_items:
            print(f"  - Index {failed['index']}: {failed['waterbody']} - {failed['error']}")
    
    # Save final output
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(final_results, f, indent=2, ensure_ascii=False)
    
    print(f"\n✓ Completed! Saved {len(final_results)} results to: {output_file}")
    
    # Clean up progress file if fully successful
    if len(progress.processed_items) == total_items and not progress.failed_items:
        if os.path.exists(progress_file):
            os.remove(progress_file)
            print(f"✓ Removed progress file (all items successful)")
    
    return final_results

def print_prompt(waterbody_rows: List):
    """Print the prompt that would be sent to the LLM."""
    parser = FishingSynopsisParser.LLMBatchParser()

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
Examples:
  # Parse regulations from file
  python debug_parser.py --file synopsis_raw_data.json
  
  # Resume interrupted processing
  python debug_parser.py --file synopsis_raw_data.json --resume
  
  # Validate existing results
  python debug_parser.py --validate llm_parsed_results.json
  
  # View prompt without processing
  python debug_parser.py --file synopsis_raw_data.json --prompt
        """
    )
    
    # Input/Output arguments
    io_group = parser.add_argument_group('Input/Output')
    io_group.add_argument('--file', type=str, metavar='PATH',
                         help='Path to synopsis_raw_data.json file to parse (required for parsing)')
    io_group.add_argument('--output', default='output/llm_parser/llm_parsed_results.json', metavar='PATH',
                         help='Path to save parsed results (default: output/llm_parser/llm_parsed_results.json)')
    io_group.add_argument('--progress-file', default='output/llm_parser/progress.json', metavar='PATH',
                         help='Path to progress file for resuming (default: output/llm_parser/progress.json)')
    
    # Processing arguments
    proc_group = parser.add_argument_group('Processing')
    proc_group.add_argument('--batch-size', type=int, default=10, metavar='N',
                           help='Number of items per batch (default: 10, smaller = safer)')
    proc_group.add_argument('--resume', action='store_true',
                           help='Resume from previous progress file')
    
    # Action arguments (mutually exclusive)
    action_group = parser.add_argument_group('Actions')
    action_group.add_argument('--validate', type=str, metavar='PATH',
                             help='Validate an existing parsed JSON file')
    action_group.add_argument('--prompt', action='store_true',
                             help='Print the LLM prompt without making API calls')
    
    args = parser.parse_args()
    
    # Handle validation mode
    if args.validate:
        print(f"\n{'='*80}\nValidating JSON file...\n{'='*80}")
        
        # Load input rows if --file provided for name matching
        input_rows = None
        if args.file:
            input_rows = load_waterbody_rows_from_file(args.file)
        
        result = validate_partial_json(args.validate, input_rows)
        
        print(f"\nValidation Results:")
        print(f"  Items checked: {result.get('items_checked', 0)}")
        print(f"  Valid: {result['valid']}")
        
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
        
        if result['valid']:
            print("\n✓ All checks passed!")
        else:
            print("\n✗ Validation failed")
            exit(1)
        exit(0)
    
    # For all other modes, require --file
    if not args.file:
        parser.error("--file is required (unless using --validate)")
    
    # Load waterbody rows
    waterbody_rows = load_waterbody_rows_from_file(args.file)
    if waterbody_rows is None:
        exit(1)
    
    # Handle prompt mode
    if args.prompt:
        print_prompt(waterbody_rows)
        exit(0)
    
    # Run LLM parsing
    run_llm_parsing(
        waterbody_rows=waterbody_rows,
        output_file=args.output,
        batch_size=args.batch_size,
        progress_file=args.progress_file,
        resume=args.resume
    )