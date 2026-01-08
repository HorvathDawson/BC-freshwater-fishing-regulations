import json
import textwrap
import os
import re
import argparse
from google import genai
from google.genai import types
from test_extract_synopsis import TEST_CASES
from extract_synopsis import WaterbodyRow, ExtractionResults
from attrs import define
from typing import List

# Configuration
API_KEY = os.environ.get("GOOGLE_API_KEY", "AIzaSyBPZigLsxFIU7JOFSux8ZqS03p9-E878VE")
client = genai.Client(api_key=API_KEY)

@define(frozen=True)
class TestRow:
    """Simplified row for testing with only water name and raw regulations."""
    water: str
    raw_regs: str

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
            All information must be preserved. All verbatim text mujst keep original punctuation and line breaks.
            Each object in the array corresponds to a waterbody with its regulations. 
            Rules must exist within the regulation block they are extracted from.
            
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
        def parse_synopsis_batch(cls, waterbody_rows: List):
            """
            Parse a list of WaterbodyRow or TestRow objects.
            
            Args:
                waterbody_rows: List of objects with water and raw_regs attributes
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
                    return json.loads(response.text)
                else:
                    return {"error": "Empty response from model"}
            except Exception as e:
                return {"error": str(e)}

# --- BATCH DEBUG RUNNER ---

def run_llm_parsing(waterbody_rows=None, output_file='output/llm_parser/llm_parsed_results.json'):
    parser = FishingSynopsisParser.LLMBatchParser()
    print(f"\n{'='*80}\nRunning LLM Batch Parsing...\n{'='*80}")
    
    # Use provided waterbody_rows or convert TEST_CASES tuples to TestRow objects
    if waterbody_rows is None:
        waterbody_rows = [TestRow(water=name, raw_regs=raw_text) for name, raw_text, _ in TEST_CASES]
    
    print(f"Processing {len(waterbody_rows)} waterbody rows...")
    
    llm_results = parser.parse_synopsis_batch(waterbody_rows)

    if isinstance(llm_results, dict) and "error" in llm_results:
        print(f"FAILED TO CONNECT OR PARSE: {llm_results['error']}")
        return None
    
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(llm_results, f, indent=2, ensure_ascii=False)
    
    print(f"✓ Saved LLM results to: {output_file}")
    return llm_results

def run_debug_visualization(llm_results_file='output/llm_parser/llm_parsed_results.json'):
    col_width = 62
    if not os.path.exists(llm_results_file):
        print(f"Error: Results file not found: {llm_results_file}")
        return
    
    with open(llm_results_file, 'r', encoding='utf-8') as f:
        llm_results = json.load(f)
    
    print(f"\n{'='*130}\n{'HIERARCHICAL LLM BATCH EXTRACTION vs. MANUAL REFERENCE':^130}\n{'='*130}")

    for idx, (name, raw_text, manual_expected) in enumerate(TEST_CASES):
        extract = llm_results[idx] if idx < len(llm_results) else {}
        print(f"\nWATER: {name}")
        print(f"{'Manual Reference':<{col_width}} | {'Hierarchical LLM Extract':<{col_width}}")
        print(f"{'-'*col_width}-|-{'-'*col_width}")
        
        llm_lines = []
        for group in extract.get('geographic_groups', []):
            llm_lines.append(f"CONTEXT: {group.get('context_header', 'General')}")
            # Highlight cleaned block context
            llm_lines.append(f"  BLOCK: {group.get('cleaned_text', '')[:100]}...")
            for rule in group.get('rules', []):
                meta = f"[{rule.get('type', 'other').upper()}]"
                llm_lines.append(f"    • {rule.get('verbatim_text', '')} {meta}")

        max_rows = max(len(manual_expected), len(llm_lines))
        for i in range(max_rows):
            m_raw = f"• {manual_expected[i]}" if i < len(manual_expected) else ""
            l_raw = llm_lines[i] if i < len(llm_lines) else ""
            
            m_wrapped = textwrap.wrap(m_raw, width=col_width-2)
            l_wrapped = textwrap.wrap(l_raw, width=col_width-2)
            
            for j in range(max(len(m_wrapped), len(l_wrapped))):
                ml = m_wrapped[j] if j < len(m_wrapped) else ""
                ll = l_wrapped[j] if j < len(l_wrapped) else ""
                print(f"{ml:<{col_width}} | {ll:<{col_width}}")

def print_prompt(waterbody_rows=None):
    """Print the prompt that would be sent to the LLM."""
    parser = FishingSynopsisParser.LLMBatchParser()
    
    # Use provided waterbody_rows or convert TEST_CASES tuples to TestRow objects
    if waterbody_rows is None:
        waterbody_rows = [TestRow(water=name, raw_regs=raw_text) for name, raw_text, _ in TEST_CASES]
    
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
    parser = argparse.ArgumentParser(description='Parse fishing regulations using LLM')
    parser.add_argument('--reload', action='store_true', help='Force reload from LLM API')
    parser.add_argument('--prompt', action='store_true', help='Only print the prompt without making API calls')
    parser.add_argument('--output', default='output/llm_parser/llm_parsed_results.json', help='Path to save/load results')
    parser.add_argument('--file', type=str, help='Path to synopsis_raw_data.json file to parse')
    args = parser.parse_args()
    
    # Load waterbody rows from file if specified
    waterbody_rows = None
    if args.file:
        waterbody_rows = load_waterbody_rows_from_file(args.file)
        if waterbody_rows is None:
            exit(1)
    
    if args.prompt:
        print_prompt(waterbody_rows=waterbody_rows)
    elif waterbody_rows is not None:
        # File was loaded, run LLM parsing
        run_llm_parsing(waterbody_rows=waterbody_rows, output_file=args.output)
        print("\nResults saved. Visualization not available for file input.")
    elif args.reload or not os.path.exists(args.output):
        run_llm_parsing(output_file=args.output)
        run_debug_visualization(args.output)
    else:
        run_debug_visualization(args.output)