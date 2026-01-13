# BC Freshwater Fishing Regulations Processing Pipeline

Automated pipeline for extracting, parsing, and processing BC freshwater fishing regulations from PDF synopsis into structured data.

## Quick Start

```bash
# Run complete pipeline
python pipeline.py all --pdf ../data/synopsis_2024.pdf

# Or run individual steps
python pipeline.py extract --pdf ../data/synopsis_2024.pdf
python pipeline.py parse --input output/extract_synopsis/synopsis_raw_data.json
python pipeline.py process-fwa --build-index  # or --build-index-only to skip processing
```

## Configuration

All defaults are configured in `config.yaml`:

```yaml
directories:
  data: ../data
  output: output
  extraction_output: output/extract_synopsis
  parsing_output: output/llm_parser
  fwa_output: output/FWA_zone_processing

extraction:
  pdf_path: ../data/synopsis.pdf  # Default PDF path
  image_dpi: 300

parsing:
  batch_size: 45  # Waterbodies per LLM batch
  model: "gemini-3-flash-preview"
  max_retry_per_item: 10
  max_validation_failures: 50

api_keys:
  - id: key1
    key: ${GEMINI_API_KEY_1}  # From environment variable
  # ... more keys
```

### Environment Variables

Set API keys as environment variables:

```bash
export GEMINI_API_KEY_1="your-key-here"
export GEMINI_API_KEY_2="your-key-here"
# ... etc
```

## Pipeline Steps

### 1. Extract (`extract_synopsis.py`)

Extracts waterbody regulation blocks from PDF synopsis.

**Input:** PDF file with regulation tables  
**Output:** JSON with waterbody names and raw regulation text

```bash
python pipeline.py extract \
    --pdf ../data/synopsis_2024.pdf \
    --output output/extract_synopsis/synopsis_raw_data.json
```

**Output format:**
```json
{
  "waterbodies": [
    {
      "water": "ALOUETTE LAKE",
      "raw_regs": "Bull trout (char) release\nNo vessels in swimming areas...",
      "page": 42
    }
  ]
}
```

### 2. Parse (`parse_synopsis.py`)

Parses regulations using LLM (Google Gemini) into structured format.

**Input:** Extracted waterbody data (JSON)  
**Output:** Structured regulations with geographic groups and individual rules

```bash
python pipeline.py parse \
    --input output/extract_synopsis/synopsis_raw_data.json \
    --output output/llm_parser \
    --batch-size 45 \
    --resume  # Resume from previous session if interrupted
```

**Features:**
- Resumable sessions (progress saved automatically)
- Multi-API-key rotation for rate limit handling
- Batch processing with validation
- Retry logic for failed items
- Detailed failure logging

**Output format:**
```json
[
  {
    "waterbody_name": "ALOUETTE LAKE",
    "raw_text": "Bull trout (char) release\n...",
    "cleaned_text": "Bull trout (char) release. No vessels...",
    "geographic_groups": [
      {
        "location": "",
        "raw_text": "Bull trout (char) release",
        "cleaned_text": "Bull trout (char) release.",
        "rules": [
          {
            "verbatim_text": "Bull trout (char) release",
            "rule": "Bull trout (char) release",
            "type": "harvest",
            "dates": null,
            "species": ["bull trout", "char"]
          }
        ]
      }
    ]
  }
]
```

### 3. Process FWA (`fwa_preprocessing.py`)

Processes BC Freshwater Atlas geographic data from the `data/` directory.

**Input:** FWA shapefiles and GDB files from `data/ftp.geobc.gov.bc.ca/`  
**Output:** Processed GDB file and waterbody index in `output/FWA_zone_processing/`

```bash
# Full processing with index building
python pipeline.py process-fwa --build-index

# Build index only from existing GDB (skip processing)
python pipeline.py process-fwa --build-index-only

# Or run directly
python fwa_preprocessing.py --build-index
python fwa_preprocessing.py --build-index-only
```

**Note:** This step uses fixed data paths configured in the script, not command-line input paths.

## Project Structure

```
scripts/
├── pipeline.py                 # Main entry point
├── extract_synopsis.py         # PDF extraction logic
├── parse_synopsis.py          # LLM parsing orchestration
├── fwa_preprocessing.py       # FWA geographic processing
├── waterbody_linking.py       # Waterbody name matching
├── config.yaml                # Configuration file
└── synopsis_pipeline/         # Shared module
    ├── __init__.py
    ├── models.py              # Data classes (WaterbodyRow, ParsedRule, etc.)
    ├── config.py              # Configuration loader
    ├── prompt_builder.py      # LLM prompt construction
    └── prompts/
        ├── parsing_prompt.txt # Main LLM prompt template
        └── examples.txt       # Example outputs for LLM
```

## Development

### Running Tests

```bash
# Run all tests
pytest tests/

# Run specific test file
pytest tests/test_models.py

# Run with coverage
pytest --cov=synopsis_pipeline tests/
```

### Validation

Validate existing results:

```bash
# Validate a session file
python parse_synopsis.py --validate output/llm_parser/session.json

# Validate parsed results
python parse_synopsis.py --validate output/llm_parser/llm_parsed_results.json
```

### Debugging

```bash
# Dry run (test without calling LLM)
python pipeline.py parse --dry-run

# View session status
python parse_synopsis.py --status output/llm_parser/session.json
```

## Data Models

### WaterbodyRow (Extraction Output)
- `water`: Waterbody name (str)
- `raw_regs`: Raw regulation text (str)
- `page`: PDF page number (int)

### ParsedWaterbody (Parsing Output)
- `waterbody_name`: Name (str)
- `raw_text`: Original text (str)
- `cleaned_text`: Cleaned text (str)
- `geographic_groups`: List[ParsedGeographicGroup]

### ParsedGeographicGroup
- `location`: Geographic area (str)
- `raw_text`: Original text for this location (str)
- `cleaned_text`: Cleaned text (str)
- `rules`: List[ParsedRule]

### ParsedRule
- `verbatim_text`: Exact text from source (str)
- `rule`: Normalized rule description (str)
- `type`: Rule category (str) - closure, harvest, gear_restriction, etc.
- `dates`: List of date ranges (List[str] or None)
- `species`: List of fish species (List[str] or None)

## Troubleshooting

### Rate Limits

If you hit API rate limits:
1. Add more API keys to `config.yaml`
2. Pipeline will automatically rotate through keys
3. Progress is saved - resume with `--resume`

### Memory Issues

If processing large PDFs:
- Reduce `batch_size` in config.yaml
- Process in chunks using `--max-items` flag

### Validation Failures

If items fail validation:
1. Check `output/llm_parser/failure_log_summary.txt`
2. Review failed items in session.json
3. Items automatically retry on `--resume`

## License

[Add your license here]

## Contributing

[Add contribution guidelines here]
