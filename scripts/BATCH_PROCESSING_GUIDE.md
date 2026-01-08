# LLM Batch Processing Guide

## Overview
The `debug_parser.py` script now supports batching with progress tracking and automatic sanity checks to ensure consistent parsing results even when rate-limited.

## Key Features

### 1. **Batching Support**
- Process large datasets in smaller, manageable batches
- Keeps context window small while maintaining consistency
- Default batch size: 10 items (configurable)

### 2. **Progress Tracking**
- Automatically saves progress after each batch
- Resume from where you left off if interrupted or rate-limited
- Progress saved to `output/llm_parser/progress.json`

### 3. **Sanity Checks**
- Validates all returned data against expected schema
- Checks for:
  - Correct number of items returned
  - All required fields present
  - Valid field names and types
  - Non-empty critical fields
  - Valid rule types (closure, harvest, gear_restriction, etc.)
  - Proper array/null formatting for dates and species

### 4. **Automatic Retry Logic**
- Exponential backoff for rate limiting (5s, 10s, 20s)
- Failed items tracked separately for review
- Validation failures trigger reprocessing

## Usage Examples

### Basic Usage (with batching)
```bash
python scripts/debug_parser.py --file scripts/output/extract_synopsis/synopsis_raw_data.json
```

### Custom Batch Size
```bash
# Process 5 items at a time (slower but more conservative)
python scripts/debug_parser.py --file scripts/output/extract_synopsis/synopsis_raw_data.json --batch-size 5

# Process 20 items at a time (faster but may hit rate limits)
python scripts/debug_parser.py --file scripts/output/extract_synopsis/synopsis_raw_data.json --batch-size 20
```

### Resume After Rate Limiting
```bash
# If the script stops due to rate limiting, simply run with --resume
python scripts/debug_parser.py --file scripts/output/extract_synopsis/synopsis_raw_data.json --resume
```

### Custom Output Locations
```bash
python scripts/debug_parser.py \
  --file scripts/output/extract_synopsis/synopsis_raw_data.json \
  --output output/custom_results.json \
  --progress-file output/custom_progress.json \
  --batch-size 10
```

### View Prompt Only (no API calls)
```bash
python scripts/debug_parser.py --prompt
```

## How It Works

### Batching Flow
1. Script loads all waterbody rows
2. Divides them into batches of specified size
3. Processes each batch sequentially
4. Saves progress after each batch
5. Continues until all items processed

### Consistency Mechanism
Each batch includes:
- **Full schema definition** - ensures LLM understands structure
- **Example outputs** - maintains parsing style consistency
- **Explicit item count** - validates correct number returned
- **Order preservation** - items returned in same order as input

### Sanity Check Process
For each batch result:
1. Validate JSON structure
2. Check item count matches input
3. Verify all required fields exist
4. Validate field types and values
5. Check waterbody names match input
6. Ensure rules arrays not empty

If validation fails:
- Item marked as failed
- Error details saved to progress file
- Can be manually reviewed in progress.json

### Rate Limiting Handling
When rate limited:
1. Script saves current progress
2. Prints resume instructions
3. Run with `--resume` flag to continue
4. Uses exponential backoff (5s → 10s → 20s)

## Progress File Structure
```json
{
  "total_items": 100,
  "processed_items": [0, 1, 2, 3, ...],  // Successfully processed indices
  "failed_items": [                       // Items that failed validation
    {
      "index": 45,
      "waterbody": "Example Lake",
      "error": "Validation failed: ..."
    }
  ],
  "results": [...]  // Parsed results indexed by position
}
```

## Best Practices

1. **Start Small**: Use smaller batch sizes (5-10) for initial runs
2. **Monitor Progress**: Check progress file if interrupted
3. **Review Failures**: Inspect `failed_items` in progress file
4. **Resume Don't Restart**: Always use `--resume` after interruption
5. **Clean Progress**: Delete progress file for fresh start

## Troubleshooting

### "Rate limited" Error
- Progress automatically saved
- Wait a few minutes
- Run with `--resume` flag

### Validation Errors
- Check `failed_items` in progress.json
- Review specific waterbody that failed
- May need manual correction of input data

### All Items Already Processed
- Delete progress file to reprocess
- Or use different output file path

## Command Line Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--file` | None | Path to synopsis_raw_data.json |
| `--batch-size` | 10 | Items per batch |
| `--progress-file` | output/llm_parser/progress.json | Progress tracking file |
| `--resume` | False | Resume from saved progress |
| `--output` | output/llm_parser/llm_parsed_results.json | Final output file |
| `--prompt` | False | Print prompt without API calls |
| `--reload` | False | Force reload (ignore existing results) |
