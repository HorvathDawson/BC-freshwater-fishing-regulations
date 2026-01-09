# Parse Synopsis Improvements

## Changes Made

### 1. **Individual Item Validation (Partial Batch Success)**
**Problem**: When ANY item in a batch failed validation, the ENTIRE batch was discarded and retried. This meant losing good work and repeatedly failing on the same problematic items.

**Solution**: 
- Modified `parse_synopsis_batch()` to validate each item individually
- Returns a new format with:
  - `results`: List of parsed items (None for failed items)
  - `item_errors`: Detailed error info for each failed item
  - `success_count` and `failed_count`
- Failed items are tracked separately and retried at the end

**Example**: If 9/10 items succeed, those 9 are saved immediately. Only the 1 failed item needs retry.

### 2. **Detailed Failure Logging**
**Problem**: Failures were only shown in console output, making it hard to analyze patterns and improve the prompt.

**Solution**:
- Added `log_failure_details()` function that logs to:
  - `output/llm_parser/failure_log.json` - Structured JSON with full details
  - `output/llm_parser/failure_log_summary.txt` - Human-readable summary
- Each failure includes:
  - Waterbody index and name
  - Input regulation text
  - Error type (name_mismatch, validation_error, parse_error)
  - Full validation error messages
  - Timestamp

### 3. **Retry-at-End Strategy**
**Problem**: Failed items were retried immediately, blocking progress on the rest of the dataset.

**Solution**:
- During main processing, failed items are tracked but not retried
- After processing all new items, a dedicated retry phase runs
- Failed items get up to 3 attempts total
- After max retries, items are marked as permanently failed

### 4. **Better Progress Tracking**
- Shows partial success: "✓ Partial Success: 9/10 items succeeded"
- Displays total progress: "Total: 450 OK, 945 pending"
- Separate retry phase with its own progress tracking

## Why PETE'S POND Keeps Failing

The LLM is "correcting" the name:
- **Input**: `"PETE'S POND" Unnamed lake at the head of San Juan River`
- **LLM Output**: `PETE'S POND` (dropped the "Unnamed lake..." part)

This fails validation because the output name must EXACTLY match the input name.

**Why this happens in both runs**: The problematic items (indices 20-29, 150-159, etc.) keep getting the same batch context, and the LLM makes the same mistake.

## How to Fix Recurring Failures

1. **Check the failure log**:
   ```bash
   cat output/llm_parser/failure_log_summary.txt
   ```

2. **Analyze patterns** in the JSON log:
   ```bash
   jq '.failures[] | {name: .waterbody_name, error: .error_type}' output/llm_parser/failure_log.json
   ```

3. **Improve the prompt** based on common error types:
   - Name mismatches → Add more emphasis on "EXACT VERBATIM" copying
   - Verbatim_text errors → Add more examples of correct vs incorrect extraction
   - Date parsing → Add more date format examples

4. **Resume processing**:
   ```bash
   python parse_synopsis.py --resume
   ```

## Benefits

1. **Efficiency**: 90% success rate means 90% of work is saved, not 0%
2. **Faster completion**: Don't waste time retrying good items with bad ones
3. **Better debugging**: Detailed logs help identify prompt improvement opportunities
4. **Resilience**: Partial API failures don't doom entire batches

## New Workflow

```
Main Processing
  ├─ Batch 1: 10 items → 9 succeed, 1 fails (tracked)
  ├─ Batch 2: 10 items → 8 succeed, 2 fail (tracked)
  └─ ... continue through all new items
  
Retry Phase
  ├─ Retry failed items from main phase
  ├─ Up to 3 attempts per item
  └─ Permanently fail after 3 attempts

Final Output
  ├─ Successful items → Full parsed data
  └─ Failed items → Error placeholder with details
```

## Failure Log Format

```json
{
  "timestamp": "2026-01-08T20:30:00",
  "batch_indices": [20, 21, 22, ...],
  "failures": [
    {
      "index": 29,
      "waterbody_name": "CAYCUSE RIVER",
      "raw_regs": "No Fishing...",
      "error_type": "name_mismatch",
      "error": "Name/order mismatch - expected 'CAYCUSE RIVER', got 'CAYUSE RIVER'",
      "all_validation_errors": [...]
    }
  ]
}
```
