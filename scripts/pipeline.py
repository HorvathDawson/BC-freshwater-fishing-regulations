#!/usr/bin/env python3
"""
BC Fishing Regulations Processing Pipeline

Unified entry point for running the complete processing pipeline:
1. Extract - Extract waterbody data from PDF synopsis
2. Parse - Parse regulations using LLM
3. Process-FWA - Link waterbodies to FWA geographic data
4. All - Run complete pipeline

Usage:
    python pipeline.py extract [--pdf PATH] [--output PATH]
    python pipeline.py parse [--input PATH] [--output PATH] [--resume]
    python pipeline.py process-fwa [--input PATH] [--output PATH]
    python pipeline.py all [--pdf PATH] [--output-dir PATH]
"""

import argparse
import sys
import os
from pathlib import Path

# Add scripts directory to path so we can import sibling modules
SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR))

from synopsis_pipeline.config import load_config


def run_extract(args):
    """Run PDF extraction step."""
    print("\n" + "=" * 80)
    print("STEP 1: PDF EXTRACTION")
    print("=" * 80 + "\n")

    config = load_config()

    # Import extract_synopsis module
    import extract_synopsis

    # Determine PDF path
    pdf_path = args.pdf or config.get("extraction", {}).get("pdf_path")
    if not pdf_path:
        print("ERROR: No PDF path specified. Use --pdf or set in config.yaml")
        return 1

    # Determine output path
    output_path = args.output or config.get("extraction", {}).get("output_file")
    if not output_path:
        output_path = os.path.join(
            config.get("directories", {}).get(
                "extraction_output", "output/extract_synopsis"
            ),
            "synopsis_raw_data.json",
        )

    print(f"PDF: {pdf_path}")
    print(f"Output: {output_path}\n")

    # Run extraction
    try:
        extract_synopsis.main([pdf_path, output_path])
        print(f"\n✓ Extraction complete: {output_path}")
        return 0
    except Exception as e:
        print(f"\n✗ Extraction failed: {e}")
        return 1


def run_parse(args):
    """Run LLM parsing step."""
    print("\n" + "=" * 80)
    print("STEP 2: LLM PARSING")
    print("=" * 80 + "\n")

    config = load_config()

    # Import parse_synopsis module
    import parse_synopsis

    # Determine input path
    input_path = args.input or config.get("parsing", {}).get("input_file")
    if not input_path:
        # Default to extraction output
        input_path = os.path.join(
            config.get("directories", {}).get(
                "extraction_output", "output/extract_synopsis"
            ),
            "synopsis_raw_data.json",
        )

    # Determine output path
    output_dir = args.output or config.get("directories", {}).get(
        "parsing_output", "output/llm_parser"
    )

    # If output is a directory, append the default filename
    if os.path.isdir(output_dir) or not output_dir.endswith(".json"):
        output_file = os.path.join(output_dir, "llm_parsed_results.json")
    else:
        output_file = output_dir

    print(f"Input: {input_path}")
    print(f"Output file: {output_file}\n")

    # Build command line args for parse_synopsis
    parse_args = ["--file", input_path, "--output", output_file]

    if args.resume:
        parse_args.append("--resume")
    if args.batch_size:
        parse_args.extend(["--batch-size", str(args.batch_size)])
    if args.max_retry:
        parse_args.extend(["--max-retry", str(args.max_retry)])
    if args.dry_run:
        parse_args.append("--dry-run")

    # Run parsing
    try:
        parse_synopsis.main(parse_args)
        print(f"\n✓ Parsing complete: {output_file}")
        return 0
    except Exception as e:
        print(f"\n✗ Parsing failed: {e}")
        return 1


def run_fwa(args):
    """Run FWA preprocessing step."""
    print("\n" + "=" * 80)
    print("STEP 3: FWA PROCESSING")
    print("=" * 80 + "\n")

    # Import fwa_preprocessing module
    import fwa_preprocessing

    print("Processing FWA data from fixed paths in data/ directory...\n")
    print("Note: FWA processing uses data paths configured in the script.")
    print("Output will be saved to: output/FWA_zone_processing/\n")

    # Build command line args for fwa_preprocessing
    fwa_args = []

    if hasattr(args, "build_index_only") and args.build_index_only:
        fwa_args.append("--build-index-only")
    elif hasattr(args, "build_index") and args.build_index:
        fwa_args.append("--build-index")

    if hasattr(args, "test_mode") and args.test_mode:
        fwa_args.append("--test-mode")

    # Run FWA processing
    try:
        fwa_preprocessing.main(fwa_args)
        print("\n✓ FWA processing complete")
        return 0
    except Exception as e:
        print(f"\n✗ FWA processing failed: {e}")
        return 1


def run_all(args):
    """Run complete pipeline."""
    print("\n" + "=" * 80)
    print("BC FISHING REGULATIONS PROCESSING PIPELINE")
    print("=" * 80 + "\n")

    # Step 1: Extract
    extract_args = argparse.Namespace(
        pdf=args.pdf, output=None  # Use defaults from config
    )
    if run_extract(extract_args) != 0:
        return 1

    # Step 2: Parse
    parse_args = argparse.Namespace(
        input=None,  # Use extraction output
        output=None,  # Use defaults from config
        resume=args.resume,
        batch_size=args.batch_size,
        max_retry=args.max_retry,
        dry_run=False,
    )
    if run_parse(parse_args) != 0:
        return 1

    # Step 3: FWA
    fwa_args = argparse.Namespace(
        input=None, output=None  # Use parsing output  # Use defaults from config
    )
    if run_fwa(fwa_args) != 0:
        return 1

    print("\n" + "=" * 80)
    print("✓ PIPELINE COMPLETE")
    print("=" * 80 + "\n")
    return 0


def main():
    """Main entry point with argument parsing."""
    parser = argparse.ArgumentParser(
        description="BC Fishing Regulations Processing Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    subparsers = parser.add_subparsers(dest="command", help="Pipeline step to run")

    # Extract command
    extract_parser = subparsers.add_parser(
        "extract", help="Extract waterbody data from PDF synopsis"
    )
    extract_parser.add_argument("--pdf", help="Path to PDF synopsis file")
    extract_parser.add_argument("--output", help="Output JSON file path")

    # Parse command
    parse_parser = subparsers.add_parser("parse", help="Parse regulations using LLM")
    parse_parser.add_argument("--input", help="Input JSON file from extraction step")
    parse_parser.add_argument("--output", help="Output directory for parsed results")
    parse_parser.add_argument(
        "--resume", action="store_true", help="Resume from previous session"
    )
    parse_parser.add_argument(
        "--batch-size", type=int, help="Number of waterbodies per batch"
    )
    parse_parser.add_argument("--max-retry", type=int, help="Maximum retries per item")
    parse_parser.add_argument(
        "--dry-run", action="store_true", help="Test without calling LLM"
    )

    # Process-FWA command
    fwa_parser = subparsers.add_parser(
        "process-fwa", help="Process FWA geographic data"
    )
    fwa_parser.add_argument(
        "--build-index",
        action="store_true",
        help="Build waterbody index after processing",
    )
    fwa_parser.add_argument(
        "--build-index-only",
        action="store_true",
        help="Only build index from existing GDB (skip processing)",
    )
    fwa_parser.add_argument(
        "--test-mode", action="store_true", help="Test mode: process only 5 layers"
    )

    # All command
    all_parser = subparsers.add_parser("all", help="Run complete pipeline")
    all_parser.add_argument("--pdf", help="Path to PDF synopsis file")
    all_parser.add_argument(
        "--resume", action="store_true", help="Resume parsing from previous session"
    )
    all_parser.add_argument(
        "--batch-size", type=int, help="Number of waterbodies per batch"
    )
    all_parser.add_argument("--max-retry", type=int, help="Maximum retries per item")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    # Route to appropriate handler
    if args.command == "extract":
        return run_extract(args)
    elif args.command == "parse":
        return run_parse(args)
    elif args.command == "process-fwa":
        return run_fwa(args)
    elif args.command == "all":
        return run_all(args)
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
