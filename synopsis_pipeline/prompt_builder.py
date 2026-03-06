"""
Prompt builder for LLM parsing of fishing regulations.

Loads prompt templates from text files and formats them with batch data.
"""

import json
import os
from pathlib import Path
from typing import List

from synopsis_pipeline.models import WaterbodyRow


def get_prompt_template_path() -> Path:
    """Get the path to the prompt template file."""
    return Path(__file__).parent / "prompts" / "parsing_prompt.txt"


def get_examples_path() -> Path:
    """Get the path to the examples file."""
    return Path(__file__).parent / "prompts" / "examples.json"


def format_examples_for_prompt(examples_json: List[dict]) -> str:
    """
    Convert examples from JSON format to text format for prompt insertion.

    Args:
        examples_json: List of example dictionaries with 'input' and 'output' keys

    Returns:
        Formatted text string ready for prompt insertion
    """
    formatted_examples = []

    for i, example in enumerate(examples_json, 1):
        # Format input section
        input_json = json.dumps(example["input"], indent=4, ensure_ascii=False)

        # Format output section
        output_json = json.dumps(example["output"], indent=4, ensure_ascii=False)

        # Build the example text
        example_text = f"INPUT:\nJSON\n{input_json}\nOUTPUT:\nJSON\n{output_json}"

        formatted_examples.append(example_text)

    # Join examples with separator
    return "\n\n---\n\n".join(formatted_examples)


def build_prompt(waterbody_rows: List[WaterbodyRow]) -> str:
    """
    Build the complete prompt for LLM parsing.

    Args:
        waterbody_rows: List of WaterbodyRow objects with water and raw_regs attributes

    Returns:
        Complete formatted prompt string
    """
    # Format inputs from WaterbodyRow objects with only the three required keys
    batch_inputs = [
        {"water": row.water, "raw_regs": row.raw_regs, "symbols": row.symbols}
        for row in waterbody_rows
    ]

    # Load prompt template
    template_path = get_prompt_template_path()
    with open(template_path, "r", encoding="utf-8") as f:
        template = f.read()

    # Load examples from JSON and format them
    examples_path = get_examples_path()
    with open(examples_path, "r", encoding="utf-8") as f:
        examples_json = json.load(f)

    examples_text = format_examples_for_prompt(examples_json)

    # Format template with batch data
    prompt = template.format(
        num_items=len(waterbody_rows),
        batch_inputs=json.dumps(batch_inputs, ensure_ascii=False),
        examples=examples_text,
    )

    return prompt
