"""
Prompt builder for LLM parsing of fishing regulations.

Loads prompt templates from text files and formats them with batch data.
"""

import json
import os
from pathlib import Path
from typing import List


def get_prompt_template_path():
    """Get the path to the prompt template file."""
    return Path(__file__).parent / "prompts" / "parsing_prompt.txt"


def get_examples_path():
    """Get the path to the examples file."""
    return Path(__file__).parent / "prompts" / "examples.txt"


def build_prompt(waterbody_rows: List) -> str:
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

    # Load examples
    examples_path = get_examples_path()
    with open(examples_path, "r", encoding="utf-8") as f:
        examples = f.read()

    # Format template with batch data
    prompt = template.format(
        num_items=len(waterbody_rows),
        batch_inputs=json.dumps(batch_inputs, ensure_ascii=False),
        examples=examples,
    )

    return prompt
