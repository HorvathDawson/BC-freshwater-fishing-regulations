"""
Test script to verify prompt_builder.py correctly builds the prompt.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from synopsis_pipeline.prompt_builder import build_prompt
from synopsis_pipeline.models import WaterbodyRow


def test_prompt_building():
    """Test that prompt builds correctly with example data."""

    # Create test waterbody row with all required fields
    test_row = WaterbodyRow(
        water="TEST LAKE",
        mu=["Region 3"],
        raw_regs="**No Fishing** Apr 1-June 30\nBait ban",
        symbols=["Incl. Tribs"],
        page=42,
        image="test_image.png",
    )

    # Build prompt
    try:
        prompt = build_prompt([test_row])

        print("✓ Prompt built successfully!")
        print(f"  Total length: {len(prompt)} characters")
        print()

        # Check key components
        checks = [
            (
                "Contains num_items=1",
                "num_items" not in prompt and "1" in prompt,
            ),  # Should be replaced
            ("Contains batch_inputs", '"water": "TEST LAKE"' in prompt),
            ("Contains examples", "ATNARKO/BELLA COOLA RIVERS" in prompt),
            ("Contains COQUIHALLA example", "COQUIHALLA RIVER" in prompt),
            ("Contains ALOUETTE example", "ALOUETTE LAKE" in prompt),
            ("No double braces", "{{" not in prompt),
            ("Has JSON schema section", "JSON SCHEMA:" in prompt),
            ("Has parsing steps", "PARSING STEPS:" in prompt),
        ]

        all_passed = True
        for check_name, condition in checks:
            status = "✓" if condition else "✗"
            print(f"{status} {check_name}")
            if not condition:
                all_passed = False

        print()

        # Show a sample of the examples section
        if "ATNARKO/BELLA COOLA RIVERS" in prompt:
            examples_start = prompt.find("EXAMPLES:")
            examples_section = prompt[examples_start : examples_start + 500]
            print("Sample of examples section:")
            print("-" * 80)
            print(examples_section)
            print("..." if len(prompt) > examples_start + 500 else "")
            print("-" * 80)

        if all_passed:
            print("\n✓ ALL CHECKS PASSED!")
            return True
        else:
            print("\n✗ SOME CHECKS FAILED")
            return False

    except Exception as e:
        print(f"✗ Error building prompt: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("=" * 80)
    print("TESTING PROMPT BUILDER")
    print("=" * 80)
    print()

    success = test_prompt_building()

    print()
    print("=" * 80)
    if success:
        print("PROMPT BUILDER TEST PASSED!")
    else:
        print("PROMPT BUILDER TEST FAILED!")
    print("=" * 80)

    sys.exit(0 if success else 1)
