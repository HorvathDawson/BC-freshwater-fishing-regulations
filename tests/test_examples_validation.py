"""
Validate that all examples in examples.json conform to the data models in models.py.

This script loads the examples and runs them through the Pydantic/attrs validation
to ensure they are structurally correct and match the schema.
"""

import json
import sys
from pathlib import Path

# Add parent directory to path to import synopsis_pipeline
sys.path.insert(0, str(Path(__file__).parent.parent))

from synopsis_pipeline.models import ParsedWaterbody, IdentityObject
import re


def test_waterbody_key_validation():
    """Test that waterbody_key validation allows flexibility while catching errors."""
    
    # Test cases: (name_verbatim, waterbody_key, should_pass)
    test_cases = [
        # Both with and without quotes should work
        ('"ANDERSON" LAKE', '"ANDERSON" LAKE', True),
        ('"ANDERSON" LAKE', 'ANDERSON LAKE', True),
        
        # Nested quotes - both versions valid
        ('"ERROCK" ("Squakum") LAKE', '"ERROCK" LAKE', True),
        ('"ERROCK" ("Squakum") LAKE', 'ERROCK LAKE', True),
        
        # UNNAMED with location
        ('UNNAMED LAKE ("Kinglet Lake") located 100 m west of Butterfly Lake', 'UNNAMED LAKE', True),
        
        # Tributaries
        ("WEST KETTLE RIVER'S tributaries", 'WEST KETTLE RIVER', True),
        
        # Parenthetical removal
        ('ARROW PARK (Mosquito) CREEK', 'ARROW PARK CREEK', True),
        ('MARBLE ("Link") RIVER (only between Hwy 19 and Alice Lake)', 'MARBLE RIVER', True),
        
        # Should fail - wrong waterbody name
        ('"ANDERSON" LAKE', '"JONES" LAKE', False),
        ('MARBLE RIVER', 'ARROW CREEK', False),
        ('FRASER RIVER', 'THOMPSON RIVER', False),
    ]
    
    print("\nTesting waterbody_key validation:")
    print("=" * 80)
    
    test_errors = []
    for name_verbatim, waterbody_key, should_pass in test_cases:
        # Create minimal waterbody for testing
        waterbody_dict = {
            "identity": {
                "name_verbatim": name_verbatim,
                "waterbody_key": waterbody_key,
                "identity_type": "STILL_WATER",
                "component_waterbodies": [],
                "alternate_names": [],
                "global_scope": {
                    "scope_type": "WHOLE_SYSTEM",
                    "includes_tributaries": False,
                }
            },
            "regs_verbatim": "Test regulation",
            "rules": []
        }
        
        wb = ParsedWaterbody.from_dict(waterbody_dict)
        validation_errors = wb.identity.validate()
        
        # Check only for waterbody_key related errors
        key_errors = [e for e in validation_errors if 'waterbody_key' in e]
        validation_passed = len(key_errors) == 0
        
        expected = "PASS" if should_pass else "FAIL"
        correct = (validation_passed == should_pass)
        status = "OK" if correct else "WRONG"
        
        print(f"{status} {expected}: '{name_verbatim[:45]}' -> '{waterbody_key}'")
        
        if not correct:
            test_errors.append(f"'{name_verbatim}' -> '{waterbody_key}' (expected {expected})")
            if validation_errors:
                print(f"   Validation errors: {validation_errors}")
    
    if test_errors:
        print(f"\n{len(test_errors)} waterbody_key validation test(s) failed")
        raise AssertionError(f"{len(test_errors)} tests failed")
    else:
        print(f"\nAll {len(test_cases)} waterbody_key validation tests passed!")


def test_alternate_names_validation():
    """Test that alternate_names validation checks words are in name_verbatim."""
    
    # Test cases: (name_verbatim, alternate_names, should_pass)
    test_cases = [
        # Valid alternate names - words found in name_verbatim
        ('"ERROCK" ("Squakum") LAKE', ['Squakum'], True),
        ('MARBLE ("Link") RIVER', ['Link'], True),
        ('ARROW PARK (Mosquito) CREEK', ['Mosquito'], True),
        ('MOOSE LAKE (formerly Alces Lake)', ['Alces'], True),
        
        # Multiple alternates
        ('"BIG QUALICUM" RIVER', ['Big Qualicum', 'Qualicum'], True),
        
        # Should fail - alternate names not in name_verbatim
        ('ANDERSON LAKE', ['Jones'], False),
        ('FRASER RIVER', ['Thompson'], False),
        ('MARBLE RIVER', ['Arrow'], False),
    ]
    
    print("\nTesting alternate_names validation:")
    print("=" * 80)
    
    test_errors = []
    for name_verbatim, alternate_names, should_pass in test_cases:
        waterbody_dict = {
            "identity": {
                "name_verbatim": name_verbatim,
                "waterbody_key": name_verbatim.split('(')[0].strip(),
                "identity_type": "STILL_WATER",
                "component_waterbodies": [],
                "alternate_names": alternate_names,
                "global_scope": {
                    "scope_type": "WHOLE_SYSTEM",
                    "includes_tributaries": False,
                }
            },
            "regs_verbatim": "Test regulation",
            "rules": []
        }
        
        wb = ParsedWaterbody.from_dict(waterbody_dict)
        validation_errors = wb.identity.validate()
        
        # Check if validation passed (no errors related to alternate_names)
        alternate_errors = [e for e in validation_errors if 'alternate_name' in e]
        validation_passed = len(alternate_errors) == 0
        
        expected = "PASS" if should_pass else "FAIL"
        correct = (validation_passed == should_pass)
        status = "OK" if correct else "WRONG"
        
        print(f"{status} {expected}: '{name_verbatim[:40]}' + {alternate_names}")
        
        if not correct:
            test_errors.append(f"'{name_verbatim}' + {alternate_names} (expected {expected})")
            if alternate_errors:
                print(f"   Errors: {alternate_errors}")
    
    if test_errors:
        print(f"\n{len(test_errors)} alternate_names validation test(s) failed")
        raise AssertionError(f"{len(test_errors)} tests failed")
    else:
        print(f"\nAll {len(test_cases)} alternate_names validation tests passed!")


def load_examples():
    """Load examples from examples.json."""
    examples_path = (
        Path(__file__).parent.parent / "synopsis_pipeline" / "prompts" / "examples.json"
    )
    with open(examples_path, "r", encoding="utf-8") as f:
        return json.load(f)


def test_example_validation():
    """Test that all examples validate successfully."""
    examples = load_examples()

    errors = []
    for i, example in enumerate(examples, 1):
        try:
            # Create ParsedWaterbody from the output
            waterbody = ParsedWaterbody.from_dict(example["output"])

            # Run validation
            waterbody.validate()

            print(
                f"✓ Example {i} ({example['input']['water'][:50]}...) validated successfully"
            )

        except Exception as e:
            error_msg = f"✗ Example {i} failed validation: {e}"
            print(error_msg)
            errors.append(error_msg)

    if errors:
        print(f"\n{len(errors)} examples failed validation:")
        for error in errors:
            print(f"  {error}")
        raise AssertionError(f"{len(errors)} examples failed validation")
    else:
        print(f"\n✓ All {len(examples)} examples validated successfully!")


def test_verbatim_chain_of_custody():
    """Test that all verbatim fields are exact substrings of their parent text."""
    examples = load_examples()

    errors = []
    for i, example in enumerate(examples, 1):
        output = example["output"]
        waterbody_name = example["input"]["water"][:50]

        # Check regs_verbatim matches input
        if output["regs_verbatim"] != example["input"]["raw_regs"]:
            errors.append(f"Example {i}: regs_verbatim doesn't match input raw_regs")

        # Check each rule's verbatim chain
        for j, rule in enumerate(output.get("rules", []), 1):
            rule_text = rule.get("rule_text_verbatim", "")

            # rule_text_verbatim must be substring of regs_verbatim
            if rule_text not in output["regs_verbatim"]:
                errors.append(
                    f"Example {i}, Rule {j}: rule_text_verbatim not found in regs_verbatim\n"
                    f"  Looking for: {rule_text[:100]}..."
                )

            # location_verbatim must be substring of rule_text_verbatim (if not null)
            scope = rule.get("scope", {})
            location = scope.get("location_verbatim")
            if location is not None and location not in rule_text:
                errors.append(
                    f"Example {i}, Rule {j}: location_verbatim not found in rule_text_verbatim\n"
                    f"  Location: {location}\n"
                    f"  Rule text: {rule_text[:100]}..."
                )

            # landmark_verbatim must be substring of location_verbatim (if both not null)
            landmark = scope.get("landmark_verbatim")
            if (
                landmark is not None
                and location is not None
                and landmark not in location
            ):
                errors.append(
                    f"Example {i}, Rule {j}: landmark_verbatim not found in location_verbatim\n"
                    f"  Landmark: {landmark}\n"
                    f"  Location: {location}"
                )

    if errors:
        print(f"\n{len(errors)} verbatim chain violations found:")
        for error in errors:
            print(f"  {error}")
        raise AssertionError(f"{len(errors)} verbatim chain violations")
    else:
        print(f"✓ All verbatim chains validated successfully!")


def test_atomic_architecture():
    """Verify examples follow atomic regulation unit architecture (1 rule = 1 restriction)."""
    examples = load_examples()

    errors = []
    for i, example in enumerate(examples, 1):
        output = example["output"]
        waterbody_name = example["input"]["water"][:50]

        for j, rule in enumerate(output.get("rules", []), 1):
            # Check that rule_text_verbatim is a string, not an array
            rule_text = rule.get("rule_text_verbatim")
            if isinstance(rule_text, list):
                errors.append(
                    f"Example {i}, Rule {j}: rule_text_verbatim is a list (should be string in ARU architecture)"
                )

            # Check that restriction is a dict, not a list
            restriction = rule.get("restriction")
            if isinstance(restriction, list):
                errors.append(
                    f"Example {i}, Rule {j}: restriction is a list (should be single object in ARU architecture)"
                )

    if errors:
        print(f"\n{len(errors)} ARU architecture violations found:")
        for error in errors:
            print(f"  {error}")
        raise AssertionError(f"{len(errors)} ARU violations")
    else:
        print(f"✓ All examples follow atomic regulation unit architecture!")


if __name__ == "__main__":
    print("=" * 80)
    print("VALIDATING EXAMPLES AGAINST MODELS.PY")
    print("=" * 80)
    print()

    print("1. Testing waterbody_key validation...")
    test_waterbody_key_validation()
    print()

    print("2. Testing alternate_names validation...")
    test_alternate_names_validation()
    print()

    print("3. Testing model validation...")
    test_example_validation()
    print()

    print("4. Testing verbatim chain of custody...")
    test_verbatim_chain_of_custody()
    print()

    print("5. Testing atomic architecture compliance...")
    test_atomic_architecture()
    print()

    print("=" * 80)
    print("ALL VALIDATION TESTS PASSED!")
    print("=" * 80)
