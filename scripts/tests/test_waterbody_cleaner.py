"""
Tests for waterbody name cleaning functionality.
"""

import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from synopsis_pipeline.waterbody_cleaner import (
    clean_waterbody_name,
    add_cleaned_names,
    group_by_region_and_cleaned_name,
)


def test_clean_waterbody_name():
    """Test the clean_waterbody_name function with various inputs."""

    test_cases = [
        # (input, expected_output)
        ('"ANDERSON" LAKE', "ANDERSON LAKE"),
        ("(Lower) CAMPBELL LAKE'S TRIBUTARIES", "CAMPBELL LAKE'S TRIBUTARIES"),
        ('"(Lower) CAMPBELL LAKE\'S TRIBUTARIES"', "CAMPBELL LAKE'S TRIBUTARIES"),
        ('"CLEAR LAKE (Quadra Island)"', "CLEAR LAKE"),
        ("CLEAR LAKE (Quadra Island)", "CLEAR LAKE"),
        ("SIMPLE LAKE", "SIMPLE LAKE"),
        ('"QUOTED" NAME (with location)', "QUOTED NAME"),
        ("Multiple   Spaces", "Multiple Spaces"),
        ("", ""),
        # Nested parentheses test case
        (
            "WIGWAM RIVER (upstream of the Forest Service recreation site adjacent to km 42 on the Bighorn (Ram) Forest Service Road)",
            "WIGWAM RIVER",
        ),
        (
            "WIGWAM RIVER (downstream of the access road adjacent to km 42 on the Bighorn (Ram) Forest Service Road)",
            "WIGWAM RIVER",
        ),
    ]

    print("Testing clean_waterbody_name():")
    print("=" * 80)

    all_passed = True
    for input_name, expected in test_cases:
        result = clean_waterbody_name(input_name)
        passed = result == expected
        all_passed = all_passed and passed

        status = "✓" if passed else "✗"
        print(f"{status} Input: {input_name!r}")
        print(f"  Expected: {expected!r}")
        print(f"  Got:      {result!r}")
        if not passed:
            print(f"  FAILED!")
        print()

    return all_passed


def test_add_cleaned_names():
    """Test adding cleaned names to a list of parsed results."""

    test_data = [
        {
            "waterbody_name": '"ANDERSON" LAKE',
            "region": "Region 1",
            "raw_text": "Some regulations...",
        },
        {
            "waterbody_name": "(Lower) CAMPBELL LAKE'S TRIBUTARIES",
            "region": "Region 2",
            "raw_text": "Other regulations...",
        },
        {
            "waterbody_name": "CLEAR LAKE (Quadra Island)",
            "region": "Region 1",
            "raw_text": "More regulations...",
        },
    ]

    print("\nTesting add_cleaned_names():")
    print("=" * 80)

    result = add_cleaned_names(test_data.copy())

    expected_cleaned = ["ANDERSON LAKE", "CAMPBELL LAKE'S TRIBUTARIES", "CLEAR LAKE"]

    all_passed = True
    for i, item in enumerate(result):
        has_field = "cleaned_waterbody_name" in item
        correct_value = (
            item.get("cleaned_waterbody_name") == expected_cleaned[i]
            if has_field
            else False
        )
        passed = has_field and correct_value
        all_passed = all_passed and passed

        status = "✓" if passed else "✗"
        print(f"{status} Item {i+1}:")
        print(f"  Original: {item['waterbody_name']!r}")
        print(f"  Cleaned:  {item.get('cleaned_waterbody_name', 'MISSING')!r}")
        print(f"  Expected: {expected_cleaned[i]!r}")
        if not passed:
            print(f"  FAILED!")
        print()

    return all_passed


def test_grouping():
    """Test grouping by region and cleaned name."""

    test_data = [
        {
            "waterbody_name": '"ANDERSON" LAKE',
            "region": "Region 1",
            "cleaned_waterbody_name": "ANDERSON LAKE",
            "raw_text": "Regs 1",
        },
        {
            "waterbody_name": "ANDERSON LAKE (North)",
            "region": "Region 1",
            "cleaned_waterbody_name": "ANDERSON LAKE",
            "raw_text": "Regs 2",
        },
        {
            "waterbody_name": "CLEAR LAKE",
            "region": "Region 1",
            "cleaned_waterbody_name": "CLEAR LAKE",
            "raw_text": "Regs 3",
        },
        {
            "waterbody_name": '"ANDERSON" LAKE',
            "region": "Region 2",
            "cleaned_waterbody_name": "ANDERSON LAKE",
            "raw_text": "Regs 4",
        },
    ]

    print("\nTesting group_by_region_and_cleaned_name():")
    print("=" * 80)

    result = group_by_region_and_cleaned_name(test_data)

    # Check structure
    has_region1 = "Region 1" in result
    has_region2 = "Region 2" in result

    region1_anderson_count = len(result.get("Region 1", {}).get("ANDERSON LAKE", []))
    region1_clear_count = len(result.get("Region 1", {}).get("CLEAR LAKE", []))
    region2_anderson_count = len(result.get("Region 2", {}).get("ANDERSON LAKE", []))

    print(f"✓ Has Region 1: {has_region1}")
    print(f"✓ Has Region 2: {has_region2}")
    print(f"\nRegion 1 - ANDERSON LAKE: {region1_anderson_count} items (expected 2)")
    print(f"Region 1 - CLEAR LAKE: {region1_clear_count} items (expected 1)")
    print(f"Region 2 - ANDERSON LAKE: {region2_anderson_count} items (expected 1)")

    all_passed = (
        has_region1
        and has_region2
        and region1_anderson_count == 2
        and region1_clear_count == 1
        and region2_anderson_count == 1
    )

    if all_passed:
        print("\n✓ All grouping tests passed!")
    else:
        print("\n✗ Some grouping tests failed!")

    return all_passed


if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("WATERBODY CLEANER TESTS")
    print("=" * 80 + "\n")

    test1 = test_clean_waterbody_name()
    test2 = test_add_cleaned_names()
    test3 = test_grouping()

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"clean_waterbody_name: {'PASSED ✓' if test1 else 'FAILED ✗'}")
    print(f"add_cleaned_names: {'PASSED ✓' if test2 else 'FAILED ✗'}")
    print(f"grouping: {'PASSED ✓' if test3 else 'FAILED ✗'}")

    if test1 and test2 and test3:
        print("\n✓ All tests passed!")
        sys.exit(0)
    else:
        print("\n✗ Some tests failed!")
        sys.exit(1)
