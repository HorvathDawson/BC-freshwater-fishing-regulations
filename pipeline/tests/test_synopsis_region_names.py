"""The synopsis parser's region-name normaliser — a pure function, so it runs in the fast tier.

Moved out of test_extract_synopsis.py (all `slow`: it parses the PDF) in the 2026-10-22 test cleanup; the
assertions are unchanged."""
from pipeline.regs.extraction.extract_synopsis import FishingSynopsisParser


def test_region_name_normalization(tmp_path):
    """Test that region names are normalized to consistent format."""
    parser = FishingSynopsisParser(output_dir=str(tmp_path))

    # Test various input formats
    test_cases = [
        ("REGION 1 - VANCOUVER ISLAND", "REGION 1 - Vancouver Island"),
        ("region 1 - vancouver island", "REGION 1 - Vancouver Island"),
        ("Region 1 - Vancouver Island", "REGION 1 - Vancouver Island"),
        ("REGION 2 - LOWER MAINLAND", "REGION 2 - Lower Mainland"),
        ("REGION 3 - thompson-nicola", "REGION 3 - Thompson-Nicola"),
        ("REGION 4 - kootenay", "REGION 4 - Kootenay"),
        ("REGION 7A - omineca-peace", "REGION 7A - Omineca-Peace"),
    ]

    for input_name, expected_output in test_cases:
        actual = parser._normalize_region_name(input_name)
        assert (
            actual == expected_output
        ), f"Failed: {input_name!r} -> {actual!r} (expected {expected_output!r})"
