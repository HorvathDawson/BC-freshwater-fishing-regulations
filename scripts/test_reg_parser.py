import pytest
from reg_parser import RegParser, merge_orphaned_details

# --- TEST DATA ---
# Each tuple is (description, input_str, expected_list)
TEST_CASES = [
    (
        "Coquihalla: Species + Brackets + Quota (Should stay together)",
        "Trout/char (including steelhead) catch and release, bait ban, downstream of the southern entrance",
        [
            "Trout/char (including steelhead) catch and release, bait ban, downstream of the southern entrance"
        ]
    ),
    (
        "Cultus Lake: Adjective + Species (No Wild Trout)",
        "No wild trout over 50 cm, 1 bull trout over 60 cm",
        [
            "No wild trout over 50 cm", 
            "1 bull trout over 60 cm"
        ]
    ),
    (
        "Standard Closure (Control Case)",
        "No Fishing upstream of the bridge, Nov 1-June 30",
        [
            "No Fishing upstream of the bridge, Nov 1-June 30"
        ]
    ),
    (
        "Multiple Sentences (Should split)",
        "Fly fishing only; bait ban upstream of the northern entrance",
        [
            "Fly fishing only",
            "bait ban upstream of the northern entrance"
        ]
    ),
    (
        "Exemptions & Quotas: Should keep 'single barbless' and 'daily quota' together",
        "EXEMPT from single barbless hooks. Walleye daily quota = unlimited. Northern pike daily quota = unlimited",
        [
            "EXEMPT from single barbless hooks",
            "Walleye daily quota = unlimited",
            "Northern pike daily quota = unlimited"
        ]
    ),
    (
        "Alice Lake: 'No trout over 50 cm' should stay together",
        "Bait ban, single barbless hook, no trout over 50 cm",
        [
            "Bait ban",
            "single barbless hook",
            "no trout over 50 cm"
        ]
    ),
    (
        "Anderson Lake: 'Trout and kokanee' should stay together",
        "Artificial fly only, bait ban, single barbless hook. Trout and kokanee catch and release",
        [
            "Artificial fly only",
            "bait ban",
            "single barbless hook",
            "Trout and kokanee catch and release"
        ]
    ),
    (
        "Buttle Lake: 'is closed' should stay together",
        "Fly fishing only; except Thelwood Creek is closed all year",
        [
            "Fly fishing only",
            "except Thelwood Creek is closed all year"
        ]
    ),
    (
        "Fuller Lake: Smallmouth Bass should stay together",
        "Smallmouth bass daily quota = 4; electric motor only - max 7.5 kW; wheelchair accessible fishing platform is located in Fuller Lake Park",
        [
            "Smallmouth bass daily quota = 4",
            "electric motor only - max 7.5 kW",
            "wheelchair accessible fishing platform is located in Fuller Lake Park"
        ]
    ),
    (
        "Great Central Lake: 'no wild rainbow trout' chain should stay together",
        "Single barbless hook, no wild rainbow trout over 50 cm",
        [
            "Single barbless hook",
            "no wild rainbow trout over 50 cm"
        ]
    ),
    (
        "Gunflint Lake: Semicolon should separate 'catch and release' from 'bait ban'",
        "Trout catch and release; bait ban, single barbless hook; electric motor only - max 7.5 kW",
        [
            "Trout catch and release",
            "bait ban",
            "single barbless hook",
            "electric motor only - max 7.5 kW"
        ]
    ),
    (
        "Little Qualicum: Full Block Test",
        "No Fishing July 15-Aug 31 [Includes tributaries] , No Fishing - All tributaries. No Fishing from the falls in Little Qualicum Falls Provincial Park downstream to the hatchery fence, Dec 1-May 31. The standard 100 m closure around a fish rearing facility has been reduced to a no fishing area from the hatchery fence to signs approximately 35 m downstream. Fly fishing only, Sept 1-Nov 30 (where open)",
        [
            "No Fishing July 15-Aug 31 [Includes tributaries]",
            "No Fishing - All tributaries",
            "No Fishing from the falls in Little Qualicum Falls Provincial Park downstream to the hatchery fence, Dec 1-May 31",
            "The standard 100 m closure around a fish rearing facility has been reduced to a no fishing area from the hatchery fence to signs approximately 35 m downstream",
            "Fly fishing only, Sept 1-Nov 30 (where open)"
        ]
    )
]

@pytest.mark.parametrize("desc, input_text, expected", TEST_CASES)
def test_reg_parsing(desc, input_text, expected):
    """
    Runs the regex parser and merger on the input text.
    Checks if the resulting 'details' list matches expected output.
    """
    parsed = RegParser.parse_reg(input_text)
    merged = merge_orphaned_details(parsed)
    
    # Extract just the details for comparison
    actual_details = [p['details'] for p in merged]
    
    # Normalize (strip whitespace) for comparison
    expected_norm = [s.strip() for s in expected]
    actual_norm = [s.strip() for s in actual_details]
    
    assert actual_norm == expected_norm, f"Failed on: {desc}"