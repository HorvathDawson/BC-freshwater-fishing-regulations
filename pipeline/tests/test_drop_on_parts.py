"""`drop_on_parts` (owner 2026-10-26): "on parts" is not an undrawn part; a named part stays."""
from pipeline.regs.parsing.drop_on_parts import is_on_parts, repair_rule
from pipeline.regs.review.precheck import UNDRAWN_REASON


def test_only_the_pure_wording_matches():
    for w in ("on parts", "on part", "On Parts", "parts of the lake (not specified in the book)"):
        assert is_on_parts(w), w
    for w in ("within 100 m of outlet", "various locations (as buoyed and signed)",
              "in swimming areas, as buoyed and signed", "south half", ""):
        assert not is_on_parts(w), w


def test_drops_the_part_and_its_reason():
    r = {"type": "vessel_rule", "aspect": "speed", "max_kmh": 8.0, "extents": [{"op": "whole"}],
         "review_reason": UNDRAWN_REASON + ": 'on parts' names no reach", "undrawn_part": "on parts"}
    assert repair_rule(r) is not None
    assert "undrawn_part" not in r and "review_reason" not in r


def test_a_reason_with_another_job_stays():
    r = {"type": "vessel_rule", "aspect": "speed", "extents": [{"op": "whole"}],
         "unresolved_locators": ["on parts"], "undrawn_part": "on parts",
         "review_reason": UNDRAWN_REASON + ": no km/h and 'on parts' names no reach"}
    repair_rule(r)
    assert "unresolved_locators" not in r and r["review_reason"] and "undrawn_part" not in r


def test_a_named_part_is_untouched():
    r = {"undrawn_part": "within 100 m of outlet", "review_reason": UNDRAWN_REASON + ": x"}
    assert repair_rule(r) is None and r["undrawn_part"]
