"""`lengths` — the size limit said once, plainly.

These pin the three things the old `over_cm`/`under_cm`/`band` trio could not say without being
reconstructed, and that consumers kept reconstructing differently.
"""
import pytest

from pipeline.regs.parsing.catalogue import (LengthBand, Period, RuleType,
                                             lengths_from_bounds)
from pipeline.regs.table.corpus import rules


class Bounds:
    """The fields `lengths_from_bounds` reads, and nothing else."""
    def __init__(self, over_cm=None, under_cm=None, band=False, take=None, within=None,
                 period=Period.daily, type=RuleType.retention_limit):
        self.over_cm, self.under_cm, self.band = over_cm, under_cm, band
        self.take, self.within, self.period, self.type = take, within, period, type


def keep(bands, cm, take=None):
    """How many of a fish THIS long the bands allow; None = not spoken about."""
    for b in bands or []:
        if b.holds(cm):
            return b.take if b.take is not None else take
    return None


# ---------------------------------------------------------------------------------------
# The three readings of `over_cm`, which is the whole reason this field exists
# ---------------------------------------------------------------------------------------
def test_over_cm_bounds_the_granted_fish_when_the_rule_is_the_whole_allowance():
    """"Trout daily quota = 2 (none over 50 cm)" — keep 2 up to 50, and none above it."""
    b = lengths_from_bounds(Bounds(over_cm=50, take=2))
    assert keep(b, 30, 2) == 2
    assert keep(b, 50, 2) == 2, "50 cm is 'not over 50' and is still granted"
    assert keep(b, 51, 2) == 0, "above the cap the answer is zero, not silence"


def test_over_cm_counts_the_big_ones_inside_a_clause_and_leaves_the_rest_to_the_parent():
    """"only 1 over 40 cm" under "Trout: 4" — a 30 cm trout is the parent's business."""
    b = lengths_from_bounds(Bounds(over_cm=40, take=1, within="trout.r1"))
    assert keep(b, 41, 1) == 1
    assert keep(b, 30, 1) is None, "the clause must not cap a small fish at its own number"


def test_over_cm_denies_outright_when_the_rule_grants_nothing():
    """"no trout over 50 cm" — zero big ones, and SILENCE about the small ones, because this
    sentence does not grant them either."""
    b = lengths_from_bounds(Bounds(over_cm=50, take=0))
    assert keep(b, 60, 0) == 0
    assert keep(b, 40, 0) is None


# ---------------------------------------------------------------------------------------
# The floor, and the endpoint that decides which way a rule runs
# ---------------------------------------------------------------------------------------
def test_a_floor_is_absolute_even_inside_a_clause():
    """"no more than 1 char (none under 60 cm)" forbids a 50 cm char outright. Handing it back
    to the parent quota — which is what a clause does with a COUNTED size — would let a reader
    keep the fish the sentence protects."""
    b = lengths_from_bounds(Bounds(under_cm=60, take=1, within="char.r1"))
    assert keep(b, 70, 1) == 1
    assert keep(b, 50, 1) == 0


def test_the_shared_endpoint_is_granted_not_denied():
    """A grant is written before the denial beneath it, so order alone settles the one length
    both ranges contain. Reverse the list and a 60 cm fish is refused."""
    b = lengths_from_bounds(Bounds(under_cm=60, take=1))
    assert keep(b, 60, 1) == 1
    assert keep(list(reversed(b)), 60, 1) == 0, "the guard is order, so prove order matters"


# ---------------------------------------------------------------------------------------
# A hole and a window are opposites, and used to be told apart by one flag
# ---------------------------------------------------------------------------------------
def test_a_band_is_a_hole_and_a_slot_is_a_window():
    hole = lengths_from_bounds(Bounds(under_cm=40, over_cm=60, band=True))
    window = lengths_from_bounds(Bounds(under_cm=40, over_cm=60, take=2))
    assert keep(hole, 50) == 0, "a band forbids the middle"
    assert keep(window, 50, 2) == 2, "a slot allows only the middle"
    assert keep(window, 30, 2) == 0 and keep(window, 70, 2) == 0


def test_a_hole_carrying_a_number_gives_it_to_the_piece_above_only():
    """Bennett Lake: "northern pike = 4" + "only 1 over 100 cm, none between 70 and 100". The
    reading this replaced capped a 50 cm pike at 1; the book allows the parent's 4."""
    b = lengths_from_bounds(Bounds(under_cm=70, over_cm=100, band=True, take=1,
                                   within="bennett_lake.r11"))
    assert keep(b, 120, 1) == 1
    assert keep(b, 80, 1) == 0
    assert keep(b, 50, 1) is None, "below the hole the parent quota of 4 governs, not this 1"


# ---------------------------------------------------------------------------------------
# The model refuses a range that says nothing
# ---------------------------------------------------------------------------------------
@pytest.mark.parametrize("kw", [{}, {"min_cm": 60, "max_cm": 40}, {"min_cm": 50, "max_cm": 50}])
def test_a_range_that_holds_no_fish_or_every_fish_is_refused(kw):
    with pytest.raises(ValueError):
        LengthBand(**kw)


# ---------------------------------------------------------------------------------------
# And it is actually on the data
# ---------------------------------------------------------------------------------------
def test_every_rule_with_a_size_bound_carries_lengths_and_no_other_rule_does():
    rs = rules()
    sized = {id(x) for x in rs if x.get("over_cm") or x.get("under_cm")}
    got = {id(x) for x in rs if x.get("lengths")}
    assert sized and got == sized


def test_the_four_rules_whose_band_was_backwards_read_as_windows():
    """Coquitlam, Gwillim, Lower Blue and Williston Zone B were flagged as holes when the
    sentence is a window, which permits exactly the fish the rule protects."""
    want = {"coquitlam_river.r3", "gwillim_lake.r1"}
    seen = 0
    for x in rules():
        if x.get("rule") not in want or not x.get("lengths"):
            continue
        seen += 1
        lo, hi = x["under_cm"], x["over_cm"]
        mid = (lo + hi) // 2
        bands = [LengthBand(**b) for b in x["lengths"]]
        assert keep(bands, mid, x.get("take")) not in (0, None), \
            f"{x['rule']}: {mid} cm is inside the window and must be keepable"
    assert seen == len(want)


# ---------------------------------------------------------------------------------------
# The three readings a size can have that are NOT a retention limit at all.
# Each was found by a reviewer reading the sentence, after the first cut of this file
# shipped them inverted.
# ---------------------------------------------------------------------------------------
def test_a_size_on_a_rule_that_is_not_about_keeping_names_the_fish_and_sets_no_number():
    """"Conservation Surcharge Stamp required to catch and keep rainbow trout over 50 cm" says
    the stamp is needed for the big ones. Read with the retention branches it came out as "keep
    zero rainbow trout over 50 cm" — a paperwork rule turned into a ban."""
    b = lengths_from_bounds(Bounds(over_cm=50, type=RuleType.document_required))
    assert keep(b, 60) is None, "a document rule sets no number for the fish it names"
    assert keep(b, 60, 0) != 0 or b[0].take is None
    assert b[0].take is None and b[0].min_cm == 50


def test_an_annual_quota_counts_a_size_class_and_forbids_nothing_beneath_it():
    """"Rainbow trout: 5 over 50 cm" is five big ones per licence year and says NOTHING about a
    30 cm rainbow, which the daily quota governs. A floor here is an invented annual ban."""
    b = lengths_from_bounds(Bounds(under_cm=50, take=5, period=Period.annual))
    assert keep(b, 60, 5) == 5
    assert keep(b, 30, 5) is None


def test_a_possession_limit_is_a_plain_ceiling_and_not_an_annual_count():
    """"Trout/char daily and possession quotas = 2 (none over 50 cm)" carries the same ceiling
    as its daily twin. Lumped in with `annual` as "not daily" it granted the 2 to fish OVER
    50 cm — exactly the ones the sentence forbids — and said nothing about the ones it allows."""
    b = lengths_from_bounds(Bounds(over_cm=50, take=2, period=Period.possession))
    assert keep(b, 40, 2) == 2
    assert keep(b, 60, 2) == 0
