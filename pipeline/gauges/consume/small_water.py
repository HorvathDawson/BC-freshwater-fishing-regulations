"""(T823) SMALL NEXT TO ITS GAUGE: for every section a donor panel speaks for, which of the panel's gauges drain so much
more than the section that their reading needs a warning.

THE SPEAKER IS THE PANEL (owner 2026-11-16 22:19, "Allow, with a warning (Recommended)"). The Conditions map colours a
section from its donor panel (the app's `speakerFor` / `buildStandingClasses`), and the sheet and a spot lead with one of
that panel's members, chosen LIVE from the feed (the heaviest reporting donor, flow first: `sheetLead` / `leadOver`).
So the warning cannot hang on `section_gauge` (a match record the map does not colour by), and Python cannot name the
leading station ahead of the feed. What Python CAN decide is, per section, every member it might lead with: the release
ships one bit per member (`flow-small/1`, paint.py), set where that member drains at least `ratio_for()` times the
section's own target area. The app shows `small_water_text` (conditions meta) when the member it leads with has its bit
set — no ratio and no threshold in the app (owner 16:50: Python decides every word and number). A big gauge still
speaks for a small water (the owner's 1.4 km² Chilliwack-valley creek, coloured by 08MH001 at 676x): it speaks with
the warning.

BOTH AREAS ARE THE ONES THE APP ALREADY SHOWS: the section's target area (`section_panel.area_km2`, the `flow-area`
file) and the member's (`panel_member.area_km2`, in paint) — the panels' own model at both ends, so the app's "676x"
and this decision are the same pair of numbers. A side channel's target area is its river's (panels.build adopts it).

A LAKE SPEAKER (a station standing in the section's lake, no panel) reads a level, not a share of a river: no bit.

THE THRESHOLD (owner 2026-11-16 22:18: "50x (Recommended)") is policy (memory: gauge representativeness is policy):
`SMALL_WATER_POLICY` names one of `SMALL_WATER_RATIOS`; a change is a release re-cut, not a bundle rebuild.
"""
from __future__ import annotations

from typing import Iterable, Optional, Sequence

#: The thresholds measured for the owner: the member drains AT LEAST this many times the section's target area.
SMALL_WATER_RATIOS: dict[str, float] = {"x50": 50.0, "x100": 100.0, "x200": 200.0}

#: THE SWITCH — owner 2026-11-16 22:18: "50x (Recommended)".
SMALL_WATER_POLICY = "x50"

#: The warning beside a reading from a member whose bit is set, word for word as the app shows it (owner 2026-11-16
#: 22:18: "Much smaller stream (Recommended)").
SMALL_WATER_TEXT = "This stream is much smaller than the river its gauge measures: the reading may not match it."

#: The conditions file's meta keys the sentence and the threshold ride in (emitted to the app by gauge_policy).
META_TEXT_KEY = "small_water_text"
META_RATIO_KEY = "small_water_ratio"

#: A panel holds at most this many members (the bitmask's width the app may rely on); more stops the release.
MAX_MEMBERS = 31


def ratio_for(policy: Optional[str] = None) -> float:
    """The threshold of `policy` (default `SMALL_WATER_POLICY`); an unknown name raises, never a default."""
    name = SMALL_WATER_POLICY if policy is None else policy
    if name not in SMALL_WATER_RATIOS:
        raise ValueError(f"small water: no policy {name!r} (one of {', '.join(SMALL_WATER_RATIOS)})")
    return SMALL_WATER_RATIOS[name]


def is_small(section_km2: Optional[float], gauge_km2: Optional[float], ratio: float) -> bool:
    """The gauge drains at least `ratio` times the section's area. A missing or non-positive area raises: every panel
    section and member carries one (panels.build), so an absent area is a bug upstream, never a small water."""
    if not section_km2 or section_km2 <= 0 or not gauge_km2 or gauge_km2 <= 0:
        raise ValueError(f"small water: no drainage to compare (section {section_km2!r}, gauge {gauge_km2!r} km²)")
    if ratio <= 1:
        raise ValueError(f"small water: a threshold of {ratio!r} would call a water as big as its gauge small")
    return gauge_km2 >= ratio * section_km2


def member_mask(section_km2: float, member_km2: Sequence[float], ratio: float) -> int:
    """Bit i set where member i (its `panel_member.ord`) drains at least `ratio` times the section's target area."""
    if len(member_km2) > MAX_MEMBERS:
        raise ValueError(f"small water: a panel of {len(member_km2)} members (the mask holds {MAX_MEMBERS})")
    return sum(1 << i for i, a in enumerate(member_km2) if is_small(section_km2, a, ratio))


def masks(sections: Iterable[tuple[int, int, float]], members: dict[int, Sequence[float]],
          ratio: float) -> list[int]:
    """One mask per `(sid, panel_id, area_km2)` row, in the order given; `members`: panel_id -> member areas by ord.
    A row naming a panel with no members raises (paint would carry a panel the map cannot read)."""
    out = []
    for sid, pid, area in sections:
        if pid not in members:
            raise ValueError(f"small water: section {sid} names panel {pid}, which has no members")
        out.append(member_mask(area, members[pid], ratio))
    return out
