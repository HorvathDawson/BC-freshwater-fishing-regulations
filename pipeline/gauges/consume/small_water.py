"""(T823) SMALL NEXT TO ITS GAUGE: which led sections carry a warning beside their lead gauge's reading.

THE NETWORK DECIDES WHO SPEAKS (owner 2026-11-16, on a 1.4 km² creek shown "Below normal" from a gauge 676x bigger:
"Could we just use our gauge network that colors stuff wtc to determine if it is significant enough? It would drop that
in theory. Then ofr small ones still give a warning if used."). `section_gauge` is that network: the drainage gate and
the trust floor already applied (shed.py), one lead per section. A section with no row has no lead and no estimate —
the owner's creek: the Chilliwack at Vedder Crossing drains 2,110 headwater links against its one, below the `weak`
floor, so it never leads there. This module answers the second half: of the sections that DO have a lead, which are
small next to it. The flag ships as `section_gauge.small` (conditions file), the sentence as the conditions meta
`small_water_text`; the app computes no ratio and no threshold (owner 16:50: Python decides every word and number).

BOTH DRAINAGES ARE MODELLED, BY THE PANELS' OWN MODEL (`pipeline.atlas.graph.drainage.AreaModel`):
  * the section's: its panel target area where it has a panel (`section_panel.area_km2`, the km² the app shows from
    `flow-area`) — that number carries a side channel's adoption by its river, which the line's own magnitude does not:
    the lower Fraser's side channels have magnitude 3 and drain 64,000 km² — else the model at its own magnitude;
  * the gauge's: the model at the node it reads (`at_river`: a diversion gauge's intake), as its panel members carry it
    (`panel_member.area_km2`).
  A ratio inside one model cancels the bias both ends share (`drainage.fit_area_model`); the app's "676x bigger" is the
  same pair. The published catchment (`gauge.area_km2`) is not used: the model runs low on big rivers (panels.build),
  and a published area over a modelled one would read a big river's creeks as smaller than they are.

ONE DIRECTION. Only a section SMALLER than its gauge is flagged: the reading is a big river's, the water is not. A section
bigger than its lead (the gauge sees part of it) is another question, not this warning.

THE THRESHOLD IS POLICY (memory: gauge representativeness is policy) and the OWNER'S CALL — PENDING (T823). Measured on
the live bundle 2026-11-16 (114,428 led sections, 24,667 on a named water across 5,216 waters):
  x50   the network's own `weak` band, measured in area (`weak` = under 1/100 of the gauge's magnitude ~ 50x its area):
        93,115 sections (81%), 12,332 named (50%) on 3,750 waters                                  <- RECOMMENDED
  x100  the app's trust ladder turns `distant` past 100x (trust.ts ERROR_BY_RATIO, 9,495 measured gauge pairs):
        76,284 sections (67%), 8,543 named (35%) on 2,938 waters
  x200  only the extremes: 44,053 sections (38%), 4,128 named (17%) on 1,690 waters
The network's floor caps every lead at ~357x (`TRUST_BANDS` weak 0.001 in magnitude), so nothing above is ever led.
"""
from __future__ import annotations

from typing import Iterable, Mapping, Optional

#: The thresholds the owner chooses between: the gauge drains AT LEAST this many times the section's own area.
SMALL_WATER_RATIOS: dict[str, float] = {"x50": 50.0, "x100": 100.0, "x200": 200.0}

#: THE SWITCH. PENDING OWNER (T823): the recommended value until he picks; one edit and a bundle rebuild moves it.
SMALL_WATER_POLICY = "x50"

#: The warning beside the lead gauge's reading on a small water, word for word as the app shows it (PENDING OWNER).
SMALL_WATER_TEXT = "This stream is much smaller than the river its gauge measures: the reading may not match it."

#: The conditions file's meta keys the sentence and the threshold ride in (emitted to the app by gauge_policy).
META_TEXT_KEY = "small_water_text"
META_RATIO_KEY = "small_water_ratio"


def ratio_for(policy: Optional[str] = None) -> float:
    """The threshold of `policy` (default `SMALL_WATER_POLICY`); an unknown name raises, never a default."""
    name = SMALL_WATER_POLICY if policy is None else policy
    if name not in SMALL_WATER_RATIOS:
        raise ValueError(f"small water: no policy {name!r} (one of {', '.join(SMALL_WATER_RATIOS)})")
    return SMALL_WATER_RATIOS[name]


def is_small(section_km2: Optional[float], gauge_km2: Optional[float], ratio: float) -> bool:
    """The gauge drains at least `ratio` times the section's area. A missing or non-positive area raises: a led
    section always has a magnitude (shed.trust_for), so an absent area is a bug upstream, never a small water."""
    if not section_km2 or section_km2 <= 0 or not gauge_km2 or gauge_km2 <= 0:
        raise ValueError(f"small water: no drainage to compare (section {section_km2!r}, gauge {gauge_km2!r} km²)")
    if ratio <= 1:
        raise ValueError(f"small water: a threshold of {ratio!r} would call a water as big as its gauge small")
    return gauge_km2 >= ratio * section_km2


def section_area(section_id: str, graph, model, panel_area: Mapping[str, Optional[float]]) -> Optional[float]:
    """The section's own drainage: its panel target area where it has one, else the model at its own magnitude."""
    a = panel_area.get(section_id)
    if a:
        return a
    n = graph.nodes[section_id]
    return model.area_km2(getattr(n, "stream_magnitude", None), getattr(n, "wsc", ""))


def small_sections(links: Iterable, graph, model, at_river: Mapping[str, str],
                   panel_area: Mapping[str, Optional[float]], ratio: float) -> set[str]:
    """The section ids among `links` (`shed.GaugeLink`, one per led section) that are small next to their lead gauge.

    `at_river`: station -> the node it reads (bundle_tables: a diversion gauge's intake). `panel_area`: section id ->
    its panel target area (`Panels.by_section`)."""
    gauge_area: dict[str, Optional[float]] = {}
    out: set[str] = set()
    for l in links:
        if l.station not in gauge_area:
            g = graph.nodes[at_river[l.station]]
            gauge_area[l.station] = model.area_km2(getattr(g, "stream_magnitude", None), getattr(g, "wsc", ""))
        if is_small(section_area(l.section_id, graph, model, panel_area), gauge_area[l.station], ratio):
            out.add(l.section_id)
    return out
