"""How many years of record a station has FOR ONE QUANTITY — the one place that is decided (T110).

A station's record is not one number. 07ED001 has 59 years of discharge and 4 of level; the bundle used to take the
longest across quantities and hand 59 to the Level view, so its level reading was weighted as a long record —
measured on bundle.sqlite, 375 panel stations had a level record at least ten years shorter than their discharge one.
The basin field took whichever `gauge_stats` row came last (level), so its Flow view under-weighted 411 stations the
other way (08LD001: 115 years of discharge, weighted as 7).

The FACT is counted once, by `climatology.build` (years with any observation, per station and parameter), and carried
as `clim.json` `stats` → the bundle's `gauge_stats`. Every reader asks `record_years(stats, station, quantity)`; nothing
else reduces a station's records to one number (`pipeline/tests/test_record_years.py` greps for it).

Pure: the feed half imports it (climatology's floor), so it may know station ids and nothing else.
"""

from __future__ import annotations

from typing import Mapping

#: The quantities a record is kept in, as `gauge_stats.parameter` names them and the app's Flow / Level views ask.
QUANTITIES: tuple[str, ...] = ("discharge", "level")

#: The shortest record that may rank a reading at all, in years. ONE constant: the climatology will not build an
#: envelope from fewer (`climatology.build`), and a donor panel will not admit a donor with fewer in the quantity asked
#: (`pipeline.gauges.consume.panel.eligible`). Mirrored to TypeScript by `pipeline.tools.emit.gauge_policy`.
#:
#: WAS TEN, AND TEN WAS ARBITRARY — see `pipeline.gauges.consume.panel` for the measurement that moved it to three: a short
#: record makes a percentile coarse, not wrong, and the record factor already prices that (3/20 of a full weight).
MIN_RECORD_YEARS = 3

#: `{station: {quantity: {"years": n, ...}}}` — `clim.json`'s `stats`, which `gauge_stats` is a copy of.
Stats = Mapping[str, Mapping[str, Mapping[str, object]]]


def record_years(stats: Stats, station: str, quantity: str) -> int:
    """Years of record `station` has in `quantity` (`discharge` | `level`); 0 when it has none.

    Never a maximum, a sum or "whichever comes first" across quantities: a Level answer is weighted by the level record
    and a Flow answer by the discharge record, because the percentile each reads was ranked against that history.
    """
    if quantity not in QUANTITIES:
        raise ValueError(f"record_years: {quantity!r} is not one of {QUANTITIES}")
    row = stats.get(station, {}).get(quantity) or {}
    return int(row.get("years") or 0)

