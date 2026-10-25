"""THE PARSED RUN TIMINGS (`data/curated/runtiming/timing.json`): the one record `parse` writes and deliver reads.

Two sources, one shape per run (T162, owner 2026-10-24):

  psf   the Pacific Salmon Foundation's (Salmon Watersheds Program) dataset #90, one curve per Conservation Unit,
        dated at RIVER ENTRY. Bound at delivery to every section whose midpoint lies in the CU's polygon
        (`spawning`) and to every section the run swims through on its way there from the sea (`passage`).
  dfo   Fisheries and Oceans Canada's NuSEDS spawner surveys, one record per surveyed stream, species and run: the
        share of surveyed years fish were IN THAT STREAM in each pentad. Bound to the stream's own sections by its
        FWA watershed code (`observed`). Better than a CU's river-entry curve on the stream it was measured in, so on
        those sections it replaces that species' `spawning` runs; the runs passing through stay.

A CURVE is 73 pentads (5-day steps from Jan 1 on the common year, `gauge_clim`'s axis), integers 0..100: for psf the
run's share of the pentad scaled to its own peak (100 = the peak pentad), for dfo the percent of surveyed years the
fish were present. Both read "how much of this run is here now, at its own scale" — what one colour ramp can show.

THE FILE IS PARSED, NOT AUTHORED: `python -m pipeline.runtiming.parse` rewrites it from the fetched sources, which are
not tracked (the PSF pages may not be re-hosted; NuSEDS is 227 MB). It is tracked because nothing else is.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

FORMAT = "runtiming-timing/1"
PENTADS = 73

#: THE COLOUR SLOTS, which are not quite the species list. Pink splits into its two brood lines: they are the same run
#: in alternate years and only one is ever in the river, so the map picks one by the year it shows. Sockeye's river-
#: and lake-type do NOT split: both run in one season, and one river really can carry both.
SLOTS: Tuple[str, ...] = ("Chinook", "Chum", "Coho", "Pink (even)", "Pink (odd)", "Sockeye", "Steelhead")

#: the credit every surface showing a timing carries (owner 2026-10-24: these words unless the owner gives others)
CREDIT = {"psf": "Pacific Salmon Foundation (Salmon Watersheds Program)", "dfo": "Fisheries and Oceans Canada"}


def slot_of(species: str, form: Optional[str] = None, year: Optional[int] = None) -> str:
    """The slot a run colours: pink by its brood line (`form` even/odd, or the survey `year`'s parity)."""
    if species == "Pink":
        if form in ("even", "odd"):
            return f"Pink ({form})"
        if year is not None:
            return "Pink (even)" if year % 2 == 0 else "Pink (odd)"
        raise ValueError("a pink run needs its brood line")
    if species not in SLOTS:
        raise ValueError(f"no slot for species {species!r}")
    return species


@dataclass(frozen=True)
class TimedRun:
    """One run as deliver ships it. `key` is the source's own id: `psf:<cuid>` or `dfo:<gfe_id>:<species>:<run>`."""
    key: str
    source: str                 # psf | dfo
    species: str
    slot: str
    name: str                   # the CU's name (psf) or the stream's (dfo)
    label: Optional[str]        # "summer", "fall", "Early Stuart" … — only where the source states one
    curve: Tuple[int, ...]
    peak: Optional[str]         # "Aug 11": psf the curve's peak, dfo the median peak of spawning
    window: Optional[Tuple[str, str]]   # psf 5th–95th percentile of entry; dfo median arrival – median end of spawning
    quality: Optional[str] = None       # psf's data-quality word
    years: Optional[int] = None         # dfo: how many surveyed years the curve is from
    first_year: Optional[int] = None
    last_year: Optional[int] = None
    cuid: Optional[int] = None          # psf
    wsc: Optional[str] = None           # dfo: the stream's FWA watershed code (trimmed)
    lon: Optional[float] = None         # dfo: the census site's point, when DFO gives one
    lat: Optional[float] = None
    site_kind: Optional[str] = None     # dfo: DFO's site type ("Stream", "Lake", "Artificial Channel" …)


@dataclass(frozen=True)
class Timing:
    psf: Tuple[TimedRun, ...]
    dfo: Tuple[TimedRun, ...]
    sources: Dict[str, dict]


def _run(d: dict) -> TimedRun:
    d = dict(d)
    d["curve"] = tuple(d["curve"])
    if d.get("window"):
        d["window"] = tuple(d["window"])
    r = TimedRun(**d)
    if len(r.curve) != PENTADS or any(not (0 <= v <= 100) for v in r.curve):
        raise ValueError(f"timing: {r.key} has no 73-pentad 0..100 curve")
    if r.slot not in SLOTS:
        raise ValueError(f"timing: {r.key} slot {r.slot!r}")
    return r


def read_timing(path: Path) -> Timing:
    """The parsed timings; RAISES on a missing or malformed file (AGENTS 37: absent curated data is a bug)."""
    p = Path(path)
    if not p.is_file():
        raise FileNotFoundError(f"{p} not found — make it: python -m pipeline.runtiming.parse")
    raw = json.loads(p.read_text())
    if raw.get("format") != FORMAT:
        raise ValueError(f"{p}: format {raw.get('format')!r}, not {FORMAT}")
    return Timing(psf=tuple(_run(r) for r in raw["psf"]), dfo=tuple(_run(r) for r in raw["dfo"]),
                  sources=raw["sources"])


def write_timing(path: Path, psf: List[dict], dfo: List[dict], sources: Dict[str, dict]) -> None:
    for r in (*psf, *dfo):
        _run(r)                                  # every record readable, or nothing is written
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps({
        "_": ["PARSED, NOT AUTHORED — rewritten by `python -m pipeline.runtiming.parse` from the fetched sources",
              "(pipeline/runtiming/timing.py says what each field is). Edit the parser, never this file."],
        "format": FORMAT, "sources": sources, "psf": psf, "dfo": dfo,
    }, indent=None, separators=(",", ":"), sort_keys=True).replace('},{"', '},\n{"') + "\n")
