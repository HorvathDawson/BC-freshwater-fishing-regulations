"""One run's record, and the run label that separates two runs on the same water.

WHY THIS IS NOT IN `generate/`. The bundler and the consumers READ these records; only the
generator writes them. If the reader lived beside the generator, every consumer would
import the module that streams 2.6 GB of stream geometry through an STRtree just to learn
when a fish shows up — the same argument `pipeline/gauges/matches.py` makes, for the same
reason.

WHAT A "RUN" IS HERE. The Pacific Salmon Explorer publishes one curve per Conservation
Unit, and a CU is already species-specific — so two runs of one species on one river are
two CUs, not one bimodal curve. The Skeena mainstem carries `Skeena Coastal Summers`
(peak Aug 11) and `Skeena Coastal Winters` (peak Jan 7) at the same time; collapsing them
to "steelhead" loses the only fact an angler is asking for.

So a reach does not have "a steelhead curve". It has a LIST of steelhead runs, and the
label is what makes them nameable.
"""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path

#: Ordered because the FIRST match wins and the words overlap: "Early Summer" must be read
#: as early-summer, not as `early` and then `summer`. Patterns are matched against the CU
#: name lower-cased.
_LABELS: list[tuple[str, str]] = [
    ("early summer", r"early[\s-]summer"),
    ("late summer",  r"late[\s-]summer"),
    ("summer",       r"\bsummers?\b"),
    ("winter",       r"\bwinters?\b"),
    ("spring",       r"\bspring\b"),
    ("fall",         r"\bfall\b"),
    ("early",        r"\bearly\b"),
    ("late",         r"\blate\b"),
]

#: Life-history qualifiers, which are NOT run labels. `(even)` and `(odd)` are the pink
#: brood line — a different population in a different year, not a second run in one season —
#: and river/lake type is where sockeye rear. They are carried separately so nothing shows
#: a reader "Pink (even) and Pink (odd) both running now": in any given year only one is.
_FORMS: list[tuple[str, str]] = [
    ("even",       r"\(even\)"),
    ("odd",        r"\(odd\)"),
    ("river-type", r"river[\s-]type"),
    ("lake-type",  r"lake[\s-]type"),
    ("cyclic",     r"\(cyclic\)"),
]


def derive_label(cu_name: str) -> str | None:
    """The run label a CU's own name states, or None when it does not state one.

    149 of 463 names carry one. The rest are single-run waters where the species IS the
    answer — and the handful that are not are what `curated/runtiming/review.json` is for.
    """
    n = cu_name.lower()
    for label, pat in _LABELS:
        if re.search(pat, n):
            return label
    return None


def derive_form(cu_name: str) -> str | None:
    """The life-history / brood-line qualifier, which is never a run label."""
    n = cu_name.lower()
    for form, pat in _FORMS:
        if re.search(pat, n):
            return form
    return None


@dataclass(frozen=True)
class Run:
    """One CU's run, as everything downstream sees it.

    `cuid` is the Pacific Salmon Explorer's own id and is the join key everywhere. It is
    stable across their releases in a way names are not — two CUs are called "Lower Skeena"
    in four different species.
    """

    cuid: int
    name: str
    species: str
    region: str
    #: "summer" / "winter" / ... — derived from the name, overridable in review.json.
    label: str | None = None
    #: "even" / "odd" / "river-type" / ... — never a run label. See `_FORMS`.
    form: str | None = None
    #: Where the label came from, so a reader can tell a stated fact from an authored one.
    label_source: str = "name"          # name | review | none
    #: 73 pentad shares of the run, summing to ~100. Empty when the CU has no curve.
    pentads: tuple[float, ...] = ()
    quality: int = 0
    quality_word: str = "Not Applicable"
    peak_date: str | None = None
    span: tuple[str, str] | None = None  # 5th - 95th percentile dates
    dfo_cu: str | None = None

    @property
    def has_curve(self) -> bool:
        return bool(self.pentads)

    def display(self) -> str:
        """What to call this run in a sentence: 'summer-run Steelhead', 'Chum'."""
        if self.label:
            return f"{self.label}-run {self.species}" if self.label in ("summer", "winter") \
                   else f"{self.label} {self.species}"
        return self.species


def read_runs(path: str | Path) -> dict[int, Run]:
    raw = json.loads(Path(path).read_text())
    out = {}
    for r in raw["runs"]:
        r = dict(r)
        r["pentads"] = tuple(r.get("pentads") or ())
        if r.get("span"):
            r["span"] = tuple(r["span"])
        out[r["cuid"]] = Run(**r)
    return out


def write_runs(path: str | Path, runs: dict[int, Run], meta: dict) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(
        {"meta": meta, "runs": [asdict(r) for r in runs.values()]}, indent=1))
