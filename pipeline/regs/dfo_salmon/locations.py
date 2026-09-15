"""locations — split the scrape into its two lifetimes.

The DFO table's five columns do not change at the same rate:

    Waters | Specific area | (+ the section banner)   ← LOCATION, ~static
    Species | Dates | Limits/Gear                     ← REGULATION, ~50% turnover/year

So the scrape emits them as two artefacts. The cron diffs the *locations* file; if it
is unchanged, nothing about geography moved and every rule can be published against the
existing curated bindings without a human.

Identity vs integrity — the distinction the whole design rests on:

* ``fingerprint``  is derived from the normalised source text. It is the **integrity
  check**. When it moves, a human should look.
* ``location_id``  lives in the entries file, is assigned once by a curator, and is
  derived from nothing. It is the **identity**. Rules and geometry hang off it.

If identity were derived from the text, ``"Highway 37 Bridge"`` -> ``"Highway 37
bridge"`` would silently orphan a curated binding. That drift is in the archives, in
both directions, twice.

Region 6's cascade is carried too: a section catch-all, an Area catch-all and the
region baseline are all Locations, distinguished by ``kind``/``precedence``, because
rules attach to them exactly as they attach to a named water.

CLI
---
    .venv/bin/python -m pipeline.regs.dfo_salmon.locations --out data/generated/regs/dfo_salmon
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from pipeline.regs.dfo_salmon.fetch import ALL_SLUGS, DEFAULT_CACHE, PAGES, normalize_slug
from pipeline.regs.dfo_salmon.parse import parse_cached
from pipeline.regs.dfo_salmon.untangle import Untangled, classify_scope, untangle
from pipeline.common.curated import GENERATED

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Normalisation — only collapses drift actually observed in the archives
# ---------------------------------------------------------------------------

_SYNONYMS = [
    (r"\bhwy\.?\b", "highway"),
    (r"#\s*(\d)", r"\1"),
    (r"\bmetres?\b|\bmeters?\b", "m"),
    (r"\bkilometres?\b|\bkilometers?\b", "km"),
    (r"\bapprox\.?\b|\bapproximately\b", "approx"),
    (r"\brd\.?\b", "road"),
    (r"\bck\.?\b|\bcr\.?\b", "creek"),
    (r"\br\.(?=\s|$)", "river"),
    (r"\s*&\s*", " and "),
]

#: Words that carry no place information — stripped for `landmarks`, never for the
#: fingerprint. Numbers are deliberately NOT normalised: "three signs" vs "4 signs" is
#: a real difference in what the source claims and must reach a human.
_STOPWORDS = {
    "the", "a", "of", "at", "on", "in", "to", "from", "and", "located", "approx",
    "fishing", "boundary", "sign", "signs", "white", "triangular", "waters", "water",
    "mainstem", "m", "km", "upstream", "downstream", "above", "below", "between",
    "river", "creek", "lake", "bridge", "including", "excluding", "tributaries",
}


def normalize(text: str) -> str:
    """Lowercase, expand abbreviations, drop punctuation, collapse whitespace."""
    t = (text or "").lower().replace("’", "'").replace("–", "-").replace("—", "-")
    for pat, rep in _SYNONYMS:
        t = re.sub(pat, rep, t)
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]+", " ", t)).strip()


def landmarks(text: str) -> List[str]:
    """Content words that name a *place* — what survives a wholesale rewrite.

    Used only to rank rebind candidates for a human. Never to auto-bind.
    """
    return sorted({w for w in normalize(text).split() if w not in _STOPWORDS and len(w) > 2})


def fingerprint(section: Optional[str], water: str, specific_area: str) -> str:
    """The integrity check. Stable under observed drift, sensitive to real change."""
    key = f"{section or ''}|{normalize(water)}|{normalize(specific_area)}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------


@dataclass
class Location:
    """One place a rule can attach to — a named water's reach, or a cascade default."""

    fingerprint: str
    region: str
    region_number: int
    #: water | section_default | area_default | region_default | closure
    kind: str
    precedence: int
    section: Optional[str]
    #: The fallback chain, widest last. Empty for regions without sections.
    inherits: List[str]
    water: str
    aliases: List[str]
    specific_area: str
    #: Decomposition — advisory, for candidate ranking and for seeding a binding.
    op: str
    #: Which KINDS of landmark the scope names — triage only, never used to bind.
    anchor_types: List[str]
    tributaries: Optional[bool]
    excludes: List[str]
    areas: List[int]
    landmarks: List[str]
    #: Spatial qualifiers deliberately NOT modelled as geometry (radius buffers, lake
    #: closure lines). Carried as text; see `spatial_caveat` in the entries file.
    notes: List[str] = field(default_factory=list)
    #: True when a note NARROWS the extent (a radius closure, a lake closure line), so
    #: binding the whole water would read as more open than the source allows.
    narrows_extent: bool = False

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class RuleRecord:
    """A regulation, keyed only by the fingerprint of the location it sits on.

    Rules carry **no identity of their own**. They are replaced wholesale every run,
    which is what makes the volatile half free: no ids, no diffing, no merge.
    """

    fingerprint: str
    species: str
    dates: str
    limits_gear: str
    fishery_notices: List[Dict[str, str]]
    source: str
    order: int

    def to_dict(self) -> dict:
        return asdict(self)


#: Region-level shape. A change here is structural — see `entries.reconcile`.
def structure_signature(u: Untangled) -> dict:
    return {
        "sections": [s.key for s in u.sections] if hasattr(u, "sections") else
                    sorted({d.key for d in u.defaults}),
        "n_locations": len(u.waters) + len(u.defaults),
        "n_waters": len(u.waters),
        "n_defaults": len(u.defaults),
        "precedences": sorted({d.kind for d in u.defaults}),
    }


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------

#: (pattern, kind, narrows_the_extent). Only a note that NARROWS the extent makes the
#: binding over-permissive, and only those set `spatial_caveat`. A gear note ("barbed
#: hooks are authorized") is worth carrying but does not change what water is covered.
_NOTE_PATTERNS = [
    # Bounded at ":" as well as ".", or the radius clause swallows the sentence after
    # it — Babine Lake's list of excluded creeks follows a colon.
    (re.compile(r"within\s+a?\s*\d+\s*m\w*\s+radius[^.:]*", re.I), "radius closure", True),
    (re.compile(r"(?:also\s+)?closed\s+(?:east|west|north|south)\w*\s+of\s+a\s+line[^.]*", re.I), "closure line", True),
    (re.compile(r"Note:\s*[^.]*\.", re.I), "source note", False),
]


def _notes_for(text: str) -> tuple[List[str], bool]:
    """Qualifiers kept as text instead of geometry, and whether any NARROWS the extent.

    Returns `(notes, narrows)`. `narrows` becomes `spatial_caveat`: binding the whole
    water while a note removes part of it is over-permissive, so the app must show the
    note and must not draw the rule as a plain fill.
    """
    out: List[str] = []
    narrows = False
    for pat, _kind, is_spatial in _NOTE_PATTERNS:
        for m in pat.finditer(text or ""):
            frag = re.sub(r"\s+", " ", m.group(0)).strip(" .;,")
            if not frag or any(frag in existing for existing in out):
                continue
            out = [e for e in out if e not in frag]   # drop notes this one subsumes
            out.append(frag)
            narrows = narrows or is_spatial
    return out, narrows


def extract(u: Untangled) -> tuple[List[Location], List[RuleRecord], dict]:
    """Split one untangled region into (locations, rules, structure signature)."""
    locations: List[Location] = []
    rules: List[RuleRecord] = []
    seen: Dict[str, Location] = {}
    order = 0

    def add(loc: Location, rule_rows) -> None:
        nonlocal order
        if loc.fingerprint not in seen:
            seen[loc.fingerprint] = loc
            locations.append(loc)
        for r in rule_rows:
            rules.append(RuleRecord(
                fingerprint=loc.fingerprint,
                species=r.species or "All", dates=r.dates, limits_gear=r.limits_gear,
                fishery_notices=r.fishery_notices, source=r.source, order=order,
            ))
            order += 1

    # Cascade defaults first — Region 6's A / B(i) / Area 5 / F all attach rules.
    kind_map = {"region": "region_default", "section": "section_default",
                "area": "area_default", "closure": "closure"}
    for d in u.defaults:
        op, a_types, _mainstem, tribs = classify_scope(d.scope)
        add(Location(
            fingerprint=fingerprint(d.key, "", d.scope),
            region=u.region, region_number=u.region_number,
            kind=kind_map.get(d.kind, d.kind),
            precedence={"region": 0, "section": 1, "area": 2}.get(d.kind, 1),
            section=d.key, inherits=list(d.falls_back_to),
            water="", aliases=[], specific_area=d.scope,
            op=op, anchor_types=a_types, tributaries=tribs, excludes=[],
            areas=list(d.areas), landmarks=landmarks(d.scope),
            notes=_notes_for(d.scope)[0], narrows_extent=_notes_for(d.scope)[1],
        ), d.rules)

    for w in u.waters:
        for reach in w.reaches:
            op, a_types, _mainstem, tribs = classify_scope(reach.scope)
            add(Location(
                fingerprint=fingerprint(w.section, w.name, reach.scope),
                region=u.region, region_number=u.region_number,
                kind="water", precedence=3,
                section=w.section, inherits=list(w.inherits),
                water=w.name, aliases=list(w.aliases), specific_area=reach.scope,
                op=op, anchor_types=a_types,
                tributaries=reach.tributaries if reach.tributaries is not None else w.tributaries,
                excludes=list(reach.excludes), areas=[],
                landmarks=landmarks(f"{w.name} {reach.scope}"),
                notes=_notes_for(reach.scope)[0] + ([w.name_note] if w.name_note else []),
                narrows_extent=_notes_for(reach.scope)[1],
            ), reach.rules)

    return locations, rules, structure_signature(u)


def emit(slug: str, out_dir: Path, cache_dir: Path = DEFAULT_CACHE) -> tuple[int, int]:
    """Write `locations/region-<slug>.json` and `rules/region-<slug>.json`."""
    parsed = parse_cached(slug, cache_dir)
    u = untangle(parsed)
    locs, rules, sig = extract(u)

    (out_dir / "locations").mkdir(parents=True, exist_ok=True)
    (out_dir / "rules").mkdir(parents=True, exist_ok=True)

    (out_dir / "locations" / f"region-{slug}.json").write_text(json.dumps({
        "region": slug, "region_number": PAGES[slug].region, "region_name": PAGES[slug].name,
        "url": parsed.url, "date_modified": parsed.date_modified,
        "structure": sig,
        "locations": [l.to_dict() for l in locs],
    }, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    (out_dir / "rules" / f"region-{slug}.json").write_text(json.dumps({
        "region": slug, "date_modified": parsed.date_modified,
        "preamble": parsed.preamble,
        "rules": [r.to_dict() for r in rules],
    }, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    return len(locs), len(rules)


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--regions", nargs="+", choices=ALL_SLUGS)
    ap.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE)
    ap.add_argument("--out", type=Path, default=GENERATED.regs.dfo_salmon)
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    todo = args.regions or [s for s in ALL_SLUGS if not PAGES[s].is_stub]
    tl = tr = 0
    for slug in todo:
        try:
            nl, nr = emit(normalize_slug(slug), args.out, args.cache_dir)
        except FileNotFoundError:
            logger.warning("region %s: no snapshot; run pipeline.regs.dfo_salmon.fetch first", slug)
            continue
        tl += nl; tr += nr
        print(f"region {slug:<3} {PAGES[slug].name:<38} locations={nl:<4} rules={nr}")
    print(f"\n{tl} locations, {tr} rules -> {args.out}/{{locations,rules}}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
