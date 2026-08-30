"""untangle — turn the flat DFO rows into waters → reaches → rules, plus the defaults.

`parse.py` gives a faithful transcription of the table. That is the right thing to
store, and the wrong thing to read: Region 6's 236 rows are one river repeated across
eight reaches and four scope widths. This module regroups them into the shape a map
actually needs:

    defaults        what applies when nothing more specific matches
      region        section A — the Region 6 baseline
      sections      B(i), B(ii), C, D, E catch-alls, and their Area-scoped sub-defaults
      closures      banner-only rules (section F closes the Fraser portion outright)

    waters          one entry per named waterbody, per section
      reaches       the distinct `specific_area` scopes on that water, in source order
        rules       species × dates × limits that bind on that reach
      inherits      the fallback chain for anything the reaches do not cover

Nothing is dropped and nothing is merged: every rule row in `parse.py` output appears
exactly once here, and `verify()` asserts that.

CLI
---
    .venv/bin/python -m pipeline.dfo_salmon.untangle --regions 6 --print
    .venv/bin/python -m pipeline.dfo_salmon.untangle --out output/dfo_salmon/untangled
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from pipeline.dfo_salmon.fetch import ALL_SLUGS, PAGES, normalize_slug
from pipeline.dfo_salmon.parse import ParsedRegion, RegRow, parse_cached

logger = logging.getLogger(__name__)

_RE_WS = re.compile(r"\s+")

#: A "Note:" tail glued onto a water name — three Region 6 names carry one.
_RE_NAME_NOTE = re.compile(r"\s*(?:—\s*)?\bNote:\s*(?P<note>.+)$", re.S)

#: "(including tributaries)" / "(excluding tributaries)" inside a name or scope.
_RE_TRIBS = re.compile(r"\(?\b(?P<verb>in|ex)cluding\s+t\w*ributaries\b\)?", re.I)

#: Parenthetical or bracketed alias: "Zymoetz (Copper) River",
#: "Zymagotitz River [also known as Zymachord River]", "Suskwa (Bear) River".
_RE_ALIAS_BRACKET = re.compile(r"\[\s*(?:also known as\s*)?(?P<alias>[^\]]+?)\s*\]", re.I)
_RE_ALIAS_PAREN = re.compile(r"\(\s*(?P<alias>[A-Z][\w' ]*?)\s*\)")

#: A parenthetical that is a *scope*, not an alias — Tatshenshini splits this way.
_RE_PAREN_SCOPE = re.compile(
    r"\(\s*(?P<scope>(?:up|down)stream[^)]*|above[^)]*|below[^)]*)\)", re.I
)

#: Directional boundary language. The captured phrase is the anchor.
_ANCHOR_PATTERNS: List[Tuple[str, re.Pattern]] = [
    ("upstream_of", re.compile(r"\b(?:upstream|above)\s+(?:of\s+)?(?P<a>.+?)(?=[.;]|$|\s+to\s)", re.I)),
    ("downstream_of", re.compile(r"\b(?:downstream|below)\s+(?:of\s+)?(?P<a>.+?)(?=[.;]|$|\s+to\s)", re.I)),
    ("from", re.compile(r"\bfrom\s+(?P<a>.+?)(?=\s+(?:to|upstream|downstream)\b|[.;]|$)", re.I)),
    ("to", re.compile(r"\bto\s+(?P<a>.+?)(?=[.;]|$)", re.I)),
]


def _norm(t: str) -> str:
    return _RE_WS.sub(" ", (t or "").replace("\xa0", " ")).strip()


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------


@dataclass
class Rule:
    """One species × dates × limits binding, stripped of its scope."""

    species: str
    dates: str
    limits_gear: str
    no_fishing: bool = False
    non_retention: bool = False
    hatchery_marked_only: bool = False
    bait_ban: bool = False
    single_barbless_hook: bool = False
    daily_limit: Optional[int] = None
    fishery_notices: List[Dict[str, str]] = field(default_factory=list)
    source: str = "table"
    row_index: int = -1

    @classmethod
    def from_row(cls, r: RegRow) -> "Rule":
        return cls(
            species=r.species or "All",
            dates=r.dates,
            limits_gear=r.limits_gear,
            no_fishing=r.no_fishing,
            non_retention=r.non_retention,
            hatchery_marked_only=r.hatchery_marked_only,
            bait_ban=r.bait_ban,
            single_barbless_hook=r.single_barbless_hook,
            daily_limit=r.daily_limit,
            fishery_notices=r.fishery_notices,
            source=r.source,
            row_index=r.row_index,
        )


@dataclass
class Reach:
    """One spatial scope on a water — what `specific_area` is describing.

    `anchors` are the raw boundary phrases ("the CNR Railway Bridge at Terrace",
    "fishing boundary signs below lower canyon"). They are deliberately kept verbatim:
    turning them into geometry is `pipeline/splits/anchors.py`'s job, and a
    half-normalised anchor is worse than the sentence it came from.
    """

    index: int
    scope: str
    kind: str
    anchors: List[Dict[str, str]] = field(default_factory=list)
    excludes: List[str] = field(default_factory=list)
    mainstem_only: bool = False
    #: True / False / None — None means "not stated, inherit the water's own setting".
    tributaries: Optional[bool] = None
    rules: List[Rule] = field(default_factory=list)


@dataclass
class Water:
    """A named waterbody within one section."""

    name: str
    region: str
    section: Optional[str]
    aliases: List[str] = field(default_factory=list)
    name_note: Optional[str] = None
    tributaries: Optional[bool] = None
    reaches: List[Reach] = field(default_factory=list)
    #: Fallback chain, widest last. Anything the reaches do not cover falls through it.
    inherits: List[str] = field(default_factory=list)


@dataclass
class ScopeDefault:
    """A catch-all: the region baseline, a section default, or an Area default."""

    key: str
    kind: str            # "region" | "section" | "area" | "closure"
    scope: str
    areas: List[int] = field(default_factory=list)
    falls_back_to: List[str] = field(default_factory=list)
    rules: List[Rule] = field(default_factory=list)


@dataclass
class Untangled:
    region: str
    region_number: int
    region_name: str
    url: str
    date_modified: Optional[str]
    preamble: List[str]
    defaults: List[ScopeDefault]
    waters: List[Water]
    cross_references: List[Dict[str, str]] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "region": self.region,
            "region_number": self.region_number,
            "region_name": self.region_name,
            "url": self.url,
            "date_modified": self.date_modified,
            "preamble": self.preamble,
            "defaults": [asdict(d) for d in self.defaults],
            "waters": [asdict(w) for w in self.waters],
            "cross_references": self.cross_references,
        }


# ---------------------------------------------------------------------------
# Name and scope analysis
# ---------------------------------------------------------------------------


def split_name(raw: str) -> Tuple[str, List[str], Optional[str], Optional[bool], Optional[str]]:
    """Break a Waters cell into (name, aliases, note, tributaries, embedded_scope).

    The source glues four different things into that one cell:
        "Zymoetz (Copper) River — Note: The section of river from Hwy 16 bridge ..."
        "Zymagotitz River [also known as Zymachord River] (including tributaries)"
        "Kitsumkalum River (including tpinributaries) Note: The mouth is ..."   <- sic
        "Tatshenshini River (downstream of the BC/Yukon border)"                <- a scope!
    """
    text = _norm(raw)
    note = None
    m = _RE_NAME_NOTE.search(text)
    if m:
        note = _norm(m.group("note"))
        text = text[: m.start()].strip()

    tribs: Optional[bool] = None
    m = _RE_TRIBS.search(text)
    if m:
        tribs = m.group("verb").lower() == "in"
        text = (text[: m.start()] + " " + text[m.end():]).strip()

    embedded_scope = None
    m = _RE_PAREN_SCOPE.search(text)
    if m:
        embedded_scope = _norm(m.group("scope"))
        text = (text[: m.start()] + " " + text[m.end():]).strip()

    aliases: List[str] = []
    m = _RE_ALIAS_BRACKET.search(text)
    if m:
        aliases.append(_norm(m.group("alias")))
        text = (text[: m.start()] + " " + text[m.end():]).strip()

    # "Zymoetz (Copper) River" -> name "Zymoetz River", alias "Copper River"
    m = _RE_ALIAS_PAREN.search(text)
    if m:
        alias_word = _norm(m.group("alias"))
        rest = _norm(text[: m.start()] + " " + text[m.end():])
        head = _norm(text[: m.start()])
        tail = _norm(text[m.end():])
        aliases.append(_norm(f"{alias_word} {tail}") if tail else alias_word)
        text = rest
        del head

    # "Tseax R." must not lose its period to the strip below and become "Tseax R".
    text = re.sub(r"\bR\.(?=\s|$)", "River", text)
    text = re.sub(r"\b(?:Ck|Cr)\.(?=\s|$)", "Creek", text)
    text = _norm(text.rstrip(" .,-–—"))
    return text, aliases, note, tribs, embedded_scope


def classify_scope(scope: str) -> Tuple[str, List[Dict[str, str]], bool, Optional[bool]]:
    """Return (kind, anchors, mainstem_only, tributaries) for a `specific_area`."""
    text = _norm(scope)
    low = text.lower()

    tribs: Optional[bool] = None
    m = _RE_TRIBS.search(text)
    if m:
        tribs = m.group("verb").lower() == "in"

    mainstem = "mainstem" in low

    if not text or low in {"all", "all waters"}:
        return "whole_water", [], mainstem, tribs

    anchors: List[Dict[str, str]] = []
    for kind, pat in _ANCHOR_PATTERNS:
        for am in pat.finditer(text):
            phrase = _RE_TRIBS.sub(" ", _norm(am.group("a")))
            phrase = _norm(phrase).rstrip(" .,;")
            if phrase and len(phrase) > 2:
                anchors.append({"relation": kind, "phrase": phrase})

    if re.search(r"\ball\s+tributaries\b", low):
        kind = "tributary_set"
    elif re.search(r"\bwaters within\b.*\bsigns?\b", low):
        # "waters within the four white triangular fishing boundary signs at the
        # confluence" is a closure polygon, not an upstream/downstream cut.
        kind = "sign_bounded_zone"
    elif "excluding" in low and not re.search(r"^\s*excluding\s+tributaries\s*$", low):
        # Babine Lake: the whole lake minus its tributaries and a set of creek mouths.
        # The "from X to Y" inside is the closure line, not the reach boundary.
        kind = "whole_water_excluding"
    elif any(a["relation"] == "from" for a in anchors) and any(a["relation"] == "to" for a in anchors):
        kind = "between"
    elif any(a["relation"] == "upstream_of" for a in anchors):
        kind = "upstream_of"
    elif any(a["relation"] == "downstream_of" for a in anchors):
        kind = "downstream_of"
    elif tribs is not None and not anchors:
        kind = "tributaries_only"
    elif re.match(r"^[A-Z][\w' ]+(?:Creek|River|Lake)\b", text):
        kind = "named_tributaries"
    else:
        kind = "described"

    return kind, anchors, mainstem, tribs


# ---------------------------------------------------------------------------
# Untangle
# ---------------------------------------------------------------------------


def _inheritance_chain(section_key: Optional[str], parsed: ParsedRegion) -> List[str]:
    """Widest-last fallback chain for a water in `section_key`.

    Section A is appended only where the section's own banner says it applies —
    B, C, D and E say so in as many words; B(i)/B(ii) inherit it through B.
    """
    if not section_key:
        return []
    chain = [section_key]
    parent = section_key.split("(")[0]
    if parent != section_key:
        chain.append(parent)
    by_key = {s.key: s for s in parsed.sections}
    if any(by_key.get(k) and by_key[k].falls_back_to_a for k in chain):
        if "A" in by_key:
            chain.append("A")
    return chain


def untangle(parsed: ParsedRegion) -> Untangled:
    """Regroup one parsed region into defaults + waters → reaches → rules."""
    defaults: List[ScopeDefault] = []

    # rank 0: the region baseline (section A).
    region_rows = [r for r in parsed.rows if r.precedence == 0]
    if region_rows:
        defaults.append(ScopeDefault(
            key=region_rows[0].section_key or "region",
            kind="region",
            scope=_norm(region_rows[0].waters),
            rules=[Rule.from_row(r) for r in region_rows],
        ))

    # ranks 1 and 2: section and Area catch-alls, keyed by their own scope text so a
    # section with several (section E has four) stays several.
    catchalls: Dict[Tuple, List[RegRow]] = {}
    for r in parsed.rows:
        if r.precedence not in (1, 2):
            continue
        catchalls.setdefault(
            (r.section_key, r.precedence, _norm(r.waters or r.specific_area), tuple(r.areas)), []
        ).append(r)

    for (sec, rank, scope, areas), rows in catchalls.items():
        banner_only = all(x.source == "section_banner" for x in rows)
        chain = _inheritance_chain(sec, parsed)
        # An Area default sits *inside* its section, so it falls back through the
        # section catch-all first; a section default falls past itself.
        defaults.append(ScopeDefault(
            key=sec or "region",
            kind="closure" if banner_only else ("area" if rank == 2 else "section"),
            scope=scope,
            areas=list(areas),
            falls_back_to=chain if rank == 2 else chain[1:],
            rules=[Rule.from_row(r) for r in rows],
        ))

    # rank 3: named waters. Key on (section, raw name) — the Skeena appears in both
    # B(i) and B(ii) because the section boundary *is* a cut in the river.
    grouped: Dict[Tuple[Optional[str], str], List[Tuple[RegRow, tuple]]] = {}
    for r in parsed.rows:
        if r.precedence != 3:
            continue
        parts = split_name(r.waters)
        # Key on the CLEANED name: "Tatshenshini River (upstream of the BC/Yukon
        # border)" and "(downstream of ...)" are one river with two reaches, not two
        # rivers. The parenthetical scope moves down onto the reach.
        grouped.setdefault((r.section_key, parts[0]), []).append((r, parts))

    waters: List[Water] = []
    for (sec, name), entries in grouped.items():
        rows = [r for r, _ in entries]
        _, aliases, note, name_tribs, _ = entries[0][1]
        for _, parts in entries[1:]:
            for a in parts[1]:
                if a not in aliases:
                    aliases.append(a)
            note = note or parts[2]
            name_tribs = name_tribs if name_tribs is not None else parts[3]
        water = Water(
            name=name,
            region=parsed.region,
            section=sec,
            aliases=aliases,
            name_note=note,
            tributaries=name_tribs,
            inherits=_inheritance_chain(sec, parsed),
        )

        by_scope: Dict[str, Reach] = {}
        for r, parts in entries:
            embedded_scope = parts[4]
            scope = _norm(r.specific_area)
            # A scope carried in the name ("Tatshenshini River (downstream of the
            # BC/Yukon border)") applies to every reach beneath it.
            full = _norm(f"{embedded_scope}; {scope}") if embedded_scope and scope else (
                scope or embedded_scope or "")
            if full not in by_scope:
                kind, anchors, mainstem, tribs = classify_scope(full)
                by_scope[full] = Reach(
                    index=len(by_scope),
                    scope=full,
                    kind=kind,
                    anchors=anchors,
                    excludes=list(r.specific_area_bullets),
                    mainstem_only=mainstem,
                    tributaries=tribs if tribs is not None else name_tribs,
                )
            reach = by_scope[full]
            for b in r.specific_area_bullets:
                if b not in reach.excludes:
                    reach.excludes.append(b)
            reach.rules.append(Rule.from_row(r))

        water.reaches = list(by_scope.values())
        waters.append(water)

    waters.sort(key=lambda w: (str(w.section), w.name))

    return Untangled(
        region=parsed.region,
        region_number=parsed.region_number,
        region_name=parsed.region_name,
        url=parsed.url,
        date_modified=parsed.date_modified,
        preamble=parsed.preamble,
        defaults=defaults,
        waters=waters,
        cross_references=parsed.cross_references,
    )


def verify(parsed: ParsedRegion, out: Untangled) -> None:
    """Every source row must land in exactly one rule. Raises AssertionError if not."""
    seen: List[int] = []
    for d in out.defaults:
        seen += [r.row_index for r in d.rules]
    for w in out.waters:
        for reach in w.reaches:
            seen += [r.row_index for r in reach.rules]
    expected = sorted(r.row_index for r in parsed.rows)
    got = sorted(seen)
    assert got == expected, (
        f"region {parsed.region}: {len(got)} rules from {len(expected)} rows; "
        f"missing={sorted(set(expected) - set(got))[:8]} "
        f"duplicated={sorted({i for i in got if got.count(i) > 1})[:8]}"
    )


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def render(out: Untangled) -> str:
    """A readable text view — the point is to be able to check it by eye."""
    L: List[str] = []
    L.append(f"REGION {out.region} — {out.region_name}   (modified {out.date_modified})")
    L.append(f"{out.url}")
    L.append("")

    L.append("DEFAULTS  (apply where nothing more specific below matches)")
    L.append("=" * 78)
    order = {"region": 0, "section": 1, "area": 2, "closure": 3}
    for d in sorted(out.defaults, key=lambda x: (x.key, order.get(x.kind, 9))):
        head = f"[{d.key}] {d.kind}"
        if d.areas:
            head += f"  areas={d.areas}"
        L.append(head)
        L.append(f"    scope: {d.scope}")
        if d.falls_back_to:
            L.append(f"    then falls back to: {' -> '.join(d.falls_back_to)}")
        for r in d.rules:
            tag = "  [from banner]" if r.source != "table" else ""
            L.append(f"      - {r.species:<22} {r.dates:<22} {r.limits_gear}{tag}")
        L.append("")

    L.append("")
    L.append(f"WATERS  ({len(out.waters)})")
    L.append("=" * 78)
    current_section = object()
    for w in out.waters:
        if w.section != current_section:
            current_section = w.section
            L.append("")
            L.append(f"--- section {w.section} " + "-" * 60)
        title = w.name
        if w.aliases:
            title += f"   (aka {', '.join(w.aliases)})"
        if w.tributaries is True:
            title += "   +tributaries"
        elif w.tributaries is False:
            title += "   -tributaries"
        L.append("")
        L.append(f"{title}")
        if w.inherits:
            L.append(f"    inherits: {' -> '.join(w.inherits)}")
        if w.name_note:
            L.append(f"    note: {w.name_note}")
        for reach in w.reaches:
            label = reach.scope or "(whole water)"
            L.append(f"    · reach {reach.index} [{reach.kind}] {label}")
            for a in reach.anchors:
                L.append(f"        anchor {a['relation']}: {a['phrase']}")
            if reach.excludes:
                L.append(f"        excludes: {', '.join(reach.excludes)}")
            for r in reach.rules:
                extra = [x["text"] for x in r.fishery_notices if x["text"] not in r.limits_gear]
                fn = "  " + ",".join(extra) if extra else ""
                L.append(f"        {r.species:<22} {r.dates:<22} {r.limits_gear}{fn}")
    return "\n".join(L)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--regions", nargs="+", choices=ALL_SLUGS, help="default: all real pages")
    ap.add_argument("--cache-dir", type=Path, default=Path("cache/dfo_salmon"))
    ap.add_argument("--out", type=Path, default=Path("output/dfo_salmon/untangled"))
    ap.add_argument("--print", dest="do_print", action="store_true", help="render to stdout instead")
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    todo = args.regions or [s for s in ALL_SLUGS if not PAGES[s].is_stub]
    if not args.do_print:
        args.out.mkdir(parents=True, exist_ok=True)

    for slug in todo:
        try:
            parsed = parse_cached(slug, args.cache_dir)
        except FileNotFoundError:
            logger.warning("region %s: no snapshot; run pipeline.dfo_salmon.fetch first", slug)
            continue
        out = untangle(parsed)
        verify(parsed, out)

        if args.do_print:
            print(render(out))
            print()
        else:
            (args.out / f"region{slug}.json").write_text(
                json.dumps(out.to_dict(), indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
            (args.out / f"region{slug}.txt").write_text(render(out) + "\n", encoding="utf-8")
            reaches = sum(len(w.reaches) for w in out.waters)
            rules = sum(len(r.rules) for w in out.waters for r in w.reaches)
            print(f"region {slug:<3} {out.region_name:<38} "
                  f"defaults={len(out.defaults):<3} waters={len(out.waters):<4} "
                  f"reaches={reaches:<4} rules={rules}")

    if not args.do_print:
        print(f"\n-> {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
