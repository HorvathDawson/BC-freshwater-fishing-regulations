"""parse — DFO region salmon table -> structured rows, with scope precedence.

The pages are one `<table class="table table-bordered">` with five logical columns:

    Waters | Specific area | Species | Dates | Limits/Gear

Three things make a naive `pandas.read_html` wrong here:

1. **rowspan/colspan everywhere** (Region 6: 104 rowspans on 243 rows). The table is
   expanded into a dense grid first; every emitted row carries all five columns.
2. **Banner rows.** Region 6 (only) uses full-width `<th colspan=5>` rows to open
   lettered *sections* — A, B, B(i), B(ii), C, D, E, F. They are scope declarations,
   not data. One of them is authored `colspan="6"` on a five-column table; the grid
   builder clamps rather than trusting the attribute.
3. **Precedence.** Region 6's own banners say section A applies only where the
   section below does not state otherwise. That makes the table a cascade, not a
   flat list, so every row is emitted with a `precedence` rank (see `RegRow`).

Cell content is not plain text either: exclusion lists arrive as `<ul>`, in-season
amendments as `<a>` to a Fishery Notice (FN####). Both are kept structured — an
exclusion list flattened into a sentence is unresolvable against a reach.

**Do not hand the whole page to a DOM parser.** Measured 2026-08-29: Regions 4 and 7
carry an *unterminated* `<!--` above the table (18 opens / 17 closes). html.parser
therefore swallows the rest of the document and `soup.find("table")` returns None —
silently, with no exception and no empty-table warning. Both regions parsed to zero
rows until the table was sliced out of the raw HTML by regex instead. Any future
rewrite that reintroduces `BeautifulSoup(page).find("table")` re-opens that hole, so
`test_dfo_salmon.py` asserts Region 4 and 7 row counts specifically.

CLI
---
    .venv/bin/python -m pipeline.dfo_salmon.parse --out output/dfo_salmon
    .venv/bin/python -m pipeline.dfo_salmon.parse --regions 6 --print
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

from bs4 import BeautifulSoup, NavigableString, Tag

from pipeline.dfo_salmon.fetch import DEFAULT_CACHE, REGIONS, load_cached

logger = logging.getLogger(__name__)

COLUMNS = ("waters", "specific_area", "species", "dates", "limits_gear")
N_COLS = len(COLUMNS)

_RE_WS = re.compile(r"\s+")
_RE_FN = re.compile(r"\bFN\s?(\d{3,5})\b", re.I)

#: Slice the table out of the raw source; see the module docstring for why the DOM
#: cannot be trusted to still contain it.
_RE_TABLE = re.compile(r"<table\b[^>]*>.*?</table>", re.S | re.I)
_RE_COMMENT = re.compile(r"<!--.*?-->", re.S)
_RE_ORPHAN_COMMENT = re.compile(r"<!--.*$", re.S)
_RE_H1 = re.compile(r"<h1\b[^>]*\bid=[\"\']wb-cont[\"\'][^>]*>", re.I)
_RE_PAGE_TAIL = re.compile(r"<(?:footer|div[^>]*class=[\"\'][^\"\']*pagedetails)", re.I)
_RE_DATE_MOD = re.compile(
    r'<time[^>]*property="dateModified"[^>]*>\s*([0-9]{4}-[0-9]{2}-[0-9]{2})', re.I
)

#: Opens a lettered section: "A. ...", "B(ii). ...", "B. Part (i): ...".
#: The delimiter after the letter is REQUIRED and must be "." or ":" — without it
#: "Colonial River - see Cayeghle River" parses as section C and "Dewdney Slough -
#: See Nicomen Slough" as section D. Both are cross-reference rows, not sections.
_RE_SECTION = re.compile(
    r"^\s*(?P<letter>[A-H])\s*(?:\((?P<roman1>[ivx]+)\))?\s*[.:]\s*"
    r"(?:Part\s*\((?P<roman2>[ivx]+)\)\s*[.:]?)?\s*(?P<title>\S.*)$",
    re.S,
)

#: A row's Waters/Specific-area text that declares a catch-all rather than a water.
_RE_CATCHALL = re.compile(
    r"^\s*(all\s+(waters|lakes|streams)|all\s+region\b|all\s+other\b)", re.I
)

#: "Dewdney Slough - See Nicomen Slough" / "Colonial River - see Cayeghle River"
_RE_SEE_ALSO = re.compile(r"^(?P<name>.+?)\s*[-–—]?\s*see\s+(?P<target>.+?)\s*$", re.I)


def _norm(text: str) -> str:
    return _RE_WS.sub(" ", text.replace("\xa0", " ")).strip()


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------


@dataclass
class Cell:
    """One grid cell, kept structured — bullets and links are load-bearing."""

    text: str = ""
    bullets: List[str] = field(default_factory=list)
    links: List[Dict[str, str]] = field(default_factory=list)

    @property
    def empty(self) -> bool:
        return not self.text and not self.bullets and not self.links


@dataclass
class Section:
    """A lettered scope band. Region 6 is the only region that uses these."""

    key: str            # "A", "B", "B(i)", "B(ii)", "C", ...
    letter: str         # "A", "B", ...
    part: Optional[str]  # "i", "ii", or None
    title: str          # full banner text, verbatim
    #: True when the banner itself says section A fills the gaps.
    falls_back_to_a: bool = False


@dataclass
class RegRow:
    """One resolved regulation row.

    `precedence` is how a lookup picks a winner for (water, species, date):

        2  named water        — a specific waterbody/stream row inside a section
        1  section catch-all  — "All waters in section B(i) ... unless otherwise stated below"
        0  region default     — Region 6 section A, "All Region 6 waters"

    Highest precedence with a matching species and date window wins; ties inside a
    rank are the source's own ordering. Regions 1-5, 7, 8 have no sections, so every
    row there is rank 2 and the cascade is a no-op.
    """

    region: int
    region_name: str
    row_index: int
    section_key: Optional[str]
    section_title: Optional[str]
    waters: str
    waters_bullets: List[str]
    specific_area: str
    specific_area_bullets: List[str]
    species: str
    dates: str
    limits_gear: str
    precedence: int
    #: FN#### fishery notices cited by this row — an in-season variation order.
    fishery_notices: List[Dict[str, str]] = field(default_factory=list)
    #: "Dewdney Slough — see Nicomen Slough": a pointer row, not a rule.
    see_also: Optional[str] = None
    #: Derived, best-effort flags. The source text is always kept verbatim above.
    no_fishing: bool = False
    non_retention: bool = False
    hatchery_marked_only: bool = False
    bait_ban: bool = False
    single_barbless_hook: bool = False
    daily_limit: Optional[int] = None

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class ParsedRegion:
    region: int
    region_name: str
    url: str
    date_modified: Optional[str]
    preamble: List[str]
    sections: List[Section]
    rows: List[RegRow]
    notes: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "region": self.region,
            "region_name": self.region_name,
            "url": self.url,
            "date_modified": self.date_modified,
            "preamble": self.preamble,
            "sections": [asdict(s) for s in self.sections],
            "rows": [r.to_dict() for r in self.rows],
            "notes": self.notes,
        }


# ---------------------------------------------------------------------------
# Cell extraction
# ---------------------------------------------------------------------------


def _extract_cell(td: Tag) -> Cell:
    """Pull text, `<li>` bullets and `<a>` links out of one cell.

    Bullets are removed from the flat text so an exclusion list does not read as a
    run-on sentence; they stay available as a list.
    """
    clone = BeautifulSoup(str(td), "html.parser")
    bullets: List[str] = []
    for li in clone.find_all("li"):
        t = _norm(li.get_text(" ", strip=True))
        if t:
            bullets.append(t)
    links = [
        {"text": _norm(a.get_text(" ", strip=True)), "href": a.get("href", "")}
        for a in clone.find_all("a")
        if a.get("href")
    ]
    for ul in clone.find_all(["ul", "ol"]):
        ul.decompose()
    for br in clone.find_all("br"):
        br.replace_with(NavigableString(" — "))
    text = _norm(clone.get_text(" ", strip=True))
    text = re.sub(r"(\s*—\s*)+$", "", text).strip()
    return Cell(text=text, bullets=bullets, links=links)


# ---------------------------------------------------------------------------
# Grid expansion
# ---------------------------------------------------------------------------


def _expand_grid(table: Tag) -> List[List[Optional[Cell]]]:
    """Expand `<tr>`s into a dense rectangular grid, honouring rowspan/colspan.

    Returns rows of exactly `N_COLS` cells; a cell repeated by a rowspan appears in
    every row it covers, so each output row is self-contained.

    A banner row (one cell spanning the full width) is returned as a single-element
    row so the caller can tell it apart from data. `colspan` is clamped to the real
    width because the source authors it wrong (Region 6 has a `colspan="6"`).
    """
    grid: List[List[Optional[Cell]]] = []
    # pending[col] = (cell, rows_remaining)
    pending: Dict[int, Tuple[Cell, int]] = {}

    body = table.find("tbody") or table
    for tr in body.find_all("tr", recursive=False):
        cells = tr.find_all(["td", "th"], recursive=False)
        if not cells:
            continue

        # A banner is a lone cell claiming (at least) the whole width, with no
        # rowspan carried in from above.
        if len(cells) == 1 and not pending:
            span = int(cells[0].get("colspan", 1) or 1)
            if span >= N_COLS - 1:
                grid.append([_extract_cell(cells[0])])
                continue

        row: List[Optional[Cell]] = [None] * N_COLS
        still: Dict[int, Tuple[Cell, int]] = {}
        for col, (cell, left) in pending.items():
            if col < N_COLS:
                row[col] = cell
            if left - 1 > 0:
                still[col] = (cell, left - 1)
        pending = still

        col = 0
        for td in cells:
            while col < N_COLS and row[col] is not None:
                col += 1
            if col >= N_COLS:
                break
            colspan = max(1, min(int(td.get("colspan", 1) or 1), N_COLS - col))
            rowspan = max(1, int(td.get("rowspan", 1) or 1))
            value = _extract_cell(td)
            for k in range(colspan):
                row[col + k] = value
                if rowspan > 1:
                    pending[col + k] = (value, rowspan - 1)
            col += colspan

        grid.append([c if c is not None else Cell() for c in row])

    return grid


# ---------------------------------------------------------------------------
# Derived flags
# ---------------------------------------------------------------------------


_RE_DAILY = re.compile(r"\b(\d+)\s+(?:hatchery[- ]marked\s+)?per\s+day\b", re.I)


def _flags(limits: str) -> dict:
    low = limits.lower()
    daily = None
    m = _RE_DAILY.search(limits)
    if m:
        daily = int(m.group(1))
    elif re.search(r"\bnon[- ]retention\b|\bno fishing\b", low):
        daily = 0
    return {
        "no_fishing": "no fishing" in low,
        "non_retention": bool(re.search(r"\bnon[- ]retention\b", low)),
        "hatchery_marked_only": "hatchery marked" in low or "hatchery-marked" in low,
        "bait_ban": "bait ban" in low or "no natural bait" in low,
        "single_barbless_hook": "single barbless hook" in low,
        "daily_limit": daily,
    }


def _parse_section(text: str) -> Optional[Section]:
    m = _RE_SECTION.match(text)
    if not m:
        return None
    letter = m.group("letter").upper()
    part = (m.group("roman1") or m.group("roman2") or "") or None
    if part:
        part = part.lower()
    key = f"{letter}({part})" if part else letter
    return Section(
        key=key,
        letter=letter,
        part=part,
        title=_norm(text),
        falls_back_to_a=bool(re.search(r'section\s*"?A"?\s*applies', text, re.I)),
    )


# ---------------------------------------------------------------------------
# Region parse
# ---------------------------------------------------------------------------


def parse_region(
    html: str,
    region: int,
    *,
    url: str = "",
) -> ParsedRegion:
    """Parse one region page into sections + precedence-ranked rows."""
    soup = BeautifulSoup(html, "html.parser")
    main = soup.find("main") or soup

    tm = main.find("time", property="dateModified")
    date_modified = _norm(tm.get_text()) if tm else None

    table = main.find("table")

    # Preamble: the prose above the table (limits, size definitions, closures).
    preamble: List[str] = []
    for node in main.find_all(["p", "li"]):
        if table is not None and table in node.parents:
            continue
        t = _norm(node.get_text(" ", strip=True))
        if t and len(t) > 2 and t not in preamble:
            preamble.append(t)

    sections: List[Section] = []
    rows: List[RegRow] = []
    notes: List[str] = []

    if table is None:
        return ParsedRegion(region, REGIONS.get(region, "?"), url, date_modified,
                            preamble, sections, rows,
                            notes=["no regulation table published on this page"])

    current: Optional[Section] = None
    idx = 0

    for grid_row in _expand_grid(table):
        # Banner: either a section header or a free-standing note.
        if len(grid_row) == 1:
            text = grid_row[0].text
            if not text:
                continue
            sec = _parse_section(text)
            if sec:
                # "B. Part (i)" refines "B"; keep both, the part becomes current.
                sections.append(sec)
                current = sec
            else:
                notes.append(text)
            continue

        waters, area, species, dates, limits = grid_row

        # Species/dates/limits all blank => the row carries no rule.
        if not (species.text or dates.text or limits.text):
            joined = " ".join(c.text for c in grid_row if c.text)
            if joined:
                notes.append(joined)
            continue

        see = None
        m = _RE_SEE_ALSO.match(waters.text)
        if m and not species.text:
            see = _norm(m.group("target"))

        scope_text = f"{waters.text} {area.text}"
        if current and current.letter == "A":
            precedence = 0
        elif _RE_CATCHALL.match(waters.text or "") or (
            not waters.text and _RE_CATCHALL.match(area.text or "")
        ):
            precedence = 1
        else:
            precedence = 2

        fns: List[Dict[str, str]] = []
        for cell in (limits, dates, area):
            for link in cell.links:
                if _RE_FN.search(link["text"]) or "notices.dfo-mpo.gc.ca" in link["href"]:
                    fns.append(link)

        rows.append(
            RegRow(
                region=region,
                region_name=REGIONS.get(region, "?"),
                row_index=idx,
                section_key=current.key if current else None,
                section_title=current.title if current else None,
                waters=waters.text,
                waters_bullets=waters.bullets,
                specific_area=area.text,
                specific_area_bullets=area.bullets,
                species=species.text,
                dates=dates.text,
                limits_gear=limits.text,
                precedence=precedence,
                fishery_notices=fns,
                see_also=see,
                **_flags(limits.text),
            )
        )
        idx += 1

    return ParsedRegion(region, REGIONS.get(region, "?"), url, date_modified,
                        preamble, sections, rows, notes)


def parse_cached(region: int, cache_dir: Path = DEFAULT_CACHE) -> ParsedRegion:
    from pipeline.dfo_salmon.fetch import _BASE

    return parse_region(load_cached(region, cache_dir), region, url=_BASE.format(n=region))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--regions", type=int, nargs="+", choices=sorted(REGIONS), help="default: all cached")
    ap.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE)
    ap.add_argument("--out", type=Path, default=Path("output/dfo_salmon"))
    ap.add_argument("--print", dest="do_print", action="store_true", help="print rows instead of writing")
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    todo = args.regions or sorted(REGIONS)
    args.out.mkdir(parents=True, exist_ok=True)

    all_rows = 0
    for region in todo:
        try:
            parsed = parse_cached(region, args.cache_dir)
        except FileNotFoundError:
            logger.warning("region %d: no snapshot; run pipeline.dfo_salmon.fetch first", region)
            continue
        all_rows += len(parsed.rows)
        print(
            f"region {region:<2} {parsed.region_name:<16} "
            f"rows={len(parsed.rows):<4} sections={len(parsed.sections):<3} "
            f"notes={len(parsed.notes):<3} mod={parsed.date_modified}"
        )
        if args.do_print:
            for r in parsed.rows:
                sec = f"[{r.section_key}] " if r.section_key else ""
                print(f"  p{r.precedence} {sec}{r.waters} | {r.specific_area[:60]} | "
                      f"{r.species} | {r.dates} | {r.limits_gear}")
        else:
            dest = args.out / f"region{region}.json"
            dest.write_text(json.dumps(parsed.to_dict(), indent=2, ensure_ascii=False) + "\n",
                            encoding="utf-8")

    if not args.do_print:
        print(f"\n{all_rows} rows -> {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
