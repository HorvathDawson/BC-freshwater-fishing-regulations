"""
in_season_scraper — Scrape in-season regulation changes from BC Gov website.

Fetches the Freshwater Fishing Regulations page and extracts the in-season
correction/change tables from each accordion panel (Provincial + Regions 1–8).

Each table has three columns:
    Water | In-season correction/change | Effective date

Output is a JSON file with one entry per section, each containing its rows.

CLI
---
    python -m pipeline.matching.in_season_scraper
    python -m pipeline.matching.in_season_scraper --out path/to/output.json
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import yaml
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set

import requests
from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)

_SOURCE_URL = (
    "https://www2.gov.bc.ca/gov/content/sports-culture/recreation/"
    "fishing-hunting/fishing/fishing-regulations"
)

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

_REQUEST_TIMEOUT = 30

# Whitespace normalization
_RE_MULTI_SPACE = re.compile(r"\s+")


def _normalize_text(text: str) -> str:
    """Collapse \\xa0, multiple spaces, and strip."""
    return _RE_MULTI_SPACE.sub(" ", text.replace("\xa0", " ")).strip()


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass
class InSeasonChange:
    """A single in-season correction/change row."""

    water: str
    change: str
    effective_date: str
    # Populated by reconcile_with_synopsis
    synopsis_matches: List[str] = field(default_factory=list)
    match_method: str = ""


@dataclass
class RegionChanges:
    """All in-season changes for one section (Provincial or a Region)."""

    section: str
    rows: List[InSeasonChange] = field(default_factory=list)


@dataclass
class ScrapedResult:
    """Complete scrape result with metadata."""

    scraped_at: str
    source_url: str
    sections: List[RegionChanges] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Scraping logic
# ---------------------------------------------------------------------------


def _fetch_page(url: str) -> str:
    """Fetch the raw HTML from the BC Gov fishing regulations page."""
    headers = {"User-Agent": _USER_AGENT}
    resp = requests.get(url, headers=headers, timeout=_REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.text


def _parse_table(table: Tag) -> List[InSeasonChange]:
    """Extract rows from a Water / Change / Effective date table."""
    rows: List[InSeasonChange] = []
    for tr in table.find_all("tr")[1:]:  # skip header row
        cells = tr.find_all("td")
        if len(cells) < 3:
            continue
        water = _normalize_text(cells[0].get_text(separator=" ", strip=True))
        change = _normalize_text(cells[1].get_text(separator=" ", strip=True))
        date = _normalize_text(cells[2].get_text(separator=" ", strip=True))
        # Skip empty placeholder rows
        if not water and not change and not date:
            continue
        rows.append(InSeasonChange(water=water, change=change, effective_date=date))
    return rows


def _extract_section_name(panel: Tag) -> Optional[str]:
    """Get the section heading text from a panel's first child element."""
    for child in panel.children:
        if isinstance(child, Tag):
            text = child.get_text(strip=True)
            if text:
                return text
    return None


def scrape_in_season_changes(url: str = _SOURCE_URL) -> ScrapedResult:
    """Scrape all in-season regulation change tables from the BC Gov page.

    Returns a ScrapedResult with one RegionChanges per accordion section.
    """
    html = _fetch_page(url)
    soup = BeautifulSoup(html, "html.parser")

    container = soup.find("div", class_="accordion-container")
    if not container:
        raise ValueError("Could not find accordion-container on page")

    panels = container.find_all("div", class_="panel", recursive=False)
    logger.info(f"Found {len(panels)} accordion panels.")

    sections: List[RegionChanges] = []
    for panel in panels:
        section_name = _extract_section_name(panel)
        if not section_name:
            continue

        table = panel.find("table")
        if not table:
            logger.debug(f"No table in section: {section_name}")
            continue

        rows = _parse_table(table)
        sections.append(RegionChanges(section=section_name, rows=rows))
        logger.info(f"  {section_name}: {len(rows)} change(s)")

    return ScrapedResult(
        scraped_at=datetime.now(timezone.utc).isoformat(),
        source_url=url,
        sections=sections,
    )


# ---------------------------------------------------------------------------
# Synopsis reconciliation
# ---------------------------------------------------------------------------

# Maps the in-season section heading to the synopsis region string.
_SECTION_TO_REGION: Dict[str, Optional[str]] = {
    "Provincial level information": None,
    "Region 1 - Vancouver Island (includes Haida Gwaii: WMU 6-12, 6-13)": "REGION 1 - Vancouver Island",
    "Region 2 - Lower Mainland": "REGION 2 - Lower Mainland",
    "Region 3 - Thompson": "REGION 3 - Thompson-Nicola",
    "Region 4 - Kootenay": "REGION 4 - Kootenay",
    "Region 5 - Cariboo": "REGION 5 - Cariboo",
    "Region 6 - Skeena": "REGION 6 - Skeena",
    "Region 7A - Omineca": "REGION 7A - Omineca",
    "Region 7B - Peace": "REGION 7B - Peace",
    "Region 8 - Okanagan": "REGION 8 - Okanagan",
}


def _build_match_table_index(
    match_table_path: Path,
) -> Dict[Optional[str], Dict[str, List[str]]]:
    """Build region → {UPPER_NAME: [original_names]} from match_table.json.

    Reads the base entry table produced by base_entry_builder, which
    includes both qualified entries and unqualified base-name entries.
    """
    with open(match_table_path, encoding="utf-8") as f:
        entries = json.load(f)

    # region → upper_name → [original name_verbatim values]
    index: Dict[Optional[str], Dict[str, List[str]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for entry in entries:
        criteria = entry.get("criteria", {})
        name = (criteria.get("name_verbatim") or "").strip()
        if not name:
            continue
        region = criteria.get("region")
        index[region][name.upper()].append(name)

    return dict(index)


def _match_water(
    water: str,
    section: str,
    region_names: Dict[str, List[str]],
    provincial_names: Dict[str, List[str]],
    all_names: Dict[str, List[str]],
) -> tuple[list[str], str]:
    """Try to match an in-season water name against the synopsis index.

    Returns (matched_synopsis_names, match_method).
    Exact matches only — unqualified base names are expected to exist as
    their own entries (created by base_entry_builder).
    """
    water_upper = water.upper()

    # 1. Exact match in region
    if water_upper in region_names:
        return region_names[water_upper], "exact"

    # 2. Exact match in provincial (no-region) entries
    if water_upper in provincial_names:
        return provincial_names[water_upper], "exact_provincial"

    # 3. Exact match in any region
    if water_upper in all_names:
        return all_names[water_upper], "exact_cross_region"

    return [], "not_found"


def reconcile_with_synopsis(
    result: ScrapedResult,
    match_table_path: Path,
) -> None:
    """Match each in-season water name to match table entries, in-place.

    Populates synopsis_matches and match_method on each InSeasonChange row.
    Uses the match table (not raw parsed_results) because it contains
    the unqualified base-name entries created by base_entry_builder.
    """
    index = _build_match_table_index(match_table_path)

    # Flatten all names across all regions for cross-region fallback
    all_names: Dict[str, List[str]] = defaultdict(list)
    for region_dict in index.values():
        for upper_name, originals in region_dict.items():
            all_names[upper_name].extend(originals)
    all_names = dict(all_names)

    provincial_names = index.get(None, {})

    for section_data in result.sections:
        synopsis_region = _SECTION_TO_REGION.get(section_data.section)
        region_names = index.get(synopsis_region, {})

        for row in section_data.rows:
            matches, method = _match_water(
                row.water,
                section_data.section,
                region_names,
                provincial_names,
                all_names,
            )
            row.synopsis_matches = list(dict.fromkeys(matches))
            row.match_method = method


def _print_reconciliation(result: ScrapedResult) -> None:
    """Print synopsis match results."""
    total = sum(len(s.rows) for s in result.sections)
    matched = sum(
        1 for s in result.sections for r in s.rows if r.match_method != "not_found"
    )
    print()
    print("─" * 70)
    print("  SYNOPSIS RECONCILIATION")
    print("─" * 70)
    for section in result.sections:
        for row in section.rows:
            water = row.water[:45].ljust(45)
            if row.match_method == "not_found":
                print(f"  ❌ {water}  not_found")
            elif row.match_method == "exact":
                print(f"  ✅ {water}  exact")
            else:
                print(f"  🔶 {water}  {row.match_method}")
                for m in row.synopsis_matches[:3]:
                    print(f"       → {m}")
    print(f"\n  {matched}/{total} matched")
    print("─" * 70)


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------


def save_result(result: ScrapedResult, path: Path) -> None:
    """Write scraped data to JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(asdict(result), f, indent=2, ensure_ascii=False)
    print(f"Wrote {sum(len(s.rows) for s in result.sections)} changes → {path}")


def _print_summary(result: ScrapedResult) -> None:
    """Print a structured summary to stdout."""
    total = sum(len(s.rows) for s in result.sections)
    print()
    print("=" * 70)
    print("  IN-SEASON REGULATION CHANGES")
    print(f"  Scraped: {result.scraped_at}")
    print("=" * 70)
    for section in result.sections:
        count = len(section.rows)
        print(f"\n  {section.section}  ({count} change{'s' if count != 1 else ''})")
        for row in section.rows:
            water = row.water[:40].ljust(40)
            date = row.effective_date[:25]
            print(f"    {water}  {date}")
            if row.change:
                # Wrap long change text
                change_preview = row.change[:90]
                if len(row.change) > 90:
                    change_preview += "..."
                print(f"      ↳ {change_preview}")
    print()
    print(f"  Total changes: {total}")
    print("=" * 70)
    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape in-season fishing regulation changes from BC Gov."
    )
    parser.add_argument(
        "--out",
        default=None,
        help="Output JSON path (default: from config.yaml)",
    )
    parser.add_argument(
        "--url",
        default=_SOURCE_URL,
        help="Override the source URL",
    )
    parser.add_argument(
        "--match-table",
        default=None,
        help="Path to match_table.json for reconciliation (default: from config.yaml)",
    )
    parser.add_argument("--quiet", action="store_true", help="Suppress summary output")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    # Resolve defaults from config.yaml
    with open(Path(__file__).resolve().parents[2] / "config.yaml") as f:
        cfg = yaml.safe_load(f)
    v2_cfg = cfg["output"]["pipeline"]
    out_path = Path(args.out) if args.out else Path(v2_cfg["in_season"])
    match_table_path = (
        Path(args.match_table) if args.match_table else Path(v2_cfg["match_table"])
    )

    print(f"Fetching: {args.url}")
    result = scrape_in_season_changes(args.url)

    if not args.quiet:
        _print_summary(result)

    # Reconcile against match table if available
    if match_table_path.exists():
        print(f"Reconciling against: {match_table_path}")
        reconcile_with_synopsis(result, match_table_path)
        if not args.quiet:
            _print_reconciliation(result)
    else:
        logger.warning(
            f"Match table not found at {match_table_path} — skipping reconciliation."
        )

    save_result(result, out_path)


if __name__ == "__main__":
    main()
