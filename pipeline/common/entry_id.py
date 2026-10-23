"""WHAT AN ENTRY ID SAYS — the ONE grammar (T109, owner 2026-10-22: one decision = one function).

An entry id is `<head>:<slug>[@<mu>+<mu>…]`. The head names the book's table the entry is printed in:

    zp            the province's table ("Everywhere in B.C.")                 kind "province"
    z<n>[<zone>]  a region's (or a lettered zone's) table: z4, z7a, z7b         kind "zone"
    r<n>          a row of a region's water table: r7:williston_lake@7-30     kind "water"

Anything else is kind "other" (test fixtures, never the corpus). The KIND is read from the head's
first letter (`zp` first), exactly as the ~70 inline `startswith("z")` / `startswith("r")` /
`startswith("zp:")` tests it replaced read it; the REGION only from a head that is the letter
then `<digits>[<letter>]`, lower case as minted ("7a").

Region 7 is printed as two zones. A zone table's id names its zone (`z7a:`), a water row's does
not (`r7:` — `matcher.region_num` reads "REGION 7A …" as "7", and minting is left so: changing it
moves every Region 7 entry id, a registry → entry resync, T109 follow-up). A water row's MUs say
which zone it was printed in; `parse(eid, registry)` resolves them (`EntryId.regions`, user
ruling 2026-09-24, `reach.outside.ZONES_FROM_UNITS`).

Nothing outside this module tests an entry id's prefix or matches it with a regex
(`pipeline/tests/test_entry_id.py` pins it).
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, Optional

PROVINCE = "province"
ZONE = "zone"
WATER = "water"
OTHER = "other"

_REGION = re.compile(r"\d+[a-z]?")


@dataclass(frozen=True)
class EntryId:
    raw: str
    #: the table head: "zp", "z7a", "r7" (the whole id when it has no ":")
    head: str
    #: "province" | "zone" | "water" | "other"
    kind: str
    #: the region (or zone) the id names, as minted: "4", "7a" — None for the province and "other"
    region: Optional[str]
    #: the slug between the head and any "@"
    slug: str
    #: the MUs a water row's id carries after "@", as written ("7-30", "7-37")
    mus: tuple[str, ...]
    #: a WATER row's regions: its own and those its MUs name, each resolved to its zone when a
    #: registry holds the region as zones and every MU of it resolves (`outside.entry_regions`);
    #: None for any other kind
    regions: Optional[tuple[str, ...]]

    @property
    def book_region(self) -> Optional[str]:
        """The region as the book's water rows name it: Zones 7A and 7B are Region 7's ("7a" -> "7")."""
        return book_region(self.region) if self.region is not None else None


def head_of(entry_id) -> str:
    return str(entry_id).split(":", 1)[0]


def kind_of(entry_id) -> str:
    h = head_of(entry_id)
    if h == "zp":
        return PROVINCE
    if h[:1] == "z":
        return ZONE
    if h[:1] == "r":
        return WATER
    return OTHER


def is_province(entry_id) -> bool:
    """`zp:` — the province's table."""
    return kind_of(entry_id) == PROVINCE


def is_zone(entry_id) -> bool:
    """`z<n>:` — a region's or zone's own table (never `zp:`)."""
    return kind_of(entry_id) == ZONE


def is_table(entry_id) -> bool:
    """A table entry, province or zone (`zp:` or `z<n>:`) — not a water row."""
    return kind_of(entry_id) in (PROVINCE, ZONE)


def is_water_row(entry_id) -> bool:
    """`r<n>:` — a row of a region's water table."""
    return kind_of(entry_id) == WATER


def region_of(entry_id) -> Optional[str]:
    """`r6:…` -> "6", `z7a:…` -> "7a"; None for `zp:` and anything else."""
    h = head_of(entry_id)
    return h[1:] if kind_of(entry_id) in (ZONE, WATER) and _REGION.fullmatch(h[1:]) else None


def zone_region(entry_id) -> Optional[str]:
    """A ZONE table's region: `z3:…` -> "3", `z7a:…` -> "7a"; None for `zp:` and every water row."""
    return region_of(entry_id) if is_zone(entry_id) else None


def book_region(region: str) -> str:
    """A zone's region as the water rows name it: "7a" -> "7"; "4" -> "4"."""
    return region[:-1] if len(region) > 1 and region[-1].isalpha() and region[:-1].isdigit() else region


def mus_of(entry_id) -> tuple[str, ...]:
    s = str(entry_id)
    if "@" not in s:
        return ()
    return tuple(mu.strip() for mu in s.split("@", 1)[1].split("+"))


def parse(entry_id, registry=None) -> EntryId:
    """What the id says. With a `registry`, a water row's MUs are resolved to their zones."""
    s = str(entry_id)
    head = head_of(s)
    kind = kind_of(s)
    rest = s.split(":", 1)[1] if ":" in s else ""
    slug = rest.split("@", 1)[0]
    mus = mus_of(s)
    region = region_of(s)
    regions = None
    if kind == WATER and region is not None and ":" in s:
        regions = _water_regions(region, mus, registry)
    return EntryId(raw=s, head=head, kind=kind, region=region, slug=slug, mus=mus, regions=regions)


def _water_regions(region: str, mus: tuple[str, ...], registry) -> tuple[str, ...]:
    got = {region}
    named: list[str] = []
    for mu in mus:
        h = mu.split("-", 1)[0]
        if h:
            got.add(h)
            named.append(mu)
    if registry is not None:
        from pipeline.atlas.reach import outside
        if outside.ZONES_FROM_UNITS:
            for r in sorted(got):
                zones = outside._zones_of(registry, r)
                mine = [mu for mu in named if mu.split("-", 1)[0] == r]
                if not zones or not mine:
                    continue
                hit = [{z for z, units in zones.items() if mu in units} for mu in mine]
                if all(hit):
                    got.discard(r)
                    got |= set().union(*hit)
    return tuple(sorted(got))


def region_sort_key(region: str):
    """"2" < "7" < "7a" < "7b" < "10": by number, then the zone letter."""
    return int(re.sub(r"\D", "", region) or 0), region


def region_words(regions: Iterable[str]) -> str:
    """The regions as the book names them, in region order: "Region 5", "Region 7A",
    "Regions 3 and 8", "Regions 2, 3 and 8". Every reader that words a region set says it so."""
    r = [x.upper() for x in sorted(regions, key=region_sort_key)]
    if not r:
        raise ValueError("region_words: no regions")
    return ("Region " + r[0]) if len(r) == 1 else "Regions " + ", ".join(r[:-1]) + " and " + r[-1]


def mint_water_row(region: str, slug: str, mus: Iterable[str]) -> str:
    """A water row's id, as `batch_exporter` mints it: `r<region>:<slug>[@<mu>+<mu>…]` with the MUs
    sorted (`region` "" mints `r?:`)."""
    m = "+".join(sorted(mus))
    base = f"r{region or '?'}:{slug}"
    return f"{base}@{m}" if m else base
