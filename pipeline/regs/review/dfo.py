"""THE DFO SALMON EXTENTS as review targets (T164): one target per scraped DFO location.

    data/curated/regulations/entries/dfo_salmon/region-*.json   the curated geography (read only)
    <generated>/regs/dfo_salmon/typed/region-*.json             the re-scraped rules (read only)

A DFO location is a named water's reach as the DFO notice words it ("from the Highway 16 bridge
downstream to the confluence with the Bulkley"), with the curated EXTENT it binds (ops + split ids;
`entries.Binding`). The review marks it like an entry (`marks.Mark`, scope `dfo`, target the
`location_id`) and on the same store: a verdict, an extent fix, and NEW SPLIT proposals
(`marks.NewSplit`) for the cuts the notice names that do not exist yet. Nothing here writes: a
proposal becomes a reviewed change through T185.

THE PRINTS a DFO mark is held against (memory dfo-salmon-churn-asymmetry: curate the geography once,
re-scrape the rules every run):

    text      the notice's own words for the place (water + specific area) — T
    meaning   the scraped table rows riding on the location — R goes stale on a re-scrape that
              changes them
    binding   the curated geography: the water's registry items + the location's extent — P goes
              stale only when the curated geography moves, never on a re-scrape
"""
from __future__ import annotations

import hashlib
import json
from functools import lru_cache
from pathlib import Path
from typing import Mapping, Optional

#: How a DFO locator's `op` becomes an extent, and how many cut-points it needs
#: (`splitwork._OP_ARITY`, the one table — imported lazily there; repeated here only as the fallback
#: when that module cannot import).
def op_arity() -> dict[str, tuple[str, int]]:
    try:
        from pipeline.regs.dfo_salmon.splitwork import _OP_ARITY
        return dict(_OP_ARITY)
    except Exception:  # noqa: BLE001 — the review must open without the DFO toolchain
        return {"between": ("between", 2), "upstream_of": ("upstream_of", 1),
                "downstream_of": ("downstream_of", 1), "whole_water": ("whole", 0),
                "tributaries_only": ("whole", 0), "whole_water_excluding": ("between", 2)}


def default_dir() -> Path:
    from pipeline.common.curated import CURATED
    return Path(CURATED.regulations.entries.dfo_salmon)


def default_typed_dir() -> Path:
    from pipeline.common.curated import GENERATED
    return Path(GENERATED.regs.dfo_salmon) / "typed"


def page_url(slug: str) -> Optional[str]:
    """The DFO notice page a region's locations were scraped from."""
    try:
        from pipeline.regs.dfo_salmon.fetch import _BASE
        return _BASE.format(slug=slug)
    except Exception:  # noqa: BLE001
        return None


def _stamp(d: Path) -> tuple:
    return tuple((p.name, p.stat().st_mtime_ns) for p in sorted(Path(d).glob("region-*.json"))) \
        if Path(d).exists() else ()


@lru_cache(maxsize=4)
def _load(d: str, _stamp_key: tuple) -> dict:
    """slug -> the region file, as JSON."""
    out = {}
    for p in sorted(Path(d).glob("region-*.json")):
        out[p.stem.split("region-", 1)[1]] = json.loads(p.read_text(encoding="utf-8"))
    return out


def regions(d: Path | None = None) -> dict:
    d = Path(d or default_dir())
    return _load(str(d), _stamp(d))


@lru_cache(maxsize=4)
def _typed(d: str, _stamp_key: tuple) -> dict[str, dict]:
    """fingerprint -> {rows, rules} from the typed feed (the re-scraped rules)."""
    out: dict[str, dict] = {}
    for p in sorted(Path(d).glob("region-*.json")):
        for loc in json.loads(p.read_text(encoding="utf-8")).get("locators", []):
            out[loc["fingerprint"]] = {"rows": list(loc.get("rows") or []),
                                       "rules": list(loc.get("rules") or [])}
    return out


def typed(d: Path | None = None) -> dict[str, dict]:
    d = Path(d or default_typed_dir())
    return _typed(str(d), _stamp(d))


def locations(d: Path | None = None) -> dict[str, tuple[str, dict, Optional[dict]]]:
    """location_id -> (region slug, the location, its water record or None)."""
    out = {}
    for slug, doc in regions(d).items():
        by_id = {w["water_id"]: w for w in doc.get("waters") or []}
        for loc in doc.get("locations") or []:
            out[loc["location_id"]] = (slug, loc, by_id.get(loc.get("water_id")))
    return out


def find(location_id: str, d: Path | None = None):
    return locations(d).get(location_id)


def rule_rows(loc: Mapping, typed_dir: Path | None = None) -> tuple[list[str], list[dict]]:
    """The scraped table rows and typed rules riding on a location (every fingerprint it holds —
    the newest wording first is not assumed; rows are de-duplicated in order)."""
    feed = typed(typed_dir)
    rows: list[str] = []
    rules: list[dict] = []
    for fp in loc.get("fingerprints") or []:
        got = feed.get(fp)
        if not got:
            continue
        rows += [r for r in got["rows"] if r not in rows]
        rules += got["rules"]
    return rows, rules


def place_text(loc: Mapping) -> str:
    """The notice's own words for the place: the water, then its specific area (verbatim)."""
    st = loc.get("source_text") or {}
    return f"{st.get('waters') or loc.get('water') or ''}\n{st.get('specific_area') or ''}"


def _h(obj) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:16]


def geography(loc: Mapping, water: Optional[Mapping]) -> dict:
    """The curated geography a P answer is about: the water's items and the location's extent
    (the cached section list is not part of it — a convenience, never a source of truth)."""
    b = {k: v for k, v in (loc.get("binding") or {}).items() if k != "sections_cached"}
    return {"items": list((water or {}).get("item_ids") or []),
            "water_tributaries": (water or {}).get("tributaries"), "binding": b}


def prints(loc: Mapping, water: Optional[Mapping], typed_dir: Path | None = None) -> dict:
    """THE PRINTS of a DFO location NOW (`marks.Prints` shape): text, and one `entry` record whose
    meaning is the scraped rows and whose binding is the curated geography (module doc)."""
    rows, _ = rule_rows(loc, typed_dir)
    return {"text": place_text(loc),
            "records": [{"key": "entry", "meaning": _h(rows), "binding": _h(geography(loc, water))}]}


def prints_of(location_id: str, d: Path | None = None, typed_dir: Path | None = None) -> Optional[dict]:
    got = find(location_id, d)
    if got is None:
        return None
    _slug, loc, water = got
    return prints(loc, water, typed_dir)


def need(loc: Mapping) -> dict:
    """How many cut-points the location's op takes, how many its extent has, and what is missing."""
    st = loc.get("source_text") or {}
    op, n = op_arity().get(st.get("op") or "", (None, None))
    ex = (loc.get("binding") or {}).get("extents") or []
    have = sum(len(e.get("splits") or []) for e in ex)
    return {"op": op, "points": n, "have": have, "bound": bool(ex),
            "missing": max(0, (n or 0) - have) if not ex else 0}
