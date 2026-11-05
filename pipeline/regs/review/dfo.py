"""THE DFO SALMON EXTENTS as review targets (T164): one target per scraped DFO location.

    data/curated/dfo_salmon/places/region-*.json   the curated geography (read only)
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
    meaning   the scraped table rows riding on the location — stamped, but NEVER compared (T366,
              owner 2026-11-03): a re-scrape that changes them stales no DFO mark; the change goes
              to the DFO digest (`dfo_salmon.refresh`, `marks._place_only`)
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
    return Path(CURATED.dfo_salmon.places)


def default_typed_dir() -> Path:
    from pipeline.common.curated import GENERATED
    return Path(GENERATED.regs.dfo_salmon) / "typed"


def page_url(slug: str) -> Optional[str]:
    """The DFO notice page a region's locations were scraped from."""
    from pipeline.regs.dfo_salmon.fetch import _BASE      # a missing constant is a bug, not "no page"
    return _BASE.format(slug=slug)


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


def rule_rows(loc: Mapping, typed_dir: Path | None = None,
              feed: Mapping[str, dict] | None = None) -> tuple[list[str], list[dict]]:
    """The scraped table rows and typed rules riding on a location (every fingerprint it holds —
    the newest wording first is not assumed; rows are de-duplicated in order). `feed`: a
    fingerprint -> {rows, rules} map to read instead of the typed dir (a refresh preview)."""
    feed = feed if feed is not None else typed(typed_dir)
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


def prints(loc: Mapping, water: Optional[Mapping], typed_dir: Path | None = None,
           feed: Mapping[str, dict] | None = None) -> dict:
    """THE PRINTS of a DFO location NOW (`marks.Prints` shape): text, and one `entry` record whose
    meaning is the scraped rows and whose binding is the curated geography (module doc)."""
    rows, _ = rule_rows(loc, typed_dir, feed)
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


# --------------------------------------------------------------------------- historic groups

#: words that never tell two places apart (place-word comparison)
_GROUP_STOP = frozenset("the of a an to at from in on and or approximately approx located that portion "
                        "waters water is are as by with its for be all known".split())
#: how alike two places' words must be (Jaccard) to be grouped without DFO's own duplicate link
GROUP_ALIKE = 0.85


def _gnorm(s: Optional[str]) -> str:
    import re
    return " ".join(re.sub(r"[^a-z0-9 ]", " ", (s or "").lower()).split())


def _place_text(loc: Mapping) -> str:
    st = loc.get("source_text") or {}
    return f"{st.get('waters') or loc.get('water') or ''} {st.get('specific_area') or ''}"


def _water_identity(loc: Mapping, water: Optional[Mapping]) -> tuple:
    """A water's identity across rewordings / name variants: its registry items, else its
    normalised name."""
    its = (water or {}).get("item_ids") or []
    return tuple(sorted(its)) if its else ("name", _gnorm(loc.get("water")))


def _place_facts(text: str) -> tuple[frozenset, frozenset, tuple]:
    """(place words, numbers, the guards that must agree: direction words and a tributary call)."""
    import re
    t = text.lower()
    words = frozenset(w for w in re.findall(r"[a-z0-9]+", t) if w not in _GROUP_STOP)
    nums = frozenset(re.findall(r"\d+(?:\.\d+)?", t))
    guards = (("upstream" in t or "above" in t), ("downstream" in t or "below" in t),
              bool(re.search(r"(including|excluding|not including)\s+(its\s+)?tributar", t)),
              bool(re.search(r"\bexcluding|\bexcept|not including", t)))
    return words, nums, guards


def group_why(hist: Mapping, cur: Mapping, cur_id: str) -> Optional[tuple[str, float]]:
    """Why a historic place is the same place as a current one on the same water, or None:
    DFO's own duplicate link, or the place words alike (`GROUP_ALIKE`) with the same numbers, the
    same direction words and the same tributary / exception call."""
    if hist.get("duplicate_of") == cur_id and hist.get("duplicate_confirmed") is not False:
        return "DFO lists it as a rewording (duplicate_of)", 1.01
    if hist.get("duplicate_of") == cur_id:               # the owner contested the link: apart
        return None
    (a, an, ag), (b, bn, bg) = _place_facts(_place_text(hist)), _place_facts(_place_text(cur))
    if an != bn or ag != bg or not (a | b):
        return None
    j = len(a & b) / len(a | b)
    if j < GROUP_ALIKE:
        return None
    return f"same water, place words {round(j * 100)}% alike, same numbers and direction", j


def groups(locs: Mapping[str, tuple[str, dict, Optional[dict]]] | None = None) -> dict[str, dict]:
    """HISTORIC PLACES GROUPED WITH THE CURRENT ONE THEY REWORD (owner 2026-10-28): each dormant
    location joins the active location on the same water (`_water_identity`) it is most alike
    (`group_why`), so one extent decision labels the group. -> location id -> {id, canonical,
    members: [{id, why}]} for every member (canonical first, its why None); a place in no group is
    absent. Read only; recording writes one mark per member the reviewer keeps (`group` links them)."""
    locs = locs if locs is not None else locations()
    by_water: dict[tuple, list[str]] = {}
    hist: list[str] = []
    for lid, (_s, loc, water) in locs.items():
        if (loc.get("status") or "active") == "active":
            by_water.setdefault(_water_identity(loc, water), []).append(lid)
        else:
            hist.append(lid)
    joined: dict[str, list[tuple[str, str]]] = {}
    for h in hist:
        _s, loc, water = locs[h]
        best = None
        for c in by_water.get(_water_identity(loc, water), []):
            got = group_why(loc, locs[c][1], c)
            if got and (best is None or got[1] > best[2]):
                best = (c, got[0], got[1])
        if best:
            joined.setdefault(best[0], []).append((h, best[1]))
    # HISTORIC-ONLY PLACES (owner 2026-11-02, the North Alouette): a dormant place no live one
    # rewords is still a place of its own; its dormant rewordings (DFO's duplicate_of, or alike
    # words on the same water) group with it, so it is one item and never vanishes
    placed = {h for ms in joined.values() for h, _w in ms}
    rest = [h for h in hist if h not in placed]
    canon_of = {h: h for h in rest}

    def root(x: str) -> str:
        while canon_of[x] != x:
            x = canon_of[x]
        return x
    for h in rest:
        _s, loc, water = locs[h]
        dup = loc.get("duplicate_of") if loc.get("duplicate_confirmed") is not False else None
        if dup in canon_of and root(dup) != root(h):
            canon_of[root(h)] = root(dup)
            continue
        for c in rest:
            if c == h or root(c) == root(h):
                continue
            if _water_identity(loc, water) == _water_identity(locs[c][1], locs[c][2]) \
                    and group_why(loc, locs[c][1], c):
                canon_of[root(h)] = root(c)
                break
    for h in rest:
        c = root(h)
        if c != h:
            why = group_why(locs[h][1], locs[c][1], c)
            joined.setdefault(c, []).append((h, why[0] if why else "DFO lists it as a rewording (duplicate_of)"))
    out: dict[str, dict] = {}
    for c, ms in joined.items():
        g = {"id": f"grp:{c}", "canonical": c,
             "members": [{"id": c, "why": None}] + [{"id": h, "why": w} for h, w in sorted(ms)]}
        for m in g["members"]:
            out[m["id"]] = g
    return out


# --------------------------------------------------------------------------- wordings (owner 2026-11-02)

def default_history_dir() -> Path:
    return Path(__file__).resolve().parents[3] / "cache" / "dfo_salmon" / "history"


def _page_date(name: str) -> Optional[tuple[str, str]]:
    import re
    m = re.match(r"region(\w+?)_(\d{4})(\d{2})(\d{2})", name)
    return (m.group(1), f"{m.group(2)}-{m.group(3)}-{m.group(4)}") if m else None


@lru_cache(maxsize=2)
def _wordings(d: str, _stamp_key: tuple) -> dict[str, dict]:
    from pipeline.regs.dfo_salmon.locations import extract
    from pipeline.regs.dfo_salmon.parse import parse_region
    from pipeline.regs.dfo_salmon.untangle import untangle
    out: dict[str, dict] = {}
    for f in sorted(Path(d).glob("region*_*.html")):
        got = _page_date(f.name)
        if got is None:
            continue
        slug, day = got
        try:
            locs, _r, _s = extract(untangle(parse_region(f.read_text(encoding="utf-8", errors="replace"), slug)))
        except Exception:  # noqa: BLE001 — an unparseable saved page dates nothing
            continue
        for loc in locs:
            w = out.setdefault(loc.fingerprint, {"waters": loc.water, "specific_area": loc.specific_area,
                                                 "dates": []})
            if day not in w["dates"]:
                w["dates"].append(day)
    for w in out.values():
        w["dates"].sort()
    return out


def wordings(history_dir: Path | None = None) -> dict[str, dict]:
    """EVERY WORDING DFO EVER PRINTED, from the saved pages (`cache/dfo_salmon/history`): fingerprint
    -> {waters, specific_area, dates: the page dates it was on}. ~1 s, cached per the dir's files."""
    d = Path(history_dir or default_history_dir())
    st = tuple((p.name, p.stat().st_mtime_ns) for p in sorted(d.glob("region*_*.html"))) if d.exists() else ()
    return _wordings(str(d), st)


def location_wordings(loc: Mapping, history_dir: Path | None = None) -> list[dict]:
    """A location's wordings (one per fingerprint it holds): the words and the page dates. A wording
    on no saved page (a page newer than the archive) keeps the location's own words, no dates."""
    ws = wordings(history_dir)
    st = loc.get("source_text") or {}
    out = []
    for fp in loc.get("fingerprints") or []:
        w = ws.get(fp)
        out.append({"fingerprint": fp,
                    "text": (w or {}).get("specific_area") if w else (st.get("specific_area") or ""),
                    "dates": list((w or {}).get("dates") or [])})
    return out or [{"fingerprint": None, "text": st.get("specific_area") or "", "dates": []}]


def places(locs: Mapping[str, tuple[str, dict, Optional[dict]]], water_id: str,
           grouped: Mapping[str, dict] | None = None) -> list[dict]:
    """THE PLACES ON ONE WATER (owner 2026-11-02): its locations folded into places — a place is a
    group (`groups`: a current wording and its historic rewordings, or a historic-only place and
    its rewordings) or a lone location; a confirmed variant (`duplicate_of`, not contested) joins the
    place of the location it rewords. -> [{id (the canonical location), members: [location id]}],
    live places first, in location order."""
    grouped = grouped if grouped is not None else groups(locs)
    on = [lid for lid, (_s, l, _w) in locs.items() if l.get("water_id") == water_id]
    canon: dict[str, str] = {}
    for lid in on:
        g = grouped.get(lid)
        canon[lid] = g["canonical"] if g else lid
    for lid in on:                                        # a variant rides the place it rewords
        l = locs[lid][1]
        dup = l.get("duplicate_of")
        if canon[lid] == lid and dup in canon and dup != lid and l.get("duplicate_confirmed") is not False:
            seen = {lid}
            while canon.get(dup, dup) != dup and dup not in seen:
                seen.add(dup)
                dup = canon[dup]
            canon[lid] = canon.get(dup, dup)
            for x in on:
                if canon[x] == lid:
                    canon[x] = canon[lid]
    out: dict[str, list[str]] = {}
    for lid in on:
        out.setdefault(canon[lid], []).append(lid)
    live = lambda c: any((locs[x][1].get("status") or "active") == "active" for x in out[c])  # noqa: E731
    order = sorted(out, key=lambda c: (not live(c), on.index(c) if c in on else 0))
    return [{"id": c, "members": [c] + [x for x in out[c] if x != c]} for c in order]
