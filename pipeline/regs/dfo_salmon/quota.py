"""T320 — THE SALMON BUCKET (owner 2026-10-31): DFO's per-species limits per water / extent as structured numbers, under
the region's combined limit — the game-fish quota tree's shape (an aggregate tier, its kinds as children).

    "The 4 salmon per day combined is the overall limit for the salmon bucket with sub amounts for each species within."

Three pure functions, no model, no credits (the pages are a table):

    read_limits(text)      a Limits/Gear cell -> its numbers {daily, hatchery, over, max_cm, notice, hours, gap_mm} and
                           `residue`: every digit the reading did not account for. A cell with residue is UNPARSED: the
                           typed rule carries `review_reason` (`typed.to_rules`), the verdict `q.u`, and the app shows
                           DFO's words, never a number it guessed.
    species_tree(...)      the rules a species' verdict was decided by (`deliver.salmon.verdict_on`) -> the verdict's
                           `{tree}` — T332: a `TreeTier` of the game-fish quota tree (the count tier; its kinds, size
                           tiers, a monthly tier's `period`), see THE MAPPING below — or `{u: 1}`, not one reading
    bucket(notes)          the region header's typed notes (`header.py`) -> {tree: the TOTAL tier, t, possession, tp,
                           min_cm, ids}; None where the page prints no combined daily limit
                           (Regions 4, 5A, 7: closed until further notice).

POSSESSION (owner 2026-10-31): only the bucket's — the total DFO prints ("You cannot possess more than 8 salmon in
total"). No per-species possession number is derived.

THE HEADER CLOSURE WINS (owner 2026-10-31): a page whose header says "There is no fishing for salmon in Region 5A …
until further notice" closes every place of that page that has rules (`header_closure`) — Regions 5A and 7 still print
the 2025 in-season rows (FN0846, FN0851: Aug 22 - Sep 30, 2025) under the restored closure. The rows stay in the feed;
the verdict answers with the header's words.

WATER BUCKETS: a header `aggregate` note naming a watershed (Region 2: "Squamish watershed: the aggregate daily limit
of hatchery-marked coho … is one (1)") is a bucket on that water and its tributaries — the app shows it on a page whose
item the note's scope covers, only while that species is open there.

THE CHECK (`over_bucket`): a species' daily limit never exceeds the bucket's; a size sub-limit never exceeds its species'.
`pipeline/tests/test_dfo_quota.py` holds every current scraped row and every typed rule to it.
"""
from __future__ import annotations

import re
from typing import Dict, List, Mapping, Optional, Sequence

from pipeline.regs.dfo_salmon import typed as T

FORMAT = "dfo-quota/2"     # T332: the verdict quota is a TreeTier (was `q`)

# the spans a limits cell's numbers live in, each read by `typed` — the residue is what none of them covers
_SPANS = (T._RE_DAILY, T._RE_DAILY_OF, T._RE_ONLY_OVER, T._RE_WHICH_MAY, T._RE_OF_WHICH_N, T._RE_UNMARKED,
          T._RE_MONTHLY, T._RE_NONE_OVER, T._RE_MAX_SIZE, T._RE_TIMES, T._RE_SOLAR_TIMES, T._RE_GAP,
          T._RE_FN, re.compile(r"\b(?:Region|Area)s?\s+\d+[A-Za-z]?\b", re.I))   # a place's name, not a limit


def residue(text: str) -> List[str]:
    """The numbers in a limits cell no reading accounts for (empty: every number is in the structure)."""
    t = text or ""
    for rx in _SPANS:
        t = rx.sub(" ", t)
    return re.findall(r"\d+(?:\.\d+)?", t)


#: why a cell is not fully read, by what its words do — first match names it (archived pages; none in today's)
_REASONS = (
    (re.compile(r"\b(?:shall|must)\s+be\s+recorded\b", re.I),
     "a recording rule (fish here count toward another water's quota)"),
    (re.compile(r"\bno\s+fishing\s+for\s+\w+\s+(?:from|between|within)\b", re.I),
     "a closure of part of the water inside the limits cell"),
    (re.compile(r"\d+\s*cm\s+or\s+less\s+and\b", re.I), "a size band the reading does not model"),
)


def unread_reason(text: str) -> str:
    """"" when every number in the cell is read; else the NAMED reason it is not (the typed rule's `review_reason`)."""
    left = residue(text)
    if not left:
        return ""
    for rx, why in _REASONS:
        if rx.search(text or ""):
            return f"{why}: numbers not read: {', '.join(left)}"
    return f"limits text has numbers the scrape did not read: {', '.join(left)}"


def read_limits(text: str) -> dict:
    """A Limits/Gear cell -> its numbers. Pure: the same reading `typed.to_rules` types rules from."""
    t = text or ""
    says = T.decode(t)
    out: dict = {"daily": says["daily_limit"], "hatchery": says["hatchery_marked_only"],
                 "no_fishing": says["no_fishing"], "release": says["non_retention"], "over": [], "max_cm": None,
                 "notice": None, "hours": None, "gap_mm": None}
    out["wild"] = int(m.group(1)) if (m := T._RE_UNMARKED.search(t)) else None
    out["monthly"] = [[int(m.group(2)), int(m.group(1))]] if (m := T._RE_MONTHLY.search(t)) else []
    sub = T._first(T._RE_ONLY_OVER.search(t), T._RE_WHICH_MAY.search(t), T._RE_OF_WHICH_N.search(t))
    if sub:
        out["over"] = [[int(sub.group(2)), T._num(sub.group(1))]]
    elif (m := T._RE_NONE_OVER.search(t)):
        out["max_cm"] = int(m.group(1))
    elif (m := T._RE_MAX_SIZE.search(t)):
        out["max_cm"] = int(m.group(1) or m.group(2))
    if (m := T._RE_FN.search(t)):
        out["notice"] = f"FN{int(m.group(1)):04d}"
    if (m := T._RE_TIMES.search(t) or T._RE_SOLAR_TIMES.search(t)):
        out["hours"] = [m.group(1), m.group(2)]             # "05:00", or (T321) "one hour before sunrise"
    if (m := T._RE_GAP.search(t)):
        out["gap_mm"] = int(m.group(1))
    out["residue"] = residue(t)
    out["unread"] = unread_reason(t)
    return out


def render(q: Mapping) -> str:
    """The reading back as words — the round trip: `read_limits(render(read_limits(x)))` has x's numbers."""
    bits = []
    if q.get("daily") is not None and not q.get("no_fishing") and not q.get("release"):
        bits.append(f"{q['daily']} {'hatchery marked ' if q.get('hatchery') else ''}per day")
    for cm, n in q.get("over") or ():
        bits.append(f"only {n} over {cm} cm")
    if q.get("wild") is not None:
        bits.append(f"{q['wild']} of which can be unmarked")
    for cm, n in q.get("monthly") or ():
        bits.append(f"monthly limit of {n} over {cm} cm")
    if q.get("max_cm") is not None:
        bits.append(f"maximum size {q['max_cm']} cm")
    if q.get("hours"):
        a, b = q["hours"]
        bits.append(f"from {a} h until {b} h only" if ":" in a else f"{a} to {b}")
    if q.get("gap_mm") is not None:
        bits.append(f"single barbless hook less than {q['gap_mm']}mm from point to shank")
    if q.get("notice"):
        bits.append(q["notice"])
    return ", ".join(bits)


def numbers(q: Mapping) -> tuple:
    """The facts a reading carries, for comparing two readings (the round trip)."""
    return (q.get("daily") if not q.get("no_fishing") and not q.get("release") else None, bool(q.get("hatchery")),
            tuple(map(tuple, q.get("over") or ())), q.get("max_cm"), q.get("notice"), q.get("wild"),
            tuple(map(tuple, q.get("monthly") or ())),
            tuple(q.get("hours") or ()), q.get("gap_mm"))


# ── the verdict's quota: a node of the game-fish quota tree ──────────────────────────────────────────────────────────
#
# T332 (owner 2026-10-30: "the quota structure data … should have same format as trout ones for consistency like same
# size datatype, quotas etc."): the species' numbers are a `TreeTier` of the answers model — the very node the trout
# card's quota tree is made of (`pipeline/deliver/answers/model/rows.py`, validated by it in the tests) — and the app
# draws it with the trout presenter (`regs/tree.ts` tierWords / kindWords). T320's bespoke `q` {d, h, w, o, mx, mo, a, u}
# is gone. THE MAPPING (the rule -> the node; the typed rule is already the catalogue's `CatalogueRule`):
#
#   "4 per day"                          rule take 4              count tier n 4, says "4 a day"
#   "hatchery marked only"               rule origin hatchery     its kind "hatchery-marked coho" (folded: the tier says it)
#   "1 of which can be unmarked"         sub-rule origin wild     a second kind "unmarked (wild) coho", n 1
#   "only 2 over 50 cm"                  lengths [{min_cm 50}] 2  size tier n 2, over_cm 50
#   "none over 50 cm", "maximum size"    lengths [{min_cm 50, take 0}]   size tier n 0, over_cm 50; the kind's sizes
#                                                                          {cm: [[0, 50]], in: [[0, 20]]}
#   "Monthly quota = 1 over 65 cm"       period monthly           size tier n 1, over_cm 65, period "monthly" (the ONE
#                                                                  shared field T332 adds to TreeTier)
#   an "All" salmon row                  species [SA]             the tier's species ["SA"], "all salmon combined here"
#   not one reading (review_reason)      —                        no tree; the verdict's `u: 1` (DFO's words stand)

#: the words, the app's before T332 (`SALMON_WORDS.quota`), now the store's like every trout tier's `says`
_NAMES = {"CH": "chinook", "CO": "coho", "SK": "sockeye", "PK": "pink", "CM": "chum", "SA": "salmon"}


def _says_daily(d: int, hatchery: bool, all_salmon: bool) -> str:
    if hatchery:
        return f"{d} a day, hatchery-marked only"
    return f"{d} a day, all salmon combined here" if all_salmon else f"{d} a day"


def _says_wild(w: int, d: int) -> str:
    return "marked or unmarked" if w >= d else f"up to {w} unmarked (wild), the rest hatchery-marked"


def _kind(species: List[str], name: str, n: int, says: str, max_cm: Optional[int], folded: bool) -> dict:
    from pipeline.deliver.answers.common import inches
    k = {"node": "kind", "species": species, "name": name, "n": n, "says": says, "detail": "",
         "sizes": {"cm": [[0, max_cm]], "in": [[0, None if max_cm is None else inches(max_cm)]]},
         "also_counts": [], "duties": [], "sentence": says}
    if folded:
        k["folded"] = True
    return k


def _size(seq: List[int], n: int, over_cm: int, says: str, period: Optional[str] = None, **kw) -> dict:
    seq[0] += 1
    t = {"node": "tier", "id": f"t{seq[0]}", "tier": "size", "n": n, "says": says, "by": 0, "children": [],
         "over_cm": over_cm, **kw}
    if period:
        t["period"] = period
    return t


def _one(rule: dict, subs: Sequence[dict]) -> dict:
    """One opening rule (and the sub-rules `within` it) -> its count tier."""
    d = int(rule.get("take") or 0)
    species = list(rule.get("species") or [])
    hatchery = rule.get("origin") == "hatchery"
    all_salmon = "SA" in species
    seq = [1]
    over, mx, monthly, wild, unread = [], None, [], None, bool(rule.get("review_reason"))
    for s in subs:
        unread = unread or bool(s.get("review_reason"))
        if s.get("origin") == "wild" and s.get("take") is not None:
            wild = int(s["take"])                                  # of the d, up to `wild` may be unmarked
            continue
        if s.get("period") == "monthly":
            monthly += [[int(b["min_cm"]), int(s["take"])] for b in s.get("lengths") or () if b.get("min_cm") is not None]
            continue
        for band in s.get("lengths") or ():
            lo, hi = band.get("min_cm"), band.get("max_cm")
            if "take" in band and band.get("take") == 0 and lo is not None:
                mx = lo if mx is None else min(mx, lo)              # "none over 50 cm", "maximum size 35 cm"
            elif lo is not None and s.get("take") is not None:
                over.append([int(lo), int(s["take"])])
            elif hi is not None:
                mx = hi if mx is None else min(mx, hi)
    says = _says_daily(d, hatchery, all_salmon)
    names = " or ".join(_NAMES.get(c, c) for c in species) or "salmon"
    # the kind the count holds, where it says more than the tier's species: an origin, a ceiling, its wild share
    # (feed size: a plain "4 per day" is the tier alone — its `species` names the fish)
    kids: List[dict] = []
    if hatchery or wild is not None or mx is not None:
        kids.append(_kind(species, f"hatchery-marked {names}" if hatchery else names, d, says, mx, True))
    if wild is not None:
        kids.append(_kind(species, f"unmarked (wild) {names}", wild, _says_wild(wild, d), mx, False))
    kids += [_size(seq, n, cm, f"only {n} over {cm} cm") for cm, n in sorted(over)]
    if mx is not None:
        kids.append(_size(seq, 0, int(mx), f"none over {int(mx)} cm"))
    kids += [_size(seq, n, cm, f"{n} over {cm} cm a month", "monthly") for cm, n in sorted(monthly)]
    tree = {"node": "tier", "id": "t1", "tier": "count", "n": d, "says": says, "by": 0, "species": species,
            "children": kids}
    return {"tree": tree, **({"u": 1} if unread else {})}


def species_tree(opens: Sequence[dict], subs: Mapping[str, Sequence[dict]]) -> Optional[dict]:
    """The verdict's quota from the rules that opened the species that day (each with its size sub-rules):
    `{tree}` (a `TreeTier`, the count tier) or `{u: 1}` — a rule not fully read, or two rules with different numbers
    holding on one day (NOT reconciled here): no number, the app shows DFO's words."""
    qs = [_one(r, subs.get(r.get("rule_id") or "", ())) for r in opens]
    if not qs:
        return None
    if any(q.get("u") for q in qs) or any(q != qs[0] for q in qs[1:]):
        return {"u": 1}
    return qs[0]


def walk(tree: Mapping):
    """Every node of a tree, the tier first (pre-order) — the order the app says them in."""
    yield tree
    for c in tree.get("children") or ():
        yield from walk(c) if c.get("node") == "tier" else iter((c,))


def words(tree: Optional[Mapping]) -> Optional[str]:
    """The quota's line as the app says it ("4 a day · only 2 over 50 cm"): the tiers and the unfolded kinds, in order."""
    if not tree:
        return None
    return " · ".join(x["says"] for x in walk(tree) if x["node"] == "tier" or not x.get("folded"))


# ── the bucket ───────────────────────────────────────────────────────────────────────────────────────────────────────

def bucket(notes: Sequence[Mapping]) -> Optional[dict]:
    """The region header's combined limits as the salmon bucket (None: the page prints no combined daily limit):
    `tree` the TOTAL tier of the quota tree ("Up to 4 salmon a day in total"; the header's annual / monthly limits its
    children, a size — or count — tier with `period`), DFO's words `t` / `tp`, `possession` the total DFO prints,
    `min_cm` per species, `ids` the notes it was read from."""
    daily = [n for n in notes if n.get("k") == "daily" and not n.get("w") and isinstance(n.get("n"), int)]
    if not daily:
        return None
    d = daily[0]
    seq = [0]
    tree = {"node": "tier", "id": "t0", "tier": "total", "n": d["n"], "says": f"Up to {d['n']} salmon a day in total",
            "by": 0, "children": []}
    for n in notes:
        if n.get("k") in ("annual", "monthly") and not n.get("w") and isinstance(n.get("n"), int):
            sp = list(n.get("sp") or ())
            if isinstance(n.get("cm"), int):
                tree["children"].append(_size(seq, n["n"], n["cm"], n["t"], n["k"], species=sp))
            else:
                seq[0] += 1
                tree["children"].append({"node": "tier", "id": f"t{seq[0]}", "tier": "count", "n": n["n"],
                                         "says": n["t"], "by": 0, "children": [], "species": sp, "period": n["k"]})
    out: dict = {"tree": tree, "t": d["t"], "ids": [d["id"]] if d.get("id") else []}
    poss = [n for n in notes if n.get("k") == "possession" and not n.get("w")]
    if poss:
        p = poss[0]
        if isinstance(p.get("n"), int):
            out["possession"] = p["n"]
        out["tp"] = p["t"]
        if p.get("id"):
            out["ids"].append(p["id"])
    mins: Dict[str, int] = {}
    for n in notes:
        if n.get("k") == "min_size" and not n.get("w") and isinstance(n.get("cm"), int):
            for sp in n.get("sp") or ():
                mins.setdefault(sp, n["cm"])
    if mins:
        out["min_cm"] = mins
    return out


def over_bucket(q: Optional[Mapping], b: Optional[Mapping]) -> List[str]:
    """What breaks the bucket: a species' daily above the total, a size or wild allowance above its species' daily."""
    tree = (q or {}).get("tree")
    if not tree or (q or {}).get("u"):
        return []
    d, bad = tree["n"], []
    total = ((b or {}).get("tree") or {}).get("n")
    if isinstance(total, int) and d > total:
        bad.append(f"daily {d} > the bucket's {total}")
    for x in walk(tree):
        if x is tree or x["n"] is None or x.get("period"):
            continue
        if x["n"] > d:
            bad.append(f"{x['says']}: {x['n']} > the species' {d} a day")
    return bad


def header_closure(notes: Sequence[Mapping], slug: str) -> Optional[List[dict]]:
    """The rules a place of a closed page answers with (owner 2026-10-31: the header closure wins), or None: the page's
    header has no region-wide `closed` note. One rule, all salmon, no fishing, in the header's words."""
    closed = [n for n in notes if n.get("k") == "closed" and not n.get("w")]
    if not closed:
        return None
    return [{"rule_id": f"{slug}:header-closed", "type": "retention_limit", "verbatim": closed[0]["t"],
             "species": ["SA"], "take": 0, "may_target": False}]
