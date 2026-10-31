"""T320 — THE SALMON BUCKET (owner 2026-10-31): DFO's per-species limits per water / extent as structured numbers, under
the region's combined limit — the game-fish quota tree's shape (an aggregate tier, its kinds as children).

    "The 4 salmon per day combined is the overall limit for the salmon bucket with sub amounts for each species within."

Three pure functions, no model, no credits (the pages are a table):

    read_limits(text)      a Limits/Gear cell -> its numbers {daily, hatchery, over, max_cm, notice, hours, gap_mm} and
                           `residue`: every digit the reading did not account for. A cell with residue is UNPARSED: the
                           typed rule carries `review_reason` (`typed.to_rules`), the verdict `q.u`, and the app shows
                           DFO's words, never a number it guessed.
    species_quota(...)     the rules a species' verdict was decided by (`deliver.salmon.verdict_on`) -> the verdict's `q`:
                           {d: daily, h: 1 hatchery-marked only, o: [[over_cm, n]] (n of the d may be over), mx: max cm,
                           w: up to w of the d may be unmarked (wild), mo: [[over_cm, n]] a month, a: 1 an all-salmon row
                           (combined at that place), u: 1 not readable as one number}
    bucket(notes)          the region header's typed notes (`header.py`) -> the bucket tier {node, tier: "total", n, says,
                           possession, min_cm, per, ids}; None where the page prints no combined daily limit
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

FORMAT = "dfo-quota/1"

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


# ── the verdict's quota ──────────────────────────────────────────────────────────────────────────────────────────────

def _one(rule: dict, subs: Sequence[dict]) -> dict:
    q: dict = {"d": int(rule.get("take") or 0)}
    if rule.get("origin") == "hatchery":
        q["h"] = 1
    if "SA" in (rule.get("species") or ()):
        q["a"] = 1
    over, mx, monthly = [], None, []
    for s in subs:
        if s.get("origin") == "wild" and s.get("take") is not None:
            q["w"] = int(s["take"])                                # of the d, up to w may be unmarked
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
    if over:
        q["o"] = sorted(over)
    if mx is not None:
        q["mx"] = int(mx)
    if monthly:
        q["mo"] = sorted(monthly)
    if rule.get("review_reason") or any(s.get("review_reason") for s in subs):
        q["u"] = 1
    return q


def species_quota(opens: Sequence[dict], subs: Mapping[str, Sequence[dict]]) -> Optional[dict]:
    """The verdict's `q` from the rules that opened the species that day (each with its size sub-rules). Two rules with
    different numbers holding on one day are NOT reconciled here: `u: 1`, and the app shows DFO's words."""
    qs = [_one(r, subs.get(r.get("rule_id") or "", ())) for r in opens]
    if not qs:
        return None
    first = qs[0]
    if any(q != first for q in qs[1:]):
        return {"u": 1}
    return first


# ── the bucket ───────────────────────────────────────────────────────────────────────────────────────────────────────

def bucket(notes: Sequence[Mapping]) -> Optional[dict]:
    """The region header's combined limits as the salmon bucket's total tier (None: the page prints no combined daily
    limit). Values only from the notes' own numbers and words; `ids` the notes it was read from."""
    daily = [n for n in notes if n.get("k") == "daily" and not n.get("w") and isinstance(n.get("n"), int)]
    if not daily:
        return None
    d = daily[0]
    out: dict = {"node": "tier", "tier": "total", "n": d["n"], "says": f"Up to {d['n']} salmon a day in total",
                 "t": d["t"], "ids": [d["id"]] if d.get("id") else []}
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
    per = [{"k": n["k"], "sp": list(n.get("sp") or ()), "over_cm": n.get("cm"), "n": n.get("n"), "t": n["t"]}
           for n in notes if n.get("k") in ("annual", "monthly") and not n.get("w") and isinstance(n.get("n"), int)]
    if per:
        out["per"] = per
    return out


def over_bucket(q: Optional[Mapping], b: Optional[Mapping]) -> List[str]:
    """What breaks the bucket: a species' daily above the total, a size sub-limit above its species' daily."""
    if not q or q.get("u") or "d" not in q:
        return []
    bad = []
    if b and isinstance(b.get("n"), int) and q["d"] > b["n"]:
        bad.append(f"daily {q['d']} > the bucket's {b['n']}")
    for cm, n in q.get("o") or ():
        if n > q["d"]:
            bad.append(f"{n} over {cm} cm > the species' {q['d']} a day")
    if q.get("w", 0) > q["d"]:
        bad.append(f"{q['w']} unmarked > the species' {q['d']} a day")
    return bad


def header_closure(notes: Sequence[Mapping], slug: str) -> Optional[List[dict]]:
    """The rules a place of a closed page answers with (owner 2026-10-31: the header closure wins), or None: the page's
    header has no region-wide `closed` note. One rule, all salmon, no fishing, in the header's words."""
    closed = [n for n in notes if n.get("k") == "closed" and not n.get("w")]
    if not closed:
        return None
    return [{"rule_id": f"{slug}:header-closed", "type": "retention_limit", "verbatim": closed[0]["t"],
             "species": ["SA"], "take": 0, "may_target": False}]
