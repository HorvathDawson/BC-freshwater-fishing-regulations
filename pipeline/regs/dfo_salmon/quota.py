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
                           ids}; None where the page prints no combined daily limit
                           (Regions 4, 5A, 7: closed until further notice).
    shared_buckets(notes)  (T349) a scoped `aggregate` note -> the region's `shared` tiers (with or without a bucket)
    sizes(notes)           (T356) the header sizes per species, `LengthBand`s, ordered: the first that holds wins

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
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

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


# ── T349: SHARED QUOTAS (owner 2026-11-02) ──────────────────────────────────────────────────────────────────────────
#
# A quota shared ACROSS waters is the quota tree's aggregate tier: tier `total` with `whose` naming its scope — a SET of
# waters or management units, not one water. Two on the pages today:
#
#   R5B   "All lakes or streams or part thereof in the aggregate (combined total), in Management Units 5-6 to 5-11 …"
#         — the region row's limits are one count over every water of MUs 5-6 to 5-11 together. The page's second
#         definition, "(i.e. Region 5 not including the Fraser River watershed)", is SETTLED to the MU list (owner:
#         one scope): it is the operative text and what `scope_places.json` `scope_extents` ("5b", "region") binds.
#   R2    "Squamish watershed: the aggregate daily limit of hatchery-marked coho in the entire Squamish non-tidal
#         systems combined is one (1)." — a header note naming a watershed: the region's `shared` tier on those waters.
#
# The location's rules are marked where they meet it (`mark_shared`: deliver.salmon.build, live.join), so the verdict
# tree (`_one`) is the aggregate tier wherever that location answers.

_RE_AGGREGATE = re.compile(r"\bin the aggregate\b|\bcombined total\b", re.I)
_RE_MU_GROUP = re.compile(r"^area:mu_group:management_units_(\d+)_(\d+)_to_(\d+)_(\d+)$")


def pool_of(loc) -> List[str]:
    """(T358) The AREAS a location's quota is pooled across (`CatalogueRule.pooled_across`): its own `within` extents,
    where its words say "in the aggregate" / "combined total"; [] where they do not. A pooled location with no `within`
    area is an error — the pool would be words with nothing to bind (it was the prose "these waters")."""
    t = str((getattr(loc, "source_text", None) or {}).get("specific_area") or "")
    if not _RE_AGGREGATE.search(t):
        return []
    areas = sorted({e.area_id for e in loc.binding.extents if getattr(e, "op", None) is not None
                    and e.op.value == "within" and e.area_id})
    if not areas:
        raise ValueError(f"{loc.location_id}: its quota is pooled ({t[:60]!r}…) but it is bound to no `within` area")
    return areas


def pool_words(areas: Sequence[str]) -> str:
    """The words of a pool's areas ("Management Units 5-6 to 5-11"). An area with no words here is an error."""
    out = []
    for a in areas:
        m = _RE_MU_GROUP.match(a)
        if not m:
            raise ValueError(f"pool_words: no words for the pooled area {a!r} — add its kind here")
        out.append(f"Management Units {m.group(1)}-{m.group(2)} to {m.group(3)}-{m.group(4)}")
    return " and ".join(out)


def mark_shared(rules: Sequence[dict], areas: Sequence[str]) -> List[dict]:
    """The location's rules with each top-level count (a retention limit above zero, no `within`) `pooled_across` its
    areas (`pool_of`)."""
    if not areas:
        return list(rules)
    return [{**r, "pooled_across": list(areas)} if (r.get("type") == "retention_limit" and not r.get("within")
                                                    and (r.get("take") or 0) > 0) else r for r in rules]


def _says_shared(d: int, names: str, hatchery: bool, scope: str) -> str:
    return f"Up to {d} {'hatchery-marked ' if hatchery else ''}{names} a day in total, all waters of {scope} combined"


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
    if rule.get("pooled_across"):   # T349 / T358: the aggregate tier — one count over a set of areas
        whose = pool_words(rule["pooled_across"])
        tree = {**tree, "tier": "total", "whose": whose, "says": _says_shared(d, names, hatchery, whose)}
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

def _over(n: Mapping) -> Optional[int]:
    """A header note's length floor (`lengths[0].min_cm`: "over 50 cm", "30 cm or more"), else None."""
    ls = n.get("lengths") or ()
    return ls[0].get("min_cm") if ls else None


def bucket(notes: Sequence[Mapping]) -> Optional[dict]:
    """The region header's combined limits as the salmon bucket (None: the page prints no combined daily limit):
    `tree` the TOTAL tier of the quota tree ("Up to 4 salmon a day in total"; the header's annual / monthly limits its
    children, a size — or count — tier with `period`), DFO's words `t` / `tp`, `possession` the total DFO prints,
    `ids` the notes it was read from. (T358) The minimum sizes are not here: they are `sizes`."""
    daily = [n for n in notes if n.get("k") == "daily" and not n.get("w") and isinstance(n.get("n"), int)]
    if not daily:
        return None
    d = daily[0]
    seq = [0]
    # (T343) the daily total says where it counts: DFO's "from tidal and fresh waters combined"
    where = ", fresh and tidal waters combined" if re.search(r"tidal and fresh|fresh and tidal", d["t"], re.I) else ""
    tree = {"node": "tier", "id": "t0", "tier": "total", "n": d["n"], "says": f"Up to {d['n']} salmon a day in total{where}",
            "by": 0, "children": []}
    out: dict = {"tree": tree, "t": d["t"], "ids": [d["id"]] if d.get("id") else []}
    for n in notes:
        if n.get("k") in ("annual", "monthly") and not n.get("w") and isinstance(n.get("n"), int):
            if n.get("id"):
                out["ids"].append(n["id"])                       # (T343) said by the bucket's tier
            sp = list(n.get("sp") or ())
            if isinstance(_over(n), int):
                tree["children"].append(_size(seq, n["n"], _over(n), n["t"], n["k"], species=sp))
            else:
                seq[0] += 1
                tree["children"].append({"node": "tier", "id": f"t{seq[0]}", "tier": "count", "n": n["n"],
                                         "says": n["t"], "by": 0, "children": [], "species": sp, "period": n["k"]})
    poss = [n for n in notes if n.get("k") == "possession" and not n.get("w")]
    if poss:
        p = poss[0]
        if isinstance(p.get("n"), int):
            out["possession"] = p["n"]
        out["tp"] = p["t"]
        if p.get("id"):
            out["ids"].append(p["id"])
    return out


def shared_buckets(notes: Sequence[Mapping]) -> List[dict]:
    """(T349) a header `aggregate` note naming waters (R2's Squamish hatchery coho) as the quota tree's aggregate tier:
    `[{tree, t, id, w}]` — `tree` a `total` tier whose scope is the named watershed(s), `w` the note's water keys (the
    payload's `scopes` turn them into items). Only where DFO printed the number. (T356) The region's own `shared`,
    whether or not the page prints a combined daily limit (`bucket`): the app reads it for every `aggregate` note with a
    number, and a missing one is an error there."""
    out = []
    for n in notes:
        if n.get("k") != "aggregate" or not n.get("w") or not isinstance(n.get("n"), int):
            continue
        sp = list(n.get("sp") or ())
        hatchery = bool(n.get("hatchery"))
        names = " or ".join(_NAMES.get(c, c) for c in sp) or "salmon"
        whose = " and ".join(f"the {w['n']} watershed" if w.get("ws") else w["n"] for w in n["w"])
        tree = {"node": "tier", "id": "t0", "tier": "total", "n": n["n"], "by": 0, "children": [], "species": sp,
                "whose": whose, "says": f"Up to {n['n']} {'hatchery-marked ' if hatchery else ''}{names} a day in "
                                        f"total across {whose}"}
        if hatchery:
            tree["origin"] = "hatchery"
        out.append({"tree": tree, "t": n["t"], "id": n.get("id"), "w": [w["key"] for w in n["w"]]})
    return out


# ── (T356 / T358) THE HEADER SIZES, resolved per species ─────────────────────────────────────────────────────────
#
# A species' sizes on a page — the minimum to keep, what an "adult" is, what a "jack" is — as `LengthBand`s (the
# catalogue's own size type, `lengths`), resolved HERE so the app looks them up and composes nothing: `sizes(notes)` is
# an ORDERED list of entries `{sp, min?, adult?, jack?, ids, w?, on?}` and the FIRST entry for a species that holds on
# the page wins (`w`: only on those waters' items, like a note; `on`: its day windows). A water's own size note (Region
# 2's "adult chinook is over 62 cm" on the Fraser, Sep 1-Dec 31) is an entry of its own, BEFORE the region's, carrying
# the region's other sizes with it — so "a water beats its region" is the order, decided here.

SIZE_KINDS = ("min_size", "adult", "jack")
_SIZE_KEY = {"min_size": "min", "adult": "adult", "jack": "jack"}


#: (owner T343, 2026-10-31) what an "adult chinook" is where the page names no length for it: 50 cm (every page that
#: names one region-wide says 50, except R6's 65 — which excepts the Fraser watershed, so that watershed gets this)
ADULT_CHINOOK = {"min_cm": 50}


def sizes(notes: Sequence[Mapping], used: Optional[set] = None) -> List[dict]:
    """The page's header sizes per species, ordered: water entries first (page order), then the region's. Species whose
    sizes are alike share an entry (`sp` a list). A region-wide size note EXCEPTING whole waters (`x`: R6's adult
    chinook "other than Fraser River Watershed") gives those waters an entry without it (adult chinook: the owner's
    default, `ADULT_CHINOOK`); one excepting PARTS of waters (`xp`) is not modelled: an error, never a guess. A page
    with sizes but no adult chinook length gets the default region-wide. `used` collects the ids of the notes said."""
    from pipeline.deliver.salmon import SPECIES
    used = set() if used is None else used
    for n in notes:
        if n.get("k") in SIZE_KINDS and n.get("xp"):
            raise ValueError(f"sizes: {n.get('id')} excepts parts of waters — a size exception is not modelled")
    region: Dict[str, dict] = {}
    excepted: List[Tuple[Mapping, str]] = []                     # (note, species) a region-wide note excepts waters of
    for n in notes:
        if n.get("k") in SIZE_KINDS and not n.get("w") and n.get("lengths"):
            for sp in n.get("sp") or ():
                e = region.setdefault(sp, {})
                if _SIZE_KEY[n["k"]] not in e:                   # the first note of a kind (page order)
                    e[_SIZE_KEY[n["k"]]] = dict(n["lengths"][0])
                    if n.get("x"):
                        excepted.append((n, sp))
            if n.get("id"):
                used.add(n["id"])
    if region and "adult" not in region.get("CH", {}):
        region.setdefault("CH", {})["adult"] = dict(ADULT_CHINOOK)
    out: List[dict] = []

    def put(sp: str, e: dict) -> None:
        last = out[-1] if out else None
        if last is not None and {k: v for k, v in last.items() if k != "sp"} == e:
            last["sp"].append(sp)
        else:
            out.append({"sp": [sp], **e})

    for n in notes:
        if n.get("k") in SIZE_KINDS and n.get("w") and n.get("lengths"):
            for sp in n.get("sp") or ():
                put(sp, {**region.get(sp, {}), _SIZE_KEY[n["k"]]: dict(n["lengths"][0]),
                         "w": [w["key"] for w in n["w"]], **({"on": n["on"]} if n.get("on") else {})})
            if n.get("id"):
                used.add(n["id"])
    for n, sp in excepted:
        e = {k: v for k, v in region[sp].items() if k != _SIZE_KEY[n["k"]]}
        if n["k"] == "adult" and sp == "CH":
            e["adult"] = dict(ADULT_CHINOOK)
        if e:
            put(sp, {**e, "w": [w["key"] for w in n["x"]]})
    for sp in SPECIES:
        if sp in region:
            put(sp, region[sp])
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
    header has no region-wide `closed` note. One rule, all salmon, no fishing, in the header's words; `header_note` the
    note's id, which its verdict carries as `c` (T356: the app reads the closure from the file, never from an id)."""
    closed = [n for n in notes if n.get("k") == "closed" and not n.get("w")]
    if not closed:
        return None
    return [{"rule_id": f"{slug}:header-closed", "type": "retention_limit", "verbatim": closed[0]["t"],
             "species": ["SA"], "take": 0, "may_target": False, "header_note": closed[0]["id"]}]
