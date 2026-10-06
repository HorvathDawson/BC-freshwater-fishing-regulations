"""THE LICENCE ANSWER — the documents each angler needs, per (licence key, segment, profile).
Consumer Stage 7.7; gap G5.

WHICH REQUIREMENTS HOLD is the reader's (`read.requirements_in_force`): placement, `on` a
classified or stamp period, the record's dates, an outright stamp waiver, the undrawn part, and
one obligation printed twice folded into one key (`also_printed`, RU-12). Three things the page
does that the reader does not (G5) are applied HERE, after it, and proposed to the reader's owner
(`PROPOSALS_FOR_READER`):

  * a requirement for the other KIND of water does not hold (`water: stream` — the province's
    Classified Waters Licence and `zp:steelhead#steelhead_targeting`); a record that RESTATES
    another reads that record's `who`, `satisfied_by` and `water` (one obligation);
  * a SUPERIOR authority's requirement (a national park fishing permit) DISPLACES every other
    requirement that has a way to satisfy it — the provincial licences are not valid there;
  * (the page also drops a steelhead-only requirement on any lake; we do NOT: AGENTS 54 puts the
    whole provincial steelhead set, stamp included, on a book-known lake such as Tenas Lake, and
    the stream-only steelhead requirement already says `water: stream`.)

WHO NEEDS WHAT is decided for every one of the 60 angler profiles (residency 3 x age 2 x guided 2
x status 5, Métis included — DESIGN.md D5): the angler is always unknown, so every answer ships
and the page picks the one the user describes (licensing-rewrite-decisions). A requirement is
the angler's when its `who` matches and its `who_except` does not; an exemption matching the
angler frees its documents; each `satisfied_by` path (and each `alternative` accepted here) is one
way to satisfy it, a `hold` path needing all its documents not freed. The documents to buy are
every document still needed by the angler's non-guiding, non-displaced requirements, base licence
first, each with WHEN it is needed and its PRICES for the angler's residency (`licence_terms`).
"""
from __future__ import annotations

import itertools
import json
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Sequence, Tuple

from pipeline.deliver.answers.common import Interner, connect, dumps, month_day, segments, \
    when_vector
from pipeline.deliver.bundle import read

#: The profile dimensions, in the page's picker order. Index = mixed-radix over this order.
PROFILE_DIMS: Tuple[Tuple[str, Tuple[str, ...]], ...] = (
    ("residency", ("resident", "non_resident", "non_resident_alien")),
    ("age", ("16_plus", "under_16")),
    ("guidance", ("non_guided", "guided")),
    ("status", ("none", "indian_bc_resident", "metis", "disabled", "aged_65_plus")),
)


def profiles() -> List[Dict[str, str]]:
    return [dict(zip((d for d, _ in PROFILE_DIMS), combo))
            for combo in itertools.product(*(v for _, v in PROFILE_DIMS))]


def profile_index(p: Dict[str, str]) -> int:
    """The page's arithmetic: the profile's row in every `documents` table."""
    i = 0
    for d, vals in PROFILE_DIMS:
        i = i * len(vals) + vals.index(p[d])
    return i


class UnknownAxis(ValueError):
    """A `who` naming an angler axis the profiles do not model — matching it would be a guess."""


def who_match(who: Optional[dict], p: Dict[str, str]) -> bool:
    if not who:
        return True
    for axis in who:
        if axis not in p:
            raise UnknownAxis(f"licence: `who` names the axis {axis!r}; the profiles model "
                              f"{', '.join(p)}")
    return all(p[axis] in (v if isinstance(v, list) else [v]) for axis, v in who.items())


DECISIONS = [
    "L1 A restating record carries the restated record's `who`, `satisfied_by` AND `water` (one "
    "obligation, RU-12); the page carries only the first two.",
    "L2 A displaced requirement (a superior authority holds) contributes NO document to buy; the "
    "page marks it 'Not valid here' but still lists its documents.",
    "L3 A steelhead-only requirement on a lake is NOT dropped (AGENTS 54: a book-known lake "
    "carries the stamp); the page drops it. The stream-only one says `water: stream` and drops "
    "by kind.",
    "L4 A key of unknown water kind (no named water: walked tributaries) keeps a kind-scoped "
    "requirement — the tributary walk walks streams only (AGENTS 15).",
    "L5 Métis is its own status (60 profiles): no record names it, so it answers as 'none' today.",
    "L6 Prices are numbers per document for the profile's residency: the annual price written for "
    "this angler first (65+, disabled), each day price, each 8-day price; a term's `classified` "
    "or `units` must match a designation in force. {} when none is printed.",
    "L7 On tidal water no province-wide record holds (the reader: `province_except` tidal); the "
    "page still lists the angling guide licence there (Nitinat Lake) — a page bug.",
]

#: G5 — the fix the reader needs, for agent B (who owns read.py). Written here, applied here
#: until the reader carries it.
PROPOSALS_FOR_READER = [
    "requirements_in_force: drop a requirement whose `water` (or, for a restating record, the "
    "restated record's `water`) is not the section's water kind (`item.kind` via item_section; "
    "keep it where the section is in no item). Return it under a new `wrong_water` key so a "
    "reader can see it, as `waived` does for the stamp waiver.",
    "requirements_in_force: when a held requirement has `authority: superior`, move every other "
    "held requirement with a non-empty `satisfied_by` to a new `displaced` map "
    "{key: [superior key, ...]} — 'a federal or park authority's requirement replaces provincial "
    "licences'. Today province_except already stops the province-placed ones in national parks, "
    "so this changes nothing on the live bundle (measured: 0 keys); it guards sections- and "
    "designation-placed records.",
    "expose `designations_in_force(db, section, on)` (today `_designations_in_force`): the "
    "licence answer reads it for the designation boxes and for which `licence_terms` "
    "(classified class, units) price this water.",
]


@dataclass(frozen=True, order=True)
class LicenceKey:
    licensing_set: Optional[int]
    province_except: str
    tidal: bool
    kind: Optional[str]
    ruleset: Optional[int]          # only where a designation of the set can be put to sleep

    def as_list(self) -> list:
        return [self.licensing_set, self.province_except, int(self.tidal), self.kind,
                self.ruleset]


@dataclass
class Corpus:
    """Every licensing record, by key `entry_id#id`, and the export's index for each."""
    records: Dict[str, dict]        # key -> record (with `kind`, `placement`, `entry_id`)
    index: Dict[str, int]           # key -> export `licensing` index
    requirements: Dict[str, dict]
    exemptions: List[Tuple[str, dict]]
    alternatives: List[Tuple[str, dict]]
    terms: List[Tuple[str, dict]]


_TABLES = (("requirement", "req_id"), ("designation", "designation_id"),
           ("exemption", "exemption_id"), ("alternative", "alternative_id"),
           ("licence_terms", "terms_id"), ("not_classified", "not_classified_id"))


def corpus(db: sqlite3.Connection) -> Corpus:
    recs: Dict[str, dict] = {}
    for tab, idcol in _TABLES:
        cols = [r[1] for r in db.execute(f"PRAGMA table_info({tab})")]
        for row in db.execute(f"SELECT * FROM {tab}"):
            d = dict(zip(cols, row))
            r = json.loads(d["record"])
            r.setdefault("kind", tab)
            r["entry_id"] = d["entry_id"]
            r["_placement"] = d.get("placement")
            recs[f"{d['entry_id']}#{d[idcol]}"] = r
    ids = sorted(recs)
    return Corpus(records=recs, index={k: i for i, k in enumerate(ids)},
                  requirements={k: r for k, r in recs.items() if r["kind"] == "requirement"},
                  exemptions=sorted((k, r) for k, r in recs.items() if r["kind"] == "exemption"),
                  alternatives=sorted((k, r) for k, r in recs.items()
                                      if r["kind"] == "alternative"),
                  terms=sorted((k, r) for k, r in recs.items() if r["kind"] == "licence_terms"))


# --------------------------------------------------------------------------------------------
# Keys
# --------------------------------------------------------------------------------------------

def keys(db: sqlite3.Connection) -> Dict[LicenceKey, List[int]]:
    """Every section carrying a rule set or a licensing set, grouped by what
    `requirements_in_force` reads of it (plus its water kind, which the G5 step reads)."""
    out: Dict[LicenceKey, List[int]] = defaultdict(list)
    for sid, k in section_keys(db).items():
        out[k].append(sid)
    return dict(sorted(out.items(), key=lambda kv: json.dumps(kv[0].as_list())))


def section_keys(db: sqlite3.Connection, sids: Optional[set] = None) -> Dict[int, LicenceKey]:
    """`{sid: LicenceKey}` for every section with a rule set or a licensing set (or only `sids`)."""
    lset = dict(db.execute("SELECT sid, set_id FROM section_licensing"))
    rset = dict(db.execute("SELECT sid, set_id FROM section_ruleset"))
    pe: Dict[int, List[str]] = defaultdict(list)
    for k, sid in db.execute("SELECT area_kind, sid FROM province_except ORDER BY area_kind"):
        pe[sid].append(k)
    tidal = {s for (s,) in db.execute("SELECT sid FROM tidal")}
    kind = dict(db.execute("SELECT s.sid, i.kind FROM item_section s JOIN item i "
                           "ON i.ord = s.ord"))
    sleepy = {(e, d) for e, d, rec in db.execute(
        "SELECT entry_id, designation_id, record FROM designation")
        if json.loads(rec).get("suspended_while")}
    sleepy_sets = {s for s, e, r in db.execute(
        "SELECT set_id, entry_id, record_id FROM licensing_set") if (e, r) in sleepy}
    out: Dict[int, LicenceKey] = {}
    for sid in sorted(rset.keys() | lset.keys()):
        if sids is not None and sid not in sids:
            continue
        ls = lset.get(sid)
        out[sid] = LicenceKey(ls, ",".join(pe.get(sid, [])), sid in tidal, kind.get(sid),
                              rset.get(sid) if ls in sleepy_sets else None)
    return out


def change_days(db: sqlite3.Connection) -> List[int]:
    """Every day on which any `when` the licence answer reads changes: requirements',
    designations', their stamp periods' and the rules a designation sleeps under. Between two of
    these days every answer is the same."""
    whens = []
    for tab in ("requirement", "designation"):
        for (rec,) in db.execute(f"SELECT record FROM {tab}"):
            r = json.loads(rec)
            whens.append(r.get("when"))
            if r.get("steelhead_stamp_during"):
                whens.append(r["steelhead_stamp_during"].get("when"))
    # a designation's `suspended_while` names a rule of its own entry
    for e, rec in db.execute("SELECT entry_id, record FROM designation"):
        for s in json.loads(rec).get("suspended_while") or []:
            for (w,) in db.execute("SELECT when_ FROM rule WHERE entry_id = ? AND rule_id = ?",
                                   (e, s["rule_id"])):
                whens.append(json.loads(w) if w else None)
    runs, _ = segments([when_vector(w) for w in whens])
    return [d for d, _ in runs]


# --------------------------------------------------------------------------------------------
# One key, one day
# --------------------------------------------------------------------------------------------

def _resolved(C: Corpus, key: str) -> dict:
    """A requirement as it binds: a restating record reads the restated one's `who`,
    `satisfied_by` and `water`."""
    r = C.requirements[key]
    rs = r.get("restates")
    if rs:
        o = C.requirements.get(f"{rs['entry_id']}#{rs['id']}")
        if o:
            return dict(r, who=o.get("who"), satisfied_by=o.get("satisfied_by"),
                        water=o.get("water"))
    return r


def holds(db, C: Corpus, sid: int, kind: Optional[str], md) -> dict:
    """The reader's answer for the section on the day, with the G5 steps applied:
    {holds: [key], displaced: {key: [superior key]}, wrong_water: [key], waived: [key],
     not_yet_mapped: [key], also_printed: {key: [key]}, designations: [key], stamp_period: bool}."""
    got = read.requirements_in_force(db, sid, md)
    desig = read._designations_in_force(db, sid, md)
    held: List[str] = []
    wrong: List[str] = []
    order = sorted(got["holds"], key=lambda k: (got["holds"][k] != "sections", k))
    for k in order:
        w = _resolved(C, k).get("water")
        if w and kind and w != kind:
            wrong.append(k)
        else:
            held.append(k)
    superior = [k for k in held if C.requirements[k].get("authority") == "superior"]
    displaced = {k: superior for k in held
                 if superior and C.requirements[k].get("authority") != "superior"
                 and _resolved(C, k).get("satisfied_by")}
    return {"holds": held, "displaced": displaced, "wrong_water": wrong,
            "waived": sorted(got["waived"]), "not_yet_mapped": sorted(got["not_yet_mapped"]),
            "also_printed": {k: v for k, v in sorted(got["also_printed"].items()) if k in held},
            "designations": sorted(f"{d['entry_id']}#{d['id']}" for d in desig),
            "_desig": desig,
            "stamp_period": any(d.get("steelhead_stamp_during") is not None and
                                read.in_force(d["steelhead_stamp_during"].get("when"), md) != "no"
                                for d in desig)}


def _alternatives_here(db, C: Corpus, sid: int) -> List[Tuple[str, dict]]:
    placed = {f"{e}#{a}" for e, a in db.execute(
        "SELECT entry_id, alternative_id FROM alternative_section WHERE sid = ?", (sid,))}
    return [(k, r) for k, r in C.alternatives
            if k in placed or r.get("_placement") in ("province", "not_placed",
                                                       "on_designation")]


def _when(r: dict) -> dict:
    d = r.get("doing") or {"act": "fishing"}
    out = {"act": d.get("act", "fishing")}
    for f in ("species", "lengths"):
        if d.get(f):
            out[f] = d[f]
    if r.get("on"):
        out["on"] = r["on"]
    return out


def _term_holds(t: dict, p: Dict[str, str], desig: Sequence[dict]) -> bool:
    """A licence term applies to this angler here (the page's `termsFor`): its `who` matches, and
    its `classified` class or `units` match a designation in force."""
    return (who_match(t.get("who"), p)
            and (not t.get("classified") or any(d.get("classified") == t["classified"]
                                                for d in desig))
            and (not t.get("units") or any(d.get("unit") in t["units"] for d in desig)))


def _prices(C: Corpus, doc: str, p: Dict[str, str], desig: Sequence[dict]) -> dict:
    """{year?, day?, eight_days?} in dollars for the angler's residency; {} when the book prints
    no price for this document and angler (a stated state, not a gap)."""
    res = p["residency"]
    ts = [r for _, r in C.terms if r.get("document") == doc and _term_holds(r, p, desig)
          and (r.get("fees_cad") or {}).get(res) is not None]
    out: dict = {}
    yr = sorted((r for r in ts if r.get("sold") == "per_licence_year"),
                key=lambda r: not r.get("who"))
    if yr:
        out["year"] = yr[0]["fees_cad"][res]
    day = list(dict.fromkeys(r["fees_cad"][res] for r in ts
                             if r.get("valid_days") == 1 or r.get("sold") == "per_day"))
    if day:
        out["day"] = day
    eight = list(dict.fromkeys(r["fees_cad"][res] for r in ts if r.get("valid_days") == 8))
    if eight:
        out["eight_days"] = eight
    return out


def documents(C: Corpus, h: dict, alts: Sequence[Tuple[str, dict]], p: Dict[str, str],
              ref: Callable[[str], int]) -> dict:
    """What one angler needs on one key and day (consumer 7.7 steps 4.6-7)."""
    freed: List[str] = []
    exempt = []
    for k, x in C.exemptions:
        if who_match(x.get("who"), p):
            exempt.append(ref(k))
            for d in x.get("documents") or []:
                if d not in freed:
                    freed.append(d)
    mine, others, guiding = [], [], []
    buy: Dict[str, dict] = {}
    fish_needs_any = False
    for k in h["holds"]:
        r = _resolved(C, k)
        is_mine = who_match(r.get("who"), p) and not (r.get("who_except")
                                                      and who_match(r["who_except"], p))
        act = (r.get("doing") or {}).get("act", "fishing")
        if not is_mine:
            others.append(ref(k))
            continue
        if act == "guiding":
            guiding.append(ref(k))
            continue
        pres = r.get("presumes") or []
        presumes_freed = bool(pres) and all(d in freed for d in pres)
        paths = [dict(x) for x in r.get("satisfied_by") or []] + \
            [dict(x, alt=ref(ak)) for ak, a in alts
             if f"{a['alternative_to']['entry_id']}#{a['alternative_to']['id']}" == k
             for x in a.get("satisfied_by") or []]
        out_paths = []
        for x in paths:
            q: dict = {}
            if "hold" in x:
                q["need"] = [d for d in x["hold"] if d not in freed]
                fr = [d for d in x["hold"] if d in freed]
                if fr:
                    q["freed"] = fr
            for f in ("accompanied_by", "as", "quota", "alt"):
                if x.get(f) is not None:
                    q[f] = x[f]
            out_paths.append(q)
        displaced = k in h["displaced"]
        row = {"req": ref(k), "when": _when(r), "paths": out_paths}
        if displaced:
            row["displaced_by"] = [ref(s) for s in h["displaced"][k]]
        if presumes_freed:
            row["presumes_freed"] = True
        if pres:
            # "Not needed if you are …": the exemption that frees every presumed document
            by = next((ek for ek, x in C.exemptions
                       if all(d in (x.get("documents") or []) for d in pres)), None)
            if by is not None:
                row["presumes_by"] = ref(by)
        terms = [ref(tk) for tk, t in C.terms if not t.get("fees_cad")
                 and t.get("document") in {d for q in out_paths for d in q.get("need") or []}
                 and _term_holds(t, p, h["_desig"])]
        if terms:
            row["terms"] = terms
        mine.append(row)
        if act == "fishing" and not presumes_freed and not displaced and any(
                q.get("need") or q.get("accompanied_by") or q.get("as") for q in out_paths):
            fish_needs_any = True
        if displaced:
            continue                                         # decision L2
        for q in out_paths:
            for d in q.get("need") or []:
                buy.setdefault(d, {"doc": d, "when": _when(r)})
    docs = sorted(buy.values(), key=lambda b: b["when"]["act"] != "fishing")
    for b in docs:
        b["base"] = b["when"]["act"] == "fishing" and "on" not in b["when"]
        b["prices"] = _prices(C, b["doc"], p, h["_desig"])
    out: dict = {"documents": docs, "none_needed": not fish_needs_any, "requirements": mine}
    if exempt:
        out["exempt"] = {"by": exempt, "from": freed}
    if others:
        out["others"] = others
    if guiding:
        out["guiding"] = guiding
    return out


# --------------------------------------------------------------------------------------------
# The year of every key
# --------------------------------------------------------------------------------------------

def produce(path: str, ref: Optional[Callable[[str], object]] = None,
            stats: Optional[dict] = None) -> Dict[LicenceKey, Dict[int, dict]]:
    """PURE: every licence key's answers, `{LicenceKey: {start_day: {"holds": {...},
    "profiles": [60 answers in `profile_index` order]}}}`. Records are named by `ref(id)` — the
    record id "entry#id" by default, the export's `licensing` index for the wire."""
    db = connect(path)
    try:
        C = corpus(db)
        K = keys(db)
        days = change_days(db)
        ref = ref or (lambda k: k)
        P = profiles()
        contested = contested_sets(db)
        memo: Dict[str, list] = {}
        out: Dict[LicenceKey, Dict[int, dict]] = {}
        for key, sids in K.items():
            sid = sids[0]
            alts = _alternatives_here(db, C, sid)
            seen = [ref(k) for k in considered(db, C, key)]
            year: Dict[int, dict] = {}
            last = None
            for d in days:
                h = holds(db, C, sid, key.kind, month_day(d))
                if stats is not None:
                    stats["key_days_wrong_water"] = stats.get("key_days_wrong_water", 0) + \
                        bool(h["wrong_water"])
                    stats["key_days_displaced"] = stats.get("key_days_displaced", 0) + \
                        bool(h["displaced"])
                wire: dict = {"holds": [ref(k) for k in h["holds"]],
                              "designations": [ref(k) for k in h["designations"]],
                              "stamp_period": h["stamp_period"],
                              "contested": key.licensing_set in contested,
                              "considered": seen}
                for f in ("wrong_water", "waived", "not_yet_mapped"):
                    wire[f] = [ref(k) for k in h[f]]
                wire["displaced"] = {str(ref(k)): [ref(s) for s in v]
                                     for k, v in h["displaced"].items()}
                wire["also_printed"] = {str(ref(k)): [ref(x) for x in v]
                                        for k, v in h["also_printed"].items()}
                mk = dumps([wire, [a for a, _ in alts]])
                if mk not in memo:
                    memo[mk] = [documents(C, h, alts, p, ref) for p in P]
                ans = {"holds": wire, "profiles": memo[mk]}
                s = dumps(wire)
                if s != last:
                    year[d] = ans
                    last = s
            out[key] = year
        return out
    finally:
        db.close()


def considered(db, C: Corpus, key: LicenceKey) -> List[str]:
    """Every record the page's "All licence sources" lists for a part (7.7 step 2): its licensing
    set's records, and every record placed `province`, `on_designation` or `not_placed` unless an
    extent stops it at one of the part's province-exception kinds — or the water is tidal, where
    no province-wide record holds (`read.requirements_in_force`; the page misses this, see the
    report)."""
    own = [f"{e}#{r}" for e, r in db.execute(
        "SELECT entry_id, record_id FROM licensing_set WHERE set_id = ? ORDER BY entry_id, "
        "record_id", (key.licensing_set,))] if key.licensing_set is not None else []
    pe = set(key.province_except.split(",")) - {""}
    glob = [] if key.tidal else [
        k for k, r in sorted(C.records.items())
        if r.get("_placement") in ("province", "on_designation", "not_placed")
        and not any(x.get("outside_area_kind") in pe for x in r.get("extents") or [])]
    return list(dict.fromkeys(own + glob))


def contested_sets(db) -> set:
    """Licensing sets the page flags "Check: part of this water is also marked 'not a Classified
    Water'": a `not_classified` record or a `contested` designation in the set."""
    return {s for s, kind, via in db.execute("SELECT set_id, kind, via FROM licensing_set")
            if kind == "not_classified" or via == "contested"}


def build(path: str, log=print) -> dict:
    """`produce`, interned for inspection and measurement, records named by their export index:
    {keys: [[licensing_set, province_except, tidal, kind, ruleset, year, sections]], years:
    [[[start_day, holds, documents table]]], holds, profile_rows, documents (60 profile-row
    indexes each), profiles}."""
    db = connect(path)
    try:
        C = corpus(db)
        K = keys(db)
    finally:
        db.close()
    stats: dict = {}
    got = produce(path, ref=C.index.__getitem__, stats=stats)
    holds_t, rows_t, tables_t, years_t = Interner(), Interner(), Interner(), Interner()
    out_keys = []
    for key, year in got.items():
        runs = [[d, holds_t(a["holds"]), tables_t([rows_t(r) for r in a["profiles"]])]
                for d, a in year.items()]
        out_keys.append(key.as_list() + [years_t(runs), len(K[key])])
    log(f"  licence: {len(out_keys)} keys, {len(holds_t.rows)} holds, {len(rows_t.rows)} profile "
        f"rows, {len(tables_t.rows)} tables; {stats}")
    return {"keys": out_keys, "years": years_t.rows, "holds": holds_t.rows,
            "profile_rows": rows_t.rows, "documents": tables_t.rows,
            "profiles": ["/".join(p[d] for d, _ in PROFILE_DIMS) for p in profiles()],
            "stats": stats, "_key_index": {k: i for i, k in enumerate(got)}}
