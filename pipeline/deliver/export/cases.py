"""`guide.cases`: one REAL test water for every mechanism of the ladder, with the answer the reader gives.

Every case is found in the data by a predicate (`cases`), its expected answer looked up in the verdict store
(`verdicts.answer_on`). A mechanism with no match is listed as `missing` and refused by `checks.case_problems`.
"""
from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from pathlib import Path
from pipeline.deliver.bundle.rules import catch_and_release
from pipeline.regs.parsing import catalogue as C
from pipeline.common import entry_id as EID
from pipeline.deliver.export.guide_words import STEELHEAD_NO_RULES
from pipeline.deliver.export.steelhead import (
    PROVINCE_STEELHEAD, _in_steelhead_region, is_steelhead_rule, steelhead_rows
)
from pipeline.deliver.export.verdicts import answer_on, verdicts_of


# --------------------------------------------------------------------------------------------
# Test waters: one REAL case of every mechanism, with the answer the reference gives
# --------------------------------------------------------------------------------------------
#
# The consumer builds its page against a handful of waters; each mechanism of the ladder needs at
# least one it can assert against ("add more test waters to cover weird ones … we need to cover all
# orthogonal items", 2026-09-26). Every case is FOUND in the data by a predicate, exactly as the
# guide's examples are (`_Pick`): never a remembered id, so a case that stops existing is replaced
# by the next match, and a mechanism with no match is reported (`problems`) and fails the tests.
#
# A case names its water (`item_id`), the rule set and licensing set its sections carry
# (`waters[item].parts`), a date, a fish, and the EXPECTED answer: `read.effective_rules` for that
# part on that date — every rule that speaks, stands beside or is shown, with `partly_lifted` where
# a lift holds only in part. Section handles never leave the bundle (AGENTS 5): the answer depends
# only on the part's rule set and whether a rainbow over 50 cm is a steelhead there
# (`anadromous_rainbow`), both of which the case carries.

#: THE MECHANISMS, AND WHAT THE PAGE SHOULD SHOW for each: words, so they live in `cases.json` beside this
#: file — {mechanism: what a case of it shows} (its order is the order of `guide.cases`) and {mechanism: one
#: plain line for the page builder}, `{water}` and `{fish}` filled from the case.
_WORDS = json.loads(Path(__file__).with_name("cases.json").read_text(encoding="utf-8"))
CASE_MECHANISMS: dict[str, str] = _WORDS["mechanisms"]
WHAT_TO_SHOW: dict[str, str] = {k: v.replace("{steelhead_no_rules}", STEELHEAD_NO_RULES)
                                for k, v in _WORDS["what_to_show"].items()}


def _leaves(x: dict) -> list[str]:
    """The FISH of the book's list (p.80) a rule is about — what a case may ask about. An open
    subject is not a fish: `ALL_FIN_FISH` is every fish but crayfish (`read.speaks_for`), and
    `PROTECTED_SPECIES` / `SALMON` hold no game fish (a case once asked about the fish
    "PROTECTED_SPECIES")."""
    def fish(codes):
        out = set()
        for c in C.expand_species(list(codes or [])):
            if c == "ALL_FIN_FISH":
                out |= set(C.GAME_FISH)
            elif c in C.BOOK_SPECIES:
                out.add(c)
        return out
    return sorted(fish(x.get("species")) - fish(x.get("species_except")))


def _keeps(x: dict) -> bool:
    from pipeline.deliver.bundle.rules import keeps
    return bool(keeps(x)) and x.get("take") is not None


def _closure(x: dict) -> bool:
    """Any closure, conditioned or not (`rules.closure_grade`, the one closure predicate)."""
    from pipeline.deliver.bundle.rules import closure_grade
    return closure_grade(x) is not None


_DAYS = [(m, d) for m in (7, 8, 6, 9, 5, 10, 4, 11, 3, 12, 2, 1) for d in (1, 15)]


class _Cases:
    """The data a case is found in: the bundle's rules (`read.rules` shape, with ladder rank), its
    interned sets, and the named water parts carrying each."""

    def __init__(self, d: dict, bundle: Path):
        from pipeline.deliver.bundle import read as RD
        self.RD, self.d, self.path = RD, d, str(bundle)
        self.R = RD._rules_of(self.path)
        db = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
        self.members: dict = defaultdict(dict)
        for s, e, r, via in db.execute("SELECT set_id, entry_id, rule_id, via FROM ruleset"):
            if (e, r) in self.R:
                self.members[str(s)][(e, r)] = via
        self.lic: dict = defaultdict(list)
        for s, e, r in db.execute("SELECT set_id, entry_id, record_id FROM licensing_set"):
            self.lic[str(s)].append(f"{e}#{r}")
        # A CASE IS ANSWERED ON ITS OWN WATER AND PART (E4): the part's RULE KEY (`part.key_ix`,
        # the bundle's parts — DATAFLOW P2), whose stored verdict every one of its sections
        # shares. A part of the same rule set on ANOTHER water may differ in steelhead presence,
        # and the `expect` would then describe a different water than `water.item_id` names.
        from pipeline.deliver.bundle.derived import parts as _bundle_parts
        self.sid = {}
        for item, ps in _bundle_parts(db).items():
            for p in ps:
                self.sid[(item, None if p.set_id is None else str(p.set_id),
                          None if p.licensing_set is None else str(p.licensing_set),
                          ",".join(p.province_except) or None, p.steelhead_water,
                          p.steelhead)] = p.key
        self.store = verdicts_of(bundle)
        db.close()
        self.parts = [(it, p) for it, w in sorted(d["waters"].items()) for p in w["parts"]]
        self.first_part: dict = {}
        for it, p in self.parts:
            if p["ruleset"] is not None:
                self.first_part.setdefault(p["ruleset"], (it, p))
        self.sets = sorted(self.first_part, key=int)

    # ---- the answer ----------------------------------------------------------------------
    def section(self, item: str, part: dict):
        return self.sid.get((item, part["ruleset"], part["licensing_set"],
                             ",".join(part.get("province_except") or ()) or None,
                             bool(part.get("anadromous_rainbow")), part.get("steelhead")))

    def key_of(self, item: str, part: dict):
        ident = (item, part["ruleset"], part["licensing_set"],
                 ",".join(part.get("province_except") or ()) or None,
                 bool(part.get("anadromous_rainbow")), part.get("steelhead"))
        if ident not in self.sid:
            raise SystemExit(f"pipeline.deliver.export: case on {item} part {part['ruleset']}/"
                             f"{part['licensing_set']} — the bundle has no such part")
        return self.sid[ident]

    def moments(self, item: str, part: dict) -> list:
        """The part's moments (`calendar.moments`, stored by the verdicts): one unless a rule of
        its set holds some weekdays or hours only."""
        key = self.key_of(item, part)
        return [] if key is None else self.store.moments(key)

    def answer(self, item: str, part: dict, on, fish: str, moment=None) -> list[dict]:
        """The stored verdict for the part's rule key on the day (`verdicts_of`), at `moment`
        (an index into `moments`; required where there are several), as the reader answered on
        any of its sections."""
        key = self.key_of(item, part)
        if key is None:
            return []           # a part wholly outside B.C.: no rule binds it (no rule set)
        return answer_on(self.store, key, on, fish, moment=moment)

    def record(self, mech, item, part, on, fish, because, ans, licensing=False,
               at=None) -> dict:
        w = self.d["waters"][item]
        return {
            "mechanism": mech, "shows": CASE_MECHANISMS[mech],
            "what_to_show": WHAT_TO_SHOW[mech].format(water=w["name"] or item,
                                                      fish=C._SPECIES_WORDS.get(fish, fish)
                                                      .lower()),
            "water": {"item_id": item, "name": w["name"], "kind": w["kind"]},
            "entries": w["entries"],
            "ruleset": part["ruleset"], "licensing_set": part["licensing_set"],
            **({"anadromous_rainbow": True} if part.get("anadromous_rainbow") else {}),
            **({"province_except": part["province_except"]} if part.get("province_except") else {}),
            **({"steelhead": part["steelhead"]} if part.get("steelhead") else {}),
            "date": f"{on[0]:02d}-{on[1]:02d}", "fish": fish,
            **({"at": at} if at is not None else {}),
            "because": sorted(set(because)),
            "expect": ans,
            **({"expect_licensing": sorted(self.lic.get(part["licensing_set"] or "", []))}
               if licensing else {}),
        }

    # ---- helpers the finders share -------------------------------------------------------
    def day(self, *xs, state="yes"):
        return next((on for on in _DAYS
                     if all(self.RD.in_force(x.get("when"), on) == state for x in xs)), None)

    def day_out(self, x):
        return next((on for on in _DAYS if self.RD.in_force(x.get("when"), on) == "no"), None)

    def first(self, mech, cands, check, licensing=False, pick=None):
        """The first candidate whose stored answer passes `check`. Where the part reads
        differently by weekday or hour (several moments), the case is asked at ONE moment — the
        first (`pick` None) or `pick(on, moments)`'s — and states it (`at`)."""
        seen = set()
        for item, part, on, fish, because in cands:
            if on is None or fish is None:
                continue
            key = (part["ruleset"], bool(part.get("anadromous_rainbow")), on, fish)
            if key in seen:
                continue
            seen.add(key)
            ms = self.moments(item, part)
            m = None if len(ms) <= 1 else (pick(on, ms) if pick else 0)
            if len(ms) > 1 and m is None:
                continue
            ans = self.answer(item, part, on, fish, moment=m)
            if check(ans):
                return self.record(mech, item, part, on, fish, because, ans, licensing,
                                   at=None if m is None else ms[m].as_json())
        return None

    def by_set(self, test):
        """(item, part, set members) for every set carried by a named water, `test`ed on its
        members; `test` returns an iterable of (on, fish, because)."""
        for s in self.sets:
            item, part = self.first_part[s]
            for on, fish, because in test(self.members[s]) or ():
                yield item, part, on, fish, because


def _speaking(ans) -> set:
    return {a["id"] for a in ans if a["state"] == "speaks"}


def _ids(ans) -> set:
    return {a["id"] for a in ans}


def cases(d: dict, bundle: Path) -> dict:
    from pipeline.deliver.bundle.rules import release_origins, same_statement
    K = _Cases(d, bundle)
    R, RD = K.R, K.RD
    rid = lambda k: f"{k[0]}::{k[1]}"                              # noqa: E731
    water = lambda k: EID.is_water_row(k[0]) and R[k]["_rank"] == 0  # noqa: E731
    zone = lambda k: EID.is_table(k[0]) and R[k]["_rank"] >= 2   # noqa: E731
    got: dict = {}

    def add(mech, case):
        if case is not None:
            got.setdefault(mech, []).append(case)

    # zone_only: no water row binds; the region's trout/char quota speaks for a rainbow
    def t(m):
        if any(EID.is_water_row(k[0]) for k in m):
            return
        z = [k for k in m if EID.is_table(k[0]) and k[1].endswith("trout_char_quota.r1")]
        if z:
            yield (7, 1), "RB", [rid(z[0])]
    add("zone_only", K.first("zone_only", K.by_set(t),
                             lambda a: any(EID.is_table(i) and "trout_char_quota" in i
                                           for i in _speaking(a))))

    # quota_beside: same fish or group, a size bound or other difference, smaller at the water
    def t(m):
        for w in m:
            a = R[w]
            if not (water(w) and _keeps(a) and not a.get("within") and m[w] == "reach"):
                continue
            for z in m:
                b = R[z]
                if zone(z) and RD.base_region(z[0]) and _keeps(b) and not b.get("within") \
                        and set(_leaves(a)) <= set(_leaves(b)) and not same_statement(a, b) \
                        and (a.get("period") or "daily") == (b.get("period") or "daily") \
                        and a["take"] < b["take"] and _leaves(a):
                    yield K.day(a, b), _leaves(a)[0], [rid(w), rid(z)]
    beside = lambda w, z: lambda a: {w, z} <= _speaking(a)    # noqa: E731
    for c in K.by_set(t):
        case = K.first("quota_beside", [c], beside(*c[4]))
        if case:
            add("quota_beside", case)
            break

    # larger_replaces: a water row's lift of the zone's aggregate for the fish it names larger
    def t(m):
        for w in m:
            a = R[w]
            if not (water(w) and _keeps(a) and m[w] == "reach"):
                continue
            for x in a.get("exempts") or []:
                z = (x["entry_id"], x["rule_id"])
                b = R.get(z)
                if z in m and b and zone(z) and _keeps(b) and not b.get("within") \
                        and a["take"] > b["take"] and not x.get("origin"):
                    fish = sorted(set(_leaves(a)) & set(x.get("species") or _leaves(b)))
                    if fish:
                        yield K.day(a, b), fish[0], [rid(w), rid(z)]
    for c in K.by_set(t):
        w, z = c[4]
        case = K.first("larger_replaces", [c], lambda a: w in _speaking(a) and z not in _ids(a))
        if case:
            add("larger_replaces", case)
            break

    # same_statement: the water's larger number replaces the zone's
    def t(m):
        for w in m:
            for z in m:
                a, b = R[w], R[z]
                if water(w) and zone(z) and _keeps(a) and _keeps(b) and same_statement(a, b) \
                        and not b.get("within") and a["take"] > b["take"] and _leaves(a):
                    yield K.day(a, b), _leaves(a)[0], [rid(w), rid(z)]
    for c in K.by_set(t):
        w, z = c[4]
        case = K.first("same_statement", [c], lambda a: w in _speaking(a) and z not in _ids(a))
        if case:
            add("same_statement", case)
            break

    # water_release: an outright release silencing a zone quota of another dimension
    def t(m):
        for w in m:
            a = R[w]
            if not (water(w) and release_origins(a) == frozenset({"wild", "hatchery"})
                    and not _closure(a)):
                continue
            for z in m:
                b = R[z]
                if zone(z) and _keeps(b) and b.get("dimension") != a.get("dimension"):
                    fish = sorted(set(_leaves(a)) & set(_leaves(b)))
                    if fish:
                        yield K.day(a, b), fish[0], [rid(w), rid(z)]
    for c in K.by_set(t):
        w, z = c[4]
        case = K.first("water_release", [c], lambda a: w in _speaking(a) and z not in _ids(a))
        if case:
            add("water_release", case)
            break

    # naming_beats_group / water_names_fish / within_clause_parent_level
    def t(m):
        for z in m:
            b = R[z]
            if not (zone(z) and b.get("type") == "retention_limit" and not b.get("within")):
                continue
            for w in m:
                a = R[w]
                if water(w) and _keeps(a) and (a["type"], a["dimension"]) == \
                        (b["type"], b["dimension"]):
                    for f in _leaves(a):
                        if RD.names_fish(b, f) and not RD.names_fish(a, f) \
                                and RD.speaks_for(b, f):
                            yield K.day(a, b), f, [rid(z), rid(w)]
                            break
    for c in K.by_set(t):
        z, w = c[4]
        case = K.first("naming_beats_group", [c],
                       lambda a: z in _speaking(a) and w not in _speaking(a))
        if case:
            add("naming_beats_group", case)
            break

    def t(m):
        for w in m:
            a = R[w]
            if not (water(w) and _keeps(a)):
                continue
            for z in m:
                b = R[z]
                if zone(z) and _closure(b) is False and b.get("take") == 0 and \
                        (a["type"], a["dimension"]) == (b["type"], b["dimension"]):
                    for f in _leaves(a):
                        if RD.names_fish(a, f) and RD.names_fish(b, f):
                            yield K.day(a, b), f, [rid(w), rid(z)]
                            break
    for c in K.by_set(t):
        w, z = c[4]
        case = K.first("water_names_fish", [c],
                       lambda a: w in _speaking(a) and z not in _speaking(a))
        if case:
            add("water_names_fish", case)
            break

    def t(m):
        for w in m:
            a = R[w]
            if not (water(w) and release_origins(a) and not _closure(a)):
                continue
            for z in m:
                b = R[z]
                if zone(z) and _keeps(b) and b.get("within"):
                    for f in _leaves(b):
                        if RD.names_fish(b, f) and RD.speaks_for(a, f) \
                                and not RD.names_fish(a, f):
                            yield K.day(a, b), f, [rid(w), rid(z)]
                            break
    for c in K.by_set(t):
        w, z = c[4]
        case = K.first("within_clause_parent_level", [c],
                       lambda a: w in _speaking(a) and z not in _speaking(a))
        if case:
            add("within_clause_parent_level", case)
            break

    # water_closure_silences_zone: a water's dated closure over the zone's named quota
    def t(m):
        for w in m:
            a = R[w]
            if not (water(w) and _closure(a) and (a.get("when") or {}).get("dates")):
                continue
            for z in m:
                b = R[z]
                if zone(z) and _keeps(b) and not b.get("within"):
                    for f in _leaves(b):
                        if f != "CRA" and RD.names_fish(b, f) and RD.speaks_for(a, f):
                            yield K.day(a, b), f, [rid(w), rid(z)]
                            break
    for c in K.by_set(t):
        w, z = c[4]
        case = K.first("water_closure_silences_zone", [c],
                       lambda a: w in _speaking(a) and z not in _speaking(a))
        if case:
            add("water_closure_silences_zone", case)
            break

    # lifts: derived, printed over a species closure, blanket, equivalent, one fish, partly
    def lifts(test):
        def t(m):
            for w in m:
                if m[w] != "reach":
                    continue
                for x in R[w].get("exempts") or []:
                    z = (x["entry_id"], x["rule_id"])
                    if z in m and z in R and test(w, x, z):
                        yield w, x, z
        return t

    def full(x):
        return not any(x.get(k) for k in ("origin", "lengths", "when_targeting", "while",
                                           "caught"))

    def lift_cases(mech, test, fish_of, check):
        finder = lifts(test)
        for s in K.sets:
            item, part = K.first_part[s]
            for w, x, z in finder(K.members[s]):
                f = fish_of(w, x, z)
                if f is None:
                    continue
                on = K.day(R[w], R[z])
                case = K.first(mech, [(item, part, on, f, [rid(w), rid(z)])], check(w, x, z))
                if case:
                    return case
        return None

    def lifted_fish(w, x, z):
        f = sorted(set(x.get("species") or _leaves(R[z])) & set(_leaves(R[w]) or _leaves(R[z])))
        return f[0] if f else None

    gone = lambda w, x, z: lambda a: rid(z) not in _ids(a)     # noqa: E731
    add("derived_lift", lift_cases(
        "derived_lift", lambda w, x, z: x.get("basis") == "names_the_fish" and full(x) and water(w),
        lifted_fish, gone))
    add("printed_lift_reopens_closure", lift_cases(
        "printed_lift_reopens_closure",
        lambda w, x, z: not x.get("basis") and full(x) and water(w) and _closure(R[z])
        and zone(z) and bool(R[z].get("species")) and not x.get("equivalent")
        and set(R[z]["species"]).isdisjoint({"ALL_GAME_FISH", "ALL_FIN_FISH"}),
        lifted_fish, gone))
    add("blanket_closure_exempt", lift_cases(
        "blanket_closure_exempt",
        lambda w, x, z: full(x) and not x.get("equivalent") and water(w)
        and R[w].get("dimension") == "lift" and _closure(R[z]) and bool(R[z].get("water"))
        and set(R[z].get("species") or []) <= {"ALL_GAME_FISH", "ALL_FIN_FISH"},
        lambda w, x, z: "RB", gone))
    add("equivalent_lift", lift_cases(
        "equivalent_lift", lambda w, x, z: bool(x.get("equivalent")) and full(x),
        lambda w, x, z: "RB", gone))
    add("counted_apart", lift_cases(
        "counted_apart",
        lambda w, x, z: zone(w) and w[0] == z[0] and bool(x.get("species")) and full(x)
        and _keeps(R[w]),
        lambda w, x, z: x["species"][0],
        lambda w, x, z: lambda a: rid(w) in _speaking(a) and rid(z) not in _ids(a)))

    def other_fish(w, x, z):
        f = sorted(set(_leaves(R[z])) - set(x["species"]))
        return f[0] if x.get("species") and f else None
    add("lift_for_one_fish", lift_cases(
        "lift_for_one_fish", lambda w, x, z: water(w) and bool(x.get("species")) and full(x),
        other_fish,
        lambda w, x, z: lambda a: rid(z) in _speaking(a)))
    add("partly_lifted", lift_cases(
        "partly_lifted", lambda w, x, z: not full(x), lifted_fish,
        lambda w, x, z: lambda a: any(i["id"] == rid(z) and i.get("partly_lifted") for i in a)))

    # closure_prints_its_exemptions: a region's species closure a water row names, not lifted
    def t(m):
        for z in m:
            b = R[z]
            if not (zone(z) and _closure(b) and b.get("species")
                    and set(b["species"]).isdisjoint(set(read_aggregates()))):
                continue
            for w in m:
                a = R[w]
                if water(w) and _keeps(a) and not any(
                        (x["entry_id"], x["rule_id"]) == z for x in a.get("exempts") or []):
                    for f in _leaves(a):
                        if RD.names_fish(a, f) and RD.names_fish(b, f):
                            yield K.day(a, b), f, [rid(z), rid(w)]
                            break
    for c in K.by_set(t):
        z, w = c[4]
        case = K.first("closure_prints_its_exemptions", [c], lambda a: z in _speaking(a))
        if case:
            add("closure_prints_its_exemptions", case)
            break

    # two_regions_lake: two regions' tables on one lake, the stricter displacing the other. Never a
    # lake the book divides into parts (Williston: its Zone A and Zone B halves each take one
    # table) — the parent's own section there is the sliver the parts leave, not the lake.
    divided = {w["part_of"] for w in K.d["waters"].values() if w.get("part_of")}
    for s in K.sets:
        item, part = K.first_part[s]
        m = K.members[s]
        bases = {RD.base_region(k[0]) for k in m if zone(k)} - {None}
        if len(bases) < 2 or K.d["waters"][item]["kind"] != "lake" or item in divided:
            continue
        found = None
        for f in ("RB", "LT", "KO", "DV", "EB", "BB", "WP", "NP", "MW"):
            ans = K.answer(item, part, (7, 1), f)
            said = _ids(ans)
            spoken = [tuple(i.split("::")) for i in _speaking(ans)]
            # the other table's rule that displaced it: a retention rule about this fish — never
            # a water closure silencing both tables (Kinbasket's undrawn "No Fishing within 200 m
            # of Bush-Sullivan Road Bridge" is held on the whole lake)
            if any(k in R and water(k) and _closure(R[k]) for k in spoken):
                continue
            other = lambda k: [o for o in spoken if o in R and zone(o)  # noqa: E731
                               and RD.base_region(o[0]) not in (None, RD.base_region(k[0]))
                               and R[o].get("type") == "retention_limit"
                               and not R[o].get("standing") and RD.speaks_for(R[o], f)]
            dropped = [k for k in m if zone(k) and RD.base_region(k[0])
                       and R[k].get("type") == "retention_limit"
                       and RD.speaks_for(R[k], f) and RD.in_force(R[k].get("when"), (7, 1)) == "yes"
                       and not R[k].get("standing") and rid(k) not in said and other(k)]
            if dropped:
                found = K.record("two_regions_lake", item, part, (7, 1), f,
                                 [rid(k) for k in dropped]
                                 + [rid(o) for k in dropped for o in other(k)], ans)
                break
        if found:
            add("two_regions_lake", found)
            break

    # home_region_differs: a stream piece whose zone rules are another region's than its row's
    fam = lambda r: EID.book_region(r) if r else r              # noqa: E731
    for s in K.sets:
        item, part = K.first_part[s]
        m = K.members[s]
        if K.d["waters"][item]["kind"] != "stream":
            continue
        bases = {RD.base_region(k[0]) for k in m if zone(k)} - {None}
        rows = {EID.region_of(k[0]) for k in m if water(k) and m[k] == "reach"}
        if len(bases) != 1 or not rows or any(fam(r) == fam(next(iter(bases))) for r in rows):
            continue
        w = sorted(k for k in m if water(k) and m[k] == "reach")[0]
        z = sorted(k for k in m if zone(k) and k[1].endswith("trout_char_quota.r1"))
        case = K.first("home_region_differs", [(item, part, (7, 1), "RB", [rid(w)] + [rid(k) for k in z])],
                       lambda a: any(EID.is_table(i) for i in _speaking(a)))
        if case:
            add("home_region_differs", case)
            break

    # pointer: a pointer-only row's water carries its target's rules
    for eid, e in sorted(K.d["entries"].items()):
        if not e.get("see") or e["rules"] or e["licensing"]:
            continue
        targets = {t for s_ in e["see"] for t in s_.get("entry_ids") or []}
        hit = None
        for it in e["matched"]:
            for p in (K.d["waters"].get(it) or {}).get("parts", []):
                if p["ruleset"] and any(k[0] in targets for k in K.members[p["ruleset"]]):
                    hit = (it, p)
                    break
            if hit:
                break
        if hit:
            case = K.first("pointer", [(hit[0], hit[1], (7, 1), "RB", [eid] + sorted(targets))],
                           lambda a: any(i.split("::")[0] in targets for i in _ids(a)))
            if case:
                add("pointer", case)
                break

    # steelhead_water: a rainbow rule over 50 cm only speaks for no rainbow
    for it, p in K.parts:
        if not (p.get("anadromous_rainbow") and p["ruleset"]):
            continue
        over = [k for k in K.members[p["ruleset"]] if RD.speaks_for(R[k], "RB")
                and R[k].get("lengths") and all((b.get("min_cm") or 0) >= 50
                                                for b in R[k]["lengths"])]
        if over:
            case = K.first("steelhead_water", [(it, p, (7, 15), "RB", [rid(k) for k in over])],
                           lambda a: not ({rid(k) for k in over} & _ids(a)))
            if case:
                add("steelhead_water", case)
                break

    # steelhead: known | possible streams, a lake with none, an own-row steelhead lake
    def steelhead_case(mech, part_test, check, fish="ST"):
        for it, p in K.parts:
            w = K.d["waters"][it]
            if p["ruleset"] and part_test(w, p, set(rid(k) for k in K.members[p["ruleset"]])):
                why = sorted(rid(k) for k in K.members[p["ruleset"]] if is_steelhead_rule(
                    K.d["rules"][rid(k)])) or sorted(w["entries"])
                case = K.first(mech, [(it, p, (9, 1), fish, why)], check, licensing=True)
                if case:
                    add(mech, case)
                    return
    has_province = lambda a: any(i.startswith(PROVINCE_STEELHEAD + "::")    # noqa: E731
                                 for i in _speaking(a))
    steelhead_case("steelhead_known", lambda w, p, m: w["kind"] == "stream"
                   and p.get("steelhead") == "known" and p.get("anadromous_rainbow")
                   and bool(set(w["entries"]) & set(w.get("steelhead_rows") or ())),
                   has_province)
    steelhead_case("steelhead_possible", lambda w, p, m: w["kind"] == "stream"
                   and p.get("steelhead") == "possible" and not p.get("anadromous_rainbow"),
                   has_province)
    steelhead_case("lake_no_steelhead", lambda w, p, m: w["kind"] == "lake" and w["entries"]
                   and not p.get("steelhead") and _in_steelhead_region(m)
                   and not any(is_steelhead_rule(K.d["rules"][i]) for i in m),
                   lambda a: not any(is_steelhead_rule(K.d["rules"][i]) for i in _ids(a)
                                     if i in K.d["rules"]))
    own_row = lambda w: any(is_steelhead_rule(K.d["rules"][i]) for e in w["entries"]  # noqa: E731
                            for i in (K.d["entries"].get(e) or {}).get("rules") or ()
                            if i in K.d["rules"])
    steelhead_case("own_row_steelhead_lake", lambda w, p, m: w["kind"] == "lake"
                   and p.get("steelhead") == "known" and own_row(w), has_province)
    _srows = steelhead_rows(K.d)
    steelhead_case("lake_bound_by_a_steelhead_rows_other_line", lambda w, p, m: w["kind"] == "lake"
                   and not p.get("steelhead") and any(
                       K.d["rules"][i]["entry_id"] in _srows
                       and K.d["rules"][i]["entry_id"] not in w["entries"] for i in m),
                   lambda a: not any(is_steelhead_rule(K.d["rules"][i]) for i in _ids(a)
                                     if i in K.d["rules"]))
    steelhead_case("steelhead_known_beyond_the_regions", lambda w, p, m: w["kind"] == "stream"
                   and p.get("steelhead") == "known" and p.get("anadromous_rainbow")
                   and "regulations" in (w.get("steelhead_source") or ())
                   and not _in_steelhead_region(m), has_province)
    # 2026-10-02: the curated list is a presence indicator — a listed water outside the steelhead
    # regions is known and carries no steelhead rule (the Okanagan River in Region 8); 2026-10-03:
    # no steelhead rule applies there (`steelhead_rules: false`), so a rainbow of any size is a
    # rainbow — the case asks about a rainbow, and the water's own rainbow line answers
    steelhead_case("steelhead_known_by_the_list", lambda w, p, m: w["kind"] == "stream"
                   and w["entries"]
                   and p.get("steelhead") == "known" and p.get("steelhead_rules") is False
                   and not p.get("anadromous_rainbow")
                   and w.get("steelhead_source") == ["curated list"]
                   and not _in_steelhead_region(m)
                   and not any(is_steelhead_rule(K.d["rules"][i]) for i in m),
                   lambda a: not any(is_steelhead_rule(K.d["rules"][i]) for i in _ids(a)
                                     if i in K.d["rules"]), fish="RB")

    # dated rules, in and out of force (the same water, fish and part)
    def t(m):
        for w in m:
            a = R[w]
            wh = a.get("when") or {}
            if water(w) and _keeps(a) and wh.get("dates") and not (
                    wh.get("hours") or wh.get("weekdays") or wh.get("unparsed")) and _leaves(a):
                yield K.day(a), _leaves(a)[0], [rid(w)]
    for c in K.by_set(t):
        (w,) = c[4]
        inside = K.first("dated_in_force", [c], lambda a: w in _speaking(a))
        e_, _, r_ = w.partition("::")
        out = K.day_out(R[(e_, r_)])
        outside = inside and K.first("dated_out_of_force", [c[:2] + (out,) + c[3:]],
                                     lambda a: w not in _ids(a))
        if inside and outside:
            add("dated_in_force", inside)
            add("dated_out_of_force", outside)
            break

    # semicolon_dated_clause: "A; B, <dates>" — A all year
    for s in K.sets:
        item, part = K.first_part[s]
        m = K.members[s]
        hit = None
        for a_ in m:
            for b_ in m:
                a, b = R[a_], R[b_]
                if a_[0] != b_[0] or a_ == b_ or a.get("when") or \
                        not (b.get("when") or {}).get("dates"):
                    continue
                text = K.d["entries"][a_[0]]["printed"] or ""
                if f"{a['verbatim']}; {b['verbatim'][:12]}" in text:
                    hit = (a_, b_)
                    break
            if hit:
                break
        if hit:
            a_, b_ = hit
            case = K.first("semicolon_dated_clause",
                           [(item, part, K.day_out(R[b_]), "RB", [rid(a_), rid(b_)])],
                           lambda a: rid(a_) in _ids(a) and rid(b_) not in _ids(a))
            if case:
                add("semicolon_dated_clause", case)
                break

    # one rule of a kind, in the answer on a day it is in force
    def single(mech, test, check=None, state="yes", pick=None):
        for s in K.sets:
            item, part = K.first_part[s]
            for k in sorted(K.members[s]):
                x = R[k]
                if not test(k, x) or (water(k) and K.members[s][k] != "reach"):
                    continue
                f = (_leaves(x) or ["RB"])[0]
                if x.get("when_targeting"):
                    f = C.expand_species(list(x["when_targeting"]))[0]
                on = K.day(x, state=state)
                ok = check(k) if check else (lambda a, k=k: rid(k) in _ids(a))
                case = K.first(mech, [(item, part, on, f, [rid(k)])], ok,
                               pick=None if pick is None else (lambda on, ms, x=x: pick(x, on, ms)))
                if case:
                    add(mech, case)
                    return

    # a rule held some hours or weekdays DECIDES at the moments it covers (user ruling
    # 2026-10-08): asked at the first moment it is in force, it speaks
    part_day = lambda x: bool((x.get("when") or {}).get("hours") or (x.get("when") or {})
                              .get("weekdays"))
    single("part_day_decides", lambda k, x: part_day(x),
           lambda k: lambda a: any(i["id"] == rid(k) and i["state"] == "speaks" for i in a),
           state="part",
           pick=lambda x, on, ms: next((i for i, m in enumerate(ms)
                                        if RD.in_force(x.get("when"), on, m) == "yes"), None))
    # suspended_while: no rule carries it today; a licensing record does ("Steelhead Stamp not
    # required until reopened to steelhead fishing") — the case asks on a day its closure speaks.
    for it, p in K.parts:
        if not (p["ruleset"] and p["licensing_set"]):
            continue
        hit = None
        for i in K.lic.get(p["licensing_set"], []):
            x = K.d["licensing"].get(i)
            for sw in (x or {}).get("fields", {}).get("suspended_while") or []:
                k = (x["entry_id"], sw["rule_id"])
                if k in K.members[p["ruleset"]]:
                    hit = (i, k)
        if hit:
            i, k = hit
            case = K.first("suspended_while", [(it, p, K.day(R[k]), "ST", [i, rid(k)])],
                           lambda a: rid(k) in _speaking(a), licensing=True)
            if case:
                add("suspended_while", case)
                break
    single("size_band", lambda k, x: water(k) and _keeps(x) and any(
        b.get("min_cm") is not None and b.get("max_cm") is not None and b.get("take") != 0
        for b in x.get("lengths") or []))
    single("origin", lambda k, x: water(k) and bool(x.get("origin")) and _keeps(x))
    single("while_or_targeting", lambda k, x: water(k) and bool(x.get("while")
                                                                 or x.get("when_targeting")))
    single("annual_clock", lambda k, x: x.get("period") == "annual")
    single("superior_authority", lambda k, x: R[k]["_rank"] < 0,
           lambda k: lambda a: rid(k) in _speaking(a))
    single("standing", lambda k, x: bool(x.get("standing")))
    single("angler_closure", lambda k, x: x.get("type") == "angler_closure")
    for s in K.sets:
        item, part = K.first_part[s]
        m = K.members[s]
        trib = [k for k in sorted(m) if m[k] == "trib" and water(k)
                and R[k].get("type") == "retention_limit"]
        if trib:
            k = trib[0]
            case = K.first("tributary_walk", [(item, part, K.day(R[k]),
                                               (_leaves(R[k]) or ["RB"])[0], [rid(k)])],
                           lambda a: rid(k) in _speaking(a))
            if case:
                add("tributary_walk", case)
                break
    single("watershed_part", lambda k, x: any(e.get("watershed") for e in x.get("extents") or []))
    single("undrawn_part", lambda k, x: bool(x.get("undrawn_part")),
           lambda k: lambda a: any(i["id"] == rid(k) and i["state"] == "not_yet_mapped"
                                   for i in a))

    def gear_only(m):
        own = [k for k in m if water(k) and m[k] == "reach"]
        return own and all(R[k]["type"] in ("tackle_restriction", "bait_restriction",
                                            "method_rule") for k in own)
    for s in K.sets:
        item, part = K.first_part[s]
        if gear_only(K.members[s]):
            own = sorted(k for k in K.members[s] if water(k))
            case = K.first("gear_only", [(item, part, (7, 1), "RB", [rid(k) for k in own])],
                           lambda a: any(rid(k) in _ids(a) for k in own))
            if case:
                add("gear_only", case)
                break

    # ---- sample waters added 2026-09-26 ----------------------------------------------------
    # clause_only_lift: a water number lifting only a clause; the fish still counts toward the
    # parent aggregate, which speaks
    def parent(z):
        return (z[0], R[z]["within"]) if R[z].get("within") else None
    add("clause_only_lift", lift_cases(
        "clause_only_lift",
        lambda w, x, z: water(w) and full(x) and not x.get("when") and zone(z)
        and parent(z) in R and not any((y["entry_id"], y["rule_id"]) == parent(z)
                                       for y in R[w].get("exempts") or []),
        lifted_fish,
        lambda w, x, z: lambda a: rid(w) in _speaking(a) and rid(z) not in _ids(a)
        and rid(parent(z)) in _speaking(a)))

    # dated_lift: on a day the lifter's dates exclude, the lifted rule speaks again
    def dated_lift_cases():
        for s in K.sets:
            item, part = K.first_part[s]
            m = K.members[s]
            for w in sorted(m):
                if m[w] != "reach" or not water(w):
                    continue
                for x in R[w].get("exempts") or []:
                    z = (x["entry_id"], x["rule_id"])
                    if z not in m or not x.get("when") or x.get("origin") or x.get("lengths"):
                        continue
                    f = lifted_fish(w, x, z)
                    for on in _DAYS:
                        if f and RD.in_force(x["when"], on) == "no" \
                                and RD.in_force(R[z].get("when"), on) == "yes":
                            yield item, part, on, f, [rid(w), rid(z)]
    for c in dated_lift_cases():
        w, z = c[4]
        case = K.first("dated_lift", [c], lambda a: z in _speaking(a) and w not in _ids(a))
        if case:
            add("dated_lift", case)
            break

    # same_statement_smaller: the water's smaller number replaces the zone's same statement
    def t(m):
        for w in m:
            for z in m:
                a, b = R[w], R[z]
                if water(w) and zone(z) and _keeps(a) and _keeps(b) and same_statement(a, b) \
                        and not b.get("within") and a["take"] < b["take"] and _leaves(a):
                    yield K.day(a, b), _leaves(a)[0], [rid(w), rid(z)]
    for c in K.by_set(t):
        w, z = c[4]
        case = K.first("same_statement_smaller", [c],
                       lambda a: w in _speaking(a) and z not in _ids(a))
        if case:
            add("same_statement_smaller", case)
            break

    # streams_only_zone_clause: "2 from streams" speaking on a stream
    for s in K.sets:
        item, part = K.first_part[s]
        if K.d["waters"][item]["kind"] != "stream":
            continue
        m = K.members[s]
        hit = [k for k in sorted(m) if zone(k) and R[k].get("water") == "stream"
               and R[k].get("within") and _keeps(R[k]) and not R[k].get("origin")
               and _leaves(R[k])]
        if not hit:
            continue
        k = hit[0]
        case = K.first("streams_only_zone_clause",
                       [(item, part, K.day(R[k]), _leaves(R[k])[0], [rid(k)])],
                       lambda a, k=k: rid(k) in _speaking(a))
        if case:
            add("streams_only_zone_clause", case)
            break

    # two_regions_closure / two_regions_release: what displaced the other region's rule
    shut = _closure                                                   # the one predicate
    released = lambda x: catch_and_release(x) and not x.get("lengths")  # noqa: E731
    for mech, how in (("two_regions_closure", shut), ("two_regions_release", released)):
        found = None
        for s in K.sets:
            item, part = K.first_part[s]
            m = K.members[s]
            bases = {RD.base_region(k[0]) for k in m if zone(k)} - {None}
            if len(bases) < 2 or K.d["waters"][item]["kind"] != "lake" or item in divided:
                continue
            for f in ("RB", "LT", "KO", "DV", "EB", "BB", "WP", "NP", "MW", "GR", "WSG"):
                for on in _DAYS:
                    ans = K.answer(item, part, on, f)
                    spoken = [tuple(i.split("::")) for i in _speaking(ans)]
                    said = _ids(ans)
                    for k in m:
                        if not (zone(k) and RD.base_region(k[0]) and rid(k) not in said
                                and R[k].get("type") == "retention_limit"
                                and RD.speaks_for(R[k], f) and not R[k].get("standing")
                                and RD.in_force(R[k].get("when"), on) == "yes"
                                and (mech == "two_regions_closure" or _keeps(R[k]))
                                and not shut(R[k])):
                            continue
                        by = [o for o in spoken if o in R and zone(o)
                              and RD.base_region(o[0]) not in (None, RD.base_region(k[0]))
                              and how(R[o]) and RD.speaks_for(R[o], f)
                              and RD.stricter(R[o], R[k])]
                        if by:
                            found = K.record(mech, item, part, on, f,
                                             [rid(k)] + [rid(o) for o in by], ans)
                            break
                    if found:
                        break
                if found:
                    break
            if found:
                break
        add(mech, found)

    # keep_beside_release: a clause keeping some of a fish beside a release of the others
    def t(m):
        for k1 in m:
            a = R[k1]
            if not (_keeps(a) and a.get("origin") and a.get("take")):
                continue
            for k2 in m:
                b = R[k2]
                if k2 != k1 and catch_and_release(b) \
                        and not b.get("lengths") and b.get("origin") \
                        and b["origin"] != a["origin"]:
                    fish = sorted(set(_leaves(a)) & set(_leaves(b)))
                    if fish:
                        yield K.day(a, b), fish[0], [rid(k1), rid(k2)]
    for c in K.by_set(t):
        k1, k2 = c[4]
        case = K.first("keep_beside_release", [c],
                       lambda a: k1 in _speaking(a) and k2 in _speaking(a))
        if case:
            add("keep_beside_release", case)
            break

    # undrawn_closure: an undrawn closure shown as a note while the zone speaks (Kinbasket first)
    order = sorted(K.sets, key=lambda s_: (not any("kinbasket" in k[0]
                                                    for k in K.members[s_]), int(s_)))
    for s in order:
        item, part = K.first_part[s]
        m = K.members[s]
        und = [k for k in sorted(m) if R[k].get("undrawn_part") and _closure(R[k])]
        zq = [k for k in sorted(m) if zone(k) and _keeps(R[k]) and not R[k].get("within")
              and R[k].get("species")]
        if not (und and zq):
            continue
        k, z = und[0], zq[0]
        f = next((x for x in _leaves(R[z]) if RD.speaks_for(R[k], x)), None)
        on = K.day(R[k], R[z])
        case = K.first("undrawn_closure", [(item, part, on, f, [rid(k), rid(z)])],
                       lambda a, k=k: any(i["id"] == rid(k) and i["state"] == "not_yet_mapped"
                                          for i in a)
                       and any(EID.is_table(i) for i in _speaking(a)))
        if case:
            add("undrawn_closure", case)
            break

    # size_clause_caution: a lift of a region size clause, carrying its caution
    add("size_clause_caution", lift_cases(
        "size_clause_caution",
        # only a row printing "(any size)" carries the caution (user ruling 2026-09-28)
        lambda w, x, z: water(w) and bool(x.get("caution")) and not x.get("origin")
        and "any size" in R[w]["verbatim"],
        lifted_fish,
        lambda w, x, z: lambda a: rid(w) in _speaking(a) and rid(z) not in _ids(a)))

    # named sample waters: Kootenay Lake's rainbow 10, the Dean beside, a straddling lake
    def named(mech, entry_prefix, fish, check, rule_test=lambda x: True, on=None):
        eids = sorted(e for e in K.d["entries"] if e.startswith(entry_prefix))
        for eid in eids:
            for it in K.d["entries"][eid]["matched"]:
                for p_ in (K.d["waters"].get(it) or {}).get("parts", []):
                    m = K.members.get(p_["ruleset"] or "", {})
                    mine = [k for k in sorted(m) if k[0] == eid and m[k] == "reach"
                            and rule_test(R[k]) and RD.speaks_for(R[k], fish)]
                    if not mine:
                        continue
                    day = on or K.day(*(R[k] for k in mine[:1])) or (7, 1)
                    zs = [k for k in sorted(m) if zone(k) and k[1].endswith("trout_char_quota.r1")]
                    case = K.first(mech, [(it, p_, day, fish, [rid(k) for k in mine[:1] + zs])],
                                   check(mine[0], zs))
                    if case:
                        return case
        return None
    add("kootenay_rainbow_10", named(
        "kootenay_rainbow_10", "r4:kootenay_lake_main_body", "RB",
        lambda w, zs: lambda a: rid(w) in _speaking(a) and not (set(map(rid, zs)) & _ids(a)),
        lambda x: x.get("take") == 10))
    add("dean_beside", named(
        "dean_beside", "r5:dean_river@", "RB",
        lambda w, zs: lambda a: rid(w) in _speaking(a) and bool(zs)
        and set(map(rid, zs)) <= _speaking(a),
        lambda x: _keeps(x) and "TROUT_CHAR" in (x.get("species") or [])))
    add("trout_word_same_statement", named(
        "trout_word_same_statement", "r1:amor_lake@", "RB",
        lambda w, zs: lambda a: rid(w) in _speaking(a)
        and "z1:trout_quota::trout_quota.r1" not in _ids(a),
        lambda x: _keeps(x) and "TROUT_CHAR" in (x.get("species") or [])))
    add("dated_zone_release_stands", named(
        "dated_zone_release_stands", "r3:shuswap_lake_see_maps", "LT",
        lambda w, zs: lambda a: rid(w) in _speaking(a)
        and "z3:trout_char_quota::trout_char_quota.r7" in _speaking(a),
        lambda x: _keeps(x) and "CHAR" in (x.get("species") or []) and x.get("take") == 1,
        on=(11, 1)))
    add("water_dates_override", named(
        "water_dates_override", "r6:cheslatta_lake", "LT",
        lambda w, zs: lambda a: rid(w) in _speaking(a)
        and "z6:trout_char_quota::trout_char_quota.r8" not in _speaking(a),
        lambda x: _keeps(x) and x.get("species") == ["LT"] and x.get("take") == 3
        and not x.get("within"), on=(11, 15)))
    add("one_side_beside", named(
        "one_side_beside", "r6:kitimat_river_angling", "RB",
        lambda w, zs: lambda a: any(i["id"] == rid(w) and i["state"] == "beside" for i in a)
        and any(i.startswith("r6:kitimat") for i in _speaking(a)),
        lambda x: bool(x.get("side")), on=(7, 1)))
    straddle = None
    for name in ("Ahbau Lake", "Mara Lake"):
        for it, w in sorted(K.d["waters"].items()):
            if w["name"] != name or w["kind"] != "lake":
                continue
            for p_ in w["parts"]:
                m = K.members.get(p_["ruleset"] or "", {})
                bases = {RD.base_region(k[0]) for k in m if zone(k)} - {None}
                if len(bases) < 2:
                    continue
                zs = sorted(k for k in m if zone(k) and k[1].endswith("trout_char_quota.r1"))
                straddle = K.first("straddling_named_lake",
                                   [(it, p_, (7, 1), "RB", [rid(k) for k in zs])],
                                   lambda a: len({i.split(":")[0] for i in _ids(a)
                                                  if EID.is_zone(i)}) == 2)
                if straddle:
                    break
            if straddle:
                break
        if straddle:
            break
    add("straddling_named_lake", straddle)

    # ---- CLEAN round (2026-10-05): one clean case per RU ruling, and the two lakes the consumer
    # asked about. Each is a NAMED water (by item id where the name repeats), its first part whose
    # rule set holds every rule of `because`, on a fixed day; the `expect` is the reference
    # reader's (`effective_rules`), and the check is what the mechanism claims of it. A check that
    # fails leaves the case out, and `missing` says so.
    def water_case(mech, item, on, fish, because, check):
        w = K.d["waters"].get(item)
        for p_ in (w or {}).get("parts", []):
            m = {rid(k) for k in K.members.get(p_["ruleset"] or "", {})}
            if set(because) <= m:
                case = K.first(mech, [(item, p_, on, fish, list(because))], check)
                if case:
                    return case
        return None

    def item_named(name, kind):
        got = sorted(i for i, w in K.d["waters"].items() if w["name"] == name and w["kind"] == kind)
        return got[0] if len(got) == 1 else None

    def says(speak=(), absent=()):
        return lambda a: set(speak) <= _speaking(a) and not (set(absent) & _ids(a))

    thompson = "r3:thompson_river_downstream_of_signs_at_kamloops_lake_outlet_t@3-13+3-14+3-18::" \
               "thompson_river_downstream_of_kamloops_lake"
    alta, bridge = "r2:alta_lake@2-9::alta_lake", "r5:bridge_lake@5-2::bridge_lake"
    z2, z3, z4 = "z2:trout_char_quota::trout_char_quota", "z3:trout_char_quota::trout_char_quota", \
        "z4:trout_char_quota::trout_char_quota"
    z5, z6, z7a = "z5:trout_char_quota::trout_char_quota", "z6:trout_char_quota::trout_char_quota", \
        "z7a:trout_char_quota::trout_char_quota"
    gr = "z7b:arctic_grayling::arctic_grayling"
    for mech, item, on, fish, speak, absent in (
            # RU-3: the CNR stretch's May release over the row's undated 2 (gnis:39492 is the
            # Thompson's item id; its CNR-stretch part carries both)
            ("same_row_dated_release", "gnis:39492", (5, 15), "RB",
             [f"{thompson}.r3"], [f"{thompson}.r2"]),
            ("same_row_dated_closure", "wbk:329303451", (12, 15), "RB",
             [f"{alta}.r1"], [f"{alta}.r2"]),
            ("zone_release_any_water", item_named("Sukunka River", "stream"), (5, 15), "GR",
             [f"{gr}.r4"], [f"{gr}.r1", f"{gr}.r3"]),
            ("size_release_vs_size_clause", item_named("Lakelse Lake", "lake"), (7, 1), "RB",
             ["r6:lakelse_lake@6-11::lakelse_lake.r1", f"{z6}.r1"], [f"{z6}.r2"]),
            ("closure_any_key", item_named("Kicking Horse River", "stream"), (5, 1), "CT",
             ["z4:spring_stream_closure::spring_stream_closure.r1"],
             [f"{z4}.r1", f"{z4}.r2", f"{z4}.r3"]),
            ("two_regions_same_statement", item_named("Ahbau Lake", "lake"), (7, 1), "RB",
             [f"{z5}.r1", f"{z5}.r2"], [f"{z7a}.r1", f"{z7a}.r2"]),
            ("zone_size_clause_moot", "wbk:329054723", (11, 1), "LT",
             [f"{z3}.r7"], [f"{z3}.r4b", f"{z3}.r1", f"{z3}.r4"]),
            ("water_release_silences_size_clause", "wbk:329518152", (11, 1), "LT",
             ["r3:griffin_lake@3-34::griffin_lake.r1"], [f"{z3}.r7", f"{z3}.r4b", f"{z3}.r1"]),
            ("bridge_lake_same_statement", "wbk:329060770", (7, 1), "LT",
             [f"{bridge}.r1", f"{z5}.r1", f"{z5}.r2"], [f"{z5}.r5"]),
            ("bridge_lake_dated_zone_release", "wbk:329060770", (11, 15), "LT",
             [f"{bridge}.r1", f"{z5}.r7"], []),
            ("alta_lake_clause_size_release", "wbk:329303451", (7, 1), "RB",
             [f"{alta}.r2", f"{alta}.r3", f"{z2}.r2"], [f"{z2}.r1"])):
        if item:
            add(mech, water_case(mech, item, on, fish, sorted(set(speak) | set(absent)),
                                 says(speak, absent)))

    # licensing, the province's exceptions, and water outside B.C.
    L = K.d["licensing"]

    def lic_case(mech, test, part_test=lambda p: True):
        for it, p in K.parts:
            if p["ruleset"] and p["licensing_set"] and part_test(p) and any(
                    test(L[i]) for i in K.lic.get(p["licensing_set"], []) if i in L):
                why = [i for i in K.lic[p["licensing_set"]] if i in L and test(L[i])]
                case = K.first(mech, [(it, p, (7, 1), "RB", why)], lambda a: True,
                               licensing=True)
                if case:
                    add(mech, case)
                    return
    lic_case("licensing_classified", lambda x: x["kind"] == "designation"
             and bool(x["fields"].get("classified")))
    lic_case("licensing_stamp", lambda x: "stamp" in json.dumps(x["fields"]))
    lic_case("province_except", lambda x: True, lambda p: bool(p.get("province_except")))
    for it, w in sorted(K.d["waters"].items()):
        if w["outside_bc"]:
            p = next((p for p in w["parts"] if p["ruleset"] is None
                      and p["licensing_set"] is None), None)
            if p:
                case = K.first("outside_bc", [(it, p, (7, 1), "RB", [])], lambda a: a == [],
                               licensing=True)
                if case:
                    add("outside_bc", case)
                    break

    return {
        "reading": "SAMPLE WATERS for the UI to look at and build against while it is being "
                   "built out — not a test oracle. One or more real waters per mechanism, most "
                   "found in the data by a test (some named: Kootenay Lake, the Dean, "
                   "Ahbau/Mara, the Thompson's CNR stretch, Lakelse, Bridge, Alta, Bonaparte and "
                   "Griffin lakes), each with a plain `what_to_show` line and the answer the "
                   "reference semantics give today "
                   "(`pipeline.deliver.bundle.read.effective_rules`). Read one as: on the "
                   "water `water.item_id`, for the part whose sections "
                   "carry `ruleset` (and `licensing_set`; `anadromous_rainbow` where a rainbow "
                   "over 50 cm is a steelhead; `steelhead`, known | possible, where steelhead "
                   "rules apply — the part's own), on `date` (MM-DD, any year), for `fish` (a leaf "
                   "code), at `at` where the part reads differently by weekday or hour (the "
                   "moment asked: `weekdays`, and `hours` with `in`), the rules in play are "
                   "`expect` — each `speaks`, stands "
                   "`beside` (of unreadable season, or one side of the channel), is "
                   "`shown` (standing, information), or is `not_yet_mapped` (a part nobody has "
                   "drawn: show it at the top, it decides nothing); `partly_lifted` where a lift "
                   "holds only for some anglers or fish. `what_to_show` is one plain line for "
                   "the page builder. `because` names the records that make it this "
                   "mechanism. A licensing case also carries `expect_licensing`: every record "
                   "its licensing set holds. The ids are the export's own; section handles "
                   "never leave the bundle.",
        "mechanisms": CASE_MECHANISMS,
        "missing": sorted(set(CASE_MECHANISMS) - set(got)),
        "cases": [c for mech in CASE_MECHANISMS for c in got.get(mech, [])],
    }


def read_aggregates():
    from pipeline.deliver.bundle.rules import AGGREGATE_GROUPS
    return AGGREGATE_GROUPS
