"""Steelhead in the export.

Which rules and records are the provincial steelhead set (`province_steelhead_member`), where steelhead rules
apply (`steelhead_rules_apply`, the same predicate as `pipeline.atlas.reach.steelhead.Presence.rules_apply`),
and the checks that refuse a part missing a member or a lake bound only by a steelhead row's other line.
"""
from __future__ import annotations

from pipeline.deliver.bundle.rules import catch_and_release
from pipeline.common import entry_id as EID


def is_steelhead_rule(x: dict) -> bool:
    """A rule ABOUT STEELHEAD: its species names steelhead (`ST`), or it names no fish and its
    sentence is about steelhead (Region 5's "steelhead fisheries within the Chilcotin River
    Watershed may be closed"). A group rule whose fish include steelhead ("Trout/char: 5") is not
    one — it is the rainbow/trout quota a big lake rainbow falls under."""
    sp = x["fields"].get("species") or []
    return "ST" in sp or (not sp and "steelhead" in str(x.get("verbatim") or "").lower())


def is_wild_steelhead_release(x: dict) -> bool:
    """The RELEASE of wild steelhead, outright (the province's "All wild steelhead must be released"
    and each steelhead region's "All wild steelhead" line — streams, and the lakes whose own row
    names steelhead; user rulings 2026-10-01): a retention_limit of steelhead alone, take 0, the fish
    may be fished for (not a closure), every wild fish (origin wild, or no origin — Regions 3 and 5
    print "ALL STEELHEAD"), with no date, size, water kind, record duty or quota clause. The
    province's "All wild steelhead must be released" and the zones' "And you must release: All wild
    steelhead" lines are these; a hatchery quota, the annual 10, the record duty, "stop fishing
    after the hatchery quota" and the Region 6 stream closure are not."""
    f = x["fields"]
    return (x.get("type") == "retention_limit" and (f.get("species") or []) == ["ST"]
            and catch_and_release(f)
            and f.get("origin") in (None, "wild")
            and not any(f.get(k) for k in ("when", "lengths", "water", "record_retention",
                                           "within", "while", "caught", "when_targeting"))
            and f.get("period") in (None, "daily"))


def is_steelhead_record(x: dict) -> bool:
    """A licensing record about fishing for steelhead (`doing.species` names `ST`): the stamp."""
    return "ST" in ((x["fields"].get("doing") or {}).get("species") or [])


#: The provincial steelhead entry, and the regions whose own tables name steelhead (p.13, 21, 28,
#: 42, 49; 4, 7A, 7B and 8 print no steelhead rule — 7A's Stellako prints only a stamp waiver).
PROVINCE_STEELHEAD = "zp:steelhead"
STEELHEAD_REGIONS = ("1", "2", "3", "5", "6")
#: THE PROVINCIAL STEELHEAD SET (book p.6, `zp:steelhead`), each member by what it says — so a
#: twin (`steelhead.r1b`, the stamp's `steelhead_targeting_known`) is the same member.
PROVINCE_SET = ("annual hatchery quota", "wild release", "record duty", "stamp")


def province_steelhead_member(x: dict) -> str | None:
    """Which member of the provincial steelhead set a rule or licensing record is, or None."""
    if x.get("entry_id") != PROVINCE_STEELHEAD:
        return None
    f = x["fields"]
    if "kind" in x:                          # a licensing record (a rule record has `type`)
        return "stamp" if x["kind"] == "requirement" and is_steelhead_record(x) else None
    if (f.get("species") or []) != ["ST"]:
        return None
    if f.get("period") == "annual" and f.get("origin") == "hatchery" and (f.get("take") or 0) > 0:
        return "annual hatchery quota"
    if is_wild_steelhead_release(x):
        return "wild release"
    if f.get("record_retention"):
        return "record duty"
    return None


def _members(table: dict, key, known: dict) -> set[str]:
    return {i for via, ids in (table.get(key) or {}).items() if via != "sections"
            for i in ids if i in known}


def steelhead_rules_apply(doc: dict, p: dict) -> bool:
    """A CHECK, not the source: whether the part's rule set carries every rule of the provincial
    steelhead set by RULE ID (`steelhead.PROVINCE_SET_RULES`, base or twin — the reach builder's
    own definition, `Presence.rules_apply`). The export's `steelhead_rules: false` is read from
    the bundle (`section_steelhead_rules`, stored from the run and proved there); this re-reads
    the same rule ids off the shipped rule sets so `steelhead_presence_problems` and the tests can
    hold the shipped flag to them."""
    from pipeline.atlas.reach.steelhead import PROVINCE_SET_RULES, PROVINCE_STEELHEAD as _PS
    if not p.get("ruleset"):
        return False
    have = set()
    for via, ids in (doc["rulesets"].get(p["ruleset"]) or {}).items():
        if via == "sections":
            continue
        for i in ids:
            e, _, r = str(i).partition("::")
            if e == _PS:
                have.add(r[:-1] if r.endswith("b") else r)
    return all(r in have for r in PROVINCE_SET_RULES)


def add_steelhead_rules(waters: dict, applies: dict[tuple[str, int], bool]) -> None:
    """`steelhead_rules` on EVERY part with a rule set — `true` or `false`, the bundle's stored
    answer per (item, part) (`part.steelhead_rules`, decision DF4: a reader never infers it from
    absence) — and `steelhead_rules: false` on a known water none of whose parts carries them.
    A part with no rule set (outside B.C.) carries none: no rule applies there at all."""
    for item_id, w in waters.items():
        any_rules = False
        for n, p in enumerate(w["parts"]):
            if p["ruleset"] is None:
                continue
            apply = applies[(item_id, n)]
            any_rules |= apply
            p["steelhead_rules"] = bool(apply)
        if w.get("steelhead") == "known" and not any_rules:
            w["steelhead_rules"] = False


def _in_steelhead_region(rule_ids) -> bool:
    """A part is in a steelhead region when it carries that region's own zone table (a water
    takes the zone rules of the region it lies in; `ladder.region`)."""
    return any(EID.zone_region(i) in STEELHEAD_REGIONS for i in rule_ids)


def province_set_missing(rule_ids, record_ids, R: dict, L: dict) -> list[str]:
    """The members of the provincial steelhead set a part lacks, in `PROVINCE_SET` order."""
    have = {province_steelhead_member(R[i]) for i in rule_ids if i in R}
    have |= {province_steelhead_member(L[i]) for i in record_ids if i in L}
    return [m for m in PROVINCE_SET if m not in have]


def steelhead_rows(doc: dict) -> set[str]:
    """THE STEELHEAD ROWS, read from the output: every water row (`r…`) with a rule naming
    steelhead (`ST`), and every row a water's `steelhead_rows` names (the rows flagged
    `anadromous_rainbow` or printing the Steelhead Stamp, whose own lines may name no fish)."""
    R = doc["rules"]
    got = {x["entry_id"] for x in R.values() if EID.is_water_row(x["entry_id"])
           and "ST" in (x["fields"].get("species") or [])}
    got |= {e for w in doc["waters"].values() for e in w.get("steelhead_rows") or ()
            if EID.is_water_row(str(e))}
    return got


def _row_bound(doc: dict, p: dict, srows: set[str], w: dict | None = None) -> list[str]:
    """The steelhead rows whose rules a part carries — the part is the BOOK's steelhead water. (A
    steelhead row's OWN water is the book's too, where no rule of it may bind: the Kingcome's row
    prints none. Such a part is known by "regulations" and, flowing, carries
    `anadromous_rainbow` — `_book`.) On a LAKE (`w` not a stream) only the lake's OWN rows count
    (user ruling 2026-10-06, `steelhead.LAKES_ONLY_BY_OWN_ROW`): the Atnarko's closure on Tenas
    Lake does not make it steelhead water."""
    R = doc["rules"]
    if not p["ruleset"]:
        return []
    got = {R[i]["entry_id"] for i in _members(doc["rulesets"], p["ruleset"], R)} & srows
    if w is not None and w.get("kind") != "stream":
        got &= set(w.get("entries") or ())
    return sorted(got)


def _book(doc: dict, w: dict, p: dict, srows: set[str]) -> bool:
    """A part of the BOOK's steelhead water: a steelhead row's rule binds it, or it is steelhead
    water (`anadromous_rainbow`) of a water a steelhead row makes known. (A listed part of such a
    water that is `anadromous_rainbow` lies where steelhead rules apply — it carries the set.)"""
    return bool(_row_bound(doc, p, srows, w)) or (
        bool(p.get("anadromous_rainbow")) and "regulations" in (w.get("steelhead_source") or ()))


def steelhead_lake_problems(doc: dict) -> list[str]:
    """A STEELHEAD RULE SHOWS ON A LAKE ONLY WHERE THE BOOK MAKES THE LAKE STEELHEAD WATER (user
    rulings 2026-10-01, 2026-10-02). Big lake rainbow fall under the rainbow size quota, not the
    steelhead quota, so every provincial and zone steelhead rule binds streams — and the
    provincial set and the zone's wild release reach a lake only where its OWN row is a steelhead
    row (Khartoum, Lois — not Tenas, 2026-10-06; never by the curated list, which binds no rule).
    Refused:
      - a lake part carrying a steelhead rule or stamp record (`is_steelhead_rule`,
        `is_steelhead_record`) — the wild release included — when no steelhead row binds it;
      - on such a lake, a steelhead rule of a row that does not match the lake other than a wild
        release or a member of the provincial set (a zone's "2 hatchery steelhead" stays on
        streams);
      - a steelhead record placed province-wide — it would hold on every lake."""
    R, L = doc["rules"], doc["licensing"]
    out = [f"steelhead record {i} is placed province-wide — it holds on every lake"
           for i, x in L.items() if is_steelhead_record(x) and x.get("placement") == "province"]
    srows = steelhead_rows(doc)
    bad: dict = {}
    for item, w in sorted(doc["waters"].items()):
        if w.get("kind") != "lake":
            continue
        own = set(w.get("entries") or ())
        for n, p in enumerate(w["parts"]):
            book = _book(doc, w, p, srows)
            for table, rec, test, key in (
                    (doc["rulesets"], R, is_steelhead_rule, "ruleset"),
                    (doc["licensing_sets"], L, is_steelhead_record, "licensing_set")):
                for i in sorted(_members(table, p[key], rec)):
                    x = rec[i]
                    if not test(x) or (book and x["entry_id"] in own):
                        continue
                    allowed = book and (province_steelhead_member(x) or (
                        key == "ruleset" and is_wild_steelhead_release(x)))
                    if not allowed:
                        bad.setdefault(i, []).append(item)
    for i, items in sorted(bad.items()):
        items = sorted(set(items))
        out.append(f"steelhead rule {i} shows on {len(items)} lake(s) that are not the book's "
                   f"steelhead water, e.g. {', '.join(items[:3])}")
    return out


def steelhead_set_problems(doc: dict) -> list[str]:
    """EVERY WATER A STEELHEAD ROW BINDS CARRIES THE WHOLE PROVINCIAL STEELHEAD SET, WHEREVER IT IS
    (user rulings 2026-10-02) — and nothing else does; THE CURATED LIST ADDS NONE OF IT. Per part:
      FORWARD  a part carrying any rule of a steelhead row (`steelhead_rows`) carries every member
               of `PROVINCE_SET` (the annual hatchery 10, the wild release, the record duty, the
               stamp), and is "known";
      REVERSE  a part carrying any member of the set is (a) a stream of a steelhead region
               (`_in_steelhead_region`) or (b) bound by a steelhead row — being "known" by the
               curated list alone is NOT a reason;
      LIST     a "known" part no steelhead row binds is known by the curated list, which its
               water's `steelhead_source` says."""
    R, L, out = doc["rules"], doc["licensing"], []
    srows = steelhead_rows(doc)
    for item, w in sorted(doc["waters"].items()):
        for n, p in enumerate(w["parts"]):
            known = p.get("steelhead") == "known"
            tag = f"water {item} part {n}"
            if not p["ruleset"]:
                if known and "curated list" not in (w.get("steelhead_source") or ()):
                    out.append(f"{tag}: known, bound by no rule, and not on the curated list")
                continue
            rules = _members(doc["rulesets"], p["ruleset"], R)
            recs = _members(doc["licensing_sets"], p["licensing_set"], L)
            region = _in_steelhead_region(rules)
            rows = _row_bound(doc, p, srows, w) or (
                list(w.get("steelhead_rows") or ())[:1] if _book(doc, w, p, srows) else [])
            if rows:
                gone = province_set_missing(rules, recs, R, L)
                if gone:
                    out.append(f"{tag}: {rows[0]} binds it, but the part lacks the provincial "
                               f"steelhead {', '.join(gone)}")
                if not known:
                    out.append(f"{tag}: {rows[0]} (a steelhead row) binds it, but it is not "
                               f"known steelhead water")
            elif known and "curated list" not in (w.get("steelhead_source") or ()):
                out.append(f"{tag}: known, but no steelhead row binds it and the water is not on "
                           f"the curated list")
            have = sorted(i for i in rules | recs
                          if province_steelhead_member((R.get(i) or L.get(i))))
            if have and not ((region and w.get("kind") == "stream") or rows):
                out.append(f"{tag}: provincial steelhead {have[0]} on a {w.get('kind')} that is "
                           f"no stream of the steelhead regions and bound by no steelhead row")
    return out


def steelhead_presence_problems(doc: dict) -> list[str]:
    """`steelhead` (known | possible), `anadromous_rainbow` and `steelhead_source` agree with each
    other and with the rules (user rulings 2026-10-01/02/03): a big rainbow is a steelhead
    (`anadromous_rainbow`) exactly on the KNOWN parts of a STREAM (the water's `kind`: a slough or
    canal is one) WHERE STEELHEAD RULES APPLY (`steelhead_rules_apply`) — known by the book or by
    the curated list; every part with a rule set says whether they apply (`steelhead_rules`, true
    or false, DF4) and a known water says `false` exactly when no part carries them; a "possible" part is a stream carrying a steelhead rule; a water's
    `steelhead_source` is present exactly where a part is known, holds "regulations" wherever a
    steelhead row binds a part (with those rows in `steelhead_rows`); the water's roll-up is its
    parts' best."""
    R = doc["rules"]
    srows = steelhead_rows(doc)
    out = []
    for item, w in sorted(doc["waters"].items()):
        got = {p.get("steelhead") for p in w["parts"]}
        top = "known" if "known" in got else "possible" if "possible" in got else None
        if w.get("steelhead") != top:
            out.append(f"water {item}: steelhead {w.get('steelhead')!r}, parts say {top!r}")
        src = w.get("steelhead_source") or []
        if bool(src) != (top == "known") or not set(src) <= {"regulations", "curated list"}:
            out.append(f"water {item}: steelhead_source {src!r} for a {top or 'unmarked'} water")
        bound = [bool(_row_bound(doc, p, srows, w)) for p in w["parts"]]
        if (any(bound) and "regulations" not in src) or bool(w.get("steelhead_rows")) != (
                "regulations" in src):
            out.append(f"water {item}: steelhead_source {src!r} / steelhead_rows "
                       f"{w.get('steelhead_rows')!r} disagree with the steelhead rows binding it")
        stream = w.get("kind") == "stream"
        applies = [steelhead_rules_apply(doc, p) for p in w["parts"]]
        if (w.get("steelhead") == "known" and not any(applies)) != (
                w.get("steelhead_rules") is False) or w.get("steelhead_rules") not in (None, False):
            out.append(f"water {item}: steelhead_rules {w.get('steelhead_rules')!r} for a "
                       f"{w.get('steelhead') or 'unmarked'} water whose parts "
                       f"{'carry' if any(applies) else 'carry no'} steelhead rules")
        for n, p in enumerate(w["parts"]):
            tag, st = f"water {item} part {n}", p.get("steelhead")
            if st not in (None, "known", "possible"):
                out.append(f"{tag}: steelhead {st!r}")
            if p.get("anadromous_rainbow") and not (st == "known" and stream and applies[n]):
                out.append(f"{tag}: anadromous_rainbow on a {st or 'unmarked'} {w['kind']}"
                           + ("" if applies[n] else " no steelhead rule applies to"))
            if st == "known" and stream and applies[n] and not p.get("anadromous_rainbow"):
                out.append(f"{tag}: a known stream where steelhead rules apply and a big rainbow "
                           f"is not a steelhead")
            # DF4: on EVERY part with a rule set, the shipped flag IS whether the rules apply
            if (p.get("steelhead_rules") is not applies[n]) if p["ruleset"] is not None \
                    else "steelhead_rules" in p:
                out.append(f"{tag}: steelhead_rules {p.get('steelhead_rules')!r} on a "
                           f"{st or 'unmarked'} part where steelhead rules "
                           f"{'apply' if applies[n] else 'do not apply'}")
            if w["kind"] != "stream" and st == "possible":
                out.append(f"{tag}: a {w['kind']} marked 'possible'")
            if st == "possible" and p["ruleset"] and not any(
                    is_steelhead_rule(R[i]) for via, ids in doc["rulesets"][p["ruleset"]].items()
                    if via != "sections" for i in ids if i in R):
                out.append(f"{tag}: \"possible\" but no steelhead rule applies")
    return out
