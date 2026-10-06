"""What every answers table shares: the bundle, the rule order, the keys, the year's segments and
the words for fish and dates.

THE RULE ORDER IS THE EXPORT'S. A rule is referred to by its index in the export's `rules` array,
which is the bundle's rules sorted by `entry_id::rule_id` (`export_codec`: "in `rule_ids` order,
sorted by entry_id, then rule_id"). `rule_index` builds that order from the bundle itself, and a
test holds it to the shipped export.

THE KEYS ARE THE STATUS INDEX'S. A section's rule answers depend on its rule set, whether a rainbow
over 50 cm is a steelhead there (`steelhead_water`) and whether the steelhead rules apply there
(`section_steelhead_rules`) — exactly `status_index.compute`'s key. Gear adds the water KIND (a
gear clause may hold `when: {water: stream}`), which a rule set never mixes (measured: 0 of 2,156
sets carry sections of two kinds; a set none of whose sections is in a named water has kind None).

THE SEGMENTS ARE THE READER'S SIGNATURE. A segment is a run of days on which every bound rule's
`when` and every lift's `when` read the same (`status_index.set_profile`'s memo key); within it
the reader gives the same answer. Each `when` is turned into its 366-day vector ONCE, so cutting a
set's year is a union of change days, not 366 reader questions.
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from functools import lru_cache
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from pipeline.deliver.bundle import read

DAYS = 366


def bundle_path(path: Optional[str] = None) -> str:
    """The bundle to read: the argument, else `UI_EXPORT_BUNDLE` (the tests' override, as
    `test_status_index` reads it), else the reader's default."""
    return str(path or os.environ.get("UI_EXPORT_BUNDLE") or read.BUNDLE)


def connect(path: str) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)


def every_rule(path: str) -> dict:
    """`{(entry_id, rule_id): rule}` — the reader's own cache (`read._rules_of`), each rule with
    its ladder rank `_rank` (`read.source_of`)."""
    return read._rules_of(path)


def rule_index(path: str) -> Dict[Tuple[str, str], int]:
    """Each rule's index in the export's `rules` array: the bundle's rules in `entry::rule`
    order."""
    ids = sorted(every_rule(path), key=lambda k: f"{k[0]}::{k[1]}")
    return {k: i for i, k in enumerate(ids)}


def month_day(day: int) -> Tuple[int, int]:
    from pipeline.deliver.status_index import month_day as md
    return md(day)


# --------------------------------------------------------------------------------------------
# Keys
# --------------------------------------------------------------------------------------------

@dataclass(frozen=True, order=True)
class RuleKey:
    """One rule answer key: what a section contributes to `read.effective_rules_bound`."""
    set_id: int
    steelhead_water: bool
    steelhead_rules: bool

    def as_list(self) -> list:
        return [self.set_id, int(self.steelhead_water), int(self.steelhead_rules)]


@dataclass
class Bundle:
    """The bundle facts every table reads, loaded once (no section id leaves this object)."""
    path: str
    rules: dict                                   # (entry, rule) -> rule dict
    index: Dict[Tuple[str, str], int]             # (entry, rule) -> export rules index
    sets: Dict[int, List[Tuple[str, str, str]]]   # set_id -> [(entry, rule, via)]
    keys: Dict[RuleKey, int]                      # key -> section count
    key_sid: Dict[RuleKey, int]                   # key -> its smallest section
    set_kind: Dict[int, Optional[str]]            # set_id -> water kind (None: no named water)
    digest: dict                                  # meta digests


def load(path: Optional[str] = None) -> Bundle:
    path = bundle_path(path)
    db = connect(path)
    try:
        sets: Dict[int, list] = defaultdict(list)
        for set_id, e, r, via in db.execute("SELECT set_id, entry_id, rule_id, via FROM ruleset "
                                            "ORDER BY set_id, entry_id, rule_id"):
            sets[set_id].append((e, r, via))
        steel = {s for (s,) in db.execute("SELECT DISTINCT sid FROM steelhead_water")}
        st_rules = {s for (s,) in db.execute("SELECT sid FROM section_steelhead_rules")}
        keys: Dict[RuleKey, int] = defaultdict(int)
        key_sid: Dict[RuleKey, int] = {}
        for sid, set_id in db.execute("SELECT sid, set_id FROM section_ruleset ORDER BY sid"):
            k = RuleKey(set_id, sid in steel, sid in st_rules)
            keys[k] += 1
            key_sid.setdefault(k, sid)
        kinds: Dict[int, set] = defaultdict(set)
        for set_id, kind in db.execute(
                "SELECT DISTINCT r.set_id, i.kind FROM section_ruleset r JOIN item_section s "
                "ON s.sid = r.sid JOIN item i ON i.ord = s.ord"):
            kinds[set_id].add(kind)
        mixed = sorted(s for s, v in kinds.items() if len(v) > 1)
        if mixed:
            raise SystemExit(f"answers: {len(mixed)} rule set(s) carry sections of two water "
                             f"kinds (e.g. {mixed[:3]}) — gear needs one kind per key")
        digest = dict(db.execute("SELECT k, v FROM meta WHERE k IN ('reach_digest', "
                                 "'section_handles', 'version')"))
    finally:
        db.close()
    return Bundle(path=path, rules=every_rule(path), index=rule_index(path), sets=dict(sets),
                  keys=dict(sorted(keys.items())), key_sid=key_sid,
                  set_kind={s: next(iter(v)) for s, v in kinds.items()}, digest=digest)


# --------------------------------------------------------------------------------------------
# Segments
# --------------------------------------------------------------------------------------------

_CODE = {"no": 0, "yes": 1, "part": 2}


@lru_cache(maxsize=None)
def _vector(when_json: str) -> Tuple[int, ...]:
    when = json.loads(when_json)
    return tuple(_CODE[read.in_force(when, month_day(d))] for d in range(1, DAYS + 1))


def when_vector(when: Optional[dict]) -> Tuple[int, ...]:
    """A `when` as its 366 day codes (0 no, 1 yes, 2 part), on the catalogue's leap calendar."""
    return _vector(json.dumps(when or None, sort_keys=True))


@lru_cache(maxsize=None)
def _changes(vec: Tuple[int, ...]) -> frozenset:
    return frozenset(d for d in range(2, DAYS + 1) if vec[d - 1] != vec[d - 2])


def rule_vectors(rules: Iterable[dict]) -> List[Tuple[int, ...]]:
    """Every `when` the reader reads for these rules: each rule's own and each of its lifts'."""
    out = []
    for x in rules:
        out.append(when_vector(x.get("when")))
        for lift in x.get("exempts") or []:
            if "when" in lift:
                out.append(when_vector(lift.get("when")))
    return out


def segments(vectors: Sequence[Tuple[int, ...]]) -> Tuple[List[List[int]], List[int]]:
    """The year cut where any vector changes: `runs` [[start_day, reading]] (contiguous, covering
    1..366) and `readings` [first start day of each distinct reading]. A reading recurs (both
    sides of a winter closure read alike) and is computed once."""
    cuts = {1}
    for v in vectors:
        cuts |= _changes(v)
    runs: List[List[int]] = []
    seen: Dict[tuple, int] = {}
    readings: List[int] = []
    for d in sorted(cuts):
        sig = tuple(v[d - 1] for v in vectors)
        i = seen.get(sig)
        if i is None:
            i = seen[sig] = len(readings)
            readings.append(d)
        if not runs or runs[-1][1] != i:
            runs.append([d, i])
    return runs, readings


# --------------------------------------------------------------------------------------------
# Interning (format-2 style: a table of distinct values, referred to by index)
# --------------------------------------------------------------------------------------------

class Interner:
    def __init__(self):
        self.rows: list = []
        self._ix: dict = {}

    def __call__(self, value) -> int:
        s = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        i = self._ix.get(s)
        if i is None:
            i = self._ix[s] = len(self.rows)
            self.rows.append(value)
        return i


# --------------------------------------------------------------------------------------------
# Words: fish and dates, as the export's species table and the page write them
# --------------------------------------------------------------------------------------------

def _species():
    from pipeline.regs.parsing import catalogue as C
    return C


def sp_name(code: str) -> str:
    """A fish or group's display name (`catalogue._SPECIES_WORDS`, the export's `species` names)."""
    return _species()._SPECIES_WORDS.get(code) or code


_PROPER = re.compile(r"^(Dolly|Arctic|Nooksack|Salish|Cultus|Enos|Coastal|Westslope|Misty|Vananda"
                     r"|Paxton|Hadley|Morrison|Rocky|Speckled|Charlotte)")


def lc(s: str) -> str:
    """Lower-case a name in a sentence, except one starting with a proper noun."""
    return s if _PROPER.match(s) else s[:1].lower() + s[1:]


def cap(s: str) -> str:
    return s[:1].upper() + s[1:] if s else s


def join(names: Sequence[str], conj: str = " and ") -> str:
    names = list(names)
    if len(names) < 2:
        return "".join(names)
    return ", ".join(names[:-1]) + conj + names[-1]


@lru_cache(maxsize=None)
def _closed_groups() -> Tuple[Tuple[str, frozenset], ...]:
    C = _species()
    out = []
    for g in sorted(C.SPECIES_GROUPS):                   # the export's `species.groups` order
        if g in C.OPEN_SUBJECTS:
            continue
        out.append((g, frozenset(C.expand_species([g]))))
    return tuple(out)


def group_for(codes: Sequence[str]) -> Optional[str]:
    """The closed group whose members are exactly these fish, by name ("Trout and char")."""
    s = set(codes)
    if len(s) < 2:
        return None
    for g, members in _closed_groups():
        if members == s:
            return sp_name(g)
    return None


def expand(codes: Optional[Sequence[str]]) -> List[str]:
    """A species list with each CLOSED group replaced by its members (the page's `expand`); an
    open subject (ALL_FIN_FISH, PROTECTED_SPECIES, SALMON) has no members and drops out."""
    C = _species()
    out: List[str] = []
    for c in codes or []:
        if c in C.SPECIES_GROUPS:
            ms = [] if c in C.OPEN_SUBJECTS else C.expand_species([c])
        else:
            ms = [c]
        for m in ms:
            if m not in out:
                out.append(m)
    return out


def lc_names(codes: Sequence[str], conj: str = " and ") -> str:
    g = group_for(codes)
    if g:
        return lc(g)
    return join([lc(sp_name(c)) for c in codes], conj)


MON = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def when_dates(when: Optional[dict]) -> List[Tuple[int, int]]:
    """`when.dates` as (MMDD, MMDD) pairs; a pair whose start is after its end wraps New Year."""
    return [(d["from_month"] * 100 + d["from_day"], d["to_month"] * 100 + d["to_day"])
            for d in (when or {}).get("dates") or []]


def fmt_md(md: int) -> str:
    return f"{MON[md // 100 - 1]} {md % 100}"


def range_txt(a: int, b: int) -> str:
    if a == b:
        return fmt_md(a)
    if a // 100 == b // 100:
        return f"{fmt_md(a)}–{b % 100}"
    return f"{fmt_md(a)}–{fmt_md(b)}"


def clean(s) -> str:
    return re.sub(r"\s+", " ", re.sub(r"\*+", "", str(s or ""))).strip()


def dumps(x) -> str:
    return json.dumps(x, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
