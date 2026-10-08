"""THE ONE KEYING MODULE of the answers layer: the bundle, the export pair it pairs with, the rule
order, the keys — and the words for fish and dates. Every
section producer (`answers.py`'s ladder and answer, `gear.py`, `licence.py`, `display.py`,
`rows.py`) reads its inputs through here; none opens the bundle or cuts a year on its own.

THE BUNDLE IS LOADED ONCE (`load`): every rule (the reader's own cache, `read._rules_of`), every
rule set's bindings, every section's rule key and its water kind, the meta digests.

THE RULE ORDER IS THE EXPORT'S. A rule is referred to by its index in the export's `rules` array,
which is the bundle's rules sorted by `entry_id::rule_id` (`export_codec`: "in `rule_ids` order,
sorted by entry_id, then rule_id"). `Bundle.index` builds that order from the bundle itself, and
`check_export` holds it to the shipped export (an export pair cut from another bundle is refused).

THE KEYS. A section's rule answers depend on its rule set, whether a rainbow over 50 cm is a
steelhead there (`steelhead_water`) and whether the steelhead rules apply there
(`section_steelhead_rules`) — exactly `status_index.compute`'s key, `RuleKey`. A PART of a named
water (the export's `waters[item].parts`) is keyed by the PART KEY (`part_keys`, the reference
harness's `partKey`): (ruleset, licensing_set, steelhead_water, steelhead presence,
steelhead_rules, province_except, home_region) — the rule key plus what the other sections read.
Every section is keyed by the part key; each section's own `scope` says which of its fields its
answers depend on.

THE CALENDAR AND THE SEGMENTS are the delivery's one calendar (`pipeline.deliver.calendar`): the
leap calendar (Jan 1 = 1, Feb 29 = 60, Mar 1 = 61, Dec 31 = 366); a key's year cut where any `when`
the reader reads changes (`calendar.rule_vectors` + `calendar.segments`); the file's segments the
union of every section's cuts (`calendar.segments_of`).
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
from collections import defaultdict
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from pipeline.deliver.bundle import read
from pipeline.deliver.calendar import DAYS  # noqa: F401  (the answers' year)

class AnswersError(RuntimeError):
    """A question the answers layer cannot answer: the build stops, naming it."""


# --------------------------------------------------------------------------------------------
# The bundle
# --------------------------------------------------------------------------------------------

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


def rule_id(k: Tuple[str, str]) -> str:
    return f"{k[0]}::{k[1]}"


#: One rule answer key: what a section contributes to `read.effective_rules_bound` (the delivery's
#: one definition, `types.RuleKey`).
from pipeline.deliver.types import RuleKey  # noqa: E402


@dataclass
class Bundle:
    """The bundle facts every producer reads, loaded once (`load`)."""
    path: str
    rules: dict                                   # (entry, rule) -> rule dict (the reader's)
    index: Dict[Tuple[str, str], int]             # (entry, rule) -> export `rules` index
    sets: Dict[int, List[Tuple[str, str, str]]]   # set_id -> [(entry, rule, via)]
    set_kind: Dict[int, Optional[str]]            # set_id -> water kind (None: no named water)
    digest: dict                                  # meta digests
    _keys: Optional[Dict[RuleKey, int]] = field(default=None, repr=False, compare=False)

    @property
    def keys(self) -> Dict[RuleKey, int]:
        """Every section's rule key -> its section count. LAZY (DATAFLOW P1b, M5): a scan of
        every one of the ~1.96 M sections that no shipped producer reads; tests and tools ask."""
        if self._keys is None:
            db = connect(self.path)
            try:
                steel = {s for (s,) in db.execute("SELECT DISTINCT sid FROM steelhead_water")}
                st_rules = {s for (s,) in db.execute("SELECT sid FROM section_steelhead_rules")}
                keys: Dict[RuleKey, int] = defaultdict(int)
                for sid, set_id in db.execute("SELECT sid, set_id FROM section_ruleset ORDER BY sid"):
                    keys[RuleKey(set_id, sid in steel, sid in st_rules)] += 1
            finally:
                db.close()
            self._keys = dict(sorted(keys.items()))
        return self._keys

    @property
    def rule_ids(self) -> List[str]:
        """The export's `rule_ids`: every rule, in the export's order."""
        return [rule_id(k) for k in sorted(self.index, key=self.index.__getitem__)]


_LOADED: Dict[str, Bundle] = {}


def load(path: Optional[str] = None) -> Bundle:
    """The bundle, read once per path per process."""
    path = bundle_path(path)
    got = _LOADED.get(path)
    if got is not None:
        return got
    if not Path(path).is_file():
        raise AnswersError(f"answers: no bundle at {path}")
    db = connect(path)
    try:
        sets: Dict[int, list] = defaultdict(list)
        for set_id, e, r, via in db.execute("SELECT set_id, entry_id, rule_id, via FROM ruleset "
                                            "ORDER BY set_id, entry_id, rule_id"):
            sets[set_id].append((e, r, via))
        kinds: Dict[int, set] = defaultdict(set)
        for set_id, kind in db.execute(
                "SELECT DISTINCT r.set_id, i.kind FROM section_ruleset r JOIN item_section s "
                "ON s.sid = r.sid JOIN item i ON i.ord = s.ord"):
            kinds[set_id].add(kind)
        mixed = sorted(s for s, v in kinds.items() if len(v) > 1)
        if mixed:
            raise AnswersError(f"answers: {len(mixed)} rule set(s) carry sections of two water "
                               f"kinds (e.g. {mixed[:3]}) — a key needs one kind")
        digest = dict(db.execute("SELECT k, v FROM meta WHERE k IN ('reach_digest', "
                                 "'section_handles', 'version')"))
    finally:
        db.close()
    rules = every_rule(path)
    ids = sorted(rules, key=rule_id)
    B = Bundle(path=path, rules=rules, index={k: i for i, k in enumerate(ids)}, sets=dict(sets),
               set_kind={s: next(iter(v)) for s, v in kinds.items()}, digest=digest)
    _LOADED[path] = B
    return B


# --------------------------------------------------------------------------------------------
# The export pair it pairs with
# --------------------------------------------------------------------------------------------

def load_export(export_dir: Path) -> Tuple[dict, dict]:
    d = Path(export_dir)
    data = json.loads((d / "ui-rules-export.json").read_text(encoding="utf-8"))
    guide = json.loads((d / "ui-rules-guide.json").read_text(encoding="utf-8"))
    return data, guide


def check_export(B: Bundle, data: dict, guide: dict, model: Optional[dict] = None) -> Dict[str, int]:
    """The export pair must be ONE pair, cut from THIS bundle, and its rule sets must be the
    bundle's — otherwise its integer rule refs would point at other rules. Returns the export's
    rule index, `{"entry::rule": index}`."""
    from pipeline.tools.export_codec import FORMAT, expand
    if data.get("about", {}).get("format") != FORMAT or guide.get("about", {}).get("format") != FORMAT:
        raise AnswersError(f"answers: the export pair is not format {FORMAT}")
    if data["about"]["bundle"] != guide["about"]["bundle"]:
        raise AnswersError("answers: ui-rules-export.json and ui-rules-guide.json carry different "
                           "about.bundle digests — not one pair")
    stamp = data["about"]["bundle"]
    for k in ("reach_digest", "section_handles"):
        if B.digest.get(k) != stamp.get(k):
            raise AnswersError(f"answers: the export's about.bundle.{k} {stamp.get(k)!r} is not "
                               f"the bundle's meta.{k} {B.digest.get(k)!r}")
    ids = data["rule_ids"]
    if ids != sorted(ids) or len(set(ids)) != len(ids):
        raise AnswersError("answers: the export's rule_ids are not the codec's order (sorted, unique)")
    if ids != B.rule_ids:
        mine = set(B.rule_ids)
        miss = sorted(mine - set(ids))[:3] + sorted(set(ids) - mine)[:3]
        raise AnswersError(f"answers: the export's rules are not the bundle's (e.g. {miss})")
    model = model if model is not None else expand(data, guide)
    for sid, s in model["rulesets"].items():
        want = sorted((i.split("::", 1)[0], i.split("::", 1)[1], via)
                      for via, members in s.items() if via != "sections" for i in members)
        if want != sorted(B.sets.get(int(sid), [])):
            raise AnswersError(f"answers: export rule set {sid} is not the bundle's set {sid}")
    if len(model["rulesets"]) != len(B.sets):
        raise AnswersError("answers: the export and the bundle hold different numbers of rule sets")
    return {k: i for i, k in enumerate(ids)}


# --------------------------------------------------------------------------------------------
# Part keys: the export's parts, keyed by what their answers can depend on
# --------------------------------------------------------------------------------------------

#: The part key's fields, in order: the reference harness's `partKey` (`reference/golden.js`),
#: with `steelhead_rules` the bundle's fact, then what the licence and the card read beyond it: the
#: water's KIND (lake, stream, wetland — `item.kind`) and whether the part is TIDAL water. Every
#: section of a part agrees on both, or the build stops.
PART_KEY_FIELDS = ("ruleset", "licensing_set", "steelhead_water", "steelhead", "steelhead_rules",
                   "province_except", "home_region", "kind", "tidal")


#: TIDAL WATER IS A DIFFERENT REGULATION SYSTEM (user ruling 2026-10-06, FIX D12): no provincial rule,
#: quota, closure, gear rule, licence or stamp holds on a part the book calls tidal (Nitinat Lake,
#: p.17). Its answers are a DOCUMENTED STATE, never an empty or computed one: the display, gear and
#: licence frames of a tidal part key are `TIDAL_STATE` (its ladder, answer and rows hold no rule by
#: definition — the key's `tidal` says why). The export's `waters[].tidal.guide` says the same words.
TIDAL_NOTE = ("Tidal water: a different regulation system. The B.C. freshwater fishing regulations do "
              "not apply here: no provincial rule, quota, closure, gear rule, licence or stamp. See "
              "the federal (DFO) tidal waters sport fishing regulations or the Fishing BC app; a "
              "federal Tidal Waters Sport Fishing Licence is required.")
TIDAL_STATE = {"tidal": True, "note": TIDAL_NOTE,
               "see": ["DFO tidal waters sport fishing regulations", "Fishing BC app"],
               "licence": "federal Tidal Waters Sport Fishing Licence"}
#: The one scope every tidal part key shares in the sections that answer it with `TIDAL_STATE`
#: (no rule set has a negative id).
TIDAL_SCOPE = RuleKey(-1, False, False)


def is_tidal(key: tuple) -> bool:
    """Whether a part key is tidal water (`PART_KEY_FIELDS` `tidal`)."""
    return bool(key_dict(key)["tidal"])


def key_dict(key: tuple) -> dict:
    return dict(zip(PART_KEY_FIELDS, key))


def rule_key(key: tuple) -> RuleKey:
    """The part key's rule key: all the ladder and the decided answer read of a part."""
    k = key_dict(key)
    return RuleKey(k["ruleset"], k["steelhead_water"], k["steelhead_rules"])


def part_keys(B: Bundle, data: dict) -> Tuple[List[tuple], Dict[str, List[Optional[int]]]]:
    """(keys, parts): `keys` the distinct part tuples (`PART_KEY_FIELDS`), `parts` {item_id: [key
    index per export part, in the export's order]}. A part with no rule set is `None` (wholly
    outside B.C.: the bundle refuses otherwise).

    The parts are THE BUNDLE'S (`part`, DATAFLOW P2, `derived.parts`): this only pairs them with
    the export's, refusing an export whose parts are not exactly the bundle's."""
    from pipeline.deliver.bundle.derived import parts as bundle_parts
    db = connect(B.path)
    try:
        P = bundle_parts(db)
    finally:
        db.close()
    keys: List[tuple] = []
    index: Dict[tuple, int] = {}
    parts: Dict[str, List[Optional[int]]] = {}
    for item, w in data["waters"].items():
        mine = P.get(item, [])
        if len(mine) != len(w["parts"]):
            raise AnswersError(f"answers: water {item} has {len(w['parts'])} export parts, the "
                               f"bundle {len(mine)}")
        row: List[Optional[int]] = []
        for pi, (arr, p) in enumerate(zip(w["parts"], mine)):
            flags = arr[4] if len(arr) > 4 else {}
            shipped = (arr[0], arr[1], tuple(flags.get("province_except") or ()),
                       bool(flags.get("anadromous_rainbow")), flags.get("steelhead"),
                       tuple(flags.get("home_region") or ()))
            own = (None if p.set_id is None else int(p.set_id), p.licensing_set,
                   p.province_except, p.steelhead_water, p.steelhead, p.home_regions)
            if shipped != own:
                raise AnswersError(f"answers: water {item} part {pi}: the export's {shipped} is "
                                   f"not the bundle's {own}")
            if p.set_id is None:
                row.append(None)
                continue
            if flags.get("steelhead_rules") is not p.steelhead_rules:
                raise AnswersError(f"answers: water {item} part {pi}: the export's "
                                   f"steelhead_rules {flags.get('steelhead_rules')!r} is not the "
                                   f"bundle's {p.steelhead_rules}")
            if w.get("kind") != p.kind or B.set_kind.get(p.set_id) != p.kind:
                raise AnswersError(f"answers: water {item} part {pi}: kind {w.get('kind')!r} is "
                                   f"not its part's {p.kind!r} or its rule set's "
                                   f"{B.set_kind.get(p.set_id)!r}")
            key = (p.set_id, p.licensing_set, p.steelhead_water, p.steelhead, p.steelhead_rules,
                   p.province_except, p.home_regions, p.kind, p.tidal)
            if key not in index:
                index[key] = len(keys)
                keys.append(key)
            row.append(index[key])
        parts[item] = row
    return keys, parts


# --------------------------------------------------------------------------------------------
# Interning (format-2 style: a table of distinct values, referred to by index)
# --------------------------------------------------------------------------------------------

class Interner:
    """An interned list: `add(value)` (or calling it) -> its index; equal values (canonical JSON)
    share one."""

    def __init__(self):
        self.rows: list = []
        self._ix: dict = {}

    def add(self, value) -> int:
        s = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        i = self._ix.get(s)
        if i is None:
            i = self._ix[s] = len(self.rows)
            self.rows.append(value)
        return i

    __call__ = add


def dumps(x) -> str:
    return json.dumps(x, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


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
