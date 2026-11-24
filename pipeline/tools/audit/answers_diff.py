"""THE CHANGED-ANSWERS LIST (T809; the trout table build spec, section 5 ticket K, section 7): every answer that differs
between two BUILT releases, per section and day range, grouped by kind of change, in plain words for the owner.

    python -m pipeline.tools.audit.answers_diff <release dir A> <release dir B> [--out DIR]

A is "before", B is "after". It reads the releases only, through the reference decoders the release already has:
the regs store reassembled from the release (`pipeline.deliver.store.release.province_store`, its `answers()` and
`export_subset()`, the section codecs of `pipeline.deliver.answers.encode`) and the salmon files
(`pipeline.deliver.salmon.decode_files`). It compares DECODED answers, never bytes: a file re-ordered or re-encoded with
the same answers gives an empty list. It has no rule logic of its own beyond comparing and classifying.

WHAT IS COMPARED, per section of the map and per day (worked in day ranges: the union of both releases' segment and run
starts, so every day is covered once; a weekday or hours moment is compared per weekday and inside / outside its window):

  regs (one answer key per section, `regs-sid`)
    fish        per fish and origin (hatchery / wild): the decided status (keep / release / closed / no limit), the
                daily number, the deciding rule (`rows` decided)
    size        per fish and origin: the decided answer's size lines (`cap`, `rel`, `outersize` with their cm bounds and
                take)
    possession  per fish and origin: the possession rules the decided answer names and the number they let you have
                per length (a rule's `take` and `lengths`, or `per_daily` times the total the fish counts toward: its
                group's total, never a share under it, `possession_bases`); the same rules with the same numbers whose
                count moved only with that day number are not listed (the limit's own change says it)
    keeping     the KEEPING LAW, alone and together (`Keeping`, `joint_changes`): what each side lets you keep here, by
                the baskets it accepts — each fish's own answer, pooled rows' numbers, the real daily limit's shared
                counts and caps, a group's total, its most here and its cap system, and from rows 34 the trout table
                read by J's decoder (`trout.check.Table`) — compared with J's method (every fish alone at every length
                piece, seeded random baskets, every limit filled to its number and one past it); a fish whose own answer
                changed is left out. A difference is a `shared_limit` in its direction
    closure     the whole water closed to all fishing (`display.status` closed) and the rules closing it
    gear        each gear element's verdict (bait, lures, methods, barbs), hook, fly only, bait ban, the counted gear
                (lines, hooks, weight, hook gap: the deciding rule's most / least, and its words)
    boats       the boat rules in force (`gear.vessel`)
    licence     per angler profile: the documents needed (or none); the classified-water designations (the closures to
                the non-resident alien, `alien_closures`, by the net below, their rules as rule ids)
    water       the key's steelhead presence, steelhead rules, water kind, tidal flag
    the rest    every section frame (ladder, rows, gear, licence, display) whole, rule refs as rule ids: a difference no
                field above explains is listed as "wording only" (only text fields differ) or "other" (its field paths
                named) — never dropped. A rows frame whose difference is only the card's layout (the rows' order and
                grouping, the fish it asks about, the steelhead line, the page's possession row; nothing under
                `LAW_PATH_KEYS`: a decided answer, a group's or a row's numbers, a stamp) with the keeping law the same
                is "the fish card's rows" (it says when rows split, join or reorder). The year strips (`rows[].year`,
                `display.strip`) are left out of this net: they restate the per-day answers that are compared directly.
  salmon (one chain per section, the salmon shards)
    per species (chinook, coho, sockeye, pink, chum): the day's verdict (open / release / closed / check, its limit
    tree and words) and the place that answers it (`a` into `l`: a DFO place or a `book:` closure); the gear lines; the
    chain's own words and caveats. Release-wide: each region's header, the unmapped and off-map places.
  release-wide
    rule wording (a rule id whose title changed), waters and parts that appear or go.

THE KINDS: closes, opens, limit lower, limit higher, size stricter, size looser, size mixed, wild/hatchery, possession,
shared limit, gear, licence, the fish card's rows, wording only, source only (same answer, another rule or place
answers), new water or part, removed water or part, other. Each change says which way it errs: toward closed, toward
open, no change to what you may do, or unclear. Possession, the keeping law and counted gear read a direction from their
numbers (fewer, a new limit: toward closed), and so does a gear ban that comes or goes; "unclear" is left for a
change both ways or one with no number. Groups: kind, then the rule or place behind it before and after (so 71,046
sections of 71 rule + place pairs are 71 lines), with section and water counts, the day ranges per fish, the water
names and one worked example.

ACROSS THE TROUT CUTOVER (rows `CUTOVER_ROWS`, spec ticket L): each release is decoded at its own rows version, so the
last release before the cutover and the cutover candidate are one run. Where the new side has a trout table, the old
frame is split as the cutover splits it and the rows the table COVERS lose `group` / `elsewhere` on both sides (the
parts of a split row their stamps; `page_possession` everywhere: `cutover_views`); every other field and every row the
table does not cover is compared. The trout groups' law is compared with the table's by the keeping law (above): the
old side's group caps against the new side's table as J reads it. The table's WORDS are J's to prove (against the
engine, every build); possession is compared through the decided answers' possession rules on the old side's group
totals.

SECTIONS ACROSS TWO ATLASES. Same `section_handles` digest: a handle is the same section in both. Otherwise a stream
section is matched by its stream key and start metre (`lines`), and a re-cut one by the other release's section holding
its midpoint on the same stream; a lake section by its water and part (`part_section`). What matches nothing is "new
water or part" / "removed water or part".

OUTPUT (`--out`, default `<generated scratch>/answers-diff/<A>-vs-<B>`): `diff.json` (complete, machine readable),
`diff.txt` (for the main chat; also printed), `owner.md` (one page, plain words). An identical pair prints "No answer
changed". Exit 0 always, unless a release cannot be read: then a clear error and exit 2 — never a partial result.
Deterministic: every list is sorted.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import sqlite3
import sys
import tempfile
import time
from bisect import bisect_right
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from pipeline.common import calendar_spec as CAL
from pipeline.deliver.answers.model.rows import PAGE_POSSESSION_SINCE, page_possession_written

DAYS = CAL.DAYS                                         # 366, the leap calendar (Jan 1 = 1, Feb 29 = 60)
ALL_DAYS = ((1 << DAYS) - 1) << 1                       # bits 1..366
ORIGINS = ("hatchery", "wild")
WEEKDAYS = CAL.WEEKDAYS                                 # Monday first, the spec's one list
ATOMS = tuple((w, inside) for w in WEEKDAYS for inside in (False, True))
SECTION_NAMES = ("ladder", "rows", "gear", "licence", "display")

REGS_RANK = {"closed": 0, "release": 1, "keep": 2, "no_limit": 3}
SALMON_RANK = {"closed": 0, "check": 1, "release": 2, "open": 3}
SALMON_SPECIES = ("CH", "CO", "SK", "PK", "CM")         # `pipeline.deliver.salmon.SPECIES`, the chain's order
SALMON_NAMES = {"CH": "chinook", "CO": "coho", "SK": "sockeye", "PK": "pink", "CM": "chum"}

#: kind -> (heading words, which way it errs when the kind alone says so)
KINDS = {
    "closes": ("Closes", "closed"),
    "opens": ("Opens", "open"),
    "limit_lower": ("Lower limit", "closed"),
    "limit_higher": ("Higher limit", "open"),
    "size_stricter": ("Size rule stricter", "closed"),
    "size_looser": ("Size rule looser", "open"),
    "size_mixed": ("Size rule changed both ways", "unclear"),
    "wild_hatchery": ("Wild or hatchery", None),
    "possession": ("Possession", None),
    "shared_limit": ("Shared limits (several fish together)", None),
    "gear": ("Gear and bait, boats", None),
    "licence": ("Licences and classified waters", None),
    "new_part": ("New water or part", "unclear"),
    "removed_part": ("Removed water or part", "unclear"),
    "other": ("Other changes (read these)", "unclear"),
    "card_rows": ("The fish card's rows (same answer for every fish)", "none"),
    "wording": ("Wording only", "none"),
    "source_only": ("Same answer, another rule or place behind it", "none"),
}
ERRS_WORDS = {"closed": "Errs toward closed.", "open": "Errs toward open.", "none": "No change to what you may do.",
              "unclear": "Direction unclear: read it."}

#: leaf keys whose values are words only (a difference only there is "wording only")
TEXT_KEYS = frozenset({"t", "says", "title", "plain", "label", "text", "sentence", "detail", "words", "w", "name",
                       "applies", "applies_trib", "heading", "lost", "won", "status_words", "note", "caution", "why",
                       "when", "days", "verbatim", "tp", "url", "asof", "headline", "fold", "duties"})
#: rule refs, per section (ints index the release's `rule_ids`; a pair/list's first element where it is [rule, n])
RULE_KEYS = {
    "rows": frozenset({"win", "narrow", "pool", "r", "rules", "outer", "wins", "of", "lifts", "by", "src"}),
    "gear": frozenset({"by", "over", "cites", "main", "clause", "decides", "repeats", "caught", "while_rules", "rule",
                       "specs",
                       "active", "timed", "in_part", "side", "lifts", "downrigger", "light", "rules"}),
    "display": frozenset({"closing", "closers", "unless_opened", "by", "lifts"}),
    # (T1020) T789 G8's closures to the non-resident alien, `alien_closures[].rule`: a rule's export index like any
    # other (rule ids are per entry, AGENTS rule 8; a release that renumbers its rules moves it). Read as an index,
    # 28,714 sections of the 0.8.4 -> 0.8.5 pair were "other" with the same closure on both sides, and a closure that
    # really changed rule at an unchanged index would have been missed.
    "licence": frozenset({"rule"}),
}
LIC_KEYS = frozenset({"holds", "designations", "considered", "wrong_water", "waived", "not_yet_mapped", "req", "terms",
                      "by", "presumes_by"})
#: rule refs where EVERY int under the key is a rule index (a list of rules, not a [rule, n] pair): gear's conduct lines
#: [[name, [rule, ...]], ...] per heading
ALL_REF_KEYS = {"gear": frozenset({"conduct"})}
#: rule refs held as ONE [rule, n] pair (n an entry index, never a rule): gear's `by` on a count or an element, a
#: circumstance's `clause` (`ClauseRef`)
PAIR_KEYS = {"gear": frozenset({"by", "clause"})}
#: derived fields left out of the whole-frame net (module doc)
DERIVED = {"rows": ("year",), "display": ("strip",)}

#: THE TROUT CUTOVER, KEYED OFF THE ROWS VERSION (spec ticket L; contract 2.7, Q-D9, 2.11): from rows
#: `CUTOVER_ROWS` (= F0's `PAGE_POSSESSION_SINCE`, the one number) a row the trout table COVERS (its cell's `covers`)
#: carries no `group` and no `elsewhere` (the table says them), a mixed row is split, and every frame carries
#: `page_possession`. Across that line those are not changes (`cutover_views`); the covered groups' law is compared with
#: the table's (`joint_changes`). A row no table covers keeps its `group` and `elsewhere` and is compared whole. Two
#: releases on the same side of the line are compared as they are.
CUTOVER_ROWS = PAGE_POSSESSION_SINCE
ROWS34_TROUT_DROPS = ("group", "elsewhere")


class DiffError(SystemExit):
    """A release that cannot be read: the run stops naming it (exit 2), never a partial list."""

    def __init__(self, msg: str):
        super().__init__(2)
        self.msg = msg

    def __str__(self) -> str:
        return self.msg


# ------------------------------------------------------------------------------------------------------- day words

def _md(day: int) -> str:
    m, d = CAL.month_day(day)
    return f"{CAL.MONTHS[m - 1]} {d}"


def mask_of(lo: int, hi: int) -> int:
    """Days lo..hi (1..366) as a bit mask."""
    return ((1 << (hi - lo + 1)) - 1) << lo


def runs_of_mask(mask: int) -> List[Tuple[int, int]]:
    out, d = [], 1
    while d <= DAYS:
        if mask >> d & 1:
            e = d
            while e + 1 <= DAYS and mask >> (e + 1) & 1:
                e += 1
            out.append((d, e))
            d = e + 1
        else:
            d += 1
    return out


def days_words(mask: int) -> str:
    """'all year', 'Jan 1 to Jun 15', 'Nov 1 to Mar 31' (a run over the new year is one), several joined by ','.
    Feb 29 (day 60) alone never makes a gap."""
    if mask & ALL_DAYS == ALL_DAYS or mask | (1 << 60) == ALL_DAYS:
        return "all year"
    runs = runs_of_mask(mask | ((1 << 60) if (mask >> 59 & 1 and mask >> 61 & 1) else 0))
    if len(runs) > 1 and runs[0][0] == 1 and runs[-1][1] == DAYS:
        runs = [(runs[-1][0], runs[0][1])] + runs[1:-1]
    return ", ".join(_md(a) if a == b else f"{_md(a)} to {_md(b)}" for a, b in runs)


def first_day(mask: int) -> int:
    return (mask & -mask).bit_length() - 1


# ------------------------------------------------------------------------------------------------------- the releases

def _manifest(d: Path) -> dict:
    p = d / "manifest.json"
    if not p.is_file():
        raise DiffError(f"answers_diff: no release at {d} (no manifest.json)")
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except ValueError as e:
        raise DiffError(f"answers_diff: {p} is not JSON ({e})")


def _file_bytes(d: Path, manifest: dict, name: str) -> bytes:
    f = manifest["files"].get(name)
    if f is None:
        raise DiffError(f"answers_diff: {d.name}'s manifest names no {name!r} file")
    p = d.parent / f["url"]
    if not p.is_file():
        raise DiffError(f"answers_diff: {d.name}: {p} (its {name!r} file) is missing")
    blob = p.read_bytes()
    raw = gzip.decompress(blob) if f["url"].endswith(".gz") else blob
    if f.get("sha256") and hashlib.sha256(raw).hexdigest() != f["sha256"]:
        raise DiffError(f"answers_diff: {d.name}: {p.name} is not the file its manifest names (sha256)")
    return raw


class Release:
    """One built release, decoded: the regs answers, the salmon chains, where each section lies."""

    def __init__(self, d, handles_file=None):
        from pipeline.deliver import salmon as SAL
        from pipeline.deliver.answers.encode import CODECS
        from pipeline.deliver.store.release import province_store
        self.dir = Path(d)
        self.manifest = _manifest(self.dir)
        self.name = self.manifest.get("data_release") or self.dir.name
        hs = {f.get("section_handles") for f in self.manifest.get("files", {}).values() if f.get("section_handles")}
        if len(hs) != 1:
            raise DiffError(f"answers_diff: {self.name}: its files name {len(hs)} section-handle digests ({sorted(hs)})")
        self.handles = hs.pop()
        # (2026-11-17) the atlas's handle table, when given (`--handles-a/-b`): handle -> section id, checked by digest
        self.ids: Optional[List[str]] = None
        if handles_file is not None:
            from pipeline.common import section_handles as SH
            hf = Path(handles_file)
            hf = hf / SH.FILENAME if hf.is_dir() else hf
            if SH.digest(hf) != self.handles:
                raise DiffError(f"answers_diff: {hf} is handle table {SH.digest(hf)}, {self.name} is {self.handles}")
            ids = hf.read_text(encoding="utf-8").split("\n")
            self.ids = ids[:-1] if ids and ids[-1] == "" else ids
        try:
            store = province_store(self.dir)
            self.wire = store.answers()
            sub = store.export_subset(check=True)
        except (Exception, SystemExit) as e:                       # noqa: BLE001 — re-raised as the run's one error
            raise DiffError(f"answers_diff: cannot read the regs store of {self.dir}: {type(e).__name__}: {e}")
        self.codecs = CODECS
        self.rule_ids: List[str] = sub["rule_ids"]
        self.lic_ids: List[str] = sub["licensing_ids"]
        self.lic_labels = [sub["licensing"][i].get("label") or i for i in self.lic_ids]
        self.sections = self.wire["sections"]
        missing = [n for n in SECTION_NAMES if n not in self.sections]
        if missing:
            raise DiffError(f"answers_diff: {self.name} holds no {missing} answers section(s)")
        self.versions = {n: self.sections[n]["version"] for n in SECTION_NAMES}
        if "trout" in self.sections:                              # the trout table (read by J's decoder: module doc)
            self.versions["trout"] = self.sections["trout"]["version"]
        #: the rows version this release was cut at: from `CUTOVER_ROWS` on, trout rows drop `group` / `elsewhere`
        #: and every frame carries `page_possession` (`cutover_views`)
        self.rows_version = int(self.versions["rows"])
        #: set by `run`: the pair crosses the trout cutover (one release before rows 34, the other at or after it)
        self.straddle = False
        #: the trout table (contract 2.7: required from rows `CUTOVER_ROWS`, optional before it)
        self.has_trout = "trout" in self.sections
        if self.rows_version >= CUTOVER_ROWS and not self.has_trout:
            raise DiffError(f"answers_diff: {self.name} is rows {self.rows_version} and holds no trout section "
                            "(contract 2.7: required from the cutover)")
        self._trout_cache: Dict[int, tuple] = {}
        #: each rule's structured fields (the export subset), by rule index: the possession and gear-count numbers
        self.rule_fields: List[dict] = [(sub["rules"].get(r) or {}).get("fields") or {} for r in self.rule_ids]
        self.titles = [r.get("title") or r.get("plain") or "" for r in self.sections["display"]["rules"]]
        if len(self.titles) != len(self.rule_ids):
            raise DiffError(f"answers_diff: {self.name}: {len(self.titles)} display rules for {len(self.rule_ids)} rule "
                            "ids")
        self._rix = {r: i for i, r in enumerate(self.rule_ids)}
        self._lix = {r: i for i, r in enumerate(self.lic_ids)}
        self.profile_labels = self.sections["licence"]["profile_labels"]
        self.fish = self.wire["fish"]
        self.akey: Dict[int, int] = store.section_akeys()
        self.place: Dict[int, int] = store.section_places()
        self.place_names = [p["names"] for p in self.wire["places"]]
        con = store.con
        strs = dict(con.execute("SELECT id, text FROM str"))
        self.item_id: Dict[int, str] = {}
        self.item_name: Dict[str, str] = {}
        for o, wid, name in con.execute("SELECT item_ord, item_id, name FROM item ORDER BY item_ord"):
            self.item_id[o] = strs[wid]
            self.item_name[strs[wid]] = name or strs[wid]
        self.part_label: Dict[Tuple[str, int], str] = {}
        for o, pix, lab in con.execute("SELECT item_ord, part_ix, label_s FROM part ORDER BY item_ord, part_ix"):
            self.part_label[(self.item_id[o], pix)] = strs.get(lab) if lab is not None else None
        store.close()
        # where each section lies: the water file's part_section (named waters; the rest is an unnamed water)
        self.water: Dict[int, Tuple[str, int]] = {}
        raw = _file_bytes(self.dir, self.manifest, "water")
        with tempfile.TemporaryDirectory(prefix="answers-diff-") as tmp:
            p = Path(tmp) / "water.sqlite"
            p.write_bytes(raw)
            wc = sqlite3.connect(str(p))
            for o, sid, pix in wc.execute("SELECT ord, sid, part_ix FROM part_section ORDER BY sid, ord"):
                self.water.setdefault(sid, (self.item_id[o], pix))
            wc.close()
        self._item_sections = Counter(w[0] for w in self.water.values())
        del raw
        # the salmon chains
        try:
            files = {n: _file_bytes(self.dir, self.manifest, n) for n in self.manifest["files"] if n.startswith("salmon")}
            if "salmon" not in files:
                raise DiffError(f"answers_diff: {self.name} has no salmon file")
            h, self.salmon, secs = SAL.decode_files(files)
        except DiffError:
            raise
        except (Exception, SystemExit) as e:                       # noqa: BLE001
            raise DiffError(f"answers_diff: cannot read the salmon files of {self.dir}: {type(e).__name__}: {e}")
        if h != self.handles:
            raise DiffError(f"answers_diff: {self.name}: the salmon files are of section handles {h}, not {self.handles}")
        self.chain_of: Dict[int, int] = dict(secs)
        self.default_chain: int = self.salmon.get("default", 0)
        self._lines = None
        self._facet_cache: Dict[tuple, dict] = {}
        self._chain_cache: Dict[int, tuple] = {}

    # --- where a section lies
    def where(self, h: int) -> Tuple[Optional[str], Optional[int], str]:
        """(item id or None, part index or None, the water's shown name)."""
        w = self.water.get(h)
        if w:
            return w[0], w[1], self.item_name.get(w[0], w[0])
        pl = self.place.get(h)
        a = self.akey.get(h)
        if pl is None or not a:
            return None, None, "Water outside B.C. or with no answer"
        kind = self.wire["keys"][a - 1][7]
        return None, None, self.place_names[pl].get(kind) or f"Unnamed {kind}"

    def lines(self) -> Dict[int, Tuple[int, int, int]]:
        """handle -> (stream key, start m, end m) of every stream section (`lines`; end 0xFFFFFFFF: open)."""
        if self._lines is None:
            from pipeline.deliver.release.lines import SLOTS, decode_block
            raw = _file_bytes(self.dir, self.manifest, "lines")
            out: Dict[int, Tuple[int, int, int]] = {}
            with tempfile.TemporaryDirectory(prefix="answers-diff-") as tmp:
                p = Path(tmp) / "lines.sqlite"
                p.write_bytes(raw)
                lc = sqlite3.connect(str(p))
                for blk, n, b in lc.execute("SELECT block, n, b FROM section_line ORDER BY block"):
                    for i, ln in enumerate(decode_block(b, n)):
                        h = blk * SLOTS + i
                        if h:
                            out[h] = ln
                lc.close()
            self._lines = out
        return self._lines

    def section_name(self, h: int) -> str:
        """The section's id as the atlas spells it: `{stream key}:{start m}` (`lines`), `lake:{waterbody key}` for a
        lake that is one section of a water; else `#{handle}`. With the atlas's handle table (`--handles-a/-b`), the id
        the table gives."""
        if self.ids is not None and 0 < h <= len(self.ids):
            return self.ids[h - 1]
        ln = self.lines().get(h)
        if ln:
            return f"{ln[0]}:{ln[1]}"
        w = self.water.get(h)
        if w and w[0].startswith("wbk:") and self._item_sections[w[0]] == 1:
            return "lake:" + w[0][4:]
        return f"#{h}"

    def universe(self) -> set:
        return set(self.akey) | set(self.chain_of)

    # --- the regs answer of a key on a day
    def key_meta(self, k: int):
        key = self.wire["keys"][k]
        starts = self.wire["segments"][key[9]]
        moms = None if key[10] is None else [self.wire["moments"][i] for i in self.wire["segment_moments"][key[10]]]
        return starts, moms

    def frames(self, k: int, s: int) -> dict:
        return {n: decode(self, n, self.sections[n]["at"][k][s]) for n in SECTION_NAMES}

    def refs(self, k: int, s: int) -> tuple:
        """The cell's frame refs: one per `SECTION_NAMES`, then the trout table's (None: none, or Q-D3's NULL)."""
        return tuple(self.sections[n]["at"][k][s] for n in SECTION_NAMES) + \
            (self.sections["trout"]["at"][k][s] if self.has_trout else None,)

    def trout(self, ref: Optional[int]) -> Optional[tuple]:
        """(the decoded trout cell, J's reading of it: `trout.check.Table`, its limits) — None for a NULL cell. A table
        J's grammar cannot read stops the run."""
        if ref is None:
            return None
        hit = self._trout_cache.get(ref)
        if hit is None:
            from pipeline.deliver.answers.trout import check as CHK
            from pipeline.deliver.answers.trout import words as TW
            cell = self.codecs["trout"].decode_frame(self.sections["trout"], ref, self.rule_ids, self.fish)
            try:
                T = CHK.Table(cell)
                hit = (cell, T, T.limits())
            except (TW.WordsError, CHK._Fail) as e:
                raise DiffError(f"answers_diff: {self.name}: trout frame {ref} cannot be read by J's decoder: "
                                f"{type(e).__name__}: {e}")
            self._trout_cache[ref] = hit
        return hit

    def rid(self, i) -> Optional[str]:
        return None if i is None else self.rule_ids[i] if isinstance(i, int) and 0 <= i < len(self.rule_ids) else i

    def title(self, rid: Optional[str]) -> str:
        """A rule id's title in this release (a `book:` salmon place is its rule); the id where it has none."""
        if rid is None:
            return ""
        if rid.startswith("book:"):
            rid = rid[5:]
        i = self._rix.get(rid)
        return rid if i is None else self.titles[i]

    def lid(self, i):
        return self.lic_ids[i] if isinstance(i, int) and not isinstance(i, bool) and 0 <= i < len(self.lic_ids) else i

    def lic_label(self, lid) -> str:
        i = self._lix.get(lid)
        return str(lid) if i is None else self.lic_labels[i]


# ------------------------------------------------------------------------------------------------------- decoding

def decode(rel: "Release", name: str, ref: int) -> dict:
    """One frame of section `name`, decoded by the release's codec AT THE RELEASE'S OWN ROWS VERSION: a rows frame of
    rows < `CUTOVER_ROWS` has five slots, from it six (`page_possession`), whichever this code writes — so the last
    release before the cutover and the cutover candidate are read by one run. Any other slot count stops the run."""
    sec, codec = rel.sections[name], rel.codecs[name]
    if name != "rows":
        return codec.decode_frame(sec, ref, rel.rule_ids, rel.fish)
    fr = sec["frames"][ref]
    slots = 6 if rel.rows_version >= CUTOVER_ROWS else 5
    if len(fr) != slots:
        raise DiffError(f"answers_diff: {rel.name}: rows {rel.rows_version} frame {ref} has {len(fr)} slots, not "
                        f"{slots}")
    want = 6 if page_possession_written() else 5
    if slots == want:
        return codec.decode_frame(sec, ref, rel.rule_ids, rel.fish)
    out = codec.decode_frame({**sec, "frames": {ref: fr[:5] + [None] * (want - 5)}}, ref, rel.rule_ids, rel.fish)
    out.pop("page_possession", None)
    if slots == 6:
        out["page_possession"] = fr[5]
    return out


def is_trout_row(row: dict) -> bool:
    from pipeline.deliver.answers.common import trout_char
    return bool(row["all_members"]) and set(row["all_members"]) <= trout_char()


def cutover_views(old: dict, new: dict, cell: Optional[dict]) -> Tuple[dict, dict]:
    """Two rows frames as compared across the trout cutover (`CUTOVER_ROWS`), `old` before it and `new` at or after it,
    `cell` the new side's trout cell (contract 2.7, Q-D9, the A0 amendment; L's `answers.cut_over`): where the new side
    has a table, the old frame is split as the cutover splits it (`trout.table.split_mixed`: each row mixing trout or
    char with other fish becomes its trout part and the rest), and on both sides the rows the table COVERS lose
    `ROWS34_TROUT_DROPS` (the table says them; its law is compared by `joint_changes`) and the parts of a split row
    their `stamps` (made again per part from the licence answer, which the licence facet compares). A NULL cell (Q-D3)
    leaves the frame whole, as the cutover does. `page_possession` is new on one side only: left out of both."""
    def bare(fr):
        return {k: v for k, v in fr.items() if k != "page_possession"}
    if cell is None:
        return bare(old), bare(new)
    from pipeline.deliver.answers.trout.table import TableError, split_mixed
    try:
        split, _cov = split_mixed(old)
    except TableError as e:
        raise DiffError(f"answers_diff: the rows before the cutover cannot be split as the cutover splits them: {e}")
    whole = {id(r) for r in old.get("rows") or ()}
    parts = {i for i, r in enumerate(split["rows"]) if id(r) not in whole}
    covers = set(cell["covers"])

    def view(fr):
        rows = []
        for i, r in enumerate(fr.get("rows") or ()):
            drop = (ROWS34_TROUT_DROPS if i in covers else ()) + (("stamps",) if i in parts else ())
            rows.append({k: v for k, v in r.items() if k not in drop})
        return {**bare(fr), "rows": rows}
    return view(split), view(new)


# ------------------------------------------------------------------------------------------------------- resolving refs

def _map_refs(v, f):
    if isinstance(v, bool) or v is None:
        return v
    if isinstance(v, int):
        return f(v)
    if isinstance(v, list):
        return [(_map_refs(x[0], f), *x[1:]) if isinstance(x, list) and x and isinstance(x[0], int)
                and not isinstance(x[0], bool) else _map_refs(x, f) for x in v]
    if isinstance(v, dict):
        return {k: _map_refs(x, f) for k, x in v.items()}
    return v


def _all_refs(v, f):
    if isinstance(v, bool) or v is None:
        return v
    if isinstance(v, int):
        return f(v)
    if isinstance(v, list):
        return [_all_refs(x, f) for x in v]
    if isinstance(v, dict):
        return {k: _all_refs(x, f) for k, x in v.items()}
    return v


def resolve(v, rule_keys: frozenset, lic_keys: frozenset, rf, lf, drop: Sequence[str] = (),
            all_refs: frozenset = frozenset(), pairs: frozenset = frozenset()):
    """`v` with its rule refs (under `rule_keys`; every int under `all_refs`; the first of an [int, int] pair under
    `pairs`) as rule ids and licensing refs (under `lic_keys`) as licensing ids; `drop`ped keys (derived fields) left
    out."""
    if isinstance(v, list):
        return [resolve(x, rule_keys, lic_keys, rf, lf, drop, all_refs, pairs) for x in v]
    if not isinstance(v, dict):
        return v
    out = {}
    for k, x in v.items():
        if k in drop:
            continue
        if k in all_refs:
            out[k] = _all_refs(x, rf)
        elif k in pairs and isinstance(x, list) and len(x) == 2 and all(type(y) is int for y in x):
            out[k] = [rf(x[0]), x[1]]
        elif k == "roles" and isinstance(x, list):
            out[k] = [[rf(r[0]), r[1], rf(r[2]) if isinstance(r[2], int) else r[2]] for r in x]
        elif k in lic_keys and lf is not None:
            out[k] = _map_refs(x, lf)
        elif k in rule_keys:
            out[k] = _map_refs(x, rf)
        else:
            out[k] = resolve(x, rule_keys, lic_keys, rf, lf, drop, all_refs, pairs)
    return out


def canon(v) -> str:
    return json.dumps(v, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def leaf_diffs(a, b, path: str = "", out: Optional[list] = None, cap: int = 12) -> List[str]:
    """The paths where `a` and `b` differ (first `cap`)."""
    out = [] if out is None else out
    if len(out) >= cap or a == b:
        return out
    if isinstance(a, dict) and isinstance(b, dict):
        for k in sorted(set(a) | set(b)):
            leaf_diffs(a.get(k), b.get(k), f"{path}.{k}", out, cap)
    elif isinstance(a, list) and isinstance(b, list) and len(a) == len(b):
        for i, (x, y) in enumerate(zip(a, b)):
            leaf_diffs(x, y, f"{path}[{i}]", out, cap)
    else:
        out.append(path or ".")
    return out


def only_words(paths: Iterable[str]) -> bool:
    paths = list(paths)
    return bool(paths) and all(any(seg.split("[")[0] in TEXT_KEYS for seg in p.split(".") if seg) for p in paths)


# ------------------------------------------------------------------------------------------------------- the facets

def fish_words(sp: str) -> str:
    from pipeline.regs.parsing.catalogue_parts.labels import species_words
    return species_words([sp])


def status_words(status: Optional[str], daily) -> str:
    if status is None:
        return "not listed"
    if status == "keep":
        return f"keep {daily} a day" if daily is not None else "keep"
    return {"release": "catch and release only", "closed": "closed", "no_limit": "keep, no daily limit"}.get(status,
                                                                                                          status)


def size_words(lines: Sequence[tuple]) -> str:
    out = []
    for t, a, b, take in lines:
        band = (f"over {a} cm" if b is None else f"under {b} cm" if not a else f"{a}-{b} cm")
        out.append(f"release {band}" if t == "rel" else f"only {take} {band}" if take is not None else f"{t} {band}")
    return "; ".join(out) or "no size rule"


def keep_profile(lines: Sequence[tuple]) -> List[float]:
    """How many of a fish of each length (0..300 cm) the size lines let you keep (no daily number)."""
    v = [float("inf")] * 301
    for t, a, b, take in lines:
        lo, hi = int(a or 0), (301 if b is None else int(b))
        for L in range(max(lo, 0), min(hi, 301)):
            v[L] = 0 if t == "rel" else min(v[L], take if take is not None else v[L])
    return v


def F(block, value, words, src, section) -> tuple:
    """A facet: (block, compare value as canonical JSON, words for the owner, source rule / place id, the section
    name it is read from, the value)."""
    return (block, canon(value), words, src, section, value)


def _digest(v) -> str:
    return hashlib.sha1(canon(v).encode("utf-8")).hexdigest()


def net_value(rel: "Release", name: str, ref: int, resolve_refs: bool, fr: Optional[dict] = None):
    """A section frame whole (or `fr`, a view of it), refs resolved (when the two releases' rule lists differ),
    derived fields dropped."""
    fr = decode(rel, name, ref) if fr is None else fr
    if name == "ladder":
        return fr
    rf = rel.rid if resolve_refs else (lambda i: i)
    lf = rel.lid if resolve_refs else (lambda i: i)
    return resolve(fr, RULE_KEYS.get(name, frozenset()) if resolve_refs else frozenset(),
                   (LIC_KEYS if name == "licence" else frozenset()) if resolve_refs else frozenset(),
                   rf, lf, DERIVED.get(name, ()), ALL_REF_KEYS.get(name, frozenset()) if resolve_refs else frozenset(),
                   PAIR_KEYS.get(name, frozenset()) if resolve_refs else frozenset())


def rows_facets(rel: "Release", fr: dict, view: dict, base: Dict[str, float], resolve_refs: bool, ck) -> dict:
    """The rows frame's facets (per fish and origin: the answer, its size lines, its possession) read from the frame
    `fr`, the possession's day number from `base` (`possession_bases`), and `("net", "rows")` the digest of `view`
    (the frame as compared: across the cutover, `cutover_views`). Cached on `ck`."""
    hit = rel._facet_cache.get(ck)
    if hit is not None:
        return hit
    out: dict = {}
    for sp in sorted(fr["fish"]):
        x = fr["fish"][sp]
        for o in ORIGINS:
            d = x.get(o)
            if d is None:
                out[("fish", sp, o)] = F("fish", None, "not listed", None, "rows")
                continue
            win = rel.rid(d.get("win"))
            out[("fish", sp, o)] = F("fish", [d["status"], d.get("daily")], status_words(d["status"], d.get("daily")),
                                     win, "rows")
            sl = [l for l in d.get("lines", []) if "a" in l or "b" in l]
            lines = sorted((l["t"], l.get("a"), l.get("b"), l.get("take")) for l in sl)
            lsrc = ",".join(sorted({rel.rid(l.get("r")) or "" for l in sl}))
            out[("size", sp, o)] = F("size", lines, size_words(lines), lsrc or None, "rows")
            roles = [r for r in d.get("roles", []) if r[1] in ("possession", "possession_cap")]
            pos = sorted({rel.rid(r[0]) for r in roles})
            prof = profile_runs(possession_profile(rel, d, base.get(sp)))
            bare = sorted({rel.rid(r[0]) for r in roles if not _possession_number(rel, r[0])})
            sig = sorted([rel.rid(r[0]), canon({k: v for k, v in _fields(rel, r[0]).items()
                                                if k in ("take", "per_daily", "lengths", "within")})] for r in roles)
            out[("possession", sp, o)] = F("possession", [pos, prof, bare, sig],
                                           ("; ".join(rel.title(p) for p in pos) or "none") +
                                           f" ({possession_words(prof)})", ",".join(pos) or None, "rows")
    out[("net", "rows")] = ("net:rows", _digest(net_value(rel, "rows", None, resolve_refs, view)), "rows", None, "rows",
                            ck)
    rel._facet_cache[ck] = out
    return out


def _fields(rel: "Release", rix) -> dict:
    return rel.rule_fields[rix] if isinstance(rix, int) and 0 <= rix < len(rel.rule_fields) else {}


def possession_bases(fr: dict, T=None) -> Dict[str, float]:
    """The day number a possession rule's `per_daily` multiplies, per fish (`GroupPossession.of`: the group's total,
    never a share under it — "Possession doubles the 4 a day, not the 2 from streams"): the total of the group the
    fish counts toward, else its pooled row's number; with a trout table (`T`, rows 34), the box's number the fish's row
    sits in. A fish in neither: its own daily (`possession_profile`)."""
    out: Dict[str, float] = {}
    for r in fr.get("rows") or ():
        if r.get("pool") is None or r.get("kind") not in ("keep", "no_limit"):
            continue
        g = r.get("group")
        n = g.get("total") if g else (r.get("daily") if r.get("kind") == "keep" else None)
        for S in r.get("all_members") or ():
            out[S] = INF if n in (None, "unlimited") else float(n)
    if T is not None:
        for b in T.boxes:
            if b["kind"] == "keep":
                for row in b["rows"]:
                    out[row.fish] = INF if b["n"] is None else float(b["n"])
    return out


def section_facets(rel: "Release", name: str, ref: int, resolve_refs: bool) -> dict:
    """{facet key: facet} read from ONE frame of one section (cached per frame: a frame is shared by many keys and
    days), with `("net", name)` the whole frame's digest."""
    ck = (name, ref)
    hit = rel._facet_cache.get(ck)
    if hit is not None:
        return hit
    fr = decode(rel, name, ref)
    out: dict = {}
    if name == "rows":
        raise AssertionError("rows facets are read per pair (`rows_facets`)")
    elif name == "display":
        closed = fr.get("status") == "closed"
        closers = sorted({rel.rid(c[0]) for c in fr.get("closing", [])}) if closed else []
        out[("closure",)] = F("closure", closed,
                              ("closed to all fishing: " + "; ".join(rel.title(c) for c in closers)) if closers else
                              ("closed to all fishing" if closed else "open to fishing"), ",".join(closers) or None,
                              "display")
        out[("tidal",)] = F("water", fr.get("status") == "tidal",
                            "tidal water" if fr.get("status") == "tidal" else "fresh water", None, "display")
    elif name == "gear":
        for e in sorted(fr.get("elements", {})):
            v = fr["elements"][e]
            by = rel.rid((v.get("by") or [None])[0])
            out[("gear", e)] = F("gear", v.get("verdict"),
                                 f"{_element_words(e)}: {'allowed' if v.get('verdict') == 'allow' else 'banned'}", by,
                                 "gear")
        for c in sorted(fr.get("counts", {})):
            v = fr["counts"][c]
            by = rel.rid((v.get("by") or [None])[0])
            says = v.get("says")
            bound = count_bound(rel, v.get("by"))
            out[("gear", "count:" + c)] = F("gear", [bound, says, None if says is not None else by],
                                            f"{c.replace('_', ' ')}: " + (says or (
                                                f"{'at most' if bound[0] == 'max' else 'at least'} {_num(bound[1])} "
                                                f"({rel.title(by)})" if bound else rel.title(by))), by, "gear")
        for f in ("hook", "fly", "bait_ban"):
            out[("gear", f)] = F("gear", fr.get(f), f"{f.replace('_', ' ')}: {fr.get(f)}", None, "gear")
        ves = fr.get("vessel") or {}
        # the answers' boat rule, read as the liveness pin's exempt `["vessel"]["active"]` (not a gauge's liveness)
        on_boats = (fr["vessel"]["active"] if ves and "active" in ves else None) or []
        boats = sorted({rel.rid(r) for r in on_boats if isinstance(r, int)}
                       | {rel.rid(r[0] if isinstance(r, list) else r) for r in (ves.get("timed") or [])
                          if isinstance(r, (int, list))})
        out[("boats",)] = F("boats", boats, "; ".join(rel.title(b) for b in boats) or "no boat rule",
                            ",".join(boats) or None, "gear")
    elif name == "licence":
        for i, p in enumerate(fr["profiles"]):
            docs = sorted({d["doc"] for d in p.get("documents", [])})
            label = rel.profile_labels[i] if i < len(rel.profile_labels) else f"profile {i}"
            words = "no licence needed" if p.get("none_needed") else \
                ("needs " + ", ".join(d.replace("_", " ") for d in docs) if docs else "no document listed")
            out[("licence", label)] = F("licence", [bool(p.get("none_needed")), docs], words, None, "licence")
        des = sorted(rel.lid(x) for x in fr["holds"].get("designations", []))
        out[("classified",)] = F("licence", des, "; ".join(rel.lic_label(x) for x in des) or "not a classified water",
                                 ",".join(map(str, des)) or None, "licence")
    net = fr if name == "ladder" else net_value(rel, name, ref, resolve_refs)
    out[("net", name)] = ("net:" + name, _digest(net), name, None, name, ref)
    rel._facet_cache[ck] = out
    return out


INF = float("inf")


def _num(n) -> str:
    return "no limit" if n is None or n == INF else (str(int(n)) if float(n).is_integer() else str(n))


def possession_profile(rel: "Release", d: dict, base: Optional[float] = None) -> List[float]:
    """How many of a fish of each length (0..300 cm) one decided answer lets you have IN POSSESSION, from the numbers
    of the rules it names in a possession role (the release's export subset): `take` (a fixed number, its `lengths`
    bands each with their own take or the rule's; outside every band the rule holds nothing), or `per_daily` times the
    day number it multiplies ("2 days' quota": `base`, the total the fish counts toward, `possession_bases`; else the
    answer's own daily). Not kept (release, closed): none. No possession number: no limit (inf)."""
    if d["status"] not in ("keep", "no_limit"):
        return [0.0] * 301
    daily = base if base is not None else INF if d.get("daily") is None else float(d["daily"])
    v = [INF] * 301
    for rix, role, *_ in d.get("roles", []):
        if role not in ("possession", "possession_cap") or not isinstance(rix, int) or not \
                0 <= rix < len(rel.rule_fields):
            continue
        f = rel.rule_fields[rix]
        if not _possession_number(rel, rix):
            continue
        base = f["take"] if f.get("take") is not None else f["per_daily"] * daily
        bands = f.get("lengths") or [{}]
        for b in bands:
            t = b.get("take", base)
            lo, hi = int(b.get("min_cm") or 0), (301 if b.get("max_cm") is None else int(b["max_cm"]))
            for L in range(max(lo, 0), min(hi, 301)):
                v[L] = min(v[L], float(t))
    return v


def _possession_number(rel: "Release", rix) -> bool:
    f = rel.rule_fields[rix] if isinstance(rix, int) and 0 <= rix < len(rel.rule_fields) else {}
    return f.get("take") is not None or f.get("per_daily") is not None


def profile_runs(v: Sequence[float]) -> List[list]:
    """A 0..300 cm profile as runs [[from cm, to cm, n | None (no limit)]]."""
    out: List[list] = []
    for L, n in enumerate(v):
        n = None if n == INF else n
        if out and out[-1][2] == n:
            out[-1][1] = L
        else:
            out.append([L, L, n])
    return out


def runs_profile(runs: Optional[Sequence[Sequence]]) -> List[float]:
    v = [INF] * 301
    for lo, hi, n in runs or []:
        for L in range(lo, hi + 1):
            v[L] = INF if n is None else n
    return v


def possession_words(runs: Sequence[Sequence]) -> str:
    if len(runs) == 1:
        n = runs[0][2]
        return "no possession number" if n is None else f"{_num(n)} in possession"
    return "in possession: " + ", ".join(f"{_num(n)} at {lo}-{hi} cm" for lo, hi, n in runs)


def count_bound(rel: "Release", by) -> Optional[list]:
    """A counted gear item's bound: ["max", n] or ["min", n] (a smallest hook gap) of the deciding rule's gear entry
    (`by` = [rule, entry index]); None where the entry states no number."""
    if not by or not isinstance(by[0], int) or not 0 <= by[0] < len(rel.rule_fields):
        return None
    g = rel.rule_fields[by[0]].get("gear") or []
    i = by[1] if len(by) > 1 and isinstance(by[1], int) else 0
    e = g[i] if 0 <= i < len(g) else {}
    return ["max", e["max"]] if e.get("max") is not None else ["min", e["min"]] if e.get("min") is not None else None


# ------------------------------------------------------------------------------------------------------- keeping law

#: the most of one fish the alone comparison counts to (a larger number reads "no limit")
ALONE_CAP = 60
#: random baskets per frame pair (and each limit filled to its number and one past it), as J's joint proof
BASKETS = 32


class Keeping:
    """WHAT ONE SIDE LETS YOU KEEP HERE, as a basket acceptor over fish (species, origin, cm, "here"): each fish's own
    decided answer (its state, its day number, its size lines: `trout.check.law`), each pooled row's number over its
    fish, the real daily limit's shared counts and caps, a group's total, the most one may keep of it here and its CAP
    SYSTEM (`GroupTotal.caps` over `cap_kinds`: legal iff every cap holds — what `card_check.check_caps` proves
    against the engine) — and, from rows 34, the trout table read by J's decoder (`trout.check.Table`: its boxes,
    bands, strips, row lines and `together`, what J proves against the engine) for the fish it holds. Two sides are
    compared by the baskets they accept (`joint_changes`), never by how the card words or arranges the limits."""

    def __init__(self, fr: dict, trout: Optional[tuple] = None):
        from pipeline.deliver.answers.trout import check as CHK
        self.limits: List[Tuple[str, object, float]] = []
        #: each cap as the signature reads it: its number and the kinds it counts (fish, origin, band)
        self.cap_sig: List[str] = []
        self.cuts: set = set()
        self.T, self.T_limits = (trout[1], trout[2]) if trout else (None, None)
        self.table_fish = {r.fish for r in self.T.rows} if self.T else set()
        if self.T:
            self.cuts |= {float(c) for c in self.T.cuts()}
        self.fish: Dict[Tuple[str, str], tuple] = {}
        for S, fa in sorted((fr.get("fish") or {}).items()):
            for o in ORIGINS:
                lw = CHK.law(fa.get(o))
                self.fish[(S, o)] = lw
                me = (lambda f, S=S, o=o: f[0] == S and f[1] == o)
                if lw is None or lw[0] != "keep":
                    self.limits.append((f"{S} {o} not kept", me, 0))
                    continue
                if lw[1] is not None:
                    self.limits.append((f"{S} {o} a day", me, lw[1]))
                for t, lo, hi, take in lw[2]:
                    lo, hi = float(lo or 0), (INF if hi is None else float(hi))
                    self.cuts |= {lo, hi}
                    self.limits.append((f"{S} {o} {t} {lo:g}-{hi:g}",
                                        lambda f, me=me, lo=lo, hi=hi: me(f) and lo <= f[2] < hi,
                                        0 if t == "rel" else INF if take is None else take))
        for r in fr.get("rows") or ():
            if r.get("pool") is None or r.get("kind") != "keep":
                continue
            mem = frozenset(r.get("all_members") or ())
            self._put(f"row {sorted(mem)}", mem, r.get("daily"))
            rd = r.get("real_daily") or {}
            for x in rd.get("shared_count") or []:
                self._put(f"together {sorted(x['members'])}", frozenset(x["members"]), x.get("take"))
            for x in rd.get("shared_cap") or []:
                self._put(f"over {x['over_cm']} together", mem, x.get("take"), over=float(x["over_cm"]))
            g = r.get("group")
            if not g:
                continue
            kinds = frozenset(g.get("kinds") or mem)
            self._put(f"group {sorted(kinds)} total", kinds, g.get("total"))
            self._put(f"group {sorted(kinds)} here", kinds, g.get("water_max"))
            ck = {k["id"]: k for k in g.get("cap_kinds") or ()}
            for k in ck.values():
                self.cuts |= {float(k.get("from") or 0), INF if k.get("to") is None else float(k["to"])}
            for i, c in enumerate(g.get("caps") or ()):
                ids = [ck[x] for x in (c.get("kinds") or {}).get("here", ()) if x in ck]
                if not ids:
                    continue
                self.limits.append((f"cap {i} {c.get('says') or ''}",
                                    lambda f, ids=ids: any(_in_kind(f, k) for k in ids), c.get("take")))
                self.cap_sig.append(canon([c.get("take"), sorted(canon({x: k.get(x) for x in
                                                                        ("species", "origin", "from", "to")})
                                                                  for k in ids)]))

    def _put(self, name, fish: frozenset, n, over: Optional[float] = None) -> None:
        if n is None or n == "unlimited":
            return
        self.limits.append((name, lambda f, fish=fish, over=over: f[0] in fish and (over is None or f[2] > over), n))

    def accepts(self, seq: Sequence[tuple]) -> bool:
        for _name, pred, n in self.limits:
            if sum(1 for f in seq if pred(f)) > n:
                return False
        if self.T is not None:
            tseq = [f for f in seq if f[0] in self.table_fish]
            if tseq and not self.T.accepts(tseq, self.T_limits):
                return False
        return True

    def alone(self, f: tuple) -> float:
        """The most of fish `f` kept alone here (every copy alike, so the smallest limit holding it; the table's own)."""
        got = [n for _name, pred, n in self.limits if pred(f)]
        if self.T is not None and f[0] in self.table_fish:
            got.append(self.T.alone(f[0], f[1], f[2], self.T_limits))
        n = min(got) if got else INF
        return INF if n >= ALONE_CAP else n

    def held(self) -> List[Tuple[str, object, float]]:
        """Every limit, the table's too (each filled to its number and one past it by `joint_changes`)."""
        out = list(self.limits)
        if self.T is not None:
            for name, pred, n in self.T_limits:
                out.append((f"table {name}", lambda f, pred=pred: pred(f, self.T.row_for(f[0], f[1], f[2])), n))
        return out

    def signature(self) -> str:
        """What the acceptor is made of, without words: equal signatures accept the same baskets."""
        return canon([sorted([list(k), list(v) if v else None] for k, v in self.fish.items()),
                      sorted([n, v] for n, _p, v in self.limits if not n.startswith("cap ")),
                      sorted(self.cap_sig),
                      None if self.T is None else self.T.t])


def _in_kind(f: tuple, k: dict) -> bool:
    """A fish (species, origin, cm) in a cap kind (`CapKind`: species, origin or either, a size band)."""
    lo, hi = float(k.get("from") or 0), (INF if k.get("to") is None else float(k["to"]))
    return f[0] == k["species"] and (k.get("origin") is None or f[1] == k["origin"]) and lo < f[2] < hi


def _fish_words(f: tuple) -> str:
    return f"{fish_words(f[0])} ({f[1]}, {f[2]:g} cm)"


def _basket_words(seq: Sequence[tuple]) -> str:
    n = Counter(seq)
    return ", ".join(f"{k} {_fish_words(f)}" for f, k in sorted(n.items()))


def joint_changes(ka: Keeping, kb: Keeping, skip: set, seed: str, st_water: bool = False) -> List[tuple]:
    """Where two sides' keeping law differs, by the baskets each accepts (J's method: every fish alone at every length
    piece, seeded random baskets, each limit of either side filled to its number and one past it). A fish whose own
    answer or size lines changed is left out (its facets say it); on a steelhead water a rainbow over the steelhead
    length and a steelhead under it are no fish (p.80, J's `_st_skip`). [(kind, errs, subject, before, after)]: a
    fish alone whose most moved (a limit it shares: the fish moving alike are one change) and a basket of several fish
    (the smallest the sides disagree on) are each a `shared_limit`."""
    import random
    from pipeline.deliver.answers.trout import check as CHK
    if ka.signature() == kb.signature():
        return []
    Ls = CHK.mids(ka.cuts | kb.cuts)
    keep = sorted({S for (S, o), lw in list(ka.fish.items()) + list(kb.fish.items())
                   if lw is not None and lw[0] == "keep" and S not in skip})
    cands = [(S, o, L, "here") for S in keep for o in ORIGINS for L in Ls if not CHK._st_skip(st_water, S, L)]
    out: List[tuple] = []
    by_subject: Dict[Tuple[str, str], List[Tuple[float, float, float]]] = defaultdict(list)
    both = []
    for f in cands:
        a, b = ka.alone(f), kb.alone(f)
        if a != b:
            by_subject[(f[0], f[1])].append((f[2], a, b))
        elif a > 0:
            both.append(f)
    # a fish alone moves here only through a limit it shares (its own answer is the same): one change per pattern,
    # every fish it moves together
    merged: Dict[tuple, List[Tuple[str, str]]] = defaultdict(list)
    for (S, o), xs in sorted(by_subject.items()):
        stricter = any(b < a for _L, a, b in xs)
        looser = any(b > a for _L, a, b in xs)
        errs = "closed" if stricter and not looser else "open" if looser and not stricter else "unclear"
        pat = (errs, tuple((L, a, b) for L, a, b in xs))
        merged[pat].append((S, o))
    from pipeline.regs.parsing.catalogue_parts.labels import species_words
    for (errs, xs), fish in sorted(merged.items(), key=lambda x: (sorted(x[1]), x[0][0])):
        species = sorted({S for S, _o in fish})
        origins = sorted({o for _S, o in fish})
        subject = species_words(species) + ("" if len(origins) == 2 else f" ({origins[0]})")
        at = "; ".join(f"{L:g} cm: {_num(a)} -> {_num(b)}" for L, a, b in xs[:3]) + (" …" if len(xs) > 3 else "")
        out.append(("shared_limit", errs, subject, f"keep {_num(xs[0][1])} of one kind alone",
                    f"keep {_num(xs[0][2])} ({at})"))
    if not both:
        return out
    rng = random.Random(f"answers_diff:{seed}")
    seen: Dict[frozenset, Tuple[set, list]] = {}

    def run(seq):
        a, b = ka.accepts(seq), kb.accepts(seq)
        if a == b:
            return
        seq = CHK._smallest(seq, lambda x: ka.accepts(x) == a and kb.accepts(x) == b)
        key = frozenset(f[0] for f in seq)
        hit = seen.setdefault(key, (set(), seq))
        hit[0].add("closed" if a else "open")
        if len(seq) < len(hit[1]):
            seen[key] = (hit[0], seq)
    for _ in range(BASKETS):
        run([rng.choice(both) for _j in range(rng.randint(2, 6))])
    for _name, pred, n in ka.held() + kb.held():
        if n is None or n == INF or n > ALONE_CAP:
            continue
        fs = [f for f in both if pred(f)]
        if not fs:
            continue
        for k in (int(n), int(n) + 1):
            if k >= 1:
                for _rep in range(2):
                    run([rng.choice(fs) for _j in range(k)])
    for key, (dirs, seq) in sorted(seen.items(), key=lambda x: sorted(x[0])):
        errs = dirs.pop() if len(dirs) == 1 else "unclear"
        a = "allowed" if ka.accepts(seq) else "not allowed"
        b = "allowed" if kb.accepts(seq) else "not allowed"
        out.append(("shared_limit", errs, species_words(sorted(key)), f"{_basket_words(seq)} together: {a}",
                    f"{_basket_words(seq)} together: {b}"))
    return out


def _allowances(says: str) -> Tuple[float, float, frozenset]:
    """A counted gear item's words ("1 line (rod); 2 while alone in a boat on a lake", "… no limit from a boat") as
    (the first clause's number, the largest, the clauses): fewer on both is stricter, more on both looser."""
    import re
    clauses = [c.strip() for c in says.split(";") if c.strip()]
    nums = []
    for c in clauses:
        m = re.search(r"\d+(?:\.\d+)?", c)
        nums.append(INF if "no limit" in c else float(m.group()) if m else None)
    known = [n for n in nums if n is not None]
    return (nums[0] if nums and nums[0] is not None else INF, max(known) if known else INF, frozenset(clauses))


def count_dir(va, vb) -> str:
    """Which way a counted gear item (lines, hooks, points, weight, bait kg: a MOST; a hook gap: a most or a LEAST)
    moved: its number, else its words' allowances. A count that appears (an item, or a number on it) is stricter, one
    that goes looser; words that gain or lose an allowance at the same number read that way; same number and words:
    none."""
    if va is None or vb is None:
        return "closed" if va is None else "open"
    (ba, wa, _), (bb, wb, _) = va, vb
    if (ba is None) != (bb is None):
        return "closed" if ba is None else "open"
    if ba is not None and bb is not None and ba != bb:
        if ba[0] != bb[0]:
            return "unclear"
        lower = bb[1] < ba[1]
        return ("closed" if lower else "open") if ba[0] == "max" else ("open" if lower else "closed")
    if wa != wb and ba is not None and ba[0] == "max" and (wa is None or wb is None):
        wa, wb = wa or _num(ba[1]), wb or _num(bb[1])      # no words: the number alone
    if isinstance(wa, str) and isinstance(wb, str) and wa != wb:
        (b1, t1, c1), (b2, t2, c2) = _allowances(wa), _allowances(wb)
        if (b1, t1) == (b2, t2):
            return "unclear"                 # the same numbers, said of other circumstances: read it
        if b2 <= b1 and t2 <= t1:
            return "closed"
        if b2 >= b1 and t2 >= t1:
            return "open"
        return "unclear"
    return "none"


def key_facets(rel: "Release", k: int) -> dict:
    key = rel.wire["keys"][k]
    return {("water", "steelhead"): F("water", [key[2], key[3], key[4]], f"steelhead {key[3]}", None, "key"),
            ("water", "kind"): F("water", key[7], f"a {key[7]}", None, "key")}


def _element_words(e: str) -> str:
    kind, _, what = e.partition(":")
    what = what.replace("_", " ")
    return {"bait": f"{what} as bait", "lure": what, "barb": f"{what} hooks", "method": what}.get(kind, e)


# ------------------------------------------------------------------------------------------------------- classifying

def _docs_dir(a: list, b: list) -> str:
    (na, da), (nb, db) = a, b
    if na != nb:
        return "open" if nb else "closed"
    sa, sb = set(da), set(db)
    return "closed" if sa < sb else "open" if sb < sa else "unclear"


def classify_regs(key: tuple, fa: tuple, fb: tuple, other_origin_same: bool) -> Tuple[str, str]:
    """(kind, errs) of one facet that differs."""
    block = fa[0] if fa else fb[0]
    va, vb = (fa[5] if fa else None), (fb[5] if fb else None)
    if block == "fish":
        if va is None or vb is None:
            return "other", "unclear"
        (sa, na), (sb, nb) = va, vb
        ra, rb = REGS_RANK.get(sa, -1), REGS_RANK.get(sb, -1)
        if ra != rb:
            errs = "closed" if rb < ra else "open"
            if other_origin_same:
                return "wild_hatchery", errs
            return ("closes" if rb < ra else "opens"), errs
        if na != nb:
            if na is None or nb is None:
                return "other", "unclear"
            return ("limit_lower", "closed") if nb < na else ("limit_higher", "open")
        return "source_only", "none"
    if block == "size":
        pa, pb = keep_profile(va or []), keep_profile(vb or [])
        le = all(x <= y for x, y in zip(pb, pa))
        ge = all(x >= y for x, y in zip(pb, pa))
        if le and not ge:
            return "size_stricter", "closed"
        if ge and not le:
            return "size_looser", "open"
        if le and ge:
            return "wording", "none"
        return "size_mixed", "unclear"
    if block == "possession":
        if va is None or vb is None:
            return "possession", "unclear"
        pa, pb = runs_profile(va[1]), runs_profile(vb[1])
        le, ge = all(x <= y for x, y in zip(pb, pa)), all(x >= y for x, y in zip(pb, pa))
        if le and ge:
            # the same numbers: another rule says them — unless a rule with no number of its own came or went
            return ("source_only", "none") if va[2] == vb[2] else ("possession", "unclear")
        return "possession", "closed" if le else "open" if ge else "unclear"
    if block == "closure":
        return ("closes", "closed") if vb else ("opens", "open")
    if block == "gear":
        sub = key[1]
        if sub in ("hook", "fly", "bait_ban"):
            if sub == "bait_ban":
                return "gear", "closed" if vb else "open"
            if sub == "fly":
                return "gear", "closed" if vb and not va else "open" if va and not vb else "unclear"
            return "gear", "unclear"
        if sub.startswith("count:"):
            d = count_dir(va, vb)
            return ("source_only", "none") if d == "none" else ("gear", d)
        if va is None or vb is None:
            # an element no rule spoke of before: a ban that comes is stricter, one that goes looser
            return "gear", "closed" if vb == "ban" else "open" if va == "ban" else "unclear"
        return "gear", "closed" if vb == "ban" else "open" if vb == "allow" else "unclear"
    if block == "boats":
        sa, sb = set(va or []), set(vb or [])
        return "gear", "closed" if sa < sb else "open" if sb < sa else "unclear"
    if block == "licence":
        if key[0] == "classified":
            sa, sb = set(va or []), set(vb or [])
            return "licence", "closed" if sa < sb else "open" if sb < sa else "unclear"
        return "licence", _docs_dir(va, vb)
    return "other", "unclear"


def classify_salmon(va: dict, vb: dict, moved: bool = False) -> Tuple[str, str]:
    """(kind, errs) of one species' salmon verdict that differs; `moved`: another place or rule answers it now."""
    ra, rb = SALMON_RANK.get(va.get("s"), -1), SALMON_RANK.get(vb.get("s"), -1)
    if ra != rb:
        return ("closes", "closed") if rb < ra else ("opens", "open")
    if va == vb:
        return "source_only", "none"
    ta, tb = va.get("tree") or {}, vb.get("tree") or {}
    if ta.get("n") != tb.get("n") and ta.get("n") is not None and tb.get("n") is not None:
        return ("limit_lower", "closed") if tb["n"] < ta["n"] else ("limit_higher", "open")
    paths = leaf_diffs(va, vb)
    if any("sizes" in p or "over_cm" in p or "lengths" in p for p in paths):
        return "size_mixed", "unclear"
    if moved:
        return "source_only", "none"
    if only_words(paths):
        return "wording", "none"
    return "other", "unclear"


# ------------------------------------------------------------------------------------------------------- one key pair

#: A CHANGE is a tuple (domain, kind, errs, block, subject, before words, after words, source before, source after,
#: moment words, field paths); each pair's changes map a change to its day mask.


def _moment_pick(starts, moms, d: int) -> Tuple[int, ...]:
    """The segment each ATOM (weekday x inside the hours window) reads on day `d`."""
    s = bisect_right(starts, d) - 1
    if moms is None:
        return (s,) * len(ATOMS)
    first = starts.index(starts[s])
    group = range(first, s + 1)
    if len(group) == 1:
        return (s,) * len(ATOMS)
    out = []
    for w, inside in ATOMS:
        hit = [i for i in group if w in moms[i]["weekdays"] and (moms[i]["hours"] is None or
                                                                 moms[i]["hours"]["in"] == inside)]
        if not hit:
            raise DiffError(f"answers_diff: day {d} has no moment for {w} (inside={inside})")
        out.append(hit[0])
    return tuple(out)


def _hours_words(h: Optional[dict]) -> str:
    if not h:
        return "the hours window"

    def end(x):
        m = x.get("offset_min") or 0
        sol = x.get("solar")
        if sol is None:
            return f"{x}"
        if not m:
            return sol
        return f"{abs(m) // 60 if abs(m) % 60 == 0 else abs(m)} {'hour' if abs(m) == 60 else 'hours' if abs(m) % 60 == 0 else 'minutes'} " \
               f"{'after' if m > 0 else 'before'} {sol}"
    return f"{end(h['start'])} to {end(h['end'])}"


def when_words(atoms: set, window: Optional[dict]) -> str:
    if len(atoms) == len(ATOMS):
        return ""
    days_full = [w for w in WEEKDAYS if (w, False) in atoms and (w, True) in atoms]
    ins = [w for w in WEEKDAYS if (w, True) in atoms and (w, False) not in atoms]
    outs = [w for w in WEEKDAYS if (w, False) in atoms and (w, True) not in atoms]
    parts = []
    if days_full:
        parts.append("on " + ", ".join(days_full) if len(days_full) < 7 else "every day")
    if ins:
        parts.append(f"from {_hours_words(window)}" + ("" if len(ins) == 7 else " on " + ", ".join(ins)))
    if outs:
        parts.append(f"outside {_hours_words(window)}" + ("" if len(outs) == 7 else " on " + ", ".join(outs)))
    return "; ".join(parts)


def diff_keys(A: Release, B: Release, ka: Optional[int], kb: Optional[int], resolve_refs: bool,
              cache: dict) -> Dict[tuple, int]:
    """{Change: day mask} of the regs answers of key `ka` (in A) against key `kb` (in B)."""
    out: Dict[tuple, int] = defaultdict(int)
    if ka is None and kb is None:
        return out
    if ka is None or kb is None:
        kind = "new_part" if ka is None else "removed_part"
        out[("regs", kind, "unclear", "water", "", "no answer" if ka is None else "answered",
             "answered" if ka is None else "no answer", None, None, "", "")] = ALL_DAYS
        return out
    sa, ma = A.key_meta(ka)
    sb, mb = B.key_meta(kb)
    starts = sorted(set(sa) | set(sb))
    window = next((m["hours"] for m in (ma or []) + (mb or []) if m.get("hours")), None)
    for i, d in enumerate(starts):
        hi = (starts[i + 1] - 1) if i + 1 < len(starts) else DAYS
        mask = mask_of(d, hi)
        pa, pb = _moment_pick(sa, ma, d), _moment_pick(sb, mb, d)
        by_pair: Dict[Tuple[int, int], set] = defaultdict(set)
        for atom, x, y in zip(ATOMS, pa, pb):
            by_pair[(x, y)].add(atom)
        for (x, y), atoms in sorted(by_pair.items()):
            for ch in _diff_frames(A, B, ka, x, kb, y, resolve_refs, when_words(atoms, window), cache):
                out[ch] |= mask
    return out


def _keeping(rel: "Release", fr: dict, ref, tref) -> Keeping:
    ck = ("keeping", ref, tref)
    hit = rel._facet_cache.get(ck)
    if hit is None:
        hit = rel._facet_cache[ck] = Keeping(fr, rel.trout(tref))
    return hit


def _rows_pair(A, B, ra, rb, resolve_refs) -> dict:
    """The rows side of one frame pair: each side's facets (read from its frame, the possession's day numbers from
    `possession_bases`), the views the net compares and each side's keeping law. Across the cutover the views are
    `cutover_views` (the new side's trout cell: its covers) and both sides take the OLD side's day numbers (its group
    totals: the possession rules are compared on one base; the table's law by `joint_changes`)."""
    fra, frb = decode(A, "rows", ra[1]), decode(B, "rows", rb[1])
    if A.straddle:
        a_new = A.rows_version >= CUTOVER_ROWS
        cell = A.trout(ra[5]) if a_new else B.trout(rb[5])
        if a_new:
            vb, va = cutover_views(frb, fra, cell and cell[0])
        else:
            va, vb = cutover_views(fra, frb, cell and cell[0])
        ba = bb = possession_bases(frb if a_new else fra)
        cka, ckb = ("rows", ra[1], ra[5], "across", rb[1], rb[5]), ("rows", rb[1], rb[5], "across", ra[1], ra[5])
    else:
        va, vb = fra, frb
        ta, tb = A.trout(ra[5]), B.trout(rb[5])
        ba, bb = possession_bases(fra, ta and ta[1]), possession_bases(frb, tb and tb[1])
        cka, ckb = ("rows", ra[1], ra[5]), ("rows", rb[1], rb[5])
    return {"fa": rows_facets(A, fra, va, ba, resolve_refs, cka), "fb": rows_facets(B, frb, vb, bb, resolve_refs, ckb),
            "va": va, "vb": vb, "ka": _keeping(A, fra, ra[1], ra[5]), "kb": _keeping(B, frb, rb[1], rb[5])}


def _diff_frames(A, B, ka, sa, kb, sb, resolve_refs, when, cache) -> List[tuple]:
    """The changes between key `ka` segment `sa` of A and key `kb` segment `sb` of B (cached per frame pair)."""
    ra, rb = A.refs(ka, sa), B.refs(kb, sb)
    ka_f, kb_f = key_facets(A, ka), key_facets(B, kb)
    keyed = [("regs", "other", "unclear", "water", k[1], x[2], kb_f[k][2], None, None, when, "")
             for k, x in sorted(ka_f.items()) if x[1] != kb_f[k][1]]
    st_water = bool(A.wire["keys"][ka][2]) or bool(B.wire["keys"][kb][2])
    ck = (ra, rb, when, st_water)
    hit = cache.get(ck)
    if hit is not None:
        return hit + keyed
    fa, fb = {}, {}
    for n, x, y in zip(SECTION_NAMES, ra, rb):
        if n != "rows":
            fa.update(section_facets(A, n, x, resolve_refs))
            fb.update(section_facets(B, n, y, resolve_refs))
    rp = _rows_pair(A, B, ra, rb, resolve_refs)
    fa.update(rp["fa"])
    fb.update(rp["fb"])
    out = []
    changed_sections = set()
    diff_keys_ = sorted((k for k in set(fa) | set(fb) if k[0] != "net" and
                         (fa.get(k) or (None, None))[1] != (fb.get(k) or (None, None))[1]), key=canon)
    diffset = set(diff_keys_)
    for k in diff_keys_:
        changed_sections.add((fa.get(k) or fb.get(k))[4])
    # per fish: hatchery and wild that change alike are one change; one that changes alone may be wild/hatchery
    done = set()
    for k in diff_keys_:
        if k in done:
            continue
        x, y = fa.get(k), fb.get(k)
        subject = ""
        other_same = False
        if k[0] == "possession" and (((not x or not y) and ("fish", k[1], k[2]) in diffset) or
                                     (x and y and x[5][0] == y[5][0] and x[5][3] == y[5][3])):
            continue            # the same possession rules with the same numbers: the number moved only with the day
            #                     number it multiplies (the fish's limit, a group's total say it); or the fish is not
            #                     asked about on one side (the fish's own change says it)
        if k[0] in ("fish", "size", "possession"):
            sp, o = k[1], k[2]
            twin = (k[0], sp, "wild" if o == "hatchery" else "hatchery")
            tx, ty = fa.get(twin), fb.get(twin)
            if twin in diffset and tx and ty and x and y and tx[1] == x[1] and ty[1] == y[1]:
                done.add(twin)
                subject = fish_words(sp)
            else:
                subject = f"{fish_words(sp)} ({o})"
                other_same = k[0] == "fish" and tx is not None and ty is not None and tx[1] == ty[1]
        elif k[0] in ("licence", "gear", "water"):
            subject = k[1]
        kind, errs = classify_regs(k, x, y, other_same)
        block = (x or y)[0]
        out.append(("regs", kind, errs, block, subject, x[2] if x else "not listed", y[2] if y else "not listed",
                    x[3] if x else None, y[3] if y else None, when, ""))
    # the keeping law: what each side lets you keep, alone and together (the group's caps, the trout table)
    skip = {k[1] for k in diffset if k[0] in ("fish", "size")}
    for kind, errs, subject, bw, aw in joint_changes(rp["ka"], rp["kb"], skip, canon([ra, rb, when]), st_water):
        out.append(("regs", kind, errs, "joint", subject, bw, aw, None, None, when, ""))
        changed_sections.add("rows")
    # the net: a frame that differs with no field above explaining it
    for n, x_ref, y_ref in zip(SECTION_NAMES, ra, rb):
        x, y = fa[("net", n)], fb[("net", n)]
        if x[1] == y[1] or n in changed_sections or (n == "ladder" and diff_keys_):
            continue
        if n == "rows":
            va = net_value(A, n, None, resolve_refs, rp["va"])
            vb = net_value(B, n, None, resolve_refs, rp["vb"])
        else:
            va = net_value(A, n, x_ref, resolve_refs)
            vb = net_value(B, n, y_ref, resolve_refs)
        paths = leaf_diffs(va, vb)
        kind = "wording" if only_words(paths) else "card_rows" if n == "rows" and (
            card_only(paths) or _reordered(va, vb)) else "other"
        detail = ", ".join(p.lstrip(".") for p in paths[:6])
        if n == "rows":
            detail = "; ".join(x for x in (rows_split_words(va, vb), detail) if x)
        out.append(("regs", kind, "unclear" if kind == "other" else "none", "net:" + n, n, "", "", None, None,
                    when, detail))
    cache[ck] = out
    return out + keyed


#: rows-frame fields that hold law (numbers, documents): a difference there is never the card's layout alone
LAW_PATH_KEYS = frozenset({"fish", "group", "conds", "items", "real_daily", "daily", "stamps"})


def card_only(paths: Sequence[str]) -> bool:
    """A rows frame difference that is the card's own layout (the rows' order and grouping, the fish it asks about,
    the steelhead line, the page's possession row, a badge) with every fish's answer and the keeping law the same — not
    one under `LAW_PATH_KEYS` (a decided answer's season limit or duty, a group's or a row's numbers, a stamp): those
    stay "other"."""
    return bool(paths) and not any(seg.split("[")[0] in LAW_PATH_KEYS for p in paths for seg in p.split(".") if seg)


def _reordered(va: dict, vb: dict) -> bool:
    """The same rows, whole, in another order (and nothing else of the frame differs)."""
    def rest(v):
        return {k: x for k, x in v.items() if k != "rows"}
    return rest(va) == rest(vb) and sorted(map(canon, va.get("rows") or ())) == sorted(map(canon, vb.get("rows") or ()))


def rows_split_words(va: dict, vb: dict) -> str:
    """'rows split or joined: CT+RB | KO -> CT | RB | KO' when the rows hold the fish otherwise; else ''."""
    def part(v):
        return [tuple(r.get("all_members") or ()) for r in v.get("rows", [])]
    pa, pb = part(va), part(vb)
    if sorted(pa) == sorted(pb):
        return "rows reordered" if pa != pb else ""
    return "rows split or joined: " + " | ".join("+".join(x) for x in pa) + " -> " + " | ".join("+".join(x) for x in pb)


# ------------------------------------------------------------------------------------------------------- salmon

def _runs_at(runs: Sequence[int], i: int) -> int:
    """The value of flat runs [start0, v0, start1, v1, ...] on day index i (0..365)."""
    j = bisect_right(runs[0::2], i) - 1
    return runs[2 * j + 1]


def _starts(runs: Sequence[int]) -> List[int]:
    return list(runs[0::2])


#: a chain's fields compared through its per-day answers (sp, a, g) or derived from them (m the map status, n the days
#: DFO speaks, c the header notes closing it every day — `salmon.closed_by_header`), and its places' indices (l, s)
CHAIN_DERIVED = ("sp", "a", "g", "m", "n", "c", "l", "s")


def chain_facts(rel: Release, c: int):
    hit = rel._chain_cache.get(c)
    if hit is None:
        ch = rel.salmon["chains"][c]
        extra = {k: v for k, v in ch.items() if k not in CHAIN_DERIVED}
        hit = (ch, canon(extra), extra)
        rel._chain_cache[c] = hit
    return hit


def salmon_source_words(rel: Release, src: Optional[str]) -> str:
    if src is None:
        return "no DFO place"
    if src.startswith("book:"):
        return "the rule book: " + rel.title(src)
    p = rel.salmon.get("p", {}).get(src)
    return f"DFO, {p['water']}" if p and p.get("water") else "a DFO row for a whole area"


def diff_chains(A: Release, B: Release, ca: int, cb: int) -> Dict[tuple, int]:
    out: Dict[tuple, int] = defaultdict(int)
    xa, ea, exa = chain_facts(A, ca)
    xb, eb, exb = chain_facts(B, cb)
    va, vb = A.salmon["verdicts"], B.salmon["verdicts"]
    ga, gb = A.salmon["gears"], B.salmon["gears"]
    starts = set()
    for x in (xa, xb):
        for r in x["sp"]:
            starts.update(_starts(r))
        for r in x.get("a") or []:
            starts.update(_starts(r))
        starts.update(_starts(x["g"]))
    starts = sorted(starts)
    for i, d0 in enumerate(starts):
        d1 = (starts[i + 1] - 1) if i + 1 < len(starts) else DAYS - 1
        mask = mask_of(d0 + 1, d1 + 1)
        for k, sp in enumerate(SALMON_SPECIES):
            v1, v2 = va[_runs_at(xa["sp"][k], d0)], vb[_runs_at(xb["sp"][k], d0)]
            s1 = _src(xa, k, d0)
            s2 = _src(xb, k, d0)
            if v1 == v2 and s1 == s2:
                continue
            kind, errs = classify_salmon(v1, v2, s1 != s2)
            if kind == "source_only" and s1 == s2:
                continue
            detail = ", ".join(p.lstrip(".") for p in leaf_diffs(v1, v2)[:6]) if kind == "other" else ""
            out[("salmon", kind, errs, "salmon", SALMON_NAMES[sp], f"{v1.get('s')}: {v1.get('t')}",
                 f"{v2.get('s')}: {v2.get('t')}", s1, s2, "", detail)] |= mask
        g1, g2 = _runs_at(xa["g"], d0), _runs_at(xb["g"], d0)
        l1 = ga[g1] if g1 >= 0 else []
        l2 = gb[g2] if g2 >= 0 else []
        if l1 != l2:
            out[("salmon", "gear", "unclear", "salmon gear", "salmon gear", "; ".join(l1) or "no salmon gear rule",
                 "; ".join(l2) or "no salmon gear rule", None, None, "", "")] |= mask
    if ea != eb:
        paths = leaf_diffs(exa, exb)
        kind = "wording" if only_words(paths) else "other"
        out[("salmon", kind, "none" if kind == "wording" else "unclear", "salmon card", "salmon card", "", "", None,
             None, "", ", ".join(p.lstrip(".") for p in paths[:6]))] |= ALL_DAYS
    return out


def _src(chain: dict, k: int, d: int) -> Optional[str]:
    """The place that answers species `k` on day index `d`: a DFO place id, or `book:<rule id>` (one rule however
    many places the salmon build made of it: its `~N` suffix is dropped)."""
    a = chain.get("a")
    if not a:
        return None
    ix = _runs_at(a[k], d)
    src = chain["l"][ix] if 0 <= ix < len(chain["l"]) else None
    if src and src.startswith("book:"):
        head, sep, tail = src.rpartition("~")
        if sep and tail.isdigit():
            src = head
    return src


# ------------------------------------------------------------------------------------------------------- matching sections

def match_sections(A: Release, B: Release) -> Tuple[List[Tuple[Optional[int], Optional[int]]], str]:
    """[(handle in A or None, handle in B or None)] covering every section of both, and how they were matched."""
    ua, ub = A.universe(), B.universe()
    if A.handles == B.handles:
        return [(h if h in ua else None, h if h in ub else None) for h in sorted(ua | ub)], "same section handles"
    la, lb = A.lines(), B.lines()
    pairs: List[Tuple[Optional[int], Optional[int]]] = []
    if getattr(A, "ids", None) is not None and getattr(B, "ids", None) is not None:
        return _match_by_id(A, B, ua, ub, la, lb), "matched by section id (the atlases' handle tables)"
    exact_a = {v[:2]: h for h, v in la.items()}
    by_blk_a: Dict[int, List[Tuple[int, int]]] = defaultdict(list)
    by_blk_b: Dict[int, List[Tuple[int, int]]] = defaultdict(list)
    for h, (blk, m0, _) in la.items():
        by_blk_a[blk].append((m0, h))
    for h, (blk, m0, _) in lb.items():
        by_blk_b[blk].append((m0, h))
    for v in (*by_blk_a.values(), *by_blk_b.values()):
        v.sort()

    def mid(ln):
        return ln[1] if ln[2] == 0xFFFFFFFF else (ln[1] + ln[2]) // 2

    def holder(by, ln):
        lst = by.get(ln[0])
        if not lst:
            return None
        j = bisect_right(lst, (mid(ln), float("inf"))) - 1
        return lst[j][1] if j >= 0 else lst[0][1]

    paired_a = set()
    for hb in sorted(lb):
        ln = lb[hb]
        ha = exact_a.get(ln[:2]) or holder(by_blk_a, ln)
        if ha is not None:
            paired_a.add(ha)
        pairs.append((ha, hb))
    for ha in sorted(la):
        if ha in paired_a:
            continue
        pairs.append((ha, holder(by_blk_b, la[ha])))
    # the rest (lakes, wetlands): by water, part and rank
    def keyed(rel, lines_, u):
        out: Dict[tuple, List[int]] = defaultdict(list)
        loose = []
        for h in sorted(u - set(lines_)):
            w = rel.water.get(h)
            (out[(w[0], w[1])] if w else loose).append(h)
        return out, loose
    ka, loose_a = keyed(A, la, ua)
    kb, loose_b = keyed(B, lb, ub)
    for k in sorted(set(ka) | set(kb), key=str):
        xa, xb = ka.get(k, []), kb.get(k, [])
        for i in range(max(len(xa), len(xb))):
            pairs.append((xa[min(i, len(xa) - 1)] if xa else None, xb[min(i, len(xb) - 1)] if xb else None))
    pairs += [(h, None) for h in loose_a] + [(None, h) for h in loose_b]
    return pairs, "matched by stream key and metre, lakes by water and part"


def _match_by_id(A: Release, B: Release, ua: set, ub: set, la: dict, lb: dict) -> List[Tuple[Optional[int], Optional[int]]]:
    """(2026-11-17) Cross-atlas matching when both atlases' handle tables are given (`--handles-a/-b`): a section is the
    same section when its id is (`354087681:0`, `lake:329026676`), which covers the unnamed lakes and wetlands the
    release alone cannot place (they are in no water and no line, so the stream/lake matching above lists ~700,000
    of them as removed and new). A section whose id is in one atlas only falls back to the stream holder (the other
    atlas's section holding its midpoint on the same line) or, for a lake, to its water and part; else it is new /
    removed."""
    by_id_a = {A.ids[h - 1]: h for h in ua if 0 < h <= len(A.ids)}
    by_id_b = {B.ids[h - 1]: h for h in ub if 0 < h <= len(B.ids)}
    by_blk_a: Dict[int, List[Tuple[int, int]]] = defaultdict(list)
    by_blk_b: Dict[int, List[Tuple[int, int]]] = defaultdict(list)
    for h, (blk, m0, _) in la.items():
        by_blk_a[blk].append((m0, h))
    for h, (blk, m0, _) in lb.items():
        by_blk_b[blk].append((m0, h))
    for v in (*by_blk_a.values(), *by_blk_b.values()):
        v.sort()

    def holder(by, ln):
        lst = by.get(ln[0])
        if not lst:
            return None
        m = ln[1] if ln[2] == 0xFFFFFFFF else (ln[1] + ln[2]) // 2
        j = bisect_right(lst, (m, float("inf"))) - 1
        return lst[j][1] if j >= 0 else lst[0][1]

    def by_water(rel_from, rel_to, h):
        w = rel_from.water.get(h)
        if not w:
            return None
        cands = sorted(x for x, v in rel_to.water.items() if v == w)
        return cands[0] if cands else None

    pairs: List[Tuple[Optional[int], Optional[int]]] = []
    for sid in sorted(set(by_id_a) & set(by_id_b)):
        pairs.append((by_id_a[sid], by_id_b[sid]))
    for sid in sorted(set(by_id_b) - set(by_id_a)):
        hb = by_id_b[sid]
        ha = holder(by_blk_a, lb[hb]) if hb in lb else by_water(B, A, hb)
        pairs.append((ha, hb))
    for sid in sorted(set(by_id_a) - set(by_id_b)):
        ha = by_id_a[sid]
        hb = holder(by_blk_b, la[ha]) if ha in la else by_water(A, B, ha)
        pairs.append((ha, hb))
    return pairs


# ------------------------------------------------------------------------------------------------------- the run

def run(dir_a, dir_b, log=print, handles_a=None, handles_b=None) -> dict:
    t0 = time.time()
    A, B = Release(dir_a, handles_a), Release(dir_b, handles_b)
    resolve_refs = not (A.rule_ids == B.rule_ids and A.lic_ids == B.lic_ids)
    A.straddle = B.straddle = (A.rows_version >= CUTOVER_ROWS) != (B.rows_version >= CUTOVER_ROWS)
    pairs, how = match_sections(A, B)
    # 1. the distinct (key A, key B) and (chain A, chain B) pairs, with their sections
    regs_pairs: Dict[Tuple[Optional[int], Optional[int]], List[int]] = defaultdict(list)
    salmon_pairs: Dict[Tuple[int, int], List[int]] = defaultdict(list)
    for i, (ha, hb) in enumerate(pairs):
        aa = A.akey.get(ha, 0) if ha is not None else 0
        ab = B.akey.get(hb, 0) if hb is not None else 0
        regs_pairs[(aa - 1 if aa else None, ab - 1 if ab else None)].append(i)
        if ha is not None and hb is not None:
            salmon_pairs[(A.chain_of.get(ha, A.default_chain), B.chain_of.get(hb, B.default_chain))].append(i)
    # 2. each pair's changes
    pair_changes: List[Tuple[str, tuple, Dict[tuple, int], List[int]]] = []
    frame_cache: dict = {}
    for (ka, kb), idx in sorted(regs_pairs.items(), key=lambda x: (x[0][0] is None, x[0][0] or 0,
                                                                   x[0][1] is None, x[0][1] or 0)):
        if ka is None and kb is None:
            continue
        ch = diff_keys(A, B, ka, kb, resolve_refs, frame_cache)
        if ch:
            pair_changes.append(("regs", (ka, kb), ch, idx))
    for (ca, cb), idx in sorted(salmon_pairs.items()):
        ch = diff_chains(A, B, ca, cb)
        if ch:
            pair_changes.append(("salmon", (ca, cb), ch, idx))
    # 3. group: kind, then the rule / place behind it before and after
    groups: Dict[tuple, dict] = {}
    for domain, pk, ch, idx in pair_changes:
        waters: Counter = Counter()
        unnamed: Counter = Counter()
        best = None
        for i in idx:
            ha, hb = pairs[i]
            rel, h = (B, hb) if hb is not None else (A, ha)
            item, part, name = rel.where(h)
            if item is not None:
                waters[(name, item)] += 1
            else:
                unnamed[name] += 1
            sk = (item is None, name, part if part is not None else -1, h)
            if best is None or sk < best[0]:
                best = (sk, i)
        hits = defaultdict(list)
        for c, mask in ch.items():
            gk = (c[0], c[1], c[2], c[3], c[7], c[8])
            hits[gk].append((c, mask))
        for gk, cs in hits.items():
            g = groups.setdefault(gk, {"sections": set(), "waters": Counter(), "unnamed": Counter(),
                                       "subjects": defaultdict(int), "before": Counter(), "after": Counter(),
                                       "when": set(), "detail": Counter(), "example": None})
            g["sections"].update(idx)
            g["waters"].update(waters)
            g["unnamed"].update(unnamed)
            for c, mask in cs:
                g["subjects"][c[4]] |= mask
                g["before"][c[5]] += len(idx)
                g["after"][c[6]] += len(idx)
                if c[9]:
                    g["when"].add(c[9])
                if c[10]:
                    g["detail"][c[10]] += len(idx)
            c0, m0 = min(cs, key=lambda x: (canon(list(x[0])), x[1]))
            ex = (best[0], best[1], c0, m0)
            if g["example"] is None or ex[:2] < g["example"][:2]:
                g["example"] = ex
    # 4. release-wide
    wide = release_wide(A, B)
    result = assemble(A, B, groups, pairs, how, resolve_refs, wide)
    log(f"answers_diff: compared in {time.time() - t0:.1f} s", file=sys.stderr)
    return result


def release_wide(A: Release, B: Release) -> List[dict]:
    out = []
    ta = dict(zip(A.rule_ids, A.titles))
    tb = dict(zip(B.rule_ids, B.titles))
    for r in sorted(set(ta) & set(tb)):
        if ta[r] != tb[r]:
            out.append({"kind": "wording", "what": "rule wording", "id": r, "before": ta[r], "after": tb[r]})
    wa, wb = set(A.wire["sections"]["display"]["waters"]), set(B.wire["sections"]["display"]["waters"])
    for w in sorted(wb - wa):
        out.append({"kind": "new_part", "what": "water", "id": w, "before": "", "after": B.item_name.get(w, w)})
    for w in sorted(wa - wb):
        out.append({"kind": "removed_part", "what": "water", "id": w, "before": A.item_name.get(w, w), "after": ""})
    for w in sorted(wa & wb):
        pa = A.wire["sections"]["display"]["waters"][w]["parts"]
        pb = B.wire["sections"]["display"]["waters"][w]["parts"]
        la = [p and p.get("label") for p in pa]
        lb = [p and p.get("label") for p in pb]
        if len(la) != len(lb):
            out.append({"kind": "new_part" if len(lb) > len(la) else "removed_part", "what": "parts", "id": w,
                        "before": f"{len(la)} parts", "after": f"{len(lb)} parts", "water": B.item_name.get(w, w)})
        elif la != lb:
            out.append({"kind": "wording", "what": "part names", "id": w, "before": "; ".join(map(str, la)),
                        "after": "; ".join(map(str, lb)), "water": B.item_name.get(w, w)})
    ra, rb = A.salmon.get("regions", {}), B.salmon.get("regions", {})
    for r in sorted(set(ra) | set(rb)):
        x, y = ra.get(r), rb.get(r)
        if x != y:
            paths = leaf_diffs(x, y) if x is not None and y is not None else ["."]
            out.append({"kind": "wording" if only_words(paths) else "other", "what": "salmon region header", "id": r,
                        "before": "", "after": "", "detail": ", ".join(p.lstrip(".") for p in paths[:6])})
    for key in ("unmapped", "off", "x"):
        x, y = A.salmon.get(key), B.salmon.get(key)
        if key == "unmapped":
            # chain indices differ between releases: compare each unmapped place by its words and its chain's answers
            x = {canon(u["items"]) + u["w"]: canon(_chain_answer(A, u["chain"])) for u in (x or [])}
            y = {canon(u["items"]) + u["w"]: canon(_chain_answer(B, u["chain"])) for u in (y or [])}
        x, y = x or {}, y or {}
        for k in sorted(set(x) | set(y)):
            if x.get(k) != y.get(k):
                kind = "new_part" if k not in x else "removed_part" if k not in y else "other"
                out.append({"kind": kind, "what": f"salmon {key} place", "id": k[:120],
                            "before": "" if k not in x else "its salmon answers before",
                            "after": "" if k not in y else "its salmon answers now differ" if k in x else "listed"})
    return out


def _chain_answer(rel: Release, c: int) -> list:
    ch = rel.salmon["chains"][c]
    v = rel.salmon["verdicts"]
    return [[(r[i], v[r[i + 1]]) for i in range(0, len(r), 2)] for r in ch["sp"]]


# ------------------------------------------------------------------------------------------------------- output

KIND_ORDER = list(KINDS)
WATER_CAP = 8


def assemble(A, B, groups, pairs, how, resolve_refs, wide) -> dict:
    out_groups = []
    for gk, g in groups.items():
        domain, kind, errs, block, sb, sa = gk
        idx = sorted(g["sections"])
        ex_sk, ex_i, ex_c, ex_mask = g["example"]
        ha, hb = pairs[ex_i]
        rel, h = (B, hb) if hb is not None else (A, ha)
        item, part, name = rel.where(h)
        plabel = rel.part_label.get((item, part)) if item is not None else None
        waters = sorted(g["waters"].items(), key=lambda x: (-x[1], x[0]))
        sec_ids = sorted({(B if pairs[i][1] is not None else A).section_name(
            pairs[i][1] if pairs[i][1] is not None else pairs[i][0]) for i in idx})
        out_groups.append({
            "domain": domain, "kind": kind, "errs": errs, "block": block,
            "source_before": sb, "source_after": sa,
            "source_before_words": _source_words(A, domain, sb), "source_after_words": _source_words(B, domain, sa),
            "sections": len(idx), "waters_count": len(g["waters"]),
            "waters": [{"name": n, "id": i, "sections": c} for (n, i), c in waters],
            "unnamed_sections": sum(g["unnamed"].values()),
            "unnamed": dict(sorted(g["unnamed"].items())),
            "subjects": {s: days_words(m) for s, m in sorted(g["subjects"].items())},
            "subject_days": {s: [list(r) for r in runs_of_mask(m)] for s, m in sorted(g["subjects"].items())},
            "before": [w for w, _ in sorted(g["before"].items(), key=lambda x: (-x[1], x[0]))],
            "after": [w for w, _ in sorted(g["after"].items(), key=lambda x: (-x[1], x[0]))],
            "when": sorted(g["when"]),
            "detail": [w for w, _ in sorted(g["detail"].items(), key=lambda x: (-x[1], x[0]))],
            "example": {"water": name, "water_id": item, "part": plabel, "part_ix": part,
                        "section": rel.section_name(h), "day": _md(first_day(ex_mask)), "subject": ex_c[4],
                        "before": ex_c[5], "after": ex_c[6], "when": ex_c[9]},
            "section_ids": sec_ids,
        })
    out_groups.sort(key=lambda g: (KIND_ORDER.index(g["kind"]), g["domain"], -g["sections"], g["block"],
                                   str(g["source_before"]), str(g["source_after"]), g["errs"]))
    return {"format": "answers-diff/1", "a": A.name, "b": B.name, "a_dir": str(A.dir), "b_dir": str(B.dir),
            "sections_compared": len(pairs), "matching": how,
            "rule_refs": "resolved to rule ids" if resolve_refs else "same rule and licensing lists in both",
            "rows_cutover": (f"across the trout cutover (rows {A.rows_version} -> {B.rows_version}): a trout row's "
                             f"{' and '.join(ROWS34_TROUT_DROPS)} and the frame's page_possession are not compared")
            if A.straddle else None,
            "versions": {"a": A.versions, "b": B.versions},
            "groups": out_groups, "release_wide": wide,
            "counts": {"groups": len(out_groups), "sections_changed": len({i for g in groups.values()
                                                                           for i in g["sections"]}),
                       "sections_answer_changed": len({i for gk, g in groups.items() if gk[2] != "none"
                                                       for i in g["sections"]}),
                       "sections_errs": {e: len({i for gk, g in groups.items() if gk[2] == e for i in g["sections"]})
                                         for e in ("closed", "open", "unclear")},
                       "by_kind": dict(sorted(Counter(g["kind"] for g in out_groups).items()))}}


def _source_words(rel: Release, domain: str, src: Optional[str]) -> str:
    if src is None:
        return ""
    if domain == "salmon":
        return salmon_source_words(rel, src)
    return "; ".join(rel.title(s) for s in src.split(","))


def _waters_line(g: dict, cap: int = WATER_CAP) -> str:
    names = [w["name"] for w in g["waters"]]
    head = ", ".join(names[:cap]) + (f" (+{len(names) - cap} more)" if len(names) > cap else "")
    un = g["unnamed_sections"]
    if un:
        head = (head + "; " if head else "") + f"{un:,} unnamed stretch{'es' if un != 1 else ''}"
    return head


def _subjects_line(g: dict) -> str:
    """'coho and chinook, Apr 1 to Jun 30' or 'coho Jan 1 to Jun 15; chinook all year'."""
    subs = g["subjects"]
    names = [s for s in subs if s]
    if len(set(subs.values())) == 1:
        days = next(iter(subs.values()))
        joined = names[0] if len(names) == 1 else ", ".join(names[:-1]) + " and " + names[-1] if names else ""
        return f"{joined}, {days}" if joined else days
    return "; ".join(f"{s or 'the water'} {d}" for s, d in subs.items())


#: kinds that change nothing you may do: listed short (the rest is in diff.json)
QUIET_KINDS = ("card_rows", "wording", "source_only")
QUIET_CAP = {"text": 15, "owner": 10}


def _capped(groups: List[dict], where: str):
    """(kind, its groups to show, how many more) in order."""
    out = []
    kinds = []
    for g in groups:
        if g["kind"] not in kinds:
            kinds.append(g["kind"])
    for k in kinds:
        gs = [g for g in groups if g["kind"] == k]
        cap = QUIET_CAP[where] if k in QUIET_KINDS else len(gs)
        out.append((k, gs[:cap], len(gs) - min(cap, len(gs))))
    return out


def text_report(r: dict) -> str:
    if not r["groups"] and not r["release_wide"]:
        return f"No answer changed: {r['a']} -> {r['b']} ({r['sections_compared']:,} sections compared, " \
               f"{r['matching']})."
    lines = [f"answers diff {r['a']} -> {r['b']}: {r['counts']['groups']} groups, "
             f"{r['counts']['sections_changed']:,} of {r['sections_compared']:,} sections changed ({r['matching']}; "
             f"rule refs {r['rule_refs']})"]
    for kind, gs, more in _capped(r["groups"], "text"):
        n = sum(1 for x in r["groups"] if x["kind"] == kind)
        lines.append(f"\n== {KINDS[kind][0]} ({n} groups, "
                     f"{sum(x['sections'] for x in r['groups'] if x['kind'] == kind):,} section-changes)")
        for g in gs:
            ex = g["example"]
            lines.append(f"[{g['domain']}/{g['block']}] {_subjects_line(g)} | {g['sections']:,} sections, "
                         f"{g['waters_count']} waters | errs {g['errs']} | {g['source_before']} -> {g['source_after']}")
            lines.append(f"    waters: {_waters_line(g)}")
            lines.append(f"    e.g. {ex['water']}" + (f" ({ex['part']})" if ex['part'] else "") +
                         f", {ex['day']}{', ' + ex['when'] if ex['when'] else ''}: {ex['subject'] or ''} "
                         f"{ex['before']} -> {ex['after']}" +
                         (f"  [{'; '.join(g['detail'][:2])}]" if g['detail'] else ""))
        if more:
            lines.append(f"... +{more} more groups of this kind (diff.json)")
    if r["release_wide"]:
        lines.append(f"\n== Release-wide ({len(r['release_wide'])})")
        for w in r["release_wide"]:
            lines.append(f"[{w['kind']}] {w['what']} {w['id']}: {w.get('before', '')} -> {w.get('after', '')}"
                         + (f" [{w['detail']}]" if w.get("detail") else ""))
    return "\n".join(lines)


_HEADLINE = {
    "closes": "now closed", "opens": "now open", "limit_lower": "lower daily limit", "limit_higher": "higher daily limit",
    "size_stricter": "stricter size rule", "size_looser": "looser size rule", "size_mixed": "size rule changed",
    "wild_hatchery": "wild or hatchery rule changed", "possession": "possession rule changed",
    "shared_limit": "shared limit changed", "card_rows": "the card shows it otherwise",
    "gear": "gear rule changed", "licence": "licence rule changed", "wording": "only the words changed",
    "source_only": "same answer, another rule now gives it", "new_part": "new", "removed_part": "gone",
    "other": "changed",
}
def _short(w: str, n: int = 160) -> str:
    w = " ".join((w or "").split())
    return w if len(w) <= n else w[:n - 1].rstrip() + "…"


_BLOCK_WORDS = {"joint": "Fish kept here", "salmon": "Salmon", "salmon gear": "Salmon gear",
                "salmon card": "Salmon card", "closure":
                "Whole water", "boats": "Boats", "licence": "Licence", "water": "The water", "fish": "Fish",
                "size": "Fish size", "possession": "Possession", "gear": "Gear",
                "net:rows": "Fish card"}


def owner_page(r: dict) -> str:
    if not r["groups"] and not r["release_wide"]:
        return f"# What changed from {r['a']} to {r['b']}\n\nNo answer changed.\n"
    by_kind = Counter(g["kind"] for g in r["groups"])
    c = r["counts"]
    lines = [f"# What changed from {r['a']} to {r['b']}", "",
             f"{c['sections_answer_changed']:,} map stretches have a different answer on some day "
             f"({c['sections_changed']:,} counting those where only the words or the rule behind the same answer "
             "changed). They are grouped by what changed and why. Each group says which way it errs.", ""]
    for kind, gs, more in _capped(r["groups"], "owner"):
        lines += [f"## {KINDS[kind][0]} ({by_kind[kind]})", ""]
        for g in gs:
            block = _BLOCK_WORDS.get(g["block"], g["block"].replace("net:", "").capitalize())
            subs = g["subjects"]
            names = [x for x in subs if x and x not in (g["block"], g["block"].replace("net:", ""))]
            joined = names[0] if len(names) == 1 else ", ".join(names[:-1]) + " and " + names[-1] if names else ""
            if len(set(subs.values())) == 1:
                head = f"{block}: {joined + ' ' if joined else ''}{_HEADLINE[g['kind']]}, {next(iter(subs.values()))}."
            else:
                head = f"{block}: {_HEADLINE[g['kind']]}: {_subjects_line(g)}."
            n_w = g["waters_count"]
            where = f"{g['sections']:,} stretch{'es' if g['sections'] != 1 else ''}" + \
                    (f" of {n_w} water{'s' if n_w != 1 else ''}" if n_w else "")
            before = "; ".join(_short(w) for w in g["before"][:3]) + (" (and others)" if len(g["before"]) > 3 else "")
            after = "; ".join(_short(w) for w in g["after"][:3]) + (" (and others)" if len(g["after"]) > 3 else "")
            ex = g["example"]
            bits = [f"- **{head}** {ERRS_WORDS.get(g['errs'], '')} On {where}: {_waters_line(g)}."]
            if before.strip() or after.strip():
                bits.append(f"  Before: {before or 'nothing'}. Now: {after or 'nothing'}.")
            if g["source_after_words"] and g["source_after_words"] != g["source_before_words"]:
                bits.append(f"  Now from {g['source_after_words']}" +
                            (f" (was {g['source_before_words']})" if g["source_before_words"] else "") + ".")
            if g["when"]:
                bits.append(f"  Only at some times: {'; '.join(g['when'])}.")
            bits.append(f"  Example: {ex['water']}" + (f", {ex['part']}" if ex['part'] else "") + f", {ex['day']}: "
                        + (f"{ex['subject']}, " if ex['subject'] else "")
                        + (f"{_short(ex['before'])} → {_short(ex['after'])}." if ex['before'] or ex['after']
                           else "see the ids."))
            ids = f"{g['domain']} · {g['source_before'] or '-'} → {g['source_after'] or '-'} · e.g. {ex['section']}"
            if g["detail"]:
                ids += " · fields: " + "; ".join(g["detail"][:2])
            bits.append(f"  <sub>ids: {ids}</sub>")
            lines += bits
        if more:
            lines.append(f"- … and {more} more groups like these (diff.json).")
        lines.append("")
    if r["release_wide"]:
        lines += [f"## Whole release ({len(r['release_wide'])})", ""]
        for w in r["release_wide"][:40]:
            lines.append(f"- {w['what'].capitalize()}: {w.get('water') or ''} {w.get('before', '')} → "
                         f"{w.get('after', '')}. <sub>{w['id']}</sub>")
        if len(r["release_wide"]) > 40:
            lines.append(f"- … and {len(r['release_wide']) - 40} more (diff.json).")
    return "\n".join(lines) + "\n"


def write(r: dict, out: Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    (out / "diff.json").write_text(json.dumps(r, indent=1, ensure_ascii=False, sort_keys=True) + "\n",
                                   encoding="utf-8")
    (out / "diff.txt").write_text(text_report(r) + "\n", encoding="utf-8")
    (out / "owner.md").write_text(owner_page(r), encoding="utf-8")


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("a", type=Path, help="release folder before (holds manifest.json)")
    ap.add_argument("b", type=Path, help="release folder after")
    ap.add_argument("--out", type=Path, help="where to write diff.json, diff.txt, owner.md "
                                             "(default <generated scratch>/answers-diff/<A>-vs-<B>)")
    ap.add_argument("--handles-a", type=Path, help="(cross-atlas pairs) A's atlas dir or its section_handles.txt: "
                                                   "every section is matched by its id, unnamed lakes included")
    ap.add_argument("--handles-b", type=Path, help="B's atlas dir or its section_handles.txt (with --handles-a)")
    args = ap.parse_args(argv)
    if (args.handles_a is None) != (args.handles_b is None):
        ap.error("--handles-a and --handles-b go together")
    try:
        r = run(args.a, args.b, handles_a=args.handles_a, handles_b=args.handles_b)
    except DiffError as e:
        print(str(e), file=sys.stderr)
        return 2
    out = args.out
    if out is None:
        from pipeline.common.curated import GENERATED
        out = GENERATED.scratch / "answers-diff" / f"{r['a']}-vs-{r['b']}"
    write(r, out)
    print(text_report(r))
    print(f"\nwrote {out}/diff.json, diff.txt, owner.md")
    return 0


if __name__ == "__main__":
    sys.exit(main())
