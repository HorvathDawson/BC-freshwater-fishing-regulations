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
    possession  per fish and origin: the possession rules the decided answer names
    closure     the whole water closed to all fishing (`display.status` closed) and the rules closing it
    gear        each gear element's verdict (bait, lures, methods, barbs), hook, fly only, bait ban, the counted gear
                (lines, hooks, weight: its words or its rule)
    boats       the boat rules in force (`gear.vessel`)
    licence     per angler profile: the documents needed (or none); the classified-water designations
    water       the key's steelhead presence, steelhead rules, water kind, tidal flag
    the rest    every section frame (ladder, rows, gear, licence, display) whole, rule refs as rule ids: a difference no
                field above explains is listed as "wording only" (only text fields differ) or "other" (its field paths
                named) — never dropped. The year strips (`rows[].year`, `display.strip`) are left out of this net: they
                restate the per-day answers that are compared directly.
  salmon (one chain per section, the salmon shards)
    per species (chinook, coho, sockeye, pink, chum): the day's verdict (open / release / closed / check, its limit
    tree and words) and the place that answers it (`a` into `l`: a DFO place or a `book:` closure); the gear lines; the
    chain's own words and caveats. Release-wide: each region's header, the unmapped and off-map places.
  release-wide
    rule wording (a rule id whose title changed), waters and parts that appear or go.

THE KINDS: closes, opens, limit lower, limit higher, size stricter, size looser, size mixed, wild/hatchery, possession,
gear, licence, wording only, source only (same answer, another rule or place answers), new water or part, removed water
or part, other. Each change says which way it errs: toward closed, toward open, no change to what you may do, or
unclear. Groups: kind, then the rule or place behind it before and after (so 71,046 sections of 71 rule + place pairs
are 71 lines), with section and water counts, the day ranges per fish, the water names and one worked example.

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
    "gear": ("Gear and bait, boats", None),
    "licence": ("Licences and classified waters", None),
    "new_part": ("New water or part", "unclear"),
    "removed_part": ("Removed water or part", "unclear"),
    "other": ("Other changes (read these)", "unclear"),
    "wording": ("Wording only", "none"),
    "source_only": ("Same answer, another rule or place behind it", "none"),
}
ERRS_WORDS = {"closed": "Errs toward closed.", "open": "Errs toward open.", "none": "No change to what you may do.",
              "unclear": "Direction unclear: read it."}

#: leaf keys whose values are words only (a difference only there is "wording only")
TEXT_KEYS = frozenset({"t", "says", "title", "plain", "label", "text", "sentence", "detail", "words", "w", "name",
                       "applies", "applies_trib", "heading", "lost", "won", "status_words", "note", "caution", "why",
                       "when", "days", "verbatim", "tp", "url", "asof", "headline", "fold"})
#: rule refs, per section (ints index the release's `rule_ids`; a pair/list's first element where it is [rule, n])
RULE_KEYS = {
    "rows": frozenset({"win", "narrow", "pool", "r", "rules", "outer", "wins", "of", "lifts", "by"}),
    "gear": frozenset({"by", "over", "cites", "main", "clause", "decides", "repeats", "caught", "while_rules", "rule",
                       "active", "timed", "in_part", "side", "lifts", "downrigger", "light", "rules"}),
    "display": frozenset({"closing", "closers", "unless_opened", "by", "lifts"}),
}
LIC_KEYS = frozenset({"holds", "designations", "considered", "wrong_water", "waived", "not_yet_mapped", "req", "terms",
                      "by", "presumes_by"})
#: derived fields left out of the whole-frame net (module doc)
DERIVED = {"rows": ("year",), "display": ("strip",)}


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

    def __init__(self, d):
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
        lake that is one section of a water; else `#{handle}`."""
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
        return {n: self.codecs[n].decode_frame(self.sections[n], self.sections[n]["at"][k][s], self.rule_ids, self.fish)
                for n in SECTION_NAMES}

    def refs(self, k: int, s: int) -> tuple:
        return tuple(self.sections[n]["at"][k][s] for n in SECTION_NAMES)

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


def resolve(v, rule_keys: frozenset, lic_keys: frozenset, rf, lf, drop: Sequence[str] = ()):
    """`v` with its rule refs (under `rule_keys`) as rule ids and licensing refs (under `lic_keys`) as licensing ids;
    `drop`ped keys (derived fields) left out."""
    if isinstance(v, list):
        return [resolve(x, rule_keys, lic_keys, rf, lf, drop) for x in v]
    if not isinstance(v, dict):
        return v
    out = {}
    for k, x in v.items():
        if k in drop:
            continue
        if k == "roles" and isinstance(x, list):
            out[k] = [[rf(r[0]), r[1], rf(r[2]) if isinstance(r[2], int) else r[2]] for r in x]
        elif k in lic_keys and lf is not None:
            out[k] = _map_refs(x, lf)
        elif k in rule_keys:
            out[k] = _map_refs(x, rf)
        else:
            out[k] = resolve(x, rule_keys, lic_keys, rf, lf, drop)
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


def net_value(rel: "Release", name: str, ref: int, resolve_refs: bool):
    """A section frame whole, refs resolved (when the two releases' rule lists differ), derived fields dropped."""
    fr = rel.codecs[name].decode_frame(rel.sections[name], ref, rel.rule_ids, rel.fish)
    if name == "ladder":
        return fr
    rf = rel.rid if resolve_refs else (lambda i: i)
    lf = rel.lid if resolve_refs else (lambda i: i)
    return resolve(fr, RULE_KEYS.get(name, frozenset()) if resolve_refs else frozenset(),
                   (LIC_KEYS if name == "licence" else frozenset()) if resolve_refs else frozenset(),
                   rf, lf, DERIVED.get(name, ()))


def section_facets(rel: "Release", name: str, ref: int, resolve_refs: bool) -> dict:
    """{facet key: facet} read from ONE frame of one section (cached per frame: a frame is shared by many keys and
    days), with `("net", name)` the whole frame's digest."""
    ck = (name, ref)
    hit = rel._facet_cache.get(ck)
    if hit is not None:
        return hit
    fr = rel.codecs[name].decode_frame(rel.sections[name], ref, rel.rule_ids, rel.fish)
    out: dict = {}
    if name == "rows":
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
                pos = sorted({rel.rid(r[0]) for r in d.get("roles", []) if r[1] in ("possession", "possession_cap")})
                out[("possession", sp, o)] = F("possession", pos, "; ".join(rel.title(p) for p in pos) or "none",
                                               ",".join(pos) or None, "rows")
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
            out[("gear", "count:" + c)] = F("gear", says if says is not None else by,
                                            f"{c.replace('_', ' ')}: {says or rel.title(by)}", by, "gear")
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
        return "possession", "unclear"
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
            return "gear", "unclear"
        if va is None or vb is None:
            return "gear", "unclear"
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


def _diff_frames(A, B, ka, sa, kb, sb, resolve_refs, when, cache) -> List[tuple]:
    """The changes between key `ka` segment `sa` of A and key `kb` segment `sb` of B (cached per frame pair)."""
    ra, rb = A.refs(ka, sa), B.refs(kb, sb)
    ka_f, kb_f = key_facets(A, ka), key_facets(B, kb)
    keyed = [("regs", "other", "unclear", "water", k[1], x[2], kb_f[k][2], None, None, when, "")
             for k, x in sorted(ka_f.items()) if x[1] != kb_f[k][1]]
    ck = (ra, rb, when)
    hit = cache.get(ck)
    if hit is not None:
        return hit + keyed
    fa, fb = {}, {}
    for n, x, y in zip(SECTION_NAMES, ra, rb):
        fa.update(section_facets(A, n, x, resolve_refs))
        fb.update(section_facets(B, n, y, resolve_refs))
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
    # the net: a frame that differs with no field above explaining it
    for n, x_ref, y_ref in zip(SECTION_NAMES, ra, rb):
        x, y = fa[("net", n)], fb[("net", n)]
        if x[1] == y[1] or n in changed_sections or (n == "ladder" and diff_keys_):
            continue
        va = net_value(A, n, x_ref, resolve_refs)
        vb = net_value(B, n, y_ref, resolve_refs)
        paths = leaf_diffs(va, vb)
        kind = "wording" if only_words(paths) else "other"
        out.append(("regs", kind, "none" if kind == "wording" else "unclear", "net:" + n, n, "", "", None, None,
                    when, ", ".join(p.lstrip(".") for p in paths[:6])))
    cache[ck] = out
    return out + keyed


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


# ------------------------------------------------------------------------------------------------------- the run

def run(dir_a, dir_b, log=print) -> dict:
    t0 = time.time()
    A, B = Release(dir_a), Release(dir_b)
    resolve_refs = not (A.rule_ids == B.rule_ids and A.lic_ids == B.lic_ids)
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
            "versions": {"a": A.versions, "b": B.versions},
            "groups": out_groups, "release_wide": wide,
            "counts": {"groups": len(out_groups), "sections_changed": len({i for g in groups.values()
                                                                           for i in g["sections"]}),
                       "sections_answer_changed": len({i for gk, g in groups.items() if gk[2] != "none"
                                                       for i in g["sections"]}),
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
QUIET_KINDS = ("wording", "source_only")
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
    "gear": "gear rule changed", "licence": "licence rule changed", "wording": "only the words changed",
    "source_only": "same answer, another rule now gives it", "new_part": "new", "removed_part": "gone",
    "other": "changed",
}
def _short(w: str, n: int = 160) -> str:
    w = " ".join((w or "").split())
    return w if len(w) <= n else w[:n - 1].rstrip() + "…"


_BLOCK_WORDS = {"salmon": "Salmon", "salmon gear": "Salmon gear", "salmon card": "Salmon card", "closure":
                "Whole water", "boats": "Boats", "licence": "Licence", "water": "The water", "fish": "Fish",
                "size": "Fish size", "possession": "Possession", "gear": "Gear"}


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
            names = [x for x in subs if x and x != g["block"]]
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
    args = ap.parse_args(argv)
    try:
        r = run(args.a, args.b)
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
