"""THE ANSWERS FILE'S WIRE FORMAT (`ui-rules-answers.json`), its encoder and its reference decoder.

`encode(model, data)` writes `answers.Model` against the export it pairs with (`data`: the shipped
`ui-rules-export.json`); `decode(wire, data)` returns exactly the model (`cli build` refuses a file
that does not decode to it). Every rule is an INTEGER index into the export's `rules` array (the
codec's order: `rule_ids` sorted), every fish an index into `fish`, every repeated value interned.

THE SECTIONS ARE GENERIC. The top level holds what every section shares — `keys`, `segments`,
`parts`, `fish` — and `sections` holds one object per named section, each `{version, at, …its
tables}`, where `at[key][segment]` is the index of that (part key, segment)'s value in the section's
`frames`. The encoder and the decoder iterate `CODECS`; a section without a codec, or without its
text in `SPEC`, is refused (`spec_gaps`, pinned by the tests).
"""
from __future__ import annotations

import hashlib
import json
from bisect import bisect_right
from typing import Dict, List, Optional, Tuple

from pipeline.deliver.answers.answers import (DAYS, ORIGINS, PART_KEY_FIELDS, RESERVED, STATUSES,
                                              AnswersError, Model, day_of)
from pipeline.deliver.bundle import read

FORMAT = "answers/0"

#: The ladder's verdict lists, in wire order.
VERDICT_LISTS = ("speaks", "beside", "shown", "not_yet_mapped", "partly", "lost")
#: The loss reasons, in wire order (`read.LOSS_REASONS`: reason -> the state it gives).
REASONS = tuple(read.LOSS_REASONS)


def rule_ids_digest(rule_ids: List[str]) -> str:
    """The SHA-256 (first 16 hex digits) of the export's `rule_ids`, one per line: the integer
    rule refs mean these rules and no others."""
    return hashlib.sha256("\n".join(rule_ids).encode("utf-8")).hexdigest()[:16]


# --------------------------------------------------------------------------------------------
# The field dictionary, shipped in the file as `spec`
# --------------------------------------------------------------------------------------------

SPEC = {
    "format": f"`about.format` is {FORMAT!r}: answers file format 0. A reader refuses any other.",
    "pairing": "This file pairs with ONE export pair (ui-rules-export.json + ui-rules-guide.json): "
               "`about.bundle` equals both files' `about.bundle` (digests `reach_digest`, "
               "`section_handles`), and `about.export.rule_ids_sha256` is the first 16 hex digits "
               "of the SHA-256 of the export's `rule_ids` joined by \"\\n\". Refuse a pair that "
               "differs: every rule ref below indexes the export's `rules` / `rule_ids`.",
    "calendar": "A day is 1..366 on the leap calendar: Jan 1 = 1, Feb 29 = 60, Mar 1 = 61 in EVERY "
                "year, Dec 31 = 366 (day = days before the month in a leap year + day of month). "
                "In a year without Feb 29, day 60 is never asked. Feb 29 is answered as the "
                "reader answers it: a rule printed to Feb 28 does not hold on it, so it can be a "
                "segment of its own.",
    "tap": "How a tap resolves, with lookups only: (1) the water's item id and the part's index i "
           "in the export's `waters[item].parts` -> k = `parts[item][i]` (null: the part has no "
           "rule set — wholly outside B.C., `water.outside_bc`); (2) the day d -> the segment s = "
           "the last index with `segments[keys[k][7]][s] <= d`; (3) per section, frame = "
           "`sections[name].frames[sections[name].at[k][s]]`; (4) in the frame, the fish (an "
           "index into `fish`) and the origin (none | hatchery | wild).",
    "top level": {
        "about": "`what`, `format`, `bundle` (the export pair's digests), `export` "
                 "{`rule_ids_sha256`, `rules`: count}, `sections` {name: version} of the sections "
                 "present, `reserved` {name: what it will hold} — sections not yet present "
                 "(absent, never empty), `counts`",
        "spec": "this dictionary",
        "fish": "[fish code] — the leaf species codes (`ui-rules-guide.json` `species`) the "
                "frames refer to by index",
        "keys": "[[ruleset, licensing_set, steelhead_water, steelhead, steelhead_rules, "
                "province_except, home_region, segments]] — one per distinct PART KEY: the export "
                "part's rule set and licensing set ids (integers, licensing set or null), "
                "`steelhead_water` (0/1: a rainbow over 50 cm is a steelhead here, the part's "
                "`anadromous_rainbow`), `steelhead` (\"known\" | \"possible\" | null), "
                "`steelhead_rules` (0/1: the bundle's fact — the export ships it only as false on "
                "a known part), `province_except` [kind], `home_region` [region], and "
                "`segments` (an index into `segments`)",
        "segments": "[[start day]] — interned; a key's segments start on these days (the first is "
                    "always 1) and run to the day before the next start (the last to 366). A "
                    "segment is a run of days on which every section's inputs read the same: no "
                    "member rule's `when` and no lift's `when` changes inside it",
        "parts": "{item_id: [key index | null]} — aligned with the export's `waters[item].parts`",
        "sections": "{name: section} — see `sections`",
    },
    "sections": {
        "ladder": {
            "what": "Stage 4, the ladder: per fish and origin, every rule the reader returns "
                    "(`read.effective_rules_bound(trace=True, origin=…)`) — speakers and losers",
            "version": "0",
            "at": "[[frame index per segment] per key]",
            "frames": "[[common, [[fish, v] | [fish, v_none, v_hatchery, v_wild]]]] — `common` a "
                      "verdict index holding the rules whose entry is the same for every fish and "
                      "origin of the frame; then per fish (index into `fish`, ascending) its own "
                      "verdict index, one for all three origins or one per origin. A fish's "
                      "verdict for an origin = `common` + its own (disjoint by rule). The fish "
                      "listed are every game fish the rule set's rules name, plus ST where the "
                      "steelhead rules apply",
            "verdicts": "[[speaks, beside, shown, not_yet_mapped, partly, lost]] — the first four "
                        "sorted rule refs in that state; `partly` [[rule, [lifting rule]]]: a "
                        "rule in one of those states that a lift holds for only some anglers or "
                        "fish (`partly_lifted`); `lost` [[rule, reason, by]]: a rule that took "
                        "part and lost, `reason` an index into `reasons`, `by` the rule that beat "
                        "or lifted it",
            "reasons": "[reason] — `read.LOSS_REASONS` keys: the step that removed the rule",
            "reason_state": "[state] parallel to `reasons`: lifted | displaced | moot",
        },
        "answer": {
            "what": "Stage 5.2 steps 1-4, the decided answer per fish and origin, from the ladder "
                    "only: the winner among the speaking rules in scope — the lowest-rank "
                    "closure, else the lowest-rank release, else the smallest daily pool",
            "version": "0",
            "at": "[[frame index per segment] per key] — the same keys and segments as `ladder`",
            "frames": "[[[fish, d] | [fish, d_none, d_hatchery, d_wild]]] — per fish (index into "
                      "`fish`, ascending, the ladder frame's fish) its decided index, one for "
                      "all three origins or one per origin",
            "decided": "[[status, daily, winner]] — `status` an index into `statuses`; `daily` "
                       "the WINNING POOL'S OWN NUMBER (consumer 5.2 step 4), BEFORE its clauses: "
                       "a sub-limit that lowers the day (\"keep 2, hatchery only\" inside a 4) "
                       "is step 5 and arrives with `rows` (v1) — do not show `daily` as the "
                       "number an angler may keep until then; null for closed, release, "
                       "no_limit, no_rule, by_origin. `winner` a rule ref (null for no_rule, "
                       "by_origin)",
            "statuses": "[status] — closed | release | keep | no_limit | no_rule (no rule in "
                        "scope speaks: the page shows no row) | by_origin (origin unknown and "
                        "the hatchery and wild answers differ: read those)",
        },
    },
    "reserved": {k: f"v1, not yet present: {v}" for k, v in RESERVED.items()},
}


# --------------------------------------------------------------------------------------------
# Interning
# --------------------------------------------------------------------------------------------

class Table:
    """An interned list: `add(value)` -> its index; equal values (canonical JSON) share one."""

    def __init__(self):
        self.rows: list = []
        self._ix: Dict[str, int] = {}

    def add(self, v) -> int:
        k = json.dumps(v, separators=(",", ":"), sort_keys=True)
        i = self._ix.get(k)
        if i is None:
            i = self._ix[k] = len(self.rows)
            self.rows.append(v)
        return i


# --------------------------------------------------------------------------------------------
# Section codecs
# --------------------------------------------------------------------------------------------

class LadderCodec:
    name = "ladder"

    def encode(self, values: List[dict], rix: Dict[str, int], fix: Dict[str, int]) -> Tuple[dict, List[int]]:
        verdicts, frames = Table(), Table()
        refs = []
        for v in values:
            cells = [(f, o, verdict) for f, by_o in v.items() for o, verdict in by_o.items()]
            common = {}
            if cells:
                first = cells[0][2]
                common = {k: e for k, e in first.items()
                          if all(c[2].get(k) == e for c in cells[1:])}
            rows = []
            for f in sorted(v, key=lambda f: fix[f]):
                own = [verdicts.add(self._verdict({k: e for k, e in v[f][o].items()
                                                   if k not in common}, rix)) for o in ORIGINS]
                rows.append([fix[f], own[0]] if own[0] == own[1] == own[2] else [fix[f]] + own)
            refs.append(frames.add([verdicts.add(self._verdict(common, rix)), rows]))
        return {"reasons": list(REASONS), "reason_state": [read.LOSS_REASONS[r] for r in REASONS],
                "verdicts": verdicts.rows, "frames": frames.rows}, refs

    @staticmethod
    def _verdict(entries: dict, rix: Dict[str, int]) -> list:
        lists = {s: [] for s in ("speaks", "beside", "shown", "not_yet_mapped")}
        partly, lost = [], []
        for k, (state, partly_by, reason, by) in entries.items():
            if state in lists:
                lists[state].append(rix[k])
                if partly_by:
                    partly.append([rix[k], sorted(rix[b] for b in partly_by)])
            else:
                lost.append([rix[k], REASONS.index(reason), rix[by]])
        return [sorted(lists[s]) for s in ("speaks", "beside", "shown", "not_yet_mapped")] \
            + [sorted(partly), sorted(lost)]

    def decode_frame(self, sec: dict, ref: int, rules: List[str], fish: List[str]) -> dict:
        common, rows = sec["frames"][ref]
        base = self.decode_verdict(sec, common, rules)
        out = {}
        for row in rows:
            f = fish[row[0]]
            own = row[1:] * 3 if len(row) == 2 else row[1:]
            out[f] = {o: {**base, **self.decode_verdict(sec, own[i], rules)}
                      for i, o in enumerate(ORIGINS)}
        return out

    @staticmethod
    def decode_verdict(sec: dict, ref: int, rules: List[str]) -> dict:
        speaks, beside, shown, nym, partly, lost = sec["verdicts"][ref]
        out = {}
        for state, lst in (("speaks", speaks), ("beside", beside), ("shown", shown),
                           ("not_yet_mapped", nym)):
            for i in lst:
                out[rules[i]] = [state, None, None, None]
        for i, by in partly:
            out[rules[i]][1] = [rules[b] for b in by]
        for i, r, by in lost:
            out[rules[i]] = [sec["reason_state"][r], None, sec["reasons"][r], rules[by]]
        return out


class AnswerCodec:
    name = "answer"

    def encode(self, values: List[dict], rix: Dict[str, int], fix: Dict[str, int]) -> Tuple[dict, List[int]]:
        decided, frames = Table(), Table()
        refs = []
        for v in values:
            rows = []
            for f in sorted(v, key=lambda f: fix[f]):
                ds = [decided.add([STATUSES.index(v[f][o][0]), v[f][o][1],
                                   None if v[f][o][2] is None else rix[v[f][o][2]]]) for o in ORIGINS]
                rows.append([fix[f], ds[0]] if ds[0] == ds[1] == ds[2] else [fix[f]] + ds)
            refs.append(frames.add(rows))
        return {"statuses": list(STATUSES), "decided": decided.rows, "frames": frames.rows}, refs

    def decode_frame(self, sec: dict, ref: int, rules: List[str], fish: List[str]) -> dict:
        out = {}
        for row in sec["frames"][ref]:
            own = row[1:] * 3 if len(row) == 2 else row[1:]
            out[fish[row[0]]] = {}
            for i, o in enumerate(ORIGINS):
                s, daily, win = sec["decided"][own[i]]
                out[fish[row[0]]][o] = [sec["statuses"][s], daily, None if win is None else rules[win]]
        return out


#: One codec per section the file may hold — the encoder and the decoder iterate this.
CODECS = {c.name: c for c in (LadderCodec(), AnswerCodec())}


# --------------------------------------------------------------------------------------------
# The file
# --------------------------------------------------------------------------------------------

def encode(model: Model, data: dict) -> dict:
    rule_ids = data["rule_ids"]
    rix = {k: i for i, k in enumerate(rule_ids)}
    fish = sorted({f for vals in model.sections.values() for per_key in vals
                   for v in per_key for f in v})
    fix = {f: i for i, f in enumerate(fish)}
    segs = Table()
    keys = []
    for key, starts in zip(model.keys, model.segments):
        k = dict(zip(PART_KEY_FIELDS, key))
        keys.append([k["ruleset"], k["licensing_set"], int(k["steelhead_water"]), k["steelhead"],
                     int(k["steelhead_rules"]), list(k["province_except"]), list(k["home_region"]),
                     segs.add(list(starts))])
    sections = {}
    for name, per_key in model.sections.items():
        codec = CODECS.get(name)
        if codec is None:
            raise AnswersError(f"answers: section {name!r} has no codec (encode.CODECS)")
        flat = [v for vals in per_key for v in vals]
        tables, refs = codec.encode(flat, rix, fix)
        at, n = [], 0
        for vals in per_key:
            at.append(refs[n:n + len(vals)])
            n += len(vals)
        sections[name] = {"version": model.versions[name], "at": at, **tables}
    wire = {
        "about": {**model.about, "format": FORMAT,
                  "export": {"rule_ids_sha256": rule_ids_digest(rule_ids), "rules": len(rule_ids)},
                  "sections": {n: s["version"] for n, s in sections.items()},
                  "reserved": dict(SPEC["reserved"]),
                  "counts": {"keys": len(keys), "segments": sum(len(s) for s in model.segments),
                             "waters": len(model.parts),
                             "parts": sum(len(p) for p in model.parts.values()),
                             "fish": len(fish)}},
        "spec": SPEC,
        "fish": fish,
        "keys": keys,
        "segments": segs.rows,
        "parts": model.parts,
        "sections": sections,
    }
    gaps = spec_gaps(wire)
    if gaps:
        raise AnswersError("answers: " + "; ".join(gaps))
    return wire


def check_pair(wire: dict, data: dict, guide: dict) -> None:
    """Refuse an answers file that is not cut for this export pair."""
    if wire["about"].get("format") != FORMAT:
        raise AnswersError(f"answers: format {wire['about'].get('format')!r} is not {FORMAT!r}")
    for name, other in (("ui-rules-export.json", data), ("ui-rules-guide.json", guide)):
        if wire["about"]["bundle"] != other["about"]["bundle"]:
            raise AnswersError(f"answers: about.bundle is not {name}'s")
    if wire["about"]["export"]["rule_ids_sha256"] != rule_ids_digest(data["rule_ids"]):
        raise AnswersError("answers: rule_ids_sha256 is not the export's rule_ids")


def decode(wire: dict, data: dict) -> Model:
    """The reference decoder: wire -> `answers.Model`."""
    if wire["about"].get("format") != FORMAT:
        raise AnswersError(f"answers: format {wire['about'].get('format')!r} is not {FORMAT!r}")
    if wire["about"]["export"]["rule_ids_sha256"] != rule_ids_digest(data["rule_ids"]):
        raise AnswersError("answers: rule_ids_sha256 is not the export's rule_ids")
    rules, fish = data["rule_ids"], wire["fish"]
    keys, segments = [], []
    for k in wire["keys"]:
        keys.append((k[0], k[1], bool(k[2]), k[3], bool(k[4]), tuple(k[5]), tuple(k[6])))
        segments.append(list(wire["segments"][k[7]]))
    sections = {}
    for name, sec in wire["sections"].items():
        codec = CODECS.get(name)
        if codec is None:
            raise AnswersError(f"answers: the file holds section {name!r}, which no codec reads")
        sections[name] = [[codec.decode_frame(sec, ref, rules, fish) for ref in at]
                          for at in sec["at"]]
    about = {k: v for k, v in wire["about"].items()
             if k not in ("format", "export", "sections", "reserved", "counts")}
    return Model(about=about, keys=keys, parts=wire["parts"], segments=segments,
                 sections=sections, versions={n: s["version"] for n, s in wire["sections"].items()})


def spec_gaps(wire: dict) -> List[str]:
    """Every section must have a codec and its text in `SPEC`; every key of the file and of each
    section must be described; a reserved section must be absent."""
    out = []
    top = SPEC["top level"]
    out += [f"spec does not describe the top-level key {k!r}" for k in wire if k not in top]
    for name, sec in wire.get("sections", {}).items():
        if name in RESERVED:
            out.append(f"section {name!r} is reserved for v1 and must be absent")
        if name not in CODECS:
            out.append(f"section {name!r} has no codec")
        text = SPEC["sections"].get(name)
        if not text:
            out.append(f"section {name!r} has no spec text")
            continue
        out += [f"spec does not describe {name}.{k}" for k in sec if k not in text]
    out += [f"codec {n!r} has no spec text" for n in CODECS if n not in SPEC["sections"]]
    return out


# --------------------------------------------------------------------------------------------
# A tap (the page's lookup, in Python)
# --------------------------------------------------------------------------------------------

def segment_index(starts: List[int], day: int) -> int:
    if not 1 <= day <= DAYS:
        raise AnswersError(f"answers: day {day} is not 1..{DAYS}")
    return bisect_right(starts, day) - 1


def tap(wire: dict, data: dict, item: str, part: int, month: int, day: int,
        fish: str, origin: str) -> Optional[dict]:
    """What the page reads for one tap: {section name: the fish's entry for that origin}, or None
    for a part with no rule set. A fish the key does not answer is refused (KeyError)."""
    k = wire["parts"][item][part]
    if k is None:
        return None
    key = wire["keys"][k]
    s = segment_index(wire["segments"][key[7]], day_of(month, day))
    out = {}
    for name, sec in wire["sections"].items():
        frame = CODECS[name].decode_frame(sec, sec["at"][k][s], data["rule_ids"], wire["fish"])
        out[name] = frame[fish][origin]
    return out
