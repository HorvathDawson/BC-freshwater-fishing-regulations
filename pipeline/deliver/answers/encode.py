"""THE ANSWERS FILE'S WIRE FORMAT (`ui-rules-answers.json`), its encoder and its reference decoder.

`encode(model, data)` writes `answers.Model` against the export it pairs with (`data`: the shipped
`ui-rules-export.json`); `decode(wire, data)` returns exactly the model (`cli build` refuses a file
that does not decode to it). Every rule is an INTEGER index into the export's `rules` array (the
codec's order: `rule_ids` sorted), every licensing record an index into its `licensing` array, every
fish an index into `fish` (ladder, answer) or its code, every repeated value interned.

THE SECTIONS ARE GENERIC. The top level holds what every section shares — `keys`, `segments`,
`parts`, `fish` — and `sections` holds one object per named section, each `{version, at, …its
tables}`, where `at[key][segment]` is the index of that (part key, segment)'s value in the section's
`frames`; a section may also carry STATIC tables (not keyed by part key and segment: per export rule,
per water, the angler profiles). The encoder and the decoder iterate `CODECS`; a section without a
codec, or without its text in `SPEC`, is refused (`spec_gaps`, pinned by the tests).
"""
from __future__ import annotations

from bisect import bisect_right
from typing import Dict, List, Optional, Tuple

from pipeline.deliver.answers.answers import (ORIGINS, PART_KEY_FIELDS, RESERVED, AnswersError,
                                              Model)
from pipeline.deliver.calendar import DAYS, day_of
from pipeline.deliver.answers.common import Interner
from pipeline.deliver.bundle import read

FORMAT = "answers/2"

#: The ladder's verdict lists, in wire order.
VERDICT_LISTS = ("speaks", "beside", "shown", "not_yet_mapped", "partly", "lost")
#: The loss reasons, in wire order (`read.LOSS_REASONS`: reason -> the state it gives).
REASONS = tuple(read.LOSS_REASONS)
#: The `keys` row's slot holding the segments index.
SEG_SLOT = len(PART_KEY_FIELDS)
#: The `keys` row's slot holding the segment-moments index, or null (the last; answers 2.1).
MOMENT_SLOT = SEG_SLOT + 1


#: The SHA-256 (first 16 hex digits) of the export's `rule_ids`, one per line: the integer rule refs
#: mean these rules and no others — the bundle's `meta.rule_ids_sha256` (one definition).
from pipeline.deliver.bundle.derived import rule_ids_digest  # noqa: E402


# --------------------------------------------------------------------------------------------
# The field dictionary, shipped in the file as `spec`
# --------------------------------------------------------------------------------------------

_RULE = "a rule ref (index into the export's `rules`)"
_LIC = "a licensing ref (index into the export's `licensing`)"

SPEC = {
    "format": f"`about.format` is {FORMAT!r}: answers file format 2. A reader refuses any other. "
              "Changed from format 1 (DATAFLOW P7): the `answer` section is gone (the decided "
              "answer is `rows`' alone); a no-limit answer and row say `no_limit` (was `nolimit`); "
              "a part's `km` and `place` are null where there is none (were 1e9 and \"\"); the "
              "ladder lists EVERY fish the verdicts asked (every game fish, and crayfish, chinook "
              "or a protected fish a member rule names); `schemas` holds every frame's JSON "
              "Schema.",
    "pairing": "This file pairs with ONE export pair (ui-rules-export.json + ui-rules-guide.json): "
               "`about.bundle` equals both files' `about.bundle` (digests `reach_digest`, "
               "`section_handles`), and `about.export.rule_ids_sha256` is the first 16 hex digits "
               "of the SHA-256 of the export's `rule_ids` joined by \"\\n\". Refuse a pair that "
               "differs: every rule ref below indexes the export's `rules` / `rule_ids`, every "
               "licensing ref its `licensing` / `licensing_ids`.",
    "calendar": "A day is 1..366 on the leap calendar: Jan 1 = 1, Feb 29 = 60, Mar 1 = 61 in EVERY "
                "year, Dec 31 = 366 (day = days before the month in a leap year + day of month). "
                "In a year without Feb 29, day 60 is never asked. Feb 29 is answered as the "
                "reader answers it: a rule printed to Feb 28 does not hold on it, so it can be a "
                "segment of its own.",
    "tap": "How a tap resolves, with lookups only: (1) the water's item id and the part's index i "
           "in the export's `waters[item].parts` -> k = `parts[item][i]` (null: the part has no "
           "rule set — wholly outside B.C., `water.outside_bc`); (2) the day d -> the segment s = "
           f"the last index with `segments[keys[k][{SEG_SLOT}]][s] <= d`; WHERE THAT START "
           "REPEATS (the day reads differently by weekday or hour: `keys[k]["
           f"{MOMENT_SLOT}]` not null), the segments sharing it are the day's MOMENTS — take the one "
           f"whose `moments[segment_moments[keys[k][{MOMENT_SLOT}]][s]]` holds the date's "
           "weekday; of two for that weekday, `hours.in` false is the answer at every hour but "
           "the window and `hours.in` true the answer inside it (show both: 'closed 1 hour after "
           "sunset to 1 hour before sunrise'); (3) per section, frame = "
           "`sections[name].frames[sections[name].at[k][s]]` (decoded as the section says); (4) in "
           "the frame, the fish and the origin (none | hatchery | wild), or the angler profile.",
    "top level": {
        "about": "`what`, `format`, `bundle` (the export pair's digests), `export` "
                 "{`rule_ids_sha256`, `rules`: count}, `sections` {name: version} of the sections "
                 "present, `reserved` {name: what it will hold} — sections not yet present "
                 "(absent, never empty), `counts`",
        "spec": "this dictionary",
        "schemas": "{section | section.table: JSON Schema} — every frame's and static table's "
                   "model (`pipeline/deliver/answers/model.py`: strict, no extra field; each "
                   "nullable field null exactly where its model says)",
        "fish": "[fish code] — the leaf species codes (`ui-rules-guide.json` `species`) the "
                "ladder and answer frames refer to by index",
        "keys": "[[ruleset, licensing_set, steelhead_water, steelhead, steelhead_rules, "
                "province_except, home_region, kind, tidal, segments, moments]] — one per distinct PART "
                "KEY: the export part's rule set and licensing set ids (integers, licensing set or "
                "null), `steelhead_water` (0/1: a rainbow over 50 cm is a steelhead here, the "
                "part's `anadromous_rainbow`), `steelhead` (\"known\" | \"possible\" | null), "
                "`steelhead_rules` (0/1: the bundle's fact — the export ships it only as false on "
                "a known part), `province_except` [kind], `home_region` [region], `kind` (the "
                "water's: lake | stream | wetland), `tidal` (0/1), `segments` (an index into "
                "`segments`) and `moments` (an index into `segment_moments`, or null: no weekday "
                "or hours rule makes the part's day read two ways — answers 2.1)",
        "segments": "[[start day]] — interned; a key's segments start on these days (the first is "
                    "always 1) and run to the day before the next start (the last to 366). A "
                    "segment is a run of days on which every section's inputs read the same: the "
                    "union of every section's cuts (a member rule's or a lift's `when`, a "
                    "requirement's or a designation's `when`)",
        "parts": "{item_id: [key index | null]} — aligned with the export's `waters[item].parts`",
        "moments": "[{weekdays, hours}] — interned MOMENTS (answers 2.1, user ruling 2026-10-08: "
                   "a rule held on some weekdays or hours DECIDES then and is out at the other "
                   "moments). `weekdays` the days of the week the segment holds on (Monday "
                   "first); `hours` null (every hour of them) or {start, end, in}: the window of "
                   "the part's hours rule as the export prints it (`at` HH:MM or `solar` sunrise "
                   "| sunset, with `offset_min`), `in` true inside the window, false at every "
                   "other hour. Model: `schemas[\"top.moments\"]`",
        "segment_moments": "[[moment index per segment]] — interned; aligned with a key's "
                           "`segments` where its slot "
                           f"{MOMENT_SLOT} is not null (a start day then repeats once per "
                           "moment group, ordered by first weekday, outside the window before "
                           "inside). A key whose slot is null holds every segment at every "
                           "moment",
        "sections": "{name: section} — see `sections`",
        "glossary": "{version, terms: [{id, term, says, example?, pages, quote, source}]} — the "
                    "plain-language GLOSSARY (answers 2.2): every piece of jargon the page shows "
                    "(classified waters, the regions, daily and possession quota, catch and "
                    "release, single barbless hook, bait ban, tributaries, hatchery and wild, "
                    "steelhead, the stamps, set lining, guided, under 16 …), GENERATED from the "
                    "export's rules, licensing records and entries and the book's own text "
                    "(`pipeline/deliver/answers/glossary.py`), never written per water. `says` the "
                    "plain words, `example` a worked case from the data, `pages` the PRINTED book "
                    "pages, `quote` the book's words verbatim, `source` what it was generated "
                    "from. A term is linked by its `id` (regions: `region_<code>`, e.g. "
                    "`region_4`, `region_7a`). Model: `schemas[\"top.glossary\"]`",
    },
    "sections": {
        "ladder": {
            "what": "Stage 4, the ladder: per fish and origin, every rule the reader returns "
                    "(`read.effective_rules_bound(trace=True, origin=…)`) — speakers and losers",
            "version": "1",
            "at": "[[frame index per segment] per key]",
            "frames": "[[common, [[fish, v] | [fish, v_none, v_hatchery, v_wild]]]] — `common` a "
                      "verdict index holding the rules whose entry is the same for every fish and "
                      "origin of the frame; then per fish (index into `fish`, ascending) its own "
                      "verdict index, one for all three origins or one per origin. A fish's "
                      "verdict for an origin = `common` + its own (disjoint by rule). The fish "
                      "listed are every fish the verdicts asked: every game fish (ST everywhere: "
                      "answered as RB where no steelhead rule applies), and crayfish, chinook or a "
                      "protected fish where a member rule names it",
            "verdicts": "[[speaks, beside, shown, not_yet_mapped, partly, lost]] — the first four "
                        "sorted rule refs in that state; `partly` [[rule, [lifting rule]]]: a "
                        "rule in one of those states that a lift holds for only some anglers or "
                        "fish (`partly_lifted`); `lost` [[rule, reason, by]]: a rule that took "
                        "part and lost, `reason` an index into `reasons`, `by` the rule that beat "
                        "or lifted it",
            "reasons": "[reason] — `read.LOSS_REASONS` keys: the step that removed the rule",
            "reason_state": "[state] parallel to `reasons`: lifted | displaced | moot",
        },
        "rows": {
            "what": "Stages 5.1-5.8, today's card, from the ladder's speakers (the page's evalSp, "
                    "buildModel, rowConds, speciesItems, quotaLines, effCap): the fish asked "
                    "about, per fish and origin the number after the winner's clauses with every "
                    "line and role, the rows (one per shared limit), their conditions, each kind's "
                    "keep range and band numbers, and the real daily limit",
            "version": "4",
            "at": "[[frame index per segment] per key]",
            "frames": "[[spp, {fish code: [decided h, decided w]}, [row], steelhead_line]] — "
                      "`spp` the fish the card asks about (5.1, the page's order); per fish the "
                      "hatchery and wild answers (indexes into `decided`, null: no rule in scope "
                      "speaks); the rows in card order (indexes into `rows`); `steelhead_line` "
                      "possible_with_rules | known_with_rules | known_no_rules | null (5.6)",
            "decided": "[{status: keep|no_limit|release|closed, win, daily, narrow, lines, roles, "
                       "lift_notes}] — `daily` the number after the winner's clauses (null for "
                       "no limit, release, closed), `narrow` the clause that lowered it; `lines` "
                       "[{t, r, a?, b?, take?, o?, keepO?, status?, daily?, min?, max?, says?, "
                       "only?, capped?, carve?, outer?}] in the page's order (t: rel, cap, "
                       "subcap, also, outer, steel, outercap, outersize, orphan, partly, caution, "
                       "tnote, annual, possession_cap, duty, record; a/b band edges in cm, b "
                       "null = no top; possession_cap is a possession limit with a number, never "
                       "a yearly limit); `roles` [[rule, role, by]] (governs, agrees, contains, "
                       "also, narrows, limit, floor, season, possession_cap, duty, possession, "
                       "falls, moot, replaced, lifted); "
                       "`lift_notes` [[lifter, {when_targeting | while | lengths}]]. Rules are "
                       "rule refs",
            "rows": "[{kind, pool, win, members, all_members, daily, narrow, everyone, groups, "
                    "prot, wins, lift_notes, scope, conds?, items?, real_daily?}] — `kind` keep | "
                    "no_limit | release | closed; `pool` (keep rows) or `win` the rule; `members` "
                    "the fish (codes) of the row, `all_members` with the fish that go back; "
                    "`everyone` the lines for every member and `groups` [{members, facts}] the "
                    "rest, each fact a line plus `members`, `rules`, `general`, `carve_of` "
                    "(origin, origin2, exc, xref included); `prot` the protected fish of an "
                    "open-subject row; `scope` {of: water | area | region | bc, entry, share, "
                    "apart} (the badge, 5.5: `entry` the area's or region's entry; `apart` the "
                    "row's fish are counted apart from a wider total they lift, with none left "
                    "to count toward; every daily limit, a water's own too, counts fish kept "
                    "elsewhere today); `conds` the conditions on keeping ({c: origin | size | "
                    "back | group | cap | subcap | outercap | outersize | origin2 | streamcap, "
                    "who?, …}; a cap with `except` is general but for those fish); `items` "
                    "[{members, bands [[from_cm, to_cm|null, number]], back, xref, sub, conds, "
                    "against?, origins?}] (5.8: ONE item per kind, each fish in exactly one; the "
                    "keep range is the first band's from and the last's to, a band of 0 a slot "
                    "to release; `conds` indexes the row's `conds` about these fish; `against` "
                    "the number a fish with its own row counts toward here, \"unlimited\" for a "
                    "total with no number; absent on other items); `real_daily` {n, "
                    "all, sum, capped_sum, rb, shared_cap [{take, over_cm}] | null, capped, "
                    "open, shared_count?} or null (5.7: \"Really {sum} a day here\" when `all`; version 4, "
                    "decision F11: `shared_count` [{take, members}] the count limits several kinds "
                    "share — 1 trout from streams is 1 for brown, cutthroat, rainbow and steelhead "
                    "TOGETHER; a cap's `except` (F12) is decided here, never on the page)",
        },
        "gear": {
            "what": "Stage 7.1-7.6, the gear answer per part key and segment (`gear.resolve`): "
                    "counts, specs, elements, circumstantial clauses, hook, fly, bait, ways to "
                    "fish, conduct by moment, vessel rules, timed / in-part / side / while rules, "
                    "overruled rules, the rules that decide and those that repeat",
            "version": "3",
            "at": "[[frame index per segment] per key]",
            "frames": "[gear answer] — on TIDAL water (key `tidal` 1) the documented tidal state "
                      "`{tidal: true, note, see, licence}` (`common.TIDAL_STATE`) and nothing else; "
                      "otherwise {counts {slot: {by, over, also?}}, specs {slot: [clause]}, "
                      "elements {slot:member: {verdict, by, over}}, main [clause], circumstantial "
                      "[{clause, while?, targeting?, note?}], hook, fly, bait [{element, ok, by, "
                      "why?, carry_kg?, also_allowed?}], bait_ban, ways [{method, allowed, by, "
                      "why?, not_for?, for?, while?, conduct?, while_rules?, devices?}], conduct "
                      "{moment: [[act, [rule]]]}, vessel {active, timed}, timed, in_part, side, "
                      "while_rules, overruled [{rule, state, reason, by}], decides, repeats}; a "
                      "clause is [rule, clause index into the export rule's `gear`]",
            "province_methods": "[method] — the ways the province allows you to sport fish",
            "parent": "{member: wider member} — the element tree a clause may name",
            "methods": "[method] — the ways to fish the answer reads",
            "moments": "[[moment, [[act, short phrase, the model's sentence]]]] — the Always cards",
            "conduct_means": "{act: sentence} — every conduct act's sentence",
        },
        "licence": {
            "what": "Stage 7.7, the licence answer per part key, segment and angler profile: the "
                    "reader's requirements in force (`read.requirements_in_force`) and, per "
                    "profile, the documents to buy, the requirements that are the angler's, "
                    "exemptions, guiding and other anglers' rules",
            "version": "3",
            "at": "[[frame index per segment] per key]",
            "frames": "[[holds, documents]] — indexes into `holds` and `documents`",
            "holds": "[{tidal: {tidal, note, see, licence}} on TIDAL water (the documented state "
                     "only) | {holds, displaced, wrong_water, waived, not_yet_mapped, also_printed, "
                     "designations, stamp_period, contested, considered}] — licensing refs: the "
                     "requirements that hold, the ones a superior authority displaces ({ref: "
                     "[superior ref]}), for the other kind of water, waived, in an undrawn part, "
                     "folded restatements ({ref: [ref]}), the designations in force, whether a "
                     "stamp period runs, whether the licensing set is contested, every record "
                     "considered",
            "documents": "[[answer per profile]] — 60 indexes into `answers`, in `profiles` order",
            "answers": "[{tidal: true} on TIDAL water (no provincial document or requirement; "
                       "the federal tidal licence is in `holds.tidal`) | {documents [{doc, when {act, species?, lengths?, on?}, base, prices, also_when?, "
                       "or?}], "
                       "none_needed, requirements [{req, when, paths, displaced_by?, "
                       "presumes_freed?, presumes_by?, terms?}], exempt?, others?, guiding?}]",
            "documents_v3": "version 3 (answers 2.2): a document's `when` is the BROADEST "
                            "requirement needing it (to fish at all > to fish for a kind > to "
                            "keep one), `also_when` the narrower ones (Babine's Steelhead Stamp: "
                            "to fish at all Sep 1-Oct 31, else to fish for steelhead — decision "
                            "L10); `or` [{need, alt?, prices}] the other ways to satisfy what it "
                            "is bought for, never a second document to buy (Teslin: basic "
                            "licence OR Yukon angling licence — decision L9)",
            "profiles": "[residency/age/guidance/status] — the 60 angler profiles; the index is "
                        "mixed radix over `profile_dims`",
            "profile_dims": "[[dimension, [value]]] — residency, age, guidance, status",
        },
        "display": {
            "what": "Derived display facts: per part key and segment the status index's code; per "
                    "export rule its kind, closure, size bands and plain sentence; per water each "
                    "export part's names and picker facts",
            "version": "3",
            "at": "[[frame index per segment] per key]",
            "frames": "[{status, closing}] — `status` base | own | closed (the verdicts' "
                      "`reading.closed` at the segment's moment: closed = every game fish under a "
                      "speaking full closure) | tidal: on TIDAL water `{status: tidal, tidal: true, "
                      "note, see, licence}` all year (no freshwater status). `closing` (version 3, "
                      "gap G1): [[rule, [fish code]]] every full closure that SPEAKS, not partly "
                      "lifted, for some game fish at this segment, with the game fish it closes "
                      "(`verdicts.project.closing`) — the rules that close the water when status "
                      "is closed (every game fish is under one), or close those fish; decided, "
                      "never to be re-derived from the ladder",
            "rules": "[{kind, closure?, bands?, plain?}] — aligned with the export's `rules`: the "
                     "page's 15-step kind, a gate that closes, size bands [[from_cm, to_cm|null, "
                     "take|null]], the plain sentence (null: the page uses the label)",
            "waters": "{item_id: {parts, picker, unresolved_licensing}} — `parts` aligned with "
                      "the export's parts ({order, label, runs, place, hint, km, closed_all_year, "
                      "paper_licence [rule]} | null outside B.C.); `picker` {choices [{parts "
                      "[export part index], closed, sections, heading, text}], headed}; "
                      "`unresolved_licensing` [licensing ref]. `km` null: no run carries a measure; `place` null: "
                      "no entry heading",
        },
    },
    "reserved": {k: f"not yet present: {v}" for k, v in RESERVED.items()},
}


# --------------------------------------------------------------------------------------------
# Interning
# --------------------------------------------------------------------------------------------

#: An interned list (`add(value)` -> its index; equal values share one): the keying module's.
Table = Interner


# --------------------------------------------------------------------------------------------
# Section codecs
# --------------------------------------------------------------------------------------------

class LadderCodec:
    name = "ladder"
    static_keys: Tuple[str, ...] = ()

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


class FrameCodec:
    """A section whose value per (key, segment) is one JSON value with its refs already integer
    (gear, display): the distinct values are the frames."""
    static_keys: Tuple[str, ...] = ()

    def __init__(self, name: str, static_keys: Tuple[str, ...] = ()):
        self.name, self.static_keys = name, static_keys

    def encode(self, values, rix, fix):
        frames = Table()
        return {"frames": frames.rows}, [frames.add(v) for v in values]

    def decode_frame(self, sec, ref, rules, fish):
        return sec["frames"][ref]


class RowsCodec:
    name = "rows"
    static_keys: Tuple[str, ...] = ()

    def encode(self, values, rix, fix):
        decided, rows, frames = Table(), Table(), Table()
        refs = []
        for v in values:
            fish = {S: [None if r is None else decided.add(r) for r in (x["hatchery"], x["wild"])]
                    for S, x in v["fish"].items()}
            refs.append(frames.add([v["spp"], fish, [rows.add(r) for r in v["rows"]],
                                    v["steelhead_line"]]))
        return {"decided": decided.rows, "rows": rows.rows, "frames": frames.rows}, refs

    def decode_frame(self, sec, ref, rules, fish):
        spp, fs, rs, line = sec["frames"][ref]
        return {"spp": spp,
                "fish": {S: {"hatchery": None if h is None else sec["decided"][h],
                             "wild": None if w is None else sec["decided"][w]}
                         for S, (h, w) in fs.items()},
                "rows": [sec["rows"][i] for i in rs], "steelhead_line": line}


class LicenceCodec:
    name = "licence"
    static_keys = ("profiles", "profile_dims")

    def encode(self, values, rix, fix):
        holds, answers, docs, frames = Table(), Table(), Table(), Table()
        refs = []
        for v in values:
            refs.append(frames.add([holds.add(v["holds"]),
                                    docs.add([answers.add(p) for p in v["profiles"]])]))
        return {"holds": holds.rows, "answers": answers.rows, "documents": docs.rows,
                "frames": frames.rows}, refs

    def decode_frame(self, sec, ref, rules, fish):
        h, d = sec["frames"][ref]
        return {"holds": sec["holds"][h], "profiles": [sec["answers"][i] for i in sec["documents"][d]]}


#: One codec per section the file may hold — the encoder and the decoder iterate this.
CODECS = {c.name: c for c in (LadderCodec(), RowsCodec(),
                              FrameCodec("gear", ("province_methods", "parent", "methods",
                                                  "moments", "conduct_means")),
                              LicenceCodec(), FrameCodec("display", ("rules", "waters")))}


# --------------------------------------------------------------------------------------------
# The file
# --------------------------------------------------------------------------------------------

def encode(model: Model, data: dict) -> dict:
    rule_ids = data["rule_ids"]
    rix = {k: i for i, k in enumerate(rule_ids)}
    fish = sorted({f for name in ("ladder",) for per_key in model.sections.get(name, [])
                   for v in per_key for f in v})
    fix = {f: i for i, f in enumerate(fish)}
    segs, moments, seg_moments = Table(), Table(), Table()
    keys = []
    per_key_moments = model.moments or [None] * len(model.keys)
    if len(per_key_moments) != len(model.keys):
        raise AnswersError("answers: the model's moments are not one per key")
    from pipeline.deliver.answers.model import validate_top
    for key, starts, at in zip(model.keys, model.segments, per_key_moments):
        k = dict(zip(PART_KEY_FIELDS, key))
        if at is not None and len(at) != len(starts):
            raise AnswersError(f"answers: key {key} has {len(at)} moments for {len(starts)} segments")
        if at is None and len(set(starts)) != len(starts):
            raise AnswersError(f"answers: key {key} repeats a start day without moments")
        keys.append([k["ruleset"], k["licensing_set"], int(k["steelhead_water"]), k["steelhead"],
                     int(k["steelhead_rules"]), list(k["province_except"]), list(k["home_region"]),
                     k["kind"], int(k["tidal"]), segs.add(list(starts)),
                     None if at is None else seg_moments.add([moments.add(m) for m in at])])
    validate_top("moments", moments.rows)
    if model.glossary is None:
        raise AnswersError("answers: the model has no glossary (answers 2.2)")
    validate_top("glossary", model.glossary)
    # THE TYPES AT THE BOUNDARY (answers/2): every distinct value of every section, and its static
    # tables, against its model before anything is encoded
    from pipeline.deliver.answers.model import validate_section
    for name, per_key in model.sections.items():
        validate_section(name, (v for vals in per_key for v in vals), model.statics.get(name))
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
        static = model.statics.get(name, {})
        if set(static) != set(codec.static_keys):
            raise AnswersError(f"answers: section {name!r} has static tables {sorted(static)}, its "
                               f"codec expects {sorted(codec.static_keys)}")
        sections[name] = {"version": model.versions[name], "at": at, **tables, **static}
    wire = {
        "about": {**model.about, "format": FORMAT,
                  "export": {"rule_ids_sha256": rule_ids_digest(rule_ids), "rules": len(rule_ids)},
                  "sections": {n: s["version"] for n, s in sections.items()},
                  "reserved": dict(SPEC["reserved"]),
                  "counts": {"keys": len(keys), "segments": sum(len(s) for s in model.segments),
                             "moments": len(moments.rows),
                             "waters": len(model.parts),
                             "parts": sum(len(p) for p in model.parts.values()),
                             "fish": len(fish)}},
        "spec": SPEC,
        "schemas": _schemas(),
        "fish": fish,
        "keys": keys,
        "segments": segs.rows,
        "moments": moments.rows,
        "segment_moments": seg_moments.rows,
        "parts": model.parts,
        "glossary": model.glossary,
        "sections": sections,
    }
    gaps = spec_gaps(wire)
    if gaps:
        raise AnswersError("answers: " + "; ".join(gaps))
    return wire


def _schemas() -> dict:
    from pipeline.deliver.answers.model import json_schemas
    return json_schemas()


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
    keys, segments, at = [], [], []
    for k in wire["keys"]:
        keys.append((k[0], k[1], bool(k[2]), k[3], bool(k[4]), tuple(k[5]), tuple(k[6]), k[7],
                     bool(k[8])))
        segments.append(list(wire["segments"][k[SEG_SLOT]]))
        m = k[MOMENT_SLOT]
        at.append(None if m is None else [wire["moments"][i] for i in wire["segment_moments"][m]])
    sections, statics = {}, {}
    for name, sec in wire["sections"].items():
        codec = CODECS.get(name)
        if codec is None:
            raise AnswersError(f"answers: the file holds section {name!r}, which no codec reads")
        sections[name] = [[codec.decode_frame(sec, ref, rules, fish) for ref in at]
                          for at in sec["at"]]
        if codec.static_keys:
            statics[name] = {k: sec[k] for k in codec.static_keys}
    about = {k: v for k, v in wire["about"].items()
             if k not in ("format", "export", "sections", "reserved", "counts")}
    return Model(about=about, keys=keys, parts=wire["parts"], segments=segments,
                 sections=sections, versions={n: s["version"] for n, s in wire["sections"].items()},
                 statics=statics, moments=at, glossary=wire.get("glossary"))


def spec_gaps(wire: dict) -> List[str]:
    """Every section must have a codec and its text in `SPEC`; every key of the file and of each
    section must be described; a reserved section must be absent."""
    out = []
    top = SPEC["top level"]
    out += [f"spec does not describe the top-level key {k!r}" for k in wire if k not in top]
    for name, sec in wire.get("sections", {}).items():
        if name in RESERVED:
            out.append(f"section {name!r} is reserved and must be absent")
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

def segment_index(starts: List[int], day: int, moments: Optional[List[dict]] = None,
                  weekday: Optional[str] = None, inside: bool = False) -> int:
    """The segment holding a day (§2). Where the day's start repeats (`moments`), the one whose
    moment holds `weekday` (a `calendar.WEEKDAYS` name) and, with an hours window, is inside it
    or not (`inside`); a weekday must then be given."""
    if not 1 <= day <= DAYS:
        raise AnswersError(f"answers: day {day} is not 1..{DAYS}")
    s = bisect_right(starts, day) - 1
    if moments is None or (s == 0 or starts[s - 1] != starts[s]) and \
            (s + 1 == len(starts) or starts[s + 1] != starts[s]):
        return s
    if weekday is None:
        raise AnswersError(f"answers: day {day} reads differently by weekday or hour — name one")
    first = starts.index(starts[s])
    for i in range(first, s + 1):
        m = moments[i]
        if weekday in m["weekdays"] and (m["hours"] is None or m["hours"]["in"] == inside):
            return i
    raise AnswersError(f"answers: no moment of day {day} holds {weekday} (inside={inside})")


def tap(wire: dict, data: dict, item: str, part: int, month: int, day: int,
        fish: Optional[str] = None, origin: Optional[str] = None,
        profile: Optional[int] = None, weekday: Optional[str] = None,
        inside: bool = False) -> Optional[dict]:
    """What the page reads for one tap: {section name: its frame}, the fish's entry for that origin
    where a section is per fish (ladder, answer, rows' decided answer), the profile's answer for
    the licence — or None for a part with no rule set. A fish the key does not answer is refused
    (KeyError)."""
    k = wire["parts"][item][part]
    if k is None:
        return None
    key = wire["keys"][k]
    m = key[MOMENT_SLOT]
    s = segment_index(wire["segments"][key[SEG_SLOT]], day_of((month, day)),
                      None if m is None else [wire["moments"][i]
                                              for i in wire["segment_moments"][m]],
                      weekday, inside)
    out = {}
    for name, sec in wire["sections"].items():
        frame = CODECS[name].decode_frame(sec, sec["at"][k][s], data["rule_ids"], wire["fish"])
        if name == "ladder" and fish is not None:
            frame = frame[fish][origin]
        elif name == "licence" and profile is not None:
            frame = {"holds": frame["holds"], "profile": frame["profiles"][profile]}
        out[name] = frame
    return out
