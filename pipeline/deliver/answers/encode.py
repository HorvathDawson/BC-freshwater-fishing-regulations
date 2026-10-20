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
    "format": f"`about.format` is {FORMAT!r}: answers file format 2 (minor 2.4: the unnamed "
              "water — top-level `places` and `unnamed`; an unnamed part takes the key of its "
              "content, a named part's where one holds it, else a new key after the named parts' "
              "keys; no frame and no named key changed). A reader refuses any other. "
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
        "places": "[{regions, label, names}] — THE REGION SETS sections lie in (answers 2.4, S1b; "
                  "user 2026-10-10: \"just show regions the section is in\"): `regions` the regions "
                  "whose zone tables they take (never empty), `label` their words (\"Region 5\", "
                  "\"Regions 3 and 8\"), `names` {lake, stream, wetland} the name to show, whole, "
                  "for an unnamed answer there of that water kind (the key's `kind`): \"Unnamed "
                  "stream · Region 5\". A section's place is the regs store's `section_place` (every "
                  "section in B.C.); its MUs are not in the answers or the store (the tiles' MU layer), never in a name. "
                  "Model: `schemas[\"top.places\"]`",
        "unnamed": "{first, parts} — THE UNNAMED WATER'S KEYS (answers 2.4; S1b keyed by content): "
                   "an unnamed part (the bundle's `unnamed_part`: unnamed sections in B.C. sharing "
                   "every part-key fact and the water kind) takes the key of its content — a named "
                   "part's where one holds the same content (the frames are the scope's either "
                   "way), else a new key; every key from `first` on is such a new one. `parts[up]` "
                   "the key of unnamed part `up`. The keys before `first` are the named parts' "
                   "(unchanged). Such a key is read exactly as a part's (`tap` steps 2-4); its "
                   "frames are the zone base and every area row its sections lie in, as their rule "
                   "set holds them. A client reaches it by section handle through the regs store's "
                   "`section_akey` (never by item); the name is `places[section_place].names[kind]`. "
                   "Model: `schemas[\"top.unnamed\"]`",
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
            "version": "27",
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
                    "against?, origins?, ranges?}] (5.8: ONE item per kind, each fish in exactly one; the "
                    "keep range is the first band's from and the last's to, a band of 0 a slot "
                    "to release; `conds` indexes the row's `conds` about these fish; `against` "
                    "the number a fish with its own row counts toward here, \"unlimited\" for a "
                    "total with no number; absent on other items; `ranges` (version 5, gap G4) on "
                    "a cross-reference of SEVERAL fish: [[fish, from_cm, to_cm|null]] each fish's "
                    "own keep range, a fish keeping none absent — `origins` is the one-fish "
                    "form); `real_daily` {n, "
                    "all, sum, capped_sum, rb, shared_cap [{take, over_cm}] | null, capped, "
                    "open, shared_count?} or null (5.7: \"Really {sum} a day here\" when `all`; version 4, "
                    "decision F11: `shared_count` [{take, members}] the count limits several kinds "
                    "share — 1 trout from streams is 1 for brown, cutthroat, rainbow and steelhead "
                    "TOGETHER; a cap's `except` (F12) is decided here, never on the page); "
                    "`group` (version 6, decision F14) on ONE row per total, else null or absent: "
                    "{pool, total (a number or \"unlimited\"), kinds, subcaps [{r, take, water "
                    "(stream | lake | null), over_cm, under_cm, of, lifted [{members, by}]}], "
                    "headline, origin, lifts, total_line, buckets, worked, states} "
                    "— the TOTAL the row's fish count toward (the row's pool, or the outer pool its "
                    "fish are contained by), every kind on the card counting toward it (steelhead "
                    "included), each count or size clause of it holding some of them here (`of`) "
                    "with the kinds a water's own rule lifts out of it (`lifted`); version 8 "
                    "(answers 2.8) THE CARD, water-first, each fact once, every number proved by "
                    "`card_check`: `headline` {n (null: no daily limit), unit, says} this water's "
                    "number with no lift (\"2 a day from this stream\"; a lake: the total), `origin` "
                    "(\"Hatchery only — release wild trout and char.\" or null), `lifts` [{says, by, "
                    "kinds, n, each}] a water's rules lifting kinds out of the share, said positively "
                    "in ONE line per lifting entry (version 9, ROUND10 W1: each kind with its number, "
                    "highest first), `each` [{kinds, n, by, fish}] every lifting rule (cite `by`'s "
                    "Book page; the lift's `by`/`n` are the first's), `total_line` the total under a share (\"No more than 4 "
                    "trout and char a day across Region 2, lakes included.\") or null, `buckets` "
                    "[{title (null: the only one), cap, by, why: share | rule | total, kinds "
                    "[{species, name, detail, n, notes, duties}], notes [{says, n, over_cm, kinds, "
                    "not_counting}]}] the kinds under the cap they draw from, each kind its species "
                    "(one picture each); version 10 (ROUND11 A1): a size cap the kinds share is the "
                    "BUCKET's note, said once (\"Only 1 of these may be over 50 cm, not counting "
                    "steelhead\"; a cap spanning buckets on the first: \"Only 1 of the day’s trout and "
                    "char may be over 50 cm\"), never on a kind row (the gate proves `n` over "
                    "`over_cm` for `kinds`, and `not_counting` still keepable after them); `worked` [{kept, q, a, species, origin, share, here, other, away}] RULINGS "
                    "Q23b worked through for a lift past the share; `states` [{r, line}] the cap "
                    "each card line states (none twice). `says` and `items` (versions 6-7) are gone; "
                    "`elsewhere` (version 6) on every "
                    "keep row: {says, ask [[question, answer]]} the kept-elsewhere line, qualified "
                    "by those lifts. Version 11 (ROUND12 1-4): a kind row's `detail` says a lifted "
                    "limit (\"any length, 50 cm or less\") and a slot's ranges (\"60 cm or less, or 90 cm "
                    "or longer\"); a water's own rule bucket(s) come first, then the share, then the "
                    "rest; EVERY cap inside the group is a bucket note, said once where it binds — a "
                    "kind's own (\"Only 1 rainbow trout may be over 90 cm\"), a count over two kind rows "
                    "(`over_cm` 0: \"Only 1 of these may be cutthroat trout or rainbow trout\"), nested "
                    "caps in order on one note (`then` [{n, over_cm, kinds}]: \"Only 2 of these may be "
                    "over 30 cm, and only 1 over 50 cm\"); a cap another implies is not said; `worked` is "
                    "ONE list (\"How it adds up\" and \"Fished elsewhere today?\" merged): the keep-N "
                    "lines then the kept-elsewhere examples they do not repeat (on a water with no lift, "
                    "those examples), each {kept, q, a, species, origin, share (keep-here lines only), "
                    "here, other, away (null: not claimed), at: here | stream | lake, over_cm, of (the "
                    "kinds the numbers count, null every kind), most: exactly | at_most} proved by the "
                    "gate; the card row's `elsewhere.ask` is empty (folded), its `says` kept. Version 12 (ROUND13 Z/S/D/P): a "
                    "kind keeping none says no size (`detail` \"catch and release\"); `not_kept` [{species, "
                    "status: closed | release, says, by, whose}] every fish of the card's row nobody may keep "
                    "here, said with whose rule (\"Steelhead: no fishing for them\", \"this river’s rule\"; cite "
                    "`by`'s Book page; gate-proved: none may be kept); a lifting bucket's `replaces` {says, "
                    "rules} (null on every other bucket) names each cap of the region its lifts replace "
                    "(\"Replaces Region 2’s 2-from-streams limit and 30 cm minimum size for these fish\") — "
                    "show it under the bucket title; `lifts` stay for proof, not for display; the lake line "
                    "says the count left (\"Kept 1 at a lake?\" \"Keep 2 more here. Fish from lakes count "
                    "toward the 4 a day, not toward the 2 from streams.\"). Version 13 (ROUND14 3-6): a count over SOME of a bucket's kinds is a "
                    "nested group `buckets[].groups` [{says, n, by, kinds}] (\"Cutthroat trout or rainbow trout: 1 "
                    "between them\", each kind row with its own size; show the groups before the bucket's size "
                    "notes; those kinds are no longer in `buckets[].kinds`); a water's own pool printing the "
                    "share's very number is its own bucket (\"This river’s rule\", the Bella Coola mainstem's 2); "
                    "the steelhead presence line `presence` {line: possible_with_rules | known_with_rules, says} "
                    "sits INSIDE the steelhead section, exactly once — on the card's steelhead kind row, on its "
                    "not-kept steelhead line, or on the steelhead's own row (`rows[].presence`); `known_no_rules` "
                    "stays the frame's `steelhead_line` only; a weekday window `rows[].week` {species, says} "
                    "(\"Keep up to 5 on Saturdays and Sundays; release Monday to Friday\") on every moment's frame "
                    "of the day alike — show it in place of the day's keep / release words, and read the year "
                    "strip at every moment of a day (the best answer), never as a list of dates; `not_kept` is "
                    "one line per status and deciding row, fish in the book's order, a group named with its "
                    "members (\"Char (Dolly Varden/bull trout, lake trout and brook trout): release every one\"), "
                    "`rules` every deciding rule (`by` the first). Version 14 (ROUND15 1/3/5-8): a SIZE CAP IS A GROUP "
                    "`buckets[].sizes` [{says, n, over_cm, by, species, not_counting, then [{n, over_cm, species}]}] "
                    "naming EXACTLY the kinds it can bind today (kept and able to pass the size, the book's order: "
                    "\"Brown trout, Dolly Varden/bull trout, lake trout or brook trout: only 1 over 50 cm between "
                    "them\") — it names its kinds, their rows stay in the bucket; show it boxed like a count group; "
                    "`notes` now hold only a count spanning buckets (`over_cm` 0); `buckets[].between` (\"Up to 2 a "
                    "day between them\") an own-rule bucket's shared cap, else null; each kept kind's `detail` says "
                    "its origin (\"hatchery, 30 cm or longer\"); a rule restating its row's winner (same fish, "
                    "number, days, sizes) is ONE ladder line (role agrees) and a note `rows[].restates` [{r, of, says}] "
                    "(the Lower West Arm's combined kokanee quota with the Upper West Arm); `week` adds `status` "
                    "(\"Weekends: keep 5 · Weekdays: release\") and `parts` [{species, days \"Sat–Sun\", when "
                    "\"Saturday and Sunday\", weekdays, status, n, says \"keep up to 5\", rules, today}] — how it was "
                    "decided per day-part, `today` the frame's own moment; a tributaries-only rule reached by the walk "
                    "is \"the Bella Coola River’s rule for its tributaries\"; no presence line on a wholly closed "
                    "water; no \"the\" before a lake's name. Version 15 (ROUND16 1/2/Q2): every group carries `table` {bands "
                    "{species: [{from, to|null, says}]}, cells {species: [{n (null: no limit), why (released | closed | "
                    "too small | too big | protected size, when n is 0), origin (only this origin may be kept), "
                    "by_origin {hatchery, wild} | null, as (\"ST\": a rainbow that size IS a steelhead, its number the "
                    "steelhead's; \"RB\" the other way)}]}, steelhead_cm} — per species the size bands from the "
                    "rules' own cut points (merged where nothing kept here changes), each cell the most of that fish "
                    "alone here today — and the CAP SYSTEM: `cap_kinds` [{id \"RB.1\" | \"RB.1.h\", species, from, "
                    "to|null, origin|null}] (a basket's vocabulary: size bands every rule's cut points draw, split by "
                    "origin only where a rule tells them apart; a rainbow over `steelhead_cm` on a steelhead water is "
                    "a steelhead kind) and `caps` [{take, kinds {here, stream, lake}, scope, by, says, if_kept_here}]: "
                    "a basket — counts per kind kept here, on another stream, on another lake of the region today — "
                    "is LEGAL IFF for every cap the basket's fish of its kinds (each place's list) number at most "
                    "`take`; `if_kept_here`: the cap holds only once the basket keeps one of its `here` kinds (a "
                    "water's own number counting fish kept elsewhere, rows F9); a take-0 cap: none of those kinds may "
                    "be kept. The basket is read as \"what may I still keep HERE after what I kept elsewhere\" (the "
                    "fish kept elsewhere first). The card gate proves every cell against the engine and that the caps "
                    "accept exactly the baskets the engine accepts (seeded random baskets with fish kept elsewhere, "
                    "every rule band at its take and one past it). The other card fields stay for now. A water's own "
                    "limit (`elsewhere.says`, ROUND16 2) says whose it is, its number and which fish count: \"The "
                    "Bella Coola River’s rule: 1 a day here. Brown trout, cutthroat trout or rainbow trout you kept "
                    "anywhere else today count toward that 1.\" `rows[].restates` (ROUND16 Q2) also holds ANOTHER "
                    "row's identical statement no nearer the stream than the winner's — shown once, credited to the "
                    "nearer row: \"Zone B also prints this: “All from Williston Lake and its tributaries”.\" A "
                    "decided answer's winner may be a size release (F13: a take-0 band releasing the fish's whole "
                    "range, status release). Version 16 (ROUND18): every group carries `tree` — the card as FOLDERS, "
                    "draw it so: ONE tier {node \"tier\", id \"t1\", tier, n, says, by, children} of tier \"total\" "
                    "(\"4 a day across Region 2, lakes included\"), holding tiers \"own\" (a water's own rule: says "
                    "\"This river’s rule: up to 4 a day\", `whose`, `replaces` \"Replaces Region 2’s 2-from-streams "
                    "limit and 30 cm / 12 in minimum size for these fish\", `why_here` \"its row covers its "
                    "tributaries, so it applies here\" where the tributary walk brought the rule, else null, `line` all "
                    "three in one line), \"share\" (\"2 a day from this stream\"), \"count\" (\"1 between them\") and "
                    "\"size\" (`over_cm`, `species` the kinds its cap names, `then` [{n, over_cm}] nested caps, `extra` "
                    "{n, species} | null — RULINGS Q16 as ONE tier: \"1 over 50 cm / 20 in a day — a second may be a hatchery "
                    "steelhead\"), and kinds {node \"kind\", species, name, n, says \"Up to 2 may be brook, brown or "
                    "cutthroat trout\", detail \"hatchery, 30 cm / 12 in or longer\", sizes {cm [[from, to|null]], in "
                    "[[from, to|null]]}, also_counts [{tier (a size tier's id) | null, by | null, n, over_cm (0: a count), "
                    "then, species, says \"only 1 over 50 cm / 20 in, if you haven’t already kept a Dolly Varden/bull "
                    "trout or lake trout over 50 cm\"}], duties, sentence \"Of the 2 a day: up to 2 may be … (…) — "
                    "…\", presence?}. Each kind sits EXACTLY ONCE, under the tightest tier holding it; a cap no tier "
                    "holds is said on the kinds it binds. `header` {says \"2 a day from this stream · 4 across Region 2, "
                    "lakes included\", tag \"hatchery only\" | null} is the group's top in one line. Sizes are said in "
                    "cm AND whole inches (the nearest; the cm is the book's) in the tree, the lift lines, the "
                    "headline's `also` and the kept-elsewhere lines; the bucket fields keep cm only and stay for now. "
                    "The card gate proves every tier's and kind's number, each kind's sizes, every `also_counts`, the "
                    "Q16 `extra` and every number in the tree's sentences and the header. Version 17 (ROUND19): sizes "
                    "read \"50 cm (20 in)\" everywhere (tree, header, also_counts, sentences, details, lift lines, "
                    "kept-elsewhere lines, table bands, cap sentences). A kind keeping one origin says it with the fish "
                    "(says \"Up to 2 may be hatchery brook, brown or cutthroat trout\", detail \"30 cm (12 in) or "
                    "longer\"; `header.tag` null — the names carry it; the card `origin` \"Keep hatchery fish only — "
                    "wild trout and char go back.\"). A size tier HOLDS every kind row its cap binds (each that may be "
                    "kept over its size), nested in the tightest tier holding them all (wrapping whole tiers where they "
                    "span tiers); a kind that can never pass the size stays outside; says \"Only 1 of these may be over "
                    "50 cm (20 in)\"; RULINGS Q16's steelhead sits INSIDE: \"… — or 2, if one is a hatchery steelhead\" "
                    "(`extra` {n, species, by}) and the steelhead kind's `size_note` {tier, says \"it counts toward the "
                    "1 over 50 cm (20 in), but once you keep one, a second fish over 50 cm (20 in) may be kept\"}; a kind's "
                    "sentence is \"of\" its nearest COUNT tier (a size tier is no count); a cap on one kind row, part "
                    "of a row, or that no tier can hold without a kind that cannot pass it stays on the kinds' "
                    "`also_counts`. A count tier names its fish (\"1 between cutthroat and rainbow trout\"); an own or "
                    "share tier holding fewer than the count tier around it says \"(part of the 4)\"; the header "
                    "\"2 a day from this stream · part of 4 a day across Region 2, lakes included\"; `replaces` names "
                    "the fish (\"… minimum size for hatchery rainbow trout\"). THE BOUNDARY STANDARD (RULINGS P37): "
                    "`table.bands[]` and `cap_kinds[]` carry `from_in` / `to_in` — whether the band holds its ends; "
                    "a fish exactly at a cut belongs to the band whose words include it (\"X or less\" / \"X or "
                    "longer\" hold X, \"under X\" / \"over X\" do not). The gate proves every size tier's placement "
                    "(each kind under it may pass its size; its caps bind no kind outside it) and that each cut "
                    "belongs to exactly one band. Version 18 (UR1, user panel round 1): totals say \"per angler\" "
                    "(\"4 a day per angler across Region 2, lakes included\", the header and `total_line` too); a share "
                    "tier on a water whose own rule lifts kinds out of it names them (\"2 a day from this stream (part "
                    "of the 4) — your hatchery rainbow trout don’t use these 2\"); a size tier says \"Only 1 of the fish "
                    "in this box may be over 50 cm (20 in) — 2 if one is a hatchery steelhead\" (draw it as a box); a "
                    "count tier \"1 cutthroat or rainbow trout in all\" (a kind's sentence \"Of the 1 cutthroat or "
                    "rainbow trout in all: …\"); a species closure \"Steelhead: no fishing for them, not even catch and "
                    "release — release any you hook by accident\" (p.4 legend); the record duty \"write each one on "
                    "your licence right away\". Version 19 (UR2, user panel round 2): the headline's unit says "
                    "\"per angler\" (\"2 a day per angler from this stream\", a lake \"4 a day per angler\"); the worked "
                    "lines read \"If you keep 3 hatchery rainbow trout\" / \"1 more fish of any kind, from here or from "
                    "a lake.\" (every number gate-proved); a kind held to fewer than its count tier \"Only 1 of the 2 "
                    "may be …\"; a steelhead kind keeping hatchery fish only \"Up to 2 may be hatchery steelhead only — "
                    "wild steelhead go back\"; where a rainbow over 50 cm is a steelhead the presence line \"Steelhead "
                    "are known to be in this water, so a rainbow trout over 50 cm (20 in) counts as a steelhead.\" "
                    "(p.80); the stop duty \"after you keep 2, stop fishing for the day\". Version 20 (UR3, user panel "
                    "round 3): a count tier inside a water's own tier or the share says \"(part of the 2)\"; the share "
                    "\"2 a day per angler from this stream\", a lifted one \"— your hatchery rainbow trout count toward "
                    "the 4, not these 2\"; a quota two waters share (`restates`) \"Kokanee from the Lower and Upper West "
                    "Arms together: no more than 5 a day.\" (the fish and number the rule's fields). Version 21 "
                    "(ROUND20, owner review of v24): `header` IS the tree's root tier — `tier` its id (\"t1\"), `says` "
                    "its line (\"4 a day per angler across Region 2, lakes included\"; a water's own total \"This "
                    "river’s rule: up to 1 a day per angler\"): draw the root's line as the group's header block and "
                    "the root's children at the top of the tree, one level less; the share tier says it sits inside "
                    "the total, \"Only 2 of the 4 may be from this stream\", a lifted one \", not counting hatchery "
                    "rainbow trout\" (the UR3 words read as 4 + 2). Version 22 (ROUND20b, owner: fewer words without missing "
                    "anything): an own tier whose only child is one kind row at its number names the fish itself "
                    "(\"This river’s rule: up to 4 hatchery rainbow trout a day\") and the kind carries `folded: true` "
                    "— draw its size, notes and pictures under the line and no sentence, so the number appears once; an "
                    "own tier's `line` is whose rule and why it applies here (no \"Replaces …\"; `replaces` stays a "
                    "field); under the total the share tier comes before a water's own tiers; the worked keep-N lines "
                    "are the two ends of the range only (\"If you keep 4 …\", \"If you keep 2 …\"). Version 23 (ROUND20f 3): a share "
                    "that is the REGION'S (a zone or province rule — the Region 2 streams' combined 2) says \"from streams\" / "
                    "\"from lakes\" (\"Only 2 of the 4 may be from streams\", the headline \"2 a day per angler from streams\"); a "
                    "share a water's own row sets keeps \"from this stream\" (`tree.share_place`). Version 24 (ROUND20g): "
                    "where the one-origin rule holds on the share's water only (Region 2's \"Wild trout/char from streams\", "
                    "p.21) the SHARE tier carries `origin` (\"From streams, keep hatchery fish only — wild trout and char go "
                    "back.\") and the group's `origin` is null — draw it under the share's line; the steelhead kind has no "
                    "`size_note` (the size tier's \"— 2 if one is a hatchery steelhead\" says it once); duties \"stop fishing here "
                    "after 2 a day\", \"record each on your licence right away\"; the presence line \"A rainbow trout over 50 "
                    "cm (20 in) here counts as a steelhead.\". Version 25 (ROUND20h): `group.possession` {per_daily, of, n, by, "
                    "example, doubles} — the possession rule's multiplier on the group's TOTAL (p.21 \"Possession quotas = 2 daily "
                    "quotas\", p.80): \"With 4 trout and char a day you may have 8: 2 days’ fish.\", where a share sits under "
                    "the total \"Possession doubles the 4 a day, not the 2 from streams.\" (null otherwise); null with no one "
                    "possession rule holding every kind. Version 26 (ROUND20i, user panel UR4): a tree kind's `size_rule` — the size as "
                    "the rule, \"keep 30–50 cm (12–20 in); release the rest\" (\"release smaller ones\" where the top is the "
                    "steelhead line; null for any size, none, a steelhead); the worked keep-share line \"none from other streams "
                    "today — up to 2 more of any kind, from here or from lakes.\"; the stop duty \"stop fishing this river after "
                    "2 a day\" (the water's own row's noun, p.21 \"stop fishing that water\"); `elsewhere.says` \"Your own "
                    "limit, counted across every Region 2 stream you fish today.\" Version 27 (ROUND UR5, user panel 5): the kept-elsewhere over-the-size question names the fish the "
                    "group's size clause holds here (Kootenay Lake main body: \"Kept cutthroat trout over 50 cm (20 in) elsewhere today?\", "
                    "not the Region 4 rule's \"rainbow trout or cutthroat trout\": rainbow has its own row there).",
        },
        "gear": {
            "what": "Stage 7.1-7.6, the gear answer per part key and segment (`gear.resolve`): "
                    "counts, specs, elements, circumstantial clauses, hook, fly, bait, ways to "
                    "fish, conduct by moment, vessel rules, timed / in-part / side / while rules, "
                    "overruled rules, the rules that decide and those that repeat",
            "version": "12",
            "at": "[[frame index per segment] per key]",
            "frames": "[gear answer] — on TIDAL water (key `tidal` 1) the documented tidal state "
                      "`{tidal: true, note, see, licence}` (`common.TIDAL_STATE`) and nothing else; "
                      "otherwise {summary {says, parts [{part: mode | lines | hook | attach | weight, says, "
                      "cites, terms}], terms, cites, changes [{when, says, cites}]} (version 7, ROUND11 "
                      "A2: the day's terminal gear in ONE sentence — version 8, ROUND12 5, no em dash: \"1 "
                      "line with a single barbless hook (one hook, lure or fly). Weight up to 1 kg.\" — version 9, "
                      "ROUND13 G/P: no hook named when nothing restricts it (\"1 line (2 while alone in a boat) "
                      "with one hook, lure or fly.\"; the `hook` part stays in `parts`), \"weight on the line up "
                      "to 1 kg\" — version 10, UR1 G: the weight in kg and lb (\"weight on the line up to 1 kg "
                      "(2.2 lb)\"); where the only circumstance is being in a boat, the lines part \"1 line from "
                      "shore; any number from a boat\" (the sentence then \"…, each with one hook, lure or fly.\") "
                      "and the Lines row \"1 line (rod) from shore; no limit from a boat\" — made from the count, hook, "
                      "barb, lure and weight answers; `terms` glossary ids for the ⓘ; `changes` the "
                      "other days of the year at this moment that read differently, in calendar "
                      "order: {when: \"May 1–31\", says: \"fly fishing only\"}), "
                      "counts {slot: {by, over, also?, cites, says?}} (version 6, ROUND10 W4: "
                      "`cites` every rule deciding the row, `says` the Lines row in plain words — "
                      "\"1 line (rod)\" (version 7: no \", in a boat too\"), \"1 line (rod); 2 while "
                      "alone in a boat\"), specs {slot: [clause]}, "
                      "elements {slot:member: {verdict, by, over}}, main [clause], circumstantial "
                      "[{clause, while?, targeting?, note?}], hook, fly, bait [{element, ok, by, "
                      "why?, carry_kg?, also_allowed?, excepted_from? {element, by} — version 11 (ROUND20h): the ban an allowed "
                      "element is an exception to, the province's fin-fish ban's roe, on the days it is in force}], bait_ban, "
                      "(version 12, ROUND20i: `summary.changes[].when` leaves out the days the water is closed; the hook "
                      "\"a single barbless hook, no trebles\"; fly fishing only \"no float or strike indicator, no sinker or "
                      "split shot, no lure, spinner or other attracting device\") ways [{method, allowed, by, "
                      "why?, not_for?, for?, while?, conduct?, while_rules?, devices?}], conduct "
                      "{moment: [[act, [rule]]]}, vessel {active, timed}, timed, in_part, side, "
                      "while_rules, caught, overruled [{rule, state, reason, by}], decides, repeats}; "
                      "`caught` (version 4, user ruling Q38): the duties for a fish CAUGHT some "
                      "way in force here — \"Any fish snagged — even by accident — must be "
                      "released\" (`display.rules[].plain`), whatever the angler does; a "
                      "clause is [rule, clause index into the export rule's `gear`]",
            "province_methods": "[method] — the ways the province allows you to sport fish",
            "parent": "{member: wider member} — the element tree a clause may name",
            "methods": "[method] — the ways to fish the answer reads",
            "moments": "[[moment, [[act, short phrase, the model's sentence]]]] — the Always cards",
            "conduct_means": "{act: sentence} — every conduct act's sentence",
            "count_nouns": "{slot: [one, many]} — the noun a count of that slot takes (version 5, answers 2.6, owner): lines_per_angler [\"line (rod)\", \"lines (rods)\"] — \"1 line (rod)\", \"2 lines (rods)\"; a set line keeps \"line\"",
        },
        "licence": {
            "what": "Stage 7.7, the licence answer per part key, segment and angler profile: the "
                    "reader's requirements in force (`read.requirements_in_force`) and, per "
                    "profile, the documents to buy, the requirements that are the angler's, "
                    "exemptions, guiding and other anglers' rules",
            "version": "6",
            "at": "[[frame index per segment] per key]",
            "frames": "[[holds, documents, under_16?]] — indexes into `holds`, `documents` and `under_16` "
                      "(absent on tidal water)",
            "under_16": "[[{residency, label, none_needed, with_adult {who, counts} | null, buy [{doc, name, "
                        "prices}], own_limit, says}] per residency] — version 5 (ROUND15 2): what an under-16 "
                        "angler needs here, per residency (not guided, no status), read off the profiles' "
                        "answers: fish with a licensed adult (`counts`: what they keep counts toward the "
                        "adult's limit) or buy `buy` (`own_limit`: and keep a limit of their own); `says` the "
                        "sentence. Version 6 (ROUND16 4): the book's p.6 said exactly — a B.C. resident \"No licence or "
                        "stamp needed, and you don’t have to fish with a licence holder. You keep your own limit.\" "
                        "(`own_limit` true); a non-resident \"Fish with someone 16 or older who holds the licences and "
                        "stamps this fishing needs: what you keep counts toward their limit. Or buy your own … and any "
                        "supplementary licences and stamps you need, and keep your own limit.\"",
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
                       "presumes_freed?, presumes_by?, terms?}], exempt?, others?, guiding?}] — "
                       "`others` / `guiding` (version 4, gap G2) [{req, paths}]: another angler's "
                       "or a guide's requirement with how it is met AS THE RECORD PRINTS IT "
                       "(`satisfied_by`: each path `need` the documents it holds, "
                       "`accompanied_by?`, `as?`, `quota?`; nothing freed, nothing to buy)",
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
            "residency_labels": "{residency: words} — version 5 (ROUND15 2): \"B.C. resident\", \"Canadian, "
                                "other province\", \"From outside Canada\" — the answer says whose it is",
            "profile_labels": "[words per profile] — version 5: each profile's answer labelled, in `profiles` "
                              "order (\"B.C. resident, 16 or older\")",
        },
        "display": {
            "what": "Derived display facts: per part key and segment the status index's code; per "
                    "export rule its kind, closure, size bands and plain sentence; per water each "
                    "export part's names and picker facts",
            "version": "16",
            "at": "[[frame index per segment] per key]",
            "frames": "[{status, closing, unless_opened?, strip}] — `status` base | own | closed (the verdicts' "
                      "`reading.closed` at the segment's moment: closed = every game fish under a "
                      "speaking full closure) | tidal: on TIDAL water `{status: tidal, tidal: true, "
                      "note, see, licence}` all year (no freshwater status). `closing` (version 3, "
                      "gap G1): [[rule, [fish code]]] every full closure that SPEAKS, not partly "
                      "lifted, for some game fish at this segment, with the game fish it closes "
                      "(`verdicts.project.closing`) — the rules that close the water when status "
                      "is closed (every game fish is under one), or close those fish; decided, "
                      "never to be re-derived from the ladder. `unless_opened` (version 4, user ruling "
                      "Z12/Q41): the closing rules printed \"unless opened\" whose proviso is the "
                      "answer here (say `rules[].unless_opened`: \"Closed unless opened by Parks "
                      "Canada — a national park fishing permit is required.\"); absent where a "
                      "closure with no proviso closes the same fish (a national park RESERVE: "
                      "plainly closed). `strip` (version 5, answers 2.6): THE OPEN-DAYS STRIP's code "
                      "for this segment, the OR of the bits of `strip` (closed 1 alone; release 2 — "
                      "no game fish on the card may be kept; fly_only 4; artificial_fly_only 8; "
                      "bait_ban 16; 0 open), from this status and the `rows` and `gear` frames at "
                      "the same key and segment — draw it, never re-derive it (absent on a tidal "
                      "frame). `inherited` (version 8, ROUND11 A3): [{says, by, lifts}] the lifts this "
                      "answer relies on that ANOTHER water's ✱ row carries to its tributaries "
                      "(RULINGS T13 / T3a) — \"Open Jul 15–Aug 31: the Quinsam River’s exemption from "
                      "the summer stream closure (MUs 1-1 to 1-6) covers its tributaries.\" — `by` "
                      "the lifting rule (cite its Book page; its `rules[].applies_trib`), `lifts` "
                      "the rules it lifts today; show it on the KEEP side; absent where none (a "
                      "quota lift the card's `lifts` already names is not repeated). Version 12 (ROUND16 Q1, RULINGS "
                      "T15): `inherited[].caution` where a NEARER water row binding this stream prints no such "
                      "exemption — the lift still holds; show the caution under the line: \"Trout Lake’s tributaries’ "
                      "row, nearer this stream, prints no such exemption; the Duncan River’s is applied here because "
                      "its row covers all its tributaries.\" (null elsewhere). Version 13 (ROUND19 2/5): rule titles "
                      "and sentences say sizes \"30 cm (12 in)\"; a slot \"none over 60 cm (24 in), under 90 cm (35 in)\" "
                      "(the boundary standard, RULINGS P37). Version 14 (UR1 G): a record duty reads \"Write each "
                      "hatchery steelhead on your licence right away\" (title and sentence). Version 15 (UR3 2): a "
                      "no-fishing place rule's `plain` is the book's words with every distance in feet too, \"No "
                      "fishing within 23 m (75 ft) downstream of …\" (the nearest 5 ft)",
            "rules": "[{kind, title, closure?, bands?, plain?, unless_opened?, subsets?}] — aligned with "
                     "the export's `rules`: `title` (version 12, ROUND16 3) every rule's effect in words, "
                     "generated from its fields (\"Release all steelhead\", \"Release lake trout, Oct 1–Nov 30\", "
                     "\"Release Dolly Varden/bull trout from streams, Aug 1–Oct 31\"; gear, boats and notices take their "
                     "label) — the heading of a rule anywhere it is listed, its verbatim the quote under it; the page's 15-step kind (`caught`: a duty for a fish "
                     "caught some way, Q38), a gate that closes, size bands [[from_cm, to_cm|null, "
                     "take|null]], the plain sentence (null: the page uses the label; every "
                     "possession line carries the possession quota's exception, `display.POSSESSION_HOME`, Q15), `unless_opened` "
                     "a closure's proviso in words (Z12), `subsets` (gap G3) [{fish, for, plain?}] "
                     "the rule said for the fish subsets a row's or an item's ladder lists it for "
                     "(`plain` null: no sentence, write `for` beside the label); version 5 (answers "
                     "2.6, HOW THIS WAS DECIDED): `book` [[printed page, PDF page]] — the entry's "
                     "page as the book prints it, and the 1-based page of the official synopsis PDF "
                     "(open it at `#page=<PDF page>`; the export guide's `pdf_pages`), `applies` one "
                     "line saying why the rule applies (its scope: B.C.-wide, a region's table, an "
                     "area, this water's own row, a federal or park authority; and what a lift "
                     "lifts; version 9, ROUND13 P: the book named by its title, \"This water’s own row in "
                     "the 2025–2027 BC Freshwater Fishing Regulations Synopsis: …\"), `applies_trib` the same line where the rule is INHERITED by the "
                     "tributary walk (a part whose rule set lists it under `trib`; null where it never is); "
                     "version 7 (answers 2.8, D2): `where` the heading a standing no-fishing place rule "
                     "shows under on the KEEP side beside the open/closed strip (\"Where you can’t "
                     "fish\": the fishway and fish-facility buffers), absent on every other rule",
            "licensing": "[{book}] — aligned with the export's `licensing_ids` (version 6, answers "
                         "2.7): a licensing record's [[printed page, PDF page]], as `rules[].book` — "
                         "the page cites a record (a Classified Water, an exemption) as it cites a rule",
            "strip": "[{state, bit, label, says}] — the open-days strip's legend (version 5): "
                     "open 0, closed 1, release 2, fly_only 4, artificial_fly_only 8, bait_ban 16, "
                     "with the label and sentence to show",
            "reasons": "{reason: {lost, won}} — why a rule lost, in words (version 5), for every "
                       "reader reason a ladder's `lost` entry carries: `lost` said of the loser "
                       "(\"{by}\": write the rule that beat it), `won` said of that winner "
                       "(\"{rule}\": write the rule it beat)",
            "waters": "{item_id: {parts, picker, unresolved_licensing}} — `parts` aligned with "
                      "the export's parts ({order, label, short, runs, place, hint, km, closed_all_year, "
                      "paper_licence [rule]} | null outside B.C.; `short` (version 6, answers 2.7) the "
                      "part strip's name from the range's end labels, mouth upward: \"Mouth → Vedder "
                      "Crossing Bridge\", \"Site C dam → Hwy. 29 bridge (2 stretches)\", \"Side "
                      "channel\" — `label` stays the sheet's full name); `picker` {choices [{parts "
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
    static_keys = ("profiles", "profile_dims", "residency_labels", "profile_labels")

    def encode(self, values, rix, fix):
        holds, answers, docs, u16, frames = Table(), Table(), Table(), Table(), Table()
        refs = []
        for v in values:
            row = [holds.add(v["holds"]), docs.add([answers.add(p) for p in v["profiles"]])]
            if v.get("under_16") is not None:
                row.append(u16.add(v["under_16"]))            # ROUND15 2
            refs.append(frames.add(row))
        return {"holds": holds.rows, "answers": answers.rows, "documents": docs.rows,
                "under_16": u16.rows, "frames": frames.rows}, refs

    def decode_frame(self, sec, ref, rules, fish):
        h, d, *u = sec["frames"][ref]
        out = {"holds": sec["holds"][h], "profiles": [sec["answers"][i] for i in sec["documents"][d]]}
        if u:
            out["under_16"] = sec["under_16"][u[0]]
        return out


#: One codec per section the file may hold — the encoder and the decoder iterate this.
CODECS = {c.name: c for c in (LadderCodec(), RowsCodec(),
                              FrameCodec("gear", ("province_methods", "parent", "methods",
                                                  "moments", "conduct_means", "count_nouns")),
                              LicenceCodec(), FrameCodec("display", ("rules", "waters", "strip",
                                                                     "reasons", "licensing")))}


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
    unnamed = model.unnamed if model.unnamed is not None else {"first": len(keys), "parts": []}
    validate_top("places", model.places)
    validate_top("unnamed", unnamed)
    used = set(unnamed["parts"])
    if unnamed["first"] > len(keys) or any(k >= len(keys) for k in used) or \
            not set(range(unnamed["first"], len(keys))) <= used:
        raise AnswersError(f"answers: `unnamed` (first {unnamed['first']}, {len(used)} keys used by "
                           f"{len(unnamed['parts'])} parts) names a key not in the {len(keys)} keys, "
                           f"or a key from `first` on no unnamed part takes")
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
                             "unnamed_keys": len(keys) - unnamed["first"],
                             "unnamed_parts": len(unnamed["parts"]),
                             "places": len(model.places),
                             "fish": len(fish)}},
        "spec": SPEC,
        "schemas": _schemas(),
        "fish": fish,
        "keys": keys,
        "segments": segs.rows,
        "moments": moments.rows,
        "segment_moments": seg_moments.rows,
        "parts": model.parts,
        "places": model.places,
        "unnamed": unnamed,
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
                 statics=statics, moments=at, glossary=wire.get("glossary"),
                 places=wire["places"], unnamed=wire["unnamed"])


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
    return tap_key(wire, data, k, month, day, fish, origin, profile, weekday, inside)


def tap_key(wire: dict, data: dict, k: int, month: int, day: int,
            fish: Optional[str] = None, origin: Optional[str] = None,
            profile: Optional[int] = None, weekday: Optional[str] = None,
            inside: bool = False) -> dict:
    """`tap` for a key index — a named part's (`parts[item][i]`) or an unnamed part's (from
    `unnamed.parts`, answers 2.4: what the regs store's `section_akey` gives a section)."""
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
