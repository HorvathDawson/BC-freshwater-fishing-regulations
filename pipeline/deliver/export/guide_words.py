"""The words of the guide and the field dictionary.

Every `*_TEXT` table the export ships, keyed by the model's own field and enum names, so
`checks.unexplained` can refuse a field, type or slot that has no words and words for one the model lost.
"""
from __future__ import annotations

from pipeline.deliver.answers.common import TIDAL_NOTE
from pipeline.regs.parsing.entry_models import Extent


#: WHERE A RULE ACTUALLY HOLDS, said once, on the record — so no reader has to cross-read
#: `fields.extents` against `provenance.uncertain` to learn that a rule it sees as `whole` binds
#: nothing (92 rules shipped that way: their row's water is not in the atlas).
BINDS_TEXT = {
    "sections": "placed: it holds on every section that carries it (find them in `rulesets`)",
    "sections_in_part": "placed on the sections of the water it is in, but it holds only in the "
                        "part `fields.undrawn_part` names, which nothing draws. SHOW it at the top "
                        "of the water, prominently, as a place not yet mapped (the record's "
                        "`not_yet_mapped`); NEVER colour or decide the water by it — it never "
                        "competes",
    "nowhere": "not placed: the reach builder could not bind it (`provenance.why`). Its "
               "`fields.extents` are what the rule states, not where it holds — a `whole` on an "
               "entry with no `matched` water names a water the atlas does not have. It can only "
               "ever raise 'unknown', never 'no rules here'",
}


NOT_PLACED = "not_placed"


#: WHAT BAIT IS, AND WHAT A BAIT BAN COVERS (UI consumer's report, 2026-10-03: worms have no entry
#: of their own and a page assumed them). The book's definitions, quoted, and what each `bait`
#: member of the model means. `unexplained` refuses a bait member a rule uses that this does not
#: explain.
BAIT_GUIDE = {
    "book": {
        "bait": {"page": 8, "text": "“Bait” is any foodstuff or natural substance used to attract "
                 "fish, other than wood, cotton, wool, hair, fur or feathers. It does not include "
                 "fin fish, other than roe. It includes roe, worms and other edible substances, "
                 "as well as scents and flavourings containing natural substances or nutrients."},
        "bait_ban": {"page": 4, "text": "Bait Ban: the use of natural bait (see the definition "
                     "of bait on page 8) is prohibited in waters with a bait ban."},
        "invertebrates": {"page": 8, "text": "Aquatic invertebrates… you may use freshwater "
                          "invertebrates (e.g., aquatic insects and crayfish) in streams as bait "
                          "unless a bait ban applies. No person shall use as bait or possess for "
                          "that purpose any freshwater invertebrate (this includes the aquatic "
                          "stage of any insect, such as dragonfly nymphs or caddisfly larvae) at "
                          "a lake."},
        "fin_fish": {"page": 8, "text": "The use of fin fish (dead or alive) or parts of fin fish "
                     "other than roe is prohibited throughout the province, with the following "
                     "exceptions: …"},
        "live_fish": {"page": 8, "text": "… never use live fish as bait …"},
    },
    "members": {
        "any_bait": "EVERY bait — roe, worms and other edible substances, scents and flavourings "
                    "containing natural substances or nutrients (p.8). A BAIT BAN is `ban: "
                    "[any_bait]`: it bans all of them, worms included, for every species and "
                    "every angler the rule names. It never reaches artificial flies or lures, or "
                    "wood, cotton, wool, hair, fur and feathers, which are not bait.",
        "roe": "fish eggs — bait (p.8), the one part of a fin fish that is; possession for bait "
               "is capped at 1 kg (`bait_possession_kg`)",
        "invertebrate": "a FRESHWATER invertebrate: aquatic insects at any aquatic stage "
                        "(dragonfly nymphs, caddisfly larvae) and crayfish (p.8). Allowed in "
                        "streams unless a bait ban applies; banned at lakes",
        "fin_fish": "fin fish, dead or alive, or parts of them — banned as bait province-wide, "
                    "except roe (`except: [roe]`)",
        "dead_fin_fish": "the head or headless body of a fin fish — allowed only when sport "
                         "fishing for sturgeon on the lower Fraser, Pitt and Harrison (Region 2), "
                         "or set lining in lakes of Region 6 and Zone A of Region 7 (p.8)",
        "live_fin_fish": "a live fish — never bait anywhere (p.8)",
    },
    "worms": {
        "says": "Worms have no rule of their own because the book names them only as BAIT (p.8, "
                "'It includes roe, worms …'). A bait ban (`any_bait`) bans them. The lake ban on "
                "invertebrates is about FRESHWATER invertebrates — aquatic insects and crayfish "
                "— and a worm sold or dug for bait is not one, so it does not reach worms. So: "
                "worms may be used on a lake or a stream where no bait ban applies, and may not "
                "where one does.",
        "reading": "an interpretation: the book never says 'worms are not freshwater "
                   "invertebrates'; it lists worms as bait and defines the invertebrates it bans "
                   "at lakes as aquatic. An aquatic worm (a leech, a freshwater worm taken from "
                   "the water) would be one.",
    },
    "what_a_bait_ban_covers": "Everything in `members.any_bait`, for every species, whatever you "
                              "fish for, unless the rule's own `when_targeting` or a clause's "
                              "`when` narrows it. Artificial flies and lures are untouched.",
}


# --------------------------------------------------------------------------------------------
# The guide's words. Every table below is checked against the model's own registry (see
# `problems`), so a type, slot, act, kind or field the model gains is refused until it is
# explained here, and one the model loses cannot linger.
# --------------------------------------------------------------------------------------------

#: WHAT `touches` MEANS — one text, used by the guide and the field dictionary.
TOUCHES_TEXT = (
    "The indexes (into this water's `parts`) of the OTHER parts of this water that this part "
    "borders. Two parts touch when a section of one and a section of the other are joined in "
    "the stream graph: river pieces joined end to end (the two sides of a split point, or of any "
    "cut between differently regulated stretches), or a branch of this same water — a side "
    "channel, a braid, a fork bearing the same name — flowing into it at a confluence. Nothing "
    "else touches: two stretches with a differently regulated stretch between them do not, and a "
    "river above a lake does not touch the river below it, because the lake between them is "
    "another water. Another water never appears here: a tributary joining the river is its own "
    "water with its own `parts`, and a lake part (`part_of`) is its own water too. Symmetric (if "
    "0 lists 1, 1 lists 0), never lists the part itself, sorted, and `[]` when the part borders "
    "no other part. A PART IS NOT ONE STRETCH: it is every section carrying the same sets, so "
    "it may be several stretches that do not meet one another; `touches` says that SOME section "
    "of it borders the other part. TO MERGE NEIGHBOURS (e.g. 'closed all year' shown once for a "
    "run of closed stretches), merge only parts connected through `touches` — the connected "
    "groups of the graph whose edges are `touches`, among the parts that qualify — never every "
    "qualifying part of the water.")

#: WHAT A TIDAL WATER MEANS TO AN ANGLER — the page's words for `waters[item].tidal`, from the
#: book's own note (p.17, Nitinat Lake).
#: The first sentences ARE the answers layer's own (`answers.common.TIDAL_NOTE`, FIX D12: "tidal
#: water — a different regulation system; see DFO regulations / the Fishing BC app"), imported.
#: ANGLER WORDS ONLY (review B17, 2026-10-08): the shipped field once ended with a builder
#: instruction ("Show this note at the top of the water …") that a page printed to anglers. How to
#: show it is in the field dictionary (`TIDAL_TEXT["guide"]`), never in the data.
TIDAL_GUIDE = TIDAL_NOTE

#: How sure we are that steelhead are present (user ruling 2026-10-01) — on a part, and rolled up on
#: the water.
#: What to show on a KNOWN part no steelhead rule applies to (`steelhead_rules: false`).
STEELHEAD_NO_RULES = ("Steelhead have been recorded here, but no steelhead rule applies: treat "
                      "any rainbow, however big, as a rainbow trout.")
STEELHEAD_TEXT = (
    "HOW SURE WE ARE THAT STEELHEAD ARE HERE — a PRESENCE INDICATOR for display. \"known\": "
    "steelhead are known to be here, for one of two reasons (`steelhead_source`). "
    "\"regulations\": the book names steelhead on this water — it is a STEELHEAD ROW's own water, "
    "or a rule of one binds it "
    "(a row any of whose rules or licensing records PRINTS steelhead: a steelhead quota, release "
    "or closure, 'Steelhead Stamp mandatory', 'Steelhead Stamp not required', 'not required "
    "unless fishing for steelhead' — or one flagged `anadromous_rainbow`; a Classified Water "
    "designation printing no steelhead is not one), by any line of that row — a STREAM; a LAKE only "
    "as the row's own water, its own row printing steelhead (Khartoum, Lois; NOT Tenas Lake, which "
    "the Atnarko/Bella Coola spring closure binds but whose own row prints no steelhead — user "
    "ruling 2026-10-06). Such a water carries the WHOLE PROVINCIAL STEELHEAD "
    "SET wherever it lies, lake or stream, and a lake also its region's wild-steelhead release. "
    "\"curated list\": the water is on the curated known-steelhead list "
    "(`data/curated/regulations/steelhead_waters.json`, built from fish observations). THE LIST "
    "BINDS NO RULE: no steelhead rule or stamp comes from it; it sets the presence indicator. In "
    "the steelhead regions (1, 2, 3, 5, 6), where the provincial steelhead rules already apply on "
    "every stream, it turns \"possible\" into \"known\"; elsewhere (Regions 4, 7A, 7B, 8 — the "
    "Okanagan River, the Fraser in Zone 7A) it marks steelhead as present on a water NO "
    "steelhead rule applies to: that part carries `steelhead_rules: false`, and there a rainbow "
    "of any size is a rainbow trout (user ruling 2026-10-03). Steelhead rules come only from the "
    "book. "
    "\"possible\": any other stream of Regions 1, 2, 3, 5 or 6; the steelhead rules apply, but "
    "steelhead may not be present. ABSENT: anywhere else. SHOW: on \"possible\", a quiet line "
    "with the steelhead rules — 'Steelhead rules apply here; steelhead may not be present in "
    "this water.' On \"known\" say steelhead are known to be here, and show the steelhead rules "
    "that apply (in the part's sets); on \"known\" with `steelhead_rules: false` show "
    "instead: '" + STEELHEAD_NO_RULES + "' Absent: show nothing about steelhead. "
    "A part says it for its sections; the water's own `steelhead` is \"known\" if any part is "
    "known, else \"possible\" if any part is (for a list or a search row). WHERE A RAINBOW OVER "
    "50 CM IS A STEELHEAD (p.80; the part's `anadromous_rainbow`): a KNOWN part of a STREAM (the "
    "water's `kind`: a slough or canal is a stream) WHERE STEELHEAD RULES APPLY — known by the "
    "book or by the list alike (the Cowichan River); never on a lake, a \"possible\" stream, or "
    "a part with `steelhead_rules: false` (user ruling 2026-10-03: 'if no steelhead rules exist, "
    "rainbow rules still apply to a steelhead').")

#: `guide.pdf_pages` — the one printed -> PDF page map (`pipeline.common.synopsis_pages`).
PDF_PAGES_TEXT = (
    "{printed page: PDF page} — an entry's `pages` are the numbers PRINTED in the synopsis's "
    "footers; the PDF's page index differs (printed = PDF - 2 up to printed 40, then four "
    "unnumbered centre pages, then PDF - 6). A link into the PDF (`fishing_synopsis.pdf#page=N`) "
    "takes the PDF page: look the printed page up here, never link the printed number. Read off "
    "the repo copy's own footers by the extractor's reader (a page with no footer of its own "
    "takes the next numbered page's offset); keys are strings (JSON)")


#: THE FILE, key by key — every top-level key (`dictionary_gaps` refuses one that is not here).
FILE_TEXT = {
    "about": "what the file is, the bundle it was read from (version, build, reach run and "
             "digest, `section_handles`), the counts, and corpus references that do not resolve",
    "guide": "how to read everything below — see `guide.contents`; `guide.pdf_pages` maps a "
             "printed page (an entry's `pages`) to the synopsis PDF's page. Ships in the GUIDE "
             "file (`ui-rules-guide.json`), with `field_dictionary` and `species`",
    "field_dictionary": "this: every key of the file and every field of its records, in words; "
                        "`encoding` says how the data file encodes them. Ships in the guide "
                        "file",
    "species": "the book's species list (p.80) under its headings, the groups and open subjects "
               "a rule may name, and the refused codes. Ships in the guide file",
    "licences": "the document register: doc_id -> {name, provincial}",
    "entries": "every synopsis row, keyed by entry_id — see `entry`",
    "rules": "every rule, keyed `entry_id::rule_id` — see `rule` (on the wire an array beside "
             "the parallel `rule_ids`: `encoding`)",
    "licensing": "every licensing record, keyed `entry_id#record_id` — see `licensing` (on the "
                 "wire an array beside `licensing_ids`)",
    "rulesets": "the interned rule sets sections carry, keyed by a set id local to this file — "
                "see `rulesets{} / licensing_sets{}` (on the wire an array indexed by set id, "
                "members as integer indexes into `rules`, the zone/province base interned in "
                "`bases`). A SET ID IS VALID ONLY WITHIN ONE BUNDLE DIGEST "
                "(`about.bundle.reach_digest` / `section_handles`): the next export renumbers "
                "its sets. To match a set across exports use its content key (`set_keys`)",
    "licensing_sets": "the interned licensing sets, the same way (ids local to one bundle "
                      "digest; content keys in `set_keys.licensing_sets`)",
    "set_keys": "{rulesets: {set id: key}, licensing_sets: {set id: key}} — each set's CONTENT "
                "KEY, a short hash of its members with their `via` (`encoding.set_keys` gives "
                "the exact recipe). Two exports' sets with the same key hold the same members "
                "reached the same way, whatever their ids; a key that changes means the set's "
                "membership changed. NOT SHIPPED: the decoder computes it",
    "waters": "every named water, keyed by its durable item_id — see `water`",
    "splits": "every cut a part's run can end at, by id — see `splits`. ONLY THE CUTS THIS "
              "FILE REFERS TO: every curated, gauge, area, border and region cut ships, but a "
              "LAKE EDGE ships only where a rule, a licensing record, an entry or a run names it "
              "(68 of the bundle's 21,636; format 2 — the count fell from 25,423 to 3,855). The "
              "unlisted lake edges still exist: in the atlas graph, in the bundle's `split` table, "
              "and as the ends of the tiles' stream sections (the tiles carry no cut layer)",
    "index": "rule ids by type and by family, licensing ids by kind — NOT SHIPPED: the "
             "decoder groups the records (`encoding.index`)",
}

#: A water, key by key (`waters[item_id]`).
#: THE CASING RULE every displayed name follows — one text for waters, entries and splits.
OFFICIAL_NAME_TEXT = (
    "ONLY where `name` was re-cased: the name exactly as its source spells it, in capitals "
    "('CLAYHURST ECOLOGICAL RESERVE', 'MITE LAKE', 'ENDAKO RIVER'). A displayed name never shouts: "
    "an all-capitals name is shown as a reader writes it ('Clayhurst Ecological Reserve') — small "
    "words lower after the first ('Dewdney and Glide Islands'), acronyms kept ('CNR', 'CFB', "
    "'CVWMA'), 'Mc' and an O'/D'/L' prefix kept ('McKenny', 'O'Rourke'), lower case after any "
    "other apostrophe ('Field's Lease'). A name that has any lower-case letter is shown as written")

WATER_TEXT = {
    "name": "the water's name, as a reader writes it (never all capitals — see `official_name`)",
    "official_name": OFFICIAL_NAME_TEXT,
    "kind": "stream | lake | wetland — THE WATER KIND, the registry's (`item.kind`), decided once "
            "by the atlas build and read by every regulation, the map and this file alike: a "
            "slough, a canal, a channel, a river's wide reach drawn as a polygon (Gravel Slough, "
            "the Vedder Canal, the Stellako's) IS A STREAM (user ruling 2026-10-03) — every \"in "
            "streams\" rule binds it, no \"in lakes\" rule does. The SHAPE a part is drawn as is "
            "its runs' (`polygon`, or stretches)",
    "sections": "how many sections the water has; its parts' `sections` sum to it. 0 on a lake "
                "cut into parts (`divided_into`): its parts own its water",
    "divided_into": "ONLY on a lake cut into PARTS: the item_ids of its parts (their `part_of` is "
                    "this id). The lake owns no section and has no `parts`; show it through these "
                    "(`placement.part_of`). Its `entries` are its parts' rows",
    "absorbed": "ONLY on a water that absorbed another item of the registry (a river's polygons, "
                "once items of their own, folded into it): the old item_ids, so a link saved "
                "under one still opens this water. No record names an absorbed id",
    "entries": "the synopsis rows that MATCH this water (their `matched` lists it) — not every "
               "row whose rules reach it: a zone, area or tributary walk reaches it through its "
               "parts' sets",
    "parts": "its sections grouped by the FIVE-TUPLE they carry together — (ruleset, "
             "licensing_set, province_except, anadromous_rainbow, steelhead); two parts may share "
             "a (ruleset, licensing_set) pair, so locate a section's part by all five — see "
             "`water.parts[]`",
    "outside_bc": "how many of its sections lie outside British Columbia (0 when none): they "
                  "carry no set; show 'outside B.C.' (`placement.outside_bc`)",
    "part_of": "ONLY on a lake PART: the item_id of the whole lake it was cut from "
               "(`placement.part_of`)",
    "steelhead": "ONLY where some part is \"known\" or \"possible\": \"known\" if any part is "
                 "known, else \"possible\" — the roll-up of `water.parts[].steelhead`, for a list "
                 "or a search row. A presence indicator: a water known only by the curated list "
                 "may carry no steelhead rule at all (`steelhead_rules`)",
    "steelhead_rules": "ONLY (false) on a KNOWN water none of whose parts any steelhead rule "
                       "applies to (the Okanagan River, Inkaneep and Vaseux creeks): show '"
                       + STEELHEAD_NO_RULES + "' — see `water.parts[].steelhead_rules`",
    "steelhead_source": "ONLY on a water with a KNOWN part: why it is known — \"regulations\" "
                        "(a steelhead row's rules bind it; those rows are `steelhead_rows`) "
                        "and/or \"curated list\" (the curated known-steelhead list names it, "
                        "`data/curated/regulations/steelhead_waters.json`: it sets the presence "
                        "indicator and binds no rule — outside the steelhead regions such a water "
                        "carries no steelhead rule, `steelhead_rules: false`). Absent on a "
                        "\"possible\" water",
    "steelhead_rows": "ONLY where `steelhead_source` holds \"regulations\": the entry ids of the "
                      "steelhead rows whose rules bind the water's known sections (its own row, "
                      "or, for a stream, a row whose line reaches it; a lake only by its own "
                      "row)",
    "tidal": "ONLY where the book calls the water tidal (Nitinat Lake): {sections, entry, guide} "
             "— see `water.tidal`",
}

#: `waters[item].tidal`, key by key.
TIDAL_TEXT = {
    "sections": "how many of the water's sections are tidal",
    "entry": "the row that says the water is tidal",
    "guide": "the ANGLER-FACING words (federal tidal regulations apply), angler text only. "
             "Show them at the top of the water, and never read its sections as 'open under "
             "the general rules'.",
}

#: A water's part, field by field.
WATER_PART_TEXT = {
    "ruleset": "the rule set its sections carry (a key of `rulesets`), or null: none",
    "licensing_set": "the licensing set its sections carry (a key of `licensing_sets`), or "
                     "null: none",
    "sections": "how many of the water's sections are in this part",
    "province_except": "present where the part lies in areas a province-wide requirement stops "
                       "at: the families of those areas — `national_parks` (for a record whose "
                       "extent names that kind), `tidal` (tidal water: EVERY province-wide "
                       "requirement stops there, whatever its record names)",
    "anadromous_rainbow": "present (true) where a rainbow over 50 cm is a steelhead: a KNOWN part "
                          "of a stream (`water.kind`: a slough or canal is one) where steelhead "
                          "rules apply — known by the book or by the curated list (user ruling "
                          "2026-10-03); never a lake, never \"possible\", never a part with "
                          "`steelhead_rules: false` (there rainbow rules speak for every rainbow)",
    "home_region": "present on a part with a section drawn across a region line: the region(s) "
                   "those straddling pieces take their zone rules from (a stream piece is held to "
                   "the region holding most of its length; a lake straddling a line is in both and "
                   "the most strict wins) — `section_home`, measured once by the atlas",
    "steelhead_rules": "ON EVERY PART WITH A RULE SET (absent only where `ruleset` is null): true "
                       "when the provincial steelhead set (the annual hatchery 10, the wild "
                       "release, the record duty) applies there, false when it does not — the "
                       "bundle's answer, never left to absence (DF4). On a KNOWN part, false is "
                       "the Okanagan River, Inkaneep and Vaseux creeks, the Fraser in Zone 7A, "
                       "known by the curated list: SHOW '" + STEELHEAD_NO_RULES + "' Every "
                       "\"possible\" part is true; an unmarked part's false means steelhead are "
                       "answered as rainbow trout there (RU-6) and says nothing to show",
    "steelhead": STEELHEAD_TEXT,
    "touches": TOUCHES_TEXT,
    "runs": "WHERE THE PART RUNS: its stretches, upstream to downstream — [{from, to, km_from, "
            "km_to}] (see `water.parts[].runs[]`, and `placement.runs`). A part of several "
            "stretches has several runs. A part that is a polygon (a lake, a wetland, or a "
            "stream's polygons its main stem does not pass through) is one run with no ends; a "
            "river's wide reach drawn as a polygon ON its stem runs with the line through it",
}

#: One run of a part, field by field.
RUN_TEXT = {
    "from": "the run's UPSTREAM end: an end token (`placement.runs.ends`) — a cut-point id (a key "
            "of `splits`), or a named natural end. null on a lake or wetland (a polygon has no "
            "ends)",
    "to": "the run's DOWNSTREAM end, the same way. Water flows from `from` to `to`",
    "km_from": "km from the water's MOUTH, along its main stem, at `from` (the larger number). "
               "null on a lake; on a `branch`, the point where the branch rejoins the main stem, "
               "or null when it never does",
    "km_to": "km from the mouth at `to` (km_to <= km_from). A run covers km_to..km_from of the "
             "main stem",
    "branch": "present (true) on a run that lies OFF the water's main stem — a side channel or "
              "braid of the same water whose sets differ from the stem beside it. Its ends are its "
              "own (its `source` and `mouth` are where it leaves and rejoins), and both km are the "
              "point it rejoins the stem",
    "polygon": "ONLY on a polygon run (a lake, a wetland, a stream's polygons off its stem — a "
               "slough drawn only as polygons): `whole` (the run is the whole polygon), or — on a "
               "lake PART (`part_of`) — the part's own name ('Williston Lake — Nation Arm')",
}

#: The end tokens a run's `from` / `to` take. One vocabulary with `spans.end_token`.
END_TEXT = {
    "<split id>": "a cut the pipeline made at a curated split or a gauge station, by the SAME id "
                  "the rule extents use (`thompson_river__cnr_bridge`); its name and place are in "
                  "`splits`",
    "area:<name>": "an area boundary that is not a region (a park, an ecological reserve, a group "
                   "of management units) — also a key of `splits`. A run with this at BOTH ends "
                   "lies inside the area. The token is an ID and keeps its source's spelling "
                   "('area:CLAYHURST ECOLOGICAL RESERVE'); show its `splits` name ('Clayhurst "
                   "Ecological Reserve boundary'), never the token",
    "region_line:<region>": "a region boundary (`region_line:6`); a key of `splits`",
    "bc_border": "the provincial (or national) border: the water runs on outside B.C.",
    "lake_inlet:<item_id>": "the run ends where it flows INTO that lake (a key of `waters`; "
                            "`lake_inlet` alone: a lake that is not a named water)",
    "lake_outlet:<item_id>": "the run begins where it flows OUT of that lake (`lake_outlet` "
                             "alone: unnamed)",
    "confluence:<item_id>": "a cut the atlas made where that tributary (a key of `waters`) flows "
                            "in, or the stretch meets that STREAM's polygon (a tributary's mouth "
                            "at a river's wide reach), or — as a lower end — the water meets that "
                            "other stream there and the two form the next water (the Sproat and "
                            "the Stamp form the Somass). Say it '<name> confluence' (that water's "
                            "`name`)",
    "polygon": "the stretch meets the water's OWN polygon — a river drawn as a wide polygon, a "
               "slough's: no cut, the same water goes on (its polygon is the next stretch, or the "
               "same part); a cut aliased onto that edge keeps its own id instead",
    "continues:<item_id>": "the water's natural lower end is NOT a mouth: it goes on downstream as "
                           "that other named stream (a key of `waters`), which begins there — the "
                           "same channel under another name, nothing else joining (the Chilliwack "
                           "becomes the Vedder at Vedder Crossing). Name the place by "
                           "where that water begins (its top run's `from`), never 'mouth'",
    "mouth": "the water's own downstream end — into the sea, another river or a lake it ends in "
             "— with no cut",
    "source": "the water's own upstream end with no cut — its source, or where it takes its "
              "name (the Thompson at Kamloops)",
}

#: One cut in `splits`, field by field.
SPLIT_TEXT = {
    "name": "a short human name for the place, UNIQUE among the places of each water it stands on: "
            "'boundary signs', 'Thompson River confluence', 'Garibaldi Park boundary' — the curated "
            "split's own label. A CONFLUENCE is named for the OTHER water that joins there (on the "
            "receiving river the tributary, on the tributary the receiving river), never the water "
            "it stands on; an offset cut says its offset from the curated split ('5 km upstream "
            "of the Halfway River confluence'); a length cut where a lake drains straight in is "
            "'<lake> outlet'. A length cut the atlas made where a SIDE CHANNEL of "
            "the same water flows back in is 'side channel', and one at a tributary with no name "
            "'unnamed tributary' — both placed by the nearest named water joining the river ('side "
            "channel above Elbow Creek'). A label that repeats at two places of one water is "
            "placed the same way ('CNR bridge below Deadman River' / 'CNR bridge above Bonaparte "
            "River'), then by its distance from that landmark, then 'lower' / 'upper'. An area is "
            "never all capitals ('Clayhurst Ecological Reserve boundary', see `official_name`)",
    "official_name": "ONLY on an area boundary whose shown name differs from its source: the "
                     "area's name exactly as its source layer spells it — re-cased ('CLAYHURST "
                     "ECOLOGICAL RESERVE'), carrying an OpenStreetMap id ('CFB Comox "
                     "[12332677]'), or cut off at 50 characters ('… ECOLOGICAL RESER'). The id "
                     "keeps that spelling (`area:CLAYHURST ECOLOGICAL RESERVE`) — ids never "
                     "change, and extents bind by them; show `name`, never the id",
    "same_place_as": "ONLY on a cut at the SAME place as another cut of the same water (one "
                     "curated cut authored under two waters' lists — the Thompson River confluence "
                     "on the Fraser, as `fraser_river__…` and `thompson_river__…`): that other "
                     "cut's id. The two share a name because they are one place; treat them as "
                     "one",
    "kind": "how the cut was placed: point | line | confluence | lake (a curated cut anchored at "
            "a lake's edge, with an offset) | gauge (a hydrometric station) | area (an area's "
            "boundary) | region_line | lake_edge (a lake's EDGE on a river: the registry's "
            "bindable lake boundary, the id a rule's extents bind by when a cut lands in the lake "
            "— named for the lake, it stands at the lake's inlet AND outlet, so `at` lists both, "
            "and a river entering one lake twice has two such ids with one name) | border (the "
            "provincial border on this water: the runs' `bc_border`)",
    "water_id": "the named water whose main stem it stands on, when it stands at ONE place on "
                "one; null otherwise (an area boundary, or several places — see `at`)",
    "km": "km from that water's MOUTH along its main stem; null with `water_id`",
    "at": "ONLY on a cut at several places on named waters' main stems: every [{water_id, km}]",
    "lake_id": "ONLY on a lake's edge (`kind: lake_edge`): the lake's item_id (a key of `waters`) — "
               "the water the runs' `lake_inlet:<lake_id>` / `lake_outlet:<lake_id>` ends name. "
               "Absent when the waterbody is not a named water",
}

#: A designation's classified period (`licensing[*].period`, designations only).
#: `licensing[].stamp_waiver` — see `add_stamp_waivers`.
STAMP_WAIVER_TEXT = {
    "outright": "true: '(Steelhead Stamp not required)' — NO steelhead stamp on the designation's "
                "sections while it is in force. false: 'not required unless fishing for "
                "steelhead' — the classified-water stamp only",
    "lifts": "the requirements (`entry_id#record_id`, each with `waived_where`) that do not hold "
             "there and then: the provincial Conservation Surcharge Stamp to fish for steelhead. "
             "[] for a conditional waiver",
    "verbatim": "the waiver as the row prints it",
    "says": "the sentence to show: the stamp is not required on <the designation's reach> "
            "<its dates>, per the row; the Classified Waters Licence and the steelhead rules "
            "still apply",
}

PERIOD_TEXT = {
    "kind": "when_open: the water is Classified whenever it is open (the book prints 'Class II "
            "water when open' or 'when/where open' — no dates, by design: licensing is consulted "
            "only while the water is open) | all_year: Classified every day ('Class I water all "
            "year') | dates: only on `dates`",
    "dates": "ONLY with kind `dates`: the designation's own `fields.when.dates`, as printed",
    "says": "the sentence to show: 'Classified (Class II) whenever this water is open', 'Class I "
            "all year', or 'Classified (Class II) Sep 1-Apr 30'",
}

TYPE_TEXT = {
    "retention_limit": "How many of a fish you may keep, on which clock, of which sizes. A "
                       "quota, a catch-and-release, a size limit and a closure to a species are "
                       "all this one type; `take`, `may_target` and `lengths` tell them apart.",
    "stop_fishing_after_quota": "Once you have caught and kept your quota of this fish (on the "
                                "clock in `period` — the book: 'your DAILY quota of hatchery "
                                "steelhead') you must stop fishing THAT WATER for the rest of "
                                "that period ('for the remainder of that day'), whatever you "
                                "fish for. It counts nothing itself.",
    "bait_restriction": "What may be on the hook — bait bans and the bait that is allowed "
                        "anyway. Carries `gear` clauses on the `bait` slot.",
    "tackle_restriction": "The rig: hooks, points, barbs, lures, flies, lines, weights. Carries "
                          "`gear` clauses.",
    "method_rule": "Whether a way of fishing is allowed at all — angling, ice fishing, spear "
                   "fishing, set lining, crayfish trapping, netting, snagging. Carries `gear` "
                   "clauses on the `method` slot or a spec slot, or `conduct`.",
    "vessel_rule": "Boats: whether they are allowed, under what propulsion, at what speed, "
                   "towing. Read `aspect` first.",
    "navigation_duty": "What a boat must do for other traffic.",
    "angler_closure": "The water is closed to ONE KIND of angler (`closed_to`), on the days in "
                      "`when`. A closure, never a quota; everyone else is unaffected.",
    "handling_rule": "What you must do with a fish or your gear — release immediately, do not "
                     "waste it. Carries `conduct` and/or `gear`.",
    "hazard": "A warning about the place. Governs nothing.",
    "advisory": "Information the book prints. Governs nothing, and must never be read as a "
                "limit.",
    "program_membership": "The water belongs to a named programme. Governs nothing.",
    "facility": "What is there — a launch, an accessible pier. Governs nothing.",
}

TYPE_CAN_CLOSE = {
    "retention_limit": "Yes — `take: 0` with `may_target: false` and no `while`, and not "
                       "`standing`, means you may not fish for those species at all. On "
                       "ALL_GAME_FISH that shuts the water for the dates in `when`.",
    "angler_closure": "For the anglers in `closed_to` only.",
    "method_rule": "Closes a method (a `ban` on the `method` slot), never the water.",
}

FAMILY_TEXT = {
    "retention": "what you may keep",
    "gear_and_method": "how you may fish",
    "vessel": "what your boat may do",
    "access": "who may fish here at all",
    "conduct": "what you must do",
    "information": "what the book tells you; governs nothing",
}

SLOT_TEXT = {
    "bait": "what is on the hook: any_bait, fin_fish, roe, invertebrate, dead_fin_fish, …",
    "lure": "the terminal object: artificial_fly, artificial_lure. `only: [artificial_fly]` is "
            "'artificial fly only' — NOT the same law as fly fishing only (see `definitions`)",
    "method": "how you fish: angling, fly_fishing, ice_fishing, set_lining, spear_fishing, … "
              "`only: [fly_fishing]` is 'fly fishing only' — NOT the same law as artificial fly "
              "only (see `definitions`)",
    "barb": "barbed | barbless — a barbless-hook rule is `only: [barbless]`",
    "set_lining": "how a set line must be built or marked",
    "crayfish_trapping": "how a crayfish trap must be built",
    "downrigger": "how a downrigger must be rigged (e.g. quick-release)",
    "light": "how a light must be used (e.g. submerged, attached to the line)",
    "ice_hut": "what an ice hut must be or carry (binds `while: [ice_fishing]`)",
    "hooks_per_line": "number of hooks on one line",
    "points_per_hook": "number of points on one hook — a single hook is `max: 1`",
    "lines_per_angler": "number of lines one angler may fish",
    "flies_per_line": "number of flies on one line",
    "terminal_attachments_per_line": "hooks, lures and flies together, on one line",
    "hook_gap_mm": "hook gap in millimetres",
    "weight_per_line_kg": "weight on one line, in kilograms",
    "bait_possession_kg": "bait you may possess, in kilograms",
    "light_to_hook_mm": "distance from a light to the hook, in millimetres",
}

#: The book's two fly-only laws, verbatim from its Definitions page, and how each is encoded. They
#: are DIFFERENT laws — a float or a sinker is lawful under one and not the other — and the corpus
#: encodes each where the book prints it; a reader must never merge them.
FLY_DEFINITIONS = {
    "lure": {
        "encoded_as": "{slot: lure, only: [artificial_fly]}",
        "book": "artificial fly: … Where gear is restricted to artificial flies, floats and "
                "sinkers may be attached to the line.",
        "means": "only artificial flies as the terminal object; floats and sinkers are allowed",
        "differs_from": "method",
    },
    "method": {
        "encoded_as": "{slot: method, only: [fly_fishing]}",
        "book": "fly fishing: angling with a line to which only an artificial fly is attached "
                "(floats, sinkers, or attracting devices may not be attached to the line when "
                "fishing is restricted to \"fly fishing only\").",
        "means": "fly fishing only: nothing but the fly on the line — no float, sinker or "
                 "attracting device",
        "differs_from": "lure",
    },
}

CLAUSE_TEXT = {
    "slot": "what the clause constrains — see `slots`",
    "of": "which members of the slot it speaks about; absent = all of them",
    "allow": "these are permitted; says nothing about the rest of the slot",
    "only": "a whitelist that closes the slot: nothing else is permitted",
    "ban": "these are prohibited; a total ban names the whole-slot member (any_bait)",
    "except": "members carved out of a `ban`",
    "members": "a choice of ONE from several kinds, qualifying a count "
               "('one hook, one lure or one fly')",
    "max": "a ceiling, in the unit the slot's name carries",
    "min": "a floor, in the unit the slot's name carries",
    "unlimited": "no ceiling on the count, said outright",
    "when": "the condition under which this clause applies (a `GearWhen`); absent = always",
    "requires": "how the thing itself must be built or carried (a `GearSpec`)",
    "must_be": "on a spec slot: the states the thing must be in",
    "unless": "conditions (each a `GearWhen`) that lift this clause",
}

GEAR_WHEN_TEXT = {
    "water": "stream | lake",
    "method": "while fishing by this method",
    "targeting": "while fishing FOR these species",
    "angler": "alone_in_boat | in_boat | in_powered_boat | from_shore — 'No angling from "
              "boats' is a method ban on angling `when: {angler: in_boat}`",
    "gear_in_use": "while this gear is in use",
    "note": "a condition the closed vocabulary cannot say; the rule then carries a "
            "review_reason",
}

GEAR_SPEC_TEXT = {
    "attached_to": "what it must be attached to (fishing_line)",
    "attachment": "how it is attached (quick_release)",
    "within_m_of_hook": "how close to the hook, in metres",
    "opening_shape": "the shape of a trap opening (circular)",
    "note": "a property the closed vocabulary cannot say; costs a review_reason",
}

#: The model's rule fields that a reader of this file meets, in words.
RULE_FIELD_TEXT = {
    "obligation": "should: the book gives this as ADVICE, not law. Absent = must (law) — "
                  "the default, so it is not repeated on every rule.",
    "species": "species codes the rule is about, as the book wrote them (groups included — "
               "expand with `species.groups`)",
    "species_except": "species carved out of `species`",
    "closed_to": "angler_closure only: WHO the water is closed to (a `Who`)",
    "closed_to_except": "angler_closure only: the anglers INSIDE `closed_to` the water stays "
                        "open to, each a `Who` — a Youth/Disabled Accompanied Water is closed to "
                        "anglers 16 and over except disabled B.C. residents and the companions of "
                        "an authorized angler",
    "gear": "ordered list of gear clauses — see `gear`",
    "derived_from": "the rule id (same entry) whose sentence implies this one",
    "condition_of": "the rule id (same entry) this rule is the proviso of",
    "while": "the means of fishing during which the rule binds; absent = any",
    "caught": "how the FISH was caught, for a rule that binds only a fish caught that way "
              "(`foul_hooked`: snagged — hooked anywhere but the mouth, on purpose or by "
              "accident, user ruling Q38); absent = any fish. A condition on the fish, never the "
              "angler's means: a release with `caught` is never an outright release or a "
              "closure, and never decides a quota for a fish hooked in the mouth",
    "conduct": "act tokens: what you must or must not do — see `gear.conduct`",
    "when": "when the rule binds — see `time`; absent = all year",
    "take": "how many you may keep",
    "unlimited": "there is no number",
    "may_target": "false = you may not fish for it; true = fish for it and release",
    "period": "daily | possession | annual | monthly — the clock a quota runs on. Carried by "
              "every retention_limit (the clock its own number runs on) and every "
              "stop_fishing_after_quota (the clock of the quota whose filling stops you: the book "
              "prints 'your daily quota … for the remainder of that day', pp.21/49/51), and by "
              "nothing else.",
    "per_daily": "a possession limit as a multiple of the daily one",
    "within": "the rule id (same entry) this is a clause of; it counts inside its parent",
    "lengths": "ordered length ranges, each with its own take — see `sizes`",
    "record_retention": "keeping this fish must be recorded on your licence",
    "water": "stream | lake — the rule binds only on that kind of water",
    "origin": "wild | hatchery; absent = both",
    "when_targeting": "the species you are fishing FOR (bait and tackle rules)",
    "aspect": "vessel_rule: propulsion | speed | towing",
    "level": "vessel_rule propulsion: none | unpowered | electric_only | power_capped",
    "max_power_kw": "vessel_rule: the motor cap, kW",
    "max_kmh": "vessel_rule: the speed cap, km/h",
    "includes_tributaries": "true/false: the rule reaches (or not) the water's tributaries; "
                            "absent = inherit the entry's",
    "tributaries_only": "the tributaries, without the named water itself",
    "extents": "where on the water (or in which areas) the rule applies, as the reach builder "
               "reads it. A rule with none of its own is not given its entry's. What the rule "
               "STATES, not where it holds: read `binds` for that. Each extent, key by key: "
               "`rule.fields.extents[]`.",
    "exempts": "what the rule lifts — see `exempts`",
    "standing": "true: holds everywhere at places no dataset can draw — see `standing`",
    "authority": "superior: a federal or park authority, above the provincial ladder",
    "notice": "the DFO fishery notice the rule was published in",
    "suspended_while": "a rule id (same entry): this rule is dormant while that one binds",
    "extent_text": "the book's words for a place nothing could draw",
    "undrawn_part": "the book's words for the PART of the bound water this rule holds in, which "
                    "nothing draws. The rule is placed on the whole water it is in; show it there "
                    "as a note ('in <part>') and never colour the water by it — see `binds`",
    "life_stage": "adult: the rule holds for fish of that LIFE STAGE only, as the sentence prints "
                  "it — 'record your retention of adult chinook salmon' (p.7). The book defines "
                  "'adult' for chinook only (p.77: over 50 cm nose to fork in most non-tidal "
                  "waters, over 62 cm in some rivers), so the stage is its word, never a length; "
                  "the label's `what` says it ('Adult chinook')",
    "side": "north | south | east | west: the rule holds on that HALF OF THE CHANNEL only, "
            "lengthwise ('No Fishing on the west half of river …', Kitimat River). It is placed "
            "on the stretch its extents draw, but an angler on the other half follows the "
            "water's other rules there: the reference answers it `beside` them (it displaces "
            "nothing), and its `parts.side` says which half — show it; never colour or close the "
            "whole width by it (`gotchas.one_side_of_channel`)",
}

#: THE PARTS OF A LINE, in words. Checked against `catalogue.LABEL_PARTS` and
#: `catalogue.LICENSING_PARTS` (see `_registries`), so a part the model gains is refused until it is
#: explained here.
PART_TEXT = {
    "what": "the rule itself — its verdict and subject: 'No fishing for bull trout in streams', "
            "'Rainbow trout — 2 per day', 'Bait ban', 'Speed restriction (10 km/h)'. ABSENT when "
            "the rule has no structured content (an advisory, a hazard, a bare exemption): show "
            "the verbatim instead, marked as the book's text",
    "size": "the length bound: 'none under 30 cm' (a bound — write it in parentheses) or 'over "
            "50 cm' (a size class — attach it: 'release all over 50 cm')",
    "conditions": "which fish or which fishing: 'wild only', 'from streams', 'when fishing for "
                  "salmon', 'while set lining', 'in possession'",
    "when": "dates, weekdays, hours, and any season nobody could read ('as printed: …' — "
            "uncertain, never all year)",
    "where": "the place in the book's words, or named from what the extents draw",
    "side": "the half of the channel the rule holds on (`fields.side`): 'west half of the "
            "channel only — on the east half, this water's other regulations apply'. Show it "
            "wherever the rule is shown",
    "in_part": "the undrawn part the rule holds in (`fields.undrawn_part`): a note, never a "
               "colour — see `binds`",
    "lifts": "what the rule exempts from",
    "duty": "what you must do: 'record your retention on your licence immediately'",
    "suspended": "'not while “<the rule it sleeps under>” is in force'",
    "notice": "the DFO fishery notice it was published in",
}
LICENSING_PART_TEXT = {
    "who": "the anglers it is about",
    "what": "the fact: a designation's class, 'Not a Classified Water', the document sold, "
            "what an alternative accepts",
    "need": "what must be held (any one of the ways joined by 'or'); on an exemption, "
            "'no <documents>'",
    "must": "a duty to do",
    "way": "a way to satisfy it that is not a document ('be accompanied by …')",
    "doing": "the activity that triggers it ('to fish', 'to keep steelhead')",
    "where": "on which water, or during which designation's period",
    "when": "the dates, days and hours it holds",
    "unit": "the licence unit(s), as the page names them",
    "stamp": "the classified-water Steelhead Stamp here: its period, or its waiver (an outright "
             "waiver: no steelhead stamp of any kind)",
    "waived": "where a requirement does not hold: on a Classified Water whose row waives the "
              "Steelhead Stamp outright, while that designation is in force",
    "terms": "how a document is sold",
    "instead": "what an alternative stands in for",
    "except": "anglers taken out of `who`",
    "suspended": "not in force while the named closure applies",
    "note": "a superior authority's note ('provincial licences are not valid here')",
    "records": "ONLY on the 'carry your paper licence' duty (`doing.act: retaining_recorded`): "
               "the fish whose retention must be recorded on the licence, in words, DERIVED from "
               "every rule carrying `record_retention: true` (catalogue.recorded_fish) — "
               "'hatchery steelhead, "
               "adult chinook, …'. The rules themselves are the record's `records`",
    "in_part": "the part of the water a requirement holds in, which nothing draws "
               "(`fields.undrawn_part`) — shown as a place not yet mapped, never required of the "
               "whole water",
}

#: Keys on an exported rule record outside `fields`.
RECORD_TEXT = {
    "id": "`entry_id::rule_id` — a rule id is unique only within its entry",
    "entry_id": "the synopsis row the rule was read from",
    "rule_id": "the rule's id inside its entry",
    "type": "one of `rule_types`",
    "family": "the type's family — see `families`",
    "dimension": "the second half of the competition key, (type, dimension) — see `ladder`",
    "label": "a PREVIEW: `parts` composed by the model's one composer (catalogue.compose). "
             "Never authored. A reader composes its own line from `parts` — see `labels`",
    "parts": "the rule's line as parts, each generated from its fields — see `labels`",
    "verbatim": "the sentence from the printed book, quoted",
    "binds": "where the rule holds: " + " | ".join(BINDS_TEXT) + " — see `placement.binds`",
    "not_yet_mapped": "ONLY on a rule that holds in a part nothing draws (`binds: "
                      "sections_in_part`): {display: 'prominent', part: the book's words for the "
                      "part, identified: false when those words name no place ('on parts', "
                      "'various locations' — the regulations never say which parts), map?: ONLY "
                      "where the book prints a map of the part {name: 'map A' | null, page: the "
                      "PRINTED page, cite: 'map A on p.28'} (T100: Shuswap's maps A/B/C, Mahood, "
                      "Canim, François), says: the sentence to show — the AREA CAVEAT, citing the "
                      "map when there is one (the answers' `rules[].applies` carries it, and "
                      "`rules[].book` the map's page)}. Show it AT THE TOP of the water, marked "
                      "as a place not yet mapped — never as the water's rule. It never decides "
                      "open or closed, never displaces or lifts anything — see "
                      "`placement.not_yet_mapped`",
    "fields": "the rule's own fields, as the bundle ships them",
    "provenance": "who wrote it and what it binds to — see below",
    "recorded_by": "ONLY on an annual quota (`period: annual`) whose record duty the book "
                   "prints: the `entry_id::rule_id` of the rule carrying `record_retention: true` "
                   "(a `retention_limit`; there is no `record_retention` type). Show its "
                   "text once, under the quota; never generate a 'record' line of your own — "
                   "see `retention.record_duty`",
    "records_for": "ONLY on a rule carrying `record_retention: true` that is an annual quota's "
                   "record duty: "
                   "the quotas (`entry_id::rule_id`) it is shown under — see "
                   "`retention.record_duty`",
}

PROVENANCE_TEXT = {
    "entry_name": "the entry's display name",
    "region": "the book region its entry id names, lower case (`z7a:…` -> \"7a\", `r6:…` -> \"6\"; null for the "
              "province's table): what a level tag says (\"Region 7A rule\"). Parsed once, by the pipeline "
              "(`pipeline.common.entry_id.region_of`) — never read off the id again",
    "authority": "who wrote it: superior | province | region",
    "binds_to": "what it binds to: region (the region's standing table), area, water",
    "rank": "the ladder position derived from (authority, binds_to); smaller speaks first",
    "who": "the two axes in words",
    "scope": "the bundle's own `scope` column: section | area — whether the rule was written "
             "for a water or for an area",
    "uncertain": "the reach builder could not place it; it binds no section",
    "why": "the reach builder's reason, when uncertain",
}

LICENSING_RECORD_TEXT = {
    "id": "`entry_id#record_id`",
    "entry_id": "the synopsis row the record was read from",
    "record_id": "its id inside the entry",
    "kind": "one of `licensing.kinds`",
    "label": "a PREVIEW: `parts` composed by catalogue.compose_licensing; never authored",
    "parts": "the record's line as parts, each generated from its fields — see `labels`",
    "verbatim": "the sentence from the printed book",
    "fields": "the record's own fields, as the bundle ships them",
    "placement": "sections | province | on_designation | unresolved | not_placed",
    "period": "ONLY on a designation: its classified period, said outright — {kind: when_open | "
              "all_year | dates, dates?, says} (see `licensing.period`). Derived from the record's "
              "own verbatim and `when`; never absent on a designation",
    "provenance": "entry_name, and for an unresolved record `uncertain` and `why`",
    "records": "ONLY on the 'carry your paper licence' duty (`fields.doing.act: "
               "retaining_recorded`): every rule carrying `record_retention: true` "
               "(`entry_id::rule_id`) — the "
               "fish you must record on your licence, and so keep only with a PAPER licence on "
               "you (p.6). Derived from the rules, never typed; `parts.records` says them in "
               "words",
    "not_yet_mapped": "ONLY on a requirement that holds in a part nothing draws "
                      "(`fields.undrawn_part`; the Creston Valley WMA permit on the south end of "
                      "Kootenay Lake's Main Body): {display, part, identified, map?, says}, as on a "
                      "rule. "
                      "Show it on the water as a place not yet mapped; it is NEVER required of "
                      "the whole water (`read.requirements_in_force` lists it under "
                      "`not_yet_mapped`, never `holds`)",
    "stamp_waiver": "ONLY on a designation that waives the Steelhead Stamp: {outright, lifts, "
                    "verbatim, says} (see `licensing.stamp_waiver`). `outright` "
                    "('(Steelhead Stamp not required)'): NO steelhead stamp on its sections while "
                    "it is in force — the classified-water stamp and every requirement in "
                    "`lifts` (the provincial stamp to fish for steelhead). Otherwise ('unless "
                    "fishing for steelhead') only the classified-water stamp; `lifts` is []. "
                    "Neither lifts the Classified Waters Licence or a steelhead rule",
}

LICENSING_KIND_TEXT = {
    "designation": "A FACT about a water: while `when` holds, the bound sections are a "
                   "Classified Water of class `classified` (I or II), in licence unit `unit`. "
                   "It obliges nothing itself; requirements fire on it. It may carry the "
                   "classified-water steelhead stamp period (`steelhead_stamp_during`) or its "
                   "waiver (`steelhead_stamp_waived`), and sleep while a closure rule binds "
                   "(`suspended_while`).",
    "not_classified": "An asserted ABSENCE: this part is NOT a Classified Water. Where a "
                      "designation also reaches it, the designation's binding there is "
                      "`contested` and the reader must say 'check'.",
    "requirement": "An OBLIGATION: anglers in `who` (minus `who_except`), `doing` this, where "
                   "and when it binds, must satisfy ANY ONE of `satisfied_by`, or do the "
                   "`conduct`. `on` ties it to a designation's period.",
    "licence_terms": "How a document is SOLD — per day or per licence year, day limits, "
                     "draws, fees. Never placed: attach it to the obligation whose document, "
                     "who, unit and class it names.",
    "exemption": "Anglers in `who` are released from `documents` AND from every duty whose "
                 "`presumes` are all among them ('produce your angling licence' means nothing to "
                 "an angler who need not hold one). Never placed.",
    "alternative": "A place where another document ALSO satisfies a requirement "
                   "(`alternative_to`). It only ever adds a path.",
}

LICENSING_FIELD_TEXT = {
    "classified": "I | II",
    "unit": "the licence unit a non-resident's per-day licence names",
    "unit_name": "the unit in words",
    "when": "when the record holds (a `When`); absent = all year",
    "extents": "where it applies, as the reach builder reads it — each extent key by key in "
               "`rule.fields.extents[]`",
    "includes_tributaries": "true/false; absent = inherit the entry's",
    "tributaries_only": "the tributaries without the named water",
    "tributary_excludes": "waters the tributary walk must not enter — each an extent "
                          "(`rule.fields.extents[]`; `walk_past` only here)",
    "steelhead_stamp_during": "{when, verbatim}: the classified-water steelhead stamp runs here "
                              "then, whatever you fish for",
    "steelhead_stamp_waived": "{verbatim}: the classified-water stamp is not required here; "
                              "with no 'unless' (an OUTRIGHT waiver, '(Steelhead Stamp not "
                              "required)') no steelhead stamp at all — see the record's "
                              "`stamp_waiver`",
    "waived_where": "steelhead_stamp_waived: this requirement does NOT hold on a section, on a "
                    "day, where a designation bound there is in force and waives the Steelhead "
                    "Stamp outright (its `stamp_waiver.outright`; Chilko upstream of Brittany "
                    "Creek Jun 11-Oct 31). Outside those dates or sections it holds as written",
    "suspended_while": "[{rule_id, verbatim}]: dormant while that closure rule (same entry) "
                       "binds",
    "review_reason": "what a curator still has to settle",
    "satisfied_by": "the ways to satisfy it — ANY ONE path (see `paths`)",
    "conduct": "act tokens the requirement obliges (see `gear.conduct`)",
    "who": "which anglers (a `Who`); absent = every angler",
    "who_except": "anglers taken out of `who`",
    "doing": "what the angler is doing that triggers it (a `Doing`)",
    "water": "stream | lake",
    "on": "classified_period | steelhead_period: holds wherever a designation's period (or its "
          "stamp period) is in force",
    "authority": "superior: a federal or park authority; displaces every provincial obligation",
    "restates": "{entry_id, id}: the provincial record this row's own words repeat",
    "document": "the document sold (see `licences`)",
    "units": "the licence units the terms are about; absent = every unit",
    "sold": "per_licence_year | per_day",
    "covers": "every_unit | one_unit",
    "max_consecutive_days": "at most this many days in a row",
    "max_days_per_licence_year": "at most this many days in a licence year",
    "max_per_licence_year": "at most this many of the document in a licence year",
    "max_units_per_licence_year": "at most this many licence units in a licence year",
    "unlimited_days": "no day limit, said outright",
    "allocation": "open | booking | draw",
    "needs": "what a buyer must supply (angling_guide_number)",
    "fee_cad": "the fee in dollars (one fee: the disabled resident's reduced fee, p.6)",
    "name": "the licence class as the fee table prints it (p.5): 'Annual Angling Licence', "
            "'One Day Angling Licence', 'Annual Licence for Age 65 Plus', 'Steelhead', 'Class I "
            "Waters Licence' — see `licensing.classes`",
    "valid_days": "a short-term licence's fixed run of consecutive days: 1 (One Day) or 8 "
                  "(Eight Day, 'covering 8 consecutive days'); an annual one says `sold: "
                  "per_licence_year` instead (April 1 to March 31)",
    "fees_cad": "{residency: dollars} — the fee table's price for each residency that may buy "
                "the class (p.5); a residency left out is one the table marks ★ 'Not "
                "available'. As printed for 2025-2027, taxes not included; fees change from year "
                "to year — see `licensing.classes.fees`",
    "undrawn_part": "ONLY on a requirement: the part of its water it holds in, in the book's "
                    "words, which nothing draws — see the record's `not_yet_mapped`",
    "documents": "the documents released",
    "presumes": "a conduct duty ABOUT these documents ('produce your angling licence'): it binds "
                "only an angler who must hold them, so an exemption releasing all of them "
                "releases the duty too",
    "alternative_to": "{entry_id, id}: the requirement this adds a path to",
}

DOING_TEXT = {
    "fishing": "any sport fishing at all",
    "targeting": "fishing FOR `species`",
    "retaining": "KEEPING `species` (of `lengths`, when given — the lengths name WHICH fish)",
    "retaining_recorded": "keeping a fish whose retention must be recorded on the licence "
                          "(which fish: rules with `record_retention`)",
    "guiding": "acting as an angling guide",
}

WHEN_TEXT = {
    "dates": "[{from_month, from_day, to_month, to_day}] — calendar ranges, no year, both ends "
             "inclusive; a range whose end is before its start wraps the year end",
    "hours": "{start, end}, each {at: 'HH:MM'} or {solar: sunrise|sunset, offset_min} "
             "(negative = before); wraps midnight the same way",
    "weekdays": "the days of the week it holds on; empty = every day",
    "unparsed": "a printed season nobody could read, verbatim. The rule is UNCERTAIN in time — "
                "never read it as all year",
}

#: One resolved lift, as the bundle ships it (`pipeline.deliver.bundle.rules.LIFT_KEYS`).
LIFT_TEXT = {
    "entry_id": "the entry of the rule lifted — resolved when the bundle was built, always present",
    "rule_id": "the rule lifted, in `entry_id`. One item per lifted rule: a zone default named by "
               "slug is resolved to each of its rules the lift reaches",
    "note": "the book's words, often the only statement of WHERE the lift reaches",
    "species": "the lift holds only for these fish (leaf codes) — the lifter names fewer than the "
               "lifted rule does, so the rule still binds every other species. Absent = every "
               "fish the lifted rule names",
    "when_targeting": "the lift holds only when fishing FOR these. The angler is unknown, so the "
                      "lifted rule stays and the lift is a condition on it",
    "while": "the lift holds only while doing these; the lifted rule binds everyone else",
    "when": "the lift holds only on these days/hours (the lifter's own `when`, shape as in "
            "`time`); on every other day the lifted rule binds. Absent = the lifter is in force "
            "every day the lifted rule is. A lift is never in force while its lifter is not",
    "origin": "the lift holds only for fish of this origin (hatchery | wild): the lifter keeps "
              "only those. Which a fish is shows only once it is caught, so the lifted rule stays "
              "and the lift is a condition on it (Kitimat River's 'Hatchery steelhead … daily "
              "quota = 2' could never reopen a closure for WILD steelhead; Kitimat's 'Hatchery "
              "rainbow trout … daily quota = 5, all year' lifts Region 6's stream release for "
              "hatchery rainbow only — a wild rainbow is still released Nov 1-June 30). "
              "A printed lift carries it as much as a derived one",
    "lengths": "[{min_cm, max_cm}] — the lift holds only for fish of these sizes (the sizes the "
               "lifter keeps). The lifted rule stays and the lift is a condition on it. A band "
               "that only restates a fish's own definition (a steelhead is a rainbow over 50 cm) "
               "is not carried. Only a derived lift (`basis`) carries it",
    "basis": "names_the_fish: the book prints no exemption; the lifter is a water row that NAMES "
             "a fish its region closes ('bass daily quota = 8' under Region 8's 'Bass: 0 quota, "
             "CLOSED TO FISHING (see tables for exceptions)'), and so lifts that closure for the "
             "fish both name — never further than the lifter covers (its dates, `origin`, "
             "`lengths`, and only where the lifter itself is bound), and never a closure whose "
             "own entry prints its exemptions. any_size (owner, ROUND13 A): the lifter is a water "
             "row's quota printing '(any size)', which drops EVERY size constraint of its region for "
             "its fish — the 'only 1 over 50 cm' clauses and the 'none under/over X cm' rules (Robb "
             "Lake's 'Bull trout daily quota = 1 (any size)' lifts Zone 7A's 1 over 50 cm for bull "
             "trout). Derived when the bundle is built — never infer it. "
             "Absent = the lifter's own printed exemption",
    "equivalent": "'<entry_id>::<rule_id>' — the lifter's printed exemption names that blanket "
                  "closure of its own region, and this item is the SAME KIND of closure in "
                  "another region the lifter's water lies in: the same kind of water, and the "
                  "same closure BY NAME — spring, summer or winter, as the lifter's own words "
                  "say it ('Exempt from spring closure') or, where they name none ('Mainstem "
                  "open all year'), as the closure it names is called. Dates never decide it: "
                  "the Nechako's 'Exempt from spring closure' lifts Region 6's Fraser-watershed "
                  "spring closure (Apr 1-June 30) and never its Skeena/Nass winter closure (Jan "
                  "1-June 15), though the two overlap. It names EVERY closure of that kind — "
                  "the Nechako's also names the Iskut part of Region 6's spring closure — but a "
                  "lift only ever acts on the lifter's own sections: the water can lift nothing "
                  "but itself, so a named closure that never touches the water changes nothing. "
                  "West Road River's Region 5 row, 'the "
                  "regional spring closure does not add to its own mainstem closure', lifts Zone "
                  "7A's spring closure on its Zone 7A pieces. It holds only where the lifter is "
                  "bound. A species closure is never lifted this way. Absent = the book names "
                  "this rule",
    "caution": "{kind, says} — an INTERPRETATION WARNING to show beside the lift. kind "
               "'size_clause_override': the lift removes a region's size clause ('only 1 over 50 "
               "cm') because the water row prints a larger number for the fish (user ruling "
               "2026-09-26) AND prints it '(any size)' (user ruling 2026-09-28) — whether that "
               "means no size limit at all or only no minimum, the book does not say. The lift "
               "applies; `says` is the plain sentence to show with it. A row printing its own "
               "sizes, or none, lifts the clause with no caution. See `gotchas`",
}

LENGTH_TEXT = {
    "min_cm": "lower bound; absent = open. Inclusive, EXCEPT on a range with take 0 (see `closed`)",
    "max_cm": "upper bound; absent = open. Inclusive, EXCEPT on a range with take 0 (see `closed`)",
    "take": "how many of THESE you may keep; absent = the rule's own `take`",
    "closed": "only on a take-0 range, only `true`: the book prints 'X cm or more' / 'X cm or less', "
              "so a fish of exactly X is in the range (goes back). Absent, a take-0 range does NOT "
              "hold its own bounds: a fish exactly on a printed size bound is LEGAL ('none under 30 "
              "cm' keeps a 30.0 cm fish, 'no trout over 50 cm' keeps a 50.0 cm one; user ruling "
              "2026-10-07, `catalogue.EXACT_BOUND_IS_LEGAL`)",
}

WHO_TEXT = {
    "residency": "resident (of B.C.) | non_resident (not a B.C. resident, but a Canadian citizen "
                 "or permanent resident, or living in Canada) | non_resident_alien (neither)",
    "age": "under_16 | 16_plus",
    "guidance": "guided | non_guided",
    "status": "indian_bc_resident | metis | disabled | aged_65_plus (65 and over — inside "
              "16_plus, so a status, not an age: the 'Annual Licence for Age 65 Plus', p.5)",
    "role": "companion — fishing as the companion of an authorized angler on a Youth/Disabled "
            "Accompanied Water (printed p.4); not a partition: most anglers are no one's companion",
}

PATH_TEXT = {
    "hold": "hold ALL of these documents",
    "accompanied_by": "be accompanied by an angler in `who` holding what this fishing requires "
                      "of them",
    "as": "satisfy the requirements AS this `Who` instead",
    "quota": "own | counts_to_companion — a NOTE on an accompaniment path: whose quota the fish "
             "count against. Render it (an asterisk, a line); it is not arithmetic.",
}

VIA_TEXT = {
    "reach": "the record names this water (or area) and binds it directly",
    "trib": "it reached this section by the tributary walk from a water whose entry includes "
            "tributaries",
    "trib_pending": "a tributary walk that was not done — never treat the set as complete",
    "contested": "a designation on a section a not_classified record also binds — say 'check'",
}

#: A rule set or licensing set (`rulesets[id]`, `licensing_sets[id]`), key by key.
SET_TEXT = {
    "sections": "how many sections carry this set",
    **{k: f"the member ids that reach these sections this way — {v}" for k, v in VIA_TEXT.items()},
}

PLACEMENT_TEXT = {
    "sections": "bound to sections; find them through `licensing_sets`",
    "province": "applies everywhere; no section rows — EXCEPT, when its extent names "
                "`outside_area_kind`, on the sections that kind covers (bundle table "
                "`province_except`; on a water, each of its `parts` lists the families its "
                "sections lie in as `province_except`). The basic licence and "
                "the stamps do not hold inside National Parks, whose own permit does. On "
                "`province_except` kind `tidal` NO province-wide requirement holds, whatever its "
                "record names: tidal water is federal (the Tidal Waters Sport Fishing Licence)",
    "on_designation": "applies wherever a designation is in force (`on`); no section rows",
    "unresolved": "could not be placed: `provenance.uncertain` is true and `why` says why. For "
                  "licensing the unsafe direction is under-requiring, so render 'check', never "
                  "'none needed'",
    NOT_PLACED: "licence_terms and exemptions are never bound to a place",
}

ENTRY_TEXT = {
    "kind": "province | zone | area | water — see `entries`",
    "chapter": "the id prefix: zp (province), z<region> (a region's chapter), r<region> "
               "(a water in that region)",
    "name": "the display name, as a reader writes it (never all capitals)",
    "official_name": OFFICIAL_NAME_TEXT,
    "full_name": "the heading as printed",
    "item_id": "the first water it matched", "matched": "every water it matched",
    "mus": "the management units the row was printed under",
    "pages": "the synopsis pages it is printed on: PRINTED page numbers (the footer's number, "
             "what the book's own cross-references mean), NOT the PDF's page index — to open the "
             "PDF there, look the page up in `guide.pdf_pages`",
    "symbols": "the printed glyphs, 1:1",
    "scope_note": "the curated sentence on which part of the water the entry covers",
    "extents": "the entry's own reach — each extent key by key in `rule.fields.extents[]`",
    "printed": "the whole printed passage",
    "rules": "its rule ids", "licensing": "its licensing record ids",
    "see": "its pointers, when it prints any: [{verbatim, entry_ids, relation}] or "
           "[{verbatim, unresolved}] — see `entries.pointers`",
}


#: THE OPS AN EXTENT SELECTS BY (`entry_models.Op`) — checked against the enum (`_registries`).
OP_TEXT = {
    "whole": "every section of every water the record covers (its entry's `matched`, or the "
             "extent's `item_id` / `item_ids`); no `splits`",
    "upstream_of": "the sections above ONE cut (`splits[0]`), following the water across the "
                   "covered waters",
    "downstream_of": "the sections below ONE cut (`splits[0]`), following the water",
    "between": "the sections between TWO cuts (`splits`), each resolved by route measure on its "
               "own blue line",
    "within": "the sections inside an area (`area_id`, or every area of a family: `area_kind`); "
              "no `splits`",
    "rest": "THE REST OF THE WATER: the record's water minus every section its `siblings` (rules "
            "of the same entry) bind — 'other parts'. A sibling that does not bind leaves the "
            "complement unknown, never the whole water",
    "steelhead_waters": "the book's steelhead waters (every steelhead row's own water and every "
                        "section its rules bind) minus what the `siblings` bind, limited by "
                        "`area_id` / `outside_area` / `outside_area_kind` / `feature_types` — "
                        "how the provincial steelhead set reaches a book-known water its base "
                        "does not (AGENTS 54)",
}


def extent_text() -> dict:
    """`rule.fields.extents[]` (also a licensing record's `fields.extents[]`, its
    `fields.tributary_excludes[]`, and an entry's `extents[]`), key by key — GENERATED from the
    model (`entry_models.Extent`: each field's own description; `op` from `OP_TEXT`), so it
    cannot drift from what the reach builder reads."""
    out = {}
    for k, f in Extent.model_fields.items():
        if k == "op":
            out[k] = ("how the extent selects sections: " + " | ".join(OP_TEXT)
                      + " — see `rule.fields.extents[].op`")
        else:
            out[k] = f.description or ""
    return out
