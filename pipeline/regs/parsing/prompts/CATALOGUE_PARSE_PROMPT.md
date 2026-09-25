# Parsing a water into the rule catalogue

You are converting one row of the BC Freshwater Fishing Synopsis into typed rules.

**You do not write labels.** You pick a TYPE and fill its CONDITIONS. The displayed line is
generated from those by `pipeline/regs/parsing/catalogue.py`. A label typed beside a number drifts
from it — that is the defect this format exists to remove.

Read `pipeline/docs/18-how-regulations-are-stored.md` first. The synopsis' own key to these tables
is `data/curated/regulations/reference/water-specific-tables.md` — it defines the terms below and
you must follow it, not your intuition about what the words mean.

---

## Absolute rules — violations fail validation

1. **`verbatim` must be an exact contiguous substring of `regs_verbatim`.** Never reword, never
   stitch two sentences, never add a connective. If the printed text is a bullet under a heading,
   quote the run that contains both.
2. **`regs_verbatim` is the printed passage, unedited.** If you find yourself improving it, stop.
3. **One restriction per rule.** *"Bait ban and single barbless hook"* is two rules.
4. **Never invent a number.** Every `take`, every bound inside `lengths`, `max_kmh`, `max_power_kw` must
   appear in that rule's own `verbatim`.
5. **If you cannot bind a location, say so** — give a `review_reason` that names what is
   missing. An honest flag beats a wrong guess.
6. **A `bait_restriction`, `tackle_restriction`, `method_rule` or `handling_rule` says what it
   constrains in `gear` (or `conduct`) — never in a flag.** *"Bait ban"* is
   `gear: [{"slot": "bait", "ban": ["any_bait"]}]`; *"roe may be used"* is
   `{"slot": "bait", "allow": ["roe"]}`. The direction is the KEY the clause uses, so it cannot
   sit in a neighbouring field and be read backwards. See "Gear" below.
7. **A `vessel_rule` with `aspect: propulsion` MUST carry `level`.** One ordered scale, strictest
   first:

   | the page says | `level` |
   |---|---|
   | *"No vessels"* | `none` |
   | *"No powered boats"* | `unpowered` |
   | *"Electric motor only"* | `electric_only` |
   | *"7.5 kW (10 hp)"* | `power_capped` + `max_power_kw: 7.5` |

   *"No vessels"* and *"No powered boats"* are both a refusal and they are **different rules** —
   one bans the boat, the other bans the motor. **Six of twenty-one rejections were this.**

---

## The catalogue is TWO TIERS: six families, fourteen types

The family is how the reader's screen is sectioned. The type is what you choose.

```
RETENTION          what you may keep
  retention_limit            how many, of what, when, how caught, at what size
  stop_fishing_after_quota   taking the quota ends your day on that water

GEAR AND METHOD    how you may fish
  bait_restriction           what may go in the water to attract fish
  tackle_restriction         what may be attached to the line
  method_rule                angling · set line · spear · trap · ice · net · snag

VESSEL             boats
  vessel_rule                        propulsion (an ordered scale) · speed · towing
  navigation_duty                    an obligation toward other vessels

ACCESS             who may fish here at all
  angler_closure             closed to ONE KIND of angler (`closed_to`) — e.g. non-guided
                             non-resident aliens on weekends

LICENSING is NOT a rule type. What you must hold goes in the entry's `licensing` list —
see "Licensing — `licensing`" below.

CONDUCT
  handling_rule              what you must do with the fish after catching it

INFORMATION
  hazard · advisory · program_membership · facility
```

Two rules **within one family** may still be unrelated — a bait ban and a hook rule are both
gear-and-method and never compete. The family groups the display; the **type plus its dimension**
decides what competes. Never reach across a family.

**Choosing between them:** two rules share a type only if one could ever override the other. A bait
ban and a hook rule never compete — you obey both — so they are different types. A closure and a
quota are one subject at two values, so they are the **same** type.

### Type by what the rule DOES, not by the words it uses

These were each filed two ways in the old corpus. The right column is the rule:

| statement | type | why |
|---|---|---|
| *"No ice fishing"* | `method_rule`, `gear: [{"slot": "method", "ban": ["ice_fishing"]}]` | it prohibits a METHOD. It says nothing about what you may keep and competes with no quota. |
| *"No powered boats"* | `vessel_rule(aspect=propulsion)` | restricts the boat, not the tackle |
| *"No angling from boats"* | `method_rule`, `gear: [{"slot": "method", "ban": ["angling"], "when": {"angler": "in_boat"}}]` | it restricts HOW you may fish, not the boat — a water can allow motoring and forbid fishing from the boat. The `when` keeps shore angling allowed |
| *"No angling from powered boats"* | the same, `"angler": "in_powered_boat"` | without it the ban reaches a canoe the book allows. (The old boat-angling rule type is retired and refused.) |
| *"Class I/II water"* | **not a rule** — a `designation` in `licensing` | a fact about the water; the provincial requirement fires on it |
| *"Youth/disabled accompanied water"* | `program_membership` | an ACCESS provision — who may be brought along, not what licence is held |
| *"Angling prohibited for non-guided non-resident aliens on Saturdays"* | `angler_closure`, `closed_to: {"residency": ["non_resident_alien"], "guidance": ["non_guided"]}` | it closes the water to **one kind of angler**. Filed as `retention_limit` it shares a key with — and can displace — a quota that binds everyone. |
| *"Exempt from the spring closure"* | **not a type** — `exempts` on the rule it lifts | an exemption takes the type of whatever it removes |
| *"Bass: 0 quota, closed to fishing"* | `retention_limit(take=0, may_target=false)` | a closure IS a limit. Filed apart from quotas the override never fires. |

**Quick test:** `retention_limit` limits what you keep · `bait_restriction` what goes in the water ·
`tackle_restriction` what is on the line · `method_rule` how you fish · `vessel_rule` the boat ·
`angler_closure` who may not fish · `handling_rule` the fish
after capture · the note types impose nothing.

---

## The distinction that matters most

The synopsis defines these two, and they are not the same:

| printed | means | encode |
|---|---|---|
| **Catch and Release** | *"You may fish for the named species, but you must release any that you catch."* | `take=0, may_target=true` |
| **No fishing for** | *"You may not **deliberately fish** for the species named **even if your intention is to release**."* | `take=0, may_target=false` |

`take=0` alone is ambiguous and validation will refuse it. **`may_target` is per species** — *"No
fishing for kokanee"* on a stream leaves that stream open for trout.

A whole-water closure is `species=ALL_GAME_FISH, take=0, may_target=false`.

---

## Conditions you will reach for

```
species        REQUIRED on retention_limit. Use a group (TROUT, CHAR, TROUT_CHAR, WHITEFISH,
               BASS, ALL_GAME_FISH) when the page names a group; leaf codes when it names fish.
               species=[] is an ERROR. "all other species" is species_except.
take           int | null. null = a size limit whose COUNT comes from the region. NOT zero.
unlimited      separate from take
may_target     see above. Required whenever take = 0.
period         daily (default) | possession | annual (licence year, Apr 1 - Mar 31) | monthly.
               ONLY on retention_limit and stop_fishing_after_quota — refused on any other type.
per_daily      a possession MULTIPLIER, not a count
within         the rule_id of the limit this one sits inside
lengths        THE SIZE LIMIT, and the only field for it — see "Sizes" below.
water          stream | lake            origin  hatchery | wild

REQUIRED ON THREE TYPES, and the commonest reason an entry is rejected. Each says WHICH WAY
the rule runs, and none of them can be inferred from the words afterwards:

gear           bait_restriction, tackle_restriction, method_rule. See "Gear" below.
aspect+level   vessel_rule. aspect is propulsion | speed | towing.
               For propulsion, `level` is required and is an ORDERED scale, strictest first:
                 none        no vessels at all          ("No vessels")
                 unpowered   no motor                   ("No powered boats")
                 electric_only  electric motors only     ("Electric motor only")
                 power_capped  a kW limit, with max_power_kw ("7.5 kW / 10 hp")
               "No vessels" and "No powered boats" are both a refusal, and they are
               different rules: the level is what tells them apart.
while          the methods during which the rule binds: "dead fin fish may be used WHEN SET
               LINING" is gear on bait with while: ["set_lining"].
when           WHEN THE RULE BINDS — see "Seasons and times" below. One object holding
               `dates`, `hours`, `weekdays` and `unparsed`.
closed_to      angler_closure only: WHO the water is closed to, as a `Who` (below)
extent_text    the reach in the page's own words, when no split can express it
undrawn_part   the page's words for the PART of the water the rule holds in, when the menu
               cannot draw it — beside `extents` that bind the water it is in (see below)
exempts        what this rule LIFTS
suspended_while  a rule id in this entry: this rule is DORMANT while that one binds. (A licensing
               designation says the same thing its own way — see "Licensing" below.)
obligation     must (default) | should — "anglers are encouraged" is should, not law
review_reason  why a human must look. A reason present IS the flag, so write one that
               names what is missing — longer than 20 characters is expected.
```

## Extents — WHERE the rule applies

**EVERY RULE CARRIES ITS OWN `extents`. On the rule, never only on the entry.**

This is the single most-missed instruction in the format. On the first full parse 1,957 of
2,397 rules came back with none — the reach went on the entry and the rules were left bare —
and a rule with no extents binds to NO WATER AT ALL. 73% of the corpus resolved to nothing.

An entry-level extent narrows the ROW ("FRASER RIVER (upstream of the CPR Bridge at Mission)").
It does not give a rule its reach. If a rule covers the whole of the water the row names, say
so explicitly:

```json
"extents": [{"op": "whole"}]
```

Most rules are exactly that. Write it out every time.

A list of extents is a UNION ("this reach plus that one"). The ops:

```
whole            the entire water. NO split ids.
upstream_of      exactly 1 split id
downstream_of    exactly 1 split id
between          exactly 2 split ids
within           an area, not a reach — area_id or area_kind
```

**Split ids come ONLY from that item's "Bindable boundaries" menu.** Never invent one, never
reuse an id you saw on another item. Ingest refuses an id the water cannot bind.

**A cut-point can have two names.** The menu shows them as:

```
- `gauge__08NM247`  — 08NM247 · Okanagan River Below Mcintyre Dam  [split]
     — also written `okanagan_river__mcintyre_dam`; BIND THE ID ABOVE
```

Those are ONE physical point. The page says *"below McIntyre Dam"*; the id that survived the build
is a gauge number. **Bind the id on the first line** — the canonical one. Binding the alias is
accepted and rewritten, but naming the canonical id directly is what you should do.

### `within_area` — limiting a reach to a polygon

`within_area` is a FIELD on an extent, not an op. It intersects whatever the extent already
selected with an area, and it is applied **after** the tributary walk — so it limits the
tributaries too.

Use it when the page bounds a rule **by a region or park line rather than by a point on the
water**. Those have no cut-point and cannot be expressed as a reach:

```json
"extents": [{"op": "whole", "within_area": "area:region:5"}]
```

*"That part of the Fraser River within Region 5"* is the whole Fraser intersected with Region 5 —
not `between` two splits, because the region boundary is not a cut-point on the river. Region 6
has **zero** Fraser mainstem sections, which is why a bounded reach could never have worked there.

Combine it freely with an op: `{"op": "upstream_of", "splits": ["x"], "within_area": "area:region:5"}`.

**When nothing fits.** Do not force a binding. Put the page's words in `extent_text`, record the
phrase in `unresolved_locators`, and give a `review_reason`. A rule bound to the
wrong point is far worse than one visibly sent to review.

**A PART of the water is never `whole` alone.** When a rule names a part of the water — an arm, a
bay, "west of the signs", "on parts", "in 5 signed swimming areas" — and the menu has no cut-point
or part for it, bind the water it is in (`{"op": "whole"}`, or `whole` with the `item_id` of the
one lake it is in) and write the page's words for the part in `undrawn_part`, with a
`review_reason` naming what would draw it. The rule is then shown on that water as a note and
never colours it. `{"op": "whole"}` beside an `extent_text` is REFUSED: `whole` alone says the
whole water, and the builder would close the whole lake for a rule about one bay. Where the place
is not inside the row's water at all, or nothing says which water it is in, keep the words in
`extent_text` with NO `extents` (the rule stays unbound). ("Mainstem only" has a field:
`includes_tributaries: false` on the rule.)

## Seasons and times — `when`

One object. Every part is optional; the object itself is omitted when the rule is all year, all
day, every day.

```json
"when": {
  "dates":    [{"from_month": 11, "from_day": 1, "to_month": 4, "to_day": 30}],
  "hours":    {"start": {"at": "21:00"}, "end": {"solar": "sunrise", "offset_min": -60}},
  "weekdays": ["Saturday", "Sunday"],
  "unparsed": []
}
```

`dates` — INCLUSIVE at both ends, and a range MAY WRAP the year end ("Nov 1-Apr 30" is one
winter). No year. An empty or absent `dates` means ALL YEAR, per the book: "When no date is
listed, the regulations apply ALL YEAR."

`hours` — both ends required; half a window renders as a total closure. Each end is EITHER
`{"at": "HH:MM"}` on the 24-hour clock OR `{"solar": "sunrise"|"sunset", "offset_min": N}`, where
**N is NEGATIVE FOR BEFORE**: "one hour before sunrise" is `{"solar": "sunrise", "offset_min": -60}`.
Do not convert a solar time to a clock time — it depends on the date and the latitude, which is
the reader's to work out and not yours.

`unparsed` — a printed season you genuinely cannot read, kept verbatim. Use it rather than
guessing or dropping: an unparsed season and an ABSENT one are opposite facts, and the second
reads as "open all year".

### Which clause a date belongs to

A date printed at the end of a run of clauses joined by "and" or commas governs the whole run:
*"Trout/char catch and release and bait ban, June 15-Aug 31"* dates both rules. **A `;` ends the
run.** *"Trout/char catch and release; bait ban, June 15-Oct 31"* dates ONLY the bait ban — the
release before the `;` carries no `when`; *"Fly fishing only; bait ban upstream of …, Jul 1-Oct
31"* dates only the bait ban; *"No Fishing Aug 1-Oct 31; bait ban"* dates only the closure. The
validator refuses a `when` that crosses a `;`.

### "EXCEPT these dates" — WRITE THE DAYS THE RULE HOLDS

`dates` is always the days the rule DOES hold. When the page prints the days it does not,
invert them yourself — a stored exception inverts the field beside it, the same failure that
once had four size rules permitting exactly the fish they protect:

```
"Open June 16-Apr 30 each year"      a CLOSURE; it holds May 1 - June 15
  -> dates: [{"from_month": 5, "from_day": 1, "to_month": 6, "to_day": 15}]

"catch and release EXCEPT February and July"
  -> dates: [{"from_month": 8, "from_day": 1, "to_month": 1, "to_day": 31},
             {"from_month": 3, "from_day": 1, "to_month": 6, "to_day": 30}]
```

The complement of one wrapping range is another wrapping range; of two, usually two. `dates` is
a list precisely so this always fits.

## Sizes — `lengths`, and read the sentence, not the preposition

The word "over" means three different things. Stored in one field, the meaning had to be
reconstructed from whatever sat beside it, and four rules ended up permitting exactly the fish
they protect. Ask **which fish go back**, then write the range and its number — there is nothing
left to infer:

```
"not more than 1 over 50 cm"   you MAY keep one big one, and the parent governs the rest
                                 -> [{"min_cm": 50}]                       (a clause: `within`)
"no trout over 50 cm"          you may keep NO big ones, and this says nothing about small ones
                                 -> [{"min_cm": 50, "take": 0}]            take=0, may_target=true
"1 bull trout over 60 cm"      the one you keep must BE big, and a 50 cm one is forbidden
                                 -> [{"min_cm": 60}, {"max_cm": 60, "take": 0}]      take=1
"Trout daily quota = 2 (none over 50 cm)"   the 2 is bounded above
                                 -> [{"max_cm": 50}, {"min_cm": 50, "take": 0}]      take=2
```

An ORDERED list of ranges, FIRST MATCH WINS. `min_cm`/`max_cm` are INCLUSIVE, either may be
omitted for "open at that end", and a range without its own `take` uses the rule's `take`.
**A length no range covers is not spoken about by this rule** — at the top level nothing else
grants it, and inside a `within` clause the parent quota governs it.

```
"Trout daily quota = 2 (none over 50 cm)"   [{"max_cm":50}, {"min_cm":50,"take":0}]
"no trout over 50 cm"                       [{"min_cm":50,"take":0}]
"not more than 1 over 50 cm"  (a clause)    [{"min_cm":50}]
"1 bull trout over 60 cm"                   [{"min_cm":60}, {"max_cm":60,"take":0}]
"20-30 cm only", quota 2                    [{"min_cm":20,"max_cm":30},
                                             {"max_cm":20,"take":0},{"min_cm":30,"take":0}]
"none between 70 cm and 100 cm"             [{"min_cm":70,"max_cm":100,"take":0}]
```

Write the grant BEFORE the denial beneath it, so a fish of exactly 60 cm is granted rather than
denied. Where a clause carries both a hole and a number — "only 1 over 100 cm, none between 70
and 100 cm" — the number belongs to the piece ABOVE the hole and the piece below is left out,
because the parent quota governs it:

```
[{"min_cm":70,"max_cm":100,"take":0}, {"min_cm":100}]
```

---

## Gear — `gear`, `while`, `conduct`

`gear` is an ORDERED list of clauses. Each names ONE `slot` and how far it is constrained.
Within one slot the FIRST clause whose `when` matches wins, so a narrow case goes before the
general one; clauses on different slots are independent and all apply.

```
SET slots        bait · lure · method · barb
                 exactly ONE of  allow  (permits what it names, says nothing of the rest)
                                 only   (a whitelist: closes the slot to everything else)
                                 ban    (prohibits what it names)
                 of      narrows which members the clause speaks about
                 except  members a ban does not reach
COUNTED/MEASURED hooks_per_line · points_per_hook · lines_per_angler · flies_per_line ·
                 terminal_attachments_per_line · hook_gap_mm · weight_per_line_kg ·
                 bait_possession_kg · light_to_hook_mm
                 max / min, in the UNIT THE NAME CARRIES ("3 cm" is hook_gap_mm min 30)
SPEC slots       set_lining · crayfish_trapping · downrigger · light · ice_hut
                 must_be: how the thing must be built ("quick_release_to_line")
any clause       when:   {water, method, targeting, angler}   — the clause holds only then
                 unless: [{…same…, gear_in_use}]               — what lifts it
```

| the page says | `gear` |
|---|---|
| *"Bait ban"* | `[{"slot": "bait", "ban": ["any_bait"]}]` |
| *"Single barbless hook"* | `[{"slot": "barb", "only": ["barbless"]}, {"slot": "points_per_hook", "max": 1}]` |
| *"Single hook"* | `[{"slot": "points_per_hook", "max": 1}]` — a POINT, not a hook: one attachment per line is already the provincial rule |
| *"Fly fishing only"* | `[{"slot": "method", "only": ["fly_fishing"]}]` |
| *"Artificial fly only"* | `[{"slot": "lure", "only": ["artificial_fly"]}]` — NOT the same as fly fishing, which also forbids floats and sinkers |
| *"No ice fishing"* | `[{"slot": "method", "ban": ["ice_fishing"]}]` |
| *"No hooks greater than 15 mm from point to shank"* | `[{"slot": "hook_gap_mm", "max": 15}]` |
| *"fin fish … other than roe is prohibited"* | `[{"slot": "bait", "ban": ["fin_fish"], "except": ["roe"]}]` |
| *"more than 1 kg of weight (not downrigger weights)"* | `[{"slot": "weight_per_line_kg", "max": 1, "unless": [{"gear_in_use": "downrigger_weight"}]}]` |
| *"one line, EXCEPT two when alone in a boat on a lake"* | `[{"slot": "lines_per_angler", "max": 2, "when": {"water": "lake", "angler": "alone_in_boat"}}, {"slot": "lines_per_angler", "max": 1}]` — ONE rule, narrow case first |

**One sentence with an "except" is one rule;** the book printing two rows is two rules.

**`while`** is the method during which the rule binds (*"dead fin fish may be used when set
lining"* → bait allow + `while: ["set_lining"]`). **`when_targeting`** is the species being fished
FOR (*"bait ban when fishing for salmon"*). Neither is a clause of its own.

**`conduct`** is an act the angler must do or refrain from, as a registered token named in the
LAWFUL direction: `do_not_waste_catch`, `release_immediately`, `remove_ice_hut_before_breakup`,
`mark_set_line_with_contact_details`. It is never a piece of gear.

**An exemption carries no gear of its own.** *"EXEMPT from single barbless hooks"* is `exempts` on
a `tackle_restriction` and nothing else.

**No ceiling is `unlimited: true`, never a zero.** *"A person in a boat may angle with an unlimited
number of rods"* is `{"slot": "lines_per_angler", "unlimited": true, "when": {"angler": "in_boat"}}`.
`max: 0` means NONE.

---

## Rules the page imposes on you

* **Bait bans and hook rules carry NO `species`.** *"During the period when bait is banned it is
  banned for all angling and for all species."* *"Trout/char catch and release, bait ban"* is two
  rules and the bait ban is on the **whole river** — the species belongs to the release clause.
* **But a bait or hook rule MAY be scoped to what you are fishing FOR** — a stream can carry a
  salmon bait ban and nothing else. That is **`when_targeting`**, not `species`. The difference:
  `species` would mean the ban protects those fish; `when_targeting` means the ban applies to that
  fishery. A salmon bait ban and a general bait ban coexist; neither displaces the other.
* **`electric_only` already means max 7.5 kW.** Set `max_power_kw` only if the water differs.
* **The MU column is a locator, never an extent.** *"Not all applicable M.U.'s may be listed."*
* **An asterisk in the first column** means every rule reaches tributaries → `includes_tributaries`
  on the ENTRY. **An asterisk after one regulation** means only that one does → on the RULE.
* **(CW)** is a symbol on the entry, not a rule. The licence obligation is provincial.
* **"Catch and release all other species" / "all species" / "all fish" on a water row is GAME
  FISH, never crayfish:** `species: ["ALL_GAME_FISH"]`, `species_except: ["CRA", …the fish the
  row names itself]`. Not `ALL_FIN_FISH` (that reaches every non-game fish). The validator
  refuses anything else.
* **A POINTER IS NOT A RULE.** *"See Lonzo Creek"*, *"A tributary of Slocan River. See Slocan
  River"*, *"For regulations on the mainstem of the West Road River, see Region 5"* state no
  regulation: write them in the ENTRY's `see` list, never as an `advisory` rule —
  `"see": [{"verbatim": "See Lonzo Creek", "entry_ids": ["r2:lonzo_marshall_creek@2-4"]}]`.
  `entry_ids` are the entries the pointer names (look them up; every one must exist). A row
  whose only content is a pointer has NO rules. A pointer at something that is not a row
  ("see page 63", "see sign at trailhead") is information: keep it an `advisory`, or give the
  `see` an `unresolved` reason instead of `entry_ids`.

---

## Licensing — `licensing`

Licensing is a list of records on the ENTRY, beside `rules`, each with a `kind`. It never
competes, never opens or closes a water, and every record's `verbatim` (and every nested quote)
is a contiguous run of `regs_verbatim`.

```
designation     "Class II water Sept 1-Apr 30; Steelhead Stamp mandatory Dec 1-Apr 30"
                {"kind": "designation", "id": "<unit>", "classified": "II", "unit": "<slug>",
                 "unit_name": "<the name a non-resident's licence names>",
                 "when": {"dates": [...]},                       # absent = all year
                 "steelhead_stamp_during": {"when": {...}, "verbatim": "Steelhead Stamp mandatory …"}
                   OR "steelhead_stamp_waived": {"verbatim": "Steelhead Stamp not required"},
                 "suspended_while": [{"rule_id": "<closure in this entry>", "verbatim": "… until reopened …"}],
                 "extents": [...], "verbatim": "Class II water Sept 1-Apr 30"}
not_classified  "Part described is NOT a Classified Water"
requirement     an obligation stated once (provincial/zone): who, doing, satisfied_by paths
licence_terms   how a licence is sold (per day, 8 consecutive days, draw, booking)
exemption       a named `who` released from named documents
alternative     a place where another document also satisfies a requirement
```

* **"Class II water when open" needs no field** — licensing is only consulted where the water is
  open. Leave `when` out.
* **"X classified licence required for non-resident anglers"** is the designation's `unit`
  (`slug(X)`), NOT a requirement scoped to non-residents — every angler needs the licence there.
* **A waiver lifts only the classified-water stamp.** "Steelhead Stamp not required unless
  fishing for steelhead" is `steelhead_stamp_waived`; the "unless" is the provincial rule.
* **`Who`** is a set per axis: `residency` (resident / non_resident / non_resident_alien), `age`
  (under_16 / 16_plus), `guidance` (guided / non_guided), `status`. "non-resident" in the book
  means `["non_resident", "non_resident_alien"]`; "Canadian resident" means
  `["resident", "non_resident"]`. Never name every member of an axis — leave it out.

---

## Output

```json
{
  "entry_id": "r3:tranquille_lake@3-29",
  "name": "TRANQUILLE LAKE",
  "display_name": "Tranquille Lake",
  "region": "3",
  "regs_verbatim": "<the printed row, unedited>",
  "source_pages": [29],
  "symbols": ["Classified"],
  "matched": ["gnis:12345"],
  "extents": [{"op": "whole"}],
  "includes_tributaries": null,
  "rules": [
    {
      "rule_id": "tranquille_lake.r1",
      "type": "retention_limit",
      "verbatim": "Rainbow trout daily quota = 8",
      "species": ["RB"],
      "take": 8
    }
  ]
}
```

Validate before submitting — `candidate.json` is your reply exactly as you will submit it, the
array of `{"index": N, "entry": {...}}`:

```bash
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.parsing.validate_catalogue batch.json candidate.json
```

Exit 0 means every entry passed. Fix what it reports and run it again — do not submit a candidate
that has not come back clean.

**This step is not optional and it is not a formality.** In the run of 2026-09-10, twenty-one of
thirty-four entries were rejected on five errors the validator names exactly, in one line each,
before a human ever sees them. Every one of those entries had to be parsed a second time. Running
the gate costs seconds; skipping it costs the whole batch.
