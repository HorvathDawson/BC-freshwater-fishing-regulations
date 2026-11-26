# 18: How regulations are stored

How a sentence in the synopsis becomes data. The field contract is `pipeline/regs/parsing/prompts/CATALOGUE_PARSE_PROMPT.md`
and the model `pipeline/regs/parsing/catalogue.py`, and they win over this page; how the book is read is RULINGS.

## A rule is a type plus conditions; the label is generated

A rule stores what it means, and `catalogue.label()` generates its label from that, so the label cannot drift from the
numbers. `verbatim` is the synopsis sentence and is always kept: the words the law used are one tap away.

```
bennett_lake.r2   type: retention_limit   species: ["LT"]   take: 1   within: bennett_lake.r1
                  lengths: [{min_cm: 60, max_cm: 90, take: 0}, {min_cm: 90}]
                  → "Lake trout (no more than 1, none between 60 cm and 90 cm)"
```

There are 13 types in 6 families (`catalogue_parts/vocab.py` `RuleType`, `species.py` `_FAMILY`):

```
RETENTION          retention_limit · stop_fishing_after_quota
GEAR AND METHOD    bait_restriction · tackle_restriction · method_rule
VESSEL             vessel_rule · navigation_duty
ACCESS             angler_closure
CONDUCT            handling_rule
INFORMATION        hazard · advisory · program_membership · facility
```

**A type is a wall: two rules override each other only if they are the same type.** Same type if one could ever
override the other (a closure and a quota are one subject at two values); different types if they never compete (a
bait ban and a hook rule: you obey both).

"No angling from boats" is a `method_rule` banning `angling` `when: {angler: in_boat}`. Paperwork is no rule type: it is
`CatalogueEntry.licensing` (below). `document_required`, `access_permission` and `angling_from_vessel_prohibited` are
refused on load.

## The two conditions people get wrong

**`take` and `may_target` are different questions**, per species (`vocab.says_no_fishing` is the one closure test):

```
take = 0, may_target = false    you may not fish for it at all
take = 0, may_target = true     fish for it, but put it back
take = 5                        keep up to five
```

"Kokanee: none from streams" is the second (a retention sub-limit on streams, never a closure). A water is closed only
when every species is `may_target = false`.

**Size limits point several ways; `lengths` writes the range and its number:**

```
"not more than 1 over 50 cm"    you MAY keep one big one       take=1, within=parent, lengths [{min_cm: 50}]
"no trout over 50 cm"           you may keep NO big ones       no take, may_target=true, lengths [{min_cm: 50, take: 0}]
"1 bull trout over 60 cm"       the one you keep must BE big   take=1, lengths [{min_cm: 60}, {max_cm: 60, take: 0}]
```

An ordered list, first match wins, bounds inclusive, except that a range keeping none does not hold its own bounds, so
a fish exactly on a printed bound is legal (`catalogue.EXACT_BOUND_IS_LEGAL`; the book's "X cm or more / or less" is the
range's `closed: true`). A range without its own `take` uses the rule's; a length no range covers is not spoken about.
`over_cm`, `under_cm` and `band` are refused, and `lengths` is refused on any type but `retention_limit`.

## The species: the book's list (p.80), and nothing else

`catalogue.BOOK_FAMILIES` is the only species set; any other code is refused (AGENTS 45, 46; RULINGS C2):

- A bull trout is a Dolly Varden: one fish, `DV`; `BT` is refused.
- The printed word "trout" is `TROUT_CHAR`; `TROUT` is refused. It leaves out a char only when the same row or zone
  table gives that char a related line of its own (`species_except: ["DV"]`); all char leave (`["CHAR"]`) only when the
  related line names char as a group. "trout/char" never excludes char (`trout_scope_problems`).
- Groups: `TROUT_CHAR`, `CHAR`, `WHITEFISH`, `BASS`, `ALL_GAME_FISH`; open subjects `ALL_FIN_FISH` (never crayfish),
  `PROTECTED_SPECIES`, `SALMON`. Chinook (`CH`) is the one salmon the book names, in SALMON, not a game fish.
- "All other species", "all species" and a bare "catch and release" on a water row are `ALL_GAME_FISH` with
  `species_except: ["CRA"]`.

## How a chapter becomes entries

1. **The verbatim.** Each chapter is transcribed word for word into `data/curated/regulations/reference/` (the source
   of truth); never edit it to fit the model. The typed rules are `data/curated/regulations/synopsis/region-*.json`.
2. **One rule per clause, with its type and conditions.** "Trout/char: 5, but not more than: 4 from streams, …" is the 5
   and the limits inside it; each clause names its parent with `within`, so the 4 comes out of the 5, not on top of it.
3. **Where it applies, on every rule** (AGENTS 13). A zone rule names its area (`[{op: within, area_id:
   "area:region:3"}]`), narrowed by `feature_types: ["stream"]` when it says "from streams"; a water row's rule is the
   whole water or a stretch cut at named points.
4. **If it cannot bind, say so** with a `review_reason` that says why. A flag of a few words is refused.

## Where a rule applies: `extents`

A list, unioned. Ops: `whole`; `upstream_of`, `downstream_of` (one cut-point id); `between` (two); `within` (an area);
`rest` ("other parts": the water minus what named sibling rules bind; a sibling that cannot bind leaves it
`complement_unknown`, never the whole water).

- `undrawn_part`: a part of the rule's own water nothing draws, in the page's words beside `[{op: whole}]`; shown as
  "not yet mapped", never deciding the water. `whole` beside `extent_text` is refused.
- A place outside the row's water keeps its words in `extent_text` / `unresolved_locators`, with no extents, and stays
  unbound.
- `side`: half of a river ("the west half") is read beside the other rules and displaces none (AGENTS 49).
- Tributaries: `includes_tributaries` (a rule's `None` inherits the entry's, AGENTS 9), `tributaries_only`,
  `tributary_excludes`. "Tributaries" walks streams only (p.80).
- Watersheds: a row printed for a whole watershed binds the FWA basin intersected with the region; a watershed part is
  `Extent.watershed`, sides decided by FWA code position.
- Area carve-outs: `within_area` intersects after the walk, `outside_area` subtracts, `outside_items` takes back a lake
  an area only touches.
- A directional extent on a zone entry carries the water's `item_id` (a zone entry matches no water).

## Seasons: `when`

`{dates, hours, weekdays, unparsed}`; absent means all year. `dates` may wrap the year end and are the days the rule
HOLDS (a printed "except" is inverted). `hours` needs both ends; `unparsed` is never shown as all year. A `;` ends a
dating run (AGENTS 47); the model refuses a rule whose quote prints dates it does not carry.

## Gear: `gear`, `while`, `conduct`

A gear rule is an ordered list of clauses, each one `slot` (bait, lure, method, barb, points_per_hook,
lines_per_angler, hook_gap_mm, …) and one bound: `allow`, `only` or `ban` for a set (never empty), `max` / `min` /
`unlimited` for a count, `must_be` for how a device is built. `when` and `unless` condition a clause; within one slot
the first matching clause wins. "Single barbless hook" is `[{slot: barb, only: [barbless]}, {slot: points_per_hook,
max: 1}]`. `while` is the means during which a rule binds; `when_targeting` the fish being fished for; `conduct` a named
duty from a closed registry. A clause `note` escapes the vocabulary and requires a `review_reason`. **Bait bans and hook
rules carry no species**; a species beside a bait ban belongs to another rule.

## Pointers and licensing

- "See Lonzo Creek" is the entry's `see` list (`[{verbatim, entry_ids}]`), never an `advisory` (AGENTS 48).
- Licensing is a list on the entry, each record a `kind` (`designation`, `not_classified`, `requirement`,
  `licence_terms`, `exemption`, `alternative`). It never competes or opens or closes a water, and every answer is
  conditional (`Who`: `residency`, `age`, `guidance`, `status`, `role`). A Youth / Disabled Accompanied Water is two
  rules: a `program_membership` notice and an `angler_closure` (`closed_to_except`).

## Which rule wins

`pipeline/deliver/bundle/read/ladder.py::effective_rules` (AGENTS 50-61, RULINGS): per fish and per day; lifts first;
then naming (a fish beats a group holding it), then place (water, inherited, area, region, province), superior
authorities above the ladder. Information, `standing` and `undrawn_part` rules never compete.

## Parsing a new water, and the gate

An agent parses the per-water tables (told by `prompts/CATALOGUE_PARSE_PROMPT.md`, gated by `validate_catalogue.py`);
**parser runs are the owner's (AGENTS A1).** The gate checks the model, each `verbatim` inside its entry's
`regs_verbatim`, and `regs_verbatim` against the printed row the agent was handed, with every number in its own rule's
sentence (the agent writes both of the first two, so they alone prove nothing).

The model refuses bad data rather than storing it (`pipeline/tests/test_catalogue.py`, `test_catalogue_entries.py`,
`test_validate_catalogue.py`): a retention rule with no species; a bait or tackle rule with species; a sub-limit bigger
than its parent; a `verbatim` not in the entry's text; a species code not on p.80; a `when` that loses or crosses a
printed date; a vague `review_reason`.

## Open

An inherited rule carries its parent's location words (Gosnell Creek inherits the Morice's "from the Dunes to Gosnell
Creek"). Nothing records when this data was true: every chapter says to check Angler Alerts.
