# 18 — How regulations are stored

How a sentence in the fishing synopsis becomes data, and how that data becomes a line on a screen.

Read this before touching regulation entries. It replaces doc 17. It is background: the field-level
contract is `pipeline/regs/parsing/prompts/CATALOGUE_PARSE_PROMPT.md` and the model
(`pipeline/regs/parsing/catalogue.py`); where this doc and either of them disagree about a field,
they win. Counts here were measured on 2026-09-29 (`pipeline/docs/CURRENT-STATE.md` says where
each comes from).

---

## The problem, in one example

The synopsis says:

> *"Lake trout daily and possession quotas = 2 (only 1 over 90 cm, none between 60 cm and 90 cm)"*

We used to store that as **two** things: the sentence, and a short label a person typed beside it.
On Bennett Lake somebody typed *"Lake trout daily/possession quota = 2, tiered size limit."*

The numbers are gone. Nobody noticed, because nothing compares the label to the sentence.

**So we stopped letting people type labels.** A rule now stores what it *means*, and the label is
generated from that. If the meaning says 90 cm, the label says 90 cm. It cannot drift.

---

## A rule is a type plus conditions

Bennett Lake's daily quota is two rules — the quota, and the size clause that sits inside it:

```
bennett_lake.r1   type: retention_limit   species: ["LT"]   take: 2
                  exempts: [{target: trout_char_quota.r2, entry_id: z6:trout_char_quota}]
                           (Region 6's "1 over 50 cm": the lake's own sizes replace it)
                  → "Lake trout — 2 per day — lifts trout char quota for lake trout"
bennett_lake.r2   type: retention_limit   species: ["LT"]   take: 1   within: bennett_lake.r1
                  lengths: [{min_cm: 60, max_cm: 90, take: 0}, {min_cm: 90}]
                  → "Lake trout (no more than 1, none between 60 cm and 90 cm)"
```

(The possession quota is two more rules, r3 and r4, with `period: possession`.)

`verbatim` is the synopsis sentence and it is **always kept**. The generated label is a summary;
the words the law used are one tap away.

There are **13 types** in **6 families**. That is the whole list:

```
RETENTION          retention_limit · stop_fishing_after_quota
GEAR AND METHOD    bait_restriction · tackle_restriction · method_rule
VESSEL             vessel_rule · navigation_duty
ACCESS             angler_closure
CONDUCT            handling_rule
INFORMATION        hazard · advisory · program_membership · facility
```

"NO ANGLING FROM BOATS" IS NOT A BOAT RULE. `angling_from_vessel_prohibited` was a type and is now
refused: it says how you may fish, so it is a `method_rule` banning `angling` `when: {angler: in_boat}`
(`in_powered_boat` for "powered boats"), which meets the province's angling methods on the ladder.

PAPERWORK IS NOT A RULE TYPE. `document_required` and `access_permission` were, and are now
refused: licensing never competes (which is the one thing a type exists to arbitrate), never
votes on open/closed, and depends on who the angler is and what they are doing, which no rule
takes. It lives on `CatalogueEntry.licensing` — see "Licensing" below.

---

## Why only 13? Because a type is a wall

Two rules can only override each other **if they are the same type**. That is the whole reason the
list is short.

Here is the bug that taught us. Region 4 says *"Bass: closed to fishing."* Wasa Lake says *"Bass
daily quota = unlimited."* Same fish, same place, opposite answers — and the app showed **both**,
because one was filed as a "closure" and the other as a "harvest". Different filing cabinets, so
nothing noticed they disagreed. **87 rules across 55 waters did this.**

So the rule for adding a type is:

> **Same type if one could ever override the other. Different types if they never compete.**

A bait ban and a hook rule never compete — you must obey both. Different types. A closure and a
quota are the same subject at different values. **Same type.**

---

## The two conditions people get wrong

### `take` and `may_target` are different questions

```
take = 0, may_target = false    you may not fish for this at all
take = 0, may_target = true     fish for it, but put it back
take = 5                        keep up to five
```

*"Kokanee: none from streams"* is the **first** one — these are spawning fish and you may not target
them in a stream at all. The stream itself stays open for trout, which is why `may_target` is per
species: read as a water closure it would shut streams across eight regions.

(A reviewer argued the other way, from the printed page: the same block writes *"Bass: 0 quota,
CLOSED TO FISHING"* when it means a closure, so *"none from streams"* should be a retention
sub-limit. The call is no-targeting, on the fishery rather than the typography.)

**And it is per species.** A water is only closed when *every* species is `may_target = false`.

### Size limits point several ways

```
"not more than 1 over 50 cm"    you MAY keep one big one       take=1, within=parent, lengths [{min_cm: 50}]
"no trout over 50 cm"           you may keep NO big ones       no take, may_target=true, lengths [{min_cm: 50, take: 0}]
"1 bull trout over 60 cm"       the one you keep must BE big   take=1, lengths [{min_cm: 60}, {max_cm: 60, take: 0}]
```

One English word carries all three; `lengths` writes the range and its number, so the reader
never has to work out which is meant. It is an ordered list, first match wins, bounds inclusive,
a range without its own `take` uses the rule's, and a length no range covers is not spoken about.
Get it backwards and you invert the rule on exactly the fish it was written to protect.
`over_cm`, `under_cm` and `band` are refused, and `lengths` is refused on any type but
`retention_limit` (a size a licence depends on is the licensing record's `doing.lengths`).

---

## The species — the book's list, and nothing else

Page 86 (*"Freshwater game fish are defined as follows"*) is the only species set
(`catalogue.BOOK_FAMILIES`): TROUT (RB, ST, CT, GB), CHAR (DV, LT, EB), WHITEFISH (LW, MW), BASS
(LMB, SMB), OTHER (KO, GR, BB, WSG, BCB, NP, YP, WP, GE, IN, CRA). A code not on it is refused.

* **A bull trout is a Dolly Varden.** *"Any bull trout that you catch and keep must be counted as
  part of your Dolly Varden quota."* One fish, `DV`; `BT` is refused. The label reads "Dolly
  Varden/bull trout".
* **"Trout" includes char unless char are excluded** (p.86), so the printed word "trout" is
  `TROUT_CHAR`; a `TROUT` code is refused. **The exclusion is scoped by the row:** when the same
  water row, or the same zone table, mentions a char on its own (char, Dolly Varden/bull trout,
  lake trout, brook trout), its bare "trout" lines are `TROUT_CHAR` with `species_except:
  ["CHAR"]`. A line printing the group word "trout/char" names char in and never excludes them;
  a "trout" line never excludes one char alone. Region 1's box (*"Trout: 4 … you must release: All
  char (includes Dolly Varden)"*) makes its trout lines trout-only; a lake row printing only
  *"Trout daily quota = 2"* counts char toward the 2. The validator refuses a row that breaks this
  either way (`trout_scope_problems`). 25 rules carry `species_except: ["CHAR"]` today.
* **Groups:** `TROUT_CHAR`, `CHAR`, `WHITEFISH`, `BASS`, `ALL_GAME_FISH`. `CHAR` NAMES its fish
  (`NAMING_GROUPS`): "All char" is how the book names char apart from trout, so it beats a
  water's group quota for a char. Open subjects with no members: `ALL_FIN_FISH` (never crayfish),
  `PROTECTED_SPECIES`, `SALMON`. **Chinook (`CH`)** is the one salmon the book names — in SALMON,
  not a game fish.
* "All other species", "all species" and a bare "catch and release" on a water row mean every
  game fish except crayfish: `ALL_GAME_FISH` with `species_except` `CRA`.

---

## How a chapter becomes entries

Worked from the real Region 3 text.

**1. Find the verbatim.** The chapter is transcribed word-for-word into
`data/curated/regulations/reference/region-3.md`. That file is the source of truth. Never edit it
to fit the model — change the model.

**2. Split one sentence into one rule each.**

> *"Trout/char: 5, but not more than: 4 from streams, 1 over 50 cm, 1 bull trout (Dolly Varden) or
> lake trout, none under 60 cm"*

is the 5 and the limits that live inside it. Each clause names its parent with `within`, so the
reader sees the 4 coming *out of* the 5 rather than in addition to it.

**3. Pick a type and fill the conditions.**

**4. Say where it applies — on every rule.** A rule never inherits its entry's place.
A zone rule names an area: `[{op: within, area_id: "area:region:3"}]`, narrowed by
`feature_types: ["stream"]` when it says "from streams" (a rule that says *"in any stream"* and
forgets that narrowing closes every lake in the region — which has actually happened). A water
row's rule is the whole water, `[{op: whole}]`, or a stretch of it cut at named points
(`upstream_of`, `downstream_of`, `between`). See "Where a rule applies" below.

**5. If you cannot bind it, say so.** Give a `review_reason` that says why. An honest flag beats a
wrong guess; 210 of the 3,317 rules carry one today.

The result — `z3:trout_char_quota`, labels exactly as `catalogue.label()` generates them:

```
  Trout and char — 5 per day, all species combined
    • Trout and char — 4 per day, all species combined, from streams
    • Trout and char (no more than 1 over 50 cm)
    • Dolly Varden/bull trout and Lake trout — 1 per day, all species combined
  Dolly Varden/bull trout and Lake trout (none under 60 cm)
  Steelhead — release all
  Dolly Varden/bull trout — release all, from streams, Aug 1-Oct 31
  Lake trout — release all, Oct 15-Jan 31
```

Every one of those lines was generated. None was typed. (Region 3's table prints "trout/char", the
group word, so none of its lines excludes char.)

---

## Where a rule applies — `extents`

A list of extents, unioned. The ops:

```
whole            the entire water the row names. No split ids.
upstream_of      one cut-point id          downstream_of   one cut-point id
between          two cut-point ids         within          an area (area_id or area_kind)
rest             "other parts": the water minus what named sibling rules bind
```

* **`rest`.** A row that names parts of its water in some rules and then says *"Other parts:
  trout/char daily quota = 1"* means the rest: `[{op: rest, siblings: ["bull_river.r1"]}]`. The
  builder binds the rule's water, with its own tributary scope, minus every section the siblings
  bind; a sibling that cannot bind leaves the rest unknown — never the whole water. 4 rules.
* **`undrawn_part`.** A part of the rule's own water that nothing draws ("on parts", "within
  200 m of the bridge") is written in the page's words beside `[{op: whole}]`. The rule is shown
  on the water as "not yet mapped" and never decides it. `whole` beside `extent_text` is refused:
  it would close the whole lake for a rule about one bay. 127 rules.
* **A place that is not part of the row's water**, or that nothing says which water it is in,
  keeps its words in `extent_text` / `unresolved_locators` with no extents, and stays unbound
  (14 rules).
* **`side`.** *"No Fishing on the west half of river between fishing boundary signs"* is
  `side: west` on the stretch. It holds on half the channel, so it is read beside the other
  rules and displaces none; on the other half the water's other regulations apply. A lake's half
  is an `undrawn_part`.
* **Tributaries.** `includes_tributaries` (entry, or rule — `None` on a rule inherits),
  `tributaries_only` (the walk without the row's own water), `tributary_excludes` (what the walk
  must not enter). "Tributaries" walks streams only (p.86).
* **Watersheds.** A row printed for a whole watershed within a zone binds the FWA basin
  intersected with the region, not only what the walk reaches. A watershed PART ("Fraser
  watershed upstream of the Williams Lake River") is `Extent.watershed`: the sides are decided
  by FWA code position. 7 rules use it (white sturgeon in the zone table and on the Region 5 Fraser
  row, Iskut, Skeena/Nass winter closure, the sturgeon licence), and all 7 bind.
* **Area carve-outs.** `within_area` intersects with a polygon after the walk; `outside_area`
  subtracts one; `outside_items` takes back a lake an area only touches (Goose Lake from the
  Malcolm Knapp forest).

A zone entry matches no water, so a directional extent on a zone entry must carry the water's
`item_id`.

---

## Seasons — `when`

`{dates, hours, weekdays, unparsed}`; absent means all year, every hour, every day (*"When no date
is listed, the regulations apply all year. Start and end dates are inclusive."*). `dates` may wrap
the year end and are always the days the rule HOLDS — a printed "except" is inverted when
written. `hours` needs both ends, each a clock time or a solar time with an offset. `unparsed`
keeps a season nobody could read; it is never shown as all year.

A date at the end of an "and"/comma chain dates the whole chain; **a `;` ends the run** —
*"Trout/char catch and release; bait ban, June 15-Oct 31"* dates only the bait ban. The model
refuses a rule whose own quote prints dates it does not carry.

---

## Gear — `gear`, `while`, `conduct`

A gear rule states its constraint as an ordered list of clauses, each one `slot` (bait, lure,
method, barb, points_per_hook, lines_per_angler, hook_gap_mm, …) and one bound: `allow`, `only`
or `ban` for a set (never an empty list — an empty list is what a dropped key writes), `max` /
`min` / `unlimited` for a count, `must_be` for how a device is built. `when` and `unless` condition
a clause; within one slot the first matching clause wins. *"Single barbless hook"* is
`[{slot: barb, only: [barbless]}, {slot: points_per_hook, max: 1}]`. `while` is the means during
which a rule binds (spear fishing, a downrigger); `when_targeting` the fish being fished for;
`conduct` a named duty from a closed registry. A clause `note` escapes the closed vocabulary and
requires a `review_reason`.

**Bait bans and hook rules carry no species** — *"banned for all angling and for all species."* A
species beside a bait ban in the same row belongs to a different rule.

---

## Pointers — `see`

*"See Lonzo Creek"* states no regulation. It is the entry's `see` list, `[{verbatim, entry_ids}]`,
never an `advisory` rule (refused). A row that is only a pointer has no rules. A pointer at
something that is not a row ("see page 63") is information and stays an advisory, or a `see`
with an `unresolved` reason. 73 entries carry one.

---

## Licensing

Licensing is a list on the entry, beside `rules`, each record with a `kind`: `designation`
(Class I/II water, licence unit, steelhead-stamp period or waiver), `not_classified`,
`requirement` (who, doing what, must hold which documents), `licence_terms`, `exemption`,
`alternative`. It never competes and never opens or closes a water, and the angler is always
unknown, so every answer is conditional. `Who` has axes `residency`, `age`, `guidance`, `status`
and `role`. A Youth/Disabled Accompanied Water is two rules, not licensing: a `program_membership`
notice and an `angler_closure` closed to `age: [16_plus]` except disabled residents and
companions (`closed_to_except`). 111 records today.

---

## Which rule wins

The reference is `pipeline/deliver/bundle/read.py::effective_rules(section, day, fish)`; the
export guide says the same in prose. In outline:

* **Per fish, per day.** Two rules compete only for a fish both speak for, and only while both
  are in force. A dated water rule gives way to its region outside its dates.
* **Lifts first.** A printed exemption (or one derived at build) removes the lifted rule for that
  fish while the lifter is in force. A closure leaves only by a lift.
* **Then naming, then place.** A rule naming the fish beats one naming a group holding it; then
  this water, then inherited by the tributary walk, then area, region, province. National parks
  and other superior authorities sit above the ladder.
* **Water quotas and zone quotas.** The exact same statement: the water's number wins. Different
  statements sit beside each other. A larger water number for a fish lifts the zone's number for
  it. A water row printing its own dates overrides a dated zone rule on the overlap; an undated
  water quota does not silence a dated zone release.
* **A water's release silences the zone** for that fish. A zone release limited to streams
  displaces its own region's same-dimension quotas on a stream.
* **Two regions on one lake:** both bases apply, and per fish the stricter wins.
* Information rules, `standing` rules and `undrawn_part` rules never compete.

---

## Parsing a new water

The per-water tables — about 3,000 rules — are parsed by an agent, not by hand. Three files govern
it, and the synopsis' own key to those tables is `reference/water-specific-tables.md`.

```
prompts/CATALOGUE_PARSE_PROMPT.md      what the agent is told (the field contract)
regs/parsing/catalogue.py              the types it must choose from
regs/parsing/validate_catalogue.py     the gate it must pass before a human sees the result
```

**The gate has three layers, and the third is the one that matters:**

1. **the model** — types, conditions, enums, arithmetic;
2. **the entry** — each rule's `verbatim` inside its own `regs_verbatim`;
3. **the source** — `regs_verbatim` against the printed row the agent was handed.

Layer 2 alone is not chain of custody. The agent writes both sides, so an invented sentence passes
— and two did, in the first authored pass. Layer 3 closes it, and so does a check that **every
number appears in its own rule's sentence**.

Two things the page tells the parser that are easy to get wrong:

* **Catch and Release** and **No fishing for** are different. The first lets you fish and makes you
  release; the second says you *"may not deliberately fish for the species named even if your
  intention is to release."* `may_target` is that bit, and it is per species.
* **Bait bans and hook rules carry no species at all** (above).

⛔ **Parser runs spend credits and are human-only.** Editing the prompt, the model and the
validator is not; dispatching batches is.

## Where the files are

```
data/curated/regulations/reference/          the synopsis, transcribed verbatim   ← source of truth
  definitions.md · provincial-regulations.md · licensing.md · region-1.md … region-8.md
  water-specific-tables.md

data/curated/regulations/entries/catalogue/  the typed rules                      ← what we build from
  region-provincial.json · region-1.json … region-8.json, region-7a.json, region-7b.json
  (11 files; 1,515 entries, 3,317 rules, 111 licensing records)

pipeline/regs/parsing/catalogue.py           the 13 types, validation, and the label generator
pipeline/deliver/bundle/read.py              effective_rules — which rule wins
pipeline/tests/test_catalogue.py             what the model refuses, and why
pipeline/tests/test_catalogue_entries.py     what the authored files must satisfy
pipeline/tests/test_validate_catalogue.py    what a candidate parse must survive
```

The model refuses bad data rather than storing it. A few of the guards, and the real rule behind
each:

| refused | because |
|---|---|
| a retention rule with no species | 115 rules stored "no species" while naming one in their own text; the blanket default puts a trout limit on bass and burbot |
| a bait or tackle rule **with** species | 20 gear rules carried codes copied from a neighbouring clause. "Trout: bait ban" is narrower than the law — the ban applies to everyone |
| a sub-limit bigger than its parent | 20 brook trout inside a limit of 4 is not a sub-limit; it is a replacement |
| a `verbatim` that is not in the entry's text | the only thing stopping a number nobody printed from being invented |
| a species code not on p.86 (`BT`, `TROUT`, …) | the book's list is the only species set; a bull trout is a Dolly Varden |
| a `when` that loses or crosses a printed date | one real miss cut "Nov 1-Mar 31" to "Nov" and read two rules as all year |
| a `review_reason` of a few words | a flag nobody can act on is not a flag |

---

## Still open

1. **Inherited location text.** Gosnell Creek flows into the Morice. The Morice has a rule *"no
   fishing from the Dunes to Gosnell Creek."* Gosnell inherits it — and inherits the words too, so
   the creek's screen describes a stretch of a different river. An inherited rule should show
   **where it came from** instead of the parent's reach.
2. **Unbound rules.** 40 rules bind nowhere, all on water rows (no matched item, or a place no cut
   expresses); each has a typed reason. Every zone and provincial rule binds.
3. **No validity window.** Every chapter says to check Angler Alerts, and Region 5's Chilcotin
   closure has openings announced in-season. Nothing records when this data was true.
4. **The app reads no regulations yet.** `app/packages/core/src/regulations.ts` is the placeholder
   where the integration plugs in, against the bundle and `effective_rules`.
