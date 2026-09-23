# 18 — How regulations are stored

How a sentence in the fishing synopsis becomes data, and how that data becomes a line on a screen.

Read this before touching regulation entries. It replaces doc 17.

---

## The problem, in one example

The synopsis says:

> *"Lake trout daily quota = 2 (only 1 over 90 cm, none between 60 cm and 90 cm)"*

We used to store that as **two** things: the sentence, and a short label a person typed beside it.
On Bennett Lake somebody typed *"Lake trout daily/possession quota = 2, tiered size limit."*

The numbers are gone. Nobody noticed, because nothing compares the label to the sentence.

**So we stopped letting people type labels.** A rule now stores what it *means*, and the label is
generated from that. If the meaning says 90 cm, the label says 90 cm. It cannot drift.

---

## A rule is a type plus conditions

```
type:      retention_limit          "this is about how many you may keep"
species:   ["LT"]                   lake trout
take:      2                        two of them
lengths:   [{max_cm: 90}, {min_cm: 90, take: 1}]   and no more than one over 90 cm
verbatim:  "Lake trout daily quota = 2 (only 1 over 90 cm...)"
```

`verbatim` is the synopsis sentence and it is **always kept**. The generated label is a summary;
the words the law used are one tap away.

There are **14 types**. That is the whole list:

```
WHAT YOU MAY KEEP     retention_limit · stop_fishing_after_quota
HOW YOU MAY FISH      bait_restriction · tackle_restriction · method_rule
BOATS                 vessel_rule · angling_from_vessel_prohibited · navigation_duty
WHO MAY FISH          angler_closure
AFTER YOU CATCH IT    handling_rule
INFORMATION           hazard · advisory · program_membership · facility
```

PAPERWORK IS NOT A RULE TYPE. `document_required` and `access_permission` were, and are now
refused: licensing never competes (which is the one thing a type exists to arbitrate), never
votes on open/closed, and depends on who the angler is and what they are doing, which no rule
takes. It lives on `CatalogueEntry.licensing` — designations, requirements, licence terms,
exemptions and alternatives.

---

## Why only 14? Because a type is a wall

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

### Size limits point two ways

```
"not more than 1 over 50 cm"    you MAY keep one big one       take=1, within=parent, lengths [{min_cm: 50}]
"none over 50 cm"               you may keep NO big ones       take=0, lengths [{min_cm: 50, take: 0}]
"1 bull trout over 60 cm"       the one you keep must BE big   take=1, lengths [{min_cm: 60}, {max_cm: 60, take: 0}]
```

One English word carries all three; `lengths` writes the range and its number, so the reader
never has to work out which is meant. Get it backwards and you invert the rule on exactly the fish
it was written to protect.

---

## How a chapter becomes entries

Worked from the real Region 3 text.

**1. Find the verbatim.** The chapter is transcribed word-for-word into
`data/curated/regulations/reference/region-3.md`. That file is the source of truth. Never edit it
to fit the model — change the model.

**2. Split one sentence into one rule each.**

> *"Trout/char: 5, but not more than 4 from streams, 1 over 50 cm, 1 bull trout or lake trout,
> none under 60 cm"*

is **four** rules: the 5, and three limits that live inside it. Each names its parent with
`within`, so the reader sees the 2 coming *out of* the 5 rather than in addition to it.

**3. Pick a type and fill the conditions.**

**4. Say where it applies.** `extents` names an area (`area:region:3`), optionally narrowed by
`feature_types: ["stream"]`. A rule that says *"in any stream"* and forgets that narrowing closes
every lake in the region — which has actually happened.

**5. If you cannot bind it, say so.** Give a `review_reason` that says why. An honest flag beats a
wrong guess; 49 of the 302 rules carry one today.

The result:

```
  TROUT AND CHAR DAILY QUOTA   [Region 3]
   ┌ Trout and char — 5 per day, all species combined
       • Trout and char — 4 per day, from streams
       • Trout and char (no more than 1 over 50 cm)
       • Bull trout, Dolly Varden and Lake trout (no more than 1, none under 60 cm)
   Steelhead — release all
   Bull trout and Dolly Varden — release all, from streams, Aug 1 - Oct 31
```

Every one of those lines was generated. None was typed.

---

## Which rule wins

Rules arrive from four places. The more specific one wins:

```
this exact water  >  the river it flows into  >  the region  >  the province
```

**Except** National Parks and Ecological Reserves, which beat everything.

Four things to know:

* **The loser stays on screen, struck through**, with a note saying what replaced it. The printed
  synopsis works by "here is the default, here are the exceptions" — hide the default and a reader
  cannot check anything.
* **A finer rule replaces a coarser one completely**, carrying its own dates. Haida Gwaii's bait ban
  runs Nov–Apr; Region 1's runs all year. On Haida Gwaii the answer is Nov–Apr, not both.
* **Except for species.** If a lake says *"rainbow: 2"* and the region says *"trout and char: 5"*,
  the region's rule keeps governing the other species. Striking it would delete the only rule about
  bull trout on that lake.
* **Two rules that differ on days, times, method or who you are never override each other.**
  Kitsumkalum River bans angling for one class of angler on Saturdays on one reach, and on Sundays
  on the whole river. Rank those by size and the Saturday rule deletes the Sunday one — which would
  tell somebody Sunday fishing is fine where it is banned all year.

---

## Parsing a new water

The per-water tables — about 3,000 rules — are parsed by an agent, not by hand. Three files govern
it, and the synopsis' own key to those tables is `reference/water-specific-tables.md`.

```
prompts/CATALOGUE_PARSE_PROMPT.md      what the agent is told
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
* **Bait bans and hook rules carry no species at all** — *"banned for all angling and for all
  species."* A species beside a bait ban in the same row belongs to a different rule.

⛔ **Parser runs spend credits and are human-only.** Editing the prompt, the model and the
validator is not; dispatching batches is.

## Where the files are

```
reference/            the synopsis, transcribed verbatim          ← source of truth
  definitions.md · provincial-regulations.md · licensing.md · region-1.md … region-8.md

entries/catalogue/    the typed rules                             ← what we build from
  region-provincial.json · region-1.json … region-8.json          106 entries, 302 rules

pipeline/regs/parsing/catalogue.py     the 15 types, validation, and the label generator
pipeline/tests/test_catalogue.py       what the model refuses, and why
pipeline/tests/test_catalogue_entries.py   what the authored files must satisfy
pipeline/tests/test_validate_catalogue.py  what a candidate parse must survive
```

The model refuses bad data rather than storing it. A few of the guards, and the real rule behind
each:

| refused | because |
|---|---|
| a retention rule with no species | 115 rules stored "no species" while naming one in their own text; the blanket default puts a trout limit on bass and burbot |
| a bait or tackle rule **with** species | 20 gear rules carry codes copied from a neighbouring clause. "Trout: bait ban" is narrower than the law — the ban applies to everyone |
| a sub-limit bigger than its parent | 20 brook trout inside a limit of 4 is not a sub-limit; it is a replacement |
| a `verbatim` that is not in the entry's text | the only thing stopping a number nobody printed from being invented |
| a `review_reason` of a few words | a flag nobody can act on is not a flag |

---

## Still open

1. **Inherited location text.** Gosnell Creek flows into the Morice. The Morice has a rule *"no
   fishing from the Dunes to Gosnell Creek."* Gosnell inherits it — and inherits the words too, so
   the creek's screen describes a stretch of a different river. Once extents are reviewed the
   section itself is the location, and an inherited rule should show **where it came from** instead
   of the parent's reach.
2. **Watershed rules are unbound.** *"Any stream in the Skeena watershed"* is a tributary walk, not
   a polygon — seed the Skeena, walk upstream, keep streams. Measured: 193 sections in, 84,624 out
   (69,551 stream, 15,073 lake). Nine rules need it; none is bound yet.
3. **49 rules flagged for review**, mostly water-scoped ones waiting to be matched to a registry
   item.
4. **No validity window.** Every chapter says to check Angler Alerts, and Region 5's Chilcotin
   closure has openings announced in-season. Nothing records when this data was true.
5. **The app still reads the old shape.** It derives its map colour from the old category field,
   which this replaces.
