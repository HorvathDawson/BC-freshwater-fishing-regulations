# What the UI is handed, and what it is never asked to work out

Companion to `05-table-generation.md`, which is about *building* a table. This is about what
crosses the line into the client.

Every number here is measured against the shipped bundle.

---

## Part 1 — The one fact the whole design rests on

| | |
|---|---|
| sections in the province | **1,956,787** |
| distinct **rule sets** among them | **2,375** |
| rules in a set | 1 – 131, **88 on average** |

Nearly two million sections have **two thousand three hundred and seventy-five** different
answers between them. The bundle already interns this: `section_ruleset(sid → set_id)` is the
whole of the section half, and `ruleset(set_id → entry_id, rule_id, via)` is the whole of the
rule half.

> **`set_id` is the only key the client needs.** Everything below is an index on it.

That is why the question "does the client get all the rules and build the table, or a region
table plus amendments?" has a third answer, and it is the right one:

> **Neither. The client is handed the rules AND the settled verdict on them, and renders whatever
> it likes from the pair.**

Part 2 is that distinction in full, and it is the whole design: the front end gets stage ③ — the
rules, complete, with every `verbatim` — so it can show provenance and lay the table out however
it wants; and it gets the result of SETTLING those rules, so it never has to run the carve
ladder itself. Passing only the rules makes every client re-implement the hard part. Passing only
a rendered table freezes the UI.

**Region base + amendments keeps its job, but that job is not resolution.** It is how the tables
are *verified* (§2.1 of `05`: a stretch is its base plus its own amendments, each attributable to
one named rule) and how they are *displayed* — a reader needs "Region 4 allows 1 a day; this
water says put it back", and every counter already carries the `Source` that says which is which.
Ship the split as **attribution on a settled line**, not as work for the client.

---

## Part 2 — Settling is not rendering, and only one of them is precomputed

The question "do we pass the rules and let the front end build the table, or pass a built
table?" has been asked twice and answered differently both times, because it is two questions
wearing one coat.

| | what it is | who does it |
|---|---|---|
| **SETTLING** | which counters bind here today, which carve which, what each number comes to | **offline, once** |
| **RENDERING** | what a reader sees, and in what order | **the front end, freely** |

Settling is the carve ladder, clauses, lifts, the date test and the origin pass. It is
deterministic and it is hard, and every time it has existed twice in this project the two copies
have drifted. Rendering is a design question with no single right answer, and freezing it into
the data is what makes "same data, different UI" impossible.

**So ship the settled ledger — not the raw rules alone, and not a rendered table.**

The proof that a settled ledger is general enough: **three different renderings already run off
one**, in this repository, today.

| consumer | what it makes of the same ledger |
|---|---|
| `rows.py` | one line per species-that-answer-alike |
| `display.py` | the nested budget tree — shared numbers drawn once, members hanging off them |
| `oracle.py` | "I have a 55 cm bull trout and a rainbow in the creel — may I keep this?" |

None of the three is privileged and none of them settles anything. A fourth is a fourth
consumer, not a change to the data.

### What crosses the line

| | size | what it is |
|---|---|---|
| **the rules** | **2.3 MB**, 3,422 records | stage ③ exactly — the flat dicts, with `verbatim`, `species`, sizes, dates, `extents`. The front end needs these anyway, for provenance: the sentence from the book is the one thing a reader can check. |
| **the settling verdict** | **3.9 KB** per (set, stretch), **~29 MB** for all 7,105 | per counter: does it bind today, what carved it, what it is a clause of, what it comes to. It references rule ids; it does not repeat rules. |
| **`section → set_id`** | ~3.9 MB packed | 1,956,787 × a 2-byte id |
| **the colour index** | ~7,105 rows | Part 2.2 |

**~35 MB for the province**, fully settled, every line traceable to its sentence.

Serialising the verdict WITH each counter's rule and source inline comes to **866 MB**, because
every counter repeats its rule's verbatim and its species list. The bundle already interns rule
sets; intern the rules the same way and the same data is 29 MB. Do not ship the fat form.

### 2.2 The colour index — for the map

The colour is not "what may I keep". It is **"is there anything here I have to read?"**, with a
closure overriding:

| colour | means | test |
|---|---|---|
| **closed** | the whole water is shut — a spring closure, a closure area | a live allowance of zero on *all game fish*, carrying no method: `ledger.shuts_the_water` |
| **this water** | someone wrote rules for this place specifically | any binding rule whose `scope` is `water` or `inherited` |
| **region only** | no rule names this water; the regional and provincial tables are the answer | neither of the above |

This is not new vocabulary. `core/status.ts` already separates OUTCOME (what you may do) from
PROVENANCE (`"specific" | "general"` — whether anyone wrote a rule here) and renders provenance
as line WEIGHT. This promotes it to the colour, and its own doc comment anticipates the move:
*"A water nobody wrote a rule about is OPEN with provenance 'general'. It is not a fourth
colour."* It is now.

Why it is the better signal: outcome makes most of the province one colour, because most of the
province is the regional default. Provenance highlights exactly the places the book singles out.

**Measured over all 2,375 rule sets** (7,105 (set, stretch) pairs, 162 s to settle):

| | (set, stretch) pairs |
|---|---|
| closed | 2,604 |
| this water | 4,332 |
| region only | 169 |

> **Colour the STRETCH, not the water.** Rolling the year up to its worst stretch paints every
> stream in Regions 3, 4, 5, 7A and 8 red all year, because they are shut January to June — 24 %
> of all sections. That is true of the year and useless on a map. The index is per (set, stretch)
> and the date picks one.

**And the closure test cannot be pattern-matched.** `take: 0` on `ALL_GAME_FISH` also describes
the provincial *"within 23 m downstream of any fishway"* rules, which are caveats nobody can draw
on a map, not closures. Matching the shape called **2,343 of 2,375** rule sets closed all year.
The ledger carries them as `only somewhere in here — nothing can draw where`, and only the ledger
knows.

## Part 3 — How a river becomes something a person can navigate

A river is not a list of sections; it is a handful of stretches that differ. The rule is:

> **Walk the sections in order along the blue line and merge neighbours that share a `set_id`.**

Chainage (`down_m` / `up_m`) gives the order; `set_id` gives the identity. A merged run takes the
union of its sections' geography — geometry, management units, regions, MU groups — because a run
is several sections and reporting only the first one's region is how a stretch came to name the
wrong region.

Measured over the 22 waters that ship: **272 sections → 102 runs, 2.7×.**

| water | sections | runs | |
|---|---|---|---|
| Fraser River | 90 | 20 | 4.5× |
| Skeena River | 35 | 7 | 5.0× |
| Kootenay River | 32 | 10 | 3.2× |
| Harrison River | 5 | 1 | 5.0× |
| Stamp River | 5 | 5 | 1.0× — every section differs |
| *(lakes)* | 1 | 1 | a lake is one run |

`pipeline/tools/build_section_data.py` already does this, and `app/design/regs-v3.html` is the
prototype of the navigation it feeds: a map of the river, a ruler along it, the runs as rungs,
and a two-step flow — **where on the river**, then **what applies there**.

---

## Part 4 — The division of labour

| | offline | the client |
|---|---|---|
| settle rules into counters | **yes** | never |
| lay those counters out as a table | no | **yes — freely, and differently per surface** |
| decide a colour | **yes** | looks it up |
| cut the year into stretches | **yes** | picks today's |
| merge sections into runs | **yes** | draws them |
| creel arithmetic ("I already have a bull trout") | impossible | **yes** — but it is subtraction over settled counters, not settling |
| weekday and time-of-day rules | impossible | applies them — they never resolve to a date |

The last two are exceptions for the same reason: they depend on something only the person holding
the rod knows — what is already in the creel, and what day and hour it is where they stand. Note
that neither requires the ladder. Spending a counter is arithmetic; `oracle.may_i_keep` is the
Python reference for it at 1.1 ms a call, and a client doing the same subtraction over the same
settled counters is not a second implementation of settling.

---

## Part 5 — What a screen needs beyond the counters

1. **Attribution per line** — region / area / this water / inherited, so "Region 4 says 1, this
   water says put it back" can be shown. `Source.scope` already carries it.
2. **The sentence from the book**, on every line. It is the only thing a reader can check.
3. **The closure's own card** — when it lifts, and who closed it. Never a table of noughts.
4. **Exemptions beside the closure they lift**, in the book's words, where the place they name
   cannot be drawn. See `05` Part 3 and the two traps in the export's
   `closures_and_exemptions`.
5. **What is NOT written** — a fish with no rule says so. A blank cell reads as "no limit", the
   most permissive failure available.
