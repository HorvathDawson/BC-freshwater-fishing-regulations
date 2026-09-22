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

> **Neither. The client looks the answer up.**

Building a table from rules means the settling ladder — carves, clauses, lifts, date-awareness,
the origin pass — in a second language. That is the failure this project keeps paying for: every
derivation that has existed twice has drifted, most recently a page reading `reaches` while the
pipeline had moved to `spends`, so a real fix never reached a reader. And there is no need: 2,375
answers is a precomputable set.

**Region base + amendments keeps its job, but that job is not resolution.** It is how the tables
are *verified* (§2.1 of `05`: a stretch is its base plus its own amendments, each attributable to
one named rule) and how they are *displayed* — a reader needs "Region 4 allows 1 a day; this
water says put it back", and every allowance already carries the `Source` that says which is
which. Ship the split as **attribution on a settled line**, not as work for the client.

---

## Part 2 — Two indexes on `set_id`

### 2.1 The colour index — for the map

```
colour[set_id][stretch] -> "closed" | "restricted" | "open"
```

**~6,800 rows.** Drawing the province is then: `section → set_id → today's stretch → colour`.
An integer lookup per section, no rule touched, no date arithmetic beyond picking the stretch.

This is the missing piece `05` Part 8 names. Today `core/status.ts::evaluate({rules, on, group})`
runs over the rules of every section in the viewport — fine for a viewport, impossible for a
province.

**The colour test, and the trap.** A `take: 0` alone is not a closure — it is a closure, *or* a
release, *or* a size floor, *or* a restriction on one method. Reading it as a closure called
**1,674 of 1,693** rulesets closed. The test that holds:

| colour | test |
|---|---|
| **closed** | a live allowance of zero on *all game fish*, carrying no method — `ledger.shuts_the_water` |
| **restricted** | not closed, but nothing may be kept |
| **open** | something may be kept |

Sampled over 60 rulesets / 163 stretches (12,782 sections): **93 open · 60 closed · 10
restricted**. Indicative only — that is 0.7 % of the province.

A first attempt used "any fish shut → amber" and made the entire province amber, because the
protected species are shut everywhere. **The colour is about the water, not about a fish.**

### 2.2 The table index — for the screen

```
table[set_id][stretch] -> the settled table, with attribution per line
```

**~6,800 tables**, from **2,375** ledgers at **78 ms** each — **about three minutes** to build
the province offline, on one core, today.

A stretch of the year averages **2.9** per rule set (max 7 in the sample), so the date is picked,
never computed.

---

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
| settle rules into a table | **yes** | never |
| decide a colour | **yes** | looks it up |
| cut the year into stretches | **yes** | picks today's |
| merge sections into runs | **yes** | draws them |
| creel arithmetic ("I already have a bull trout") | impossible | `oracle.may_i_keep`, 1.1 ms |
| weekday and time-of-day rules | impossible | applies them — they never resolve to a date |

Two rows there are honest exceptions, and they are exceptions for the same reason: they depend on
something only the person holding the rod knows. Everything else is a lookup.

---

## Part 5 — What a screen needs that a settled table alone does not carry

1. **Attribution per line** — region / area / this water / inherited, so "Region 4 says 1, this
   water says put it back" can be shown. `Source.scope` already carries it.
2. **The sentence from the book**, on every line. It is the only thing a reader can check.
3. **The closure's own card** — when it lifts, and who closed it. Never a table of noughts.
4. **Exemptions beside the closure they lift**, in the book's words, where the place they name
   cannot be drawn. See `05` Part 3 and the two traps in the export's
   `closures_and_exemptions`.
5. **What is NOT written** — a fish with no rule says so. A blank cell reads as "no limit", the
   most permissive failure available.
