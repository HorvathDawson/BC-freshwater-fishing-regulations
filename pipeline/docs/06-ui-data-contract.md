# What the UI is handed, and what it is never asked to work out

Companion to `05-table-generation.md`, which is about *building* a table. This is about what
crosses the line into the client.

Numbers in Parts 1, 3 and 6 are measured against the shipped bundle (2026-09-23). The Part 2
figures were measured with the settling layer before it was removed, and say so.

---

## Part 1 — The one fact the whole design rests on

| | |
|---|---|
| sections carrying a rule | **1,956,787** |
| distinct **rule sets** among them | **2,288** |
| rules in a set | 1 – 119, **75 on average** |

Nearly two million sections have **two thousand two hundred and eighty-eight** different
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

> **Not yet, though.** The settling layer has been removed from the repository until it is
> rebuilt, so what ships today is the left half only: the records, with their provenance and a
> guide to reading them (Part 6). This section is the target, not a description of
> `ui-rules-export.json` as it stands.

The proof that a settled ledger is general enough: **three different renderings already run off
one**, in this repository, today.

| consumer | what it made of the same ledger |
|---|---|
| `rows.py` | one line per species-that-answer-alike |
| `display.py` | the nested budget tree — shared numbers drawn once, members hanging off them |
| `oracle.py` | "I have a 55 cm bull trout and a rainbow in the creel — may I keep this?" |

None was privileged and none settled anything. A fourth would have been a fourth consumer, not a
change to the data.

> **All three are gone**, with the settling layer itself, to be rebuilt properly. They are listed
> because they are the evidence for the claim above: three renderings ran off one settled ledger,
> so the split between settling and rendering is a demonstrated fact rather than a design hope.
> What the repository holds today is the left-hand column of the table above and nothing else —
> the rules, and which rules apply where.

### What crosses the line

| | size | what it is |
|---|---|---|
| **the records** | **2.9 MB** of rules (3,269) + 0.1 MB of licensing (106) | exactly as the bundle ships them, with `label`, `verbatim` and provenance — Part 6. The front end needs these anyway: the sentence from the book is the one thing a reader can check. |
| **the settling verdict** | **3.9 KB** per (set, stretch), **~29 MB** for all 7,105 (measured before removal) | per counter: does it bind today, what carved it, what it is a clause of, what it comes to. It references rule ids; it does not repeat rules. **Not built today** — this is what the rebuilt layer owes. |
| **`section → set_id`** | ~3.9 MB packed | 1,956,787 × a 2-byte id |
| **the colour index** | ~7,105 rows | Part 2.2. **Not built today** — it needs a settled ledger to know a closure from a caveat. |

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

**Measured over the 2,375 rule sets of 2026-09-22** (7,105 (set, stretch) pairs, 162 s to
settle), with the settling layer that has since been removed:

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

Measured over the 22 waters in `sections.json`: **277 sections → 102 runs, 2.7×.**

| water | sections | runs | |
|---|---|---|---|
| Fraser River | 90 | 20 | 4.5× |
| Skeena River | 40 | 9 | 4.4× |
| Kootenay River | 32 | 9 | 3.6× |
| Harrison River | 5 | 1 | 5.0× |
| Stamp River | 5 | 5 | 1.0× — every section differs |
| *(lakes)* | 1 | 1 | a lake is one run |

`pipeline/tools/build_section_data.py` already does this for 22 sample waters (every
regulation fact from the bundle; lake geometry still from FWA and the curated lake parts), and
`app/design/regs-v3.html` is the
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
   cannot be drawn. See `05` Part 3 and the export's `guide.exempts`.
5. **What is NOT written** — a fish with no rule says so. A blank cell reads as "no limit", the
   most permissive failure available.

---

## Part 6 — What ships today: `ui-rules-export.json`

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.tools.export_ui_rules [--bundle B] [--out OUT]

Written to `data/generated/regs/ui-rules-export.json` (not tracked). Everything is read from
`bundle.sqlite`, and nothing is sampled or settled.

| key | what | size (indent 1) |
|---|---|---|
| `about` | the bundle's build digests, computed counts, and `unresolved_references` — corpus references that do not resolve | — |
| `guide` | how to read everything below; its `contents` is the table of contents | 0.1 MB |
| `field_dictionary` | every key the records carry, and what it means | — |
| `species` | every fish and every group the book writes (open groups have no member list on purpose) | — |
| `licences` | the document register | — |
| `entries` | all 1,480 synopsis rows: kind (province / zone / area / water), printed passage, matched waters, their rule and licensing ids | 0.9 MB |
| `rules` | all 3,269 rules, keyed `entry_id::rule_id` | 2.9 MB |
| `licensing` | all 106 licensing records, keyed `entry_id#record_id` | 0.1 MB |
| `rulesets` / `licensing_sets` | the bundle's interned sets: members grouped by `via`, and how many sections carry each | 8.0 MB |
| `waters` | every named water by `item_id`: its entries and the sets its sections carry, with section counts | 3.3 MB |
| `index` | ids grouped by type, family and kind | 0.3 MB |

About **16 MB** in all. Membership is per SET and per named WATER, never per section: section
handles never leave the bundle (AGENTS 5). Set ids are local to one build.

**Every record reads itself.** A rule or licensing record carries its generated `label`, its
`verbatim`, its `fields` exactly as the bundle ships them (by alias, empty values left out), and
`provenance` — for a rule: entry, authority, `binds_to`, `rank`, the bundle's `scope`, and
`uncertain` with the reach builder's reason; for a licensing record: `placement`, and `uncertain`
with its reason. Nothing derived that hides reasoning is added: no "reads as", no settled table.

**The guide cannot drift from the code.** Its lists of types, families, slots, clause fields,
conduct acts, `Who` axes, `Doing` acts, path fields and licensing kinds are generated from the
model's registries, and `problems()` refuses the export when a registry member has no words or
the words name a member the model no longer has. Every example is a live record found by a test
over the data, never a remembered id.

**Refused, not worked around.** The export refuses a bundle that lacks `entry.matched` or
`rule.unresolved`, and refuses to write when any key in the output is a retired field name
(`RETIRED_ANYWHERE`, and `RETIRED_ON_RULE` for names current elsewhere, such as `method` in a
gear clause's `when`). `pipeline/tests/test_export_ui_rules.py` pins both with mutations.

## Part 7 — What ships today: `status_index.bin` (the colour index, built)

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.deliver.status_index [--bundle B] [--out OUT]

Default out `data/generated/bundle/status_index.bin`, beside `bundle.sqlite`; ~55 s, read from
the bundle only, deterministic. The dev server serves it at `/status_index.bin`
(`app/tools/serve-tiles.mjs`; `STATUS_INDEX=<file>` serves a side build) and the app fetches it
next to `atlas.pmtiles`. It is Part 2.2 made real: per SECTION and per DAY, three answers.

| status | decided as |
|---|---|
| **closed** | on that day, for EVERY game fish (p.86's list minus crayfish), `read.effective_rules` returns a rule that *speaks* and is an unconditional closure — take 0, may not fish for it, no length / origin / `while` / target / `side`, not partly lifted. A "beside" closure (hours, weekdays, unreadable season, one half of the channel) and a not-yet-mapped note never close. A closure of some species only is not `closed`. |
| **own** | not closed, and a WATER TABLE's row (`r<n>:` — a named water, a cut piece, an area row such as the CVWMA waters or the Liard watershed, or such a row reaching it by the tributary walk) binds the section. Independent of the day. |
| **base** | neither: only zone / provincial / superior tables bind it. **Not in the file** — absence means base. |

The Part 2.2 draft keyed "this water" on `scope` water/inherited; the index keys on the TABLE
(row vs zone), because a zone table's rule that names waters (the Skeena/Nass winter closure,
the white sturgeon licence waters) is still the base — 181k extra sections read "own" otherwise.
`tidal` (Nitinat) and `outside` (past the border) carry their own codes and read as no status.

A WATER (`item_id`) is rolled up from its parts: closed only when every part is closed, own when
any part has a row, base otherwise (a zone closure on part of a row-less water is the base).

**Format** (spec in the module docstring; decoder `app/packages/core/src/statusIndex.ts`):
magic `BCSI`, version, the 8-byte `section_handles` digest, ~112 shared 366-day range tables
(Feb 29 is day 60 every year; a season over New Year is two runs), then sections as runs of
consecutive handles in three varint columns (gap, count, table), then waters as front-coded
`item_id`s with a table each. Section keys are the tiles' feature ids (`section_id` = the bundle
`sid`), so colouring needs no bundle query; the app refuses a file whose digest is not its
bundle's, exactly as it refuses a mixed tiles/bundle pair — handles never leave that one set.

**Measured 2026-10-01** (bundle `147b20dce7d8576c`): 891,384 of 1,958,036 sections listed
(607,188 closed on some day, 576,101 own on some day), 10,478 of 19,754 waters; **1.64 MB raw,
421 KB gzip -9, 360 KB brotli**; Node decode 22 ms, then O(1) per lookup.

**Held to the reader**: `pipeline/tests/test_status_index.py -m slow` asks `effective_rules`
directly for 6,000+ (section, date) pairs — listed and absent sections, every table's boundary
days, Dec 31 / Jan 1, Feb 29 — and requires agreement; checks every ruleset an unlisted section
carries binds no row and reads base across the year; checks the digest; and shows the check fails
on a swapped table. The builder goes through `read.effective_rules_bound` (the same code
`effective_rules` runs, with a section's bindings in hand) once per (ruleset, steelhead) and once
per distinct reading of the rules' dates.
