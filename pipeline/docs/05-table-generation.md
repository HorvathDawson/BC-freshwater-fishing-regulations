# How a regulation table is generated

One worked example throughout: **Region 3 · streams**, the hardest standing table in the book —
a six-month closure over everything, a combined quota over three chars, and a different season
per member of that quota.

Every number here was measured on `redesign/stream-sections`, 2026-09-21, and can be
re-measured with the command beside it. Where a claim came from a design pass rather than the
running code it says **PROPOSED**.

---

## 1. The flow

```
data/curated/regulations/entries/catalogue/region-*.json      ← LLM parser (HUMAN-ONLY)
  11 files · 1,480 entries · 3,422 rules
        │
        ├─ pipeline/atlas/          decides WHICH SECTIONS a rule binds (reach, tributary walk)
        ▼
data/generated/bundle/bundle.sqlite         63.7 MB · 210,095 rulesets · 1,956,787 section_ruleset
        │                                   (the bundle DROPS `extents`; corpus re-joins them)
        ▼
pipeline/regs/table/corpus.py               rules() · catalogue() · rid() = "entry_id::rule_id"
        ▼
pipeline/regs/table/state.py                SELECTION kept apart from CONSTRUCTION
   rules_for(region | region+area | water,run)  ──►  build(rules, kind, here, label)
        │                                              │
        │                            ┌─────────────────┴─────────────────┐
        │                            ▼                                   ▼
        │                 build.ledger(rules,…)              method_build.table(rules,…)
        │                 what you may KEEP                  how you may FISH
        │                   ▼                                   ▼
        │                 ledger.py  Allowances settled       method.py  Terms settled
        │                 pairwise by Source.rank             (permit/ban · rig · keep)
        │                   ▼
        │                 rows.py  rows(L) → Row   (a derived VIEW, not a model unit)
        ▼
provenance.py / method_provenance.py   → display JSON
  ├─ pipeline/tools/emit_base_tables.py    → base.json   5.5 MB · 22 standing tables
  └─ pipeline/tools/emit_regs_v3_tables.py → <script id="t">  3.1 MB
                    oracle.py  may_i_keep(…)  ← the same counters; the acceptance test
```

Ownership: the **atlas** owns *where*; the **bundle** owns *what the corpus says*; `corpus.py`
owns *identity*; `state.py` owns *selection*; `build.py`/`method_build.py` own *settlement*;
`rows.py`/`provenance.py` own *presentation*; `oracle.py` owns *the decision*.

**`build()` takes a list of rule dicts and cannot ask which region it is looking at** — a test
fails if it ever has to (`test_a_table_is_a_function_of_the_rules_handed_to_it_and_nothing_else`).
That is what lets the table builder be lifted out of this project whole.

---

## 2. Precomputed versus live

| Artefact | Built offline | Client must do |
|---|---|---|
| 22 region standing tables | `emit_base_tables.collect()` → `base.json` | nothing |
| Dated views of a standing table | one per stretch (**53** total) | pick the stretch for today |
| Area states (MUs, parks) | one per named area | pick the area |
| Section ("this water") tables | `provenance.section(water, run)` | pick the day from each counter's `windows` |
| **Dated section views** | **not emitted** | apply the date itself |
| Weekday / time-of-day rules | never applied (`applies.within_day`) | apply, or show as a note |
| Creel arithmetic | impossible offline | call `oracle.may_i_keep` |
| Map colour | **not shipped** | see §9 |

The date primitive is `provenance.in_force(counter, on)`; one row as a day finds it is
`provenance.as_of(L, row, on)`. Neither re-resolves anything — `as_of` only chooses *which
settled counters the day can see*.

```
$ PYTHONPATH=. .venv/bin/python -c "from pipeline.regs.table import state as ST; \
  print(sum(len(ST.state(r,k).schedule()) for r in ST.REGIONS for k in ST.KINDS))"
53
```

---

## 3. Conditions: what selects a table

A region does not have a table. It has a table **per condition**, and there are two:

- **WHERE** — a region, a named area inside it (`MUs 1-1 to 1-6`, `National Parks`,
  `Ecological Reserves`, `Creston Valley Wildlife Management Area`), or a water and stretch.
- **WHEN** — a day.

`state.conditions(region, kind)` enumerates every `(area, stretch)` pair: **218** over the 22
region/kind tables. `rows.schedule(rows, also=gear_terms)` cuts the year — the signature is
every counter in force that day **on every row**, plus every gear term live, because a cap
coming into force changes the table without changing a number. It wraps at the year's end.

Whole-pipeline emit: **45 s**. A single `provenance.section`: **0.44 s** cold.

---

## 4. The two-stage model, and the invariant

`build.base(rules, kind)` is the region's standing table — only rules whose
`authority.Source.is_base` holds, i.e. `Scope.region`. `build.ledger(…)` overlays the rest.

`Source` is **two typed axes**: `Authority` (superior / province / region) × `Scope` (region /
area / water / inherited). `rank` is derived, never stored. A water-scoped rule therefore
**cannot enter a base by construction** — a single `rank` integer once let three Region 5 rules
written for one river bind all twenty Fraser stretches.

> **`section == apply(region base, named overrides)`**, every difference attributable to exactly
> one named override.

Enforced by `test_every_difference_from_the_standing_table_is_attributable_to_one_named_rule`
(>500 gear deltas, 0 unattributable), `test_an_area_is_the_region_plus_exactly_its_own_rules`,
and `python -m pipeline.regs.table.comply` / `.method_comply` → `COMPLIES, 0 unaccounted`.

Why it matters: a wrong number points at one named override, not at the pipeline.

---

## 5. Quota — the logic tree  **PROPOSED**

Input: a settled `Ledger` L, a water `kind`, optionally a date `on`. Output: one view per
origin. **No region is named anywhere.**

```
STEP 1  ORIGIN IS AN AXIS WITH TWO VALUES
        Origin.both is not a third value — it is the absence of the axis, and
        both.covers(wild) == both.covers(hatchery) == True, so running the pass
        twice needs no expansion step.
        V[WILD] := VIEW(L, WILD, on) ;  V[HATCHERY] := VIEW(L, HATCHERY, on)

STEP 2  WHAT BINDS      L.binds(a, sp, origin, None, on) — the ledger decides
        STEADY(a) := (a.applies.always or a.applies.unless) and not within_day
        on=None → only STEADY counters may state an answer; the rest become SEASONS
        on=date → a counter live that day is steady for that day; SEASONS is empty

STEP 3  THE HEADLINE, AND WHAT SURVIVES IT
        head[sp] := strictest daily, non-gate, non-clause, any-size counter
        A ZERO HEADLINE ANNIHILATES: under release or closed, a cap, a floor, an
        annual ceiling and a possession multiple are all true and all unactionable.
        Keep them in provenance; REMOVE THEM FROM THE DISPLAY MODEL.

STEP 4  LEAVES — fish the reader cannot tell apart
        key = (frozenset(identity(a) for a in shown), frozenset(identity(a) for a in seasons))
        identity(a) = (rule_id, outcome.kind, n, period, pooled, size.kind, size.lo, size.hi)
        NOT the Allowance object, and NOT the counter set including moot ones.

STEP 5  THE NESTING LATTICE, DERIVED
        a is a GROUP NODE iff  quota · pooled · daily · any-size · STEADY · |reach(a)| > 1
        `a.within` is NOT consulted — a clause ("1 bull trout or lake trout", inside the
        trout/char 5) is exactly the inner group. Excluding clauses is what loses the nesting.
        PARENT = the smallest strict superset among surviving reaches.
        It is a forest by construction: reaches are sets, containment is a partial order,
        and the family is laminar because every node is a species group or a clause of one.
        DROP a node whose reach equals a leaf (the leaf already says it).
        DROP a node where every leaf inside has a zero headline (it is moot).

STEP 6  SIZE, SIMPLIFIED — four tests, in order
        (8a) UNREACHABLE COUNT CAP  n >= head.n and reach(a) ⊂ reach(head) → DROP
        (8b) UNREACHABLE CLASS CAP  same, on a size class — ONLY when not pooled
        (8c) DUPLICATE STATEMENT    same (bound, lo, hi, n) → keep the WIDER reach
        (8d) NEVER drop a statement where a.pooled and reach(a) ⊃ this leaf's fish.
             A shared cap binds through the other members even when this line's own
             floor makes it look vacuous.

STEP 7  WHERE ORIGIN IS DRAWN            (see §7)
STEP 8  SEASONS — table-level / group-level / leaf-level by reach(s)
STEP 9  ORDER — every sort key a tuple of scalars; no set iteration order is used
```

**Determinism**: every decision reads only `Allowance` fields and `Ledger` methods. Same ledger
+ same date → same tree.

---

## 6. Gear — the logic tree  **PROPOSED**

One structural change, everything else follows from it:

> **The display unit is a PERIOD, not a year.** A period is a maximal run of days over which
> every displayable fact is constant. Inside a period nothing is conditional on a date and no
> two printed lines can contradict. The year view is the list of periods; the dated view is one
> period, pinned. Both are the *same* renderer.

```
A1  SELECT     state.rules_for(…) — the display must not re-derive selection
A2  SETTLE     method_build.table(rules, kind, here, label) → MethodTable
               closures come from the QUOTA ledger, so the two tables cannot disagree
A3  SEGMENT    fingerprint(day) = (closure, per method: standing, rig, keep)
               — rig is IN the fingerprint; today it is not, and Haida Gwaii's calendar
                 is a single Jan 1 – Dec 31 segment on a place whose bait rules flip Nov 1
A4  RE-SETTLE  per period, with _whenever() vacuous inside the period
A5  CLOSURE GATE  first, and it silences rig and keep entirely
A6  STANDING CONTEST  permit vs ban; on the province table a third answer, "depends where"
A7  RIG FOLDING   onto the row; an exception prints on the line it modifies, never as a line
A8  SPECIES LIMIT  compute the positive complement ("only non-game fish and burbot")
A9  HOIST      band = lines shared by ≥2 methods that are ALLOWED here, grouped by carrier set
A10 EMIT       one copy of each sentence; lines reference by id
```

---

## 7. The origin question, settled with numbers

Measured across all 22 standing tables — 42 species × 22 tables = **924 species-slots**:

| | count |
|---|---|
| slots whose answer differs between the wild and hatchery views | **38 (4.1 %)** |
| of those, inside `TROUT_CHAR` | **38 / 38** |
| of those, where the wild answer is `release` | **38 / 38** |

Paired answers: `release / 2` ×26, `release / 5` ×7, `release / 4` ×3, `release / (none)` ×2.

Only five rule families ever name an origin: `trout_quota` (R1), `trout_char_quota` (R2, R6),
`hg_quota` (Haida Gwaii), and the province's `steelhead.r1` / `.r2`. **Nothing outside trout and
char has ever been written with `wild` or `hatchery` in it**, so "only rules that explicitly
mention an origin cause a split" is free: `Subject.origin` defaults to `both`.

Table shapes:

| shape | tables | which |
|---|---|---|
| the two views are identical | **4** | 3 lake, 3 stream, 5 lake, 5 stream |
| they differ in the steelhead line only | **16** | province ×2, 1 lake, 1hg ×2, 2 lake, 4 ×2, 6 ×2, 7a ×2, 7b ×2, 8 ×2 |
| they differ across the family | **2** | 1 stream (7 of 15 fish), 2 stream (15 of 15) |

### The rule, procedural

```
for each group node g:
    if   g.answer(WILD) != g.answer(HATCHERY):   HOIST  — origin above the heading; two tables
    elif any member's headline differs:          SPLIT  — one table; those lines carry an origin
    else:                                        NONE   — one table, no origin control at all
```

**HOIST on 2, SPLIT on 16, NONE on 4.** No region is named; the test reads `Allowance.outcome`
and `Allowance.scope.origin` only.

Origin above the family is right **where the family number itself is an origin question** — and
that is Region 1 streams and Region 2 streams. Elsewhere it costs a whole screen of duplicated
rows to change one line, and on the 4 collapsing tables it would manufacture a distinction out
of a counter nobody can act on (Region 3's steelhead is `release` in both views from a rule that
does not mention origin; the only difference is a moot `10 per licence year` under a release).

**One risk that must be in the design:** the HOIST control hides a whole table. A reader landing
in the hatchery view of Region 2 streams and not noticing reads "2 a day" for a fish that must be
released. It must be a **required choice** phrased as the reader's own question — *"Is it a
hatchery fish? Look for a clipped adipose fin"* — not a silent tab.

---

## 8. Worked example — Region 3 · streams

`Table.schedule()` gives six stretches:
`Jan 1 – Jan 31 · Feb 1 – Jun 30 · Jul 1 – Jul 31 · Aug 1 – Oct 14 · Oct 15 – Oct 31 · Nov 1 – Dec 31`.

### Layout A — the year, closure as a banner

> ### ⚠ Every stream in Region 3 is closed **Jan 1 – Jun 30**
> *"Spring closure: No Fishing in any stream in Region 3 from Jan 1-June 30."*
> The table below is the answer for **Jul 1 – Dec 31**.

| I am fishing for | Jul 1 – Dec 31 |
|---|---|
| **▸ TROUT AND CHAR — 4 between them a day** *(8 in possession)* · only **1 over 50 cm** between them | |
| ⠀⠀9 kinds of trout and char | 4 (the shared 4) |
| ⠀⠀**▸ of which — 1 char between them**, and it **must be at least 60 cm** | |
| ⠀⠀⠀⠀Bull trout or Dolly Varden | 4, but only 1 char — **put it back Aug 1 – Oct 31** |
| ⠀⠀⠀⠀Lake trout | 4, but only 1 char — **put it back Oct 15 – Jan 31** |
| Steelhead | Put it back |
| Whitefish | 15 between them · 30 in possession |
| Burbot | 2 · 4 in possession |
| Bass · Yellow perch · Protected species | You may not fish for them |

### Layout B — date first, one table per stretch

> **Today is 21 Sep** → *Aug 1 – Oct 14*
>
> | I am fishing for | Today |
> |---|---|
> | ▸ Trout and char — 4 between them, 1 over 50 cm | |
> | ⠀⠀9 kinds of trout and char | 4 |
> | ⠀⠀Bull trout / Dolly Varden | **Put it back** |
> | ⠀⠀Lake trout | 4, only 1 char, at least 60 cm |
> | Steelhead · Kokanee · White sturgeon | Put it back |
> | Whitefish 15 · Burbot 2 · Crayfish 25 | |
> | Bass · Yellow perch · Protected | You may not fish for them |
>
> *[ show the whole year ]*

**Ship B, with A one tap away.** The argument, not the aesthetic:

1. **The reader's question has a date in it.** "Bull trout, what is my quota?" is unanswerable in
   Region 3 without one: `Row.calendar` gives `0` (Jan 1 – Jun 30), `4` (Jul), `release`
   (Aug 1 – Oct 31), `4` (Nov 1 – Dec 31). A makes the reader run that per line; B runs it once.
2. **The dominant fact is a closure and A buries it.** 151 of 366 days sit in `Feb 1 – Jun 30`
   where every line reads 0. At 400px the banner is off-screen by the third row, and a table
   printing "4 a day" for six months on a shut stream is wrong in the worst direction.
3. **Nesting costs horizontal space 400px does not have.** A's deepest line is three levels in.
   B usually collapses one: on Aug 1 – Oct 14 the bull trout is released, so the "1 char" node
   has one live member and STEP 5 drops it.
4. **What B costs:** the reader loses the year at a glance, and a reader planning a trip needs
   it. Hence A one tap away, and `Season` objects stay on the model in both — B is a *filter over
   the same tree*, not a second tree.

**The attractive wrong answer** was one row per fish with a season column. It fails because the
two chars **share one number and not one season**: a season column forces either two rows (and
the shared "1 char" prints twice — the five-times-the-legal-limit read) or one row with two
seasons and no way to say which fish each belongs to.

---

## 9. "This water", and map colouring

### This water

```python
from pipeline.regs.table import state as ST
t = ST.water_state("Fraser River", 18)     # → Table, through the SAME build()
```

`water_state` → `rules_for(kind=…, water=…, run=…)` → `corpus.section_rules` → `build(...)`.
Measured: run 18 → label `Region 5 – Region 7A boundary`, `here={'5','7a'}`, 13 rows, 8 stretches.

Differences from a region: the rule list carries `via` (`reach` or `trib` — a ladder rung, not
decoration); `here` can hold two regions at a boundary; `label` feeds `where.cut_for_this`; and
the ledger is base **+** overrides.

> ⚠ **The integration gap.** `corpus.py:103` and `build.py:31` both parse
> `app/design/regs-v3.html`'s `<script id="d">` block **at module import**. That block covers
> **22 waters / 102 stretches**, hand-chosen for the design prototype. The bundle holds
> **1,956,787** `section_ruleset` rows. Region tables are bundle-derived and complete; *this
> water* is gated behind a prototype file. The generalisation path exists —
> `section_ruleset`/`ruleset` already carry everything `corpus.section_rules` reconstructs,
> including `via` — but nobody has written it. **This is the single biggest thing between here
> and the app.**

### Map colouring

No `bundle.sqlite` column and no tile property carries status. The app computes it live:
`app/packages/core/src/status.ts::evaluate({rules, on, group})` →
`closed | restricted | open | unknown`, fed by `statusFor(ids, on, group)` — one SQL per chunk
plus O(rules) per section. Fine for a viewport, not for the province.

Two traps: a `take:0` carrying a **method** is not a closure (reading `take===0 &&
mayTarget===false` alone returned closed for **1,674 of 1,693** rulesets); and `evaluate`'s
four-state ladder is deliberately coarser than the `Ledger` — do not blend the vocabularies.

For cheap province-wide colouring the missing artefact is **`set_id × schedule-segment →
outcome`**. `set_id` is already interned (210,095 sets, collapsing to far fewer distinct
answers). It does not exist today.

### The oracle

`oracle.may_i_keep(ledger, fish, on, creel=None, name=None) -> Verdict`, **1.1 ms** per call on a
built ledger. `Verdict.kind ∈ {closed, release, gate, spent, ok, unlimited, unwritten}`, and each
`Check` carries the counter, `used` and `remaining`. It is the acceptance test because it decides
only through `Ledger.counters` / `binds`, and rows derive from the same counters —
`test_every_counter_the_oracle_decides_by_is_drawn_on_the_page` fails if a verdict could rest on
something no row shows.

---

## 10. What is wrong today — all verified against the running code

### Quota

| | measured |
|---|---|
| bands with no single answer (`answer: None`) | **8 of 20** — 4 lake/stream, 7a, 7b, 8 |
| tables drawing a trout/char fish outside any trout/char band | **16 of 22** |
| orphan entries (a trout/char fish with `band: null`) | **28**, 22 of them steelhead |

Root cause, one sentence: **`rows()` keys a row by its whole counter set, including counters
that are moot**, so a counter nobody can act on splits a group the reader sees as one.

Haida Gwaii is the clean illustration of STEP 6 working both ways on one rule:

```
1hg lake   : cap n=3 ("3 Dolly Varden") inside headline 5  →  CAN BIND     — keep it
1hg stream : cap n=3 ("3 Dolly Varden") inside headline 2  →  can never bind — drop it
```

Same rule, same code path, opposite outcome, decided by the data. The renderer currently patches
this; the **model** should drop it.

### Gear

| | measured |
|---|---|
| Haida Gwaii streams on **Dec 1** prints 4 bait lines — 3 of them permissions — beside `No bait` | ✅ |
| province spear: `verdict: "allowed"` while the governing term is stamped `"closed here by a stricter rule"` | ✅ both kinds |
| band names `set_lining` where that row reads *not allowed* | **20 of 22** |
| a rig rule printed on more than one row (all-or-nothing hoisting) | **3 tables** |

The Dec 1 case is subtle: passing the date into `rig(on)` makes `No bait` appear only in its
season, but does **not** make it suppress the year-round permissions beside it, because those are
in force every day. The fault is in `_settle_rig`, whose folding guard requires the folding rule
to be live *every day* the folded one is — which a seasonal ban can never satisfy. The book
settles it: `zp:bait::bait.r4` ends *"…as bait unless a bait ban applies."*

### The invariant to gate on

> **No two printed lines in a period may contradict.** For every pair of printed rig lines on one
> method in one period: not (`a.polarity != b.polarity` and `a.topic == b.topic` and `covers_rig`
> holds between them).

Assert it against the **rendered output**, not the input terms — `comply.py`'s header records
that an input-seeded check laundered failures three times in this project's history.

---

## 11. Gotchas for an integrator

1. **Never run the parser.** `run_parse.sh`, `pipeline.regs.parsing.dispatch` — spends credits.
2. **`rule_id` is unique only within an entry.** 408 rules share 140 ids; `species_quotas.r1` is
   nine different rules. Key on `corpus.rid(x)`.
3. **`section_id` / `dense_id` must never leave the bundle** — no URL, no pin, no API. Bind to
   `item_id`. Never cache a resolved section list across bundle versions (15 % of surviving items
   changed theirs in one rebuild).
4. **The bundle drops `extents`**; `corpus.catalogue()` re-joins them from the curated tree. The
   table layer needs the bundle *and* that tree on disk.
5. **An empty `spans` list binds no stretch** — reading it as "no filter" once shut all six
   Atnarko stretches and the Bella Coola with them.
6. **`tributaries_pending`** — 547 rules reach tributaries by a walk that does not exist scoped to
   a reach; nothing downstream may treat such a binding as complete.
7. **Weekday and time-of-day rules never decide a `(month, day)`.** The oracle surfaces them in
   `Verdict.notes`; the client must apply them.
8. **The repo PDF is not byte-identical to gov.bc.ca.** Claim equivalence, proved by
   `quota_print.cross_check` / `method_print.cross_check` — not identity.
9. **`rtk pytest` has reported a clean suite while tests failed.** Run gating suites unfiltered:
   `.venv/bin/python -m pytest -q`.
10. **Curation defects are fixed by curation**, never by string-matching in the client. See the
    open list in `HANDOFF-regs-tables.md`.
