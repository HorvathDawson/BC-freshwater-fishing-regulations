# How a regulation table is generated

Read this in order. Every stage is explained by following **one real rule** all the way
through, and every step of the logic tree has a worked example beside it.

The rule we follow is Region 2's, printed in the synopsis as **"2 from streams (must be
hatchery)"**.

---

## Part 1 — The flow

### 1.1 The five stages

```
  ①  CURATED ENTRY      a human-checked sentence from the printed book
           ↓
  ②  BUNDLE             that sentence, plus WHICH WATERS it binds
           ↓
  ③  RULE               one flat dict the table layer reads
           ↓
  ④  ALLOWANCE          the sentence as a COUNTER (a budget you spend)
           ↓
  ⑤  LEDGER             every counter here, settled against every other
           ↓
  ⑥  ROW  →  TABLE      what you actually see
```

### 1.2 The same rule, at each stage

**① Curated entry** — `data/curated/.../region-2.json`, checked by a human against the book:

```
verbatim : "2 from streams (must be hatchery)"
```

**② Bundle** — `data/generated/bundle/bundle.sqlite`. The atlas works out which stretches of
which rivers this reaches, and stores the sentence once with a list of sections pointing at it.

**③ Rule** — what `corpus.rules()` hands the table layer. It is a flat dict; **sizes are
fields on it like any other**, so nothing downstream parses prose:

| field | our rule `r4` | a sized sibling `r2` | a floor `r8` |
|---|---|---|---|
| `verbatim` | "2 from streams (must be hatchery)" | "1 over 50 cm" | "Hatchery trout/char under 30 cm from streams" |
| `species` | `["TROUT_CHAR"]` | `["TROUT_CHAR"]` | `["TROUT_CHAR"]` |
| `origin` | `hatchery` | — | `hatchery` |
| `water` | `stream` | — | `stream` |
| `take` | `2` | `1` | `0` |
| `over_cm` | — | **`50`** | — |
| `under_cm` | — | — | **`30`** |

Read the last three rows together and every kind of size statement in the book is one pair:

| `take` | `over_cm` | `under_cm` | means |
|---|---|---|---|
| 2 | — | — | keep 2, any size |
| 1 | 50 | — | of those, only 1 may be **over 50 cm** — a *cap on a size class* |
| 0 | — | 30 | you may keep **none under 30 cm** — a *floor* |
| 0 | 50 | — | none **over** 50 cm — a ceiling |

A floor is just an allowance of **zero** on a size class. That is why closures, releases and
size limits all travel as one kind of object: they are all "you may keep N of this class", and
for a floor N is nought.

**④ Allowance** — the same fact as a counter you spend:

```
scope   : 15 trout and char · hatchery · any size
outcome : 2 per day, POOLED  (2 between all of them, not 2 each)
source   : Region 2 · region-wide
```

**⑤ Ledger** — it meets the other rules in the chapter and they sort themselves out:

| rule | says | what happens to it |
|---|---|---|
| `r1` "Trout/char: 4" | 4 per day | **replaced on streams** by `r4` — `r4` is its stream clause |
| `r4` "2 from streams (must be hatchery)" | 2 per day, hatchery | **governs** |
| `r6` "Wild trout/char from streams" | release, wild | **governs the wild side** |
| `r8` "Hatchery trout/char under 30 cm" | nothing under 30 cm | a size floor on `r4` |
| `r2` "1 over 50 cm" | 1 big one, pooled | a cap inside `r4` |
| `r5` "1 char (bull trout, Dolly Varden, or lake trout)" | 1 of those three | a cap inside `r4` |
| `r3` "2 hatchery steelhead over 50 cm allowed" | 2 big steelhead | **carves steelhead out of `r2`** |

Nobody wrote that table. It falls out of **`Source.rank`**, and rank is not stored anywhere —
it is computed from two independent facts about every rule.

**Axis 1 — who wrote it** (`Authority`): `superior` (federal, national parks, ecological
reserves) · `province` · `region`.

**Axis 2 — what it binds to** (`Scope`): `region` (the whole region or province) · `area` (a
named place inside it) · `water` (one named water or a cut piece) · `inherited` (reached from a
downstream water by the tributary walk).

The rank is a single number, and **smaller speaks first**:

| rank | rule is… | example |
|---|---|---|
| **−1** | any `superior` authority | "Fishing is prohibited in Ecological Reserves" |
| **0** | scope = `water` | a rule written for the Chilliwack |
| **1** | scope = `inherited` | a rule reaching here from the river downstream |
| **2** | scope = `area` | "MUs 1-1 to 1-6, no fishing Jul 15 – Aug 31" |
| **3** | scope = `region`, written by a **region** | "Trout/char: 4" in Region 2's chapter |
| **4** | scope = `region`, written by the **province** | "All wild steelhead must be released" |

Two consequences worth stating out loud:

- **Scope beats authority.** A province-authored rule written for one lake (rank 0) speaks on
  that lake before the region's standing table (rank 3). "This water overrides regional" is a
  statement about *what a rule binds to*, not who wrote it.
- **A superior authority is outside the ladder.** At −1 nothing below can open what it closed —
  which is why a national park closure cannot be lifted by a regional quota.

This replaced a single hand-set `rank` integer. Conflating the two axes was the root of a whole
family of bugs where a rule written for one named river bound every water in the region.

**⑥ Row → table** — the reader sees:

```
Hatchery trout and char    2 a day, between them
                           at least 30 cm · only 1 over 50 cm
    of which bull trout, Dolly Varden or lake trout   only 1, at least 60 cm
    steelhead                                          2 may be over 50 cm, 10 a year
Wild trout and char        Put it back
```

### 1.3 The one thing to remember

> **The builder takes a list of rules and nothing else.**
> `build(rules, kind, here, label)` cannot ask which region it is looking at — a test fails if
> it ever does. Choosing *which* rules is a separate job (`rules_for`). That is why a region, an
> area inside a region, and one stretch of one river all go through the same code.

---

## Part 2 — What picks a table

A region does not have *a* table. It has one per **condition**, and there are two conditions.

**WHERE** — and it is a **two-step lookup, not three alternatives**:

| step | you get | how many there are |
|---|---|---|
| 1. the **base** — a region, and an area inside it if you are in one | a whole settled table | 22 regions × kind, plus 5 areas |
| 2. the **amendments** — the rules written for this water and stretch | a short named list | 599 water rules + 62 inherited, over 102 stretches |

> **A stretch is its base table plus its own amendments, and nothing else.**
> So the verification order is: get the region and area tables perfect — there are only 22 of
> them and they are checked line by line against the printed synopsis — and then a stretch can
> only be wrong in its own short list of amendments, each attributable to one named rule.

**WHEN** — a day.

### 2.1 This is a correctness fix, not just tidier

The code does not do this today. `build.base()` re-derives the base **from whatever rules the
atlas happened to bind to that stretch**, rather than looking it up by region. Measured over the
100 single-region stretches that ship:

| | |
|---|---|
| stretches whose base matches their region's standing base | **37** |
| stretches whose base **differs** | **63** |
| distinct region-wide rules missing from some stretch's base | **18** (98 omissions) |
| rules in a stretch's base that its region does *not* have | **0** |

The difference is entirely one-directional: a stretch can only **lose** region-wide rules,
never gain them. Most often missing:

| times | rule |
|---|---|
| 23 | `z4:invasive_species_notice::invasive_species_notice.r1` |
| 16 | `z6:trout_char_quota::trout_char_quota.r11` |
| 11 | `z2:species_quotas::species_quotas.r1` |
| 11 | `z2:protected_species::protected_species.r1` |
| 11 | `z5:bass_illegal::bass_illegal.r1` |

Building each stretch the proposed way — region base **+** that stretch's own overrides —
**changes the answer on 23 of the 100 stretches**. The Fraser's Region 2 stretches gain a bass
row (20 a day) they do not have today.

Not every omission is visible: the Fraser keeps a *Protected species* row because the
**provincial** protected-species rule still binds, even though Region 2's own is missing. That
is the point — today the table is right by luck of a second rule, and nothing tells you which
lines are standing on their own base and which are not.

`state.conditions(region, kind)` lists every combination: **218** across the 22 region/kind
tables. The year is cut into **53 stretches** total — a stretch is a run of days over which
nothing changes.

Region 3's streams have six:

```
Jan 1 ──── Jun 30 │ Jul 1 ─ Jul 31 │ Aug 1 ─ Oct 14 │ Oct 15 ─ Oct 31 │ Nov 1 ─ Dec 31
   everything                          bull trout &      + lake trout      back to
     closed                            Dolly Varden       released           normal
                                        released
```

---

## Part 3 — Closures

**If the water is shut, do not print a table.** A table of twelve rows all reading "No fishing"
is the same sentence twelve times, and a reader who scrolls past a banner reads a limit on a
closed river.

Instead:

```
┌────────────────────────────────────────────┐
│  ⛔  CLOSED                                 │
│                                            │
│  Every stream in Region 3 is closed        │
│  until 30 June — 87 days from today.       │
│                                            │
│  ▸ why   "Spring closure: No Fishing in    │
│          any stream in Region 3 from       │
│          Jan 1-June 30."                   │
│          Region 3 · region-wide            │
│                                            │
│  ▸ see the rules that apply from 1 July    │
└────────────────────────────────────────────┘
```

Three requirements:

1. **Say when it opens**, not just that it is closed.
2. **Carry the provenance** — the sentence and who wrote it — because that is the one thing a
   reader can check against the printed book.
3. **A closure must be liftable.** The book writes exemptions ("see water specific regulations
   table for exceptions"), and a water rule can lift a regional closure. The model already
   supports this: a lift is an `Allowance` like any other and `Ledger.lifted` records it. So the
   closure card is produced by *asking the ledger*, never by a flag — if a water's own rules lift
   the spring closure, that water simply has no closed stretch and gets a normal table.

---

## Part 4 — The logic tree, with examples

Input: a settled `Ledger`. Output: the display tree. **No region is ever named.**

### Step 1 — Origin has two values, not three

`Origin.both` is not a third value; it is the *absence* of the question. Run the whole pass
twice, once as wild and once as hatchery — a `both` rule answers True to both, so nothing needs
expanding.

```
Example   r4 "2 from streams (must be hatchery)"  → answers only the hatchery pass
          r6 "Wild trout/char from streams"       → answers only the wild pass
          "Whitefish: 15"  (no origin)            → answers both, identically
```

### Step 2 — A zero headline annihilates

Under *"put it back"* or *"no fishing"*, a size limit, a cap, an annual ceiling and a possession
multiple are all still true and all unspendable. **Drop them from the display.**

```
Region 2 streams, WILD bull trout — what the model holds today:
    release                       ← the answer
    only 1 char between them       ← true, unspendable
    must be at least 60 cm         ← true, unspendable

  what the reader should see:     Put it back
```

This one step fixes **8 of 20** bands that currently have no single answer, **28** orphan
entries, and **16 of 22** tables that draw a trout-or-char outside the trout-and-char group.

### Step 3 — Fish the reader cannot tell apart become one line

Group species by *what binds them*, not by their identity.

```
Region 2 streams, WILD:  all 15 trout and char have exactly one live counter — release.
                         → ONE line, not three.
Today they are three lines, because the char carry a dead "1 char" cap.
```

### Step 4 — Groups nest, and the nesting is derived

A counter is a **group** when it is a pooled daily quota over more than one fish. The parent of
a group is the smallest group that strictly contains it.

```
Region 2 streams, HATCHERY:

  15 trout and char ── 2 a day, pooled ────────────────── r4   ← group
        └── bull trout, Dolly Varden, lake trout ─ 1 ──── r5   ← group inside it
        └── steelhead ─────────────────────────── 2 >50 ─ r3   ← its own cap

  Why r5 is a group and not a footnote: it is pooled and it reaches 3 fish.
  Nothing hand-codes "trout and char" — the containment of the species sets does it.
```

**Drop a group** when its reach equals a single line (the line already says it), or when every
fish inside it is released (nothing to count).

### Step 5 — Sizes, simplified in a fixed order

```
(a) UNREACHABLE COUNT CAP     cap ≥ the number it sits inside     → drop
      Haida Gwaii streams: "3 Dolly Varden" inside a headline of 2.
      You cannot keep 3 of a fish you may keep 2 of.
      Same rule on Haida Gwaii LAKES, inside a headline of 5 → kept.
      Same code, opposite outcome, decided by the data.

(b) UNREACHABLE CLASS CAP     same test on a size class          → drop
      only when the cap is NOT shared with other fish.

(c) DUPLICATE                 same bound, same number            → keep the wider one
      Region 6 prints "only 1 over 50 cm" AND "only 1 over 50 cm between them".

(d) NEVER drop a shared cap.  If the cap is pooled and reaches fish beyond this line,
    it binds through them even when this line's own floor makes it look pointless.
      Region 2: bull trout must be ≥60 cm, so "only 1 over 50 cm" looks redundant —
      but keeping that bull trout spends the whole family's single over-50 fish,
      and a rainbow over 50 then cannot be kept. 47 of 88 size statements are this kind.
```

### Step 6 — Where the origin control goes

```
for each group:
    its own number differs wild vs hatchery   → the whole table splits in two
    only some members differ                   → one table; those lines carry wild/hatchery
    nothing differs                            → one table, no origin control at all
```

Measured across 22 tables: **2 split in two** (Region 1 streams, Region 2 streams) · **16 differ
in the steelhead line only** · **4 are identical**.

### Step 7 — Seasons attach where they reach

```
reaches every fish in the table  → one banner above the table
reaches every fish in a group    → on the group
otherwise                        → on the line, labelled with the fish it names

Region 3 streams: bull trout & Dolly Varden go back Aug 1 – Oct 31; lake trout Oct 15 – Jan 31.
They share ONE number and do NOT share a season, so the season sits on the member, never
on the group — a season shown against a fish it does not name closes a legal fishery.
```

---

## Part 5 — Layouts

All layouts show the **same table**: Region 2 · streams, the hardest one — a wild/hatchery split
across the whole family, a shared number, a size floor, a shared big-fish cap, a tighter cap on
three chars, a steelhead exception to that cap, and an annual limit. **Every figure is real.**

Three rules learned from the last round, applied to all of them:

1. **One statement per row.** `at least 30 cm · only 1 over 50 cm` in a single cell is two rules
   pretending to be one. They get a row each.
2. **Never say "the shared 2".** A member row should not restate the group's number at all — the
   group owns it, and the member row carries only what is **extra**.
3. **The annual limit is not a column.** Measured: **steelhead is the only fish in the book with
   one**, on all 22 tables, from one provincial rule (`zp:steelhead.r1`, 10 a year). A column
   that is empty on every row but one is noise; it belongs on the steelhead line.

---

### Option 3 — One table, origin only where it changes the answer  ⭐

**REGION 2 · STREAMS** — 1 July – 31 Dec

#### Trout and char · **hatchery** — keep **2 a day**, 4 in possession

*between all 15 kinds — not 2 of each*

| These apply to every one of them | |
|---|---|
| Minimum size | **30 cm** |
| Of your 2, how many may be over 50 cm | **1** |

| I am fishing for | On top of that |
|---|---|
| 9 kinds of trout and char | nothing more |
| Bull trout, Dolly Varden or lake trout | only **1** of your 2 may be one of these · minimum **60 cm** |
| Steelhead — **10 a licence year** | **2** of your 2 may be over 50 cm, not 1 |

#### Trout and char · **wild** — **put it back**

*all 15 kinds, every size*

#### Everything else

| I am fishing for | Size | Per day | In possession |
|---|---|---|---|
| Whitefish | any | 15 between them | 30 |
| Bass | any | 20 between them | 40 |
| Black crappie | any | 20 | 40 |
| Crayfish | any | 25 | 50 |
| Kokanee | — | Put it back | — |
| White sturgeon | — | Put it back | — |
| Protected species (12 kinds) | — | You may not fish for them | — |

**What changed** — the number is stated **once**, by the group, and never restated; each size
statement is its own row with a plain label; "the shared 2" is gone entirely, replaced by *"on
top of that"*, which is what a member row actually carries; the annual limit sits on the
steelhead line. The steelhead exception now reads as an exception (*"2 of your 2 … not 1"*)
instead of as a number that quietly contradicts the row above it.

**Still against it** — the wild block sits below the hatchery block, so a wild-fish angler reads
past detail that does not apply to them. On the 16 tables where only steelhead differs, the two
blocks collapse and this is not an issue.

---

### Option 4 — Look up one fish  ⭐

🔍 `bull trout`

| **BULL TROUT** — Region 2 streams, 1 July – 31 Dec | |
|---|---|
| **Wild** | **Put it back** |
| **Hatchery** | **Keep 1** · minimum 60 cm |

| That 1 also spends | shared with |
|---|---|
| 1 of only **3 chars** a day | Dolly Varden, lake trout |
| 1 of your **2 trout and char** a day | all 15 kinds |
| your **1 fish over 50 cm** a day | all 15 kinds except steelhead |
| 1 of **4 in possession** | all 15 kinds |

**For** — answers the real question in one screen, and states the shared budgets as budgets
instead of implying them with an indent, which is the thing nesting communicates worst.
**Against** — you must know what you are looking for; poor for browsing, and poor for checking a
chapter against the printed book.

---

### Option 5 *(new)* — One row per limit

No nesting and no groups. **Every row is a limit**, and the "applies to" column carries what the
indentation used to. Every row has exactly the same shape.

**REGION 2 · STREAMS · hatchery** — 1 July – 31 Dec

| Limit | How many | Applies to |
|---|---|---|
| Per day | **2** | all 15 trout and char, between them |
| In possession | **4** | all 15 trout and char, between them |
| Minimum size | **30 cm** | all 15 trout and char |
| Of those, over 50 cm | **1** | all trout and char **except steelhead** |
| Of those, over 50 cm | **2** | steelhead |
| Of those, chars | **1** | bull trout, Dolly Varden, lake trout |
| Minimum size | **60 cm** | bull trout, Dolly Varden, lake trout |
| Per licence year | **10** | steelhead |

**REGION 2 · STREAMS · wild** — put back every trout and char, every size.

**For** — the steelhead exception becomes two adjacent rows that obviously disagree on purpose,
which is far clearer than a footnote on a nested line; nothing is ever restated; one shape at
any width. **Against** — it inverts the reader's question. They arrive asking about a *fish* and
this is a list of *limits*, so they must scan the right-hand column to assemble their own answer.
Best as the **"show me the rules"** view behind Option 3, or as the reviewer's view for checking
against the book — where it is genuinely the best of the four, because each printed sentence maps
to exactly one row.

---

### Option 6 *(new)* — The decision ladder

The oracle, rendered. The reader answers about the fish in their hand and each step is a rule
with its own provenance.

> **I have a bull trout · 47 cm · hatchery** — Region 2 stream, 12 September

| | check | answer | |
|---|---|---|---|
| 1 | Is it wild? | no | ✅ *(every wild trout and char goes back here)* |
| 2 | Is it at least 30 cm? | 47 cm | ✅ |
| 3 | Bull trout, Dolly Varden and lake trout must be 60 cm | 47 cm | ❌ |

> ### Put it back
> *"1 char (bull trout, Dolly Varden, or lake trout)" — Region 2 · region-wide*

And when it passes, the same ladder becomes the budget check:

> **I have a rainbow trout · 38 cm · hatchery** — and 1 trout already in the creel

| | check | answer | |
|---|---|---|---|
| 1 | Is it wild? | no | ✅ |
| 2 | At least 30 cm? | 38 cm | ✅ |
| 3 | Over 50 cm? | no — the 1-over-50 slot is untouched | ✅ |
| 4 | Trout and char today | 1 of 2 used | ✅ **1 left** |

> ### You may keep it — your last one today

**For** — this is the only layout that answers *"may I keep **this** one"* rather than *"what are
the rules"*, and it is exactly what `oracle.may_i_keep` already computes (1.1 ms, and every check
carries `used` and `remaining`). Perfect for mobile: one question per line, no table at all.
**Against** — it answers about one fish at a time and cannot show the chapter; it needs input
(species, length, origin, what is already in the creel) that a browsing reader has not given.

---

### How they fit together

| view | answers | when |
|---|---|---|
| **Option 3** | "what are the rules here" | the default — browsing, planning |
| **Option 4** | "I am fishing for X" | tap a fish |
| **Option 5** | "show me the rules themselves" | the reviewer's view, and the check against the book |
| **Option 6** | "may I keep **this** one" | fish in hand |

They are four renderings of **one** tree, not four data models — Option 5 is the tree's
constraints listed flat, Option 4 is one leaf with its ancestors, and Option 6 is the same
counters walked in order. Nothing extra has to be computed for any of them.

---

## Part 6 — Where this connects to the app

### This water

```python
from pipeline.regs.table import state as ST
t = ST.water_state("Fraser River", 18)     # the SAME builder as a region
```

> ⚠ **The gap that matters.** `corpus.py:103` and `build.py:31` both parse
> `app/design/regs-v3.html` at import. That file covers **22 waters / 102 stretches**,
> hand-picked for the prototype. The bundle holds **1,956,787** section-to-rule rows. Region
> tables are complete and bundle-derived; *this water* is not. Everything needed to generalise
> is already in `section_ruleset` / `ruleset`, including how a rule reached the water (`via`).
> Nobody has written it. **This is the single biggest thing between here and the app.**

### What is precomputed

| | offline | the client does |
|---|---|---|
| 22 region tables | yes | nothing |
| 53 dated stretches | yes | pick today's |
| areas (MUs, parks) | yes | pick the area |
| section tables | yes | pick the day itself |
| dated section views | **no** | apply the date |
| creel arithmetic | impossible | call the oracle |
| map colour | **no** | see below |

### Map colouring

Nothing precomputed. The app computes it live —
`core/status.ts::evaluate({rules, on, group})` → `closed | restricted | open | unknown`, one
query per chunk plus O(rules) per section. Fine for a viewport, not a province.

Two traps: a `take: 0` carrying a **method** is not a closure (reading `take===0` alone called
**1,674 of 1,693** rulesets closed), and that four-state ladder is deliberately coarser than the
`Ledger` — do not mix the two vocabularies.

For province-wide colour the missing piece is a precomputed **`rule-set × stretch → outcome`**
table. Rule sets are already interned (210,095 of them, collapsing to far fewer answers).

### The oracle

`oracle.may_i_keep(ledger, fish, on, creel) -> Verdict` — **1.1 ms** a call. This is what an app
asks when someone has a fish in hand, and it is the acceptance test for everything above,
because it decides using the same counters the table draws.

---

## Part 7 — What is wrong today (all reproduced)

| | measured |
|---|---|
| bands with no single answer | **8 of 20** |
| tables drawing a trout/char fish outside the trout/char group | **16 of 22** |
| orphan entries (22 of them steelhead) | **28** |
| Haida Gwaii streams on **Dec 1**: bait lines shown | **4** — three of which say you may use bait, beside "No bait" |
| province spear: verdict says allowed, governing rule stamped disapplied | both kinds |
| gear band names set lining where that row says *not allowed* | **20 of 22** |
| a rig rule printed twice on one table | **3 tables** |

The quota faults share one cause: **rows are keyed by their whole counter set, including
counters nobody can act on**. Step 2 fixes all three.

The gear fault is separate: the display unit should be a **period** — a run of days over which
nothing changes — not a year. A year view is a union of days, and a union of days is a stack of
contradictory tables. The book already says what to do: `zp:bait::bait.r4` ends *"…as bait
unless a bait ban applies."*

### The gate to build against

> **No two printed lines may contradict each other.**

Check it against the rendered output, not the rules going in — an input-seeded check has
laundered failures three times in this project already.

---

## Part 8 — Gotchas

1. **Never run the parser** — it spends the owner's credits. Hand over the command.
2. **A rule id is unique only inside its entry.** `species_quotas.r1` is nine different rules.
   Key on `entry::rule`.
3. **Section ids must never leave the bundle** — no URLs, no saved pins. Bind to `item_id`.
4. **The bundle drops `extents`**; the table layer needs the curated tree on disk too.
5. **An empty span list binds nothing** — reading it as "no filter" once shut seven rivers.
6. **Weekday and time-of-day rules never resolve to a date.** The client must apply them.
7. **The repo PDF is not byte-identical to gov.bc.ca** — claim equivalence, not identity.
8. **Run gating tests unfiltered**: `.venv/bin/python -m pytest -q`.
9. **Curation defects are fixed by curation**, never by string-matching in the client.
