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

Two independent questions. Keep them apart; they were tangled in an earlier draft of this
document and that is what made it hard to follow.

### 2.1 WHERE — a base, then amendments

Not three alternatives. **A two-step lookup:**

| step | you get | how many exist |
|---|---|---|
| 1. the **base** — the region, and the area inside it if you are in one | a whole settled table | 22 region×kind tables, plus 5 named areas |
| 2. the **amendments** — rules written for this water and stretch | a short named list | 599 water rules + 62 inherited, across 102 stretches |

> **A stretch is its base table plus its own amendments, and nothing else.**

That gives the verification order: get the 22 region tables and 5 areas right — they are checked
line by line against the printed synopsis — and after that a stretch can only be wrong in its own
short amendment list, each item attributable to one named rule.

### 2.2 The code does not do this yet, and it costs answers

`build.base()` re-derives the base **from whatever rules the atlas happened to bind to that
stretch**, instead of looking it up by region. Measured over the 100 single-region stretches that
ship:

| | |
|---|---|
| base matches its region's standing base | **37** |
| base **differs** | **63** |
| region-wide rules missing from some stretch's base | **18 rules, 98 omissions** |
| rules in a stretch's base its region does *not* have | **0** |

One-directional: a stretch can only **lose** region-wide rules, never gain them. Building the
proposed way changes the answer on **23 of the 100** — the Fraser's Region 2 stretches gain a
bass row (20 a day) they do not have today.

The clearest illustration: the Fraser still shows a *Protected species* row, but it is the
**provincial** protected-species rule holding it up — Region 2's own is missing from that
stretch's base. The table is right by luck of a second rule, and nothing on the page tells you
which lines are standing on their own base and which are not.

### 2.3 WHEN — the year, cut into stretches

A **stretch** is a run of days over which nothing changes. The year is cut wherever any counter
or gear term comes into or goes out of force. Across the 22 tables there are **53** of them; ten
tables have exactly one and never change all year.

Region 3's streams have six:

| | Jan 1 – Jun 30 | Jul | Aug 1 – Oct 14 | Oct 15 – Oct 31 | Nov – Dec |
|---|---|---|---|---|---|
| everything | **closed** | open | open | open | open |
| bull trout · Dolly Varden | closed | 4 | **put back** | **put back** | 4 |
| lake trout | closed | 4 | 4 | **put back** | **put back** |

`state.conditions(region, kind)` multiplies the two questions together — every (area, stretch)
pair a region has. **218** across the 22 tables.

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

## Part 5 — The table

Two renderings of one tree. **Option 3** is the table; **Option 4** is what a tap on a fish
opens. Earlier drafts had five; the rest are gone.

### 5.1 The five rules the layout obeys

Each was forced by a real defect, listed in Part 7.

1. **Never print a count in place of a name.** `9 kinds of trout and char` tells a reader with a
   bull trout nothing — and on Regions 2 and 3 it invites them to assume they are in it when
   they are not. A group prints a **handle** plus **every member**, always visible.
2. **A shared number is drawn once, as a budget** — never as a numeral repeated on each member
   row. Member rows say what they *spend*, not how many they may keep.
3. **A member may state its own number** when it is smaller than the group's. Region 6 streams
   gives trout **1 a day** inside a family of **5**; a layout that forbids the member from
   showing a number puts a reader five fish over.
4. **A released fish leaves the group.** It is not drawn inside a live shared number, and its
   name disappears from every sharer list on that date.
5. **Nothing may be kept ⇒ no size is printed.** "must be at least 60 cm" beside "Put it back"
   reads as permission to keep a 61 cm fish.

---

### 5.2 Option 3 — the table

#### Region 2 · streams — **all year** *(this table has one stretch: Jan 1 – Dec 31)*

**▮ HATCHERY TROUT AND CHAR — 2 a day, 4 in possession, shared**

> **2 in total, not 2 of each.** Keep two rainbow and your day is done.
> *Arctic char · Brook trout · Brown trout · Bull trout · Coastal cutthroat · Cutthroat ·
> Dolly Varden · Golden trout · Lake trout · Rainbow · Splake · Steelhead · Westslope
> cutthroat* — 13 kinds
> `"2 from streams (must be hatchery)"` — Region 2 · region-wide

**Shared budgets inside that 2** — spend one on any fish and it is spent for all of them:

| shared budget | how many | who spends it |
|---|---|---|
| ▮ over 50 cm | **1 a day** | every kind above **except steelhead** |
| ▮ the chars | **1 a day** | Bull trout · Dolly Varden · Lake trout |

**Steelhead's own budgets** — these do *not* touch the shared ones:

| budget | how many | who |
|---|---|---|
| ▯ steelhead over 50 cm | **2 a day** | Steelhead only |
| ▯ steelhead a year | **10 a licence year** | Steelhead only |

**Each kind:**

| kind | smallest you may keep | what it spends |
|---|---|---|
| Arctic char · Brook trout · Brown trout · Coastal cutthroat · Cutthroat · Golden trout · Rainbow · Splake · Westslope cutthroat | 30 cm | 1 of the ▮ 2 · and if over 50 cm, the ▮ 1-over-50 |
| **Bull trout · Dolly Varden · Lake trout** | **60 cm** | 1 of the ▮ 2 · **the ▮ 1 char** · and the ▮ 1-over-50 |
| **Steelhead** | 30 cm | 1 of the ▮ 2 · 1 of its own ▯ 2-over-50 · 1 of its ▯ 10 a year |

> **Worked check, printed on the page:** keep a 62 cm bull trout and you have spent the 1 char,
> the 1 over 50 cm **and** 1 of your 2. The only fish left today is a hatchery trout **under
> 50 cm** — a 55 cm rainbow is now illegal.

**WILD TROUT AND CHAR — put every one back.** All 13 kinds, every size.
`"Wild trout/char from streams"`, and for steelhead `"All wild steelhead"`.
*No size is shown: nothing may be kept, so nothing can be measured.*

**Everything else**

| kind | per day | in possession |
|---|---|---|
| Whitefish — *Lake whitefish · Mountain whitefish* | ▮ **15 between them** | 30 |
| Bass — *Largemouth · Smallmouth* | ▮ **20 between them** | 40 |
| Black crappie | 20 | 40 |
| Crayfish | 25 | 50 |
| Kokanee | Put it back | — |
| White sturgeon | Put it back | — |
| Protected species — *12 kinds, tap to list* | You may not fish for them | — |

---

#### Region 3 · streams — **Aug 1 – Oct 14** (the closed-member case)

*stretch picker:* `Jan 1–31 · Feb 1–Jun 30 · Jul 1–31 ·` **`Aug 1–Oct 14`** `· Oct 15–31 · Nov 1–Dec 31`

**▮ TROUT AND CHAR — 4 a day, 8 in possession, shared**

> *Arctic char · Brook trout · Brown trout · Coastal cutthroat · Cutthroat · Golden trout ·
> **Lake trout** · Rainbow · Splake · Westslope cutthroat* — 10 kinds **today**
> ⚠ **Bull trout, Dolly Varden and steelhead are released until 31 October — they are not in
> this 4.**

| shared budget | how many | who spends it |
|---|---|---|
| ▮ over 50 cm | **1 a day** | all 10 kinds above |
| ▯ lake trout | **1 a day** | **Lake trout only, today** |

That second row is rule `"1 bull trout (Dolly Varden) or lake trout"`. Two of the three fish it
names must be released until 31 October, so **today it is a lake-trout limit**. The book's words
stay in the provenance line for a reviewer; they stay out of the reader's answer.

| kind | smallest | what it spends |
|---|---|---|
| the 9 kinds above except lake trout | any size | 1 of the ▮ 4 · and if over 50 cm, the ▮ 1-over-50 |
| **Lake trout** | **60 cm** | 1 of the ▮ 4 · the ▯ 1 · and — since it must be over 60 cm — **always** the ▮ 1-over-50 |

**Released today — put every one back**

| kind | until |
|---|---|
| Bull trout · Dolly Varden | 31 October |
| Steelhead | all year on Region 3 streams |
| Kokanee · White sturgeon | all year |

**Everything else**

| kind | per day |
|---|---|
| Burbot | 2 |
| Whitefish — *Lake whitefish · Mountain whitefish* | ▮ 15 between them |
| Crayfish | 25 |
| Bass · Yellow perch | Closed to fishing |
| Protected species — *12 kinds* | You may not fish for them |
| Arctic grayling · Black crappie · Goldeye · Inconnu · Northern pike · Walleye | **Region 3's chapter sets no limit for these.** Check the water-specific table before you keep one. |

That last row is the honest rendering of a row with no answer — **12 of them exist**. Never a
blank cell: a blank reads as "no limit", which is the most permissive possible failure.

---

### 5.3 Option 4 — tap a fish

🔍 `bull trout` — Region 2 streams

| **BULL TROUT** | |
|---|---|
| **Wild** | **Put it back** |
| **Hatchery** | **Keep 1** · minimum 60 cm |

| That 1 also spends | shared with |
|---|---|
| 1 of only **3 chars** a day | Dolly Varden, lake trout |
| 1 of your **2 trout and char** a day | 13 kinds |
| your **1 fish over 50 cm** a day | 12 kinds — not steelhead, which has its own 2 |
| 1 of **4 in possession** | 13 kinds |

---

### 5.4 Two decisions

**Split the complex block from the simple quotas — yes, but split by *structure*, not by name.**
Across the 22 tables, **177 entries sit outside any shared number**, and only 11 of them carry a
size statement at all — 4 of those are trout/char rows anyway. So the real division is *fish that
share a number* versus *fish with a number of their own*, which is about 3 rows against 8 on a
typical table. Splitting by the words "trout and char" would put Region 7a's lake trout (inside
the band) and its bull trout (released, outside it) in the same block while the model has them in
different ones.

**A fish-picker dropdown — no. A search that jumps — yes.** A dropdown's option list *is* the
member enumeration from rule 1; if the members are on the page the dropdown duplicates them, and
if they are not, the dropdown is the only place they appear and browsing is still broken. Worse,
filtering a shared-budget table to one fish hides the other spenders — destroying the one thing
the table exists to show. A search field that **scrolls to and highlights** the row, leaving the
table intact, costs one line and answers the real question ("is my fish here at all"). It must
match **members, not headings**: on 46 of the 53 tables the word a reader types — `splake`,
`dolly varden`, `westslope` — appears in no heading on the page.

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

## Part 7 — What is wrong today

Every line reproduced against the running builder. Counts are **my own measurements**; where a
design pass reported a different number I give mine and say so.

### The display draws things that are not true on the date

| | measured | what a reader gets |
|---|---|---|
| a released or closed fish drawn **inside a live shared number** | **21** (band, stretch, member) | "keep 4 between them" over a fish they must release |
| a shared cap naming **released fish as sharers** | R3 streams Aug 1: the "1 char" names 3, only lake trout is keepable | a budget shared with two phantoms — or worse, read as permission |
| pooled counters printing with **no spender at all** on some stretch | **30** (counter, stretch) pairs | a limit for a group nobody may keep from |
| pooled counters collapsing to **exactly one fish** | **5** | "1 between them" about a single species |
| size statements printed under a zero answer | **11** year-round (more on dated views) | "at least 60 cm" beside "Put it back" |

**The root cause is one field.** `Ledger.reaches` asks *"does this counter speak about this fish
on **some** date"*, and drops a fish only when the carve is `always` — so a **seasonal** release
never removes anyone. It has no `on` parameter at all. The correct test is `reaches` **and** that
fish's headline is non-zero on the date, which lives only on `Row.headline(on)`. The ledger needs
that primitive; four places currently re-derive it and disagree.

### The display understates or hides a shared number

| | measured |
|---|---|
| pooled counters whose **hatchery** sharer set is larger than the wild one — and the code picks wild as the representative | **15 of 84** |
| Region 4's `"1 rainbow trout or cutthroat trout over 50 cm"` prints **`only 1 over 50 cm`** with an empty sharer list | pooled over both, so a reader keeps a 55 cm rainbow *and* a 55 cm cutthroat |
| hidden anadromous forms leaking into sharer text | `brook trout (anadromous)`, `dolly varden (anadromous)` — names in no heading anywhere |

Understating who shares a budget is the dangerous direction.

### Structure

| | measured |
|---|---|
| a member whose own number is **smaller than its band's** | **21** (incl. members at 0 under a live band) — Region 6 streams gives trout **1** inside a family of **5** |
| bands with no single answer | **8 of 20** |
| rows with **no answer at all** | **12** — Region 7a streams gives nine species no number, no release, nothing |
| a clause drawn against a parent that was **replaced** | Region 8 streams: `"1 over 50 cm"` is `within` `"Trout/char: 5"`, which is replaced on streams by `"4 from streams"` |
| a group heading that names a set by an umbrella it does not fill | `9 kinds of all game fish` — Region 7a streams, 4 tables |

### Gear

| | measured |
|---|---|
| Haida Gwaii streams on **Dec 1**: bait lines shown | **4** — three say you may use bait, beside `No bait` |
| province spear: verdict `allowed`, governing rule stamped disapplied | both kinds |
| band names set lining where that row reads *not allowed* | **20 of 22** |
| a rig rule printed twice on one table | **3** |

The gear fault is one idea: **the display unit should be a period** — a run of days over which
nothing changes — not a year. A year view is a union of days, and a union of days is a stack of
contradictory tables. The book already says what to do: `zp:bait::bait.r4` ends *"…as bait unless
a bait ban applies."*

### The gate to build against

> **No two printed lines may contradict each other.**

Checked against the **rendered output**, not the rules going in — an input-seeded check has
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
