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

### 2.4 Base + amendments, worked

Measured, not designed: **60 stretches** across **12 of the 22 base tables** really change their
region's standing table. Both sides of every example below come out of `state.build` — the base
from `rules_for(region, kind)`, the water from `rules_for(kind=…, water=…, run=…)` — and the
difference is read off the two display trees. Nothing resolves twice.

| base table | stretches that amend it | base table | stretches that amend it |
|---|---|---|---|
| Region 1 · lakes | 1 | Region 4 · streams | **19** |
| Region 1 · streams | 4 | Region 5 · streams | 9 |
| Region 2 · streams | 11 | Region 6 · lakes | 1 |
| Region 3 · lakes | 1 | Region 6 · streams | 3 |
| Region 3 · streams | 2 | Region 8 · lakes | 3 |
| Region 4 · lakes | 3 | Region 8 · streams | 3 |

The ten tables not listed have no named water that changes them — every water in them reads its
region's table unaltered, which is the invariant this whole arrangement exists to make checkable.

#### A. Fording River at Josephine Falls — Region 4 streams — **the inherited case**

The only stretch in the corpus where a rank-0 rule and six rank-1 rules speak at once, and the
one that shows why rank is not a tie-breaker but the whole ladder:

| rank | scope | via | what it says |
|---|---|---|---|
| **0** | water | reach | **No Fishing** |
| 1 | inherited | trib | No Fishing, Sept 1 – Oct 31 |
| 1 | inherited | trib | Trout/char daily quota = 1 (none under 30 cm) |
| 1 | inherited | trib | bait ban, June 15 – Aug 31 |
| 1 | inherited | trib | Bull trout catch and release *(twice — Elk River's tributaries and Kootenay Lake's)* |
| 1 | inherited | trib | Class II water when open, including tributaries |

`via: trib` is the Fording answering to **Elk River's tributaries** — a rule written about
another water that reaches this one by the walk, and it sits at rank 1 rather than rank 0
precisely so the Fording's own voice speaks first.

| | base (Region 4 streams) | the Fording at Josephine Falls |
|---|---|---|
| bull trout, Dolly Varden | 1 | **0** |
| rainbow, cutthroat, westslope cutthroat | 2 | **0** |
| the trout-and-char budget | 2 across 13 | **gone** |
| the whitefish budget | 15 across 2 | **gone** |

The amendment is **one rule** and the whole table goes. The six inherited rules are still in the
ledger and still settled — they are simply behind a closure, and if the closure lifted they
would speak. That is the difference between a table that is shut and a table that is empty.

#### B. Okanagan Lake — Region 8 lakes — **narrows, opens, and reshapes a budget, at once**

Four water rules at rank 0, doing three different things:

| | base (Region 8 lakes) | Okanagan Lake | what the rule says |
|---|---|---|---|
| rainbow trout | 5 | **2** | `"Rainbow trout daily quota = 2 (only one over 50 cm)"` — a water **narrowing** the region |
| largemouth bass, smallmouth bass | 0 | **8** | `"bass daily quota = 8"` — a water **opening** what the region closed |
| yellow perch | 0 | **20** | `"yellow perch daily quota = 20"` |

And the budget is **reshaped**, which is the part a member-row layout cannot show:

- region: `1 over 50 cm` shared across **13** fish
- Okanagan Lake: `1 over 50 cm` shared across **12** — the rainbow left it for its own — plus a
  **new** budget of `8` across the two bass

A layout where a shared number is a numeral repeated on each member row has no way to say "this
fish is no longer in that number". The budget block does it by name.

#### C. Atlin Lake — Region 6 lakes — **a member's own cap, three times over**

Eight water rules, and the interesting ones are caps rather than answers:

| fish | per day | its own cap |
|---|---|---|
| lake trout | 3 *(inside the region's 5)* | `at most 1 over 60 cm` |
| Arctic grayling | 3 | `at most 1 over 35 cm` |
| northern pike | 5 | `at most 1 over 70 cm` |
| whitefish | **5** *(the region's 15, replaced)* | — |

Every one of those caps is a **pooled quota on a size class**, not a size gate — the difference
`Size.is_gate` draws. A gate sends a fish back; a cap says how many of your day's fish may be
that big. They are printed in different places for that reason, and reading the second as the
first is what put "only 1 over 50 cm" in a size column where a length belongs.

Atlin also carries the corpus's most awkward sentence — *"EITHER none over 60 cm, OR only 1 over
60 cm and the other 2 must be 60 cm or less"* — and it resolves to exactly the cap above,
because the two branches have the same effect on a creel of three.

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

Building it added four more, each from a defect the build itself produced:

6. **Every counter that binds is drawn somewhere.** Not "a headline and the budgets" with the
   rest falling through — a fish's counters are **partitioned**, and a counter in none of the
   three parts raises. A counter the reader never sees always reads as *more* fish.
7. **A size has two ends.** A floor, a ceiling, or both — one statement per bound, because
   "at least 30 cm · only 1 over 50 cm" is one muddled sentence and two rows are two facts.
8. **The definition reaches the number, not only the words.** A steelhead *is* a rainbow over
   50 cm. Printing "must be at least 50 cm" from that definition and then printing **5** beside
   a cap of "1 over 50 cm" is two halves of one fact with the half that matters missing.
9. **The cooler follows the day.** A possession limit is N × a **daily** one, and the daily one
   it multiplies is the family's, not this fish's.

### 5.1b What the tree actually carries

One `table` per (ledger, kind, date, label). `views` is `{"both": …}` or `{"wild": …,
"hatchery": …}`, decided by `origin_mode`; `names` maps every species code to its name, once,
so nothing downstream has to zip two differently-sorted lists.

| on a **leaf** (a fish, or fish a reader cannot tell apart) | |
|---|---|
| `answer` | the sentence the **book** writes, with `source` |
| **`most`** | **the number you may actually keep** — the answer with every budget it sits inside, every cap of its own, and the definition already applied |
| `sizes` | one statement per bound: `floor`, `ceiling`, `band`; `definition: true` where the word itself set it |
| `spends` | the budgets it counts against, innermost first |
| `own` | limits that are this fish's alone today — mostly a pooled cap the season narrowed to one survivor |
| `annual`, `possession`, **`may_have`** | the licence year, the cooler as written, and the cooler after `most` |
| `members`, `handle` | every member named — never a count |

| on a **budget** (a shared number two or more fish spend) | |
|---|---|
| `n`, `size`, `sized` | the number, and the size class it counts |
| `spends` | who spends it **on this date** — a released fish is not in it |
| `parent`, `clause_of` | the budget this one sits inside, by `Allowance.within` first |

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

## Part 6 — How you may fish

The quota half answers *what may I keep*. This half answers *how may I fish*. They share one
abstraction — "what you may keep **by this method**" is an `Allowance` in a `Ledger`, exactly
like a quota — and nothing else.

One structural difference decides the whole layout:

> **The gear display unit is a PERIOD, not a year.** A year view is a union of days, and a union
> of days is a stack of contradictory tables.

### 6.1 The bait contradiction, and why it is not a display bug

Haida Gwaii · streams, asked for **1 December**, prints four bait lines:

| rule | says | in force |
|---|---|---|
| `zp:bait::bait.r1` | Fin fish may not be used as bait | always |
| `zp:bait::bait.r4` | Freshwater invertebrates **may** be used | always |
| `zp:bait::bait.r6` | Roe **may** be used | always |
| `z1:hg_bait_ban_streams::…r1` | **No bait** | Nov 1 – Apr 30 |

Three of the four name a bait you may use, on a day you may not use any.

The cause is one line. `_settle_rig` decides whether one rule folds another by asking
`_whenever(ban, allowance)` — *is the ban live on every day the allowance is?* The ban is
seasonal and the allowance is year-round, so it is false, and **a seasonal ban can never fold a
year-round allowance**.

The proof that no new logic is needed: **Region 1 carries the same ban with the dates removed**
(`"Bait ban: applies to all streams of Region 1, all year"`) and already prints exactly one bait
line, with the other three folded away. Same code, opposite outcome, decided only by the date
guard. Settle *inside a period* and `_whenever` is trivially true, so the existing algebra does
the work.

The book agrees: `zp:bait::bait.r4` ends *"…in streams as bait **unless a bait ban applies**."*

### 6.2 The logic tree

```
0  CUT THE YEAR into periods — rows.schedule(rows, also=gear_terms) already does this
1  SETTLE INSIDE THE PERIOD  — drop terms live nowhere in it; treat the rest as year-round
2  THE WATER FIRST           — a closure prints the card of Part 3 and one rule: "do not place
                               any fishing gear in any water during a No Fishing period"
3  VERDICT PER METHOD        — the standing contest; a ban beats a permit at equal rank; the
                               default is stated with its reason
4  ON THE PROVINCE'S TABLE   — a fourth verdict, "depends where", built from region_limited
5  A ZERO VERDICT ANNIHILATES— but a DUTY survives (see below)
6  RIG CONDITIONS ACCUMULATE — the ladder only picks between two statements about the same tackle
7  A SHARED CONDITION IS DRAWN ONCE, over the methods that can spend it — and it NAMES them
8  HOIST PER CONDITION       — one shared by ≥2 live methods hoists and carries its sharers
9  NO TWO PRINTED LINES CONTRADICT — checked against the rendered output
```

**Step 5 has a counter-example that defines it.** "Drop every keep counter under a banned method"
is wrong: snagging reads *not allowed* on all 22 tables and carries *"any fish willfully or
accidentally snagged must be released immediately"* — a **duty that exists because you did the
banned thing**. So the test is on the counter, not the method: a `release` survives, a `closed`
goes. That keeps 22 snagging rows and drops 31 rows of unspendable text.

### 6.3 The layout — a verdict ledger, then one rig list

Chosen from the shape of the corpus, not from taste. Across the 22 tables there are 176
(table, method) rows:

| | |
|---|---|
| rows reading **not allowed** | **94 of 176** |
| tables where the rig list is **entirely shared** | **19 of 22** |
| rig lines per table | 7 – 11 (median 9) |
| longest single printed condition | **440 characters** (Region 8's turtle advisory) |

Half the page is "no", so a card per method spends four lines to say nothing 94 times. And at
400px a full-width line holds ~45 characters, so a three-column band leaves ~25 for text — **the
rig block must be a list, not a table.** Only the verdict ledger and the species tables may be
tables, because their right-hand cells are short and bounded.

#### Region 1 · streams — all year

**Can I?**

| | |
|---|---|
| **Rod and line** | ✅ Yes |
| **Ice fishing** | ✅ Yes — 2 conditions |
| **Crayfish traps** | ✅ Yes — release everything else |
| Set line (unattended) | ❌ No — nothing in the book allows it here |
| Spear or bow | ❌ No — banned in Regions 1, 2 and 4 |
| Nets · Chumming | ❌ No |
| Snagging | ❌ No — and release anything you foul-hook |

**How must I rig it?** — *Rod and line · Ice fishing*

- **Bait — none.** *"Bait ban: applies to all streams of Region 1, all year"* · Region 1
- **Hooks — one, barbless.** *"Single barbless hook: must be used in all streams of Region 1"*
- **Lines — 1 per angler**, at most 1 artificial fly, at most 1 kg of weight (not downriggers)
- **May** use a downrigger with a quick-release · **must not** use a light to attract fish
  unless submerged within 1 m of the hook

#### Haida Gwaii · streams — 1 December

**Bait — none, until 30 April.** *"Bait ban: applies to all streams in Management Units 6-12 and
6-13, Nov 1-Apr 30."*
> From **1 May** you may again use freshwater invertebrates and roe.

Eight lines instead of eleven, and the contradiction is gone — not filtered out, *folded* by the
model, exactly as Region 1's year-round ban already folds.

#### All of B.C. · streams — where the verdict is a map

| where | | the sentence |
|---|---|---|
| **Regions 1, 2 and 4** | ❌ not at all | *"No spear fishing of any kind is permitted in Region 1, 2, and 4."* |
| **Regions 3, 5, 6, 7 and 8** | ✅ non-game fish, and burbot | *"Only non-game fish (such as carp) may be speared"* · *"except burbot…"* |
| **anywhere** | ❌ no game fish, no salmon, no protected species | `zp:spear_fishing::spear_fishing.r4` |

Today that row reads **`allowed`** while its governing term is stamped *"closed here by a
stricter rule"* — a permission a reader in Region 2 would act on.

### 6.4 Three more defects, all verified

| | measured |
|---|---|
| lake tables printing **two live line limits as peers** — *"1 line per angler"* beside *"2 lines per angler, from lakes — alone in a boat"* | **11 of 11** |
| tables whose *Also* block mixes a **permission and a prohibition** with identical polarity, so both read as instructions | **22 of 22** |
| Region 8's crayfish trapping **governed by a 440-character advisory about turtles**, with the real permit folded away as "says the same thing" | **both kinds** |

The line-limit pair is one sentence in the book — *"Angle with more than one line, **EXCEPT** a
person who is alone in a boat on a lake may angle with two lines"* — so r2 is a carve into r1,
not a peer, and the existing `carves` machinery would print it correctly once curated.

### 6.5 What the model must change

| # | where | change |
|---|---|---|
| 1 | `method_provenance.base_table` / `section` | take a **period**; emit the period list from `rows.schedule`, which already accepts gear terms |
| 2 | `MethodTable.__init__` | settle **inside** the period — drop terms live nowhere in it |
| 3 | `_whenever` (`method.py:373`) | inside a period it is trivially true; it must stop deciding the printed answer |
| 4 | `MethodTable.calendar` | fingerprint on `(closure, standing, live rig, live keep)` — the same `sig` `rows.schedule` uses |
| 5 | `MethodRow.verdict_word` | a fourth verdict, `depends`, when `region_limited` is non-empty on the province's table |
| 6 | `row_json` | under a zero verdict emit no rig, and keep only **duties** (`release`), never `closed` |
| 7 | `terms_of` | an **advisory must not govern** a method |
| 8 | `hoist` | band members are hook methods **whose verdict is allowed** |
| 9 | `hoist` | hoist per condition, carrying the sharer names; a row keeps only what nobody shares |
| 10 | `method_comply.audit` | the contradiction gate, **against the rendered output** |

---

## Part 7 — What licence do I need? (research, not a table yet)

No licence table is being built. This is the parameter set one would need, and an honest account
of what the corpus can answer today.

### 7.1 The documents

| document | trigger | in the corpus? |
|---|---|---|
| **Basic angling licence** (annual / one-day / eight-day) | any sport fishing, 16+ | the requirement yes; **the durations no** |
| **Classified Waters Licence** | a classified stream, during its classified period | **78 rules** |
| **Class I / Class II day licence** | non-resident or alien on a classified water | class yes; the per-day purchase no |
| **Steelhead Conservation Surcharge Stamp** | targeting steelhead *anywhere*, keep or release — plus most classified waters in period | **49 rules** |
| **Non-tidal salmon stamp** | keeping a salmon other than kokanee | 1 rule, `on_retention` |
| **Kootenay / Shuswap rainbow · Shuswap char stamps** | keeping a rainbow > 50 cm or char > 60 cm on named waters | 6 rules |
| **White Sturgeon Conservation Licence** | targeting sturgeon, Fraser watershed, Mission → Williams Lake River | 1 rule |
| **National Park Fishing Permit** | inside a national park — *and a B.C. licence is not valid there* | 2 rules |
| **Creston Valley WMA permit · landowner permission** | named places | 4 rules |

Waivers the book states: under 16 and resident (no licence, own quota); under 16 non-resident
(accompanied by a licensed adult); *"If you are an Indian and a resident of B.C., you are not
required to obtain any type of fishing licence"*; Métis **are** required. **Family Fishing
Weekend** — the basic-licence waiver on Father's Day weekend — is **0 rules in the corpus**.

### 7.2 The parameters, by where the value comes from

**Ask the angler once (7 facts) — nothing in the codebase asks any of them today:**
residency (resident / non-resident / non-resident alien) · guided? · 16+? · 65+? ·
Indian & B.C. resident / Métis / disabled · target species · intend to keep?

**Read off the water and date (7 facts, 6 of which exist):** classified + class + whether the
date is inside the window · which named licence to buy · the steelhead-stamp window and its
exceptions · park or reserve membership · the white-sturgeon reach · named-stamp waters ·
non-resident day allocation *(text only, not computable, on 66 of 73 rules)*.

### 7.3 What the corpus actually holds

| | measured |
|---|---|
| `document_required` rules | **148** |
| of those, classified waters · steelhead stamp · basic licence | 78 · 49 · 10 |
| `water_class` set | **71 rules — 62 Class II, 9 Class I** |
| `angler_class` set | **38 of 3,422 rules — 1.1 %** |
| `issuing_jurisdiction` — declared in the model | **set on 0 rules** |

> **The water half is nearly done; the angler half is barely started.** Which stamp, which
> permit, whether classified and in what class and when — all of that is in the corpus and bound
> to sections. Residency, guiding, age and status are not, and `app/packages/core/src/status.ts`
> treats `document_required` and `access_permission` only as members of a `licensing` family and
> reads none of the fields.

### 7.4 What must be curated

1. **Three CW-marked waters have no catalogue entry at all** — `QUINN CREEK CW 4-22`,
   `SKOOKUMCHUCK CREEK CW 4-20`, `KILBELLA RIVER CW 5-7`. Skookumchuck is named in the
   province-wide booking advisory and still has no water of its own.
2. **`BIGHORN (Ram) CREEK CW 4-2`** is printed as classified but its only rule is a
   cross-reference — *"A tributary of Wigwam River; see Wigwam River"* — and nothing resolves it.
   Its class is unknown.
3. **Five waters carry `water_class` on a `steelhead_stamp` rule, not a
   `classified_waters_licence` rule** — West Road (Blackwater), Babine, Stellako, Sustut, Telkwa.
   A "does this need a Classified Waters Licence" query keyed on `document` misses all five.
4. **Do not key on the `Classified` entry symbol.** It is on 21 entries while 71 carry a class
   or a CW rule. Key on the rule.
5. **The Skeena has two separate Class II sections** needing separate per-day licences, and the
   corpus knows there are two but carries no identifier a purchase screen could use.

### 7.5 Honest unknowns

The corpus cannot say what a licence **costs**, cannot choose between annual / one-day /
eight-day, cannot apply the Family Fishing Weekend waiver, cannot name the specific Skeena
section licence, and does not know Bighorn (Ram) Creek's class.

---

## Part 8 — Where this connects to the app

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

## Part 9 — What is wrong today

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

### The display tree, reviewed, found unsafe — and fixed

`pipeline/regs/table/display.py` was written to this design and reviewed by three independent
passes (regulatory correctness, architecture, test adequacy). **Every finding was real**, every
one was reproduced against the module before being recorded, and every one is now closed.
`pipeline/tests/test_regs_display.py` holds them shut. The history is kept here because the
shape of the fix came out of the shape of the failure.

**Seven of them were three.** Three separate reviewers wrote up seven defects, and rewriting the
module showed that three of the seven were one defect wearing three hats:

> The tree drew a **headline** (a plain daily number) and a **shared budget** (a pooled number
> with two or more spenders). A counter that was neither simply vanished.

| what vanished | what the reader got | where |
|---|---|---|
| a pooled cap the season narrowed to **one** fish | R3 streams, 20 Aug: **4** lake trout where the book allows **1** | `pools()` dropped a budget with one spender |
| a **non-pooled sized** cap | R1 lakes, hatchery steelhead: **4** where the book allows **2** | `"2 hatchery steelhead over 50 cm"` is neither plain nor pooled |
| a **clause** | Region 8's three budgets came out as coequal roots | `within` was read only for nesting |

So the fix is not three patches. A fish's counters are now **partitioned** — exactly one is the
answer, the shared ones become budgets, and everything left is drawn on the fish as its own
limit — and `display._check` **raises** when a counter lands in none of the three. It ran clean
over 22 tables × 11 dates. A dropped counter always reads as *more* fish than the book allows,
so it is a crash and not a log line.

| finding | fix | evidence |
|---|---|---|
| prints a bigger number than the ledger allows | `leaf["most"]` — the answer with every budget it sits inside already applied | **0 over-statements in 19,360 species-slots** (22 tables × 11 dates × 2 origins × species) |
| takes half the definitional size | `_whole_fish` — a steelhead **is** a rainbow over 50 cm, so `"1 over 50 cm"` on a steelhead row is simply 1 | R6 hatchery steelhead 5 → **1**; R1 lakes 4 → **2** |
| a live maximum size disappears | `sizes()` reads every `Size.kind`, and a slot gives **both** ends | R7A lakes: *"must be at least 30 cm"* **and** *"must be 50 cm or shorter"* |
| origin mode inverted | keyed on the pool's **identity** (rule, size class, clause), not on its spender set | 22 split / 0 hoist / 0 none — and every table's difference is steelhead, 4 of them wider |
| `_shape` omits `annual` | `_shape` carries every visible field | R3 and R5, whose only origin difference **is** the annual ten, no longer collapse |
| a seasonal pool drawn out of season | `binds` on both ends of pool membership, not `reaches` | **0** seasonal budgets in any standing view |
| nesting by strict containment | `Allowance.within` first, then a **narrowness** order | 222 clauses parented, **0 orphans, 0 cycles** (was 52 unparented) |

**Three more defects the rewrite introduced, and the tests caught.** Worth recording because all
three are under-statements, which no sweep for over-statements can find:

- **A sized budget clamped every member.** `by_definition` was computed per *budget*, so
  steelhead's definition put its 1 onto the Arctic char sharing the same number — 5 → 1.
  It is a question about the fish, not the budget.
- **The sharer lists named the wrong fish.** `leaf["fish"]` is sorted by species code and
  `leaf["members"]` by name; zipping them — the obvious thing, and what the page did — listed a
  Dolly Varden among the fish spending Region 6's five, and the Dolly Varden is released. The
  table now carries a `names` map, which cannot be zipped wrong.
- **The cooler ignored the day.** A possession counter is N × a **daily** one, and the daily one
  it multiplies is the family's. R6 hatchery steelhead may keep one a day and the row offered
  **ten**, explaining itself as *"2 × the daily 5"*. On a closed water it offered ten beside
  twelve rows reading *"No fishing"*.

**Mutation-tested, and one survivor reported honestly.** Each defect was re-introduced and the
suite re-run: **11 of 12 mutants caught**. Two further mutants (dropping `own` or `most` from
`_shape`) **survive and are equivalent on this corpus** — every table already differs between
origins on `annual`, so those fields cannot change an output here. That is a property of the
data, not a hole in the tests, and it is written down rather than rounded up.

### What was cleared

| change | verdict |
|---|---|
| `Ledger.keepable` | safe — its permissive default can only lengthen a sharer list |
| `spenders` in `rows.py` / `provenance.py` | **safe, strictly more restrictive** — 160 "between them" gained, 0 lost; 73 names removed, every one released that day |
| removing `ADV` / `AEB` | **safe and proven inert** — 22 tables × 25 dates, 102 sections × 5 dates, zero change to any headline, deciding rule or counter set |
| `Row.moot` dropping `and not a.is_zero` | **fixed, and the blunt guard was wrong too** — see below |
| `display.py` | **safe** — 16 tests, 11/12 mutants caught, 2 equivalent |

#### `Row.moot` — and why "daily only" was not the fix

A size gate under "Put it back" reads as permission, so `moot` had to stop ending
`and not a.is_zero`. A reviewer then found the opposite hazard: `moot` asks about the **daily**
headline and was suppressing counters of *every* period, so a daily release could hide an annual
ceiling you may lawfully be spending on fish taken elsewhere. The guard added for that —
`and a.period == "daily"` — broke two real tests, and both were right to break:

| shape | the blunt guard | `daily`-only guard | what is true |
|---|---|---|---|
| **a possession multiple** — the Fraser's bull trout, "twice the daily quota" over a release | moot ✓ | **live: 2 beside "Put it back"** | twice nothing is nothing |
| **an independent annual ceiling** — the province's ten hatchery steelhead a year, on a water that releases them all year | moot ✓ | live ✗ | none can be spent *from here* |
| the same ceiling on a water closed only **Aug 15 – Dec 31** | **hidden all year** ✗ | live ✓ | it is exactly what a reader needs on the open days |

So the rule is neither "everything" nor "daily only". A **derived** counter is moot whenever the
counter it derives from is; an **independent** clock is moot only where the zero is year-round.
The three columns above are three different answers and the model now gives each of them.

### The gate to build against

> **No two printed lines may contradict each other.**

Checked against the **rendered output**, not the rules going in — an input-seeded check has
laundered failures three times in this project already. Every test in
`pipeline/tests/test_regs_display.py` compares the tree against a number derived independently
from `Ledger`, never against the tree itself.

### What the page stopped doing

The renderer in `app/design/standing/body.html` went from **1,353 lines to 714**. The 655 lines
that went were the browser re-deriving, in JavaScript, what the ledger had already settled in
Python: which fish shared which number, which size bound survived, which counter was moot, which
name belonged in a sharer list. Every one of those existed twice, and the two drifted — the
emitter learned who could really spend a number (`spends`) and the page went on reading the old
field (`reaches`), so that fix never reached a reader at all. The page now draws `quota` and
decides nothing.

---

## Part 10 — Gotchas

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
