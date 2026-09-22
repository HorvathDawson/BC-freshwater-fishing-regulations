# How a regulation table is generated

> ## ⚠ THE CODE THIS DESCRIBES IS NOT IN THE REPOSITORY
>
> The settling layer was removed to be rebuilt properly — `ledger.py`, `build.py`, `rows.py`,
> `display.py`, `quota_print.py`, `comply.py`, `oracle.py` and the eight `method_*` modules, about
> 7,000 lines. What remains is `corpus.py`, `authority.py` and `state.py`: the rules, how a rule is
> classified, and which rules apply where.
>
> **This document is kept as the reference to rebuild from**, and every number in it was measured
> against the code when it ran. Read it as a record of what the problem is and how it was solved
> once, not as a map of the current tree. `06-ui-data-contract.md` is what the rebuilt layer owes.
>
> Two of the removed modules were not settling and are wanted back: `quota_print` checked every
> line of each region's printed quota box against the rule accounting for it (**384 of 384**), and
> `comply` guaranteed that settling drops nothing silently.

Read in order. One real rule is followed all the way through — Region 2's **"2 from streams
(must be hatchery)"** — and every step of the logic tree has a worked example beside it.

---

## Part 1 — The flow

```
  ①  CURATED ENTRY   a human-checked sentence from the printed book
  ②  BUNDLE          that sentence, plus WHICH WATERS it binds
  ③  RULE            one flat dict the table layer reads
  ④  ALLOWANCE       the sentence as a COUNTER you spend
  ⑤  LEDGER          every counter here, settled against every other
  ⑥  ROW → TABLE     what you see
```

**① Curated entry** — `data/curated/.../region-2.json`: `verbatim: "2 from streams (must be
hatchery)"`.

**② Bundle** — `data/generated/bundle/bundle.sqlite`. The atlas works out which stretches this
reaches and stores the sentence once, with a list of sections pointing at it.

**③ Rule** — what `corpus.rules()` hands the table layer: a flat dict. **Sizes and dates are
fields like any other**, so nothing downstream parses prose.

| field | our `r4` | a sized sibling `r2` | a floor `r8` | a seasonal rule |
|---|---|---|---|---|
| `verbatim` | "2 from streams (must be hatchery)" | "1 over 50 cm" | "Hatchery trout/char under 30 cm" | "No Fishing Apr 1 – Oct 31" |
| `species` | `["TROUT_CHAR"]` | `["TROUT_CHAR"]` | `["TROUT_CHAR"]` | `["ALL_GAME_FISH"]` |
| `origin` / `water` | `hatchery` / `stream` | — | `hatchery` / `stream` | — |
| `take` | `2` | `1` | `0` | `0` |
| `over_cm` / `under_cm` | — | **`50`** / — | — / **`30`** | — |
| `period` | `daily` | `daily` | `daily` | `daily` |
| `windows` | `[]` | `[]` | `[]` | `[{from:{4,1}, to:{10,31}}]` |
| `windows_are` | `applies` | `applies` | `applies` | `applies` |
| `when_open` | `false` | `false` | `false` | `false` |

**And its provenance, on the rule itself.** A regulation a reader cannot trace to the book is a
regulation they cannot check, so the trace is a field like any other rather than something a
consumer joins for itself:

| field | our `r4` |
|---|---|
| `who` · `authority` · `binds_to` · `rank` | "Region 2 · region-wide" · `region` · `region` · `3` |
| `entry` · `entry_name` | `z2:trout_char_quota` · "Trout and char daily quota" |
| **`synopsis_pages`** | `[22]` — checkable by anyone holding the book |
| **`printed_box`** | *"Region 2 Daily Quotas. Trout/char: 4, but not more than 1 over 50 cm … 2 from streams (must be hatchery) …"* — the rule IN CONTEXT, which is how a clause is told from a peer |
| **`proves_printed_line`** | `"• 2 from streams (must be hatchery)"` — page 23. Not "it came from page 22" but "it is the answer to this line of that page" |

The last one is produced by checking, not by claiming: `quota_print` reads the printed box out of
the PDF and matches every line to the rule that accounts for it, so a rule that proves nothing —
or a printed line nothing proves — is visible rather than assumed.

**Size is one pair of fields**, and every size statement in the book falls out of it:

| `take` | `over_cm` | `under_cm` | means |
|---|---|---|---|
| 2 | — | — | keep 2, any size |
| 1 | 50 | — | of those, only 1 may be **over 50 cm** — a *cap on a size class* |
| 0 | — | 30 | keep **none under 30 cm** — a *floor* |
| 0 | 50 | — | none **over** 50 cm — a ceiling |

A floor is an allowance of **zero** on a size class — which is why closures, releases and size
limits travel as one object: all are "keep N of this class", and for a floor N is nought.

**Dates are three fields**, and they are on every rule (617 of 3,422 carry a real window):

| field | values | means |
|---|---|---|
| `windows` | a list of from/to month-days | the days it speaks about |
| `windows_are` | `applies` (3,419) · `excepts` (3) | whether those are the days it holds, or the days it does **not** — *"Kokanee catch and release, EXCEPT Apr 1–3 and Jul 1–2, when daily quota = 5"* |
| `when_open` | `false` (3,386) · `true` (36) | it binds only while the water is open at all — *"artificial fly only upstream of Bugaboo Creek when open"*; carries no dates of its own |
| `period` | `daily` (3,380) · `possession` (34) · `annual` (8) | which clock the number is on |

`applies.py` turns the three into one value with one reading, so no caller re-decides polarity. A
rule with no window is not "undated": it is the standing answer, true on every day nothing
seasonal speaks.

**④ Allowance** — the same fact as a counter:

```
scope   : 15 trout and char · hatchery · any size
outcome : 2 per day, POOLED  (2 between all of them, not 2 each)
source  : Region 2 · region-wide
```

**⑤ Ledger** — it meets the other rules in the chapter and they sort themselves out:

| rule | says | what happens |
|---|---|---|
| `r1` "Trout/char: 4" | 4 per day | **replaced on streams** by `r4`, its stream clause |
| `r4` "2 from streams (must be hatchery)" | 2 per day, hatchery | **governs** |
| `r6` "Wild trout/char from streams" | release, wild | **governs the wild side** |
| `r8` "Hatchery trout/char under 30 cm" | none under 30 cm | a size floor on `r4` |
| `r2` "1 over 50 cm" | 1 big one, pooled | a cap inside `r4` |
| `r5` "1 char (bull trout, Dolly Varden or lake trout)" | 1 of those three | a cap inside `r4` |
| `r3` "2 hatchery steelhead over 50 cm allowed" | 2 big steelhead | **carves steelhead out of `r2`** |

Nobody wrote that table. It falls out of **`Source.rank`**, which is not stored but computed from
two independent facts: **who wrote it** (`Authority`: `superior` / `province` / `region`) and
**what it binds to** (`Scope`: `region` / `area` / `water` / `inherited`, the last reached from a
downstream water by the tributary walk). **Smaller speaks first:**

| rank | rule is… | example |
|---|---|---|
| **−1** | any `superior` authority | "Fishing is prohibited in Ecological Reserves" |
| **0** | scope = `water` | a rule written for the Chilliwack |
| **1** | scope = `inherited` | a rule reaching here from the river downstream |
| **2** | scope = `area` | "MUs 1-1 to 1-6, no fishing Jul 15 – Aug 31" |
| **3** | scope = `region`, by a **region** | "Trout/char: 4" |
| **4** | scope = `region`, by the **province** | "All wild steelhead must be released" |

**Scope beats authority**: a province-authored rule for one lake (rank 0) speaks there before the
region's table (rank 3), so "this water overrides regional" is about what a rule *binds to*, not
who wrote it. **A superior authority is outside the ladder**: at −1 nothing below can open what it
closed. This replaced a hand-set `rank` integer, whose conflation of the two axes was the root of
a family of bugs where a rule for one river bound every water in its region.

**⑥ Row → table** — the reader sees:

```
Hatchery trout and char    2 a day, between them · at least 30 cm · only 1 over 50 cm
    bull trout, Dolly Varden, lake trout   only 1, at least 60 cm
    steelhead                              2 may be over 50 cm, 10 a year
Wild trout and char        Put it back
```

> **The builder takes a list of rules and nothing else.** `build(rules, kind, here, label)`
> cannot ask which region it is looking at — a test fails if it does. Choosing *which* rules is
> a separate job (`rules_for`). That is why a region, an area inside it, and one stretch of one
> river all go through the same code.

---

## Part 2 — What picks a table

Two independent questions: **where**, and **when**.

### 2.1 WHERE — a base, then amendments

Not three alternatives. A two-step lookup:

| step | you get | how many |
|---|---|---|
| 1. the **base** — the region, and the area inside it if you are in one | a whole settled table | 22 region×kind tables, 47 named places |
| 2. the **amendments** — rules written for this water and stretch | a short named list | 599 water rules + 62 inherited, across 102 stretches |

> **A stretch is its base table plus its own amendments, and nothing else.**

That gives the verification order. Get the region tables and areas right — they are checked line
by line against the printed synopsis — and a stretch can then only be wrong in its own short
amendment list, each item attributable to one named rule.

**The code does not do this yet, and it costs answers.** `build.base()` re-derives the base from
whatever rules the atlas happened to bind to that stretch, instead of looking it up by region.
Over the 100 single-region stretches that ship:

| | |
|---|---|
| base matches its region's standing base | **37** |
| base **differs** | **63** |
| region-wide rules missing from some stretch's base | **18 rules, 98 omissions** |
| rules in a stretch's base its region does *not* have | **0** |

One-directional — a stretch can only **lose** region-wide rules — and building the proposed way
changes the answer on **23 of the 100**. The Fraser still shows a *Protected species* row, but the
**provincial** rule is holding it up; Region 2's own is missing from that stretch's base. The
table is right by luck of a second rule, and nothing says which lines stand on their own.

### 2.2 WHEN — the year, cut into stretches

A **stretch** is a run of days over which nothing changes. The year is cut wherever any counter
or gear term comes into or goes out of force: **53** across the 22 tables, ten of which have
exactly one and never change.

Region 3's streams have six:

| | Jan 1 – Jun 30 | Jul | Aug 1 – Oct 14 | Oct 15 – Oct 31 | Nov – Dec |
|---|---|---|---|---|---|
| everything | **closed** | open | open | open | open |
| bull trout · Dolly Varden | closed | 4 | **put back** | **put back** | 4 |
| lake trout | closed | 4 | 4 | **put back** | **put back** |

`state.conditions(region, kind)` multiplies the two questions — every (area, stretch) pair a
region has: **165** across the 22 tables, over **47** places.

It was 218 over 69 until two places turned out to be one: the book closes the National Parks in
one sentence, then Pacific Rim, Gwaii Haanas and the Gulf Islands in another, and those three
*are* National Park Reserves. `state._fold` drops the narrower name when every area id it names
sits under a kind the wider one claims wholesale. The test is that **ground**, not matching
tables — Ecological Reserves draw an identical table and are a different place.

The duplicate looked harmless because the reserve rule bound **nothing**: its extent is prose, so
it became an undrawable "somewhere" caveat — while the area selected had been built *from that
rule's own extents*. The place is drawn; it is the one picked. `state._here` clears the caveat
for an area's own rule.

### 2.3 Base + amendments, worked

Measured: **60 stretches** across **12 of the 22 base tables** really change their region's table.
Both sides come out of `state.build`, and the difference is read off the two display trees.

| base table | amending stretches | base table | amending stretches |
|---|---|---|---|
| Region 1 · lakes | 1 | Region 4 · streams | **19** |
| Region 1 · streams | 4 | Region 5 · streams | 9 |
| Region 2 · streams | 11 | Region 6 · lakes | 1 |
| Region 3 · lakes | 1 | Region 6 · streams | 3 |
| Region 3 · streams | 2 | Region 8 · lakes | 3 |
| Region 4 · lakes | 3 | Region 8 · streams | 3 |

The ten not listed have no named water that changes them — every water reads its region's table
unaltered, which is the invariant this arrangement exists to make checkable.

**A. Fording River at Josephine Falls — Region 4 streams — the inherited case.** The one stretch
where a rank-0 rule and six rank-1 rules speak at once:

| rank | scope · via | says |
|---|---|---|
| **0** | water · reach | **No Fishing** |
| 1 | inherited · trib | No Fishing, Sept 1 – Oct 31 |
| 1 | inherited · trib | Trout/char daily quota = 1 (none under 30 cm) |
| 1 | inherited · trib | bait ban, June 15 – Aug 31 |
| 1 | inherited · trib | Bull trout catch and release *(twice — Elk River's tributaries and Kootenay Lake's)* |
| 1 | inherited · trib | Class II water when open, including tributaries |

`via: trib` is the Fording answering to **Elk River's tributaries** — a rule about another water,
reaching this one by the walk, at rank 1 so the Fording's own voice speaks first.

| | base | the Fording |
|---|---|---|
| bull trout, Dolly Varden | 1 | **0** |
| rainbow, cutthroat, westslope | 2 | **0** |
| the trout-and-char budget | 2 across 13 | **gone** |
| the whitefish budget | 15 across 2 | **gone** |

One rule and the whole table goes. The six inherited rules are still settled, simply behind a
closure — that is the difference between a table that is **shut** and one that is **empty**.

**B. Okanagan Lake — Region 8 lakes — narrows, opens and reshapes at once.** Four water rules:

| | base | Okanagan Lake | the rule |
|---|---|---|---|
| rainbow trout | 5 | **2** | `"Rainbow trout daily quota = 2 (only one over 50 cm)"` — **narrowing** |
| largemouth, smallmouth bass | 0 | **8** | `"bass daily quota = 8"` — **opening** what the region closed |
| yellow perch | 0 | **20** | `"yellow perch daily quota = 20"` |

And the budget is **reshaped**: the region's `1 over 50 cm` across 13 fish becomes 12 — the
rainbow left it for its own — plus a new `8` across the two bass. A layout that repeats a shared
numeral on each member row cannot say "this fish is no longer in that number".

**C. Atlin Lake — Region 6 lakes — a member's own cap, three times.**

| fish | per day | its own cap |
|---|---|---|
| lake trout | 3 *(inside the region's 5)* | `at most 1 over 60 cm` |
| Arctic grayling | 3 | `at most 1 over 35 cm` |
| northern pike | 5 | `at most 1 over 70 cm` |
| whitefish | **5** *(the region's 15, replaced)* | — |

Each is a **pooled quota on a size class**, not a size gate (`Size.is_gate`). A gate sends a fish
back; a cap says how many of your day's fish may be that big, and reading the second as the first
put "only 1 over 50 cm" in a size column where a length belongs. Atlin carries the corpus's most
awkward sentence — *"EITHER none over 60 cm, OR only 1 over 60 cm and the other 2 must be 60 cm
or less"* — which resolves to exactly that cap: both branches agree on a creel of three.

---

## Part 3 — Closures

**If the water is shut, do not print a table.** Twelve rows reading "No fishing" is one sentence
twelve times, and a reader who scrolls past a banner reads a limit on a closed river. Instead:

```
⛔  CLOSED
    Every stream in Region 3 is closed until 30 June.
    why   "Spring closure: No Fishing in any stream in Region 3 from Jan 1-June 30."
          Region 3 · region-wide
```

1. **Say when it opens**, not just that it is closed. Adjacent open stretches merge — the year is
   cut wherever *any* rule changes, so a water that reopens on 1 July and stays open reads as
   four separate openings otherwise.
2. **Carry the provenance** — the sentence and who wrote it is the one thing a reader can check
   against the book. Only the rules that shut the *water*: a rule closing one fish is a fact
   about that fish, and listing "white sturgeon is protected" beside the spring closure reads as
   though the sturgeon were why nobody may fish.
3. **A closure must be liftable.** The book writes exemptions, and a water rule can lift a
   regional closure. The card is produced by *asking the ledger*, never by a flag — if a water's
   rules lift the closure, it has no closed stretch and gets a normal table.

**"Unwritten" is not "shut".** Most province-wide fish have no number at all; counting those as
closed alongside the one real closure reported the whole province shut to fishing.

---

## Part 4 — The logic tree

Input: a settled `Ledger`. Output: the display tree. **No region is ever named.**

**1 · Origin has two values, not three.** `Origin.both` is the *absence* of the question, not a
third value. Run the pass twice, wild and hatchery; a `both` rule answers True to both.

> `r4` "2 from streams (must be hatchery)" → hatchery pass only · `r6` "Wild trout/char" → wild
> pass only · "Whitefish: 15" → both, identically.

**2 · A zero headline annihilates.** Under *put it back* or *no fishing*, a size limit, a cap, an
annual ceiling and a possession multiple are all still true and all unspendable.

> Region 2 streams, wild bull trout holds `release` + `only 1 char between them` + `must be at
> least 60 cm`. The reader should see: **Put it back.**

This one step fixes 8 of 20 bands with no single answer, 28 orphan entries, and 16 of 22 tables
drawing a trout-or-char outside the trout-and-char group.

**3 · Fish a reader cannot tell apart become one line** — grouped by *what binds them*, not by
identity. Region 2 streams, wild: all 15 trout and char have one live counter, `release` → one
line, not three.

**4 · Groups nest, and the nesting is derived.** A counter is a group when it is a pooled daily
quota over more than one fish. Parent = the rule the book says it is a clause of (`within`), and
only failing that the narrowest budget containing it.

```
Region 2 streams, hatchery:
  15 trout and char ── 2 a day, pooled ───────── r4
        └── bull trout, Dolly Varden, lake trout ─ 1 ── r5
        └── steelhead ──────────────────────── 2 >50 ── r3
```

Containment alone left 52 clause pairs unparented: "1 over 50 cm" inside "4 trout and char"
reaches exactly the same fish — it narrows by **size**, not species — so the sets are equal and
neither was the other's parent. Ordering by narrowness (a clause is narrower than a plain number,
a sized one narrower than an unsized) fixes it and makes a cycle impossible. **222 clauses
parented, 0 orphans.**

**5 · Sizes, simplified in a fixed order.**

```
(a) UNREACHABLE COUNT CAP   cap ≥ the number it sits inside            → drop
      Haida Gwaii streams: "3 Dolly Varden" inside a headline of 2.
      Same rule on Haida Gwaii LAKES, inside 5 → kept. Same code, decided by the data.
(b) UNREACHABLE CLASS CAP   same test on a size class                  → drop
      only when the cap is NOT shared with other fish.
(c) DUPLICATE               same bound, same number                    → keep the wider
(d) NEVER drop a shared cap. If pooled and reaching fish beyond this line, it binds
      THROUGH them even where this line's own floor makes it look pointless.
      Region 2: a bull trout must be ≥60 cm, so "only 1 over 50 cm" looks redundant — but
      keeping it spends the family's single over-50 fish, and a 55 cm rainbow is then
      illegal. 47 of 88 size statements are this kind.
```

**6 · Where the origin control goes.** Its own number differs wild vs hatchery → two tables;
only some members differ → one table, those lines carry the origin; nothing differs → one table,
no origin anywhere. Measured: **0 of 22 differ in a shared number**, so in practice always one
table; 18 differ on steelhead alone, 4 across the wider family.

**7 · Seasons attach where they reach** — every fish in the table → a banner; every fish in a
group → on the group; otherwise on the line, labelled with the fish it names.

> Region 3 streams: bull trout and Dolly Varden go back Aug 1 – Oct 31, lake trout Oct 15 – Jan
> 31. They share one number and do **not** share a season, so the season sits on the member. A
> season shown against a fish it does not name closes a legal fishery.

---

## Part 5 — The table

### 5.1 The rules the layout obeys

Each was forced by a measured defect.

1. **Never a count in place of a name.** `9 kinds of all game fish` tells a reader holding a
   burbot nothing, and nine of twenty-eight game fish are not "all game fish". The group's name
   only where the set **is** the group; otherwise every member, however many.
2. **A shared number is drawn once, as a budget.** Member rows say what they *spend*, never the
   figure again.
3. **A member may state its own number** when it is smaller — Region 6 gives trout 1 inside a
   family of 5, and forbidding the member a number puts a reader five fish over.
4. **A released fish leaves the group**, and every sharer list, that day.
5. **Nothing kept ⇒ no size.** "at least 60 cm" beside "Put it back" reads as permission — and
   "any size" is not "no size", it is the most permissive size there is.
6. **Every counter that binds is drawn somewhere.** Counters are **partitioned** — one answer,
   the shared ones budgets, the rest the fish's own — and one landing nowhere **raises**. A
   counter the reader never sees always reads as *more* fish.
7. **A size has two ends**, one statement each: "at least 30 cm · only 1 over 50 cm" is one
   muddled sentence, two rows are two facts.
8. **The definition reaches the number, not only the words.** A steelhead *is* a rainbow over
   50 cm, so "1 over 50 cm" on that row is simply 1.
9. **The cooler follows the day.** A possession limit is N × a *daily* one, and that daily is the
   family's, not this fish's.

### 5.2 What the tree carries

One `table` per (ledger, kind, date, label). `names` maps every species code to its name once —
`leaf["fish"]` sorts by code and `leaf["members"]` by name, so zipping them names the wrong fish.

| on a **leaf** | |
|---|---|
| `answer` | the sentence the **book** writes, with `source` |
| **`most`** | **the number you may actually keep** — the answer with every budget, cap and definition applied |
| `sizes` | one statement per bound: `floor`, `ceiling`, `band`; `definition: true` where the word set it |
| `spends` | the budgets it counts against, innermost first |
| `own` | limits that are this fish's alone today — mostly a pooled cap the season narrowed to one survivor |
| `annual`, `possession`, **`may_have`** | the licence year, the cooler as written, the cooler after `most` |
| `complex` | which of the two tables it belongs on |
| `same` / `both` / `sides` | one line, or one line per origin |

| on a **budget** | |
|---|---|
| `n`, `size`, `sized` | the number and the size class it counts |
| `spends`, `spends_by`, `only` | who spends it **on this date**, per origin |
| `parent`, `clause_of` | the budget it sits inside, by `Allowance.within` first |

### 5.3 One table, and two kinds of fish

**Wild and hatchery are one table.** Drawn as two, the difference in all 22 regions is the same
single fact — *the wild trout go back* — printed as a second page of fourteen rows. The pages did
not even share a shape: with nothing keepable the wild page has no shared numbers at all, so
Region 1's streams showed a bare list beside a nested one. The origin goes on the **line** that
differs:

| Fish | Size | Per day | May have |
|---|---|---|---|
| **Steelhead** `10 a year` | | | |
| &nbsp;&nbsp;`WILD` | — | **Put it back** | — |
| &nbsp;&nbsp;`HATCHERY` | must be at least 50 cm | **1** | 2 |

Most rows carry no origin. `display.merged` groups by **both** origins at once rather than
matching two groupings afterwards: the groupings differ (wild releases every trout into one line;
hatchery keeps six apart) and pairing them cannot be done without guessing.

**And two tables, by fish.** A trout shares a budget with a clause with a size class, and half the
trout go back; a burbot is a fish and a number. In one list the burbot looks complicated and the
structure is buried. So **trout, char and salmon** in one table, **everything else** in another,
split by the book's own families. A budget follows its spenders — Region 6's whitefish share
fifteen and are not complicated by it.

A line is wholly in one table or the other, and that is not free: on the province's tables every
fish has the same answer, so grouping on the answer alone made one line of all twenty-eight, to
be drawn under "Trout, char and salmon" with the crayfish in it. Which table a fish belongs to is
part of the grouping key — and so is **the rule**, because kokanee, white sturgeon and the char
all read "Put it back" in Region 1 from three different rules.

### 5.4 Tap a fish

🔍 `bull trout` — Region 2 streams

| **BULL TROUT** | |
|---|---|
| **Wild** | **Put it back** |
| **Hatchery** | **Keep 1** · minimum 60 cm |

| that 1 also spends | shared with |
|---|---|
| 1 of only **3 chars** a day | Dolly Varden, lake trout |
| 1 of your **2 trout and char** a day | 13 kinds |
| your **1 fish over 50 cm** a day | 12 kinds — not steelhead, which has its own 2 |

### 5.5 Two decisions

**A fish-picker dropdown — no. A search that jumps — yes.** A dropdown's option list *is* the
member enumeration from rule 1, so it either duplicates the page or is the only place the members
appear. And filtering a shared-budget table to one fish hides the other spenders, destroying what
the table exists to show. A search that **scrolls to and highlights** costs one line — matching
**members, not headings**, because on 46 of 53 tables the word a reader types (`splake`, `dolly
varden`, `westslope`) is in no heading.

**A row with no answer says so in words.** 12 exist. Never a blank cell: a blank reads as "no
limit", the most permissive possible failure.

---

## Part 6 — How you may fish

The quota half answers *what may I keep*; this answers *how may I fish*. They share one
abstraction — "what you may keep **by this method**" is an `Allowance` in a `Ledger` — and
nothing else. One structural difference decides the layout:

> **The gear display unit is a PERIOD, not a year.** A year view is a union of days, and a union
> of days is a stack of contradictory tables.

### 6.1 The bait contradiction

Haida Gwaii · streams, asked for **1 December**, prints four bait lines:

| rule | says | in force |
|---|---|---|
| `zp:bait::bait.r1` | fin fish may not be used as bait | always |
| `zp:bait::bait.r4` | freshwater invertebrates **may** be used | always |
| `zp:bait::bait.r6` | roe **may** be used | always |
| `z1:hg_bait_ban_streams::…r1` | **no bait** | Nov 1 – Apr 30 |

Three of the four name a bait you may use, on a day you may not use any.

One line causes it. `_settle_rig` asks `_whenever(ban, allowance)` — *is the ban live every day
the allowance is?* The ban is seasonal, the allowance year-round, so **a seasonal ban can never
fold a year-round one**. No new logic is needed: Region 1 carries the same ban with the dates
removed and already prints one bait line — same code, opposite outcome, decided only by the date
guard. Settle *inside a period* and `_whenever` is trivially true. The book agrees: `bait.r4` ends
*"…unless a bait ban applies."*

### 6.2 The logic tree

```
0  CUT THE YEAR into periods — rows.schedule(rows, also=gear_terms) already does this
1  SETTLE INSIDE THE PERIOD  — drop terms live nowhere in it; treat the rest as year-round
2  THE WATER FIRST           — a closure prints the card of Part 3 and one rule
3  VERDICT PER METHOD        — the standing contest; a ban beats a permit at equal rank
4  ON THE PROVINCE'S TABLE   — a fourth verdict, "depends where", from region_limited
5  A ZERO VERDICT ANNIHILATES— but a DUTY survives
6  RIG CONDITIONS ACCUMULATE — the ladder only picks between two statements about one tackle
7  A SHARED CONDITION IS DRAWN ONCE, over the methods that can spend it — and it NAMES them
8  HOIST PER CONDITION       — one shared by ≥2 live methods hoists, carrying its sharers
9  NO TWO PRINTED LINES CONTRADICT — checked against the rendered output
```

**Step 5's counter-example defines it.** Snagging reads *not allowed* on all 22 tables and carries
*"any fish willfully or accidentally snagged must be released immediately"* — a **duty that
exists because you did the banned thing**. So the test is on the counter, not the method:
`release` survives, `closed` goes. Keeps 22 snagging rows, drops 31 of unspendable text.

### 6.3 The layout — a verdict ledger, then one rig list

Chosen from the shape of the corpus. Across 22 tables, 176 (table, method) rows:

| | |
|---|---|
| rows reading **not allowed** | **94 of 176** |
| tables where the rig list is **entirely shared** | **19 of 22** |
| rig lines per table | 7 – 11 (median 9) |
| longest single printed condition | **440 characters** (Region 8's turtle advisory) |

Half the page is "no", so a card per method spends four lines to say nothing 94 times. At 400px a
full-width line holds ~45 characters and a three-column band leaves ~25 — **the rig block must be
a list, not a table.** Only the verdict ledger and the species tables may be tables.

**Region 1 · streams — all year.** A verdict ledger (rod and line ✅ · ice fishing ✅, 2
conditions · crayfish traps ✅ release everything else · set line, spear, nets, chumming ❌ ·
snagging ❌ *and release anything you foul-hook*), then one rig list under it: bait — none; hooks
— one, barbless; lines — 1 per angler, at most 1 artificial fly, at most 1 kg of weight.

**Haida Gwaii · streams — 1 December.** *Bait — none, until 30 April.* Eight lines instead of
eleven, and the contradiction is gone — not filtered, **folded**, exactly as Region 1's
year-round ban already folds.

**All of B.C. — where the verdict is a map.** Spear fishing is ❌ in Regions 1, 2 and 4; ✅ for
non-game fish and burbot in 3, 5, 6, 7 and 8; and never for game fish, salmon or protected
species. Today that row reads **`allowed`** while its governing term is stamped *"closed here by a
stricter rule"* — a permission a reader in Region 2 would act on.

### 6.4 Three more defects, verified

| | measured |
|---|---|
| lake tables printing **two live line limits as peers** | **11 of 11** |
| tables whose *Also* block mixes a **permission and a prohibition** with identical polarity | **22 of 22** |
| Region 8's crayfish trapping **governed by a 440-character turtle advisory**, the real permit folded away as "says the same thing" | both kinds |

The line-limit pair is one sentence in the book — *"Angle with more than one line, **EXCEPT** a
person alone in a boat on a lake may angle with two"* — so r2 is a carve into r1, not a peer, and
the existing `carves` machinery would print it correctly once curated.

### 6.5 What the model must change

| # | where | change |
|---|---|---|
| 1 | `method_provenance.base_table` / `section` | take a **period**; emit the period list from `rows.schedule` |
| 2 | `MethodTable.__init__` | settle **inside** the period |
| 3 | `_whenever` (`method.py:373`) | inside a period it is trivially true; stop deciding the printed answer |
| 4 | `MethodTable.calendar` | fingerprint on `(closure, standing, live rig, live keep)` |
| 5 | `MethodRow.verdict_word` | a fourth verdict, `depends`, where `region_limited` is non-empty |
| 6 | `row_json` | under a zero verdict emit no rig; keep **duties** (`release`), never `closed` |
| 7 | `terms_of` | an **advisory must not govern** a method |
| 8–9 | `hoist` | band members are hook methods **whose verdict is allowed**; hoist per condition, carrying sharer names |
| 10 | `method_comply.audit` | the contradiction gate, **against the rendered output** |

---

## Part 7 — What licence do I need? (research, not a table)

No licence table is being built. This is the parameter set one would need, and what the corpus
can answer today.

| document | trigger | in the corpus? |
|---|---|---|
| **Basic angling licence** (annual / one-day / eight-day) | any sport fishing, 16+ | the requirement yes; **the durations no** |
| **Classified Waters Licence** | a classified stream in its classified period | **78 rules** |
| **Class I / II day licence** | non-resident or alien on a classified water | class yes; the per-day purchase no |
| **Steelhead Conservation Surcharge Stamp** | targeting steelhead *anywhere*, keep or release | **49 rules** |
| **Non-tidal salmon stamp** | keeping a salmon other than kokanee | 1 rule, `on_retention` |
| **Kootenay / Shuswap rainbow · Shuswap char stamps** | a rainbow > 50 cm or char > 60 cm on named waters | 6 rules |
| **White Sturgeon Conservation Licence** | Fraser watershed, Mission → Williams Lake River | 1 rule |
| **National Park Fishing Permit** | inside a park — *and a B.C. licence is not valid there* | 2 rules |
| **Creston Valley WMA permit · landowner permission** | named places | 4 rules |

**Waivers the book states:** under 16 and resident (no licence, own quota); under 16 non-resident
(with a licensed adult); *"If you are an Indian and a resident of B.C., you are not required to
obtain any type of fishing licence"* — Métis **are** required. **Family Fishing Weekend** is **0
rules in the corpus**.

**Ask the angler once (7 facts, none asked today):** residency · guided · 16+ · 65+ · Indian &
B.C. resident / Métis / disabled · target species · intend to keep.

**Read off the water and date (7, of which 6 exist):** classified, its class, and whether the date
is in the window · which licence to buy · the steelhead-stamp window and exceptions · park
membership · the white-sturgeon reach · named-stamp waters · non-resident day allocation *(text
only, on 66 of 73 rules)*.

| what the corpus holds | measured |
|---|---|
| `document_required` rules | **148** — classified 78 · steelhead stamp 49 · basic licence 10 |
| `water_class` set | **71 rules — 62 Class II, 9 Class I** |
| `angler_class` set | **38 of 3,422 rules — 1.1 %** |
| `issuing_jurisdiction` | **0 rules** |

> **The water half is nearly done; the angler half is barely started.** Which stamp, which permit,
> whether classified and when — all in the corpus and bound to sections. Residency, guiding, age
> and status are not, and `app/packages/core/src/status.ts` reads none of the fields.

**To curate.** Three CW-marked waters have no catalogue entry (`QUINN CREEK CW 4-22`,
`SKOOKUMCHUCK CREEK CW 4-20`, `KILBELLA RIVER CW 5-7`). `BIGHORN (Ram) CREEK CW 4-2` is printed
classified but its only rule is a cross-reference, so its class is unknown. Five waters — West
Road, Babine, Stellako, Sustut, Telkwa — carry `water_class` on a `steelhead_stamp` rule rather
than a `classified_waters_licence` one, so a query keyed on `document` misses all five. **Do not
key on the `Classified` entry symbol**: 21 entries carry it, 71 carry a class or CW rule. The
Skeena's two Class II sections need separate per-day licences and have no identifier.

**Unknowns.** The corpus cannot say what a licence costs, choose between annual / one-day /
eight-day, apply the Family Fishing Weekend waiver, name the Skeena section licence, or give
Bighorn (Ram) Creek's class.

---

## Part 8 — Where this connects to the app

```python
from pipeline.regs.table import state as ST
t = ST.water_state("Fraser River", 18)     # the SAME builder as a region
```

> ⚠ **The gap that matters.** `data/generated/regs/sections.json` covers **22 waters / 102
> stretches**, hand-picked. The bundle holds **1,956,787** section-to-rule rows. Region tables are
> complete and bundle-derived; *this water* is not. Everything needed to generalise is already in
> `section_ruleset` / `ruleset`, including how a rule reached the water (`via`). Nobody has
> written it. **This is the single biggest thing between here and the app.**

| | offline | the client does |
|---|---|---|
| 22 region tables · 53 dated stretches · areas | yes | pick today, pick the area |
| section tables | yes | pick the day |
| dated section views | **no** | apply the date |
| creel arithmetic | impossible | call the oracle |
| map colour | **no** | see below |

**Map colouring.** Nothing precomputed; the app computes it live —
`core/status.ts::evaluate({rules, on, group})` → `closed | restricted | open | unknown`, one query
per chunk plus O(rules) per section. Fine for a viewport, not a province. Two traps: a `take: 0`
carrying a **method** is not a closure (reading `take===0` alone called **1,674 of 1,693**
rulesets closed), and that four-state ladder is deliberately coarser than the `Ledger` — do not
mix vocabularies. The missing piece is a precomputed **`rule-set × stretch → outcome`** table;
rule sets are already interned (210,095, collapsing to far fewer answers).

**The oracle.** `oracle.may_i_keep(ledger, fish, on, creel) -> Verdict` — **1.1 ms** a call. The
acceptance test for everything above, because it decides from the same counters the table draws.
Its `reasons`, `decided_by` and `checks` are now in one order; they were not, so every caller
pairing a reason with its rule got the wrong rule.

---

## Part 9 — What is still wrong

Reproduced against the running builder. Everything in the display tree that earlier drafts of
this document listed as broken is now fixed and held by `pipeline/tests/test_regs_display.py`;
what remains is below.

| | measured |
|---|---|
| a stretch's base re-derived from bound rules rather than looked up by region | **63 of 100** differ; 23 change an answer (§2.1) |
| **every gear defect in Part 6** — settled over a year rather than a period, and the four it causes | §6.1, §6.4; the ten changes are §6.5 |
| rows with **no answer at all** | **12** — Region 7a streams gives nine species nothing |
| a clause drawn against a parent that was **replaced** | Region 8 streams: `"1 over 50 cm"` is `within` `"Trout/char: 5"`, replaced on streams by `"4 from streams"` |
| no precomputed map-colour field | province-wide colour impossible (§8) |
| `sections.json` covers 22 waters where the bundle has 1.96 M section-rule rows | §8 |

### The gate to build against

> **No two printed lines may contradict each other** — checked against the **rendered output**,
> not the rules going in. An input-seeded check has laundered failures three times in this
> project. Every test in `test_regs_display.py` compares the tree against a number derived
> independently from `Ledger`, never against the tree itself.

### What the display tree cost

Three independent reviews found seven defects in the first build, and **three of the seven were
one**: the tree drew a headline and a shared budget, and a counter that was neither vanished.
Each fix is stated above as the rule it became (§4, §5.1); the evidence is that they are closed:

| | |
|---|---|
| over-statements across 19,360 species-slots | **0** |
| clauses parented · orphans · cycles | **222 · 0 · 0** |
| seasonal budgets in a standing view | **0** |
| tables differing between origins in a shared **number** | **0 of 22** |
| mutants caught | **16 of 18** |

The two survivors — dropping `own` or `most` from `_shape` — are **equivalent on this corpus**,
because every table already differs between origins on `annual`. A property of the data, not a
hole in the tests, written down rather than rounded up.

**Four defects the rewrite itself introduced**, all under-statements no over-statement sweep can
find, and all caught by the tests: a sized budget clamping every member (steelhead's 1 onto the
Arctic char's 5 — a question about the fish, never the budget); sharer lists naming the wrong
fish; the cooler ignoring the day; and one line citing one rule for fish three rules had released.

### `Row.moot` is three-way, not two

A size gate under "Put it back" reads as permission, so `moot` cannot end `and not a.is_zero`.
But suppressing every clock off a daily zero hides an annual ceiling you may lawfully be spending
elsewhere — and the guard for *that*, `and a.period == "daily"`, broke two tests, both rightly:

| shape | suppress all | `daily` only | true |
|---|---|---|---|
| a possession multiple — "twice the daily quota" over a release | moot ✓ | **2 beside "Put it back"** | twice nothing is nothing |
| an independent annual ceiling, water released all year | moot ✓ | live ✗ | none can be spent *from here* |
| the same ceiling, water shut only **Aug 15 – Dec 31** | **hidden all year** ✗ | live ✓ | needed on the open days |

A **derived** counter is moot whenever its parent is; an **independent** clock only where the
zero is year-round.

### What the page stopped doing

`app/design/standing/body.html` went from **1,353 lines to ~770** and decides nothing. What went
was the browser re-deriving in JavaScript what the ledger had already settled in Python — every
one of those existed twice and the two drifted, so when the emitter learned who could really
spend a number (`spends`) the page went on reading `reaches` and the fix never reached a reader.

The prototype page is gone too. `corpus.py` and `build.py` each opened a 7.7 MB design file at
import and pulled a 4.2 MB JSON block out with a regular expression — the pipeline's input was a
web page nobody could delete. It is `data/generated/regs/sections.json` now, written from the
bundle by `pipeline/tools/build_section_data.py`.

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
10. **Key order in `sections.json` is data.** Writing it sorted reordered `WATERS` and moved which
    waters a test's slice covered.
