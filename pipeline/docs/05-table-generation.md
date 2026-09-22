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

**③ Rule** — what `corpus.rules()` hands the table layer:

```
id       : z2:trout_char_quota::trout_char_quota.r4
species  : ["TROUT_CHAR"]        ← all 15 trout and char
origin   : hatchery              ← this is the ONLY reason wild and hatchery differ here
water    : stream                ← does not apply on lakes
take     : 2
```

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

Nobody wrote that table. It falls out of `Source.rank`, which is derived from two axes —
**who wrote it** (province / region) × **what it binds to** (region / area / water).

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

**WHERE** — one of:

```
a region                Region 2
an area inside it       Region 1 → Management Units 1-1 to 1-6
a water and stretch     Fraser River, stretch 19
```

**WHEN** — a day.

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

## Part 5 — Four layouts to choose between

All four show the **same table**: Region 2 · streams. It is the hardest one for display because
it has everything — a wild/hatchery split across the whole family, a shared number, a size
floor, a shared big-fish cap, a tighter cap on three chars, a steelhead exception to that cap,
and an annual limit. All figures are real.

---

### Option 1 — Two columns: wild and hatchery side by side

```
REGION 2 · STREAMS                       1 July – 31 Dec

                              WILD          HATCHERY
 ─────────────────────────────────────────────────────
 TROUT AND CHAR            Put it back      2 a day
 all 15 kinds                            between them
                                          4 in possession
                                     ≥ 30 cm · only 1
                                          over 50 cm
   ├ 9 kinds of trout      Put it back      ↳ the shared 2
   │  and char
   ├ bull trout, Dolly     Put it back      only 1 of
   │  Varden, lake trout                    these three
   │                                        ≥ 60 cm
   └ steelhead             Put it back      ↳ the shared 2
                                            2 may be over
                                            50 cm · 10 a
                                            year
 ─────────────────────────────────────────────────────
 Whitefish                    15 a day between them
 Bass                         20 a day between them
 Black crappie                20 a day
 Crayfish                     25 a day
 Kokanee                      Put it back
 White sturgeon               Put it back
 Protected species (12)       You may not fish for them
```

**For:** one screen, nothing hidden, the wild/hatchery difference is the most visible thing on
the page. **Against:** two number columns at 400px is tight; and on the 16 tables where only
steelhead differs, one column is a near-duplicate of the other.

---

### Option 2 — Origin asked once, up front

```
REGION 2 · STREAMS                       1 July – 31 Dec

 ┌───────────────────────────────────────────────┐
 │  Is your fish wild or hatchery?               │
 │  Look for a clipped adipose fin.              │
 │                                               │
 │     [ WILD ]            [ HATCHERY ]          │
 └───────────────────────────────────────────────┘

 ── you picked HATCHERY ──────────────────────────

 TROUT AND CHAR                          2 a day
 all 15 kinds                       between them
                                  4 in possession
 every one at least 30 cm
 only 1 of them over 50 cm

   9 kinds of trout and char        the shared 2
   bull trout, Dolly Varden,        only 1 of these
     lake trout                     three · ≥ 60 cm
   steelhead                        the shared 2
                                    2 may be over 50 cm
                                    10 a licence year

 ─────────────────────────────────────────────────
 Whitefish 15 · Bass 20 · Black crappie 20
 Crayfish 25 · Kokanee and white sturgeon go back
 Protected species — you may not fish for them
```

**For:** widest layout for the detail; matches how the book is written ("and you must release:
wild trout/char from streams"). **Against:** it hides half the answer behind a choice. A reader
who mis-taps reads *"2 a day"* for a fish that must be released — so this can only be a
**required** choice, never a silent tab, and only on the 2 tables where the family number itself
depends on origin.

---

### Option 3 — One table, origin only on the lines that need it

```
REGION 2 · STREAMS                       1 July – 31 Dec

 TROUT AND CHAR
 ┌─ hatchery ─── 2 a day between them · 4 in possession
 │               at least 30 cm · only 1 over 50 cm
 │
 │   9 kinds of trout and char            the shared 2
 │   bull trout, Dolly Varden,       only 1 of these three
 │     lake trout                              ≥ 60 cm
 │   steelhead            the shared 2 · 2 may be over
 │                        50 cm · 10 a licence year
 └─ wild ────── Put them all back
                all 15 kinds, every size

 Whitefish                     15 a day between them
 Bass                          20 a day between them
 Black crappie                 20 a day
 Crayfish                      25 a day
 Kokanee                       Put it back
 White sturgeon                Put it back
 Protected species (12)        You may not fish for them
```

**For:** nothing hidden and nothing duplicated; the origin appears exactly where it changes the
answer, and reads as the book's own "you must release" block. Scales down cleanly — on the 16
steelhead-only tables the `wild`/`hatchery` pair appears on one line and the rest of the table is
untouched. **Against:** the wild answer sits below the hatchery block, so a wild-fish angler
reads past detail that does not apply to them.

---

### Option 4 — Look up one fish

```
REGION 2 · STREAMS                       1 July – 31 Dec

   🔍  [ bull trout                            ]

 ┌───────────────────────────────────────────────┐
 │  BULL TROUT                                   │
 │                                               │
 │  WILD        Put it back                      │
 │              "Wild trout/char from streams"   │
 │                                               │
 │  HATCHERY    keep 1                           │
 │              at least 60 cm                   │
 │                                               │
 │  That 1 also counts against:                  │
 │   · 1 of only 3 chars — shared with Dolly     │
 │     Varden and lake trout                     │
 │   · 2 trout and char a day — shared with      │
 │     all 15 kinds                              │
 │   · 1 fish over 50 cm a day — shared with     │
 │     all 15 kinds                              │
 │                                               │
 │  In possession   4 trout and char             │
 │                                               │
 │  ▸ show the whole table                       │
 └───────────────────────────────────────────────┘
```

**For:** answers the actual question ("I am fishing for X") in one screen, with the shared
budgets spelled out instead of implied by indentation — the thing nesting communicates worst.
**Against:** you must know what you are looking for, and it is poor for browsing or for checking
a whole chapter against the book. Best as a *companion* to one of the other three, not a
replacement.

---

### My recommendation

**Option 3 as the table, Option 4 as the tap-through.** Option 3 keeps everything visible and
duplicates nothing, degrades gracefully from the 2 hard tables to the 16 easy ones, and never
hides an answer behind a control. Option 4 then answers the angler's real question without
making them read a hierarchy. Option 1 is the best of the "everything at once" family if you
want no interaction at all; Option 2 I would not ship except on Region 1 and 2 streams, and only
as a required choice.

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
