# Handoff — quota & gear regulation tables

State as of 2026-09-21, branch `redesign/stream-sections`.

**Gates (all green, verified unfiltered, 2026-09-21):**

```
.venv/bin/python -m pytest -q          # no path argument — testpaths covers both suites
  -> 1403 passed, 171 deselected   (2 skipped by name when data/source/official/ is absent)
.venv/bin/python -m pipeline.regs.table.comply         -> COMPLIES, 0 unaccounted
.venv/bin/python -m pipeline.regs.table.method_comply  -> COMPLIES, 0 reaching no table
```

Use `.venv/bin/python` with `PYTHONPATH=.`. The conda python on PATH has no pytest.
`rtk pytest` has once reported a clean suite while tests failed — run it unfiltered.

---

## What was built

The browser used to resolve regulations itself, with four independent precedence
ladders. That is gone. Resolution happens in the pipeline, once, and the page renders
what it is given.

### The model — `pipeline/regs/table/`

- **`authority.py`** — `Authority` (superior / province / region) × `Scope` (region-wide /
  area / water / inherited-by-tributary-walk), **two independent typed axes**. These
  replaced a single `rank` integer that conflated "who wrote it" with "what it binds to".
  That conflation was the root of a whole family of bugs where a region's rule written for
  one named river bound every water in the region. `Source.rank` is derived, never stored;
  `Source.is_base` = region-wide.
- **`ledger.py`** — an `Allowance` is one counter: scope (fish, origin, size class),
  outcome, period, pooled, `Source`, dates. **Closures, releases and size bounds are all
  allowances of zero** — one vocabulary, no special cases. A `Ledger` settles them pairwise
  by the owner's ladder.
- **`build.py`** — `base(rules, water_kind)` is a region's standing table, a pure function
  of (region, water kind); `ledger(...)` overlays a section's overrides. **A water-scoped
  rule cannot enter a base and a stream rule cannot reach a lake, by construction.**
- **`rows.py`** — rows are a derived *view*, not a unit of the model.
- **`oracle.py`** — `may_i_keep(species, length, origin, date, creel)` returns a verdict
  naming the counter that decided it. This is the acceptance test for the whole thing.
- **Gear**: `method.py`, `method_build.py`, `method_deltas.py`, `method_print.py`,
  `method_comply.py`. Gear has three kinds of statement modelled as three — a **permit/ban**
  (one governs), **rig conditions** (never compete; they fold), and **"what you may keep by
  it"**, which *is* the quota `Allowance`. That last one is the only shared abstraction.

### The ladder (the domain owner's words, verbatim)

> Regional always overrides provincial (except full closure), and this water overrides
> regional always (except closures unless they are lifted in this water's regs).

### Two invariants that make debugging tractable

1. **`section table == apply(region base, [overrides reaching this section])`**, with every
   difference attributable to exactly one named override. An unattributable difference is a
   test failure. Gear: 820 deltas over 102 sections, 0 unattributable.
   *This is the thing that stops tail-chasing* — a wrong number points at one delta, not at
   the pipeline.

2. **The checks can fail.** They could not, before:
   - the totality test compared a frozenset against itself (same predicate both sides) — a
     tautology for every possible ledger;
   - `comply` filled 89% of its buckets from the *input record's own fields*, using the same
     tests the generator uses to *skip* a rule, so anything the generator refused to look at
     was accounted for by construction.

   Now every bucket is proved against an **output**, and mutation tests pin it: flipping a
   rule's `type` to `advisory` or adding a stray `water: "lake"` now reports UNACCOUNTED.
   **Never reintroduce a check seeded from the input.** This has laundered failures three
   times in this project's history.

---

## Source of truth

`data/source/fishing_synopsis.pdf` — 88 pages, edition 2025-2027, git-tracked.
Official URL recorded at `pipeline/regs/extraction/extract_synopsis.py:1412`.

**Page map** (each regional quota table is one page; provincial spans 8–12):

| region | 1 | 2 | 3 | 4 | 5 | 6 | 7A | 7B | 8 |
|---|---|---|---|---|---|---|---|---|---|
| page | 15 | 23 | 30 | 36 | 48 | 55 | **64** | 70 | 74 |

Region 7A's page header is a bare "Regional Regulations", not "REGION 7A" — every
heading scan misses it. Region 5's header is doubled-character encoded
("RREEGGIIOONN 55"). Page 15 carries *both* a Region 1 box headed "(excluding Haida
Gwaii)" and a separate "Haida Gwaii Daily Quotas" column.

### ⚠ The repo PDF is NOT byte-identical to what gov.bc.ca serves

Three files, all edition 2025-2027, three checksums: repo copy `6eb14ec7`; the
extractor's `SYNOPSIS_URL` served `930d743a` on 09-16 and `33ba0893` on 09-17 (it moves
day to day); the long-named official file `c76f6851` (stable across both fetches).

**Do not claim byte identity for the repo copy.** What is claimed instead, and tested:
`quota_print.cross_check` and `method_print.cross_check` prove every quota box and every
General Regulations panel in the repo copy reads line for line as the same box in the
regional chapter that *is* byte-identical to gov.bc.ca (10/10 and 9/9).
`test_the_source_is_the_edition_it_claims` pins path, edition string read from page 1, and
md5 — a swapped PDF fails there.

### Nothing the gates need lives outside the repository (2026-09-21)

The print tests once read gov.bc.ca's chapter files from the session scratchpad, which is
ephemeral: four days later the suite was red with `FileNotFoundError`, and the quota
cross-check asserted all-False instead of skipping. Both print diffs now read
`data/source/fishing_synopsis.pdf` (git-tracked) and nothing else.

The gov.bc.ca copies are a **cross-check**, not a dependency: `quota_print.OFFICIAL_DIR`
(`$SYNOPSIS_DIR`, else `data/source/official/`, git-ignored) is where they live when
fetched. A missing chapter makes the two cross-check tests **skip, naming the file**; a
chapter whose md5 is not the one recorded, or whose box differs, fails. They are 6.8 MB
(nine chapters) or 11.6 MB (the long-named full file, `c76f6851`); whether one of those
belongs in git is the owner's call — the suite is green without them.
### Validation rules that must not be relaxed

- **Never compare against a curated `verbatim` field.** A curated verbatim is itself the
  thing under test; diffing against it is a tautology that agrees with itself.
- **A ✓ without a found sentence is a ✗.** This rule caught a false tick where column
  debris had broken the printed sentence while the table side matched.
- The `.txt` extractions in `scratchpad/synopsis/` are **scrambled by rotated map art** —
  use `pdfplumber` layout extraction, not those files.

### Current print agreement

- **Quota: 382/386 lines agree.** The four are one finding — Haida Gwaii "3 Dolly Varden".
- **Gear: 134 lines over 20 standing tables, 0 failures.**
- Both diffs are **executable** — a test fails on a new disagreement *and* on a vanished one.

---

## Open curation defects (data, not code — do not paper over in code)

- **`zp:national_park_reserves.r1` is one place under a second name.** `Source.place` falls
  back to `extent_text` for an area-scoped rule (`authority.py:177`), so this rule's place is
  "Pacific Rim National Park Reserve Of Canada, Gwaii Haanas…" while every other National Park
  rule's place is "National Parks" — and the areas picker, which keys on `place`, offers two
  Where items for one place. It is also **redundant**: its own sibling `r2` lists the seven
  parks as "Kootenay, Yoho, Glacier, Mount Revelstoke, Pacific Rim, Gwaii Haanas and Gulf
  Islands", so the three reserves are inside the parks `superior_closures.r1` already shuts,
  which is why it measures as changing no line a reader reads. Fix by curation (one place
  name), not by matching strings in the picker.

| # | defect | effect |
|---|---|---|
| 1 | `r5:fraser_river@5-2` extents are `{"op":"whole"}` | binds all 20 Fraser stretches; a Region 5 sturgeon closure **shuts the lower Fraser fishery 304 days/yr** |
| 2 | `r3:fraser_river@3-14.r2` unbounded `downstream_of` | 76 days of a closure Region 2 never wrote, on Fraser runs 0–2 |
| 3 | `r4:kootenay_lake_s_tributaries.r1` + its exclusion filed as `advisory` | tributary walk reaches the Elk/Fording; **false closure** on a printed fishery below Elko Dam |
| 4 | four inverted `band: true` flags — `r7:gwillim_lake.r1`, `r7:lower_blue_lake.r2`, `r7:williston_lake_in_zone_b.r4`, `r2:coquitlam_river.r3` | tells the angler to release exactly what the book lets them keep. **`size.py` is correct — do not change it**; ten other rules use identical wording with the flag right |
| 5 | `z5:white_sturgeon` r1–r3 bind 0 sections | 61 days/yr of silence where the book says catch-and-release |
| 6 | `z2:species_quotas.r1` ("Bass: 20, excluding Mill Lake") and `z2:protected_species` bind 0 sections | no bass number anywhere in Region 2 |
| 7 | `z6:steelhead_stream_closure.r1` self-exempts | worked around, not fixed |
| 8 | `z1:bait_ban_streams.r1` reaches Haida Gwaii | the islands get the all-year ban plus their own seasonal one. Quota side already separates HG correctly; this is gear-only |
| 9 | `zp:set_lining.r2–r4` prose `extent_text` + `scope: section` | land on no stretch |
| 10 | `r4:pend_doreille_river.r1` exempts only the region's barbless rule, not the province's | river reads "exempt" and "barbless required" at once |
| 11 | `r4:goat_river.r3` carries an **exclusion** in `extent_text` | never binds. Only gear rule in that shape |
| 12 | `zp:bait.r3` typed `area:region:2` but names three rivers | rides as a caveat on all Region 2 water |
| 13 | Kootenay Lake Main Body trout/char total | the printed line genuinely does not say whether the water's rainbow 10 sits inside or beside the regional 5 — **a question for the region, not for code** |

**Retracted** (kept on record): "set lining Allowed on 15 Skeena stretches" was a fault in
the gear agent's own reference, not the data — `zp:set_lining.r2–r4` carry
`feature_types: [lake]`, a second spelling of water kind that its kind test did not read.
Both tables now read `water` and `feature_types`. The same blind spot had moved 7 Skeena
quota ledgers.

---

## The app page (`app/design/regs-v3.html`) — ported 2026-09-21

The page renders what the pipeline hands it and computes nothing. The four browser-side
ladders (`kindBeaten`, `authBeaten`, `tribBeaten`, `shutAll`), `retentionTable`,
`gearTable`, the closure calendar and their helpers are deleted (~1,700 lines); a test
fails if any of them comes back.

- **`pipeline/tools/emit_regs_v3_tables.py`** writes a second JSON block, `<script id="t">`,
  after the `d` block: `provenance.section` and `method_provenance.section` for every
  stretch of every water, in the shape the artifacts render. Every counter, term and gear
  row is written ONCE into `pool` by content (22 MB naive → 6 MB); the page rehydrates on
  load. Run it **after** `build_regs_v3_data`, never instead of it — the `d` block is what
  `corpus.section_rules`, `comply` and the delta invariant read, and the emitter never
  touches it (tested).
- **`LEDGER`** (a `<style>` + `<script>` before the page's own script) is the artifacts'
  renderer, scoped under `.ledger`: bands with "between them", a size on every row, wild and
  hatchery as two lines under one species, a stop as loud as a permission, possession never
  a naked number ("4 in possession — twice the daily 2"), a tag that opens the sentence.
  Inputs: water, stretch, `[TODAY.month, TODAY.day]`, the word for the water. A fish finder
  filters rows in place and survives a repaint (`LEDGER.after(screen)`).
- The page's `whatView` calls `LEDGER.quota(...)` and `LEDGER.gear(...)`; `bucket.gear`
  (conduct notices the old gear table printed underneath) still rides beneath the gear
  table as plain lines. The `eaten` routing at `whatView` remains — it only decides which
  zone rules are *tables* and which are *notices*; it resolves nothing.
- Two collisions bit during the port, both from the page attaching handlers by attribute:
  `screen.querySelectorAll("[data-fish]")` folds species and `[data-prov]` toggles the
  working — a tag carrying either repainted the screen on tap. The ledger uses
  `data-lfish` and `data-sentence`. Do not reuse the page's `data-*` names.
- The screen is a phone-width frame whatever the window, so the stacked row layout is the
  layout, not a breakpoint.
- `pipeline/tests/test_regs_v3_page.py`: ladders gone; every stretch has both tables; a
  sample re-resolved here matches the page counter for counter (a stale `t` fails); the
  emitter leaves `d` alone.

## The standing-tables page (artifact 17, 2026-09-21)

`https://claude.ai/artifact/TzMSTVCayHe7zWNDbrs4k9`. Its source is **`app/design/standing/`**
(`head.html`, `body.html`, `README.md`, committed `72cf9124`) plus `base.json` from
`pipeline.tools.emit_base_tables`; the page is the three concatenated — see that README for
the build command. It lived only in a session scratchpad until then, which is ephemeral.

Four things changed on it, each of which was a defect:

1. **A combined quota is the grouping** (`provenance.present` → `combine`). Region 3 drew
   bull trout / Dolly Varden and lake trout as two rows identical to the last digit, sharing
   cap and all, because the lake trout is released Oct 15 – Jan 31 and the bull trout is not.
   They are one entry now, with each member's own season on its own line. Two entries merge
   when their non-seasonal counters and sizes are equal AND they share a pooled combined
   quota that names both and is not the band above them. **Counter equality alone is refused**
   — 7A's lakes and 7B's streams each have two entries with identical numbers and no shared
   pool, and they stay apart.
2. **Seasons are drawn at all.** The page showed none: 68 of 246 standing rows carry a
   seasonal counter and the only place any of them appeared was the print check beside the
   table. A season a row's own counter carries is now a line under the fish; one carried by
   every row that may keep anything is stated once above the table.
3. **A cap that takes over the day takes over possession too.** `period()` skips `within`
   counters, so a row reading "3, of the 5 shared" printed the family's 10 beside it — ten
   Dolly Varden in the truck of a fish you may take three of. The cap's own possession twin
   is the answer (Haida Gwaii 10 → 6, Region 3 8 → 2).
4. **A number that is never in force is not an answer.** Three rows over the 22 tables have a
   calendar with no day on which the headline holds — 7A's bull trout showed the trout-and-char
   5 while the book gives it 1 for 303 days and release for 62. Those rows now say so and
   point at their seasons.

## The year, as tables you can pick from (2026-09-21)

The standing table is the table of a YEAR: where a rule is seasonal it can state no number,
and the season rides beside the row as a line the reader has to apply themselves. Three rows
over the 22 tables had no number at all for that reason. That is honest and unusable — the
question at the water is *what may I keep today*.

- **`rows.schedule(rows)`** cuts the year into the stretches over which one table holds. The
  signature is every counter **in force** on the day, on every row — not the headline, because
  a cap coming into force changes the table without changing a number. It wraps at the year's
  end the way `Row.calendar` does. **52 stretches over the 22 tables**; eleven tables have
  exactly one and never change.
- **`provenance.as_of(L, row, on)`** is one row as a day finds it: counters not in force are
  not on it, and the headline is re-read from what is left. Nothing is resolved a second time
  — it only chooses which settled counters the day can see.
- **`provenance.present(d, on)`** groups for that day. `steady()` became **`in_force(c, on)`**:
  with no date, in force every day; with a date, in force that day. One function, because a
  table for a day and a table for the year are the same table asked a different question.
  Region 3's streams are the proof — bull trout, Dolly Varden and lake trout are one line
  Feb–Jul, the lake trout alone in August, both released Oct 15–31, and the lake trout alone
  again in November.
- **`emit_base_tables`** emits `quota.views`, one per stretch. A single-stretch table carries
  `same: true` and no rows — it *is* the standing table. Dated rows drop `behind` and their
  counters' `source` (1.8 MB); the year-round table keeps all of it and is what the print
  check compares against. `base.json` 3.6 MB → 5.3 MB.
- The page gained a **year bar + chips + date input**. The standing table is still the
  default and still what the 386/390 print checks are read against; a note says so whenever a
  stretch is selected. Gear carries no windows at base level and the page says so.

### A quota inside a quota now looks like one

- A band and the rows under it printed the same figure in the same weight — "Trout 4" over
  "Rainbow trout 4" reads as eight. An in-band row whose answer *is* the band's rule now shows
  the number quiet with **"the same 4 as above"**, the band says **"not one each"**, and every
  in-band row carries a rail back to it.
- **`rowCap` covers rather than equals.** It demanded the cap name exactly the row's fish,
  which held only while a combined quota kept its members in one row; a date breaks that. It
  also fixed a live defect on the year-round table: **Region 6 streams printed 5 lake trout
  where the book says "3 Dolly Varden/bull trout and/or lake trout combined"** — one row
  changed out of 260.
- A pooled cap or possession shared with fish that are not on the row now **names them**
  ("shared with bull trout, Dolly Varden"), instead of a bare "between them" beside one fish.
- `outside_of` was emitted and never drawn: a fish sitting *beside* a shared number it is not
  part of now says **"on top of the 5 shared by … — these do not come out of it"**.
- **Collapsing two origins no longer keeps one of their labels.** Where wild and hatchery both
  go back, the one line was drawn from the wild row and kept its "wild only" — a rule true of
  every steelhead reading as one about wild fish.

### A shut stretch, a shared number, and one sentence per fact

- **A band under a closure printed a limit.** Every row beneath Region 3's trout-and-char band
  reads "No fishing" from January through June and the band went on offering four a day and
  eight in possession. `present` now marks a band **moot when its counter is moot on every row
  beneath it** — one released fish among five must not empty the other four — and the band then
  says what they all say. Four stretches over the 22 tables; no standing table has one.
- **"Between them" was worded three ways.** The row's number took the counter's `pooled` flag;
  a size tier took `size[].shared`, which is only set when the cap reaches fish *outside* the
  row — so Haida Gwaii's "1 over 50 cm", pooled across all fourteen trout and char and drawn on
  the row holding ten of them, said nothing at all beside a "3 between them" that did. One
  `poolWords()` now answers it everywhere, and **names the pool by its group** ("shared with all
  trout and char") rather than listing twelve species, or **"between them and lake trout"** when
  the row is several fish and the pool reaches one more.
- **The floor is the bottom of the first tier.** "must be at least 30 cm" in Size beside "up to
  50 cm" in Per day is one band of fish described in two cells; the tier now reads **"30 – 50
  cm"** and the floor leaves the Size column. `sizeText` takes a `moved` flag so an emptied cell
  can never fall back to **"any size"**, which would say the opposite of the rule.
- **A size class that is every fish you may keep is not a size class.** "at least 60 cm · only 1
  over 60 cm between them" is the same 60 cm twice; it now reads **"must be at least 60 cm ·
  only 1 between all trout and char"**. A cap on the row's *own* fish, at or above the number
  that governs the row, goes entirely — not pooled means no other row is carrying it, which is
  the test that keeps every shared cap on the page.
- **A bound the band states is not restated on each row under it**, and a cap that became the
  row's number is not also printed as prose beside it.
- Rows are **13% shorter** on every one of the 22 tables (20,300 px → 17,608 px), mostly from
  origin lines that were sized like full rows.

Pinned by fifteen tests in `pipeline/tests/test_regs_table.py`: the stretches cover the year
exactly once and adjacent ones differ; every `Row.calendar` boundary is a schedule boundary;
a dated row carries only what is in force; Region 3's streams split and rejoin on the right
days; no dated band is left without its number; **the year-round table is byte-identical
with and without the date machinery**; Region 6's lake trout cap; and a moot band agrees with
every row under it; every condition a region has builds a settled table; an area's table is
the region's plus exactly that area's rules; the summer closure reaches its units and its dates
and no further; Haida Gwaii carries its own bait ban; **a table is a function of the rules
handed to it and nothing else** (if `build` ever has to ask which region it is looking at,
that one fails); a named water needs no new builder; and there is one definition of what rules
a region has. The renderer is checked by diffing
all 818 rendered rows against the previous build — every difference has to be one of the named
changes above.

## Where and when are inputs to one generator (`pipeline/regs/table/state.py`)

A region does not have a table. It has a table **per condition**, and there are two:

- **WHERE** — a named area inside the region: Management Units 1-1 to 1-6, a National Park,
  a wildlife management area. These are area-scoped, and an area-scoped rule **cannot enter a
  region's base by construction** — that is the rule that stops one river's regulation binding
  a whole region. The cost was that they were on **no table at all**: Region 1's summer closure
  of every stream in six management units is printed in bold in the synopsis and appeared
  nowhere on this page.
- **WHEN** — a day.

**Selection and construction are kept apart, and that is the point of the module:**

```
SELECTION                                  CONSTRUCTION
knows the corpus                           knows only the rules it is handed
─────────────────────────────────          ──────────────────────────────────
region_rules(region)      ┐                build(rules, kind, here, label)
areas(region, kind)       ├─ rules_for ──→   → build.ledger(rules, …)
section_rules(water, run) ┘                  → method_build.table(…)
```

`build()` takes a list of rule dicts and cannot ask which region it is looking at, so the
builder lifts out of this project whole. `state.conditions(region, kind)` enumerates every
(area, stretch) pair — **218** over the 22 region/kind tables, 262 selectable states on the
page — and the emitter and the tests call the same function, so a combination a reviewer can
reach is one a test walks.

**A named water is one more precondition, already working**: `water_state("Fraser River", 18)`
→ *Region 5 – Region 7A boundary*, 13 rows, 8 stretches of the year, through the same
`build()`. `section_rules` was always a rule list; what was special was the REGION path, and
it no longer is.

Two things were collapsed on the way, and neither changed a byte of emitter output:

- **Two definitions of "what rules does Region 1 have"** — one in `quota_print`, one in
  `method_build`. That is not theoretical: it is exactly how the two sides came to disagree
  about Haida Gwaii. Both now call `state.region_rules`; `quota_print.base_rules_for` is a
  one-line forwarder kept because the print checks and several tests import it.
- **Two construction paths** — regions built a `Ledger` flat, sections built it two-stage
  through `build.ledger`. Checked across all 83 region×area conditions first: quota and gear,
  zero differences. They agreed, so collapsing was safe — but that agreement was a coincidence
  someone had to keep re-checking, and now there is nothing to re-check.

- `quota_print.base_rules_for(reg)` is now the single definition of what a region's base is
  made of, and `base_ledger(reg, kind, extra)` lays an area on top of it.
- `method_build.region_base(region, kind, extra)` does the same for gear — and gained a
  **`1hg` branch, which is a defect fix**: Haida Gwaii's rules live in Region 1's chapter under
  `hg_` and are area-scoped, so both halves of the old filter missed them (`is_base` is false,
  and `"z" + region` is `"z1hg"`, a prefix no entry has). **Haida Gwaii's gear table was the
  province's rules and nothing else**, and the page said "Roe may be used" for the half-year
  the book bans bait in every stream there.
- `method_provenance.row_json` now passes the date into `MethodRow.rig(on)`. It never did, so a
  gear table asked about a day answered with every seasonal condition in the book.
- `rows.schedule(rs, also)` takes the gear terms too — a bait ban that touches no number still
  changes what a reader may do, and without it Haida Gwaii had one stretch.

**What the page emits per area**: a full table only where the area really draws one. An area
that shuts everything says so and carries its rules (twelve rows of "No fishing" is the same
sentence twelve times); an area that changes no line a reader reads says *that*, so a reviewer
knows it was checked rather than forgotten. Without those two rules the payload was 19.8 MB,
over the 16 MB artifact ceiling; with them it is 5.6 MB. Of the five areas, only **MUs 1-1 to
1-6** draws a table of its own.

## Origin splits the entry, not the family (2026-09-21)

The idea was origin ABOVE "Trout and char", so the synopsis's *"And you must release: • Wild
trout/char from streams • All wild steelhead"* falls out of the structure. Measured first:

- **14 of the 20 bands hold entries of more than one origin.** Only Region 2's two tables are
  hatchery-only — which is the table the idea was formed on. A WILD / HATCHERY heading above
  the family would either duplicate the shared-number row into both sections or tear rows out
  of the number they count inside. **Not done.**
- **27 trout-and-char entries split by origin; all 148 entries outside that family split by
  nothing.** Origin is a level of one family, in every region, and of nothing else anywhere.

So origin splits the ENTRY. A species whose wild and hatchery answers differ is two entries,
and the band grouping that already exists sorts them out: a released wild row has no shared
number to belong to, so it lands outside the band by construction rather than by a rule
written for it. The defect this fixes is real — Region 2's streams drew a **"Trout and char —
HATCHERY ONLY"** band with a **WILD** line beneath it, inside a number that does not apply to
wild fish at all.

**This reverses "we should never have 2 rows for same species"** (the owner asked for it). A
split species now appears twice; the wild ones are ordered together under a *"Wild fish — you
must release every one of these"* heading. In Region 2 that reads as the book does; in Region
1 lakes the hatchery steelhead sits inside the trout band and the wild one is further down, so
the two are further apart than they were as stacked lines. That is the trade.

Every entry is now ONE row, so the table has one row shape throughout.

- `test_the_presented_table_names_each_fish_once…` demanded a species appear in exactly one
  entry — the constraint that forced the stacked-line shape. It is now the stronger rule it
  was standing in for: a fish is answered **exactly once per origin**, and **never both** as
  `either` and as a single origin, which would be two answers to one question.
- **A bug the split caused, caught by the sweep**: `done` was keyed by fish alone, so Region 2
  streams silently lost its entire hatchery band — three rows and the shared number — because
  the wild rows claimed the id first. The sweep now asserts no entry goes undrawn.
- `only 1 between all trout and char` → **`only 1 of all trout and char may be over 50 cm`**.
  It is NOT redundant with the per-day 1: they are different rules that happen to both be 1
  (the row's own combined cap, and the family's single over-50 fish). Restating it at the
  row's 60 cm floor made them look like one sentence printed twice.

## Open work

1. Groups still named by exclusion — "Any other char" / "Any other trout". The readability
   review ruled this out; member names beneath are a partial fix.
2. Duplicate answers from two authorities print twice (Shuswap wild steelhead:
   `release [Region 3] · release [All of B.C.]`) — dedupe to the closest authority.
3. Weekday and time-of-day rules never decide a `(month, day)`; the client must apply them.
4. Region 7B possession exceptions are verified against curated text only — no 7B chapter
   PDF was fetched (the full synopsis p.70 now covers the quota table).
5. The page's species-group toggles (`st.fish_`, the where-view) do not filter the ledger;
   the ledger has its own finder. One filter would be better than two.
6. Dated views are **base tables only** — `provenance.section(water, run, on)` takes a date
   but the section emitters do not yet emit views, so "this water" overrides have no
   schedule. That is the next thing v4 needs.
7. **`app/design/regs-v3.html` is stale against `present()`.** Its `<script id="t">` block
   predates `combined`, so Region 3's waters still draw the two split rows. `LEDGER` would
   also need to learn `combined`: it renders a two-line entry with an origin badge, and a
   merged group's two lines are both "either", so the members would be unlabelled. The owner
   has accepted this — v4 is built from the structure, not from this page.
8. The quota/custody artifact (`c16d220a…`) and the gear artifact (`56c5f7d1…`) were not
   regenerated and show the pre-merge shape.
9. Region 6's hatchery steelhead prints "only 1 over 50 cm · only 1 over 50 cm between them"
   — `narrowCaps` absorbs a duplicate threshold only when the row tiers, and this row has no
   standing number to tier inside.

## Artifacts

- Quota table + per-region base tables + chain of custody:
  https://claude.ai/code/artifact/c16d220a-6eee-4149-9692-49c04862e0a2
- Gear/methods table + 20 standing tables with print diff:
  https://claude.ai/code/artifact/56c5f7d1-fd3e-4109-a44d-44134a28f3ad

## House rules that bit us

- ⛔ **Parser runs are human-only.** Never run `run_parse.sh` or anything dispatching to the
  `claude` CLI — it spends the owner's credits.
- **Never `git add -A`** — a concurrent agent in this repo commits with it and will sweep
  unrelated work into its commits. Stage explicit paths.
- Prefix shell commands with `rtk`. `archive/` and `pipeline/docs/archive/` are prior art.
