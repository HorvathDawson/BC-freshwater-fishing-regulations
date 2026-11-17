# Trout table: the payload contract (data side)

Revised 2026-11-16 after the fresh review (trout-table-review-2026-11-16.md). Every D finding is applied here or in
the spec's tickets; the app changes that follow from them are listed in section 6, "For the app chat". Two owner
decisions are still open and are marked **PENDING OWNER** (section 3).

2026-11-16 17:00 (clock read), written by an Opus worker for the main chat. This is the agreement between the Python
pipeline, which writes each water's trout table into the release, and the app, which only draws it. It answers the
app chat's eleven questions (Q-D1 to Q-D11, in `trout-table-app-check-2026-11-16.md` section 10.8) and corrects that
file's first-cut contract (its section 10). The spec (`trout-table-build-spec-2026-11-16.md`) keeps the design and
the tickets. The spec's old section 4 (4.1 to 4.7) is now only a pointer to this file: **this file is the contract,
and it wins** over any example or shape still quoted elsewhere. The app chat confirms or contests it before ticket I
or M starts.

Tags: **[read]** = I read the code or file at HEAD `384470bad`; **[ran]** = I ran it; **[inferred]** = my reasoning,
not checked.

**The governing rule** (owner 16:50, verbatim): "we want to make sure the plan is robust so that we change the data
all the way in python and the front end and it all just works withotu fighting it and makign sure everythign is
streamlined etc". This contract turns that rule into five mechanical guarantees:

1. **One source for the shape.** The payload is a strict pydantic model in `pipeline/deliver/answers/model/trout.py`,
   registered in `FRAMES["trout"]` (`model/frames.py:20`). Its TypeScript is generated into
   `app/packages/core/src/answers.generated.ts` by the existing emitter (`model/emit.py:63` `emitted_ts`, kept by
   `--check`, AGENTS 40). The app writes no payload type by hand.
2. **No numbers the app could count with.** The payload carries words, structure (box, row, band, which rows a band
   spans) and rule indexes. It carries no limit, size or day count as a bare number. The app has nothing to add,
   compare or re-word. The proof (ticket J) reads the law back from the shipped words (see 2.10; 2.9 is bytes).
   **One stated exception:** `YearBar.runs` carries day counts (`[state, days]`), because the bar is drawn to scale.
   The app uses them only as lengths to draw; it states no rule from them and adds no word.
3. **Every empty field states why.** Each nullable field is paired in a model validator with the condition that
   empties it. This is the convention of the answers models (`model/__init__.py:1-6`).
4. **One fixture.** One committed vectors file, written from Python and read by both test suites (2.8). Where the app
   must compose something (the day phrase, picking the segment), the vectors carry the finished result, and the app
   must reproduce it. The screen-reader label is the one exception: its join order **is the payload's order** (2.8),
   so Python writes no second composer for it, and only the TS output is pinned (an app golden).
5. **One trout path after the switch.** The `--trout-table` flag, the app's dev switch and the trout rows' `group`
   leave in one landing: the cutover merge (spec section 7, step list). After it, no trout word comes from the old
   group tree or from a TS composer.

---

## 1. The eleven answers

| Q | Decision | Why (code or ruling) | Cost |
|---|---|---|---|
| **Q-D1** Box words | **Agreed: Python writes them whole.** Each box has `head`: "Keep 5 in all" on a keep box, and the column word ("Release", "Closed") on a release or closed box. Every band text ("up to the 5", "only 3", "only 1 of these 4") is Python's too. The box carries **no `n`**: the spec's `Box.n` and the app draft's `n: number` are removed | AGENTS 33 (`AGENTS.md:228`): a sentence is pinned whole, never built from a number. Guarantee 2: an `n` the app can read is a number it could count with. The app check found TS re-deriving tier numbers today (`grouphead.ts:125-145` `headCtx`, item 10 of its 12.1) | J must parse numbers out of words (2.9). The app's 200% view, which folds bands into bold lines, uses the band text as written |
| **Q-D2** The day phrase | **Agreed, with one change.** No Python string says "today" except the stop duty ("…stop fishing this river today", which is about the angler's day). The top line carries **one `{day}` slot** where the day phrase goes: `"Up to 5 {day}"`, `"Closed for trout and char {day}"`. The app puts its own words in the slot ("today" or "on Jul 15"). Python decides **where** the slot goes, and whether there is one at all | The page shows any day (`strip.ts`, `WORDS.TODAY_LC` and `ON_DAY`). Appending a day word to every lead would be wrong, as in "Closed … until Oct 31 today". A slot keeps word order Python's. The validator allows `{day}` only in `top.lead`, at most once | The app replaces one token. The vectors carry the result for both day modes, so the replacement is pinned (2.8) |
| **Q-D3** NULL cell | **Agreed, made exact.** `cell.trout` is NULL exactly when the page draws no trout table: (a) the display frame at the same key and segment says `closed` (the whole-water banner speaks), (b) it says `tidal`, or (c) the rows frame asks about no trout or char fish. The builder reads (a) and (b) from the **display frame the app also reads**, so the two cannot disagree. Outside B.C. there is no key at all. Unnamed parts are covered like named ones. A trout-only closure (Fraser Region 3, Oct) is **not** NULL. It is a table whose top line is "Closed for trout and char {day}" | `answers.py:612-616`: a section composed from other sections at the same key and segment. The display frame's status is what the banner draws (spec 2.3; app check 6 "Closed banner") | A build check: NULL ⇔ (a) or (b) or (c), over every cell (part of I) |
| **Q-D4** Loose rows and rows without a group | **Agreed: every trout and char fish on the frame is in the table, exactly once** (a keep box row, or the release or closed group). That includes loose rows (Region 8's stream brook trout, t670 K4 and K5) and the 2,202 frames with no group. Which rows the table replaces is a fact Python ships: `covers`, the rows-frame row indexes. The app's trout or other-fish split reads `covers`, **never `TROUTISH`** | The trout set is one Python predicate: `common.trout_char()` (`common.py:525`) = CT, DV, EB, GB, LT, RB, ST [ran]. The TS hand list `others.ts:24` `TROUTISH` adds BT, CT_ws and CT_c. Two lists put a fish in both places or in neither (app check 12.9 #4) | A covered row whose members are not all trout or char stops the build. Ticket A0 counts such mixed rows. If any exist, this contract is amended. They are not handled silently |
| **Q-D5** Two cards today | **Agreed: one table**, one top line, the keep boxes in Python's order (the two-box "counted apart" form, spec 3.1). Kootenay Lake's 43 frames, and Lois and Khartoum, become one table each | `scope.apart` and `parent` are stored on the rows (spec 2.2). The top line's form is Python's choice from them | None beyond F |
| **Q-D6** Rule refs | **Decided:** every element that states a rule carries `src`: the rule indexes, in the same index space as the rows frames (`RuleIx`, the water file's `rule.rix`, `contract.py:131`). A row's `src` holds the rule that decides its state or count first, then size, origin, possession, the duty, and the rule behind its date line, with no repeats. A sheet's `src` is that list cut to 4 by Python. The table's `src` is every rule any part cites, in first-cited order: it feeds "Sources ›". **The app never sorts, unions or cuts a list** | Spec 3.4 ("this fish's rules first, up to 4"). The app check lists TS choosing and ordering rules today (12.2 #45 `decided.ts:71-200`) | J checks that every ref exists, and that every rule the engine says decides a trout fish on that reading is in the table's `src`. "Sources" can never miss the rule that decides |
| **Q-D7** Angler-only notes | **PENDING OWNER. Recommended (and what this contract is written for): the notes stay in the page's caveat block, and the table carries none** (`notes` is removed from v1). Every mention of table notes in the spec (3.1, F, O) is conditional on the other answer. If the owner wants them in the table, the table carries them and the caveat block skips them by the rule indexes the table names. They are never in both places | The 34 angler-closure rules close a water to a class of angler (youth waters, Skeena/Babine weekends), not trout alone (adversarial review, finding 8). They are page-wide like the other caveats. `caveats.ts:187-191` `anglerOf` already places them by moment from stored frames. Ticket A0 counts any angler-only rule that closes trout or char alone | Recommended option: none. It departs from the round 7 mock (notes above the top line), which is why the owner is asked. If he picks the table: `notes: Tuple[Note, ...]` (text, today flag, src) is added to `TroutTable` before the `trout` version is frozen, F writes them, and the app's caveat block skips them by the rule indexes the table names |
| **Q-D8** Possession | **Decided:** the page keeps its line (owner 15:42). A trout row carries a possession line only where `card.possession_duty(P, model, S)` (`card.py:446`, T805 B, RULINGS P58) returns one. The row form of the line goes in `lines` (bold, today's rule). When possession is below the day's number (Cunningham), the band is `kind: "possession"` ("only 2 in your possession") and the explanation goes in `quiet` ("3 a day, but never more than 2 lake trout with you"). When every row of a box shares the line, it goes in the box's `strip` (Bowron: "In your possession: only 1 in all, not 1 of each"). The sheet's sentence comes from the **same** function. **Two fixes come with it:** (1) the page's possession lead is chosen once, in Python, and the page reads it; (2) at the switch the page's possession row stops repeating trout fish lines (see the fight list) | Python's `card.page_possession` (`card.py:410`) chooses "written for every game fish, else the largest". TS `standing.ts:97-104` `possessionRules` chooses "names no fish, else most days". Those are two different rules, and the table's "differs from the page" test is made against Python's choice [read] | Ticket **F0** (data, 2.11): the page's lead and the page row's kept also-lines ship in `RowsFrame.page_possession`, inside the cutover's rows 34 bump; F0's code and tests land before F. S (app) reads them and stops picking |
| **Q-D9** The rows 34 drop list | **Decided, with the rule for it.** On trout rows (those in some cell's `covers`), `group` and `elsewhere` are dropped at rows 34. `presence` is dropped if S's grep finds no reader outside the trout path. `year`, `week`, `items`, `conds`, `real_daily`, `everyone` and `stamps` **stay**: the year bar, `caveats.ts` and `card.ts` read them for the whole page. A field leaves a trout row only when **both** hold: the table says what it said, and the app's grep at S finds no other reader. The `Row` validator `_pool_rows` (`model/rows.py:1124`: "the kept-elsewhere line is set <=> a keep row") becomes "set <=> a keep row that no trout table covers" | Size: the trout groups are 110.5 kB of regs-6 as stored [app check 11b, ran]. The other fields are small and are read elsewhere | L re-aims the build checks that walk `group.tree` (`answers.py:584-598` `origin_unsaid` / `across_unsaid`, `card_check.check_tree`, `possession_problems`) at J, with a mutation test per check: without `group` they would check nothing on trout rows (fight list #9). Nothing is just deleted |
| **Q-D10** Where the sheets live | **Decided: a separate interned table `trout_sheet`.** The table body names each sheet by id. Decoded, the frame holds the sheets inline. The sheet's coming-change sentence stays in the date set (it changes by segment) | Sheets are the bulk of the words, and many repeat across waters, for example a region's default lake trout sheet. Interning them apart keeps one copy even when the tables differ. That is the `rows` 30 lesson: the strip inside the row made 3,794 rows out of 2,512 (`encode.py:806-809`) | One more blob family and zdict (2.9). Ticket A1 may fold the sheets back into the body if it measures no gain. The decoded shape does not change |
| **Q-D11** Side release for previews | **The staging test channel is real for phones but cannot carry a side release today.** `deploy/staging/test-updates.mjs` (T251) moves only the manifest and the feed indexes under `test/`. Its `release` scenario re-points the live files. `upload.sh:121-124` clears everything the live release does not name, except `test/**` [read]. **Recommended:** make the picture pack (R) from a **local** side release through the web export, so no upload is needed. Add a small `side <dir>` scenario that uploads the side release's files under `test/release/<id>/` and points `test/manifest.json` at them, used only for the fresh-install walk on a phone | Staging upload and clear are allowed (memory: staging R2 + deploy allowed). The web always serves the app and the release together, so it cannot preview a side release | Owned by ticket **V-py** (data), which also owns the refresh script (2.8) [inferred: whether the manifest's file URLs resolve under `test/` was not checked; V-py checks it first] |

---

## 2. The contract

### 2.1 Where it lives, and the optional-section rule (M0)

**Files.** Each `regs-{r}.sqlite.gz` gains one column and four tables:

```
cell.trout     INTEGER NULL   -- the trout frame id for (akey, s); NULL per Q-D3. LAST column of `cell`.
trout_frame    (id, j)        -- [table id, dates id | null, covers]     covers = rows-frame row indexes (Q-D4)
trout_table    (id, j)        -- the table BODY: every NOW word; interned across keys, segments and waters
trout_dates    (id, j)        -- a DATE SET: every NEXT word of one cell (2.5); interned
trout_sheet    (id, j)        -- one fish sheet (Q-D10); interned
```

Blobs use the release's existing codec with a zdict per table. The zdicts live in `water-shared`'s `zdict` table,
like every other region table's (`store.ts:126`). `covers` sits in the frame, not in the body, because the same body
is shared by waters whose rows frames are laid out differently.

**The optional-section rule** (the data half of the app's M0). The rule is all or nothing per release:

- `meta.layout.versions.trout = 1` in the water file, **and** the manifest's `regulations.versions.trout = 1`,
  **and** the column and the four tables exist in every regs file. Or none of the three: no version, no column, no
  tables. A release without the section has **no** `cell.trout` column. An all-NULL column is not allowed, because
  "absent" and "no table here" must not look the same (AGENTS 32: "we have not asked" vs "there are none").
- `contract.py` gains `OPTIONAL_SECTION_TABLES = {"trout": {"cell": ["trout"], "tables": [the four]}}`. The name
  keeps it apart from `contract.OPTIONAL` (`contract.py:27`, optional **files**, emitted as `PACK_OPTIONAL`) and from
  the app's hand-typed version list (proposed name `OPTIONAL_VERSIONS`, section 6). `pack.py`
  (`tools/emit/pack.py:26`) emits it as `PACK_OPTIONAL_SECTION_TABLES` into `pack.generated.ts`, so the app's M0
  reads the **column and table names** from generated code. `test_release.py` (which holds a built release to the
  contract exactly, `contract.py:3-6`) also holds the section all or nothing.
- **The version stays hand-typed on the app side, on purpose.** `versions.ts` says which version the reader was
  *written for*. A generated version would silently accept a v2 the code was not written for. The pin is ticket
  **I0** (lands first, before any other trout code): one test holding **every** `A.SECTIONS` version (ladder, rows,
  gear, licence, display; today only the tidal three are pinned, `test_answers_tidal.py:121,128-132`), plus the
  optional `trout`, equal to `app_reader_versions()`. I0 extends `app_reader_versions` (`test_answers_tidal.py:113`)
  to parse the app's optional list as well as `KNOWN_VERSIONS`, and to fail when a section is on one side only.
  Until the app lists `trout`, I0 holds it as "Python has it, the app does not yet" by an explicit, named
  allowance, never by skipping.
- **Any shape change bumps `trout`.** During the build-behind, APKs from M0 on read side releases. A change to any
  trout model field, validator or wire layout bumps `Section("trout", n)`, and I0 fails until the app's list
  moves too. There is no "still 1, slightly different".
- At the switch (the cutover merge), `trout` moves from optional to required: into `_REGION_TABLES` and `cell`
  (`contract.py:17-23`) and into `SECTION_NAMES` and `KNOWN_VERSIONS`. `OPTIONAL_SECTION_TABLES` is then empty
  again. **And the regs file format is bumped**, `regs-region/1` → `regs-region/2` in `contract.py` FILES
  (`contract.py:142`), so the release machinery itself knows an older reader cannot open it (2.7).
- **PENDING OWNER (decision 1): the section ships in normal releases before the switch.** Recommended yes if A1
  measures the section (with its zdicts) at about 100 kB or less on the first-tap moment. Yes: once I, J and A1
  are green, `--trout-table` is on in every release (still optional, still `regs-region/1`); J runs on every real
  release; staging previews are real releases; the first-tap limit is raised by A1's measure plus 10% at that point
  (owner rule O5), and comes back down at the cutover when the trout groups leave; the cutover shrinks to rows 34 +
  the format bump + deletions. No: the section ships only in side releases (fixture, mini release, staging `side`
  scenario) until the cutover merge, which turns the flag on; J runs on side builds and the mini release only; any
  limit raise happens in L. The rest of this contract is the same either way.

### 2.2 Where it is built (no new day cuts)

The trout section is a **whole-key compose**. It runs once per answer key over all the key's segments, after the
rows post-passes (weekday words, year strips, licence stamps and the T793 and T805 checks, `answers.py:556-608`) and
after the display compose (`:610-616`). It needs a new hook beside `compose`: `Section.compose_key(values per
section, starts, moments) -> [value per segment]`. That is ticket I's work.

- **It adds no day cuts.** Its value is a function of the rows and display values over the key's segments. Any
  change in them already starts a segment (`answers.py:654` `cut`), so inside one segment the next change is
  always the same. The build checks this: two adjacent segments whose rows and display values are equal must not
  get different trout frames unless their dates differ.
- **The body is drafted per reading, inside the rows derive**, where `card_of` runs today
  (`rows/assemble.py:118-120`). It needs the reader's `P` and `model` to reuse `card.possession_duty`, `_duties`
  (`card.py:367`), `tree.across_rows` (`tree.py:105`), `A_BOTH_LAKES_SAID` (`tree.py:101`) and `size_rule`
  (`tree.py:942`) **as they are, not re-derived** (spec risk 9). The draft leaves the derive on a private key that
  is stripped before rows encoding and is never in `RowsFrame`.
- **Three parts are composed in `compose_key`, not in the derive**, because their inputs are written by the rows
  post-passes, which run after the derive (`answers.py:556-609`): the weekday lines (from `rows[].week` after
  `say_weekdays`, `:571`), the sheet's licence / stamp line (from the row stamps after `stamp_licence_documents`,
  `:587`), and the steelhead stamp (after `drop_closed_steelhead_stamp`, `:609`). `compose_key` reads the
  **post-pass** values of `rows` and `licence` at the same key and segment, so the table can never say a stamp the
  licence answer dropped, or miss a weekday line the page says. The draft carries no such line; a test plants a
  stamp drop and a weekday split and checks the table follows the post-pass value.
- **Hours windows.** The app taps `inside=false` (`tap.ts:55`), so the table is drawn from outside any hours window.
  A trout answer that changes inside a window is therefore said by the table in words: F writes an hours-window
  line in `lines` (words from G), composed in `compose_key` from the start's moments, like the weekday words. A0
  counts how many trout answers change inside a window. After the switch the page's timed caveat (b) skips covered
  fish (S), so no fish is spoken twice.
- **The dates are built in the whole-key pass**, from the engine's answers: per fish and origin, the decided state,
  count and bands in `rows.fish`, plus the possession fact carried out of the derive. They are **never** built from
  the table's words. J checks them against `verdicts.sqlite` on its own, not against the builder's inputs.

### 2.3 Shape (pydantic: the one source; the TypeScript is generated from it)

Field names are Python's snake_case. The emitter writes property names as they are (as `all_members` today), so
the app draft's `hatchLink`, `hatcheryOnly` and so on become `hatch_link` and `label`. Every field is **required**.
Nullable fields have no default, so the generated type is `x: T | null`, never `x?:`. The wire always carries every
key, and the zdict makes the repeats cheap.

```python
# pipeline/deliver/answers/model/trout.py  (sketch; validators named, not written)
from typing import Annotated, Dict, Literal, Optional, Tuple
from pydantic import Field, StrictInt, StrictStr
from pipeline.deliver.answers.model.base import Model, RuleIx, FishLit, OriginLit
from pipeline.deliver.answers.model.rows import YearRun

Txt = Annotated[StrictStr, Field(min_length=1)]   # a whole phrase, drawn unchanged. Validator: no leading or trailing
                                                  # space, no "{" or "}" (the one exception: TopLine.lead's "{day}")
Src = Tuple[RuleIx, ...]                          # rule indexes (rule.rix); each must exist (checked at build)
RowKey = Annotated[StrictStr, Field(pattern=r"^(CT|DV|EB|GB|LT|RB|ST)(/(wild|hatchery))?$")]   # = common.trout_char()

class TopLine(Model):
    lead: Txt                 # "Up to 5 {day}" | "Up to 2 {day}, or 5 if they're rainbow trout" |
                              # "Up to 6 rainbow trout or steelhead and 4 other trout and char {day}" |
                              # "Release every one {day}" | "Closed for trout and char {day}" | "Nothing to keep {day}"
                              # Validator: "{day}" appears at most once
    second: Optional[Txt]     # the cross-box wild limit (Lois, Khartoum). Set <=> TroutTable.together is set,
                              # with the same words (validator)

class Fold(Model):            # the existing "Trout and char" entry's short line (owner 15:47)
    value: Txt                # "Up to 5" | "Up to 2 or 5" | "Up to 6 rainbow, 4 others" | "Release" | "Closed" |
                              # "None to keep"
    tone: Literal["keep", "release", "closed", "none"]   # style only

class Band(Model):            # the right-hand column of a keep box
    from_: StrictInt = Field(alias="from", ge=0)          # first row index in the box
    to: StrictInt = Field(ge=0)                           # last row index in the box (inclusive)
    text: Txt                                             # "up to the 5" | "only 3" | "only 1 of these 4" |
                                                          # "only 2 in your possession"
    kind: Literal["upto", "own", "shared", "possession"]  # layout: "upto" = one cell per row (owner 13:48);
                                                          # the others = one white box across from..to

class TRow(Model):
    key: RowKey               # unique in the table; "/wild" or "/hatchery" ONLY on twin rows
    fish: FishLit             # picture and FISH_TIPS code
    name: Txt                 # "Dolly Varden/bull trout", as the book prints it
    origin: Optional[OriginLit]   # drawn as the word itself. Set <=> the row is one of a twin pair, where wild fish
                                  # have a quota or size of their own (Kitimat, Lois, Khartoum, Ruby, Cultus) (validator)
    label: Optional[Txt]      # the filled label beside the name: "Hatchery only" (owner 15:48, 16:00). Null when the
                              # box carries it (validator: never both on a box and its rows)
    sub: Optional[Txt]        # "a rainbow over 50 cm (20 in)"
    lines: Tuple[Txt, ...]    # NOW, bold, in this order: size, "uses the 1 over 50 cm (20 in)", possession, stop duty
    quiet: Tuple[Txt, ...]    # NOW, regular weight: Cunningham's "3 a day, but never more than 2 lake trout with you";
                              # the steelhead line (rows_frame.steelhead_line, today TS STEEL_LINE, tail.ts:83)
    src: Src
    sheet: StrictInt          # WIRE: trout_sheet id. DECODED: replaced by the FishSheet (see TroutFrame)

class Box(Model):
    kind: Literal["keep", "release", "closed"]
    head: Txt                 # keep: "Keep 5 in all" | release: "Release" | closed: "Closed"  (Q-D1)
    label: Optional[Txt]      # keep only: "Hatchery only" when it covers every row of the box (owner 16:00)
    strip: Optional[Txt]      # keep only: "only 1 over 50 cm (20 in)" | "only 2 wild, each 40 cm (16 in) or less" |
                              # "In your possession: only 1 in all, not 1 of each"
    tag: Optional[Txt]        # keep only: "Lake's rule"
    rows: Tuple[TRow, ...]    # min 1, in Python's order (a shared band's rows are next to each other, round 3)
    bands: Tuple[Band, ...]   # keep: covers rows 0..len-1 exactly once, in order, no gaps or overlaps (validator);
                              # release and closed: empty

class YearBar(Model):         # only when EVERY trout/char fish shares one state today (all release, or all closed) AND the
                              # water is open (owner 12:42, his words above T810 item 9). runs = the guarantee 2 exception (day counts, drawn only)
    title: Txt                # "Trout and char through the year"
    runs: Tuple[YearRun, ...] # [state, days] over the 365 book days, as rows[].year (model/rows.py:1061)

class FishSheet(Model):
    head: Txt                 # "Keep · only 1 of these 4 · counts toward the 5" | "Put it back" | "Closed · do not fish for it"
    here: Tuple["SheetLine", ...]   # "Here, today", in order; includes the licence line and the one-day note (Adams)
    src: Src                  # at most 4 (Python cuts; validator)

class SheetLine(Model):
    t: Txt
    bold: bool                # the stop duty

class TroutTable(Model):      # the BODY: every NOW word; no date words (they live in DateSet)
    top: TopLine
    fold: Fold
    hatch_link: bool          # "How to tell a hatchery fish ›" (an app link word). True <=> some row or box has a
                              # label or an origin (validator)
    boxes: Tuple[Box, ...]    # keep boxes first, then at most one release box, then at most one closed box (validator)
    together: Optional["Together"]  # a limit across two keep boxes (Lois's wild 2; Vedder; Echoes)
    year: Optional[YearBar]
    elsewhere: Tuple[Txt, ...]      # "Fished elsewhere today? ›" lines (from today's group.worked, groups.py:285);
                                    # empty = no link
    src: Src                  # every rule any part cites, first-cited order: "Sources ›"

class Together(Model):
    text: Txt
    src: Src

class DateSet(Model):         # every NEXT text of one cell; small, quiet, on its own line
    top: Optional[Txt]        # under the top line: "reopens Nov 1" while the table is closed today (owner 15:36), or
                              # the "until …" of a release-every-one table
    card: Optional[Txt]       # above the table: "Until Oct 31, then closed to all fishing"
    fold: Optional[Txt]       # beside the folded entry's value: "until Mar 31"
    box: Dict[StrictStr, Txt]       # "b" -> the strip date | the Release/Closed column date under the word
    band: Dict[StrictStr, Txt]      # "b:k" -> under a white box
    row: Dict[StrictStr, Txt]       # "b:r" -> the row's last line (a release/closed date over 30 characters goes
                                    # here, under the name: Python applies the rule, round 11)
    sheet: Dict[RowKey, Tuple[Txt, ...]]   # the coming change as sheet sentences ("From Nov 1 every wild one goes back.")

class TroutFrame(Model):      # FRAMES["trout"]: the DECODED value of one cell (what the app's reader returns)
    table: TroutTable         # with each TRow.sheet replaced by its FishSheet (the decoded TRow has `sheet: FishSheet`)
    dates: Optional[DateSet]  # null <=> no fish of the table changes at any later segment of the key's year
    covers: Tuple[Annotated[StrictInt, Field(ge=0)], ...]   # rows-frame rows this table replaces (Q-D4); min 1
    # Validators on the frame (they need both parts):
    #  - every DateSet key resolves: box b exists; band k exists in box b; row r exists in box b; sheet key is a TRow.key
    #  - ONE level per row's date (placement rule, spec 3.3 rule 7): for each row, at most one of card / its box /
    #    its band / its row carries a date
    #  - top: set => the table is in a closed or release-every-one form
    #  - a REOPENING date ("reopens Nov 1") sits only where something is closed TODAY: DateSet.top on a closed
    #    table, or the box / row date of the closed box (a closed fish: Bish Creek's steelhead, May 20). Owner
    #    15:36: "drop reopen unless it is currently closed then reopen date matters". A coming closure never
    #    carries one. G marks the reopening form; the builder places it; J checks the day and the place (2.10)
```

**Wire against decoded: decided, both emitted.** The codec (`TroutCodec` in `encode.py`, ticket I) turns
`TRow.sheet` into a `trout_sheet` id and the frame into `[table id, dates id | null, covers]`. The model above is the
**decoded** value, as `RowsFrame` is today (`encode.py:827-842`). The wire shapes are pydantic models too and are
emitted beside it: `TroutFrameWire` (the triple), `TroutTableWire` / `BoxWire` / `TRowWire` (the body blob, with
`sheet: int`), and `FishSheet` and `DateSet` (the same on both sides). The app's decoder (`reads.ts`) types what it
reads from the wire with these, so no wire type is hand-kept.

**`CellIds` and the table names are emitted too, now.** `CellIds` (`core/src/regs/inputs.ts:24`, hand-typed today)
is emitted from `contract._REGION_TABLES["cell"]` by `pack.py` (in I0, before the trout column exists, so the
column joins it by generation). The other hand lists of region table names are derived in ticket I from one
registry beside `_REGION_TABLES`, or held equal to it by a test where a list must stay: `store/build.py:37-43`
`FAMILIES` (and its refusal of an unknown section, `:282`), `store/common.py:58-62` (codec per table),
`release/shards.py:22-26` (`TOP`, `SHARD_TABLES`), and the slices (`release/slices.py:49-52`, plus the new zdict
rows in `water-shared`). A table name added in one place and not the others fails a test, not a release build.

### 2.4 Who writes which word

**Python writes, whole:** `top.lead` (except the slot), `top.second`, `fold.value`, every `Box.head`, `label`,
`strip`, `tag`, every `TRow.name`, `sub`, `lines`, `quiet`, every `Band.text`, `together.text`, `year.title`,
`elsewhere`, every sheet `head` and `here` line, and every `DateSet` text. That includes the HATCHERY ONLY label (as
`label`) and the Release and Closed column words (as `head`). They all come from one words module
(`answers/trout/words.py`, ticket G), with whole-string tests, and are listed in G's catalogue for the owner. **The
words module lands before F** (G's first half), so F is written against it and never grows its own strings; G's
catalogue tool comes after F. The catalogue also shows the owner the two pairs that sit on one screen: the fold's
grammar (Python `fold.value`, "Up to 5") beside the Other fish list's (TS `shortValue`, "5 a day", `others.ts:63`),
and the table's card line beside the bar's "Closed from …" (`strip.ts:137-174`, whole water).

**The app writes only navigation words** (`words.ts`): the entry's name "Trout and char" (exists today), "How to
tell a hatchery fish ›", "Sources ›", "Fished elsewhere today? ›", the sheet's section heads ("Here, today", "How to
tell it", "Sources", "All sources ›"), the shape marks ↩ and ✕, and the day phrase in the `{day}` slot. **No app
word states a rule.** The origin tag draws `TRow.origin` as the word itself. Upper-casing the label is styling.

### 2.5 Days, "until …" lines, and where a date sits

- **One table body and one date set per (answer key, segment).** The app picks the segment with `segmentIndex`
  (`tap.ts:54`, the port of `encode.py:1045`), as it does today. That is the only date choice the app makes.
- **The body holds no date words.** Every NEXT text is in `DateSet`, so a body that reads the same today is one blob
  across segments and waters (Q-D10, 2.1).
- **The date rule is spec 3.3, decided in Python.** The first later segment whose answer for the fish differs is
  dated. A change that only loosens reads "rule changes <day>". A tighter change, or a change of state, is said in
  words. A coming closure gets no reopening date. A reopening date appears only where something is closed today:
  in `DateSet.top` when the whole table is closed, or on the closed box's date when one fish is closed (Bish Creek's
  steelhead can say "reopens …"). Owner 15:36: "drop reopen unless it is currently closed then reopen date
  matters". The year wraps. A gap under 10 days raises a build warning. The Adams one-day segment
  puts 80 cm on the row and one `here` line in the sheet.
- **Weekdays and hours.** The walk steps over **start days**, not segments. One start's moments (its weekday
  classes, outside any hours window, as `year.py:12` does) count as one answer, a tuple. A fish whose answer changes
  by weekday gets its weekday words as a `lines` entry, composed in `compose_key` from the post-pass `rows[].week`
  (`say_weekdays`, `answers.py:556-571`; see 2.2). **No date line ever names a weekday**: Python does not know the
  calendar year, and the app computes weekdays from it (`tap.ts:85-89`). The inside of an hours window is never
  what the table is drawn from, because the app taps `inside=false` (`tap.ts:55`); a trout answer that changes
  inside a window gets an hours-window line written by F (2.2). Those cells are built and proved like any other,
  so none is ever missing.
- **Placement is Python's** (spec 3.3 rule 7): card if every fish shares the date and words, else the box (strip)
  if every row of the box shares it, else the band (white box) if all its fish share it, else the row. The
  frame validator allows one level per row. The app draws a key where it points and decides nothing.

### 2.6 What NULL and empty mean

| Value | Meaning | The app draws |
|---|---|---|
| no `cell.trout` column (no `versions.trout`) | this release has no trout section | the old card (build-behind only); after the switch such a release is `regs-region/1`, which the S-built app does not read (2.7) |
| `cell.trout` NULL | Q-D3 (a), (b) or (c) | no trout table and no Trout and char entry; the banner or tidal page speaks |
| `frame.dates` null | no fish of the table changes later in the key's year | no small lines anywhere |
| `top.second`, `together` null | no cross-box limit | nothing |
| `TRow.origin`, `label`, `sub` null | no twin row / no label / no sub-line | nothing |
| `lines`, `quiet`, `elsewhere` empty | nothing to say | nothing; an empty `elsewhere` hides its link |
| `year` null | the fish do not all share one state today (all release or all closed), or the water is closed | no bar |
| `Box.strip`, `tag`, `label` null | none | the head alone |

A null is never a fallback for missing data. Data the builder cannot place stops the build (AGENTS W46).

### 2.7 Versions, and what older apps see

- **Before the switch:** releases without the section (or, if decision 1 is yes, with it), and side releases with
  it at `trout: n`. All are `regs-region/1`. Older apps check only `SECTION_NAMES` (`store.ts:116-122`) and select
  named columns (`reads.ts:298`), so they never see it [read]. The new app (after M0) reads it when listed and
  otherwise draws the old card.
- **At the switch:** `rows` 33 → 34 (Q-D9), `trout` required, and **`regs-region/1` → `regs-region/2`** for every
  `regs-{r}` file in `contract.py` FILES (`contract.py:142`). This is the fix for the review's blocker B1.
  - **Why the format, not only the section versions.** `readsRelease` (`manifest.ts:79-92`) and the held set
    (`boot.ts:353`, T403) compare **file formats** only; section versions are checked only when the regs store opens
    (`store.ts:116-123`), and that refusal is final (`WaterScreen.tsx:123`, no retry for `RegsStoreError`). With the
    format unchanged, (a) the S-built app would open its held rows-33 set (formats match) and refuse every regs page
    until the next launch, offline = no regs; and (b) an older app could install a rows-34 release and fail in the
    store instead of being asked to update.
  - **What the bump does.** The S-built app reads `regs-region/2` only (its formats come from `pack.generated.ts`,
    regenerated from `contract.py`), so the held-set path installs the new set over the old one before the map. An
    older app sees a format it cannot read, keeps its data, and `app_gate.oldest_reader` derives `minAppVersion`
    from the format through `app_reads.json` (the S-built app's row records `regs-region/2`). The stamp is never
    typed by hand (the T555 lesson).
  - **Dropped:** the earlier idea of recording `sections` in `app_reads` (old 0.7.x rows carry none, so it fixed
    only the stamp, and only with a backfill). The format is the one gate.
  - Optional, the app's (M0): `readsRelease` also checks `manifest.regulations.versions` against `KNOWN_VERSIONS` +
    the optional list, as a general guard (section 6).
- **Any later change of a regs table's shape** that an older reader cannot read bumps `regs-region/n` the same way;
  an additive optional section does not.

### 2.8 The shared fixture (AGENTS 40, the `dfo_vectors` pattern)

**Owner: ticket V-py (data), after I.** V-py owns everything in this section: the `trout_vectors` emitter,
`trout-hand.json`, the `drafts` section, `regs_fixture --item` read from the case list, the refresh script, and the
staging `side` scenario (Q-D11). The app's V reads what V-py writes.

**Files.**
- `app/fixtures/trout/trout-vectors.generated.json`, written by `python -m pipeline.tools.emit.trout_vectors`
  and registered in `pipeline/tools/emit/__init__.py`, so `python -m pipeline.tools.emit --check` covers it.
- `app/fixtures/trout/trout-hand.json`: hand-written contract edge cases that **both** sides must refuse. It is the
  single shared file, like `dfo-rows.json`, never a copy per side.

**Where the vectors come from, so CI can check them.** A full answers build cannot run in CI (AGENTS 39). So the
vectors are **decoded from the committed regs fixture release** (`app/packages/data/dev/regs-release/`, written by
`release/regs_fixture.py` from a built release). `--check` decodes those cells again and compares, with no build
needed. The regs fixture keeps the case waters, because its `--item` list is **read from the same case list**
(`trout_vectors.CASES`). One list, so the two cannot disagree. **`--check` proves vectors = fixture, not
vectors = builder**: a builder change reaches the vectors only through a re-cut, and each re-cut also moves
`taps.json`, the 9 VM fixtures and `sync.mjs` (`regs_fixture.py:1-27`). So V-py ships **one refresh script** that
runs the whole chain in order: side release → `regs_fixture` → `trout_vectors` → golden sync (`sync.mjs`), and
fails if any step leaves a file stale. Nobody runs the steps by hand. The mini release (`tools/mini_release.py --check`, the real pipeline
on invented waters) runs the builder and J in fast-fail with the flag on, so a broken builder is caught even before a
full rebuild.

**One case:**
```json
{"name": "skeena-sedan-mill-jul15", "water": "Skeena River", "anchor": {"stream": "<stream key>", "m": 123456},
 "day": "07-15", "weekday": "wed", "s": 4, "frame": {"…": "the decoded TroutFrame"},
 "composed": {"lead_today": "Up to 5 today", "lead_on": "Up to 5 on Jul 15"}}
```
`s` pins the app's `segmentIndex`. `composed` pins the `{day}` slot (a single token replace). **The screen-reader
label is not in the vectors.** Its rule is fixed here instead: a row's label is its payload texts **in payload
order** (name, origin, label, sub, `lines`, `quiet`, the band text that covers it, then its date), joined by ". ".
Python writes no composer for it; the app pins its own output in a golden over every vector row. Anchors follow
AGENTS W46 (stream key and metre, or a lake's wbk; never a part number).

**The cases** (about 45; each line says what it pins). From the spec's picture pack, the four attack reports, T805
and the app check:

| Water | Day(s) | Pins |
|---|---|---|
| Skeena River, Sedan Cr → Mill Cr (`gnis:2936`) | Jul 15, Sep 15, Nov 15 | one hatchery-only steelhead row with the label; stop duty "this river"; own "only 3"; shared "only 1 of these 4"; row, band and strip dates |
| Lois Lake (`wbk:329197058`), Khartoum Lake (`wbk:329197063`) | Jul 15 | two boxes counted apart; `top.second` = `together`; hatchery-only char (T805 C); steelhead hatchery only (owner 15:56); the same body interned for both lakes |
| Windsor, Dodd (`wbk:329197061`), Horseshoe, Nanton, Ireland | Jul 15 | char hatchery only (C5); Dodd's "only 2 wild" box sentence |
| Shamrock Creek, Kilgard Creek | Jul 15 | no 20–30 cm steelhead (T805 D) |
| Echoes Lakes | Jul 15 | `together` "from both lakes together" (T805 D) |
| Bowron Lake, Istsi Creek | Jul 15, a lake trout release day | park possession in the strip; lake trout's Region 7 one-day line on its own row (owner 16:37); Istsi's possession that came and went |
| Tchentlo Lake (Nation Lakes), Cunningham Lake | Jul 15 | possession below the day's number: `possession` band and `quiet` line; both lines as printed (owner 16:37) |
| Tagish Lake (`wbk:328961735`), Takla Lake | Jul 15 | possession with a size clause; Region 7 "1 day's limit" (T805 B) |
| Buckinghorse River (`gnis:10118`) | Jul 15 | bull trout possession = 1 |
| Fraser River in Region 2 at Mission | Jul 15 | box-level HATCHERY ONLY label; 30 cm; no wild rows under Release (owner 16:00) |
| Fraser River, Region 3 stretch (`gnis:39325`, by stream key and metre) | Sep 15, Oct 15, Nov 15 | box date "until Sep 30, then closed to trout and char"; closed-today form with `DateSet.top` "reopens Nov 1"; reopened |
| Bish Creek (`gnis:3480`) | May 20 | only steelhead closed: a closed group of one, not a closed water; its closed box may say "reopens …" (owner 15:36) |
| Adams River (`gnis:39257`, the Jul 31 stretch) | Jul 15, Jul 31 | 80 cm on the row; one-day sheet line; summed tier never drawn |
| Kamloops Lake (`wbk:329563838`) | Jan 15, Jul 15 | the summed "only 3" never drawn; DV and LT share "only 1 of these 2" |
| Elk River, mouth to Elko Dam (`gnis:16880`) | Nov 15, Apr 15, Jul 15 | all-release table with fold "Release", `year` bar; a whole-water closed day → `cell.trout` NULL; "Up to 1" |
| Kootenay Lake, main body | Nov 15 | two folds today → one table with two keep boxes |
| Glenn Creek (`gnis:10457`), Madden Creek (`gnis:12076`), Kaslo River, Keen Creek, Vedder River, Ross Lake | Jul 15 | each top-line form ("or", three numbers, "plus", across Region 2, size across boxes) |
| Kitimat River, Humphrys Creek (`gnis:2046`), Ruby Lake, Cultus Lake | Jul 15, Nov 1 | twin wild and hatchery rows; a wild size naming its fish; number and size changes dated |
| Bella Coola River | Mar 15 | looser-only change: "rule changes Apr 1" |
| Andi Creek (`gnis:10031`) | Jul 15 | the strip date is never given when a row's change is later (4th attack, fault 2) |
| Denetiah Creek | the day before its 15-day gap | a short gap still dated |
| Shuswap Lake (`wbk:329518145`), Bruer Creek, Mat Creek | Jul 15 | simple case; t670 K3 to K5 loose rows now inside the table |
| One of the 8 rule sets with no stored headline | Jul 15 | the top line built for them (D, then F) |
| A tidal key; an unnamed Region 6 stream | Jul 15 | NULL for tidal; the unnamed water covered |

**Hand cases (`trout-hand.json`), each refused by the pydantic model and by the app's reader:** a band gap; a band
overlap; a dangling rule index; a date key to a missing box or row; a row with dates at two levels; a key used twice;
a label on both a box and its rows; `{day}` twice, or outside `top.lead`; `covers` out of range; a `top` date on a
table that is not closed or release-every-one today; a `year` bar on a table that is not all release. The Python
test asserts the model refuses each one. The app's test asserts
`reportFailure` and no table drawn. The same file serves both.

**Before the builder exists** (app check 10.6): the first hand-written tables, for layout work, go in a third
section `drafts` and are marked as drafts. They are never the source of a word. When the builder exists, `--check`
replaces them with built cases, and any word that changed appears in G's catalogue diff for the owner.

### 2.9 Bytes, and how tickets A0 and A1 measure them

**Two tickets, not one** (review should-fix 3). **A0 (now, before E and F):** a prototype estimate of the section on
R6 and R5 with a crude table, plus three counts the contract depends on: rows-frame rows mixing trout and
non-trout members (Q-D4), angler-only rules closing trout or char alone (Q-D7), and trout answers that change
inside an hours window (2.2). Any non-zero count amends this contract before F. **A1 (after I and J):** the method
below on the real cutover form, every `WEB_BUDGET` moment, and the `water-shared` zdicts. **A1 gates L** (and,
if decision 1 is yes, the first normal release that carries the section).

**What is known.**
- [ran, app check 11b] The trout groups are 110,539 B of regs-6 as stored (26.7%), 128,216 of regs-5 and 122,374
  of regs-3. This was on r-46e4355a7277, before T805, whose lines make them somewhat larger.
- [ran, the spec's old 4.5, kept here] On r-46e4355a7277, `release.build.web_moments`: `first_tap` 4,198,757 of
  4,200,000 (1,243 B of room), `first_tap_low` 40,618 B of room, `cold_load` 307,463, `first_tap_unnamed` 181,994.
  `regs-{region}` is priced as the largest region file, regs-6 (413,612 B gz). Distinct trout row sets over frames
  per region: 94 (R8) to 425 (R5), 251 in R6; 108 to 489 with segments. The spec's per-region single-stream gzip of
  the whole group tree (R6 54,739 B) understates the stored cost by about half (app check 11b), and so did its 15 to
  40 kB guess for the section. The app check reads about 88 kB of first-tap room after the salmon lane's raise
  (`91cf5e8fc`, not on a built release; the review found the gate still at 4,200,000 at HEAD) [inferred].
- [ran, mine] Each region table's zdict in `water-shared` costs 0.5 to 7.3 kB as gz (23 dicts, 76,528 B in all).
  `water-shared` is in every first-tap moment, so the **four new zdicts cost about 8 to 25 kB** there, beside
  whatever the regs files carry.
- The app check estimates the section at 30 to 80 kB per region as stored. Per-blob deflate with a 32 kB dictionary
  compresses about half as well as one gzip stream.

**Budget.** The hard limit is every `WEB_BUDGET` moment (`contract.py:244`) on the **cutover** candidate: trout
section in, trout `group` out, new zdicts in. The soft target: in each region, section plus new zdicts ≤ the trout
groups it frees. If the candidate is over a hard limit, the levers are tried in this order: (1) sheets interned
apart (already in this contract); (2) phrases interned through the water file's `str` table, a codec change that
leaves the decoded shape the same; (3) moving `elsewhere` to a lazily read table. If still over, the owner's rule
applies (15:42, RULINGS O5): raise the limit by what was measured plus 10%, in L, and shrink later.

**Ticket A1's method** (committed as `pipeline/tools/audit/trout_size.py`, run through
`scripts/heavy.sh trout-size 50 -- …` because it needs a full answers build):
1. Build the answers and the release on a full bundle that has T805, twice: flag off (the baseline) and
   `--trout-table` on (the side release). Never promote either.
2. Build the **cutover** form from the side release: re-pack `rows_row` with `group` (and per Q-D9 `elsewhere`)
   removed from covered rows, with the release's own zdict, then VACUUM and gzip -9. That is the app check's m5
   method, which reproduced the shipped size to within 2 B.
3. Per region file: gz of the baseline, the side release, and the cutover form. Per new family: rows, distinct
   bodies, date sets and sheets, raw and as-stored bytes. `water-shared` with and without the new zdicts.
4. `release.build.web_moments` on the cutover form against `WEB_BUDGET` **at HEAD** (after the salmon raise lands):
   each moment's used, limit and room. The priced file is the largest region file after the cut, which may no longer
   be regs-6.
5. The ten largest bodies and sheets, by name, so the first lever has a target.
6. For decision 1: the same moments on the **side release** (old trout groups and the section together), so the
   owner sees what shipping the section before the switch costs until the cutover.
The output is a dated table in ticket A1. It passes when step 4 is within every limit, or when the overage is
stated for the owner's raise.

### 2.10 What the proof (J) reads, and what it proves

J is a separate decoder: it imports nothing from the builder (a test checks the imports). It reads the **decoded
table** and the law from the engine, never the builder's inputs.

- **Reading the words.** J parses each `Box.head`, `Band.text`, `strip`, `lines` entry, `together` and `top` form
  with the grammar of G's words module (G exports the grammar beside the composers, so one module states both).
  **A sentence J cannot parse fails the build**; it is never skipped as "free text".
- **Joint limits through the engine's basket proof.** Per-fish counts are not enough: box totals, shared bands,
  `together`, and possession below the day's number are joint limits. J reuses `engine.Engine`'s basket proof as
  `card_check.check_caps` does today (`card_check.py:630-660`): it builds baskets from the decoded table's limits
  and proves each against the engine. Nothing is re-written; the same proof runs over a new reading.
- **Possession.** A row or box possession line is present ⇔ that fish's possession differs from the page's lead
  (2.11), checked over every cell.
- **Dates.** Every `DateSet` text is checked against the following segments (spec 3.3): "rule changes <day>" only
  for a looser-only change on that day; worded lines only for a tighter change or a change of state; a reopening
  date only on something closed today, and on the right day.
- **Stamp.** J's report in the build stamp says how many tables it checked; that number must be at least the
  count of non-NULL `cell.trout` (a floor, so a J that silently checks nothing fails).
- **Mutation tests**, one per check: change one number, one size edge, one origin, one date, one scope, one
  possession line, a shared band's span, and an unparseable sentence in a built table: each must fail.

### 2.11 The page's possession line (ticket F0)

The page's possession lead is chosen once, in Python. F0 ships, in `RowsFrame.page_possession` (new at rows 34, so
**inside the cutover bump**): the lead sentence and its `src`, and the also-lines the page row keeps, which after the
switch are the non-trout fish only (the trout ones are on the table's rows; fight list #3). The rule is
`card.page_possession` (`card.py:410-422`). F0's code and tests land before F (the field is written only at rows
34): a pytest that `possession_duty` and the shipped lead use the same rule (a fish line exists ⇔ it differs from
the shipped lead), over every frame of the mini release and the fixture. J uses the same function. S makes
`standing.ts` read the field and delete `possessionRules`' pick (section 6).

### 2.12 The screen half (the app side; tickets in the spec's section 6)

Written by the app chat's Opus worker, 2026-11-16 17:30, against the app at `19bc7b7dd`. This is what the app does
with the payload above, and nothing more. Each line names the spec 6 ticket that builds it.

- **Gate (M0, then S).** The app lists `trout` in `OPTIONAL_VERSIONS` (`core/src/regs/versions.ts`, one line, the
  shape I0 parses). Absent = fine; present at another version = refused, by the store (`store.ts:116-123`) and, at
  install, by `readsRelease` (`manifest.ts:79-92`) over `manifest.regulations.versions`. At S, `trout` moves into
  `SECTION_NAMES` / `KNOWN_VERSIONS` and the region format is `regs-region/2`, read from `pack.generated.ts` and, in
  the store, from `REGS_FORMATS` (`versions.ts:10`), which a vitest then holds equal to `PACK_FILES`.
- **Read (M).** One cell per (key, segment): the app picks the segment with `segmentIndex` as today, and reads one
  `TroutFrame` for the shown day, never the whole year. Types are the emitted ones (wire and decoded); `CellIds` is
  emitted into **core**, because core cannot import `@app/data`. No section ⇒ `trout` absent; a NULL cell ⇒ `null`.
- **Check (N).** Before drawing, the app refuses exactly the cases in `trout-hand.json` (the same list the pydantic
  model refuses), through `reportFailure`, and draws no table. A refused table is a reported failure, never a
  fallback to the old card.
- **Compose: two things only (N).** (1) The `{day}` slot: one token replaced by `dayPhrase(day, today)` ("today" / "on
  Jul 15"), the helper the strip uses; the crawler page (T) calls it with its build day, so it reads "today" like the
  rest of that page. (2) The screen-reader label: a row's payload texts in payload order (2.8) joined by ". ". Both
  are pinned: the first by the vectors' `composed`, the second by an app golden over every vector row.
- **Draw (O, P).** Every `Txt` drawn unchanged. Layout only: boxes in order, bands beside their rows, the label as a
  filled tag, the date at the one level the frame names, the year bar from `table.year` (drawn only when set; `runs`
  used only as lengths, with the page strip's own fills). The folded entry shows `fold.value` and its arrow; the
  arrow replaces "Details" on the salmon and trout folds only (owner about 17:15). The fish sheet draws `FishSheet`
  and `DateSet.sheet`; its Sources are `src` as given.
- **Split (N behind the dev switch, then S).** Other fish = the rows-frame rows **not in `covers`**. Each row is drawn
  exactly once (a test over every vector and fixture cell). Fewer than two others: drawn in full as today (owner
  "Stays as today"). Caveat (b) skips covered fish (a test: no fish spoken twice).
- **Never in the app:** a trout word that states a rule, a count, a comparison of numbers, a sort / union / cut of
  rule refs, a choice of which date to show, a choice of whether the year bar shows, or a hand-typed payload type.
  After S, `TROUTISH`, `TROUT_LINE_FISH`, `troutLine`, `STEEL_LINE`, `quietOf` and `possessionRules`' pick are gone.
- **The page's game-fish strip is not the table's year bar.** The strip's trout pill ("Trout release only until Mar
  31", `strip.ts:122-135`) is deleted at S (owner about 16:55, "Drop it"); the table's "Trout and char through the
  year" bar is Python's `year`, shown only when every trout and char fish shares one state on an open water (owner
  12:42). Two elements, two answers, both kept (spec 6).

---

## 3. For the owner

Two questions, both **PENDING OWNER**. The plan is written with the recommended option of each.

1. **Ship the trout section in normal releases before the switch?** Recommended **yes, if A1 measures it at about
   100 kB or less**. Yes: J proves the tables on every real release, staging previews are ordinary releases, and the
   switch shrinks to the app update plus deleting the old trout card; the first-tap limit goes up by that amount until
   the switch, then comes back down. No: the tables exist only in test releases until the switch, which is bigger.
   (2.1 says how the plan changes either way.)
2. **Q-D7: where the "closed to some anglers" notes sit** (youth waters, Skeena and Babine weekends). Recommended:
   **stay in the page's caveat block**, where they show today, above all fish. They close the water to some anglers,
   not trout alone, and showing them in two places would say them twice. Or: inside the trout table, above the top
   line, as in the round 7 mock; the page block then leaves them out.

Decided, recorded here so nobody re-asks: a reopening date on a closed fish's row (Bish Creek steelhead) is allowed
(his 15:36: "drop reopen unless it is currently closed then reopen date matters"); the year bar only when every
fish shares one state (all release or all closed; owner 12:42); "All rows get arrow" = **the arrow replaces "Details" on the salmon and trout folded
entries, and the Other fish list stays as it is** (owner confirmed about 17:15, "Salmon + trout folds").

Everything else is decided from the code or from his earlier answers. The exact sentences (the closed-today form,
"rule changes <date>", the folded entry's short line) reach him in G's catalogue.

---

## 4. Where the two sides could still fight (for the next reviewer)

The app check's section 12 lists every place the trout card decides in TypeScript today (49 entries). The ones this
contract removes, with how they are removed, are 1 to 7. The ones that stay are 8 to 14.
Entries 15 to 18 were added by the fresh review (2026-11-16).

1. **Which fish are trout:** `others.ts:24` `TROUTISH` (with BT, CT_ws, CT_c) and `strip.ts:122` `TROUT_LINE_FISH`
   against `common.trout_char()`. **Removed (S):** other fish = rows not in `covers` (Q-D4); `splitRows`,
   `looseRows` and `otherFishOf` (`others.ts:24-53`) stop reading `TROUTISH`, and `TROUTISH` and `TROUT_LINE_FISH`
   are deleted. Tests: over every fixture cell, each rows-frame row is drawn exactly once; a lone other fish stays.
2. **The page's possession lead is picked twice**, by `card.page_possession` (`card.py:410-422`) and by
   `standing.ts:97-104` `possessionRules`, with different rules. The table's "differs from the page" is judged
   against Python's choice. **Fix:** F0 (data) ships the lead inside the cutover bump (2.11); S reads it; a vitest
   checks `standing.ts` holds no pick logic.
3. **The page's possession row repeats per-fish lines:** `standing.ts:133` `possessionOf` draws every non-lead
   possession sentence as an `also` line (T670 K1). After the switch, the trout ones are also on the table's rows.
   **Fix:** F0 ships which also-lines the page row keeps (non-trout fish only); S draws them. The TS
   `possessionValue` regex with its silent null (`standing.ts:117`, app check 12.2 #40) is a W46 item of its own.
4. **Date words from three TS engines** (`plainrow.ts:62-169` `YearWords`, `strip.ts:128` `troutLine`,
   `caveats.ts:225-243` per-moment answers) beside `DateSet`. **Removed for trout:** `troutLine` goes (owner, about
   16:55, "Drop it"). `plainrow.ts` leaves the trout path (`covers`). `caveats.ts` timed (b) skips covered fish (S),
   with a vectors test that no fish is spoken twice; the table says their weekday words and hours-window words
   instead (2.2, 2.5; F writes them; A0 counts the hours-window ones). `strip.ts:137-174`'s 21-day "Closed from …"
   stays (whole water), and it can sit beside the table's `card` line, "Until May 31, then closed to all fishing".
   Both are true. **Owner: G**, whose catalogue shows the owner that pair once.
5. **TS rewrites Python's sentences:** `tree.ts:111` `dutyWords` ("this river" → "the Skeena River") and `tree.ts:56`
   `subcapLead`. **Removed:** the table's strings are drawn unchanged. N's test checks every drawn string is in the
   vectors' frame, none added or changed.
6. **The steelhead line** (`tail.ts:83-91` `STEEL_LINE` and `quietOf`, TS words over Python's enum). **Removed for
   trout:** Python writes both as `quiet` lines (2.3; F, words from G). S deletes `STEEL_LINE` and `quietOf` unless
   the other-fish path still reaches them (S2 measures).
7. **The hand-typed version gate:** `versions.ts` and `app_gate` see no section versions. **Fix:** 2.1 (table names
   generated; I0 pins every section version, optional ones included) and 2.7 (the regs format bump to
   `regs-region/2` at the switch, so the format-only gate and the held set do the right thing).
8. **What stays composed in TS:** the `{day}` slot and `segmentIndex` (`tap.ts:54`, against `encode.py:1045`),
   pinned by the vectors; and the screen-reader join, which follows the payload's order (2.8) and is pinned by the
   app's own golden only. A drift fails one suite, and the fix goes on both sides in one change.
9. **Build checks that will check nothing:** `answers.py:584-598` (`origin_unsaid`, `across_unsaid`) and
   `card_check.check_tree` / `possession_problems` walk `group.tree`, and skip rows with no group
   (`tree.py:141-146`, `card.py:406`). Once L drops `group` on trout rows, they skip every trout card silently.
   **Fix:** J holds the cross-row, joint-limit and possession proofs (2.10); **L re-aims each check at J, with a
   mutation test per check** that it still fires on a planted trout case. Nothing is deleted without its re-aimed
   twin. This is the "checks seeded from input launder" lesson in another form.
10. **Two wordings of one duty during the build-behind:** the old card's `_duties` ("stop fishing this river after 1
    a day", `card.py:385`) and the table's ("after keeping 1, stop fishing this river today"). Both are Python. The
    old one goes at the switch: the stop duty is steelhead-only, so trout-only. G owns the table's form, and the
    sheet's duty sentence is built by the same function as the row's.
11. **The wire decoder is hand-written TS** (`reads.ts`: the frame triple, sheet ids, zdicts). It is pinned by the
    app's store test decoding the committed fixture release, and by the vectors, which are that same release
    decoded in Python. A codec change fails both until both move.
12. **The app's shape checks repeat the pydantic validators** (bands, refs, date keys). Both refuse the same
    `trout-hand.json` cases. A check on one side only is a gap, not a fight. Keep the lists in step through that
    file.
13. **FISH_TIPS and pictures** come from `tools/emit-fish.mjs` (JS, the fish manifest), not Python. That is content,
    not law. Pin: N's test asserts every `fish` code in the vectors has a picture and tips, so a missing one is
    reported, never a blank.
14. **Out of scope, still TS rule logic** (not trout, so not this plan): `plainrow.ts`, `grouphead.ts`, `tree.ts`
    and `tail.ts` for other fish; `caveats.ts` angler and timed wording; `strip.ts` "Closed from …";
    `banner.ts:122` `sayOnce`. These are the follow-up ticket the spec proposes (risk 6).
15. **Hand lists of table names** (`store/build.py:37-43`, `store/common.py:58-62`, `release/shards.py:22-26`, the
    slices, the app's `CellIds`). **Fix:** emitted or derived from `contract._REGION_TABLES`, or held to it by a
    test (2.3; I0 for `CellIds`, I for the rest).
16. **Lines built before the pass that decides them** (weekday words, licence / stamp line, steelhead stamp).
    **Fix:** composed in `compose_key` from post-pass values (2.2).
17. **Two fold grammars on one screen:** the table's `fold.value` ("Up to 5") and the Other fish list's TS
    `shortValue` ("5 a day"). Both stay (other fish are out of scope); G's catalogue shows the pair to the owner.
18. **The day-word slot on the crawler page** (`seo/model.ts`, ticket T): the server-rendered page has no "today".
    T owns the rule (section 6); Python's part is only that `{day}` is the one slot.

---

## 5. Not checked

- I ran no build, no answers build, no release and no test. Every line anchor is read at HEAD `384470bad`, except
  those quoted from the app check, which carry its tags.
- The case table's anchors: the waters named without a `gnis` or `wbk` were not resolved. The emitter resolves them,
  and refuses a name that matches more than one water.
- Whether any angler-only closure names trout or char alone (it would change the Q-D7 recommendation for that
  rule). Ticket A0 counts it.
- Whether any rows-frame row mixes trout and non-trout members (Q-D4). Ticket A0 counts it.
- How many trout answers change inside an hours window (2.2). Ticket A0 counts it.
- Whether a manifest under `test/` can point at files under `test/release/` (Q-D11). V-py checks it first.
- Bytes: nothing here is measured beyond the zdict sizes. The section's size is A0's estimate and A1's measure.
- 2026-11-16 revision: I re-read at HEAD `bb08463fe` only the anchors the revision leans on: `contract.py:27`
  (`OPTIONAL`), `:142` (`regs-region/1`), `:17-23`; `pack.py:29,50-51` (`PACK_OPTIONAL`); `store/build.py:37-43,
  280-282`; `store/common.py:58-62`; `shards.py:20-27`; `slices.py:42-52`; `answers.py:556-616`;
  `test_answers_tidal.py:113-121`; `app_gate.py:54`. The app anchors (`manifest.ts`, `boot.ts`, `store.ts`,
  `WaterScreen.tsx`, `others.ts`, `tail.ts`, `KeepTab.tsx`) are the review's, not re-read.

---

## 6. For the app chat

The app changes that follow from the review's D findings. Section 6 of the spec (M0 to S2) is the app chat's; these
are for it to fold in, or contest.

**Status 2026-11-16 17:30 (app chat's Opus worker): all 16 are folded into the spec's section 6, each with its files,
tests and dependencies; the screen half is summed up in 2.12.** Two need the main chat (marked **ASK**), and two
corrections to the review come from the code (items 1 and 4).

1. **S reads `regs-region/2`** (2.7). Check that the held-set path (T403, `boot.ts:353`) installs a `/2` set over a
   held `/1` set before the map, in a test. **DONE → spec 6, S.** Correction: the format is not only generated. The
   store checks a hand-typed `REGS_FORMATS.region = "regs-region/1"` (`core/src/regs/versions.ts:10`, read at
   `data/src/regs/store.ts:193`). S moves it to `/2` and adds a vitest holding `REGS_FORMATS` equal to `PACK_FILES`.
   The held-set test goes in `tools/release-boot.test.ts` beside T403's case (`:688`).
2. **M0 (optional, recommended):** `readsRelease` (`manifest.ts:79-92`) also checks
   `manifest.regulations.versions` against `KNOWN_VERSIONS` + the optional list, so a version mismatch is caught at
   install time, not as a final `RegsStoreError` on every page (`WaterScreen.tsx:123`). **DONE → spec 6, M0** (taken,
   not optional; a manifest with no versions passes; tests in `manifest.readable.test.ts`).
3. **Rename** the app's hand-typed optional list to `OPTIONAL_VERSIONS` (it holds versions; the table and column
   names come generated as `PACK_OPTIONAL_SECTION_TABLES`). I0 parses it, so its line shape is agreed before M0
   lands: `export const OPTIONAL_VERSIONS = Object.freeze({ trout: 1 } as const);`. **DONE → spec 6, M0.** For I0:
   the line matches `KNOWN_VERSIONS`' regex (`test_answers_tidal.py:116`); at S it becomes `Object.freeze({} as
   const)`, which I0 must read as "no optional sections"; and `KNOWN_VERSIONS` also holds `glossary: 5`, which is not
   an `A.SECTIONS` name.
4. **Fix the stale test pins:** `store.test.ts:17` hand-types `rows: 33`; `versions.ts:4` cites a test file that does
   not exist. Point both at I0's test. **DONE → spec 6, M0** (`store.test.ts:17` built from `KNOWN_VERSIONS`; `:110`
   dropped; `:136` uses `KNOWN_VERSIONS.rows + 1`). Correction: `versions.ts:4`'s reference is live, the file is
   `app/conformance/src/regs/store.release.test.ts` (it holds `KNOWN_VERSIONS` to the golden's layout); the comment
   adds I0's pytest beside it.
5. **Generated types:** use `CellIds` from `pack.generated.ts` (I0) instead of `inputs.ts:24`, and the emitted
   `TroutFrameWire` / `TroutTableWire` / `BoxWire` / `TRowWire` in `reads.ts` (I). **DONE → spec 6, M. ASK (I0):**
   `CellIds` cannot come from `pack.generated.ts`: that file is in `@app/data`, and core, where `CellIds` lives and
   is read (`inputs.ts:24,34,54`), imports nothing. Emit it into core (`answers.generated.ts` through
   `model/emit.py`, or a new core `*.generated.ts`). `keyCells`' SELECT (`reads.ts:298`) and `cellOf` (`:647`) are
   built from the same generated column list.
6. **S, the Other fish split:** other fish = rows not in `covers`; delete `TROUTISH` and `TROUT_LINE_FISH`;
   `splitRows`, `looseRows`, `otherFishOf` (`others.ts:24-53`) stop reading them. Tests: each rows-frame row drawn
   exactly once over every fixture cell; "lone other fish stays". **DONE → spec 6, N** (the `covers` split and both
   tests, behind the dev switch) **and S** (the deletions).
7. **S, deletions:** `STEEL_LINE` and `quietOf` (`tail.ts:83-91`) (Python writes both as `quiet` lines). **DONE →
   spec 6, S** (with the call at `keep.ts:66`, `GroupVM.quiet`, and their tests; `quietOf` reads only rows holding
   RB, which are all covered after S; S2 confirms).
8. **S, possession:** `standing.ts` reads `RowsFrame.page_possession` (F0) for the lead and the kept also-lines,
   and loses `possessionRules`' pick; a vitest that it holds no pick logic. **DONE → spec 6, S. ASK (F0):** also
   ship the lead's value words ("2 days' quota"); today they are cut from the sentence by the `possessionValue`
   regex (`standing.ts:117-120`), which returns a silent null when the sentence does not match.
9. **S, timed caveat (b)** (`caveats.ts:225-243`): skip covered fish; a vectors test that no fish is spoken twice.
   The caveats skip and the `covers` split can land **behind the dev switch before the cutover** (additive).
   **DONE → spec 6, N** (behind the dev switch, with the test) **and S** (unconditional).
10. **N/O, screen-reader label:** join in payload order (2.8); pin it with an app golden over every vector row. The
    vectors no longer carry `composed.a11y`. **DONE → spec 6, N** (O draws N's labels; the golden moves only
    through V-py's refresh script).
11. **O, the fold row:** the expand arrow replaces "Details" on the **salmon and trout** folded entries
    (`KeepTab.tsx:43-48`, `FoldRow` `KeepBlocks.tsx:250-292`); the Other fish list stays as it is (owner, about
    17:15, "Salmon + trout folds"). **DONE → spec 6, O**; the spec's 16:55 wording is corrected there, with the
    17:15 answer recorded. Found with it: the walkthrough says "Tap Details" (`help.ts:171-179`), so O rewords it
    and bumps `WALKTHROUGH_VERSION`.
12. **O / F, angler notes:** conditional on Q-D7 (PENDING OWNER). With the recommended answer the table has no
    `notes`, and O draws none; the caveat block keeps them. **DONE → spec 6, O** (both branches written; the
    table branch adds about 0.25).
13. **O, year bar:** only when every trout/char fish shares one state today (all release, or all closed) on an open
    water (owner 12:42: "i like it on the elk where it says trout and char through the year when they are all
    release (or all closed etc) if some are open it seems silly"). **DONE → spec 6, O and S.** Checked in the code:
    this bar and the 16:55 "Drop it" line are **two different elements**. The dropped one is the trout pill on the
    page's game-fish strip ("Trout release only until Mar 31", `strip.ts:122-135`, drawn `YearStrip.tsx:93-96`): S
    deletes it. The kept one is the table's `year` ("Trout and char through the year"), which Python sets: O draws
    it. Both answers hold.
14. **T, the crawler page's `{day}` slot:** the server render has no "today"; T needs a rule (for example the
    page's stated date, or a day-free form), and the vectors can carry it once T names it. **DONE → spec 6, T.**
    Rule: the crawler page is the app's page on its build day (`seo/model.ts:281`), and it already says "today"
    against that day (`:165-170`). T fills the slot with N's `dayPhrase(day = today = build day)`: "Up to 5 today".
    The vectors' existing `lead_today` pins it; nothing new for V-py.
15. **V reads V-py's files;** the refresh script (side release → fixture → vectors → golden sync) is V-py's, and
    the app's goldens move only through it. **DONE → spec 6, V** (loader, fast-fail step beside the DFO vectors
    step at `scripts/fast-fail.sh:284`) **and R** (the picture pack's waters are the vectors' case list).
16. **Version bumps:** any change of the `trout` shape bumps its version; APKs from M0 on refuse a side release at
    another version, as designed. Bump `OPTIONAL_VERSIONS` in the same change, or I0 fails. **DONE → spec 6, M0**
    (stated as M0's rule).
