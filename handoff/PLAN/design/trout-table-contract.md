# Trout table: the payload contract (data side)

2026-11-16 17:00 (clock read), written by an Opus worker for the main chat. This is the agreement between the Python
pipeline, which writes each water's trout table into the release, and the app, which only draws it. It answers the
app chat's eleven questions (Q-D1 to Q-D11, in `trout-table-app-check-2026-11-16.md` section 10.8) and corrects that
file's first-cut contract (its section 10). The spec (`trout-table-build-spec-2026-11-16.md`) keeps the design and
the tickets. Where the spec's 4.1 to 4.7 and this file disagree, this file wins. The app chat confirms or contests
it before ticket I or M starts.

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
   compare or re-word. The proof (ticket J) reads the law back from the shipped words (see 2.9).
3. **Every empty field states why.** Each nullable field is paired in a model validator with the condition that
   empties it. This is the convention of the answers models (`model/__init__.py:1-6`).
4. **One fixture.** One committed vectors file, written from Python and read by both test suites (2.8). Where the app
   must compose something (the day phrase, the screen-reader label, picking the segment), the vectors carry the
   finished string, and the app must reproduce it.
5. **One trout path after the switch.** The `--trout-table` flag, the app's dev switch and the trout rows' `group`
   leave in one landing (spec 7.4). After it, no trout word comes from the old group tree or from a TS composer.

---

## 1. The eleven answers

| Q | Decision | Why (code or ruling) | Cost |
|---|---|---|---|
| **Q-D1** Box words | **Agreed: Python writes them whole.** Each box has `head`: "Keep 5 in all" on a keep box, and the column word ("Release", "Closed") on a release or closed box. Every band text ("up to the 5", "only 3", "only 1 of these 4") is Python's too. The box carries **no `n`**: the spec's `Box.n` and the app draft's `n: number` are removed | AGENTS 33 (`AGENTS.md:228`): a sentence is pinned whole, never built from a number. Guarantee 2: an `n` the app can read is a number it could count with. The app check found TS re-deriving tier numbers today (`grouphead.ts:125-145` `headCtx`, item 10 of its 12.1) | J must parse numbers out of words (2.9). The app's 200% view, which folds bands into bold lines, uses the band text as written |
| **Q-D2** The day phrase | **Agreed, with one change.** No Python string says "today" except the stop duty ("…stop fishing this river today", which is about the angler's day). The top line carries **one `{day}` slot** where the day phrase goes: `"Up to 5 {day}"`, `"Closed for trout and char {day}"`. The app puts its own words in the slot ("today" or "on Jul 15"). Python decides **where** the slot goes, and whether there is one at all | The page shows any day (`strip.ts`, `WORDS.TODAY_LC` and `ON_DAY`). Appending a day word to every lead would be wrong, as in "Closed … until Oct 31 today". A slot keeps word order Python's. The validator allows `{day}` only in `top.lead`, at most once | The app replaces one token. The vectors carry the result for both day modes, so the replacement is pinned (2.8) |
| **Q-D3** NULL cell | **Agreed, made exact.** `cell.trout` is NULL exactly when the page draws no trout table: (a) the display frame at the same key and segment says `closed` (the whole-water banner speaks), (b) it says `tidal`, or (c) the rows frame asks about no trout or char fish. The builder reads (a) and (b) from the **display frame the app also reads**, so the two cannot disagree. Outside B.C. there is no key at all. Unnamed parts are covered like named ones. A trout-only closure (Fraser Region 3, Oct) is **not** NULL. It is a table whose top line is "Closed for trout and char {day}" | `answers.py:612-616`: a section composed from other sections at the same key and segment. The display frame's status is what the banner draws (spec 2.3; app check 6 "Closed banner") | A build check: NULL ⇔ (a) or (b) or (c), over every cell (part of I) |
| **Q-D4** Loose rows and rows without a group | **Agreed: every trout and char fish on the frame is in the table, exactly once** (a keep box row, or the release or closed group). That includes loose rows (Region 8's stream brook trout, t670 K4 and K5) and the 2,202 frames with no group. Which rows the table replaces is a fact Python ships: `covers`, the rows-frame row indexes. The app's trout or other-fish split reads `covers`, **never `TROUTISH`** | The trout set is one Python predicate: `common.trout_char()` (`common.py:525`) = CT, DV, EB, GB, LT, RB, ST [ran]. The TS hand list `others.ts:24` `TROUTISH` adds BT, CT_ws and CT_c. Two lists put a fish in both places or in neither (app check 12.9 #4) | A covered row whose members are not all trout or char stops the build. Ticket A counts such mixed rows. If any exist, this contract is amended. They are not handled silently |
| **Q-D5** Two cards today | **Agreed: one table**, one top line, the keep boxes in Python's order (the two-box "counted apart" form, spec 3.1). Kootenay Lake's 43 frames, and Lois and Khartoum, become one table each | `scope.apart` and `parent` are stored on the rows (spec 2.2). The top line's form is Python's choice from them | None beyond F |
| **Q-D6** Rule refs | **Decided:** every element that states a rule carries `src`: the rule indexes, in the same index space as the rows frames (`RuleIx`, the water file's `rule.rix`, `contract.py:131`). A row's `src` holds the rule that decides its state or count first, then size, origin, possession, the duty, and the rule behind its date line, with no repeats. A sheet's `src` is that list cut to 4 by Python. The table's `src` is every rule any part cites, in first-cited order: it feeds "Sources ›". **The app never sorts, unions or cuts a list** | Spec 3.4 ("this fish's rules first, up to 4"). The app check lists TS choosing and ordering rules today (12.2 #45 `decided.ts:71-200`) | J checks that every ref exists, and that every rule the engine says decides a trout fish on that reading is in the table's `src`. "Sources" can never miss the rule that decides |
| **Q-D7** Angler-only notes | **Owner's call. Recommended: the notes stay in the page's caveat block, and the table carries none** (`notes` is removed from v1). If the owner wants them in the table, the table carries them and the caveat block skips them by the rule indexes the table names. They are never in both places | The 34 angler-closure rules close a water to a class of angler (youth waters, Skeena/Babine weekends), not trout alone (adversarial review, finding 8). They are page-wide like the other caveats. `caveats.ts:187-191` `anglerOf` already places them by moment from stored frames | Recommended option: none. It departs from the round 7 mock (notes above the top line), which is why the owner is asked |
| **Q-D8** Possession | **Decided:** the page keeps its line (owner 15:42). A trout row carries a possession line only where `card.possession_duty(P, model, S)` (`card.py:446`, T805 B, RULINGS P58) returns one. The row form of the line goes in `lines` (bold, today's rule). When possession is below the day's number (Cunningham), the band is `kind: "possession"` ("only 2 in your possession") and the explanation goes in `quiet` ("3 a day, but never more than 2 lake trout with you"). When every row of a box shares the line, it goes in the box's `strip` (Bowron: "In your possession: only 1 in all, not 1 of each"). The sheet's sentence comes from the **same** function. **Two fixes come with it:** (1) the page's possession lead is chosen once, in Python, and the page reads it; (2) at the switch the page's possession row stops repeating trout fish lines (see the fight list) | Python's `card.page_possession` (`card.py:410`) chooses "written for every game fish, else the largest". TS `standing.ts:97-104` `possessionRules` chooses "names no fish, else most days". Those are two different rules, and the table's "differs from the page" test is made against Python's choice [read] | A small ticket before F: ship the lead, and have `standing.ts` read it |
| **Q-D9** The rows 34 drop list | **Decided, with the rule for it.** On trout rows (those in some cell's `covers`), `group` and `elsewhere` are dropped at rows 34. `presence` is dropped if S's grep finds no reader outside the trout path. `year`, `week`, `items`, `conds`, `real_daily`, `everyone` and `stamps` **stay**: the year bar, `caveats.ts` and `card.ts` read them for the whole page. A field leaves a trout row only when **both** hold: the table says what it said, and the app's grep at S finds no other reader. The `Row` validator `_pool_rows` (`model/rows.py:1124`: "the kept-elsewhere line is set <=> a keep row") becomes "set <=> a keep row that no trout table covers" | Size: the trout groups are 110.5 kB of regs-6 as stored [app check 11b, ran]. The other fields are small and are read elsewhere | L also moves the build checks that walk `group.tree` (`answers.py:584-598`, T793 and T805): without `group` they would check nothing on trout rows (fight list #9) |
| **Q-D10** Where the sheets live | **Decided: a separate interned table `trout_sheet`.** The table body names each sheet by id. Decoded, the frame holds the sheets inline. The sheet's coming-change sentence stays in the date set (it changes by segment) | Sheets are the bulk of the words, and many repeat across waters, for example a region's default lake trout sheet. Interning them apart keeps one copy even when the tables differ. That is the `rows` 30 lesson: the strip inside the row made 3,794 rows out of 2,512 (`encode.py:806-809`) | One more blob family and zdict (2.9). Ticket A may fold the sheets back into the body if it measures no gain. The decoded shape does not change |
| **Q-D11** Side release for previews | **The staging test channel is real for phones but cannot carry a side release today.** `deploy/staging/test-updates.mjs` (T251) moves only the manifest and the feed indexes under `test/`. Its `release` scenario re-points the live files. `upload.sh:121-124` clears everything the live release does not name, except `test/**` [read]. **Recommended:** make the picture pack (R) from a **local** side release through the web export, so no upload is needed. Add a small `side <dir>` scenario that uploads the side release's files under `test/release/<id>/` and points `test/manifest.json` at them, used only for the fresh-install walk on a phone | Staging upload and clear are allowed (memory: staging R2 + deploy allowed). The web always serves the app and the release together, so it cannot preview a side release | A small deploy ticket [inferred: whether the manifest's file URLs resolve under `test/` was not checked] |

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
- `contract.py` gains `OPTIONAL_SECTIONS = {"trout": {"cell": ["trout"], "tables": [the four]}}`. `pack.py`
  (`tools/emit/pack.py:26`) emits it into `pack.generated.ts`, so the app's M0 reads the **column and table
  names** from generated code. `test_release.py` (which holds a built release to the contract exactly,
  `contract.py:3-6`) also holds the section all or nothing.
- **The version stays hand-typed on the app side, on purpose.** `versions.ts` says which version the reader was
  *written for*. A generated version would silently accept a v2 the code was not written for. The app's
  `OPTIONAL_SECTIONS = {trout: 1}` is pinned to `Section("trout", 1)` by extending `test_answers_tidal.py:113`
  `app_reader_versions` to read it, as `KNOWN_VERSIONS` is pinned today.
- At the switch (L and S together), `trout` moves from optional to required: into `_REGION_TABLES` and `cell`
  (`contract.py:17-23`) and into `SECTION_NAMES` and `KNOWN_VERSIONS`. `OPTIONAL_SECTIONS` is then empty again.

### 2.2 Where it is built (no new day cuts)

The trout section is a **whole-key compose**. It runs once per answer key over all the key's segments, after the
rows post-passes (weekday words, year strips, licence stamps and the T793 and T805 checks, `answers.py:556-608`) and
after the display compose (`:610-616`). It needs a new hook beside `compose`: `Section.compose_key(values per
section, starts, moments) -> [value per segment]`. That is ticket I's work.

- **It adds no day cuts.** Its value is a function of the rows and display values over the key's segments. Any
  change in them already starts a segment (`answers.py:654` `cut`), so inside one segment the next change is
  always the same. The build checks this: two adjacent segments whose rows and display values are equal must not
  get different trout frames unless their dates differ.
- **The body is built per reading, inside the rows derive**, where `card_of` runs today
  (`rows/assemble.py:118-120`). It needs the reader's `P` and `model` to reuse `card.possession_duty`, `_duties`
  (`card.py:367`), `tree.across_rows` (`tree.py:105`), `A_BOTH_LAKES_SAID` (`tree.py:101`) and `size_rule`
  (`tree.py:942`) **as they are, not re-derived** (spec risk 9). It leaves the derive on a private key that is
  stripped before rows encoding and is never in `RowsFrame`.
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

class YearBar(Model):         # only when every fish is in one state today AND the water is open (spec 3.1)
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
    top: Optional[Txt]        # under the top line, only while closed today: "reopens Nov 1" (owner 15:36)
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
    #  - top: set => the table is in a closed or release-every-one form (no reopening date on a coming closure,
    #    owner 15:36)
```

Wire against decoded: the codec (`TroutCodec` in `encode.py`, ticket I) turns `TRow.sheet` into a `trout_sheet` id
and the frame into `[table id, dates id | null, covers]`. The model above is the **decoded** value, as
`RowsFrame` is today (`encode.py:827-842`). Two copies of the decoded shape are emitted, one per side of the sheet
id (`TRowWire` and `TRow`), or the wire is left to the codec spec text. Ticket I picks one and says which.

### 2.4 Who writes which word

**Python writes, whole:** `top.lead` (except the slot), `top.second`, `fold.value`, every `Box.head`, `label`,
`strip`, `tag`, every `TRow.name`, `sub`, `lines`, `quiet`, every `Band.text`, `together.text`, `year.title`,
`elsewhere`, every sheet `head` and `here` line, and every `DateSet` text. That includes the HATCHERY ONLY label (as
`label`) and the Release and Closed column words (as `head`). They all come from one words module
(`answers/trout/words.py`, ticket G), with whole-string tests, and are listed in G's catalogue for the owner.

**The app writes only navigation words** (`words.ts`): the entry's name "Trout and char" (exists today), "How to
tell a hatchery fish ›", "Sources ›", "Fished elsewhere today? ›", the sheet's section heads ("Here, today", "How to
tell it", "Sources", "All sources ›"), the shape marks ↩ and ✕, and the day phrase in the `{day}` slot. **No app
word states a rule.** The origin tag draws `TRow.origin` as the word itself. Upper-casing the label is styling.

### 2.5 Days, "until …" lines, and where a date sits

- **One table body and one date set per (answer key, segment).** The app picks the segment with `segmentIndex`
  (`tap.ts:54`, the port of `encode.py:1045`), as it does today. That is the only date choice the app makes.
- **The body holds no date words.** Every NEXT text is in `DateSet`, so a body that reads the same today is one blob
  across segments and waters (spec 4.1's interning).
- **The date rule is spec 3.3, decided in Python.** The first later segment whose answer for the fish differs is
  dated. A change that only loosens reads "rule changes <day>". A tighter change, or a change of state, is said in
  words. A coming closure gets no reopening date. The reopening date appears only in `DateSet.top`, and only while
  the fish is closed today. The year wraps. A gap under 10 days raises a build warning. The Adams one-day segment
  puts 80 cm on the row and one `here` line in the sheet.
- **Weekdays and hours.** The walk steps over **start days**, not segments. One start's moments (its weekday
  classes, outside any hours window, as `year.py:12` does) count as one answer, a tuple. A fish whose answer changes
  by weekday gets its weekday words as a `lines` entry (from `rows[].week`, `say_weekdays`, `answers.py:556-571`).
  **No date line ever names a weekday**: Python does not know the calendar year, and the app computes weekdays from
  it (`tap.ts:85-89`). The inside of an hours window is never what the table is drawn from, because the app taps
  `inside=false` (`tap.ts:55`). Those cells are built and proved like any other, so none is ever missing.
- **Placement is Python's** (spec 3.3 rule 7): card if every fish shares the date and words, else the box (strip)
  if every row of the box shares it, else the band (white box) if all its fish share it, else the row. The
  frame validator allows one level per row. The app draws a key where it points and decides nothing.

### 2.6 What NULL and empty mean

| Value | Meaning | The app draws |
|---|---|---|
| no `cell.trout` column (no `versions.trout`) | this release has no trout section | the old card (build-behind only); after the switch the release is refused |
| `cell.trout` NULL | Q-D3 (a), (b) or (c) | no trout table and no Trout and char entry; the banner or tidal page speaks |
| `frame.dates` null | no fish of the table changes later in the key's year | no small lines anywhere |
| `top.second`, `together` null | no cross-box limit | nothing |
| `TRow.origin`, `label`, `sub` null | no twin row / no label / no sub-line | nothing |
| `lines`, `quiet`, `elsewhere` empty | nothing to say | nothing; an empty `elsewhere` hides its link |
| `year` null | not one state today, or the water is closed | no bar |
| `Box.strip`, `tag`, `label` null | none | the head alone |

A null is never a fallback for missing data. Data the builder cannot place stops the build (AGENTS W46).

### 2.7 Versions, and what older apps see

- **Before the switch:** releases without the section, and side releases with it at `trout: 1`. Older apps check
  only `SECTION_NAMES` (`store.ts:116-122`) and select named columns (`reads.ts:298`), so they never see it [read].
  The new app (after M0) reads it when listed and otherwise draws the old card.
- **At the switch:** `rows` 33 → 34 (Q-D9) and `trout: 1` required. Every older app refuses the release and keeps
  its data. The `minAppVersion` stamp asks for the update (`publish.py:84`, `boot.ts:131`).
- **The stamp must not be typed by hand.** `app_gate` derives the oldest allowed app from **file formats only**
  (`app_reads.json` records `regs-*` as `regs-region/1` and nothing about sections) [read]. A `rows` 34 release is
  still `regs-region/1`, so the gate passes an app that refuses it. That is the T555 failure again (water/3 shipped
  on a hand stamp). **Fix, part of L:** `pipeline.tools.emit.app_reads` records each app version's section versions
  too (`KNOWN_VERSIONS` and the optional ones), and `app_gate.oldest_reader` matches them against the manifest's
  `regulations.versions`. Recorded rows stay unchanged; new rows carry `sections`.

### 2.8 The shared fixture (AGENTS 40, the `dfo_vectors` pattern)

**Files.**
- `app/fixtures/trout/trout-vectors.generated.json`, written by `python -m pipeline.tools.emit.trout_vectors`
  and registered in `pipeline/tools/emit/__init__.py`, so `python -m pipeline.tools.emit --check` covers it.
- `app/fixtures/trout/trout-hand.json`: hand-written contract edge cases that **both** sides must refuse. It is the
  single shared file, like `dfo-rows.json`, never a copy per side.

**Where the vectors come from, so CI can check them.** A full answers build cannot run in CI (AGENTS 39). So the
vectors are **decoded from the committed regs fixture release** (`app/packages/data/dev/regs-release/`, written by
`release/regs_fixture.py` from a built release). `--check` decodes those cells again and compares, with no build
needed. The regs fixture keeps the case waters, because its `--item` list is **read from the same case list**
(`trout_vectors.CASES`). One list, so the two cannot disagree. A builder change reaches the vectors by: side
release → `regs_fixture` → `trout_vectors`. The mini release (`tools/mini_release.py --check`, the real pipeline
on invented waters) runs the builder and J in fast-fail with the flag on, so a broken builder is caught even before a
full rebuild.

**One case:**
```json
{"name": "skeena-sedan-mill-jul15", "water": "Skeena River", "anchor": {"stream": "<stream key>", "m": 123456},
 "day": "07-15", "weekday": "wed", "s": 4, "frame": {"…": "the decoded TroutFrame"},
 "composed": {"lead_today": "Up to 5 today", "lead_on": "Up to 5 on Jul 15",
              "a11y": {"RB": "Rainbow trout. 30–50 cm (12–20 in). Only 1 of these 4. Until Oct 31, then release."}}}
```
`s` pins the app's `segmentIndex`. `composed` pins the two places the app composes: the `{day}` slot, and the
screen-reader label of each row joined in core. Both come from Python's reference functions, so the join order is
Python's too. Anchors follow AGENTS W46 (stream key and metre, or a lake's wbk; never a part number).

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
| Bish Creek (`gnis:3480`) | May 20 | only steelhead closed: a closed group of one, not a closed water |
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
table that is not closed today. The Python test asserts the model refuses each one. The app's test asserts
`reportFailure` and no table drawn. The same file serves both.

**Before the builder exists** (app check 10.6): the first hand-written tables, for layout work, go in a third
section `drafts` and are marked as drafts. They are never the source of a word. When the builder exists, `--check`
replaces them with built cases, and any word that changed appears in G's catalogue diff for the owner.

### 2.9 Bytes, and how ticket A measures them

**What is known.**
- [ran, app check 11b] The trout groups are 110,539 B of regs-6 as stored (26.7%), 128,216 of regs-5 and 122,374
  of regs-3. This was on r-46e4355a7277, before T805, whose lines make them somewhat larger.
- [ran, spec 4.5] First-tap room today is 1,243 B. The app check reads about 88 kB after the salmon lane's raise
  (`91cf5e8fc`, not on a built release) [inferred].
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

**Ticket A's method** (committed as `pipeline/tools/audit/trout_size.py`, run through
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
The output is a dated table in ticket A. It passes when step 4 is within every limit, or when the overage is
stated for the owner's raise.

---

## 3. For the owner

Only one question, Q-D7: **where the "closed to some anglers" notes sit** (youth waters, Skeena and Babine weekends).
- **Recommended: stay in the page's caveat block**, where they show today, above all fish. They close the water to
  some anglers, not trout alone, and showing them in two places would say them twice.
- Or: inside the trout table, above the top line, as in the round 7 mock. The page block then leaves them out.

Everything else is decided from the code or from his earlier answers. The exact sentences (the closed-today form,
"rule changes <date>", the folded entry's short line) reach him in G's catalogue.

---

## 4. Where the two sides could still fight (for the next reviewer)

The app check's section 12 lists every place the trout card decides in TypeScript today (49 entries). The ones this
contract removes, with how they are removed, are 1 to 7. The ones that stay are 8 to 14.

1. **Which fish are trout:** `others.ts:24` `TROUTISH` (with BT, CT_ws, CT_c) and `strip.ts:122` `TROUT_LINE_FISH`
   against `common.trout_char()`. **Removed:** the app partitions by `covers` (Q-D4). It holds while no trout code
   reads `TROUTISH`; a lint test in N checks `trout.ts` imports neither name.
2. **The page's possession lead is picked twice**, by `card.page_possession` (`card.py:410`) and by
   `standing.ts:97` `possessionRules`, with different rules. The table's "differs from the page" is judged against
   Python's choice. **Fix:** Python ships the lead (a small ticket before F), and `standing.ts` reads it and stops
   picking.
3. **The page's possession row repeats per-fish lines:** `standing.ts:133` `possessionOf` draws every non-lead
   possession sentence as an `also` line (T670 K1). After the switch, the trout ones are also on the table's rows.
   **Fix:** Python ships which also-lines the page row keeps (non-trout fish only). The TS `possessionValue` regex
   with its silent null (`standing.ts:117`, app check 12.2 #40) is a W46 item of its own.
4. **Date words from three TS engines** (`plainrow.ts:62-169` `YearWords`, `strip.ts:128` `troutLine`,
   `caveats.ts:225-243` per-moment answers) beside `DateSet`. **Removed for trout:** `troutLine` goes (owner, about
   16:55, "Drop it"). `plainrow.ts` leaves the trout path (`covers`). `caveats.ts` timed (b) must skip covered fish:
   the table says their weekday words (2.5). `strip.ts:137-174`'s 21-day "Closed from …" stays (whole water), and
   it can sit beside the table's `card` line, "Until May 31, then closed to all fishing". Both are true. G's
   catalogue should show the owner that pair once.
5. **TS rewrites Python's sentences:** `tree.ts:111` `dutyWords` ("this river" → "the Skeena River") and `tree.ts:56`
   `subcapLead`. **Removed:** the table's strings are drawn unchanged. N's test checks every drawn string is in the
   vectors' frame, none added or changed.
6. **The steelhead line** (`tail.ts:83` `STEEL_LINE`, TS words over Python's enum). **Removed for trout:** Python
   writes it as a `quiet` line (2.3). It stays in TS only for the old card's other-fish use, if that has any.
7. **The hand-typed version gate:** `versions.ts` and `app_gate` see no section versions. **Fix:** 2.1 (table names
   generated, version pinned by test) and 2.7 (`app_reads` records sections, the gate derives the stamp).
8. **What stays composed in TS, pinned by the vectors:** the `{day}` slot, the screen-reader join, and
   `segmentIndex` (`tap.ts:54`, against `encode.py:1045`). Each vector carries the finished result. A drift fails
   one suite, and the fix goes on both sides in one change.
9. **Build checks that will check nothing:** `answers.py:584-598` (`origin_unsaid`, `across_unsaid`) and
   `card_check.check_tree` / `possession_problems` walk `group.tree`. Once L drops `group` on trout rows, they skip
   every trout card silently (`if not g … continue`, `tree.py:144-146`). **Fix in L:** re-aim them at the table (J
   holds the cross-row and possession proofs), plus a mutation test that each still fires on a planted case. This is
   the "checks seeded from input launder" lesson in another form.
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

---

## 5. Not checked

- I ran no build, no answers build, no release and no test. Every line anchor is read at HEAD `384470bad`, except
  those quoted from the app check, which carry its tags.
- The case table's anchors: the waters named without a `gnis` or `wbk` were not resolved. The emitter resolves them,
  and refuses a name that matches more than one water.
- Whether any angler-only closure names trout or char alone (it would change the Q-D7 recommendation for that
  rule). Ticket A can count it.
- Whether any rows-frame row mixes trout and non-trout members (Q-D4). Ticket A counts it.
- Whether a manifest under `test/` can point at files under `test/release/` (Q-D11).
- Bytes: nothing here is measured beyond the zdict sizes. The section's size is ticket A's.
