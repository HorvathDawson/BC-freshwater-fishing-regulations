# Trout table: build spec (from the Python pipeline to the screen)

2026-11-16, written by an Opus worker at the owner's request of 15:18 ("spec out what would be involved in transitioning
the trout portion of the app to the new table format… implemented clean from python up to front end"). Nothing is
built and no ticket is created by this file. Tickets below are lettered (A, B, C…). The main chat gives them numbers.

Tags: **[ran]** = I ran it, **[read]** = I read the code or file, **[inferred]** = my reasoning, not checked.
Release measured: `r-46e4355a7277` (what `data/release/LATEST` names today). Design read: rounds 7 to 11 in the mockup
notes, the four attack reports (`trout-adversarial-review-2026-11-16.md`), the owner's answers in `OWNER-TODO.md`.

---

## 1. For the owner (one page)

**What changes for someone using the app.** The "Trout and char" part of a water's page becomes the table you picked
at 15:16 (the round 8 look without the outline, round 11's spacing). It has one big line ("Up to 5 today"). Below that
is a shaded box for each limit ("Keep 5 in all, only 1 over 50 cm"). Each fish gets one row with its picture and its size.
A white box on the right holds any tighter limit ("only 1 of these 4"). Fish you must put back are listed under the
box, and so are fish that are closed. Today's rule is in big type, and a coming change is in small type under it
("until Oct 31, then release"). A closure says what it closes and when it opens again. Tapping a fish opens a sheet
with three parts: what applies here today, how to tell the fish, and where the rule comes from. The sheet closes
with a swipe down. Nothing in the law changes: the app says the same law in a clearer way. A few places where the
law is still missing from the data (possession limits, some wild-fish limits) get fixed first.

**What has to be built, in seven steps**
1. Fill the data holes the reviewers found. These are possession limits (Region 7 lake trout and bull trout, Cunningham,
   Tagish, the Bowron park lakes), the wild-fish limit at Lois and Khartoum, and a few odd lines (Shamrock Creek, the
   cross-border lakes). This also fixes **today's** app.
2. Build a tool that lists every answer that changed between two releases, so you see each change before it ships.
   No such tool exists today. That step is done by hand.
3. Teach the data side to work out, for every fish on every water and every day, the next change that matters,
   skipping changes that only make the rule looser.
4. Have the data side write the whole table, word for word, for every water and day range. The app only lays it out.
5. Add a permanent check. For every water and every day, it proves the table says the same law as the rule
   engine, and that every "until …" line is true. If not, the build stops.
6. On the app side: draw the table, the fish sheet, large text, dark mode and the colour-blind shapes, against test data.
   You do not see any of it until step 7.
7. One switch. In a single release and app build, the new table replaces the old card everywhere, and the old code
   is deleted. Older app versions are asked to update.

**What is risky**
- **Download size.** The first tap on a water is right at its size limit today: 1,243 bytes of room (I measured it).
  The new table has to take less room than the old card it replaces. If it doesn't, you will be asked to raise the limit.
- **Saying something false with confidence.** The "until …" lines are the hardest part. The step 5 check exists for this.
- **Other fish.** The old card is also used when you open kokanee, whitefish and other fish. To delete the old code,
  those need the new look too. Otherwise a second copy of the old code stays behind.

**What you decide or review, and when**
- Now: the three questions in section 8 (short choices).
- After step 4: one list of every sentence the table can say (a few hundred lines, read once).
- After step 6: a picture pack of about 45 real waters, on staging, phone size, light and dark.
- Before step 7: the list of changed answers. It should be empty apart from the step 1 fixes you already saw.

**Rough size.** About 12 to 15 worker sessions on the pipeline side and 9 to 12 on the app side. My confidence is
medium-low (it could be 40% more). The date rule and the size limit are where it could grow.

**What could ship first.** Step 1 alone, in today's card. It fixes real gaps (possession, wild limits) in the app people
use now and does not wait for the redesign. Step 2 is useful for every release, not only trout.

---

## 2. Today, end to end

### 2.1 Book → engine (pipeline): reuse as is

| Stage | Where | What it decides | Verdict |
|---|---|---|---|
| Reader / competition | `pipeline/deliver/bundle/read/ladder.py:226` `effective_rules`, `:450` `effective_rules_bound`; steps `:801` in force, `:912` competition, `:1053` closures, `:1184` water release, `:1242` moot size [read, via map] | Which rules speak for one fish, origin, section and day | **Reuse.** This is the oracle the proof compares against |
| Verdicts | `pipeline/deliver/verdicts/build.py:111` `one_key` → `verdicts.sqlite` (never shipped) [read, via map] | The traced reader per rule key × reading × fish × origin | **Reuse** as the proof's independent side |
| Calendar | `pipeline/deliver/calendar.py:242` `segments`, `:214` `moment_segments` [read, via map] | Splits a key's year into segments (and weekday or hour moments) where any answer changes | **Reuse.** The table is keyed by segment |

### 2.2 Engine → `rows` section (pipeline)

| Stage | Where | Writes | Verdict |
|---|---|---|---|
| Section declared | `pipeline/deliver/answers/answers.py:303` `Section("rows", 33, …)` [read] | rows v33 | Becomes 34 at cutover |
| Assemble | `answers/rows/assemble.py:27` `produce` [read]; `rows/evaluate.py:58` `eval_sp`; `rows/card_model.py:97` `build_model`; `rows/conds.py:129` `row_conds`, `:295` `fish_bands`, `:436` `origin_lines`, `:482` `eff_cap`, `:542` `_shared_counts`; `rows/groups.py:23` `total_of`, `:285` `elsewhere_of` [read, via map] | Per fish and origin: `fish` (decided, hatchery, wild, lead); rows with `daily`, `items` (bands, origins, back), `conds`, `everyone`, `real_daily`, `scope`, `parent` | **Reuse** as the table builder's input |
| Card / tree wording | `answers/card.py:706` `card_of` (1,089 lines), `:221` `possession_of`, `:367` `_duties`; `answers/tree.py:360` `tree_of` (912 lines), `:855` `size_rule` [read] | `group`: headline, tiers, kinds, not_kept, worked, possession | **Mostly goes.** The duty words, size rule, possession and "elsewhere" logic are lifted into the table builder. The tree shape is deleted |
| Card gate | `answers/card_check.py` (990 lines) [read, via map] | Re-proves card claims against the engine | **Pattern reused** for the table proof (ticket J). Its tree checks go |
| Year strip | `answers/year.py:91` `stamp_years` [read] | Per-ROW `[state, days]` runs (not per fish; finding 6) | Replaced by the per-fish timeline (ticket E). The strip stays for the page-wide year bar |
| Size words | `answers/common.py:452` `inches`, `:471` `cm_in`, `:482` `band_words` [read, via map] | "30 cm (12 in) or longer" | **Reuse** (one size grammar, RULINGS P37) |
| Encoding | `answers/encode.py:802` `RowsCodec` [read]: interns `decided`, `rows` (no year), `strips`; frame = `[spp, fish, [row ix], steelhead_line, [strip ix]]` | Tables `rows_decided`, `rows_row`, `rows_frame` and the `cell.rows` column in each `regs-{r}.sqlite.gz` (`release/contract.py:18`) [read] | New codec beside it (ticket I) |
| Possession gap | `card.py:236-240` [read]: `possession_of` returns None unless exactly one possession rule with `per_daily` covers every kind | Possession is blank on most rows | Ticket B |

### 2.3 Bundle → screen (app)

| Stage | Where | What it does | Verdict |
|---|---|---|---|
| Versions | `app/packages/core/src/regs/versions.ts:14` `KNOWN_VERSIONS = {ladder:1, rows:33, gear:12, licence:7, display:20, glossary:5}` [read] | The store refuses a mismatch **only for listed sections** (`data/src/regs/store.ts:116-122`) [read] | A new `trout` section is invisible to older apps. That is what makes the build-behind possible |
| Reads | `data/src/regs/reads.ts:81` `cell`, `:298` reads `cell` by **named columns** [read]; `inputs.ts:46` `keyed` | One cell per (answer key, segment) | A new `cell.trout` column is safe for old apps [read] |
| Card model | `core/src/regs/card.ts:131` `hydRow`, `:154` `rowsAt`, `:188` `buildModel` [read, via map] | Wire → `CardModel` | Keep for the other tabs. The trout part goes |
| Page builder | `core/src/regs/page.ts:46-71` `buildRegsPage`; trout part at `keep.ts:122-133` [read] | A row becomes a `GroupVM` if it has `group.headline`, a `PlainRowVM` if `isPlainRow`, else dropped | Replace with `troutOf(...)` |
| TS composers (1,683 lines with standing/others) | `grouphead.ts` (252), `tree.ts` (290), `tail.ts` (95), `plainrow.ts` (217), `keep.ts` (141), `glance.ts` `groupGlance` [read wc] | **Rule logic in TypeScript** (against the standing decision): `tree.ts:37` `sizeOf` (regex over band words), `:236` `liftCounts` (restructures the tree), `:118` `presOf`, `:181` steelhead link by regex; `grouphead.ts:132-139` picks the head tier; `plainrow.ts:70-121` "next year" day maths, `:154` 14-day "only" windows [read, via map] | **Delete** at cutover (ticket S) |
| Other TS rule logic, not trout-only | `standing.ts:97-117` picks the page's possession rule; `strip.ts:128` `troutLine`; `others.ts:33` `splitRows` / `TROUTISH` [read] | Same class of problem | Out of this scope; listed in section 8 |
| Components | `ui-native/src/regs/KeepTab.tsx` (124; GroupCard at `:47` glance fold and `:113`), `GroupCard.tsx` (293; `:235`), `PlainRow.tsx` (145), `KeepBlocks.tsx` (`GlanceFold` ~`:243`), `SheetBody.tsx:71` (GroupCard inside the **Other fish** sheet), `sheet/SourcesView.tsx`, `sheet/FishImage.tsx`, `YearStrip.tsx` [read] | Draws the card | GroupCard and the trout parts of KeepTab and PlainRow go. `Pictures`, SheetHost, SourcesView, FishImage and YearStrip are reused |
| Fish content | `core/src/regs/fish.generated.ts` `FISH_TIPS` `:208`, from `tools/emit-fish.mjs` [read, via map]; thumbs in `apps/mobile/assets/fish/thumbs/` | How to tell it | **Reuse** |
| Glance | Always on: `ui-native/src/shell/WaterModeHost.tsx:89` passes `glance` bare. No flag [read, via map] | The trout fold row | Its words come from the table (`fold`) |

**Other fish use the old card too [ran].** In this release, 3,320 non-trout rows carry a `group.headline` and are
drawn by `GroupCard` inside the Other fish sheet (`SheetBody.tsx:71`). No frame draws a non-trout group in the main
block (4,528 frames checked, `scratchpad/trout-spec/meas4.py`). Deleting GroupCard and the group tree therefore needs
the Other fish sheets moved to the new table too (owner question 1).

### 2.4 Tests that pin today's block
- Pipeline: `pipeline/tests/test_answers_*` (`_r18_tree`, `_r19_tree`, `_r20h` possession, `_year_strip`, `_t763_duty_band`,
  `_t793_origin_on_card`, `_kitimat_steelhead_lift`, `_one_kind_group`, `_keep_answers` …), `test_card_gate_in_chain.py`,
  `test_mini_release.py`, `test_regs_store.py`. The versions pair: `test_answers_tidal.py:106-121` (`app_reader_versions`)
  [read, via map].
- App: core `keep|grouphead|tree|tail|plainrow|strip|standing|others.test.ts`, `t475–t481.test.ts`; ui-native
  `KeepTab.test.tsx`, `QuotaWording.test.tsx` and about 20 tests reading `ui-native/src/regs/fixtures/*.json`; conformance
  `keep.props.parity.test.ts`; `app/fixtures/golden/taps.json`, `page-oracle.json`; the committed fixture release
  `app/packages/data/dev/regs-release/` (written by `pipeline/deliver/release/regs_fixture.py`). There are no picture
  snapshot tests. Pictures come from `tools/store-shots.mjs` and the Maestro test bed [read, via map].

---

## 3. The target, element by element

"Fact" = what the builder needs. "Exists?": **yes** = stored in the bundle today; **derive** = computable at build from
stored facts; **missing** = a data gap (ticket named). Every word below is written by Python. The app only lays it out.

### 3.1 The card

| Element | Precise rule | Fact | Exists? |
|---|---|---|---|
| Kicker | "TROUT AND CHAR", plus "How to tell a hatchery fish ›" when any row of the table names an origin | row origins | yes |
| Notes for some anglers | A note in force today (flag 2) goes above the top line; one not in force today (flag 1) goes directly under it, above the table, never under the table (round 7 item 8). One line each, with "⚠" | the 34 angler-closure rules (Skeena/Babine weekends, youth waters) | yes (the page's caveats today, `caveats.ts`) [inferred: placement by flag needs the rule's in-force test per segment, derive] |
| Top line, plain | "Up to N today". N = the stored headline | `group.headline.n` | yes |
| Top line, "or" form | "Up to 2 today, or 5 if they're rainbow trout" when the headline has a stored second half that starts "or"/"plus" (87 waters) | `headline.also[].says` | yes |
| Top line, three numbers | "Up to 2 today, or 5 if they're rainbow trout and no more than 2 Dolly Varden/bull trout" (Madden-type, 30 waters; owner yes 12:42) | also + the bull trout box | derive |
| Top line, two boxes counted apart | "Up to 6 rainbow trout or steelhead and 4 other trout and char today" (426 answers) | `scope.apart`, `parent` | yes |
| Top line, "plus" | "Up to 2 today, plus 2 Dolly Varden/bull trout" (Kaslo, Keen) | over-region tier | **derive. Gap:** 39 answers / 8 keys have no stored headline (round 2 grammar). Ross Lake: headline 2 against `real_daily.n` 4 needs tracing (ticket F) |
| Top line, second half for a cross-box wild limit | "…today," then under it "only 2 wild trout or char, each under 40 cm (16 in)", in the same words as the "All together" line (Lois, Khartoum) | one wild quota across rows | **missing** (T805 item 1) |
| Top line, no keeping | "Release every one until <date>" / "Closed for trout and char until <date>" / "Nothing to keep today" | timeline | derive (ticket E) |
| Card line (small) | Above the table when **every fish of the card** changes on the same day to the same closure: "Until Oct 31, then closed to all fishing" (whole-water closure) or "until Sep 30, then closed to trout and char, reopens Nov 1" (scope, round 11 4a) | timeline + closure scope + reopen day | derive (E). The scope is the winning closure rule's species set |
| Keep box frame | Look A: one tint block. Strip across the top, rails as inset shadows, bottom edge, a right band with no lines between rows. No outline (owner 15:16) | none (layout) | n/a |
| Strip | "Keep **N** in all" + box sentence on one line where it fits: size ("only 1 over 50 cm (20 in)"), wild ("only 2 wild, each 40 cm (16 in) or less", which must name its fish when not all of them: "no wild rainbow, cutthroat or brown trout over 50 cm (20 in)"), possession for the whole box ("In your possession: only 1 in all, not 1 of each", Bowron). Optional tag "· Lake's rule" | size tier, origin2 cond, possession | size: yes (`tree.py:855`); wild: yes since T793 (`da961f55c` is in HEAD [ran]) but not in release r-46e4355a7277 [read: 4th attack]; possession: **missing** for most (B) |
| Strip date (small, own line) | Only when **every row** of the box has the same next change (round 11 4b). Otherwise each row carries its own. Never "the others:" | timeline | derive (E) |
| Row | Picture (`pic` = fish code), name as the book prints it ("Dolly Varden/bull trout" in full), origin word ("hatchery" / "wild") only where twin rows exist; `sub` ("a rainbow over 50 cm (20 in)"); then bold lines in this order: size (hidden when it is "any size" and the box carries a size sentence), "hatchery only: wild ones go back", "uses the 1 over 50 cm (20 in)", possession line, stop line; then a quiet line; then the row's date (small, last) | items, bands, origins | yes |
| "uses the 1 over 50 cm" | When the fish's smallest keep size is above the box's size mark, or it is a steelhead under a plain 1-over-50 limit | bands, size tier | derive |
| Stop-fishing duty | On every keepable steelhead row with the duty: "after keeping N, stop fishing this <noun> today". The noun comes from the stored duty ("stream", "river", "creek", "slough", "channel"), never "here" | `_duties` (`card.py:367`) | yes |
| Possession on a row | "in your possession: only N" / "only one day's limit"; with a size: "in your possession: only 2 lake trout, only 1 over 50 cm (20 in)". When possession is **below** the day's number (Cunningham), the white box carries "only 2 in your possession" and a quiet line "3 a day, but never more than 2 lake trout with you" | possession per fish, with members, size clause | **missing**: Region 7 (T805.2), size band (T805.3), Tagish size clause (4th attack), Bowron members miss lake trout (2nd attack); B |
| Hatchery-only char | Dodd, Lois, Khartoum (and Horseshoe, Nanton, Windsor, Ireland): char rows say "hatchery only: wild ones go back" (RULINGS C5) | origin on the char rows | **missing / inconsistent**: the three lakes store the char three ways (3rd attack). C5 is "not enforced" until T793 lands in a release (`RULINGS.md:405`) [read]; C |
| Right band, no tighter limit | "up to the N" (N = the box's number), one cell per row (owner 13:48) | box n | derive |
| Right band, own limit | White box "only 3" level with its fish | item band n | yes |
| Right band, shared limit | White box "only 1 of these 4" spanning consecutive rows. Rows are sorted so that each shared box is one piece (round 3). A fish in a shared box that also has its own limit keeps the own limit as a bold line under its name | `real_daily.shared_count`, subcaps | yes |
| Summed tiers | Never drawn. A tier whose n differs from its rule's `take` is the sum of two limits (Kamloops, Adams p3, Williston B) | tier n vs rule take | derive (2nd attack: detectable) |
| White box date | Small, under the limit, when all its fish share the next change | timeline | derive |
| Release / Closed group | Under the keep boxes: no fill, faded picture, one column "↩ Release" / "✕ Closed" spanning the group. Its date is small under the word; a date over 30 characters goes under the fish's name (round 11) | decided per fish | yes |
| "All together" line | Under the table, only for a limit across two boxes (the Lois wild limit; Vedder "no more than 4 a day across Region 2"; Ross's size). In the same words as the top line's second half | cross-box limit | derive. Lois: **missing** (T805.1) |
| Year line | Only when **every** fish of the table is in one state today (all release, or all closed) **and** the water is open: "Trout and char through the year" bar (Elk). Never on a closed day (owner 12:42) | per-fish year | derive (E) |
| Links | "Sources ›" (all rules) and "Fished elsewhere today? ›" | rule refs; `elsewhere` / `worked` | yes |
| Folded row (glance) | Words per top-line form: "5 a day", "2 to 5 a day", "6 rainbow, 4 others", "release" / "closed" with the date small, "none to keep" (round 3) | top line | derive |

### 3.2 Two sizes of text (owner 15:16)
Today's rule is bold ink: the strip sentence, row sizes, "uses…", "hatchery only…", limits, "up to the N", white box text.
A coming change is smaller and regular, in the quiet colour, on its own line: strip date, row date (last line of the row),
white box date, Release column date, card line. Measured in the mock: the quietest line is 5.88:1 in light and 7.96:1
in dark. Rows sit halfway between round 8 and round 9 spacing (round 11). The data marks each text `now` or `next`; the app picks the style.

### 3.3 The date rule (one rule, computed in Python)
For each (answer key, segment, fish, origin):
1. Walk the following segments (wrapping into next year) and compare the fish's **answer**: state (keep / release /
   closed / absent), count, size bands, origin rule, possession.
2. **Skip a change that is only looser.** Every length and origin that is legal today stays legal at the same or a
   higher count, and the state does not tighten (4th attack fault 3; Bella Coola Apr 1).
3. Date the **first change that is tighter, or a change of state**: "until <last day>, then <words>". The words are
   "release", "closed to all fishing", "closed to trout and char, reopens <day>" (when the reopening is inside the
   year), "closed for steelhead", "1 in all", "<size> or longer", "every wild one goes back".
4. A change that comes back within a short gap still gets dated. The shortest gap in the release is 15 days (Denetiah)
   [read: 4th attack]. Add a build warning under 10 days.
5. A one-day segment that is looser but gets skipped (Adams Jul 31, 60 cm for one day) gets one sentence in the fish's
   sheet, generated from the skipped segment (this replaces hand note `NOTE9`).
6. Placement: card line if every fish shares the date and words; else the strip if every row of the box shares it;
   else the white box if all its fish share it; else the row.
This replaces the mock's hand-written `OV9`, `BOX9`, `NOTE9` (`work/r9add.js:65-77`), `NOUN9` (`:7`) and `CLOSE11`
(`work/r11add.js:7`) [read].

### 3.4 The fish sheet
| Part | Rule | Fact | Exists? |
|---|---|---|---|
| Header | Picture, full name, origin word, today's answer for this fish: "Keep · only 1 · counts toward the 5" / "↩ Put it back · until Jan 31" / "✕ Closed · do not fish for it". Closes by swipe down (over 90 px), ✕, tap outside, Escape. No sideways moves (owner 12:42) | row | derive |
| Here, today | Ordered lines: size; hatchery / wild; "Counts toward the N in all, together with …"; "Only K of these M: this fish and …"; "This fish: only N"; the box's size limit, plus "A kept one is always over 50 cm (20 in), so it is that one" where true; possession ("Only 1 in your possession, not two days' worth. Fish at home don't count."; the last sentence is `POSSESSION_HOME`, `display/rule.py:170` [read, via map]); the stop duty in bold ("Once you have kept 1 steelhead here, stop fishing this river for the rest of the day: the whole river, not only this stretch"); "Licence: 10 a year · record each right away · Steelhead stamp"; the coming change as a sentence ("From Nov 1 every wild one goes back"); the skipped one-day note | as the card | as the card |
| How to tell it | App pictures (both for Dolly Varden/bull trout), `FISH_TIPS`, the clipped-fin drawing and the "Hatchery or wild?" sheet link on hatchery rows. "Often mistaken for" dropped (owner) | fish manifest | yes (app) |
| Sources | This fish's rules first (up to 4: title, the book's words, "Rule book p.N ↗"), then "All sources ›" | rule refs → the water file's `rule(rix)` table (`contract.py:131,171`) | yes |

### 3.5 Sources sheet, accessibility, theme
- **Sources ›** under the table opens today's every-rule sheet (`sheets/sources.ts:51` `srcSheet`, `SourcesView.tsx`), fed by the table's rule refs.
- **200% text:** the strip, rails and bottom edge stay. The right band folds away: each limit becomes a bold line under
  its fish ("only 1 of these 4"), with a left rail joining a shared limit's rows (round 8 A). No sideways scroll at
  360 px.
- **Dark mode:** tokens only (`--frame`, `--wash`, `--sub`). Contrast as in 3.2.
- **Colour-blind ("shapes only"):** state is carried by words and marks ("↩ Release", "✕ Closed"), by the tint block
  against no fill, and by the white boxes. Never by hue alone.
- **Screen reader:** each row reads as one label, joined from its Python texts in order (name, origin, size, limits,
  date). Buttons use ARIA props (AGENTS 34). Press targets are at least 44 px.

---

## 4. The data contract

### 4.1 Shape: a new answers section `trout`, version 1
A separate section, not a new slot in `rows`, so that it can ship inside releases before any app reads it. The store
checks only the sections it knows (`store.ts:116`), and the reads name their cell columns (`reads.ts:298`) [read].

```
regs-{r}.sqlite.gz:
  cell.trout     INTEGER NULL      -- trout frame id for (akey, segment); NULL = no trout block (tidal, no rows)
  trout_table    (id, j)           -- interned table BODY: same for every segment that reads the same today
  trout_dates    (id, j)           -- interned DATE SET: the coming-change words for one segment
  trout_frame    (id, j)           -- [table id, dates id]
```
Days are encoded the way the bundle already does it: one cell per (answer key, segment), where the segments split the
year wherever any answer changes (`calendar.segments`), and the app picks the segment with `segmentIndex` (selection
only). Bodies are interned, and the date words are a separate interned table. This is the trick `rows` 30 used for
strips: holding the strip inside the row made 3,794 rows out of 2,512 and broke the size gate (`encode.py:806`) [read].

```ts
type TroutTable = {
  top: { big: string; second?: string };          // "Up to 5 today" / Lois's wild line
  fold: string;                                   // glance words
  hatchLink: boolean;
  notes: { text: string; today: boolean; src: number[] }[];
  boxes: Box[];                                   // keep boxes first, then release, then closed
  together?: { text: string; src: number[] };
  year?: [state: "keep"|"release"|"closed", days: number][];   // only the all-one-state case
  elsewhere: string[];                            // "Fished elsewhere today?" lines (from group.worked today)
  sheets: Record<string, FishSheet>;              // key "LT" or "ST/hatchery"
};
type Box = { kind: "keep"|"release"|"closed"; n?: number; strip?: string; tag?: string;
             rows: Row[]; band: Band[] };
type Row = { key: string; fish: string; origin?: "hatchery"|"wild"; name: string; sub?: string;
             lines: { t: string; w: "now"|"quiet" }[]; src: number[] };
type Band = { from: number; to: number; text: string; kind: "upto"|"own"|"shared"|"possession" };
type DateSet = { card?: string; box?: Record<number, string>; band?: Record<string, string>;
                 row?: Record<string, string>; sheet?: Record<string, string[]> };   // all "next" texts
type FishSheet = { head: string; here: { t: string; bold?: boolean }[]; src: number[] };
```
`src` holds rule indexes into the water file's `rule` table. The numbers in the examples below are placeholders.

### 4.2 Example: Skeena River, Sedan Creek → Mill Creek (`gnis:2936` part 12), Wed Jul 15
```json
{"table": {
  "top": {"big": "Up to 5 today"}, "fold": "5 a day", "hatchLink": true,
  "notes": [{"text": "Saturdays and Sundays: closed to some visiting anglers", "today": false, "src": [71]}],
  "boxes": [
   {"kind": "keep", "n": 5, "strip": "only 1 over 50 cm (20 in)",
    "rows": [
     {"key": "EB", "fish": "EB", "name": "Brook trout", "lines": [{"t": "30 cm (12 in) or longer", "w": "now"}], "src": [40]},
     {"key": "LT", "fish": "LT", "name": "Lake trout", "lines": [{"t": "30 cm (12 in) or longer", "w": "now"}], "src": [40, 44]},
     {"key": "RB", "fish": "RB", "name": "Rainbow trout", "lines": [{"t": "30–50 cm (12–20 in)", "w": "now"}], "src": [40, 45]},
     {"key": "ST/hatchery", "fish": "ST", "origin": "hatchery", "name": "Steelhead", "sub": "a rainbow over 50 cm (20 in)",
      "lines": [{"t": "uses the 1 over 50 cm (20 in)", "w": "now"},
                {"t": "after keeping 1, stop fishing this river today", "w": "now"}], "src": [45, 9]},
     {"key": "GB", "fish": "GB", "name": "Brown trout", "lines": [{"t": "30 cm (12 in) or longer", "w": "now"}], "src": [45]},
     {"key": "CT", "fish": "CT", "name": "Cutthroat trout", "lines": [{"t": "30 cm (12 in) or longer", "w": "now"}], "src": [45]}],
    "band": [{"from": 0, "to": 0, "text": "up to the 5", "kind": "upto"},
             {"from": 1, "to": 1, "text": "only 3", "kind": "own"},
             {"from": 2, "to": 5, "text": "only 1 of these 4", "kind": "shared"}]},
   {"kind": "release", "rows": [
     {"key": "ST/wild", "fish": "ST", "origin": "wild", "name": "Steelhead", "lines": [], "src": [9]},
     {"key": "DV", "fish": "DV", "name": "Dolly Varden/bull trout", "lines": [], "src": [41]}], "band": []}],
  "elsewhere": ["…"], "sheets": {"ST/hatchery": {"head": "Keep · only 1 of these 4 · counts toward the 5",
    "here": [{"t": "Once you have kept 1 steelhead here, stop fishing this river for the rest of the day: the whole river, not only this stretch.", "bold": true},
             {"t": "Licence: 10 a year · record each right away · Steelhead stamp"}], "src": [9, 45]}}},
 "dates": {"row": {"0:1": "until Sep 14, then release"}, "band": {"0:2": "until Oct 31, then release"},
           "sheet": {"LT": ["From Sep 15 every one goes back."]}}}
```
(Rows, limits and dates are as the round 11 picture `shots/r11/skeena.png` draws them from the release [read].)

### 4.3 Example: Lois Lake (`wbk:329197058`), Jul 15
```json
{"table": {
  "top": {"big": "Up to 6 rainbow trout or steelhead and 4 other trout and char today",
          "second": "only 2 wild trout or char, each under 40 cm (16 in)"},
  "fold": "6 rainbow, 4 others", "hatchLink": true, "notes": [],
  "boxes": [
   {"kind": "keep", "n": 6, "tag": "Lake's rule", "rows": [
     {"key": "RB", "fish": "RB", "name": "Rainbow trout", "lines": [{"t": "any size", "w": "now"}], "src": [3]},
     {"key": "ST/hatchery", "fish": "ST", "origin": "hatchery", "name": "Steelhead", "lines": [{"t": "any size", "w": "now"}], "src": [3]}],
    "band": []},
   {"kind": "keep", "n": 4, "strip": "only 1 over 50 cm (20 in)", "rows": [
     {"key": "EB", "fish": "EB", "name": "Brook trout", "lines": [], "src": [1]},
     {"key": "GB", "fish": "GB", "name": "Brown trout", "lines": [], "src": [1]},
     {"key": "CT", "fish": "CT", "name": "Cutthroat trout", "lines": [], "src": [1]},
     {"key": "DV", "fish": "DV", "name": "Dolly Varden/bull trout", "lines": [
        {"t": "hatchery only: wild ones go back", "w": "now"}, {"t": "60 cm (24 in) or longer", "w": "now"},
        {"t": "uses the 1 over 50 cm (20 in)", "w": "now"}], "src": [1, 2]},
     {"key": "LT", "fish": "LT", "name": "Lake trout", "lines": ["…same three lines…"], "src": [1, 2]}],
    "band": [{"from": 0, "to": 2, "text": "up to the 4", "kind": "upto"},
             {"from": 3, "to": 4, "text": "only 1 of these 2", "kind": "shared"}]}],
  "together": {"text": "only 2 wild trout or char, each under 40 cm (16 in)", "src": [3]}},
 "dates": {}}
```
Note: "up to the 4" is one cell per row in the drawing. The band entry with `from`–`to` stands for three equal cells.
The app draws one cell per row index. **Needs T805.1** (one wild quota across both rows) and **C5** on the char rows
before the build can emit this; today the release stores neither [read: 4th attack].

### 4.4 Example: a closure case, Fraser River part 11 (`gnis:39325` part 11, Region 3), Tue Sep 15
```json
{"table": {"top": {"big": "Up to 4 today"}, "fold": "4 a day", "hatchLink": false, "notes": [],
  "boxes": [
   {"kind": "keep", "n": 4, "strip": "only 1 over 50 cm (20 in)", "rows": [
     {"key": "EB", "fish": "EB", "name": "Brook trout", "lines": [], "src": [5]},
     {"key": "GB", "fish": "GB", "name": "Brown trout", "lines": [], "src": [5]},
     {"key": "CT", "fish": "CT", "name": "Cutthroat trout", "lines": [], "src": [5]},
     {"key": "RB", "fish": "RB", "name": "Rainbow trout", "lines": [{"t": "50 cm (20 in) or less", "w": "now"}], "src": [5]},
     {"key": "LT", "fish": "LT", "name": "Lake trout", "lines": [{"t": "60 cm (24 in) or longer", "w": "now"}], "src": [6, 7]}],
    "band": [{"from": 0, "to": 3, "text": "up to the 4", "kind": "upto"}, {"from": 4, "to": 4, "text": "only 1", "kind": "own"}]},
   {"kind": "release", "rows": [{"key": "DV", "fish": "DV", "name": "Dolly Varden/bull trout", "lines": [], "src": [8]}], "band": []},
   {"kind": "closed", "rows": [{"key": "ST", "fish": "ST", "name": "Steelhead", "sub": "a rainbow over 50 cm (20 in)", "lines": [], "src": [2]}], "band": []}]},
 "dates": {"box": {"0": "until Sep 30, then closed to trout and char, reopens Nov 1"},
           "row": {"1:0": "until Sep 30, then closed to trout and char, reopens Nov 1"},
           "sheet": {"LT": ["No fishing for trout and char Oct 1–31 (whitefish and burbot stay open). From Nov 1 lake trout may be fished again, but every one goes back until Jan 31."]}}}
```
The scope "trout and char" comes from the winning closure rule (`fraser_river.r1`, "No fishing for trout and char,
Oct 1–31") [read: 4th attack and the mock's sources]. Whitefish and burbot staying open is why the words are not
"closed to all fishing". The mock's Fraser card was drawn on the round 11 numbering (part 11). The proof (J) must find the
stretch by stream key and metre, never by a typed part number (AGENTS W46 test anchors).

### 4.5 Size: measured and estimated
- **Room today [ran]** (`release.build.web_moments` on r-46e4355a7277): `first_tap` 4,198,757 of 4,200,000 (**1,243 B
  of room**, not the 88 kB in the brief); `first_tap_low` 40,618; `cold_load` 307,463; `first_tap_unnamed` 181,994.
  `regs-{region}` is priced as the largest region file, regs-6 (413,612 B gz; its own gate is 1.5 MB).
- **What the old card costs [ran]** (gunzip in memory, inflate each blob with water-shared's zdict, gzip -9 of the JSON
  joined by newlines; `scratchpad/trout-spec/meas.py`): the `group` tree inside `rows_row`, in gz bytes per region:
  R1 14,789 · R2 27,460 · R3 69,366 · R4 34,607 · R5 74,436 · **R6 54,739** · R7a 54,489 · R7b 22,959 · R8 21,308.
  That is about 75% of every `rows_row` (R6: 79,798 gz in all).
- **How many distinct trout answers [ran]** (`meas2.py`): distinct trout row sets over frames per region are 94 (R8) to
  425 (R5), 251 in R6. With segments, 108 to 489. Today's group JSON for those R6 sets is 1.62 MB, which is about 6.4 kB per set.
- **Estimate for the new section [inferred]:** the table carries the words the group tree carries, minus the tree's
  repeats (headline, tiers, kinds, worked examples all restate the same limits). A synthetic structure-only table
  interns to about 1 kB gz per region (`meas3.py`), which is a lower bound and too lossy to trust. The upper bound is
  today's group tree. Best guess: **15 to 40 kB gz per region** for tables, dates and sheets, against 55 kB removed from
  R6 at cutover. Net R6: about −40 to −15 kB. **Ticket A measures this with the real encoder before anything else is built.**
- **Consequence:** a promoted release can **never** carry both the old group tree and the new section. The build-behind
  ships the new section only in side releases (fixture, staging test channel). The cutover release carries the section
  and drops `group` in the same release.

### 4.6 What runs where
Python, at build: every number, limit, sort order, date, scope, sentence, the a11y-relevant texts and the fold words.
App: picks the cell for today (`segmentIndex`, as today), maps texts to styles (`now` / `next` / `quiet`), lays out
the bands (one cell per row index), the 200% fold, and sheet gestures. **No TypeScript compares, sums, filters or picks
a rule.** One open tension: AGENTS 33 says sentences are composed in `core/` where a test can pin them. The standing
decision (rules resolved in Python) and today's practice (the group tree already ships Python sentences) win. Python
pins every whole sentence in pytest, and the app pins that the screen shows those strings unchanged.

### 4.7 Versions
- Build-behind: `trout` section version 1 in `answers.py` (beside `:303`). The app does **not** list it in
  `KNOWN_VERSIONS` until ticket M; `test_answers_tidal.py` `app_reader_versions` is extended to the new key once the app
  lists it.
- Cutover: `rows` 33 → 34 (drop `group`, and any field only the old block read, e.g. `real_daily`, `elsewhere`; ticket L
  lists them by grep of the app's readers). The app's `KNOWN_VERSIONS` gets `rows: 34, trout: 1`. Older apps refuse rows 34
  (`store.ts:116`) [read], so the cutover release is published with a `minAppVersion` stamp (`release/publish.py:84`,
  T251) [read]: older apps keep their data and ask for an update.

---

## 5. Pipeline work (main chat)

| Ticket | Goal | Files | Tests (with mutation-style) | Depends on |
|---|---|---|---|---|
| **A. Size spike and contract** | Prototype `TroutCodec` and a crude table for one region (R6) and R5; measure gz against the `group` tree; settle the section layout in 4.1; write the format spec text into `encode.py`'s spec | `answers/encode.py`, scratch | the measurement script committed under `pipeline/tools/audit/` | none |
| **B. Possession on the fish row** | Possession per fish (and per box) from every possession rule: Region 7 "one day's limit" (`possession_exceptions.r1/r2`, p.58/p.64), with a size clause (Cunningham p.59; Tagish p.53 size missing from `tagish_lake.r2`), below the daily number (Cunningham, Tchentlo, Tsayta, Indata, Nakinilerak), Bowron park members to include lake trout, and the 4th attack's Bowron artifact (possession only on lake-trout-release days: Istsi, Wolverine p1, Sootheran p1) | `rows/evaluate.py:243-268`, `card.py:221-253` (or the new module), the corpus for Tagish (A2 backup rule) | per case a whole-sentence test; mutation: drop the size clause → the check (J) fails; a possession larger than daily on a row → the build refuses | none. **Changes shipped answers** (about 14,200 R7 entries gain a line): owner list via K |
| **C. One wild limit across rows; char origin per C5** | Store Lois/Khartoum's wild quota once across both rows (T805.1); make Dodd, Lois, Khartoum, Horseshoe, Nanton, Windsor, Ireland store the char the same way (C5); confirm the T793 rules reach a built release | `rows/card_model.py:60` `_origin2_rule`, `rows/conds.py` | a cross-row wild limit counted once (mutation: split it again → fails); the seven lakes give one answer | T793 merged and delivered. **Changes shipped answers** |
| **D. Small data lines** | Shamrock Creek's "steelhead 20–30 cm" (book p.24: trout/char 20–30 cm, Jun 16–Jan 31); the cross-border quota (Atlin p.50, Bennett, Tagish, Teslin, Laidlaw, Swift, Rancheria) as a stated note in `notes`; Echoes Lakes "in the aggregate from both lakes" as `together`; trace Ross Lake's headline 2 against `real_daily` 4; the 8 rule sets with no headline; check François Lake's outlet char rule | corpus (A2), `rows/groups.py` | one test per water, by stream key / water through `under_test` | none. Shamrock changes an answer |
| **E. Per-fish timeline and the date rule** | For every (akey, segment, fish, origin): the answer and the next dated change per 3.3 (skip looser-only, scope, reopen day, one-day notes, weekday moments, year wrap) | new `answers/trout/timeline.py`; reads rows frames over all segments and the closure rule's species | over the release: never "until X, then release" when keepable the next day; never "until X" when not keepable after; no tighter change skipped; mutations: shift a date by a day, drop the skip rule, mark a closure whole-water → each fails | none (reads `produce` output) |
| **F. Table builder** | Boxes, rows, sort, bands ("up to the N", own, shared, possession), summed-tier detection, top-line forms, strip sentences, row lines, release/closed groups, notes with today flags, `together`, `year`, `fold`, rule refs; reuses `band_words`, `_duties`, `size_rule` | new `answers/trout/table.py`; lifts parts of `card.py`, `tree.py` | the 43 mock waters as named cases (stream key + metre); every top-line form; summed tier never drawn (mutation: draw it → J fails) | B, C, E |
| **G. Words** | Every sentence in one module with whole-sentence tests; emit a catalogue of every distinct sentence with counts for the owner | `answers/trout/words.py`; `pipeline/tools/audit/trout_words.py` | whole strings only (AGENTS 33) | F |
| **H. Sheets and sources** | Per fish: header answer, "Here, today" lines, licence line, the coming-change sentence, rule refs ordered (this fish's first, up to 4); move the "Fished elsewhere" lines (`groups.py:285`) into `elsewhere` | `answers/trout/sheets.py` | whole strings; every rule ref resolves to a rule (mutation: a dangling ref → the build stops) | F |
| **I. Section and codec** | `Section("trout", 1, …)` behind `--trout-table` (off by default); `TroutCodec` (tables, date sets, frames interned); `cell.trout`; contract tables; pydantic models in `answers/model/` + generated TS (`model/emit.py`, `answers.generated.ts`); `regs_fixture.py` and the mini release built with the flag | `answers.py`, `encode.py`, `release/contract.py`, `answers/model/`, `release/regs_fixture.py`, `tools/mini_release.py` | round-trip encode/decode; frame refs in range; `test_mini_release.py`; size gates in `test_release.py` measured with the flag on | A, F, H |
| **J. The proof (permanent build gate)** | For every (akey, segment, moment): derive the law **from the table alone** (a separate decoder: per fish, origin and length bucket the count allowed, closed or release, possession) and compare it with the engine's (`rows.fish` decided + `verdicts.sqlite`), not with the table builder's inputs. Check every date claim against the following segments (as the reviewer's `check.py`). Zero unexplained differences, or the answers build stops (like `card_check.py`) | `answers/trout/check.py`; hooked where `card_check` runs | mutation tests: change one number, one size edge, one origin, one date, one scope in a built table → each must fail; a test that the decoder does not import the builder | E, F, I |
| **K. Changed-answers list** | Diff two releases (or two builds) per water and day range in plain words, grouped by kind of change, with counts and water names; output one page for the owner | `pipeline/tools/audit/answers_diff.py` | a built pair with a known planted difference; an identical pair → empty | none. **Build first:** B, C and D need it |
| **L. Cutover, pipeline half** | Flag removed (always on); `rows` 34 without `group` and the fields only the old block read; delete the tree's wording paths, `card_check`'s tree checks and their tests; `minAppVersion` stamp on the release | `answers.py`, `card.py`, `tree.py`, `card_check.py`, tests | the full answers suite; size gates; `app_reader_versions` | J, the app's S ready to land together |

Order: **K → (B, C, D in any order, each with its own owner list) → A → E → F → G, H → I → J → L.** A can start at once
in parallel with K. B, C and D change today's app and ship on their own through the normal release (no flag).

---

## 6. App work (app chat)

| Ticket | Goal | Files | Tests | Depends on |
|---|---|---|---|---|
| **M. Read the section** | `trout: 1` in `KNOWN_VERSIONS` (still unused by the page); `cell.trout` read; `trout_table/dates/frame` blobs through the existing codec and zdict; generated types | `core/src/regs/versions.ts`, `data/src/regs/reads.ts`, `inputs.ts`, `store.ts` | `store.test.ts` on the committed fixture release (re-cut with the flag) | I |
| **N. View model** | `troutOf(inputs)` → `TroutVM`: a pure map from section to VM, styles from `now/next/quiet`, a11y labels joined in order; no comparisons or sums (a lint-style test: the module imports no `rows`/`group` types) | new `core/src/regs/trout.ts`, `model.ts` | whole-string tests per mock water from the fixture release; the VM's strings equal the section's strings (none added or changed) | M |
| **O. The card** | `TroutCard`: kicker, notes, top line, card line, keep box (strip, rows, right band with white boxes and "up to the N" cells), release/closed group, All together, year line, links; look A tokens; two text sizes; round 11 spacing; dark; 200% fold (bands → bold lines with a rail); shapes-only; the glance fold row's words | new `ui-native/src/regs/TroutCard.tsx` (+ parts), `KeepTab.tsx` behind a dev-only switch | render tests on fixtures; no sideways scroll at 360/390 px, 100/200%; every target ≥ 44 px; ARIA props | N |
| **P. The fish sheet** | Header, Here today, How to tell it (`FISH_TIPS`, both DV pictures, clipped-fin drawing, "Hatchery or wild?" link), Sources (this fish first); swipe down over 90 px, ✕, tap outside, Escape; no sideways moves; reuse `SheetHost`, `FishImage`, `SourcesView` | `core/src/regs/sheets/trout.ts`, `ui-native/src/regs/sheet/TroutSheet.tsx` | open every fish of every fixture water; gestures on web and native (file pairs per AGENTS 34a) | N |
| **Q. Other fish on the same table** (if owner question 1 is yes) | The Other fish sheet (`SheetBody.tsx:71`) draws a one-box table from the same section shape for non-trout rows | `sheets/other.ts`, `SheetBody.tsx` | as O | O, and the pipeline emitting non-trout tables (part of F) |
| **R. Picture pack and the test bed** | The 43 mock waters plus Fraser p11 at 390/360 px, light/dark, 100/200%, from a side release on the staging test channel, for the owner; a Maestro walk that opens the card and a sheet on a fresh install | `tools/store-shots.mjs`, `tools/testbed` | the walk on a fresh install (AGENTS W46 test bed) | O, P |
| **S. Cutover and deletion, app half** | The page reads only `trout`; `rows: 34`; delete `grouphead.ts`, `tree.ts`, `tail.ts` (the parts not moved), the group branch of `keep.ts:122-133`, `glance.ts` `groupGlance`, `GroupCard.tsx` (keep `Pictures`, moved), the trout use of `plainrow.ts`; their tests and fixtures; re-cut `taps.json`, page oracle and VM fixtures (`node app/tools/regs-recut/sync.mjs`); update `keep.props.parity.test.ts`; knip clean; `app/MAP.md` | as listed | `pnpm check`; conformance; the full tier once | L ready, owner sign-off |

The dev-only switch in O exists only between O and S. It is removed in S, so no second path is left behind.

---

## 7. Proof and rollout

1. **Same law, every water and day.** Ticket J is a build gate, not a one-off audit: every release proves that the
   table's law equals the engine's for every answer key, segment and weekday or hour moment. Segments partition the
   year exactly, so this covers every day. It also proves every date claim. It never compares the table against the data
   it was built from (the "checks seeded from input launder" lesson), and its mutation tests prove it can fail.
2. **Old against new.** Before the switch, K diffs the last release before cutover against the cutover candidate at the
   law level (expected: no change), and also prints old and new card text for a sample of waters for reading. Any law
   difference is a bug or one of B, C, D's already-approved changes.
3. **What the owner sees, in order:** B/C/D changed-answer lists (each before its own staging release); the sentence
   catalogue (G); the picture pack (R); the cutover changed-answers list (expected empty). Each goes into OWNER-TODO
   and the owner to-do page with pictures (owner rules).
4. **Landing order, branch never red:** pipeline tickets land with the flag off (releases unchanged; tests build the mini
   release and the fixture with the flag on). App M to R land against the committed fixture release, which carries the
   section; old app paths are untouched and older releases still load. **Cutover = one merge carrying L and S
   together** (a format-pair test fails if either lands alone), then fast-fail, a staging release with `minAppVersion`,
   the APK through the benches, then Play internal.
5. **Deleted after:** the group tree and its wording paths (`tree.py`, the card-only parts of `card.py`,
   `card_check` tree checks), the TS composers and `GroupCard`, their tests and fixtures, the flag, and the dev switch.
   Then `pipeline/docs/MAP.md`, `app/MAP.md` and AGENTS rule 53's note on the guide get updated.

---

## 8. Risks and open questions

### Risks, ranked
1. **Size [ran].** First tap has 1,243 B of room. A promoted release cannot carry the old tree and the new section
   together, so the owner never sees the new card on a normal staging release until cutover. Mitigation: ticket A first,
   side releases on the test channel, net-negative cutover; if it is not negative, owner question 3.
2. **False date lines.** The looser/tighter order across counts, sizes, origins and possession is the hardest logic. A
   mistake reads as a confident false "until". Mitigation: J's date check over every segment, mutation tests, and the
   reviewer's simulation (1.9 million fish-days, never wrong) as a target.
3. **Clean deletion depends on other fish.** GroupCard also draws 3,320 non-trout rows in the Other fish sheet [ran]. A
   trout-only build leaves the old tree and card in place (a dual path). Owner question 1.
4. **Data fixes change many answers.** Region 7 possession alone touches about 14,200 water entries. The owner's list
   must be grouped, not raw.
5. **The cutover forces an app update** (rows 34). Fine on Play internal; iOS is benched, so nothing to coordinate there now.
6. **TS rule logic outside the block stays:** `standing.ts:97-117` (page possession pick), `strip.ts:128` `troutLine`,
   `others.ts:33` `splitRows`. These are not this spec's work. Propose a follow-up ticket so the "no rule logic in TS" decision holds app-wide.
7. **Card height:** 13 of 43 mock cards are taller than one 640 px screen (round 11: Vedder 833, Lois 808). This is
   accepted design, not a build risk.
8. **Strip against row contradiction** (4th attack fault 2). It was fixed in design by "strip only when every row shares".
   How often it fires is unmeasured. J measures it.

### Questions for the owner (choices)
1. **Other fish:** when you open kokanee, whitefish and other fish, show them in the same new table look? (a) Yes, same
   look, so all the old code goes (**recommended**). (b) Keep today's card for other fish for now. This leaves the old code in place.
2. **Possession line on the page:** the page-wide line ("Possession: twice the daily quota") stays as it is, and the
   trout table speaks only where trout possession differs (**recommended**). Or repeat the possession line inside the trout table.
3. **Only if ticket A shows the cutover does not fit:** raise the first-tap limit by up to the measured amount and shrink
   later, as you ruled for search (O5) (**recommended**). Or hold the switch until it fits.

### Questions for the chats
| Question | Who | Recommended |
|---|---|---|
| New section `trout` (4.1) or a sixth slot in the rows frame? | main | New section (build-behind needs it) |
| Can the owner preview on a side release in the staging test channel (`test/`) without promoting it? | main | Yes; it is staging and allowed |
| Do a11y labels come from Python or get joined in core? | app + main | Joined in core from Python texts, order only |
| Does the "Fished elsewhere" sheet move into `trout.elsewhere` or stay on `rows`? | main | Move (it is the trout block's) |
| Do non-trout tables come from the same builder (only if Q1 is a)? | main | Yes, one builder for any keep row |
| Is the picture pack a gate or a review? | app | A review (no pixel gate exists today) |

---

## 9. Not looked at
- I did not build or run any of this. I ran no answers build, release, app build or test suite.
- I did not open the mock in a browser. I read its pictures (`shots/r11/skeena.png`, `lois.png`, `fraser.png`), notes and data.
- I did not run the reviewer's `alldays.json` scripts, and I did not re-check its counts.
- The rule indexes in section 4's examples are placeholders, and the example texts come from the round 11 mock and the
  reviewer, not from a Python run.
- I did not re-verify the line anchors that came from my read-only code maps (tagged "[read, via map]"). I checked these
  myself: `versions.ts:14`, `answers.py:303`, `keep.ts:120-135`, `page.ts:46-50`, `store.ts:100-122`, `encode.py:800-845`,
  `encode.py:1045`, `card.py:221-242`, `card.py:367`, `card.py:706`, `tree.py:360`, `year.py:91`, `assemble.py:27`,
  `contract.py:16-21,244-295`, `others.ts:20-60`, `SheetBody.tsx:71`.
- I did not read where the angler-only notes are composed (`caveats.ts`) or how their today flag would be derived.
- I did not check whether T793's merged code (`da961f55c`, in HEAD [ran]) is in any built release. The ticket ledger
  still says "not merged".
- I did not check the Fraser stretch's part number in the next release's numbering.
- The 88 kB first-tap room in the brief does not match what I measured (1,243 B on r-46e4355a7277). It may refer to
  another release or another moment (`first_tap_low` has 40,618 B).
- Size of the new section: **estimated, not measured** (4.5). Ticket A exists to measure it.
