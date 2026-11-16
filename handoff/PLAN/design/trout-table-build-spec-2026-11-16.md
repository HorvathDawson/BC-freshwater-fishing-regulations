# Trout table: build spec (from the Python pipeline to the screen)

2026-11-16, written by an Opus worker at the owner's request of 15:18 ("spec out what would be involved in transitioning
the trout portion of the app to the new table format… implemented clean from python up to front end"). Nothing is
built and no ticket is created by this file. Tickets below are lettered (A, B, C…). The main chat gives them numbers.

Tags: **[ran]** = I ran it, **[read]** = I read the code or file, **[inferred]** = my reasoning, not checked.
Release measured: `r-46e4355a7277` (what `data/release/LATEST` names today). Design read: rounds 7 to 11 in the mockup
notes, the four attack reports (`trout-adversarial-review-2026-11-16.md`), the owner's answers in `OWNER-TODO.md`.
Revised 16:40 the same day (Opus worker) to take in the owner's answers of 15:36 to 16:37; the accepted design is the
list at the end of `app-integration/tickets/T810-…md`. Where this file and that list disagree, the list wins.

## Changes 2026-11-16 16:40

Each change, with the owner's words it comes from (verbatim, from `OWNER-TODO.md`):

1. **Trout and char only.** The new table opens under the existing "Trout and char" entry, which stays folded to its
   short line ("Up to 2", "Up to 4") with the expand arrow. Kokanee, whitefish and the other fish entries are not
   touched and keep the old card code. Ticket Q is dropped. Tickets L and S delete only the trout path, not the old
   card. (15:47: "we should just have the trout table under the trout entry like it currently is with the better text
   like 2 or 4 with expand arrow etc."; 15:42: "we want to migrate the new trout style into the current app not the
   other way around.")
2. **No "reopens" on a coming closure.** A coming closure says what closes and when: "until Sep 30, then closed to trout
   and char". The reopening date is shown only while the water or fish is closed today. (15:36: "yes but drop reopen
   unless it is currently closed then reopen date matters then")
3. **A change that only loosens a rule** is no longer skipped. It reads "rule changes <date>", with no details.
   (15:36: "could just mention a rule change happens on that date not specifics")
4. **Wild and hatchery: three cases only.** Same rule for both: no mention. Only hatchery may be kept: ONE row per fish
   with a filled "HATCHERY ONLY" label beside the name, and no wild row under Release. When that covers every fish of a
   keep box, the box's top line carries the same filled label once. Wild and hatchery rows apart only where wild fish
   have a quota or size of their own. The row model (one row per fish and origin) changes to match. (15:48: "we should
   jsut say hatchery only unless specifically wild has a seperate quota to wild like 1 wild and 2 hatchery or
   something.... otherwise it is hatrchery only or unspecified (the same for both no mentiuon needed)"; 16:00: "A filled
   label then if it applies to all fish in category use same fill for where that is located."; 15:56, Lois and
   Khartoum steelhead: "Hatchery only (Recommended)")
5. **Adams River Dolly Varden / bull trout:** the stricter 80 cm on the row; the one-day rule goes in the fish's sheet.
   (15:36: "Yes, 80 cm on the row (Recommended)")
6. **Possession:** the page keeps its own line. A fish row carries possession only where that fish's differs. This is
   now built in today's card by T805 part B; the table builder lifts it. (15:42: "Keep the page line (Recommended)";
   16:37 part B: "Go (Recommended)")
7. **Size:** if the switch does not fit the first-tap limit, raise the limit and shrink later. Owner question 3 is
   answered. (15:42: "Raise the limit, shrink later (Recommended)")
8. **Two text sizes confirmed.** Today's rule is in bigger text, and the coming change is smaller underneath. (Accepted
   design, item 2; 15:36: "it looks good")
9. **Fraser and the 30 cm minimum: not a fault.** The 15:36 card was the Fraser in Region 3, which has no 30 cm minimum
   and no wild/hatchery split. Region 2's Fraser at Mission shows both from the data (round 12, 15:47). It is added as
   a named test case. (15:36: "it is missing the general 30cm or bigger for trout/char in region 2 fraser?"; 15:42:
   "also the last fraser example you sent doesnt have hatchery bs wild along with the no 30cm minimum")
10. **Status.** Step 1 (T805 parts C, B, D) is DONE and merged (`39f46ef56`). Step 2 (T809, the changed-answers tool
    `pipeline/tools/audit/answers_diff.py`) is DONE (`dac013480`). Tickets B, C, D and K below are marked done with what
    is left. The owner's go for the transition (16:37: "so maybe we can just work on the transisiton to the new trout
    redisgn?") starts steps 3 to 7.
11. **App half (M to S):** changed here only where the answers above require it. The app chat's check of M to S against
    the real app code (worker started ~16:30) will replace that table. The slot is marked in section 6.
12. **Open, not a blocker:** François Lake's outlet-area lines are bound to the whole lake. This is a placement fix that
    needs its own rebuild and list, and the owner has not been asked yet (T805, 16:37). It is listed as a data item that
    was there before this work.

---

## 1. For the owner (one page)

**What changes for someone using the app.** The page keeps its shape. The "Trout and char" entry stays where it is,
folded to its short line ("Up to 2", "Up to 4") with the expand arrow. What opens under it becomes the table you
picked (the round 8 look without the outline, round 11's spacing, round 15's hatchery label). The other fish entries
are not touched. The table has one big line ("Up to 5 today"). Below that is a shaded box for each limit ("Keep 5 in
all, only 1 over 50 cm"). Each fish gets one row with its picture and its size. Where only hatchery fish may be kept,
the row has a filled "HATCHERY ONLY" label beside the name. When the label covers every fish of a box, it sits once in
the box's top line. Wild fish get their own row only where they have their own limit. A white box on the right holds
any tighter limit ("only 1 of these 4"). Fish you must put back are listed under the box, and so are fish that are
closed. Today's rule is in big type, and a coming change is in small type under it ("until Oct 31, then release"). A
coming closure says what closes ("until Sep 30, then closed to trout and char"). The reopening date shows only while
it is closed today. A change that only loosens a rule reads "rule changes <date>". Possession stays the page's own
line, and a fish row speaks only where that fish's possession differs. Tapping a fish opens a sheet with three parts:
what applies here today, how to tell the fish, and where the rule comes from. The sheet closes with a swipe down.
Nothing in the law changes: the app says the same law in a clearer way.

**What has to be built, in seven steps**
1. **DONE (T805, merged 16:37).** Fill the data holes the reviewers found: possession on the fish row (Region 7 lake
   trout and bull trout, Cunningham, Tagish, Bowron), the one wild limit at Lois and Khartoum, wild char hatchery-only
   at the seven lakes, Shamrock and Kilgard creeks, Echoes Lakes. These reach today's app at the next release.
2. **DONE (T809, merged 16:19).** A tool that lists every answer that changed between two releases
   (`pipeline/tools/audit/answers_diff.py`).
3. Teach the data side to work out, for every fish on every water and every day, the next change. A tighter change
   or a change of state is said in words. A change that only loosens reads "rule changes <date>".
4. Have the data side write the whole table, word for word, for every water and day range. The app only lays it out.
5. Add a permanent check. For every water and every day, it proves the table says the same law as the rule
   engine, and that every "until …" line is true. If not, the build stops.
6. On the app side: draw the table under the Trout and char entry, the fish sheet, large text, dark mode and the
   colour-blind shapes, against test data. You do not see any of it until step 7.
7. One switch. In a single release and app build, the new table replaces the old trout card, and the old trout code is
   deleted. The old card stays for the other fish, as you chose. Older app versions are asked to update.

**What is risky**
- **Download size.** The first tap on a water is right at its size limit today: 1,243 bytes of room (I measured it).
  The other fish keep their old card data, so the switch saves less room than first estimated. You ruled: if it does
  not fit, raise the limit and shrink later.
- **Saying something false with confidence.** The "until …" lines are the hardest part. The step 5 check exists for this.

**What you decide or review, and when**
- The section 8 questions are answered: trout only, keep the page line, raise the limit if needed.
- After step 4: one list of every sentence the table can say (a few hundred lines, read once). It includes the folded
  entry's short line.
- After step 6: a picture pack of about 45 real waters, on staging, phone size, light and dark.
- Before step 7: the list of changed answers. It should be empty apart from the step 1 fixes you already approved.

**Rough size.** Steps 3 to 7: about 10 to 13 worker sessions on the pipeline side and 8 to 11 on the app side (ticket Q
is dropped). The app chat's check may change the app number. My confidence is medium-low (it could be 40% more). The
date rule and the size limit are where it could grow.

**What shipped first.** Step 1, in today's card (next release), and step 2, which is useful for every release.

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
| Card / tree wording | `answers/card.py:706` `card_of` (1,089 lines), `:221` `possession_of`, `:367` `_duties`; `answers/tree.py:360` `tree_of` (912 lines), `:855` `size_rule` [read] | `group`: headline, tiers, kinds, not_kept, worked, possession | **Stays for the other fish; its trout use goes.** The duty words, size rule, possession and "elsewhere" logic are lifted into the table builder. Since 16:37 this includes T805's additions: `card.possession_duty` (`card.py:446`, part B), `tree.across_rows` / `across_unsaid` (`tree.py:105,138`, part C), `tree.A_BOTH_LAKES_SAID` (`tree.py:101`, part D) [ran: grep]. The tree shape stays because the other fish sheet draws it (owner 15:47) |
| Card gate | `answers/card_check.py` (990 lines) [read, via map] | Re-proves card claims against the engine | **Pattern reused** for the table proof (ticket J). Its tree checks stay for the other fish's groups. `possession_problems` (`card_check.py:257`, T805 B) is a model for J's possession check |
| Year strip | `answers/year.py:91` `stamp_years` [read] | Per-ROW `[state, days]` runs (not per fish; finding 6) | Replaced by the per-fish timeline (ticket E). The strip stays for the page-wide year bar |
| Size words | `answers/common.py:452` `inches`, `:471` `cm_in`, `:482` `band_words` [read, via map] | "30 cm (12 in) or longer" | **Reuse** (one size grammar, RULINGS P37) |
| Encoding | `answers/encode.py:802` `RowsCodec` [read]: interns `decided`, `rows` (no year), `strips`; frame = `[spp, fish, [row ix], steelhead_line, [strip ix]]` | Tables `rows_decided`, `rows_row`, `rows_frame` and the `cell.rows` column in each `regs-{r}.sqlite.gz` (`release/contract.py:18`) [read] | New codec beside it (ticket I) |
| Possession gap | `card.py:236-240` [read]: `possession_of` returns None unless exactly one possession rule with `per_daily` covers every kind | Possession is blank on most rows | **Closed by T805 B** (16:37): a fish whose possession differs from the page's line carries one duty line (`possession_duty`). 3,166 named waters gain a line. Part of it (the Region 6 corpus edit for Bennett, Morley, Tagish) needs the usual reach and bundle rebuild before it reaches a release |

### 2.3 Bundle → screen (app)

| Stage | Where | What it does | Verdict |
|---|---|---|---|
| Versions | `app/packages/core/src/regs/versions.ts:14` `KNOWN_VERSIONS = {ladder:1, rows:33, gear:12, licence:7, display:20, glossary:5}` [read] | The store refuses a mismatch **only for listed sections** (`data/src/regs/store.ts:116-122`) [read] | A new `trout` section is invisible to older apps. That is what makes the build-behind possible |
| Reads | `data/src/regs/reads.ts:81` `cell`, `:298` reads `cell` by **named columns** [read]; `inputs.ts:46` `keyed` | One cell per (answer key, segment) | A new `cell.trout` column is safe for old apps [read] |
| Card model | `core/src/regs/card.ts:131` `hydRow`, `:154` `rowsAt`, `:188` `buildModel` [read, via map] | Wire → `CardModel` | Keep for the other tabs and the other fish. The trout part goes |
| Page builder | `core/src/regs/page.ts:46-71` `buildRegsPage`; trout part at `keep.ts:122-133` [read] | A row becomes a `GroupVM` if it has `group.headline`, a `PlainRowVM` if `isPlainRow`, else dropped | Replace with `troutOf(...)` |
| TS composers (1,683 lines with standing/others) | `grouphead.ts` (252), `tree.ts` (290), `tail.ts` (95), `plainrow.ts` (217), `keep.ts` (141), `glance.ts` `groupGlance` [read wc] | **Rule logic in TypeScript** (against the standing decision): `tree.ts:37` `sizeOf` (regex over band words), `:236` `liftCounts` (restructures the tree), `:118` `presOf`, `:181` steelhead link by regex; `grouphead.ts:132-139` picks the head tier; `plainrow.ts:70-121` "next year" day maths, `:154` 14-day "only" windows [read, via map] | **Delete the trout path** at cutover (ticket S). Whatever the Other fish sheet still calls stays (owner 15:47: other fish untouched). The app chat's check says which parts are which |
| Other TS rule logic, not trout-only | `standing.ts:97-117` picks the page's possession rule; `strip.ts:128` `troutLine`; `others.ts:33` `splitRows` / `TROUTISH` [read] | Same class of problem | Out of this scope; listed in section 8 |
| Components | `ui-native/src/regs/KeepTab.tsx` (124; GroupCard at `:47` glance fold and `:113`), `GroupCard.tsx` (293; `:235`), `PlainRow.tsx` (145), `KeepBlocks.tsx` (`GlanceFold` ~`:243`), `SheetBody.tsx:71` (GroupCard inside the **Other fish** sheet), `sheet/SourcesView.tsx`, `sheet/FishImage.tsx`, `YearStrip.tsx` [read] | Draws the card | The trout parts of KeepTab and PlainRow go. **GroupCard stays** (the Other fish sheet draws it). `Pictures`, SheetHost, SourcesView, FishImage and YearStrip are reused |
| Fish content | `core/src/regs/fish.generated.ts` `FISH_TIPS` `:208`, from `tools/emit-fish.mjs` [read, via map]; thumbs in `apps/mobile/assets/fish/thumbs/` | How to tell it | **Reuse** |
| Glance | Always on: `ui-native/src/shell/WaterModeHost.tsx:89` passes `glance` bare. No flag [read, via map]. `KeepTab.tsx:43` builds the folds (`GlanceFold`) [read] | The Trout and char entry's folded row with its expand arrow | **Keep the entry, its place and its arrow** (owner 15:47). Its short words come from the table (`fold`), and the new table is what it opens |

**Other fish use the old card too [ran].** In this release, 3,320 non-trout rows carry a `group.headline` and are
drawn by `GroupCard` inside the Other fish sheet (`SheetBody.tsx:71`). No frame draws a non-trout group in the main
block (4,528 frames checked, `scratchpad/trout-spec/meas4.py`). **Owner 15:47: trout only.** So GroupCard, the group
tree and the `group` field stay for those rows. Only the trout path is replaced and deleted. This second path is the
owner's choice, not a leftover.

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
| Place | The table is the body of the existing "Trout and char" entry. It opens from the folded row with its expand arrow (owner 15:47). The entry's own heading names it, so the table has no kicker of its own. "How to tell a hatchery fish ›" sits at the top of the opened table when any row or box carries the HATCHERY ONLY label or an origin word | row labels, origins | yes |
| Notes for some anglers | A note in force today (flag 2) goes above the top line; one not in force today (flag 1) goes directly under it, above the table, never under the table (round 7 item 8). One line each, with "⚠" | the 34 angler-closure rules (Skeena/Babine weekends, youth waters) | yes (the page's caveats today, `caveats.ts`) [inferred: placement by flag needs the rule's in-force test per segment, derive] |
| Top line, plain | "Up to N today". N = the stored headline | `group.headline.n` | yes |
| Top line, "or" form | "Up to 2 today, or 5 if they're rainbow trout" when the headline has a stored second half that starts "or"/"plus" (87 waters) | `headline.also[].says` | yes |
| Top line, three numbers | "Up to 2 today, or 5 if they're rainbow trout and no more than 2 Dolly Varden/bull trout" (Madden-type, 30 waters; owner yes 12:42) | also + the bull trout box | derive |
| Top line, two boxes counted apart | "Up to 6 rainbow trout or steelhead and 4 other trout and char today" (426 answers) | `scope.apart`, `parent` | yes |
| Top line, "plus" | "Up to 2 today, plus 2 Dolly Varden/bull trout" (Kaslo, Keen) | over-region tier | **derive. Gap:** 39 answers / 8 keys have no stored headline (round 2 grammar; still open, ticket F). Ross Lake: headline 2 traced by T805 D, it is right |
| Top line, second half for a cross-box wild limit | "…today," then under it "only 2 wild trout or char, each under 40 cm (16 in)", in the same words as the "All together" line (Lois, Khartoum) | one wild quota across rows | **yes since T805 C** (`tree.across_rows`; in today's card as "no more than 2 wild trout and char in all, …"). F lifts it; the exact words are settled in G |
| Top line, no keeping | "Release every one until <date>" / "Closed for trout and char until <date>" / "Nothing to keep today". While closed today, the reopening day may be said ("reopens Nov 1", round 12's closed-day picture): this is the only place a reopening date appears (owner 15:36) | timeline | derive (ticket E) |
| Card line (small) | Above the table when **every fish of the card** changes on the same day to the same closure: "Until Oct 31, then closed to all fishing" (whole-water closure) or "until Sep 30, then closed to trout and char" (scope, round 11 4a). **No reopening date** on a coming closure (owner 15:36) | timeline + closure scope | derive (E). The scope is the winning closure rule's species set |
| Keep box frame | Look A: one tint block. Strip across the top, rails as inset shadows, bottom edge, a right band with no lines between rows. No outline (owner 15:16) | none (layout) | n/a |
| Strip | "Keep **N** in all" + box sentence on one line where it fits: the filled **HATCHERY ONLY** label when every fish of the box may be kept only as hatchery (owner 16:00; round 15: "Keep 2 in all [HATCHERY ONLY] only 1 over 50 cm (20 in)", Fraser Region 2 at Mission), then size ("only 1 over 50 cm (20 in)"), a wild limit only where wild fish have their own quota ("only 2 wild, each 40 cm (16 in) or less", which must name its fish when not all of them: "no wild rainbow, cutthroat or brown trout over 50 cm (20 in)"), possession for the whole box only where it differs from the page's line ("In your possession: only 1 in all, not 1 of each", Bowron). Optional tag "· Lake's rule" | size tier, origin2 cond, possession | size: yes (`tree.py:855`); wild: yes since T793 (`da961f55c` is in HEAD [ran]), not in release r-46e4355a7277 [read: 4th attack], next release; possession: **yes since T805 B** (next release) |
| Strip date (small, own line) | Only when **every row** of the box has the same next change (round 11 4b). Otherwise each row carries its own. Never "the others:" | timeline | derive (E) |
| Row | Picture (`pic` = fish code), name as the book prints it ("Dolly Varden/bull trout" in full); the filled **HATCHERY ONLY** label beside the name when only hatchery fish of it may be kept and the box's top line does not already carry the label (owner 15:48, 16:00); the origin word ("hatchery" / "wild") only where wild fish have a quota or size of their own and the fish has two rows (Kitimat, Lois, Khartoum, Ruby, Cultus); `sub` ("a rainbow over 50 cm (20 in)"); then bold lines in this order: size (hidden when it is "any size" and the box carries a size sentence), "uses the 1 over 50 cm (20 in)", possession line (only where it differs from the page's line), stop line; then a quiet line; then the row's date (small, last). Wild and hatchery with the same rule: no origin mention at all | items, bands, origins | yes |
| "uses the 1 over 50 cm" | When the fish's smallest keep size is above the box's size mark, or it is a steelhead under a plain 1-over-50 limit | bands, size tier | derive |
| Stop-fishing duty | On every keepable steelhead row with the duty: "after keeping N, stop fishing this <noun> today". The noun comes from the stored duty ("stream", "river", "creek", "slough", "channel"), never "here" | `_duties` (`card.py:367`) | yes |
| Possession on a row | Only where this fish's possession differs from the page's own line (owner 15:42 "Keep the page line"). "in your possession: only N" / "only one day's limit"; with a size: "in your possession: only 2 lake trout, only 1 over 50 cm (20 in)". When possession is **below** the day's number (Cunningham), the white box carries "only 2 in your possession" and a quiet line "3 a day, but never more than 2 lake trout with you". Bowron Lake Park: lake trout follow Region 7's "1 day's limit" on their own row, apart from the park's 1 for the other trout (owner 16:37 "Keep today's"). Nation Lakes: both lines as printed (owner 16:37) | possession per fish, with members, size clause | **yes since T805 B** (`card.possession_duty`, RULINGS P58; checked by `card_check.possession_problems`). F lifts it; the table's words are settled in G |
| Hatchery-only char | Dodd, Lois, Khartoum, Horseshoe, Nanton, Windsor, Ireland: char rows carry the filled HATCHERY ONLY label (RULINGS C5), or the box's top line carries it when every fish of the box is hatchery only | origin on the char rows | **yes since T805 C** (all seven lakes now store hatchery char only, `rows.evaluate.A_SIZE_CLAUSE_RELEASES_AS_A_RULE`); next release |
| Right band, no tighter limit | "up to the N" (N = the box's number), one cell per row (owner 13:48) | box n | derive |
| Right band, own limit | White box "only 3" level with its fish | item band n | yes |
| Right band, shared limit | White box "only 1 of these 4" spanning consecutive rows. Rows are sorted so that each shared box is one piece (round 3). A fish in a shared box that also has its own limit keeps the own limit as a bold line under its name | `real_daily.shared_count`, subcaps | yes |
| Summed tiers | Never drawn. A tier whose n differs from its rule's `take` is the sum of two limits (Kamloops, Adams p3, Williston B) | tier n vs rule take | derive (2nd attack: detectable) |
| White box date | Small, under the limit, when all its fish share the next change | timeline | derive |
| Release / Closed group | Under the keep boxes: no fill, faded picture, one column "↩ Release" / "✕ Closed" spanning the group. Its date is small under the word; a date over 30 characters goes under the fish's name (round 11). A fish whose hatchery ones may be kept is **not** listed here as a wild row: its keep row's HATCHERY ONLY label says it (owner 15:48) | decided per fish | yes |
| "All together" line | Under the table, only for a limit across two boxes (the Lois wild limit; Vedder "no more than 4 a day across Region 2"; Ross's size; Echoes Lakes "from both lakes together", T805 D). In the same words as the top line's second half | cross-box limit | derive. Lois: **yes since T805 C**. Where it sits (outside both boxes) is still open in the design (T810) |
| Year line | Only when **every** fish of the table is in one state today (all release, or all closed) **and** the water is open: "Trout and char through the year" bar (Elk). Never on a closed day (owner 12:42) | per-fish year | derive (E) |
| Links | "Sources ›" (all rules) and "Fished elsewhere today? ›" | rule refs; `elsewhere` / `worked` | yes |
| Folded row (glance) | The existing Trout and char entry's short line, with its expand arrow (owner 15:47: "the better text like 2 or 4 with expand arrow"): "Up to 2", "Up to 4", and per top-line form "Up to 2 or 5", "Up to 6 rainbow, 4 others", "Release" / "Closed" with the date small, "None to keep". The exact forms go in G's sentence catalogue for the owner | top line | derive |

### 3.2 Two sizes of text (owner 15:16; confirmed 15:36 "it looks good", accepted design item 2)
Today's rule is in the bigger, bold ink: the strip sentence, row sizes, "uses…", limits, "up to the N", white box text.
The HATCHERY ONLY label is a filled label in the same place in a row and in a strip (owner 16:00). It is today's rule,
and it must stand out on a single fish's row (owner 15:56, the Skeena steelhead).
A coming change is smaller and regular, in the quiet colour, on its own line: strip date, row date (last line of the row),
white box date, Release column date, card line. Measured in the mock: the quietest line is 5.88:1 in light and 7.96:1
in dark. Rows sit halfway between round 8 and round 9 spacing (round 11). The data marks each text `now` or `next`; the app picks the style.

### 3.3 The date rule (one rule, computed in Python; revised for the owner's 15:36 answers)
For each (answer key, segment, fish, row; a row is the fish, or the fish and origin where wild has its own quota):
1. Walk the following segments (wrapping into next year) and compare the fish's **answer**: state (keep / release /
   closed / absent), count, size bands, origin rule, possession. Take the **first** segment whose answer differs.
2. **A change that is only looser** is dated with no details: "rule changes <day>" (owner 15:36: "could just mention a
   rule change happens on that date not specifics"). "Only looser" means every length and origin that is legal today
   stays legal at the same or a higher count, and the state does not tighten (4th attack fault 3; Bella Coola Apr 1).
   This is never false, which is why the old "skip it" rule is dropped.
3. **A tighter change, or a change of state**, is dated with its words: "until <last day>, then <words>". The words are
   "release", "closed to all fishing", "closed to trout and char", "closed for steelhead", "1 in all", "<size> or
   longer", "every wild one goes back", "hatchery only". **No reopening day on a coming closure** (owner 15:36: "drop
   reopen unless it is currently closed"). A change that is tighter in one part and looser in another counts as
   tighter and is said in words.
4. **Closed today:** the top line says when it opens again ("Closed for trout and char until Oct 31" or "reopens Nov
   1"; the exact form is settled in G). This is the only place a reopening date appears.
5. A change that comes back within a short gap still gets dated. The shortest gap in the release is 15 days (Denetiah)
   [read: 4th attack]. Add a build warning under 10 days.
6. A one-day segment (Adams River Jul 31, 60 cm for one day) puts the stricter 80 cm on the row (owner 15:36: "Yes,
   80 cm on the row") and one sentence in the fish's sheet, generated from that segment (this replaces hand note
   `NOTE9`). The row's date line follows rules 2 and 3 like any other ("rule changes Jul 31") [inferred: the owner was
   not shown that line; it goes in G's catalogue].
7. Placement: card line if every fish shares the date and words; else the strip if every row of the box shares it;
   else the white box if all its fish share it; else the row.
This replaces the mock's hand-written `OV9`, `BOX9`, `NOTE9` (`work/r9add.js:65-77`), `NOUN9` (`:7`) and `CLOSE11`
(`work/r11add.js:7`) [read].

### 3.4 The fish sheet
| Part | Rule | Fact | Exists? |
|---|---|---|---|
| Header | Picture, full name, the HATCHERY ONLY label or origin word as on the row, today's answer for this fish: "Keep · only 1 · counts toward the 5" / "↩ Put it back · until Jan 31" / "✕ Closed · do not fish for it". Closes by swipe down (over 90 px), ✕, tap outside, Escape. No sideways moves (owner 12:42) | row | derive |
| Here, today | Ordered lines: size; hatchery / wild (where the row says it: "Hatchery only: wild ones go back", or the wild fish's own quota); "Counts toward the N in all, together with …"; "Only K of these M: this fish and …"; "This fish: only N"; the box's size limit, plus "A kept one is always over 50 cm (20 in), so it is that one" where true; possession ("Only 1 in your possession, not two days' worth. Fish at home don't count."; the last sentence is `POSSESSION_HOME`, `display/rule.py:170` [read, via map]); the stop duty in bold ("Once you have kept 1 steelhead here, stop fishing this river for the rest of the day: the whole river, not only this stretch"); "Licence: 10 a year · record each right away · Steelhead stamp"; the coming change as a sentence, by the same rule as 3.3 ("From Nov 1 every wild one goes back"; a looser-only change: "The rule changes on Apr 1"); the one-day note (Adams) | as the card | as the card |
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
  fold: string;                                   // the Trout and char entry's short line ("Up to 4")
  hatchLink: boolean;
  notes: { text: string; today: boolean; src: number[] }[];
  boxes: Box[];                                   // keep boxes first, then release, then closed
  together?: { text: string; src: number[] };
  year?: [state: "keep"|"release"|"closed", days: number][];   // only the all-one-state case
  elsewhere: string[];                            // "Fished elsewhere today?" lines (from group.worked today)
  sheets: Record<string, FishSheet>;              // key = Row.key
};
type Box = { kind: "keep"|"release"|"closed"; n?: number; strip?: string; tag?: string;
             label?: "hatchery_only";             // filled label in the strip: every fish of the box (owner 16:00)
             rows: Row[]; band: Band[] };
type Row = { key: string;                         // "LT"; "ST/wild" + "ST/hatchery" ONLY where wild has its own quota
             fish: string; origin?: "hatchery"|"wild";   // set only on such twin rows (owner 15:48)
             label?: "hatchery_only";             // filled label beside the name; never when the box carries it
             name: string; sub?: string;
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
     {"key": "ST", "fish": "ST", "label": "hatchery_only", "name": "Steelhead", "sub": "a rainbow over 50 cm (20 in)",
      "lines": [{"t": "uses the 1 over 50 cm (20 in)", "w": "now"},
                {"t": "after keeping 1, stop fishing this river today", "w": "now"}], "src": [45, 9]},
     {"key": "GB", "fish": "GB", "name": "Brown trout", "lines": [{"t": "30 cm (12 in) or longer", "w": "now"}], "src": [45]},
     {"key": "CT", "fish": "CT", "name": "Cutthroat trout", "lines": [{"t": "30 cm (12 in) or longer", "w": "now"}], "src": [45]}],
    "band": [{"from": 0, "to": 0, "text": "up to the 5", "kind": "upto"},
             {"from": 1, "to": 1, "text": "only 3", "kind": "own"},
             {"from": 2, "to": 5, "text": "only 1 of these 4", "kind": "shared"}]},
   {"kind": "release", "rows": [
     {"key": "DV", "fish": "DV", "name": "Dolly Varden/bull trout", "lines": [], "src": [41]}], "band": []}],
  "elsewhere": ["…"], "sheets": {"ST": {"head": "Keep · hatchery only · only 1 of these 4 · counts toward the 5",
    "here": [{"t": "Hatchery only: wild ones go back."},
             {"t": "Once you have kept 1 steelhead here, stop fishing this river for the rest of the day: the whole river, not only this stretch.", "bold": true},
             {"t": "Licence: 10 a year · record each right away · Steelhead stamp"}], "src": [9, 45]}}},
 "dates": {"row": {"0:1": "until Sep 14, then release"}, "band": {"0:2": "until Oct 31, then release"},
           "sheet": {"LT": ["From Sep 15 every one goes back."]}}}
```
(Rows, limits and dates are as the round 11 picture `shots/r11/skeena.png` draws them from the release [read]. Revised
16:40 for the owner's 15:48 and 16:00 rule: the steelhead is ONE row with the filled HATCHERY ONLY label, as in round
14 A and round 15. There is no wild steelhead row under Release, because wild steelhead have no quota of their own here.)

### 4.3 Example: Lois Lake (`wbk:329197058`), Jul 15
```json
{"table": {
  "top": {"big": "Up to 6 rainbow trout or steelhead and 4 other trout and char today",
          "second": "only 2 wild trout or char, each under 40 cm (16 in)"},
  "fold": "6 rainbow, 4 others", "hatchLink": true, "notes": [],
  "boxes": [
   {"kind": "keep", "n": 6, "tag": "Lake's rule", "rows": [
     {"key": "RB", "fish": "RB", "name": "Rainbow trout", "lines": [{"t": "any size", "w": "now"}], "src": [3]},
     {"key": "ST", "fish": "ST", "label": "hatchery_only", "name": "Steelhead", "lines": [{"t": "any size", "w": "now"}], "src": [3]}],
    "band": []},
   {"kind": "keep", "n": 4, "strip": "only 1 over 50 cm (20 in)", "rows": [
     {"key": "EB", "fish": "EB", "name": "Brook trout", "lines": [], "src": [1]},
     {"key": "GB", "fish": "GB", "name": "Brown trout", "lines": [], "src": [1]},
     {"key": "CT", "fish": "CT", "name": "Cutthroat trout", "lines": [], "src": [1]},
     {"key": "DV", "fish": "DV", "label": "hatchery_only", "name": "Dolly Varden/bull trout", "lines": [
        {"t": "60 cm (24 in) or longer", "w": "now"},
        {"t": "uses the 1 over 50 cm (20 in)", "w": "now"}], "src": [1, 2]},
     {"key": "LT", "fish": "LT", "label": "hatchery_only", "name": "Lake trout", "lines": ["…same two lines…"], "src": [1, 2]}],
    "band": [{"from": 0, "to": 2, "text": "up to the 4", "kind": "upto"},
             {"from": 3, "to": 4, "text": "only 1 of these 2", "kind": "shared"}]}],
  "together": {"text": "only 2 wild trout or char, each under 40 cm (16 in)", "src": [3]}},
 "dates": {}}
```
Note: "up to the 4" is one cell per row in the drawing. The band entry with `from`–`to` stands for three equal cells.
The app draws one cell per row index. The one wild quota across both rows and C5 on the char rows are **built by T805
part C** (merged 16:37, in the next release; today's release r-46e4355a7277 stores neither). The steelhead is hatchery
only, with the label (owner 15:56: "Hatchery only (Recommended)"). Lois is a case where wild fish have a quota of their
own (2 across both boxes), so that wild limit is said once in the top line's second half and in `together`. The
rainbow and the other trout carry no origin word, because the wild quota counts them all. The exact words come from
part C's sentence and are settled in G.

### 4.3a Example: one box all hatchery only, Fraser River in Region 2 at Mission (round 15)
```json
{"kind": "keep", "n": 2, "label": "hatchery_only", "strip": "only 1 over 50 cm (20 in)",
 "rows": [{"key": "CT", "fish": "CT", "name": "Cutthroat trout", "lines": [{"t": "30 cm (12 in) or longer", "w": "now"}], "src": [0]},
          "…one row per fish, none with a label (the strip carries it), no wild rows under Release…"],
 "band": [{"from": 0, "to": 5, "text": "up to the 2", "kind": "upto"}]}
```
Drawn as "Keep 2 in all [HATCHERY ONLY] only 1 over 50 cm (20 in)" (owner 16:00). The fish list and numbers are
placeholders. The builder takes them from the release, and this water is one of F's named cases.

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
 "dates": {"box": {"0": "until Sep 30, then closed to trout and char"},
           "row": {"1:0": "until Sep 30, then closed to trout and char"},
           "sheet": {"LT": ["From Oct 1 no fishing for trout and char here (whitefish and burbot stay open)."]}}}
```
Revised 16:40 (owner 15:36): a coming closure gives no reopening day, on the card or in the sheet. On a day inside
the closure (for example Oct 15) the table is the closed form: top line "Closed for trout and char until Oct 31" (or
"reopens Nov 1", as round 12's closed-day picture; one form, settled in G), and that is where the reopening date
appears. This is Region 3: no 30 cm minimum and no wild/hatchery split, which is right by the book (owner 15:42 asked;
checked against `region-3.md`, Region 2's Fraser at Mission shows both, round 12).
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
- **Correction 16:40 (trout only):** the 55 kB above is the whole `group` tree, and it includes the non-trout groups that
  the Other fish sheet keeps drawing (3,320 rows over all regions). Only the trout share goes at cutover, so the saving is
  smaller than 55 kB [inferred; A measures the trout share separately]. Since T805 B and C, today's group carries extra
  possession and wild-limit lines, so A measures on a build that has them.
- **Consequence:** a promoted release can **never** carry both the old trout group tree and the new section. The
  build-behind ships the new section only in side releases (fixture, staging test channel). The cutover release
  carries the section and drops the trout rows' `group` in the same release. If the cutover still does not fit,
  **raise the first-tap limit by the measured amount and shrink later** (owner 15:42, as O5 for search).

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
- Cutover: `rows` 33 → 34 (drop `group` **on trout rows only**, and any field only the old trout block read, e.g.
  `real_daily`, `elsewhere`; ticket L lists them by grep of the app's readers, **including the Other fish sheet's**, which
  keeps reading `group` for non-trout rows). The app's `KNOWN_VERSIONS` gets `rows: 34, trout: 1`. Older apps refuse rows 34
  (`store.ts:116`) [read], so the cutover release is published with a `minAppVersion` stamp (`release/publish.py:84`,
  T251) [read]: older apps keep their data and ask for an update.

---

## 5. Pipeline work (main chat)

| Ticket | Goal | Files | Tests (with mutation-style) | Depends on |
|---|---|---|---|---|
| **A. Size spike and contract** | Prototype `TroutCodec` and a crude table for one region (R6) and R5; measure gz against the `group` tree; settle the section layout in 4.1; write the format spec text into `encode.py`'s spec | `answers/encode.py`, scratch | the measurement script committed under `pipeline/tools/audit/` | none |
| **B. Possession on the fish row: DONE (T805 B, `994941826`, merged `39f46ef56`)** | Built in today's card: `card.possession_duty`, `card_check.possession_problems`, RULINGS P58; 3,166 named waters gain a line; Region 6 corpus edit (Bennett, Morley, Tagish) needs reach + bundle rebuild to reach a release. Bowron: lake trout keep Region 7's "1 day's limit" on their own row (owner "Keep today's"). Nation Lakes as printed. No more possession patching outside the redesign (owner 16:37). **Left for F:** lift `possession_duty` into the table, said only where it differs from the page's line. *Original scope:* Possession per fish (and per box) from every possession rule: Region 7 "one day's limit" (`possession_exceptions.r1/r2`, p.58/p.64), with a size clause (Cunningham p.59; Tagish p.53 size missing from `tagish_lake.r2`), below the daily number (Cunningham, Tchentlo, Tsayta, Indata, Nakinilerak), Bowron park members to include lake trout, and the 4th attack's Bowron artifact (possession only on lake-trout-release days: Istsi, Wolverine p1, Sootheran p1) | `rows/evaluate.py:243-268`, `card.py:221-253` (or the new module), the corpus for Tagish (A2 backup rule) | per case a whole-sentence test; mutation: drop the size clause → the check (J) fails; a possession larger than daily on a row → the build refuses | none. **Changes shipped answers** (about 14,200 R7 entries gain a line): owner list via K |
| **C. One wild limit across rows; char origin per C5: DONE (T805 C, `0f6fe2868`)** | Built: `tree.across_rows` / `across_unsaid` (the answers build stops on a card that leaves it out), `rows.evaluate.A_SIZE_CLAUSE_RELEASES_AS_A_RULE`; Lois, Khartoum, Windsor change, all stricter (owner go 16:09). **Left for F:** lift `across_rows` into `top.second` / `together`. *Original scope:* Store Lois/Khartoum's wild quota once across both rows (T805.1); make Dodd, Lois, Khartoum, Horseshoe, Nanton, Windsor, Ireland store the char the same way (C5); confirm the T793 rules reach a built release | `rows/card_model.py:60` `_origin2_rule`, `rows/conds.py` | a cross-row wild limit counted once (mutation: split it again → fails); the seven lakes give one answer | T793 merged and delivered. **Changes shipped answers** |
| **D. Small data lines: DONE (T805 D, `764fe73fc`)** | Built: Shamrock and Kilgard creeks keep no 20–30 cm steelhead (`A_STEELHEAD_IS_OVER_50_EVERYWHERE`, RULINGS S13); Echoes Lakes "from both lakes together" (`tree.A_BOTH_LAKES_SAID`, P59). Looked at, unchanged: Ross Lake's headline 2 is right; the cross-border line is on the rule list. **Left:** the 8 rule sets with no headline (moves to F); **François Lake's outlet-area lines are bound to the whole lake**: a placement fix needing a rebuild and its own list, owner not asked yet. It is a data item that was there before this work and does **not** block the build. *Original scope:* Shamrock Creek's "steelhead 20–30 cm" (book p.24: trout/char 20–30 cm, Jun 16–Jan 31); the cross-border quota (Atlin p.50, Bennett, Tagish, Teslin, Laidlaw, Swift, Rancheria) as a stated note in `notes`; Echoes Lakes "in the aggregate from both lakes" as `together`; trace Ross Lake's headline 2 against `real_daily` 4; the 8 rule sets with no headline; check François Lake's outlet char rule | corpus (A2), `rows/groups.py` | one test per water, by stream key / water through `under_test` | none. Shamrock changes an answer |
| **E. Per-fish timeline and the date rule** | For every (akey, segment, fish, row): the answer and the next dated change per 3.3 (first change; looser-only → "rule changes <day>"; tighter or state change → words; scope; **no reopening day on a coming closure**; reopening day only on a closed-today table; one-day notes; weekday moments; year wrap) | new `answers/trout/timeline.py`; reads rows frames over all segments and the closure rule's species | over the release: never "until X, then release" when keepable the next day; never "until X" when not keepable after; no change left undated; "rule changes" never used for a tighter change; no "reopens" outside a closed-today table; mutations: shift a date by a day, call a tighter change looser, mark a closure whole-water, add a reopening day to a coming closure → each fails | none (reads `produce` output) |
| **F. Table builder (trout and char only)** | Boxes, rows, sort, bands ("up to the N", own, shared, possession), summed-tier detection, top-line forms, strip sentences, row lines, release/closed groups, notes with today flags, `together`, `year`, `fold`, rule refs; reuses `band_words`, `_duties`, `size_rule`, and T805's `possession_duty`, `across_rows`, `A_BOTH_LAKES_SAID`. **Origin rule (owner 15:48, 16:00):** same rule for wild and hatchery → one row, no mention; keepable only as hatchery → one row with `label: hatchery_only`, no wild row under Release, and the label moves to the box when every fish of the box carries it; twin rows only where wild fish have a quota or size of their own. Possession only where it differs from the page's line. The 8 rule sets with no stored headline (from D). No non-trout tables (Q dropped) | new `answers/trout/table.py`; lifts parts of `card.py`, `tree.py` (copies or shared helpers: the old card keeps them for the other fish) | the 43 mock waters as named cases (stream key + metre), plus Fraser Region 2 at Mission, Skeena (one hatchery-only steelhead row), Adams River (80 cm on the row); every top-line form; summed tier never drawn (mutation: draw it → J fails); the origin rule's three cases (mutations: emit a wild row under Release for a hatchery-only fish; drop the box label when every row has it; show an origin word where wild and hatchery are the same → each fails) | B, C, D (done), E |
| **G. Words** | Every sentence in one module with whole-sentence tests; emit a catalogue of every distinct sentence with counts for the owner, including the folded entry's short line, "rule changes <day>" and the closed-today reopening form | `answers/trout/words.py`; `pipeline/tools/audit/trout_words.py` | whole strings only (AGENTS 33) | F |
| **H. Sheets and sources** | Per fish: header answer, "Here, today" lines, licence line, the coming-change sentence, rule refs ordered (this fish's first, up to 4); move the "Fished elsewhere" lines (`groups.py:285`) into `elsewhere` | `answers/trout/sheets.py` | whole strings; every rule ref resolves to a rule (mutation: a dangling ref → the build stops) | F |
| **I. Section and codec** | `Section("trout", 1, …)` behind `--trout-table` (off by default); `TroutCodec` (tables, date sets, frames interned); `cell.trout`; contract tables; pydantic models in `answers/model/` + generated TS (`model/emit.py`, `answers.generated.ts`); `regs_fixture.py` and the mini release built with the flag | `answers.py`, `encode.py`, `release/contract.py`, `answers/model/`, `release/regs_fixture.py`, `tools/mini_release.py` | round-trip encode/decode; frame refs in range; `test_mini_release.py`; size gates in `test_release.py` measured with the flag on | A, F, H |
| **J. The proof (permanent build gate)** | For every (akey, segment, moment): derive the law **from the table alone** (a separate decoder: per fish, origin and length bucket the count allowed, closed or release, possession; a HATCHERY ONLY label on a row or box decodes as "wild: release", and a row with no origin as "same for both") and compare it with the engine's (`rows.fish` decided + `verdicts.sqlite`), not with the table builder's inputs. Check every date claim against the following segments (as the reviewer's `check.py`): a "rule changes <day>" line must point to a looser-only change on exactly that day, and a worded line to a tighter change or a change of state. Zero unexplained differences, or the answers build stops (like `card_check.py`) | `answers/trout/check.py`; hooked where `card_check` runs | mutation tests: change one number, one size edge, one origin, one date, one scope in a built table → each must fail; a test that the decoder does not import the builder | E, F, I |
| **K. Changed-answers list: DONE (T809, merged `dac013480`)** | `python -m pipeline.tools.audit.answers_diff <A> <B> [--out DIR]` → `diff.json`, `diff.txt`, `owner.md`. **Left on T809, needed before the cutover diff (section 7 point 2):** a cross-atlas pair never run; the regs side proven only on the mini release; possession and gear-count directions read "unclear"; card rows and shared limits fall into the catch-all. *Original scope:* Diff two releases (or two builds) per water and day range in plain words, grouped by kind of change, with counts and water names; output one page for the owner | `pipeline/tools/audit/answers_diff.py` | a built pair with a known planted difference; an identical pair → empty | none. **Build first:** B, C and D need it |
| **L. Cutover, pipeline half** | Flag removed (always on); `rows` 34 without `group` **on trout rows** and the fields only the old trout block read; delete the tree's **trout-only** wording paths, `card_check`'s trout tree checks and their tests. The group tree, `card_check`'s tree checks and `GroupCard`'s data stay for the non-trout rows (owner 15:47). `minAppVersion` stamp on the release; first-tap limit raised if A/I measured it does not fit (owner 15:42) | `answers.py`, `card.py`, `tree.py`, `card_check.py`, tests | the full answers suite; size gates; `app_reader_versions`; a test that no trout row carries `group` and every non-trout row that had one still does | J, the app's S ready to land together |

Order (revised 16:40): **K, B, C, D are done.** Next: **A → E → F → G, H → I → J → L.** A and E can start at once in
parallel (E reads `produce` output and does not need A). François Lake's outlet fix and T809's leftovers run beside
the build on their own tickets and do not block it.

---

## 6. App work (app chat)

> **Slot for the app chat's check (16:40).** A worker in the app chat started ~16:30 to check M to S against the real
> app code. Its revised app ticket list **replaces this table** when it hands back. Until then, the table below changes
> only where the owner's answers require it: trout only (Q dropped), the table opens under the existing folded Trout and
> char entry, the HATCHERY ONLY label, and S keeps GroupCard and the group path for the other fish. File paths and line
> numbers here are the 15:40 read and are not re-checked.

| Ticket | Goal | Files | Tests | Depends on |
|---|---|---|---|---|
| **M. Read the section** | `trout: 1` in `KNOWN_VERSIONS` (still unused by the page); `cell.trout` read; `trout_table/dates/frame` blobs through the existing codec and zdict; generated types | `core/src/regs/versions.ts`, `data/src/regs/reads.ts`, `inputs.ts`, `store.ts` | `store.test.ts` on the committed fixture release (re-cut with the flag) | I |
| **N. View model** | `troutOf(inputs)` → `TroutVM`: a pure map from section to VM, styles from `now/next/quiet`, a11y labels joined in order; no comparisons or sums (a lint-style test: the module imports no `rows`/`group` types) | new `core/src/regs/trout.ts`, `model.ts` | whole-string tests per mock water from the fixture release; the VM's strings equal the section's strings (none added or changed) | M |
| **O. The table** | `TroutCard`, drawn as the body of the **existing** Trout and char entry: the entry keeps its place, its folded short line ("Up to 2", from `fold`) and its expand arrow (owner 15:47); no kicker of its own. Inside: hatchery link, notes, top line, card line, keep box (strip with the optional filled HATCHERY ONLY label, rows with the label beside the name, right band with white boxes and "up to the N" cells), release/closed group, All together, year line, links; look A tokens; two text sizes; round 11 spacing; the filled label stands out on a single row (owner 15:56); dark; 200% fold (bands → bold lines with a rail); shapes-only (the label is a word, not a colour) | new `ui-native/src/regs/TroutCard.tsx` (+ parts), `KeepTab.tsx` behind a dev-only switch | render tests on fixtures; no sideways scroll at 360/390 px, 100/200%; every target ≥ 44 px; ARIA props; the label reads as "hatchery only" to a screen reader | N |
| **P. The fish sheet** | Header, Here today, How to tell it (`FISH_TIPS`, both DV pictures, clipped-fin drawing, "Hatchery or wild?" link), Sources (this fish first); swipe down over 90 px, ✕, tap outside, Escape; no sideways moves; reuse `SheetHost`, `FishImage`, `SourcesView` | `core/src/regs/sheets/trout.ts`, `ui-native/src/regs/sheet/TroutSheet.tsx` | open every fish of every fixture water; gestures on web and native (file pairs per AGENTS 34a) | N |
| ~~**Q. Other fish on the same table**~~ | **DROPPED** (owner 15:47: trout only; the other fish entries are untouched and keep the old card) | n/a | n/a | n/a |
| **R. Picture pack and the test bed** | The 43 mock waters plus Fraser p11 (Region 3) and the Fraser at Mission (Region 2) at 390/360 px, folded and opened, light/dark, 100/200%, from a side release on the staging test channel, for the owner; a Maestro walk that opens the card and a sheet on a fresh install | `tools/store-shots.mjs`, `tools/testbed` | the walk on a fresh install (AGENTS W46 test bed) | O, P |
| **S. Cutover and deletion, app half (trout path only)** | The trout entry reads only `trout`; `rows: 34`; delete the **trout** path: the group branch of `keep.ts:122-133`, the trout use of `glance.ts` `groupGlance`, `plainrow.ts`, `grouphead.ts`, `tree.ts`, `tail.ts`. **Keep** whatever the Other fish sheet (`SheetBody.tsx:71`) still calls, including `GroupCard.tsx` (owner 15:47); the app chat's check says which files go whole and which stay. Delete their trout tests and fixtures; re-cut `taps.json`, page oracle and VM fixtures (`node app/tools/regs-recut/sync.mjs`); update `keep.props.parity.test.ts`; knip clean; `app/MAP.md` | as listed | `pnpm check`; conformance; the full tier once | L ready, owner sign-off |

The dev-only switch in O exists only between O and S. It is removed in S, so no second trout path is left behind. The
old card that stays for the other fish is the owner's choice and is not a second trout path.

---

## 7. Proof and rollout

1. **Same law, every water and day.** Ticket J is a build gate, not a one-off audit: every release proves that the
   table's law equals the engine's for every answer key, segment and weekday or hour moment. Segments partition the
   year exactly, so this covers every day. It also proves every date claim. It never compares the table against the data
   it was built from (the "checks seeded from input launder" lesson), and its mutation tests prove it can fail.
2. **Old against new.** Before the switch, K diffs the last release before cutover against the cutover candidate at the
   law level (expected: no change), and also prints old and new card text for a sample of waters for reading. Any law
   difference is a bug or one of B, C, D's already-approved changes. The non-trout rows must come out identical,
   including their `group` (trout only). T809's leftovers (cross-atlas pair, regs side on a real release, the
   "unclear" possession direction) are closed before this run.
3. **What the owner sees, in order:** ~~B/C/D changed-answer lists~~ (done: C 16:09, B and D 16:37); the sentence
   catalogue (G); the picture pack (R); the cutover changed-answers list (expected empty). Each goes into OWNER-TODO
   and the owner to-do page with pictures (owner rules).
4. **Landing order, branch never red:** pipeline tickets land with the flag off (releases unchanged; tests build the mini
   release and the fixture with the flag on). App M to R land against the committed fixture release, which carries the
   section; old app paths are untouched and older releases still load. **Cutover = one merge carrying L and S
   together** (a format-pair test fails if either lands alone), then fast-fail, a staging release with `minAppVersion`,
   the APK through the benches, then Play internal.
5. **Deleted after:** the **trout** use of the group tree and its trout-only wording paths, the trout path of the TS
   composers, their tests and fixtures, the flag, and the dev switch. The group tree, `card_check`'s tree checks and
   `GroupCard` stay for the other fish (owner 15:47).
   Then `pipeline/docs/MAP.md`, `app/MAP.md` and AGENTS rule 53's note on the guide get updated.

---

## 8. Risks and open questions

### Risks, ranked
1. **Size [ran].** First tap has 1,243 B of room. A promoted release cannot carry the old trout tree and the new section
   together, so the owner never sees the new card on a normal staging release until cutover. Trout only means less is
   removed at cutover than the 55 kB measured (4.5). Mitigation: ticket A first, side releases on the test channel;
   if it does not fit, **raise the limit and shrink later** (owner 15:42, answered).
2. **False date lines.** The looser/tighter order across counts, sizes, origins and possession is the hardest logic. A
   mistake reads as a confident false "until", or as "rule changes" on a day that tightens. Mitigation: J's date check
   over every segment, mutation tests, and the reviewer's simulation (1.9 million fish-days, never wrong) as a target.
3. **Two card paths by design.** GroupCard keeps drawing 3,320 non-trout rows in the Other fish sheet [ran]. The owner
   chose trout only (15:47). The risk is that L or S deletes a helper the other fish still use, or that the trout
   builder and the old card drift apart on shared helpers (`possession_duty`, `across_rows`, `size_rule`). Mitigation:
   the L test that non-trout `group` is unchanged, and the cutover diff (section 7 point 2) on non-trout rows.
4. **Data fixes changed many answers: done.** T805 B, C, D were shown to the owner as grouped lists and approved. They
   reach a release at the next build. Part B's Region 6 corpus edit needs the reach and bundle rebuild.
5. **The cutover forces an app update** (rows 34). Fine on Play internal; iOS is benched, so nothing to coordinate there now.
6. **TS rule logic outside the block stays:** `standing.ts:97-117` (page possession pick), `strip.ts:128` `troutLine`,
   `others.ts:33` `splitRows`. These are not this spec's work. Propose a follow-up ticket so the "no rule logic in TS" decision holds app-wide.
7. **Card height:** 13 of 43 mock cards are taller than one 640 px screen (round 11: Vedder 833, Lois 808). This is
   accepted design, not a build risk.
8. **Strip against row contradiction** (4th attack fault 2). It was fixed in design by "strip only when every row shares".
   How often it fires is unmeasured. J measures it.

9. **Fixes that live only in the old card.** T805 B, C and D were built into `card.py` / `tree.py` / `card_check.py`.
   If F re-derives them instead of lifting them, the table could lose them. Mitigation: F's named cases include
   Cunningham, Bowron, Lois, Khartoum, Windsor, Shamrock and Echoes, and J proves possession and the cross-row wild limit.

### Questions for the owner: all answered
1. **Other fish:** trout only. "we should just have the trout table under the trout entry like it currently is with
   the better text like 2 or 4 with expand arrow etc." (15:47)
2. **Possession line on the page:** "Keep the page line (Recommended)" (15:42).
3. **If the cutover does not fit:** "Raise the limit, shrink later (Recommended)" (15:42).

### Open items, not blockers
- **François Lake's outlet-area lines** are bound to the whole lake (T805 D). This is a placement fix with a rebuild
  and its own list. It was there before this work, and the owner has not been asked yet.
- From T810: where Lois's "All together" line sits (outside both boxes).
- The 8 rule sets with no stored headline (F).

### Questions for the chats
| Question | Who | Recommended |
|---|---|---|
| New section `trout` (4.1) or a sixth slot in the rows frame? | main | New section (build-behind needs it) |
| Can the owner preview on a side release in the staging test channel (`test/`) without promoting it? | main | Yes; it is staging and allowed |
| Do a11y labels come from Python or get joined in core? | app + main | Joined in core from Python texts, order only |
| Does the "Fished elsewhere" sheet move into `trout.elsewhere` or stay on `rows`? | main | Move (it is the trout block's); the other fish's own "elsewhere" lines stay on `rows` |
| ~~Do non-trout tables come from the same builder?~~ | n/a | Dropped: trout only |
| Does the folded Trout and char entry today show "Up to N" words, and where do they come from? | app (its check) | The table's `fold` feeds it at cutover |
| Is the picture pack a gate or a review? | app | A review (no pixel gate exists today) |

---

## 9. Not looked at
- 16:40 revision: I read the owner's answers (15:36 to 16:37), T805, T809, T810 and the T805 commit messages. I
  confirmed by grep that `possession_duty`, `possession_problems`, `across_rows`, `across_unsaid`, `A_BOTH_LAKES_SAID`,
  `A_SIZE_CLAUSE_RELEASES_AS_A_RULE` and `A_STEELHEAD_IS_OVER_50_EVERYWHERE` exist, and that `rows` is still 33 on both
  sides. I did not re-measure size and did not re-check the app half.
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
