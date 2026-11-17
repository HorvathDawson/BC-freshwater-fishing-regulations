# Trout table: build spec (from the Python pipeline to the screen)

Revised 2026-11-16 after the fresh review (trout-table-review-2026-11-16.md). Sections 1 to 5, 7 and 8 and the
pipeline tickets take in every data-side (D) finding; section 6 (the app chat's) is unchanged, and the app changes
that follow are listed in the contract's section 6, "For the app chat". The payload shape lives only in
`trout-table-contract.md`: this file's old section 4 is now a pointer to it, and the contract wins.

## Changes 2026-11-16, after the fresh review

1. **B1:** at the switch the regs files go `regs-region/1` → `regs-region/2` (ticket L), so the held set installs over
   the old set and `app_gate` derives `minAppVersion` from the format. The `app_reads` "sections" idea is dropped.
2. **B2:** new ticket **I0**, landing first: one test pins every answers section version, plus the optional `trout`,
   to the app's reader; `app_reader_versions` learns the app's optional list.
3. New ticket **F0** (page possession lead and the page row's kept lines, in rows 34); new ticket **V-py** (the shared
   fixture's Python half, the refresh script, the staging `side` scenario); A split into **A0** (now) and **A1**
   (after I and J; gates L).
4. J reuses the engine's basket proof over the decoded table, fails on an unparseable sentence, stamps a floor count,
   and checks possession against the page's lead. L re-aims the old tree checks at J, one mutation test each.
5. Week lines, the licence / stamp line and the steelhead stamp are composed in `compose_key` from post-pass values;
   F writes the hours-window trout words.
6. A reopening date may sit on a closed fish's row (Bish Creek); the year bar shows only when every fish shares one state (all release or all closed; owner 12:42); Q-D7
   notes are conditional; the fold arrow replaces "Details" on the salmon and trout folds only (owner, about 17:15).
7. The switch is small: additive parts land first behind the flag or the dev switch; the cutover merge is flag on,
   rows 34 + the format bump, re-cut fixture and goldens, and the deletions. Order of work: section 7.
8. **PENDING OWNER:** (1) ship the trout section in normal releases before the switch (recommended yes if A1 measures
   about 100 kB or less); (2) Q-D7 angler-only notes stay in the page's caveat block (recommended).

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
   and char". The reopening date is shown only while the water or fish is closed today: on the closed table's top
   line, or on a closed fish's row (Bish Creek's steelhead). (15:36: "yes but drop reopen unless it is currently
   closed then reopen date matters then")
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
it (or that one fish) is closed today. A change that only loosens a rule reads "rule changes <date>". Possession stays the page's own
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
- **PENDING OWNER, two questions:** (1) send the new table out in normal releases before the switch, so it is
  checked on every release and you can preview it on staging (recommended yes if it costs about 100 kB or less of
  download; the switch then becomes small); (2) the "closed to some anglers" notes stay in the page's warning block,
  not in the trout table (recommended).
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
| Other TS rule logic, not trout-only | `standing.ts:97-117` picks the page's possession rule; `strip.ts:128` `troutLine`; `others.ts:33` `splitRows` / `TROUTISH` [read] | Same class of problem | Now in scope where they fight the table: F0 + S (possession pick), S (`troutLine` dropped, `covers` split); section 8 risk 6 |
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
| Notes for some anglers | **Conditional on Q-D7 (PENDING OWNER).** Recommended: none in the table; they stay in the page's caveat block, where they show today. Only if the owner picks the table: a note in force today (flag 2) goes above the top line; one not in force today (flag 1) goes directly under it, above the table, never under the table (round 7 item 8), one line each with "⚠", and the caveat block skips them | the 34 angler-closure rules (Skeena/Babine weekends, youth waters) | yes (the page's caveats today, `caveats.ts`) [inferred: placement by flag needs the rule's in-force test per segment, derive] |
| Top line, plain | "Up to N today". N = the stored headline | `group.headline.n` | yes |
| Top line, "or" form | "Up to 2 today, or 5 if they're rainbow trout" when the headline has a stored second half that starts "or"/"plus" (87 waters) | `headline.also[].says` | yes |
| Top line, three numbers | "Up to 2 today, or 5 if they're rainbow trout and no more than 2 Dolly Varden/bull trout" (Madden-type, 30 waters; owner yes 12:42) | also + the bull trout box | derive |
| Top line, two boxes counted apart | "Up to 6 rainbow trout or steelhead and 4 other trout and char today" (426 answers) | `scope.apart`, `parent` | yes |
| Top line, "plus" | "Up to 2 today, plus 2 Dolly Varden/bull trout" (Kaslo, Keen) | over-region tier | **derive. Gap:** 39 answers / 8 keys have no stored headline (round 2 grammar; still open, ticket F). Ross Lake: headline 2 traced by T805 D, it is right |
| Top line, second half for a cross-box wild limit | "…today," then under it "only 2 wild trout or char, each under 40 cm (16 in)", in the same words as the "All together" line (Lois, Khartoum) | one wild quota across rows | **yes since T805 C** (`tree.across_rows`; in today's card as "no more than 2 wild trout and char in all, …"). F lifts it; the exact words are settled in G |
| Top line, no keeping | "Release every one until <date>" / "Closed for trout and char until <date>" / "Nothing to keep today". While closed today, the reopening day may be said ("reopens Nov 1", round 12's closed-day picture). The only other place a reopening date may appear is a fish closed today in an open table (its row or the Closed column date: Bish Creek's steelhead) (owner 15:36: "drop reopen unless it is currently closed then reopen date matters") | timeline | derive (ticket E) |
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
| Year line | Only when **every** trout/char fish shares one state today (**all release, or all closed**) **and** the water is open: "Trout and char through the year" bar (Elk in winter) (owner 12:42: "i like it on the elk where it says trout and char through the year when they are all release (or all closed etc) if some are open it seems silly"). The fresh review's "all-release only" read T810 item 9 (the fish sheet) too narrowly. Never on a closed day (owner 12:42) | per-fish year | derive (E) |
| Links | "Sources ›" (all rules) and "Fished elsewhere today? ›" | rule refs; `elsewhere` / `worked` | yes |
| Folded row (glance) | The existing Trout and char entry's short line, with its expand arrow (owner 15:47: "the better text like 2 or 4 with expand arrow"): "Up to 2", "Up to 4", and per top-line form "Up to 2 or 5", "Up to 6 rainbow, 4 others", "Release" / "Closed" with the date small, "None to keep". The exact forms go in G's sentence catalogue for the owner, beside the Other fish list's own grammar ("5 a day", TS `shortValue`), which stays. **The arrow:** it replaces "Details" on the salmon and trout folded entries; the Other fish list stays as it is (owner, about 17:15, "Salmon + trout folds") | top line | derive |

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
4. **Closed today:** a closed table's top line says when it opens again ("Closed for trout and char until Oct 31" or
   "reopens Nov 1"; the exact form is settled in G), and so may a fish closed today inside an open table (its row or
   the Closed column date: Bish Creek's steelhead). These are the only places a reopening date appears; a coming
   closure never carries one. E builds it, G words it, J checks the place and the day.
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
- **Screen reader:** each row reads as one label, joined from its Python texts **in payload order** (contract 2.8:
  name, origin, label, sub, lines, quiet, band text, date). Python writes no second composer; only the app's output
  is pinned, by an app golden. Buttons use ARIA props (AGENTS 34). Press targets are at least 44 px.

---

## 4. The data contract: see `trout-table-contract.md`

The shape, the examples, the size numbers and the version rules that stood here (4.1 to 4.7) went stale against the
contract (`Box.n`, the `label` enum, `w`, `notes`, the fold "5 a day") and were removed in the 2026-11-16 revision.
**The contract is the only statement of the payload, and it wins.** Where to look:

| Was | Now |
|---|---|
| 4.1 shape, tables | contract 2.1 (files, the optional-section rule) and 2.3 (the pydantic models; wire and decoded types, both emitted) |
| 4.2 to 4.4 examples (Skeena, Lois, Fraser R2 / R3) | the shared vectors (contract 2.8): built cases, not hand JSON; the same waters are named cases |
| 4.5 size, measured and estimated | contract 2.9 (the measured numbers are kept there; A0's estimate, A1's method) |
| 4.6 what runs where | contract 2.4 (who writes which word) and guarantees 1 to 5 |
| 4.7 versions | contract 2.1 and 2.7 (`trout` optional then required; rows 34; the `regs-region/2` format bump) |

---

## 5. Pipeline work (main chat)

| Ticket | Goal | Files | Tests (with mutation-style) | Depends on |
|---|---|---|---|---|
| **I0. Version and name pins** (new; lands first, now) | Review blocker B2. One test holding **every** `A.SECTIONS` version (ladder, rows, gear, licence, display) plus the optional `trout` equal to `app_reader_versions()`; extend `app_reader_versions` (`test_answers_tidal.py:113`) to parse the app's optional list (`OPTIONAL_VERSIONS`, contract 6 item 3) as well as `KNOWN_VERSIONS`, and to fail on a section present on one side only (`trout` held by a named allowance until M0 lists it, never by a skip). Emit `CellIds` from `contract._REGION_TABLES["cell"]` through `pack.py` | `pipeline/tests/` (the pin, beside `test_answers_tidal.py`), `pipeline/tools/emit/pack.py` | mutation: change one version on either side, drop a section from the app's line, add one to Python only → each fails | none. **Lands behind nothing:** tests and an emitted type, no release change |
| **A0. Size estimate and three counts** (now) | Prototype `TroutCodec` + a crude table on R6 and R5: estimate gz against the trout groups. Count (1) rows-frame rows mixing trout and non-trout members (Q-D4), (2) angler-only rules closing trout or char alone (Q-D7), (3) trout answers that change inside an hours window. A non-zero count amends the contract before F. Output: a dated table in the ticket | `pipeline/tools/audit/trout_size.py` (prototype mode), scratch | the counts re-run on the mini release with a planted case of each | none. Lands behind nothing (an audit tool) |
| **B. Possession on the fish row: DONE (T805 B, `994941826`, merged `39f46ef56`)** | Built in today's card: `card.possession_duty`, `card_check.possession_problems`, RULINGS P58; 3,166 named waters gain a line; Region 6 corpus edit (Bennett, Morley, Tagish) needs reach + bundle rebuild to reach a release. Bowron: lake trout keep Region 7's "1 day's limit" on their own row (owner "Keep today's"). Nation Lakes as printed. No more possession patching outside the redesign (owner 16:37). **Left for F:** lift `possession_duty` into the table, said only where it differs from the page's line. *Original scope:* Possession per fish (and per box) from every possession rule: Region 7 "one day's limit" (`possession_exceptions.r1/r2`, p.58/p.64), with a size clause (Cunningham p.59; Tagish p.53 size missing from `tagish_lake.r2`), below the daily number (Cunningham, Tchentlo, Tsayta, Indata, Nakinilerak), Bowron park members to include lake trout, and the 4th attack's Bowron artifact (possession only on lake-trout-release days: Istsi, Wolverine p1, Sootheran p1) | `rows/evaluate.py:243-268`, `card.py:221-253` (or the new module), the corpus for Tagish (A2 backup rule) | per case a whole-sentence test; mutation: drop the size clause → the check (J) fails; a possession larger than daily on a row → the build refuses | none. **Changes shipped answers** (about 14,200 R7 entries gain a line): owner list via K |
| **C. One wild limit across rows; char origin per C5: DONE (T805 C, `0f6fe2868`)** | Built: `tree.across_rows` / `across_unsaid` (the answers build stops on a card that leaves it out), `rows.evaluate.A_SIZE_CLAUSE_RELEASES_AS_A_RULE`; Lois, Khartoum, Windsor change, all stricter (owner go 16:09). **Left for F:** lift `across_rows` into `top.second` / `together`. *Original scope:* Store Lois/Khartoum's wild quota once across both rows (T805.1); make Dodd, Lois, Khartoum, Horseshoe, Nanton, Windsor, Ireland store the char the same way (C5); confirm the T793 rules reach a built release | `rows/card_model.py:60` `_origin2_rule`, `rows/conds.py` | a cross-row wild limit counted once (mutation: split it again → fails); the seven lakes give one answer | T793 merged and delivered. **Changes shipped answers** |
| **D. Small data lines: DONE (T805 D, `764fe73fc`)** | Built: Shamrock and Kilgard creeks keep no 20–30 cm steelhead (`A_STEELHEAD_IS_OVER_50_EVERYWHERE`, RULINGS S13); Echoes Lakes "from both lakes together" (`tree.A_BOTH_LAKES_SAID`, P59). Looked at, unchanged: Ross Lake's headline 2 is right; the cross-border line is on the rule list. **Left:** the 8 rule sets with no headline (moves to F); **François Lake's outlet-area lines are bound to the whole lake**: a placement fix needing a rebuild and its own list, owner not asked yet. It is a data item that was there before this work and does **not** block the build. *Original scope:* Shamrock Creek's "steelhead 20–30 cm" (book p.24: trout/char 20–30 cm, Jun 16–Jan 31); the cross-border quota (Atlin p.50, Bennett, Tagish, Teslin, Laidlaw, Swift, Rancheria) as a stated note in `notes`; Echoes Lakes "in the aggregate from both lakes" as `together`; trace Ross Lake's headline 2 against `real_daily` 4; the 8 rule sets with no headline; check François Lake's outlet char rule | corpus (A2), `rows/groups.py` | one test per water, by stream key / water through `under_test` | none. Shamrock changes an answer |
| **E. Per-fish timeline and the date rule** | For every (akey, segment, fish, row): the answer and the next dated change per 3.3 (first change; looser-only → "rule changes <day>"; tighter or state change → words; scope; **no reopening day on a coming closure**; a reopening day on a closed-today table **and on a closed-today fish's row** (Bish Creek, owner 15:36); one-day notes; weekday moments; year wrap; the year bar only for an all-release table) | new `answers/trout/timeline.py`; reads rows frames over all segments and the closure rule's species | over the release: never "until X, then release" when keepable the next day; never "until X" when not keepable after; no change left undated; "rule changes" never used for a tighter change; a reopening day only on something closed today; Bish Creek May 20 steelhead says its reopening; mutations: shift a date by a day, call a tighter change looser, mark a closure whole-water, add a reopening day to a coming closure → each fails | none (reads `produce` output). Lands as a module, not wired into a release |
| **G. Words module** (before F) | `answers/trout/words.py`: every sentence form the table, the dates and the sheets can say, with whole-sentence tests (AGENTS 33), **and** the grammar J parses them back with (one module states both). The reopening form is marked as such. F and H take every string from it and grow it as they go | `answers/trout/words.py` | whole strings only; a composer whose grammar J cannot parse fails a round-trip test | none. Lands as a module |
| **F0. The page's possession line** (new) | Contract 2.11. `card.page_possession` (`card.py:410-422`) is the one choice of the page's lead; F0 ships the lead (+ `src`) and the page row's kept also-lines (non-trout fish only after the switch) in `RowsFrame.page_possession`, **written only at rows 34** (inside the cutover bump) | `answers/card.py`, `answers/model/rows.py` (the rows-34 field, on the cutover branch) | pytest: over every frame of the mini release and the fixture, a fish possession line exists ⇔ it differs from the shipped lead (`possession_duty` and the lead use one rule); the kept also-lines name no covered fish | none. **Code and tests land now; the field lands in the cutover merge** |
| **F. Table builder (trout and char only)** | Boxes, rows, sort, bands ("up to the N", own, shared, possession), summed-tier detection, top-line forms, strip sentences, row lines, release/closed groups, `together`, `year` (all-release only), `fold`, rule refs; reuses `band_words`, `_duties`, `size_rule`, and T805's `possession_duty`, `across_rows`, `A_BOTH_LAKES_SAID`. **Two stages (contract 2.2):** a draft in the rows derive (needs `P` and `model`), finished in `compose_key` from **post-pass** values: weekday lines, the hours-window lines (new: the table says a trout answer that changes inside an hours window), and the steelhead `quiet` lines (`steelhead_line` and today's TS `quietOf` words). **Origin rule (owner 15:48, 16:00):** same rule for wild and hatchery → one row, no mention; keepable only as hatchery → one row with the label, no wild row under Release, and the label moves to the box when every fish of the box carries it; twin rows only where wild fish have a quota or size of their own. Possession only where it differs from F0's lead. The 8 rule sets with no stored headline (from D). **Angler notes: none** (Q-D7 recommended, PENDING OWNER; if he picks the table, F writes them). No non-trout tables (Q dropped). Every string from G | new `answers/trout/table.py`; lifts parts of `card.py`, `tree.py` (copies or shared helpers: the old card keeps them for the other fish) | the 43 mock waters as named cases (stream key + metre), plus Fraser Region 2 at Mission, Skeena (one hatchery-only steelhead row), Adams River (80 cm on the row); every top-line form; summed tier never drawn (mutation: draw it → J fails); the origin rule's three cases (mutations: emit a wild row under Release for a hatchery-only fish; drop the box label when every row has it; show an origin word where wild and hatchery are the same → each fails); a planted weekday split and an hours window reach the table | B, C, D (done), E, G, F0. Lands as a module |
| **H. Sheets and sources** | Per fish: header answer, "Here, today" lines, the licence / stamp line (composed in `compose_key` from the post-pass stamps, after `stamp_licence_documents` and `drop_closed_steelhead_stamp`), the coming-change sentence, rule refs ordered (this fish's first, up to 4); move the "Fished elsewhere" lines (`groups.py:285`) into `elsewhere` | `answers/trout/sheets.py` | whole strings; every rule ref resolves to a rule (mutation: a dangling ref → the build stops); a planted stamp drop leaves the sheet without the stamp | F. Lands as a module |
| **G2. Sentence catalogue** (after F and H) | Emit every distinct sentence the tables, dates and sheets say, with counts and example waters, for the owner: the folded entry's short line, "rule changes <day>", the reopening forms (closed table, closed fish), the hours-window and weekday lines; and the two pairs that sit on one screen: the fold grammar (`fold.value` "Up to 5") beside the Other fish list's TS `shortValue` ("5 a day", `others.ts:63`), and the table's card line beside the bar's "Closed from …" (`strip.ts:137-174`). When the builder replaces the `drafts`, any changed word is in the catalogue diff | `pipeline/tools/audit/trout_words.py` | whole strings; a word not from G's module fails the tool | F, H. Lands behind nothing (an audit tool); the owner reads it |
| **I. Section and codec** | `Section("trout", 1, …)` behind `--trout-table` (off by default) with the new `compose_key` hook, fed the **post-pass** `rows` and `licence` values; `TroutCodec` (bodies, date sets, sheets, frames interned); `cell.trout`; `contract.OPTIONAL_SECTION_TABLES` (renamed from `OPTIONAL_SECTIONS`, apart from `contract.OPTIONAL`) emitted by `pack.py`; pydantic models in `answers/model/` + generated TS, **wire and decoded both** (`TroutFrameWire`, `TroutTableWire`, `BoxWire`, `TRowWire`, `TroutFrame`, `TRow`); the hand lists of region table names derived from one registry or held to `_REGION_TABLES` by a test (`store/build.py:37-43` and `:282`, `store/common.py:58-62`, `release/shards.py:22-26`, `release/slices.py:49-52`); `regs_fixture.py` and the mini release built with the flag. **Rule:** any shape change bumps the `trout` version (I0 then fails until the app follows) | `answers.py`, `encode.py`, `release/contract.py`, `answers/model/`, `store/`, `release/shards.py`, `release/slices.py`, `release/regs_fixture.py`, `tools/mini_release.py`, `tools/emit/pack.py` | round-trip encode/decode; frame refs in range; the all-or-nothing section in `test_release.py`; NULL ⇔ Q-D3 over every cell; adjacent equal segments give equal frames unless dates differ; `test_mini_release.py`; a table added to one hand list only → a test fails | A0, F, H, I0. **Lands behind the flag** (off: releases unchanged) |
| **V-py. Shared fixture, Python half** (new) | Contract 2.8: the `trout_vectors` emitter (registered for `python -m pipeline.tools.emit --check`), `trout-hand.json`, the `drafts` section, `regs_fixture --item` read from `trout_vectors.CASES`, **one refresh script** (side release → `regs_fixture` → `trout_vectors` → golden sync `sync.mjs`, failing on any stale file), and the staging `side <dir>` scenario in `deploy/staging/test-updates.mjs` (checks first that a `test/` manifest can point at `test/release/<id>/`) | `pipeline/tools/emit/trout_vectors.py`, `app/fixtures/trout/`, `release/regs_fixture.py`, a script under `scripts/`, `deploy/staging/test-updates.mjs` | `--check` green; each `trout-hand.json` case refused by the model; the refresh script run twice is a no-op | I. **Lands behind the flag** (side releases and the committed fixture only) |
| **J. The proof (permanent build gate)** | Contract 2.10. For every (akey, segment, moment): read the law **from the decoded table alone** (a separate decoder: per fish, origin and length bucket the count allowed, closed or release, possession; a HATCHERY ONLY label decodes as "wild: release", a row with no origin as "same for both"), with G's grammar; **an unparseable sentence fails the build**. Joint limits (box total, shared bands, `together`, possession below the day's number) through **`engine.Engine`'s basket proof, reused** as `card_check.check_caps` uses it (`card_check.py:630-660`), against the engine (`rows.fish` decided + `verdicts.sqlite`), never the builder's inputs. Possession line ⇔ differs from F0's lead. Every date claim against the following segments (looser-only "rule changes", worded tighter changes, reopening only where closed today). The stamp reports tables checked ≥ non-NULL `cell.trout` (a floor). Zero unexplained differences, or the answers build stops | `answers/trout/check.py`; hooked where `card_check` runs | a mutation test per check: one number, one size edge, one origin, one date, one scope, one possession line, a shared band's span, an unparseable sentence → each fails; the decoder imports nothing from the builder; a J that checks nothing fails the floor | E, F0, F, G, I. **Runs whenever the flag is on** (mini release in fast-fail, side releases; every release if decision 1 is yes) |
| **A1. Size, measured on the cutover form** (after I and J) | Contract 2.9's method on the real cutover form: every `WEB_BUDGET` moment at HEAD, the `water-shared` zdicts, the ten largest bodies and sheets, and the side release's cost for decision 1 | `pipeline/tools/audit/trout_size.py`, run through `scripts/heavy.sh` | the script's own check on the mini release | I, J. **Gates L**, and (decision 1 yes) the first normal release with the section |
| **K. Changed-answers list: DONE (T809, merged `dac013480`)** | `python -m pipeline.tools.audit.answers_diff <A> <B> [--out DIR]` → `diff.json`, `diff.txt`, `owner.md`. **Left on T809, needed before the cutover diff (section 7 point 2):** learn rows 34, so a trout row's dropped `group` (and the new `page_possession`) is not read as a change; a cross-atlas pair never run; the regs side proven only on the mini release; possession and gear-count directions read "unclear"; card rows and shared limits fall into the catch-all. *Original scope:* Diff two releases (or two builds) per water and day range in plain words, grouped by kind of change, with counts and water names; output one page for the owner | `pipeline/tools/audit/answers_diff.py` | a built pair with a known planted difference; an identical pair → empty | none. **Build first:** B, C and D need it |
| **L. Cutover, pipeline half** (in the cutover merge, with S) | Flag on for good (removed); `rows` 34: `group` and `elsewhere` dropped on covered trout rows, `_pool_rows` re-stated (contract Q-D9), F0's `page_possession` written; `trout` required (into `_REGION_TABLES` and `cell`; `OPTIONAL_SECTION_TABLES` empty again; the app moves it into `SECTION_NAMES` and `KNOWN_VERSIONS` in S); **`regs-region/1` → `regs-region/2`** for every `regs-{r}` in `contract.py` FILES (`:142`), so the held set installs over the old set and `app_gate` derives `minAppVersion` from the format (no hand stamp; the `app_reads` "sections" idea is dropped); **re-aim** `origin_unsaid`, `across_unsaid`, `card_check.check_tree` and `possession_problems` **at J, with a mutation test per check** (nothing deleted without its re-aimed twin); delete the tree's trout-only wording paths. The group tree, the non-trout checks and `GroupCard`'s data stay for the other fish (owner 15:47). First-tap limit raised by A1's measure plus 10% if over (owner 15:42); re-cut fixture and goldens through V-py's refresh script | `answers.py`, `card.py`, `tree.py`, `card_check.py`, `release/contract.py`, `answers/model/rows.py`, tests | the full answers suite; size gates; I0's pin with rows 34 and `trout` required; `app_gate` gives the S-built app as the oldest reader; a test that no covered trout row carries `group` and every non-trout row that had one still does; each re-aimed check fires on a planted case | J, A1, K (rows 34), F0; the app's S ready to land in the same merge |

**Order of work: section 7 (the step list, with owner and what each lands behind).** K, B, C, D are done. François
Lake's outlet fix and T809's other leftovers run beside the build on their own tickets and do not block it.

---

## 6. App work (app chat)

Written by the app chat, 2026-11-16 16:57, from its worker's check of the app code (file:line in the full report;
the report is kept beside this spec as `trout-table-app-check-2026-11-16.md`: sections 2–6 the app today, 9 risks and
spec errors, 10 the draft screen half of the contract (now in `trout-table-contract.md`), 11 the main chat's three
questions, 12 every place today's trout card decides in TypeScript). It replaces the 15:40 table.

**Owner, 2026-11-16 ~16:55 (app chat dialog, verbatim labels):** fold row "All rows get arrow" (every folded row,
other fish too, shows its words and an expand arrow: O changes `KeepBlocks` fold rows for all fish, not only trout);
pinned top line "Drop it (Recommended)" (S removes the trout pin line); a lone other fish beside trout "Stays as today
(Recommended)"; the year bar's "Trout: release until …" line "Drop it (Recommended)" (S removes `troutLine`).

**The rule for every ticket below (owner 16:50, via the main chat):** Python decides every word, number, date and
which rows exist; the app draws only; payload types are generated, never hand-written; one fixture; one trout path
after the switch. The one app-side phrase is the day words ("today" / "on Aug 15"), because the page shows any day.

Letters kept where they fit. Q is dropped (owner 15:47). Sizes are worker sessions. Proof column: U = unit, G = golden
/ fixture, B = Android bench / test bed, P = picture pack.

| Ticket | Scope | Files | Needs (pipeline) | Size | Proof |
|---|---|---|---|---|---|
| **M0. Optional section gate** (new, first) | An `OPTIONAL_SECTIONS = {trout: 1}`: absent = fine, present at another version = refused; manifest check the same. `cell` read adds `trout` to the SELECT only when the layout lists the section (else the column does not exist) | `core/src/regs/versions.ts`, `data/src/regs/store.ts:116-122`, `reads.ts` keyCells | none | 0.25 | U: store.test (absent ok, wrong version refused, present ok) on a hand-made sqlite |
| **M. Read the section** | `trout_table` / `trout_dates` / `trout_frame` families; today's cell only into `RegsInputs.trout`; generated types | `reads.ts`, `inputs.ts`, `core/src/regs/inputs.ts`, `answers.generated.ts` (emitted) | I (format, codec, pydantic models, fixture release with the flag) | 0.75 | U: reads.test on the fixture release; G: `inputs.release.test` |
| **V. Shared vectors, app half** (new) | App tests read the committed vectors file (section 10.6); a loader + `--check` wiring in fast-fail | `app/fixtures/trout/` reader, `scripts/fast-fail.sh` line | V-py: `python -m pipeline.tools.emit.trout_vectors` | 0.25 | the vectors themselves |
| **N. View model** | `troutOf(inputs, day)` → `TroutVM`: shape checks (bands partition, refs resolve, date keys valid) refusing with `reportFailure`; day words; styles; a11y labels; fold row VM; `KeepVM.trout` + a `"trout"` block in `keepOrder` | new `core/src/regs/trout.ts`, `model.ts`, `keep.ts`, `glance.ts` | vectors | 1 | U: whole strings per vector; mutation: a broken partition / dangling ref is refused; no rule logic (imports no `rows`/`group` types) |
| **O. The table** | `TroutCard`: kicker + hatchery link, notes, top line, card line, keep boxes (band groups: each band = its rows beside one cell, so a shared white box spans its rows without a grid), filled HATCHERY ONLY label (row and box), release / closed group, all-together line, year line, links; two text sizes; tokens light/dark/cvd; large-text fold; 44 px targets; one fold row in `KeepTab` replacing group folds and trout plain rows; dev-only switch | new `ui-native/src/regs/TroutCard.tsx` (+ 2–3 parts), `KeepTab.tsx`, `KeepBlocks.tsx` (fold value/arrow), `theme.ts` + token source, `type.ts` | vectors | 2 to 2.5 | U: render each vector; a11y.test (labels, ARIA, 2× fold, no sideways at 360/390); palette/cvd/contrast tests; P (web export on the vectors) |
| **P. Fish sheet** | New sheet key (e.g. `tf:<row key>`, parse + golden bridge), core builds it from existing blocks (header answer, Here today, How to tell it from `FISH_TIPS` + both DV pictures + hatch link, Sources: this fish's rules first, then all); opened from a row press | `core/src/regs/sheetKey.ts`, new `sheets/trout.ts`, `sheets.ts`, small `SheetBody` addition if a header block is needed | vectors (sheet texts) | 1 | U: every fish of every vector opens; SheetHost test; gesture tests already exist |
| **T. Website text** (new) | `seo/model.ts` sections from the trout VM; `verify.ts`; check that the server render with the fold closed still carries the words | `tools/seo/model.ts`, `site.test.ts`, `t670.web.release.test.ts` | real section in a release (I + a side release) | 0.5 | U + G: site tests on the fixture release |
| **R. Picture pack and test bed** | ~45 waters (the mock's + Fraser R2/R3) at 390/360, light/dark/cvd, 100/200%, from a side release; Maestro: open the fold and a sheet on a fresh install; re-shoot `18-regs-trout` | `tools/store-shots*`, testbed scenarios, `regs-walk.mjs`, `regs-web/rows.mjs` | a side release with the section (I…J) | 1 to 1.5 (+ heavy lanes) | B + P |
| **S. Switch, app half** | Page reads only the table for trout; `rows: 34`, `trout: 1` required; remove the trout branch of `keepOf`, trout `looseRows`, trout row sheets, `troutLine` (Q4), group pin lines (Q2), the dev switch; re-cut fixtures (`sync.mjs`); retire/re-pin tests (section 3); t670 K3–K5; keep.props trout part; `app/MAP.md` | as listed in section 3 | L, J green; owner sign-off on R | 1.5 to 2 | `pnpm check`, conformance, full tier once; B (APK on the bench) |
| **S2. Dead-branch cleanup** (optional) | Coverage over every other-fish row; delete composer branches only trout reached | `grouphead.ts`, `tree.ts`, `tail.ts` | S | 0.5 | U |

**Total: about 8 to 11 sessions** (M0 0.25, M 0.75, V 0.25, N 1, O 2–2.5, P 1, T 0.5, R 1–1.5, S 1.5–2, S2 0–0.5).
Confidence medium: the layout (O) and the switch (S) carry the spread. Not counted: design changes after the vectors
are frozen.

Dependency picture: M0 → (V → N → O, P) can run now on vectors; M, T, R need the pipeline's section (I) and a side
release; S needs L and J.

**Corrections to the 15:40 app half** (details in the check, section 9): M as written would make the app refuse every
release without the section (hence M0 first); "Up to 5 today" cannot be written by Python (day words are core's);
S deletes about 300 lines, not about 1,700 (the group composers stay for the other fish); the website text (T), the pin
bar, the browser tools and store-shot checks were missing; search and Methods/Gear need no change; the sheet already
closes on a flick or past a third of its height, not "over 90 px"; one fold replaces today's group folds AND the trout
plain rows (about half of today's water-days show release-only or closed trout as full lines; the owner should see the
Elk in winter in R).

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
   including their `group` (trout only). K first learns rows 34 (a dropped trout `group` and the new
   `page_possession` are not changes), and T809's leftovers (cross-atlas pair, regs side on a real release, the
   "unclear" possession direction) are closed before this run.
3. **What the owner sees, in order:** ~~B/C/D changed-answer lists~~ (done: C 16:09, B and D 16:37); the two
   PENDING OWNER questions (decision 1 with A1's numbers beside it; Q-D7); the sentence catalogue (G2); the picture
   pack (R); the cutover changed-answers list (expected empty). Each goes into OWNER-TODO and the owner to-do page
   with pictures (owner rules).
4. **Landing order, branch never red.** Every additive part lands first, behind the flag (pipeline) or the dev
   switch (app), or behind nothing when it changes no release. **The cutover merge is small:** flag on, rows 34 and
   the `regs-region/2` bump, the re-cut fixture and goldens (V-py's refresh script), and the deletions. A version
   mismatch can no longer land alone: I0's pin fails if the app's `KNOWN_VERSIONS` and Python's sections differ, and
   the format bump means an app that cannot read the files is never offered them.

**The order of work** (data = main chat, app = app chat). Steps on one line can run in parallel.

| Step | Ticket | Owner | Lands behind | Needs |
|---|---|---|---|---|
| 1 | **I0** version and name pins | data | nothing (tests + an emitted type) | none |
| 1 | **A0** size estimate + three counts | data | nothing (audit tool); a non-zero count amends the contract before F | none |
| 1 | **M0** optional section gate | app | nothing (absent = fine; older releases unchanged) | I0's agreed `OPTIONAL_VERSIONS` line |
| 2 | **E** per-fish timeline | data | nothing (module, not wired) | none |
| 2 | **G** words module (+ the grammar J parses) | data | nothing (module) | none |
| 2 | **F0** page possession lead, code and tests | data | nothing now; its field is written only at rows 34 (cutover) | none |
| 3 | **F** table builder | data | nothing (module) | E, G, F0, A0's counts |
| 3 | **V, N, O, P** (on drafts / hand vectors) | app | the dev switch | M0; the contract |
| 4 | **H** sheets and sources | data | nothing (module) | F |
| 5 | **I** section and codec, `compose_key` | data | the `--trout-table` flag (off) | A0, F, H, I0 |
| 6 | **V-py** vectors, hand cases, refresh script, staging `side` scenario | data | the flag (side releases, committed fixture) | I |
| 6 | **M** read the section | app | reads only when the layout lists it; drawn only behind the dev switch | I, the fixture release |
| 7 | **J** the proof | data | runs whenever the flag is on (mini release in fast-fail, side releases) | E, F0, F, G, I |
| 7 | **G2** sentence catalogue → owner | data | nothing (audit tool) | F, H |
| 7 | **N, O, P** re-run on built vectors; the caveats (b) skip and the `covers` split | app | the dev switch | V-py |
| 8 | **A1** size on the cutover form → decision 1 | data | nothing (audit tool) | I, J |
| 8 | **T** website text, **R** picture pack (staging `side` scenario) | app | the dev switch / a side release | V-py, J green |
| 8 | **K** learns rows 34 (+ T809's leftovers) | data | nothing (audit tool) | none |
| 9 | (decision 1 yes only) flag on in normal releases, first-tap limit raised by A1 + 10% | data | the section is optional and `regs-region/1`; older apps ignore it | A1, J green on a full build |
| 10 | **Cutover merge: L + S together** | data + app | one merge: flag on, rows 34 (+ F0's field), `trout` required, `regs-region/2`, re-cut fixture and goldens, re-aimed checks, deletions, dev switch gone | A1, J, K rows 34, owner sign-off on G2 and R |
| 11 | K's cutover diff (expected empty), fast-fail, staging release (`minAppVersion` derived), APK on the benches, Play internal | data + app | n/a | step 10 |
| 12 | **S2** dead-branch cleanup; MAP and AGENTS notes | app (+ data docs) | nothing | step 11 |

5. **Deleted after:** the **trout** use of the group tree and its trout-only wording paths, the trout path of the TS
   composers, their tests and fixtures, the flag, and the dev switch. The group tree, `card_check`'s tree checks and
   `GroupCard` stay for the other fish (owner 15:47); the old checks that walked trout groups are re-aimed at J, never
   only deleted. Then `pipeline/docs/MAP.md`, `app/MAP.md` and AGENTS rule 53's note on the guide get updated.

---

## 8. Risks and open questions

### Risks, ranked
1. **Size [ran].** First tap has 1,243 B of room on r-46e4355a7277 (contract 2.9). The trout groups the cutover frees
   are 110.5 kB of regs-6 as stored (app check 11b); the section's size is unknown until A1. Carrying both before the
   switch needs a temporary limit raise: that is decision 1 (**PENDING OWNER**, recommended yes if A1 measures about
   100 kB or less). Mitigation: A0's estimate first, A1 on the real cutover form before L; if it does not fit,
   **raise the limit and shrink later** (owner 15:42, answered).
2. **False date lines.** The looser/tighter order across counts, sizes, origins and possession is the hardest logic. A
   mistake reads as a confident false "until", or as "rule changes" on a day that tightens. Mitigation: J's date check
   over every segment, mutation tests, and the reviewer's simulation (1.9 million fish-days, never wrong) as a target.
3. **Two card paths by design.** GroupCard keeps drawing 3,320 non-trout rows in the Other fish sheet [ran]. The owner
   chose trout only (15:47). The risk is that L or S deletes a helper the other fish still use, or that the trout
   builder and the old card drift apart on shared helpers (`possession_duty`, `across_rows`, `size_rule`). Mitigation:
   the L test that non-trout `group` is unchanged, and the cutover diff (section 7 point 2) on non-trout rows.
4. **Data fixes changed many answers: done.** T805 B, C, D were shown to the owner as grouped lists and approved. They
   reach a release at the next build. Part B's Region 6 corpus edit needs the reach and bundle rebuild.
5. **The cutover forces an app update** (rows 34 and `regs-region/2`). The format bump makes it safe: an older app is
   never offered files it cannot read, and the S-built app never opens its held rows-33 set (contract 2.7, review
   B1). Fine on Play internal; iOS is benched, so nothing to coordinate there now.
6. **TS rule logic outside the block.** Now in scope, because they fight the table: the page possession pick
   (`standing.ts:97-117`, F0 + S), `troutLine` (dropped by the owner, S), `splitRows` / `TROUTISH` (`covers`, S), the
   timed caveat (b) for trout (S skips covered fish; F says hours-window and weekday words). What stays (other fish,
   whole-water lines, `sayOnce`) is the follow-up ticket, so the "no rule logic in TS" decision holds app-wide.
10. **A check that quietly checks nothing.** The tree checks skip rows with no group (`tree.py:141-146`,
    `card.py:406`); after rows 34 they would skip every trout row. Mitigation: L re-aims each at J with a mutation
    test, and J's stamp floor (tables checked ≥ non-NULL `cell.trout`).
11. **The fixture drifting from the builder.** `--check` proves vectors = fixture, not = builder. Mitigation: V-py's one
    refresh script, and the mini release running the builder and J with the flag on in fast-fail.
7. **Card height:** 13 of 43 mock cards are taller than one 640 px screen (round 11: Vedder 833, Lois 808). This is
   accepted design, not a build risk.
8. **Strip against row contradiction** (4th attack fault 2). It was fixed in design by "strip only when every row shares".
   How often it fires is unmeasured. J measures it.

9. **Fixes that live only in the old card.** T805 B, C and D were built into `card.py` / `tree.py` / `card_check.py`.
   If F re-derives them instead of lifting them, the table could lose them. Mitigation: F's named cases include
   Cunningham, Bowron, Lois, Khartoum, Windsor, Shamrock and Echoes, and J proves possession and the cross-row wild limit.

### Questions for the owner
Answered:
1. **Other fish:** trout only. "we should just have the trout table under the trout entry like it currently is with
   the better text like 2 or 4 with expand arrow etc." (15:47)
2. **Possession line on the page:** "Keep the page line (Recommended)" (15:42).
3. **If the cutover does not fit:** "Raise the limit, shrink later (Recommended)" (15:42).
4. **Reopening date on a closed fish's row** (Bish Creek steelhead): yes, from his 15:36 words ("drop reopen unless it
   is currently closed then reopen date matters").
5. **Year bar:** only when every fish shares one state (all release or all closed), owner 12:42.
6. **The fold arrow:** replaces "Details" on the salmon and trout folded entries; the Other fish list stays as it is
   (about 17:15, "Salmon + trout folds").

**PENDING OWNER** (the plan is written with the recommended option):
7. **Ship the trout section in normal releases before the switch?** Recommended yes if A1 measures about 100 kB or
   less. How the plan changes either way: contract 2.1.
8. **Q-D7, angler-only notes:** recommended, they stay in the page's caveat block and the table carries none.

### Open items, not blockers
- **François Lake's outlet-area lines** are bound to the whole lake (T805 D). This is a placement fix with a rebuild
  and its own list. It was there before this work, and the owner has not been asked yet.
- From T810: where Lois's "All together" line sits (outside both boxes).
- The 8 rule sets with no stored headline (F).

### Questions for the chats
| Question | Who | Recommended |
|---|---|---|
| New section `trout` (contract 2.1) or a sixth slot in the rows frame? | main | Decided: new section (build-behind needs it) |
| Can the owner preview on a side release in the staging test channel (`test/`) without promoting it? | main | Yes; it is staging and allowed |
| Do a11y labels come from Python or get joined in core? | app + main | Decided: joined in core, in payload order (contract 2.8); pinned on the TS output only |
| Does the "Fished elsewhere" sheet move into `trout.elsewhere` or stay on `rows`? | main | Move (it is the trout block's); the other fish's own "elsewhere" lines stay on `rows` |
| ~~Do non-trout tables come from the same builder?~~ | n/a | Dropped: trout only |
| Does the folded Trout and char entry today show "Up to N" words, and where do they come from? | app (its check) | Answered by the app check (11a): no value today; the table's `fold` feeds it at cutover |
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
- Size of the new section: **estimated, not measured** (4.5). Tickets A0 (estimate) and A1 (on the cutover form, after I and J) measure it.
