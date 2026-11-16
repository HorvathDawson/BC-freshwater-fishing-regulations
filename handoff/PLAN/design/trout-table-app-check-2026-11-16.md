# Trout table: the app half, checked against the code

2026-11-16 16:44 (clock read). Written by an Opus worker for the app chat and the main chat. Read-only: nothing in the
repo was changed. Checks `handoff/PLAN/design/trout-table-build-spec-2026-11-16.md` (the spec) section 6 (tickets M to S)
against the app code on `redesign/stream-sections` @ 7c413b629, with release `r-46e4355a7277` (LATEST).

Tags: **[read]** I read the code or file; **[ran]** I ran it; **[inferred]** my reasoning, not checked.

---

## 1. For the owner (one page)

**What the app has to do.** The data side will write every word of the new trout table. The app's job is to draw it:
the shaded keep box, the white boxes for tighter limits, the rows with fish pictures, the two text sizes, the fish
sheet, dark mode, large text and the colour-blind look. The app does not work out any rule.

**How big.** About 8 to 11 worker sessions on the app side. That is close to the first estimate (9 to 12), for two
reasons that cancel out. Some work goes away: other fish keep their present look (your 15:47 answer), so there is
no redraw for them. Some work is new: the website's text, the pinned line at the top of the page, and the test tools
also read today's trout card, and the first estimate missed them.

**What is easier than expected.**
- The fish sheet already closes with a swipe down, the back button, Escape or a tap outside. Its pictures, "how to
  tell it" lists and source lists already exist. The new sheet is mostly new words in existing parts.
- The search results and the Methods and Gear tabs do not show trout limits. They need no change.
- The download size works out better than first thought. I measured the real file: the trout part of the old card
  is 110 kB of the biggest region file (a quarter of it), twice the first estimate. Keeping the old card for other
  fish costs only 12 kB of that. The new table's size is still a guess, and the same measuring method probably makes
  it bigger than first guessed too, but it should still fit in what the old card frees.
- Today the folded trout row says only "Trout and char" and "Details", with no number. Some waters show two folded
  rows, and release-only or closed waters show no folded row at all. Section 11 has the exact words on real waters.

**What the first plan got wrong (found in the code).**
1. The app would have refused every normal data update while the new table is being built. The app must treat the
   new table as optional until the switch. This is a small fix, but it has to be the very first step.
2. "Up to 5 today" cannot be written by the data side as is. The app can show any day you pick, so the data side
   must leave "today" out and the app adds the right day.
3. Little old code can be deleted, because other fish still use it. About 300 lines go, not the 1,200 the first plan
   counted. This is expected, now that other fish stay as they are.
4. Today, on about half the waters and days, trout that are release-only or closed show as plain lines, not as a
   card. With the new design they move into the table under the folded trout row. This is a visible change on
   many waters (the Elk in winter, for example). It follows your design, but you should know about it.

**What can start now, before the data side is ready.** The "optional table" step. The new colours. The table and
the fish sheet, drawn against a shared file of about 45 sample tables that the data side and the app agree on.
The sample file is the contract between the two halves.

**What must wait.** Real tables for every water, the picture pack of real waters, the website text, and the switch.

**Main risks.** (1) The design keeps changing after the data side has written its words. (2) Two drawing paths live
side by side for weeks. (3) The table's layout must work at 360 px wide, at double text size, and on the web.

**Your decisions, short.** Four small ones are in section 9: the fold row's arrow, the pinned top line, where a lone
other fish goes, and the year bar's trout line.

---

## 2. Today's trout path, end to end

### 2.1 Read (data layer)
| Step | Where | Notes |
|---|---|---|
| Format and version gate | `app/packages/data/src/regs/store.ts:105-122` [read] | Refuses a release whose `meta.layout.versions` differs from `KNOWN_VERSIONS` **for every name in `SECTION_NAMES`**, including an absent one (`versions[n] ?? "absent"`); then the same against the manifest |
| Versions | `app/packages/core/src/regs/versions.ts` (32 lines) [read] | `SECTION_NAMES = [ladder, rows, gear, licence, display]`, `KNOWN_VERSIONS = {…rows: 33…}` |
| Cell read | `data/src/regs/reads.ts` (648 lines) `keyCells` ~`:297`: `SELECT akey, s, ladder, rows, gear, licence, display FROM cell` [read] | Named columns. A `trout` column added to this SELECT fails with "no such column" on every release without it [inferred from SQL] |
| Frame families | `reads.ts` `familyOf` / `blob` ~`:270-290` [read] | Generic: one table per family, blobs through the water file's `zdict` (`store.ts:126-130`) |
| Inputs | `data/src/regs/inputs.ts` (155 lines) `keyed` `:46` [read] | Reads the rows frame of **every segment** (`rowsBySeg`) for the year; the new table needs today's cell only |

### 2.2 View model (core, no React)
| Step | File (lines) | Main functions |
|---|---|---|
| Page | `core/src/regs/page.ts` (102) | `buildRegsPage` → `buildModel` (card.ts, 194), `keepOf`, gear, methods, licence, `sheet(key)` |
| Keep tab | `keep.ts` (141) | `keepOf`: open day → for each row of `splitRows(card).main`: `group.headline` → `groupOf` (GroupVM), else `isPlainRow` → `plainRowOf`, else dropped (shown in the compact list, `looseRows`) |
| Trout/other split | `others.ts` (127) | `TROUTISH`, `splitRows` (trout rows first; **if fewer than 2 other rows, every row is drawn in full**), `looseRows`, `otherFishOf` |
| Group head | `grouphead.ts` (252) | `headCtx`, `groupHeadOf`, `rowTitle`, `scopeOf`, `parentRow`, `drawnRows`, `shortOf` |
| Tree | `tree.ts` (290) | `treeOf`, `pictures`, `fishNames`, `stampLine`; regex over band words (`sizeOf`), `liftCounts` |
| Tail | `tail.ts` (95) | `notKeptOf`, `workedOf`, `quietOf`, `moreOf` |
| Plain rows | `plainrow.ts` (217) | `plainRowOf`, `yearWindowOf` (day maths over the stored year) |
| Fold row | `core/src/glance.ts` (82) | `groupGlance`: title + picture, **value null** (owner 2026-10-28: "a folded row says NO count") |
| Pin bar | `pins.ts` (179) | `groupLine`, `pinChain`: the sticky "Trout and char · 4 a day …" line and its breadcrumb |
| Year strip | `strip.ts` (179) `troutLine` `:128` | "Trout: release until …" on the game-fish bar when no trout row is keepable |
| Sheets | `sheets.ts` (296), `sheets/fish.ts` (38), `sheets/sources.ts` (90), `sheets/other.ts` (129) | `sp:` fish sheet (pictures + `FISH_TIPS`), `src:rN` a row's sources, `share:rN`, `multi:rN` ("Fishing more than one water today?"), `other:oN` (an other fish's article: **a `group` block drawn with GroupCard**) |
| Words | `words.ts` (541) | the TS sentence pieces |

### 2.3 Drawing (ui-native; the phone and the web are the same components through react-native-web)
| Component | File (lines) | Notes |
|---|---|---|
| Keep tab | `ui-native/src/regs/KeepTab.tsx` (124) | Under `glance` (always on in the app: `shell/WaterModeHost.tsx:89` [read]) each trout group is a fold row (`groupGlance`) opening a `GroupCard`; trout plain rows are drawn **outside** the fold as `PlainRow` |
| Group card | `GroupCard.tsx` (293) | head, origin, tiers/kinds/size chips, not-kept, worked-examples fold, pictures (`Pictures`), anchors for the pin bar |
| Plain row | `PlainRow.tsx` (145) | release-only / closed rows, and any other fish drawn in full |
| Fold rows, other fish | `KeepBlocks.tsx` (369) `FoldRow` `:250`, `SpeciesRows`, `OtherFish` | "Details" / "Hide" with an expand mark; a `value` slot exists (salmon uses it) |
| Pin bar | `PinBar.tsx` (83), `pinStore.ts` (100), `useAnchors.ts` (175), `RegsWide.tsx` (70) | Reads `GroupVM.pinLine` and tier anchors |
| Sheets | `SheetHost.tsx` (53), `SheetBody.tsx` (102), `sheet/*` | One `Sheet`: scrim, Done, drag down from the handle (`useSheetDrag`), back/Escape (`useBackLayer`) [read] |
| The web site | `app/tools/seo/model.ts` `sectionsOf` `:100-135` [read] | The crawler text of a water page is built from `keep.groups` (head, tree, not-kept) and `keep.plainRows` |

Rough size of today's trout-facing code: about 3,150 lines across the files above [ran wc]; most of it also serves
other fish.

### 2.4 Measured shapes in the release [ran, `scratchpad/m/m3.py` over every `rows_frame` of r-46e4355a7277]
- 4,528 rows frames (9,572 cells). Every frame but one empty one has a trout row.
- **2,202 frames have trout rows but no trout card**: all their trout rows are plain (release/closed) or loose lines.
  Today they show as plain rows or compact-list lines, not inside a fold.
- 2,282 frames have one trout card, **43 have two** (Kootenay-type), 456 mix a trout card with trout plain rows.
- **1,616 frames (2,929 cells) have trout rows plus exactly one other fish, always a plain row**: because of the
  "fewer than two others" rule, that one other fish is drawn in full in the main block today, beside the trout.

---

## 3. What the new table needs, what the app then does, what goes away

**Needs from the release (per the spec, unchanged):** one table per (answer key, segment): boxes, rows, bands, notes,
"all together", year line (all-one-state only), fold words, "elsewhere" lines, per-fish sheet texts, and a separate
set of coming-change texts; every word written by Python, rule refs as rule indexes. Section 10 is the app's draft
of this contract.

**What the app then does: lay out only, plus three small jobs [inferred, from the code]:**
1. Pick the cell for the day (`segmentIndex`, as today) and read one table + one date set (not the whole year).
2. Add the day words ("today" / "on Jul 15") in core: the page can show any day (the date pill, `strip.ts` uses
   `WORDS.TODAY_LC` vs `WORDS.ON_DAY`) [read]. See section 8.
3. Check the table's shape and refuse a bad one (bands must cover each box's rows exactly once, every rule ref must
   exist, every date key must point at a box/row that exists) through `reportFailure`, never a silent fallback
   (AGENTS W46). Then: styles from `now` / `next` / `quiet`, a11y labels joined in order, the large-text fold.

**What goes away in core when the data side writes the words** (owner: trout only, other fish untouched):
| Goes | Stays (other fish still use it) |
|---|---|
| the trout branch of `keepOf` (`keep.ts:117-127`), `groupGlance` for trout, `troutLine` in `strip.ts` (if the owner agrees, Q4), the trout part of `looseRows` (K4/K5 rows move into the table), the trout `src:` / `share:` / `multi:` row sheets, the group lines of the pin bar (`pins.ts` group part, `useAnchors` `anchorsOf`, GroupCard anchors) | `grouphead.ts`, `tree.ts`, `tail.ts`, `GroupCard.tsx` (the `other:oN` sheet draws a `group` block: `sheets/other.ts:94`), `plainrow.ts` + `PlainRow.tsx` (the 1,616 frames' lone other fish, and every other fish shown in full), `others.ts` split, `card.ts` |
| roughly **250 to 350 lines** [inferred] | roughly 1,300 lines |

Branches inside the group composer that only trout reached (steelhead lift, hatchery tiers, Vedder exceptions) stay as
dead branches. knip does not see dead branches; a coverage run over every other-fish row after the switch can show
them (optional cleanup, section 6, S2).

**Tests and goldens that change or retire** [ran grep + test counts]:
| Group | Files (test count) | Fate |
|---|---|---|
| Core composer tests pinned on trout waters | `grouphead` (16), `tree` (20), `tail` (7), `plainrow` (17), `t475` (8), `t479` (4), `t480` (5), `t481` (6) | Keep the files (the code stays); cases that only exercise trout paths are retired or re-pinned on other-fish rows. About a third of ~83 cases [inferred] |
| Keep composition | `keep.test` (12), `others.test` (11), `glance.test` (5, expects `value: null`), `strip.test` (21, `troutLine` cases), `pins.test` (29), `sheets.test` (27, `share:`/`multi:`/`src:`) | Rewritten for the trout block; pins tests mostly retire if the pin bar loses its group line |
| ui-native renders | `KeepTab.test` (17), `QuotaWording` (4), `T479Wording` (3), `T480SizeStyle` (1), `T481MultiWater` (3), `rowFrame` (5), `useAnchors.test` (4), `a11y.test` (14, part), `palette.render` (29, part), `style-tokens` (5, part), `SheetBody.test` (6, part) | Trout cases move to new `TroutCard` / `TroutSheet` tests |
| VM fixtures | `ui-native/src/regs/fixtures/*.json`: 9 page fixtures each hold a trout group (`bc1_501`, `glenn_501/615`, `klwa_1010/1012`, `vedder_1010/515`, `x35p_1010`, `edge_x15p2_1010`); 28 test files read the folder | Re-cut by `node app/tools/regs-recut/sync.mjs` once the section exists; their `keep` changes shape |
| Conformance | `keep.props.parity.test` (4 properties over every golden state, checked against the old prototype's rules: "number once", form a/b), `t670.release.test` (K3 Shuswap lake trout, K4 Bruer Creek brook trout, K5 Mat Creek: trout lines), `decidedStory.fixture`, `twoRegions.fixture` | keep.props: the trout part retires (the properties are about the tree); t670 K3–K5 re-pinned on table strings |
| Goldens | `app/fixtures/golden/taps.json` (470 kB), `page-oracle.json` (1.5 MB: the **v36 prototype's** rendering) | taps re-cut; the page oracle stops being an oracle for the trout block (the new table has no prototype). Keep it for the rest of the page |
| Web site | `tools/seo/site.test.ts` (26), `seo/t670.web.release.test.ts`, `seo/verify.ts` | `sectionsOf` reads the trout VM; re-pin |
| Browser tools | `tools/regs-walk.mjs:43-68` (`keep-group`), `tools/regs-web/rows.mjs` (`krow`, `khead`), `tools/store-shots/checks.mjs:22` (`18-regs-trout`), testbed `scenarios/regs-switch.mjs`, `stalls.mjs` | Selectors and checks move to the new test ids |
| Pipeline pair | `pipeline/tests/test_answers_tidal.py` `app_reader_versions` | gains `trout` when the app lists it |

I ran the five core trout test files once: 70 pass, 0 fail [ran].

---

## 4. The "other fish" question

Owner 15:47 settled it: trout only, other fish untouched (T810). What that means in the code [read + ran]:
- The whole group composer and `GroupCard` stay: the Other fish sheet (`other:oN`) draws a non-trout group with them.
  In this release 153 distinct non-trout rows carry a group (R1 14 … R6 36) [ran m2.py]; the spec counted 3,320
  frame uses.
- `PlainRow` stays: the 1,616 frames' lone other fish, and pages where every row is drawn in full.
- **A staged switch is feasible** and is what the owner chose. The cost is the dual path the spec feared, but small:
  the old code is not duplicated, it simply keeps serving other fish.
- **Bytes: the saving survives.** Measured on the real file (section 11b): dropping `group` on trout rows only takes
  regs-6 from 413,611 to 303,072 gz (−110.5 kB); dropping every group would take −122.8 kB. Keeping the old card for
  other fish costs 12.3 kB in R6 (3.3 kB in R5, 1.7 kB in R3) [ran m5.py].
- One small question remains (Q3): the lone other fish in the 1,616 frames. Leave it drawn in full under the trout
  fold, as today, or move it into the Other fish list. Either is a few lines.

---

## 5. Size budget

**Today's room** [ran `release.build.web_moments` on r-46e4355a7277, against `contract.WEB_BUDGET` on HEAD]:
| Moment | Used | Gate | Room |
|---|---|---|---|
| first_tap | 4,198,757 | 4,200,000 | **1,243** |
| first_tap_low | 3,359,382 | 3,400,000 | 40,618 |
| cold_load | 3,042,537 | 3,350,000 | 307,463 |
| first_tap_unnamed | 6,018,006 | 6,200,000 | 181,994 |

`regs-{region}` is priced as the largest region file: regs-6, 413,612 gz (regs-5 387,155; regs-3 351,239).

**After the salmon lane lands** [read]: commit `91cf5e8fc` on `lane/salmon-rules-1116` raises first_tap to 4,400,000 and
first_tap_low to 3,600,000. The owner's list at ~14:05 says the book closures bring first_tap to about 4,312,000 and
first_tap_low to about 3,472,600. So the room becomes about **88 kB** (first_tap) and **127 kB** (first_tap_low). That
explains the spec's "88 kB in the brief" puzzle. Not measured on a built release [inferred].

**How the table is carried.** In each `regs-{r}` file (spec 4.1: `cell.trout`, `trout_table`, `trout_dates`,
`trout_frame`), recommended. It is fetched with the page already, so no new request, no new `WEB_BUDGET` file name,
and `request-walk.pin.json` / `openCost.release.test.ts` see no new fetch [inferred]. A separate `trout-{r}` file would
add one request on every first tap and a new budget entry: worse. The zdicts for the three new tables live in the
water file's `zdict` table (`store.ts:126`: "water holds the region tables' too"), so `water-shared` grows by the
dictionaries (a few kB each) [inferred].

**Likely cost.** The spec's 15 to 40 kB per region came from gzipping the JSON as one stream. The release stores
each blob deflated on its own with a 32 kB dictionary, which compresses less well: the same group trees are 54.7 kB
as one stream but 110.5 kB as stored in R6 (section 11b). The new table will be stored the same way, so expect
**roughly 30 to 80 kB per region** [inferred: the spec's range × the measured 2× ratio], against **110.5 kB freed in
R6** at the switch. Net R6 about −80 to −30 kB. Per water: tables are interned, so a water that shares its region's
common table costs one integer per cell (9,572 cells in all) plus its share of a body; a distinct table is roughly
120 to 320 B as stored [inferred: 251 distinct R6 sets]. Ticket A (pipeline) measures it on the real encoder.

**Two app-side budget facts:** (1) during the build-behind, a release can carry the new section only in a side
release: with 1.2 kB (or ~88 kB after the salmon raise) of room, carrying both old and new is out on a promoted
release. (2) After the switch the new section and the dropped trout groups land in the same release; the app reads no
extra file, so the app's own budgets (`tools/release-budget.test.ts`, `seo/budget.ts` page sizes) move only by the
web page text, which should shrink (the table says less than the tree) [inferred].

---

## 6. Shared surfaces that must move (or need not)

| Surface | Moves? | Evidence |
|---|---|---|
| Fish sheet (tap a fish) | **Yes**, new sheet key | Today `sp:CODE` = pictures + "What to look for" (`sheets/fish.ts`). New: today's answer, Here today, How to tell it, Sources. All buildable from existing `SheetBlock` kinds (`head`, `para`, `list` with `strong`, `image`, `link`, `sources`, `rule`) [read model.ts:323-351]; maybe one new "answer header" block. Gestures exist (drag down from the handle, Done, scrim, back/Escape) [read SheetHost/Sheet]. **Open:** the owner said "swipe down to close"; today the drag starts from the handle only ("G1: from the handle"). Dragging from the body changes the shared `Sheet` for every sheet |
| Methods / Gear tabs | **No** | `gear.ts`, `methods.ts` do not read the group tree [ran grep] |
| Folded Keep row | **Yes** | One "Trout and char" fold per page (today one per group, 43 frames have two; 2,202 frames have none and show plain rows). Its value = the table's `fold` words ("2 to 5 a day"): this reverses the owner's 2026-10-28 rule "a folded row says NO count" (glance.ts header, glance.test), which his round 3 answer (11-15 23:25) replaced. "Details" vs the expand arrow: Q1 |
| Closed banner | **No change** | A whole-water closed day draws the banner and no rows (`keepOf`, `rowsDrawn`) [read]. The table shows only on an open day; a trout-only closure (Fraser R3) is inside the table |
| Year strip trout line | **Decide** (Q4) | `strip.ts:128` `troutLine` says "Trout: release until …" on the bar; the table says it too |
| Pin bar | **Yes** (missed by the spec) | Its only content is the group line and tier breadcrumb (`pins.ts`, `useAnchors.anchorsOf`). With trout in the table and other fish in sheets, nothing feeds it. Q2: drop the group pin, or pin the table's top line |
| Search results | **No** | Search reads the status index (open/closed) only (`SearchSheet.tsx:61,80`) [read] |
| Website (regs-web / SEO) | **Yes** (missed) | `seo/model.ts` `sectionsOf` reads `k.groups` and `k.plainRows`; `pane.tsx` server-renders the real screen, so the table appears there by construction, but the crawler sections and `verify.ts` must read the trout VM. The page budget (`seo/budget.ts`, 30 kB/page) should not be at risk |
| Store shots | **Yes** | `18-regs-trout` (`store-shots/checks.mjs:22`) re-shot; a Play listing picture shows the old card |
| Large text | **Yes** | Text scales to 2× (numbers and header capped) (`a11y.test.tsx:233`). The right band must fold into bold lines at large text: a `useWindowDimensions().fontScale` threshold (works on both platforms, no file pair needed) [inferred] |
| Screen reader | **Yes** | One label per row (Python texts joined in core); ARIA props only (AGENTS 34; `check-platform.mjs` refuses `accessibilityState`); the band cell must be read with its rows, not as a separate column [inferred] |
| Dark mode, colour-blind | **Yes** | Colours are tokens (`theme.ts` `paletteFor("light"/"dark"/"cvd")` over `color.ui.*`). New tokens (box wash, white box, quiet text) need all three palettes, `check-colours`, `cvd.test.ts`, `palette.render.test.tsx`. The app ships a separate `cvd` theme [read] |
| Hatchery link | Reuse | `HowTell` → the `hatch` sheet (GroupCard.tsx:33) [read] |
| Dev / browser tools | **Yes** | `regs-walk.mjs`, `regs-web/rows.mjs`, `a11y-walk.mjs` scenes, testbed scenarios |

---

## 7. Revised app tickets

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

---

## 8. What can start now, what must wait

**Now, without the pipeline:**
1. M0, the optional-section gate (safe, a quarter session, no data).
2. With the main chat: the contract (section 10) and the vectors file format; the first ~10 vectors written by hand
   from the round 11–15 mocks (Skeena Jul 15, Lois, Fraser R3 Sep 15, Fraser R2 at Mission, Elk, Kootenay, Madden,
   Cunningham, Adams, Kitimat).
3. N, O and P against those vectors, behind the dev switch.
4. New colour tokens with contrast and cvd checks.
5. The four owner decisions (section 9).

**Must wait:** M (the real format), T (real VM on a release), R (real tables on ~45 waters), S (the switch), the t670
re-pins, and any picture the owner is asked to judge as "the real thing".

---

## 9. Risks, spec errors, owner decisions

### Top 3 app-side risks
1. **Contract churn.** Python writes the words; a design tweak after the vectors are frozen costs a pipeline change,
   a release and an app change. Mitigation: the vectors file is the one contract; the app never re-words.
2. **The build-behind window.** Two paths live side by side behind a dev switch, and releases without the section
   must keep loading (M0). An APK built in that window must read both shapes; after the switch older APKs are told to
   update (`minAppVersion`, `release/publish.py:84`, `data/src/release/boot.ts:131` [read]).
3. **Layout.** Spanning white boxes, two text sizes, the 2× fold, 360 px with no sideways scroll, screen-reader order
   across two columns, and cards taller than a screen (13 of 43 mocks) must work the same on Android and the web.
   No pixel tests exist; the picture pack and the a11y walk are the proof.

### What the code shows to be wrong or missing in the spec's app half
1. **M as written breaks every normal release.** Adding `trout` to `KNOWN_VERSIONS`/`SECTION_NAMES` makes
   `store.ts:116-122` refuse a release without the section, and adding the column to the `cell` SELECT
   (`reads.ts` keyCells) fails on old files. The spec's "invisible to older apps" is true of old apps, not of the
   new app reading old releases. Fix: M0.
2. **"Up to 5 today" cannot be Python's.** The page shows any picked day. The day phrase must be core's (section 10).
3. **S's deletion list.** `grouphead.ts`, `tree.ts`, `tail.ts`, `GroupCard.tsx`, `plainrow.ts` stay (other fish;
   the 1,616 lone-other-fish frames). Deleted code is ~300 lines, not the 1,683 the spec's table implies.
4. **Missing surfaces:** the pin bar, the website text (`seo/model.ts`), the browser tools and store-shot checks,
   the strip's trout line, t670 K3–K5, the glance "no count" rule.
5. **Not needed:** search results and Methods/Gear do not move; the sheet's gestures exist already.
6. **One fold, not one per group:** today 43 frames show two trout folds and 2,202 show trout as plain rows outside
   any fold; the new design is one fold. A visible change on about half the frames (owner should see Elk in winter).
7. **Row model is out of date:** owner 15:48 / 16:00 (hatchery only = one row with a filled label, wild apart only
   with its own quota; box-level label) supersedes the spec's one-row-per-origin and its `"ST/hatchery"` keys.
8. **The cutover saving** was computed by dropping every group as one gzip stream (55 kB in R6). Measured on the
   stored file, trout groups only: 110.5 kB in R6. The new table's estimate must be redone the same way (section 5).
10. **The sheet closes past a third of its height or on a downward flick** (`ui/src/gestures.ts` `useSheetDrag`), not
    "over 90 px" as the spec's P says. Nothing to build; the spec's number is just not the app's.
9. **Spec type mismatch:** 3.2 says texts are marked `now`/`next`, the type says `"now"|"quiet"` with next texts in
   `DateSet`. Section 10 settles it.

### Owner decisions (short, recommended first)
- **Q1 Fold row:** the trout row shows its words ("2 to 5 a day") and an expand arrow; other fold rows keep "Details"
  (recommended), or all fold rows change to the arrow.
- **Q2 Pinned top line:** drop the trout pin line while scrolling (recommended: the table's top line is short), or pin
  the table's top line.
- **Q3 A lone other fish** beside trout (1,616 frames): stay drawn in full under the trout fold as today
  (recommended, no change), or go into the Other fish list.
- **Q4 The year bar's "Trout: release until …" line:** drop it once the table says the same (recommended), or keep it.

---

## 10. Draft contract (app side's first cut) — A PROPOSAL FOR THE MAIN CHAT TO CORRECT

Everything below is a proposal from the app side. The main chat owns the data side and corrects it. Where it differs
from the spec's 4.1, the reason is given.

### 10.1 Where it lives
- Answers section `trout`, version 1, in each `regs-{r}.sqlite.gz`: `cell.trout INTEGER NULL` (one per answer key ×
  segment, as the other five frames), interned `trout_table (id, j)`, `trout_dates (id, j)`, `trout_frame (id, j)`
  where a frame is `[table id, dates id | null]`. Blobs in the existing codec with a zdict per table (in the water
  file's `zdict`).
- `cell.trout` NULL means "no trout table": outside B.C., tidal, a closed display day (the banner speaks), no trout
  row. The app draws nothing, never a fallback.
- `meta.layout.versions.trout = 1` and the manifest's `regulations.versions.trout = 1` say the section exists.

### 10.2 What an older app sees
- Before the switch: the section is unknown to it; it checks only its five sections and selects named columns, so it
  never sees the new tables [read store.ts, reads.ts].
- The new app during the build-behind (after M0): reads the section when listed, else draws the old card.
- At the switch: `rows` 34 (trout groups dropped) → every older app refuses the release and keeps its data;
  `minAppVersion` asks for the update. The web always serves app and release together.

### 10.3 Shape (TypeScript view; pydantic is the source, emitted to `answers.generated.ts`)
```ts
type Txt = string;                 // a whole sentence or phrase, written by Python, drawn unchanged
type Src = number[];               // rule indexes into the water file's rule table (rix); every one must exist

type TroutTable = {
  top: {                           // the big line, WITHOUT a day word (core adds "today" / "on Jul 15")
    lead: Txt;                     // "Up to 5" | "Up to 2, or 5 if they're rainbow trout" | "Release every one" |
                                   // "Closed for trout and char" | "Nothing to keep"
    second?: Txt;                  // Lois: "only 2 wild trout or char, each under 40 cm (16 in)"
  };
  fold: { value: Txt; tone: "keep" | "release" | "closed" };   // "2 to 5 a day", "release", "closed", "none to keep"
  hatchLink: boolean;              // "How to tell a hatchery fish ›" on the kicker line
  notes: { text: Txt; today: boolean; src: Src }[];             // angler-only notes; today = above the top line
  boxes: Box[];                    // keep boxes first, then at most one release box, then at most one closed box
  together?: { text: Txt; src: Src };                          // a limit across two keep boxes
  year?: { state: "keep" | "release" | "closed"; days: number }[];   // only when every fish shares one state
  elsewhere: Txt[];                // "Fished elsewhere today?" lines (empty: no link)
  sheets: Record<string, FishSheet>;                            // by Row.key
};

type Box =
  | { kind: "keep"; n: number;     // "Keep 5 in all": the app writes the frame words "Keep N in all"? -> see Q-D1
      strip?: Txt;                 // "only 1 over 50 cm (20 in)" (one line where it fits)
      hatcheryOnly?: boolean;      // the filled label once, on the strip (owner 16:00), and no row carries it
      tag?: Txt;                   // "Lake's rule"
      rows: Row[];
      bands: Band[] }              // MUST cover rows 0..rows.length-1 exactly once, in order
  | { kind: "release" | "closed"; rows: Row[] };              // one column word "↩ Release" / "✕ Closed" (app words)

type Row = {
  key: string;                     // unique in the table: the fish code, plus "/wild" only where wild has its own quota
  fish: string;                    // picture code (fish manifest)
  name: Txt;                       // "Dolly Varden/bull trout", as the book prints it
  origin?: "wild" | "hatchery";    // ONLY on twin rows (Kitimat, Lois, Khartoum, Ruby, Cultus)
  hatcheryOnly?: boolean;          // the filled HATCHERY ONLY label beside the name (single fish, Skeena steelhead)
  sub?: Txt;                       // "a rainbow over 50 cm (20 in)"
  lines: Txt[];                    // today's rule, bold, in order: size, "uses the 1 over 50 cm (20 in)", possession,
                                   // stop duty ("after keeping 1, stop fishing this river today")
  quiet?: Txt[];                   // regular weight, e.g. Cunningham "3 a day, but never more than 2 lake trout with you"
  src: Src;
};

type Band = { from: number; to: number; text: Txt;            // "up to the 5" | "only 3" | "only 1 of these 4"
              kind: "upto" | "own" | "shared" | "possession" };
// "upto" spanning several rows is drawn one cell per row (owner 13:48); "own"/"shared" as one white box.

type DateSet = {                   // every coming-change text (small, quiet) for ONE segment; keys must resolve
  card?: Txt;                      // "Until Oct 31, then closed to all fishing" (every fish, same day and words)
  box?: Record<number, Txt>;       // by box index (strip date)
  band?: Record<string, Txt>;      // "box:band"
  row?: Record<string, Txt>;       // "box:row" (last line of the row; a Release/Closed row's date under its word)
  sheet?: Record<string, Txt[]>;   // by Row.key: sentences for the sheet ("From Nov 1 every wild one goes back.")
};
// Owner rules the words must follow (round 12): a coming closure says what closes, no reopening date; the reopening
// date only while it is closed today; a change that only loosens = "rule changes <date>"; otherwise the next
// stricter change is dated.

type FishSheet = {
  head: Txt;                       // "Keep · only 1 of these 4 · counts toward the 5" / "↩ Put it back · until Jan 31"
  here: { t: Txt; bold?: boolean }[];   // "Here, today", in order (duty bold; licence line; skipped one-day note)
  src: Src;                        // this fish's rules, most specific first (up to 4); "All sources ›" uses every Src
};
// "How to tell it" is the APP's content: pictures and FISH_TIPS by Row.fish, both DV pictures, the hatch sheet link
// when origin/hatcheryOnly is set. No data needed.
```

### 10.4 Word strings the data side writes in full
Top line (`lead`, `second`), fold value, notes, strip, tag, row name, sub, lines, quiet lines, band texts, together,
every date text, sheet head and here-lines, elsewhere lines. All pinned as whole strings in pytest (G) and listed in
the sentence catalogue for the owner.
**App words (fixed, in `words.ts`):** the kicker "TROUT AND CHAR", "How to tell a hatchery fish ›", "HATCHERY ONLY",
"↩ Release", "✕ Closed", "Keep N in all" (Q-D1), "Sources ›", "Fished elsewhere today? ›", the day phrase, the sheet
section heads ("Here, today", "How to tell it", "Sources"), "Trout and char through the year".

### 10.5 Day ranges and "until …"
One table + one date set per (answer key, segment); weekday and hour moments are segments already (`segmentIndex`).
The app reads only the day's cell. Every "until …" text is valid for the whole segment (the last day before the next
tighter change, per the spec's 3.3). Year wrap is the data side's.

### 10.6 Shared committed fixture (AGENTS rule 40 pattern)
- `python -m pipeline.tools.emit.trout_vectors` writes `app/fixtures/trout/trout-vectors.json`: about 45 cases, each
  `{name, anchor: {stream key + metre | water + part}, day, weekday, table, dates}` built by the real table builder
  from the release under test, plus a short `hand` list of contract edge cases written once (a band gap, a dangling
  rule ref, a date key to a missing row, a row key twice: each must be refused by the app's decoder and never
  emitted by the builder).
- Python side: `--check` fails when the committed file differs from what the builder makes (in `scripts/fast-fail.sh`
  with a "caught:" line); pytest pins the sentences.
- App side: `troutOf` whole-string tests, `TroutCard` render tests, the fish sheet, the a11y and palette tests, and the
  picture pack's web export all read the same file. Before the builder exists, the main chat and the app write the
  first ~10 cases by hand in the same file; the builder must reproduce them (owner-approved words) or the diff is
  read by the owner.
- Anchors follow AGENTS W46 (named by stream key + metre, never a section id).

### 10.7 Rough byte cost
See section 5: roughly 30 to 80 kB per region as stored (estimate), one integer per cell, about 120 to 320 B per
distinct table; the switch frees 110.5 kB in R6 (measured, before T805). To be measured by ticket A.

### 10.8 Open questions for the data side
1. **Q-D1** Box frame words: does Python write "Keep 5 in all" whole (and the "only 1 of these 4" forms), or the app
   from `n`? App proposal: Python writes it (`strip` already sits on the same line).
2. **Q-D2** The day phrase: agree the top line carries no "today" (the app adds it), and that no other text says
   "today" except the stop duty ("…stop fishing this river today")? That one is about the angler's day, not the
   shown day: keep.
3. **Q-D3** `cell.trout` NULL on a closed display day, a tidal key and outside B.C.: agreed?
4. **Q-D4** Loose trout rows (Region 8's "20 brook trout from streams", the Duncan tributaries' bull trout, t670
   K4/K5) and trout rows without a group (2,202 frames): all inside the table?
5. **Q-D5** Two trout cards today (43 frames, Kootenay): one table with two keep boxes and one top line?
6. **Q-D6** Which rule refs per row and sheet (the winning rule only, or every rule that shapes the line), and their
   order.
7. **Q-D7** Angler-only notes move into the table: does the page's caveat block then drop them (no double)?
8. **Q-D8** Possession: the page keeps its line (owner 15:42); the table speaks only where a trout differs. Which
   rows carry it, and as `lines` or `quiet`?
9. **Q-D9** The `rows` 34 drop list: which fields only the trout block read (`group` on trout rows; `real_daily`,
   `elsewhere`?) — the app's readers after S are listed by grep at S time.
10. **Q-D10** Sheet texts in the table body or in a separate interned table (they are the bulk and are read only on a
    press)? The app does not mind either.
11. **Q-D11** A side release for previews: is the staging test channel real? (Not checked by me.)

---

## 11. Answers to the main chat's three questions (16:55)

Spec re-read in the main checkout at `f71022a7e` (revised 16:40; section 6 slot). Section 7 above is my replacement
for its M–S table.

### 11a. What the folded "Trout and char" entry shows today, word for word [ran]
Run: the app's own `loadRegsInputs` → `buildRegsPage` → `groupGlance` on r-46e4355a7277 (a scratch vitest config,
`scratchpad/vt/fold.dump.test.ts`; nothing written in the repo). The row draws: picture, name, the value only when not
null, then an outlined button "Details" with an expand mark (`KeepBlocks.tsx` `FoldRow` `:250-292`).

| Water, day | What the Keep tab shows for trout today | Spec's proposed fold |
|---|---|---|
| **Simple:** Skeena River, Sedan Creek → Mill Creek (`gnis:2936` part 12), Jul 15 | one row: [rainbow picture] **Trout and char** [Details ⌄]. No number (owner rule 2026-10-28, `glance.ts:50` value `null`). Opened, the card's head reads "5 a day per angler across Region 6" | "Up to 5" |
| **Simple, Region 3:** Shuswap Lake (`wbk:329518145`), Jul 15 | [rainbow] **Trout and char** [Details ⌄]; opened: "5 a day per angler across Region 3" | "Up to 5" |
| **Mixed:** Lois Lake (`wbk:329197058`), Jul 15 | **two** rows: [brook trout picture] **Trout and char** [Details ⌄] (opened: "4 a day per angler across Region 2") and [rainbow] **Rainbow trout and steelhead** [Details ⌄] (opened: "This lake’s rule: up to 6 a day per angler") | "Up to 6 rainbow, 4 others" (one row) |
| **Mixed:** Kootenay Lake main body (`wbk:-20`), Nov 15 | **two** rows: [Dolly Varden picture] **Trout and char** [Details ⌄] ("5 a day per angler across Region 4") and [rainbow] **Rainbow trout** [Details ⌄] ("This lake’s rule: up to 10 a day per angler") | one row |
| **Release-only:** Elk River, mouth to Elko Dam (`gnis:16880` part 0), Nov 15 | **no fold.** The bar reads "Open · today · Trout release only until Mar 31", then a plain row drawn in full: "Brook trout, brown trout, cutthroat trout, Dolly Varden/bull trout, lake trout and rainbow trout" · pill "↩ Release" · "You can fish for them, but release every one until Mar 31 next year. Then the whole water is closed Apr 1–Jun 14. You can keep them from Jun 15 next year." | "Release" (the spec's fold for an all-release table) |
| **Closed:** Elk River, Apr 15 | **no fold, no rows.** The bar "Closed · today · Opens Jun 15", then the closed banner: "No fishing for any game fish Apr 1–Jun 14." / "From streams, Apr 1–Jun 14." (Region rule, p.34) | none: on a whole-water closed day the table is not drawn (Q-D3); "None to keep" is for an open water where no trout may be kept |
| Elk River, Jul 15 | [rainbow] **Trout and char** [Details ⌄]; opened: "This river’s rule: up to 1 a day per angler" | "Up to 1" |

So the new fold changes three things at once: a number appears (reversing the 2026-10-28 rule, as the owner asked at
23:25 on 11-15), two folds become one (43 frames), and release-only waters get a fold where they had a full row
(2,202 frames). Whether "Details" stays beside the arrow is Q1.

### 11b. The trout share of today's first-tap bytes [ran]
**Release measured: `r-46e4355a7277`** (LATEST; built before T805). T805's re-cut release in the main checkout is the
**mini** release (`app/packages/data/dev/mini-release/r-5da61ff3220b`, commit `39f46ef56`): its regs files are 911 to
4,841 B, a few test waters, so it cannot price a region. No full release with T805 B/C exists on this machine
(`data/release` holds r-46e4355a7277, r-f749475d0962, r-a9914d6b25b7). The T805 possession and wild lines make the
trout groups somewhat larger; ticket A must re-measure on a full build that has them.

Method (`scratchpad/m/m5.py`): open each shipped `regs-{r}.sqlite.gz`, re-pack every `rows_row` blob with the
release's own zdict (`water-shared` `zdict`, 32,768 B for `rows_row`) with `group` removed from trout rows only, VACUUM,
gzip -9, compare. Re-serializing unchanged reproduces the shipped size to within 2 B, so the method is faithful.

| Region file | Shipped gz | Without trout groups | Trout share | Without every group |
|---|---|---|---|---|
| regs-6 (the one first_tap prices) | 413,612 | 303,072 | **110,539 (26.7%)** | 290,801 |
| regs-5 | 387,155 | 258,939 | 128,216 (33.1%) | 255,642 |
| regs-3 | 351,239 | 228,867 | 122,374 (34.8%) | 227,131 |

Other regions by blob sums (`m4.py`, same zdict, no VACUUM): R1 38 kB, R2 65 kB, R4 55 kB, R7a 128 kB, R7b 38 kB, R8
33 kB.

**On first_tap:** the trout groups are 110,539 of 4,198,757 B (2.6%); they sit entirely in the `regs-{region}` term.
After the switch the largest region file would be regs-6 at about 303 kB (regs-4 ≈ 287 kB), so first_tap falls by
about 110 kB before the new table is added. The same 110 kB falls out of `cold_load`, `first_tap_low` and
`first_tap_unnamed`, which also price `regs-{region}`. The trout rows' other fields (items, conds, real_daily …) stay:
the engine-side answers are not part of this saving.

**Compared with the spec's 4.5:** its 54,739 B for R6's whole group tree was a single-stream gzip of the JSON; the
file stores blobs one by one, so the real cost is about twice that. Its 15–40 kB guess for the new section has the
same bias (section 5).

### 11c. Corrections to the spec's section 6 (and the 2.3 references it leans on) [read at 7c413b629 / f71022a7e]
| Spec says | Code says |
|---|---|
| M: put `trout: 1` in `KNOWN_VERSIONS` | `versions.ts:12` `SECTION_NAMES` + `:14` `KNOWN_VERSIONS`; `store.ts:116-118` refuses **any** listed section that is absent, `:119-122` the same against the manifest. Needs a separate optional list (my M0) |
| M: `cell.trout` read at `reads.ts:298` | `:298` is the `keyCells` SELECT with fixed column names; adding `trout` there breaks on every release without the column. `:81` is the interface's `cell()` |
| N: `troutOf(inputs)` | needs the day too (`troutOf(inputs, day)`): the page shows any picked day |
| O: the group branch to replace at `keep.ts:122-133` | the open-day loop is `keep.ts:123-131`; the group/plain choice `:128-129` |
| O/S: `KeepTab.tsx` | folds built at `:43-48` (`groupGlance` `:46`, `GroupCard` in the fold `:47`); unfolded `GroupCard` `:113`, `PlainRow` `:114` |
| O: `KeepBlocks.tsx` `GlanceFold` ~`:243` | `GlanceFold` interface `:243`, the drawn row `FoldRow` `:250-292` ("Details"/"Hide" + expand icon `:287-289`) |
| P: new `ui-native/src/regs/sheet/TroutSheet.tsx` | probably not needed: `SheetBody.tsx` draws `SheetBlock`s (`core/src/regs/model.ts:323-351`); the sheet is core (`sheets/trout.ts`) + at most one new block kind |
| P: "swipe down over 90 px" | `ui/src/gestures.ts:263` `useSheetDrag`: closes past a third of the sheet's height or on a flick, from the handle |
| P: reuse `SourcesView` | yes; `sheets/sources.ts:73` `srcSheet` (a row's sources), `:44` `srcsSheet` (2.3 / 3.5 say `:51`) |
| S: delete `plainrow.ts`, `grouphead.ts`, `tree.ts`, `tail.ts` | all four stay: `sheets/other.ts:94` builds the Other fish sheet from `groupOf`; `others.ts` and `plainrow.ts` draw the lone other fish in 1,616 frames. Delete only the trout branch (section 3) |
| S: `GroupCard` stays because of `SheetBody.tsx:71` | correct (`SheetBody.tsx:71`, `case "group"`) |
| S: misses | the pin bar's group lines (`pins.ts`, `useAnchors.ts:70-84`, `PinBar.tsx`), `strip.ts:128` `troutLine`, `seo/model.ts:112-120`, `regs-walk.mjs:43-68`, `regs-web/rows.mjs`, `store-shots/checks.mjs:22`, t670 K3–K5 |
| R: `tools/store-shots.mjs`, `tools/testbed` | exist; the trout shot is `18-regs-trout`; testbed scenarios `regs-switch.mjs`, `stalls.mjs` (and `scenarios-ios/stalls.mjs`) touch the regs page |
| 2.3: `card.ts:131/154/188`, `GroupCard.tsx:235`, `fish.generated.ts:208`, `WaterModeHost.tsx:89`, `versions.ts:14`, `store.ts:116-122` | correct |
| 2.3: `tree.ts:37` `sizeOf`, `:236` `liftCounts`, `:118` `presOf` | correct |
| 2.3: `standing.ts:97-117` "picks the page's possession rule" | `:97` is `possessionRules`; `possessionOf` is `:133` |

---

## 12. Every place today's trout card DECIDES in TypeScript

Input for the reviewer who will attack the plan. Owner rule (relayed): Python decides every word, number and date;
the app draws only; no hand-written payload types. Read on `redesign/stream-sections` @ f71022a7e. Paths under
`app/packages/` unless stated.

Kinds: **W** composes or rewrites a word/sentence · **N** picks or derives a number · **D** derives a date or a
date sentence · **C** compares answers or matches text by regex · **F** a fold / order / selection / grouping choice ·
**B** a fallback (a silent drop, a default word, an untested path) · **T** a hand-written type mirroring a payload.

Fate: **OFF** = the trout path stops passing through it (the function stays only if other fish still call it) ·
**PY** = the decision moves to Python (the table writes it) · **LAYOUT** = stays, as pure layout · **STAYS** = stays
and still decides (out of this plan's scope, or not trout-only).

### 12.1 Core: the Keep composition and the group card (the trout path today)
| # | file:line | Function | Decides | Kind | Fate | Why |
|---|---|---|---|---|---|---|
| 1 | `core/src/regs/others.ts:24` | `TROUTISH` | which fish codes are "trout and char" (a hand list; includes `BT`, which AGENTS 45 refuses) | F, T | **PY** | The table builder decides which rows are in the table; the app must take "this row is in the table" from the data, or the two lists can disagree |
| 2 | `others.ts:33-41` | `splitRows` | trout rows first; with **fewer than 2 other rows every row is drawn in full** (1,616 frames) | F | STAYS for other fish; trout part **PY** | Owner Q3 decides where the lone other fish goes |
| 3 | `others.ts:48-53` | `looseRows` | a trout row with a pool and no headline becomes a compact-list line (t670 K4/K5) | F | **PY** | Those rows belong in the table (Q-D4) |
| 4 | `others.ts:63-66`, `caveats.ts:64-68` | `shortValue`, `answerTxt` | "5 a day" / "release" / "closed" from status + daily | W, N | STAYS for other fish and the timed caveat; trout fold **PY** (`fold`) | Same answer worded twice (12.6 #4) |
| 5 | `others.ts:69-77` | `SIZE_IN_GROUP` regex over the group JSON, `sizeInItems` | whether a row shows the size hint | C | STAYS (other fish) | Regex over a JSON string |
| 6 | `core/src/regs/keep.ts:123-131` | `keepOf` loop | group card if `group.headline`, plain row if `isPlainRow`, else dropped to the list | F, B | **OFF** (replaced by the table block) | The drop to the list is a silent re-route (comment `:130`) |
| 7 | `grouphead.ts:32-45` | `rowTitle` | the card title: "Trout" when the pool is TROUT_CHAR minus CHAR, the group name, or the members joined | W, C | **PY** (`Row.name`, kicker is app word) | Reads untyped `pool.f.species` / `species_except` with casts |
| 8 | `grouphead.ts:60-67` | `separateFrom` | "Separate from your N trout and char" by comparing roles across rows | C, W | **OFF** | The table's boxes say this structurally |
| 9 | `grouphead.ts:69-79`, `:88-99` | `scopeOf`, `scopeBadge` | "Region 2-wide", "All Region 2 streams", "This water"; `SCOPE_MISSING` when a pooled keep row has no scope | W, B | **PY** (`tag`, `strip`) | `SCOPE_MISSING` is a drawn default word |
| 10 | `grouphead.ts:125-145` | `headCtx` | moves the scope tag by regex `/ across /` and `-wide$`; picks `relTier` by comparing `headline.n` with tier numbers | C, F | **OFF** | Structure re-derived from the tree in TS |
| 11 | `grouphead.ts:148-157` | `bigWords` | which number in a sentence is drawn big (regex on the number) | C | **OFF** (the table marks nothing big but `lead`) | |
| 12 | `grouphead.ts:160-198` | `groupHeadOf` | form a/b head; **legacy head "UNTESTED against a golden"** (`:162-169`); "Exception to regional quota" from `over_region`; the multi link when `/ across /` matches | W, N, C, B | **OFF** | An untested fallback path on the live screen |
| 13 | `grouphead.ts:205-216` | `originOf` | splits the stored origin at " — " into tag and words | W, C | **OFF** (`HATCHERY ONLY` label is data) | |
| 14 | `grouphead.ts:223-249` | `shortOf` | the pin bar's short lines, by regex over Python `says` unless `t.short` is stored (T481 half-moved) | W, C | **OFF** (pin bar, Q2) | Half of these come from Python, half are TS rewrites |
| 15 | `tree.ts:37-49` | `sizeOf` | splits a detail into size words and qualifiers by regex | C | **OFF** (`lines` carry the size) | |
| 16 | `tree.ts:56-59` | `subcapLead` | **rewrites Python's "Of these, only 1 …" to "Of your 2 trout and char, only 1 …"** | W | **OFF** | TS edits a Python sentence (12.6 #2) |
| 17 | `tree.ts:68-81` | `sizeVM` | splits `size_rule` at "; release"; steelhead definition chip by regex `/\brainbow trout$/`, "Counts as a steelhead:" | W, C | **PY** (`Row.sub` "a rainbow over 50 cm (20 in)") | |
| 18 | `tree.ts:84-108`, `:163-169` | `tierWords`, `kindWords` | which numbers are bold (regex "up to N" / "only N", " — ") | C | **OFF** | |
| 19 | `tree.ts:111-113` | `dutyWords` | **rewrites "stop fishing this river" to "the <water name>"** | W | **OFF**; the table must not route its duty through it | The design wants "this river" (owner, 2nd attack); TS rewrites it today (12.6 #1) |
| 20 | `tree.ts:118-123` | `presOf` | borrows a presence line from an undrawn row with the same fish set | C, B | **OFF** | |
| 21 | `tree.ts:172-205` | `kindRow` | "How to tell hatchery ›" link by regex `/wild steelhead go back/`; steelhead stamp link; drops a stamp the duty links; `presDup` by regex `/counts as a steelhead/`; **reads `size_note`, a field not in the generated `TreeKind`** (cast at `:190`) | C, F, T | **OFF** (`hatchLink`, labels are data) | A field read outside the generated type |
| 22 | `tree.ts:207-208` | `foldsShare` | folds a share tier whose words match a regex | C, F | **OFF** | |
| 23 | `tree.ts:220-243` | `reachesPast`, `liftCounts` | restructures the tree: lifts count tiers out of a size box | C, F | **OFF** | Rule structure decided in TS |
| 24 | `tail.ts:30-41` | `notKeptOf` | the not-kept lines; "✕" mark for closed | F | **OFF** (release / closed boxes are data) | |
| 25 | `tail.ts:51-72` | `childNotKept` | **decides which child-row fish get a release/closed line** (status compare over `mainRes`, grouping by status + rule) | C, F | **PY** | A whole answer-selection rule in TS (t670 K3) |
| 26 | `tail.ts:75-81` | `workedOf` | the "Examples of how to interpret regulations" fold | F | **PY** (`elsewhere`) | |
| 27 | `tail.ts:88-91` | `quietOf` | "recorded here, but no steelhead rule applies" only over rainbow trout | C, W | **PY** or LAYOUT (open: a table note?) | Not in the spec's table shape: a gap |
| 28 | `plainrow.ts:56` | `isPlainRow` | a row with no pool and no headline is a plain row | F | **OFF** for trout | |
| 29 | `plainrow.ts:62-128` | `YearWords` (`nextYr`, `runUntil`, `closedRuns`, `runTxt`) | **date sentences from the stored year strip**: "until Mar 31 next year", "Then the whole water is closed Apr 1–Jun 14", "You can keep them from Jun 15 next year" | D, W | **OFF** for trout (**PY**: `DateSet`) | A full TS date engine (12.6 #6) |
| 30 | `plainrow.ts:133-142` | `yearWindowOf` | draws "Through the year" unless the strip only repeats the water's closures | C, F | **PY** (`year`, all-one-state only) | |
| 31 | `plainrow.ts:147-157` | `shortWindowsOf` | "Keeping allowed only Apr 1–3" for keep runs **of 14 days or fewer** | D, N | **OFF** | A hard threshold in TS |
| 32 | `plainrow.ts:159-169` | `runSentence` | "You can fish for them, but release every one until …" / "Don’t fish for them …" | W, D | **OFF** for trout | Elk Nov 15's whole plain row (11a) is this |
| 33 | `plainrow.ts:171`, `:180` | `namedIn`, `weekOf` | the fish a rule's words list (regex); the weekday box | C, W | **OFF** for trout | |
| 34 | `core/src/glance.ts:37-47`, `:50-52` | `thumbOf`, `groupGlance` | the fold picture (rainbow preferred); **value null** (owner rule 10-28) | F | **PY** (`fold`); picture: LAYOUT or data (open) | Owner reversed the no-count rule (11-15 23:25) |
| 35 | `pins.ts:51-122` | `groupLine`, `breadcrumb`, `pinChain` | the pin bar's words and which lines pin | W, F | **OFF** (Q2) | |
| 36 | `card.ts:154-171`, `:175-183` | `rowsAt`, `storedLead` | hydrates every segment's rows; **refuses** a frame whose lead is inconsistent | — | STAYS (other fish); the table reads only today's cell | Good pattern: refuses, never re-picks |

### 12.2 Core: blocks around the card that also speak about trout
| # | file:line | Function | Decides | Kind | Fate | Why |
|---|---|---|---|---|---|---|
| 37 | `strip.ts:122-135` | `TROUT_LINE_FISH`, `troutLine` | "Trout release only until Mar 31" on the year bar when no trout row is keepable (`["RB","CT","GB"]` hand list) | D, W, F | **PY** or drop (Q4) | The same date as the table's, worded twice (12.6 #6) |
| 38 | `strip.ts:137-174` | `stripOf` (`SOON_WINDOW = 21`, `nextOpen`) | "Closed from Jun 1" when a whole-water closure starts within **21 days**; "Opens Jun 15" | D, N | STAYS (whole-water) | Overlaps the table's card line "Until May 31, then closed to all fishing": two dates for one closure on one screen |
| 39 | `standing.ts:97-104` | `possessionRules` | **picks the page's leading possession rule** (no-species first, else largest `per_daily`), drops others with the same `per_daily` | N, C | STAYS (owner: the page keeps its line) | Reads untyped `f.per_daily`, `f.species` |
| 40 | `standing.ts:117-120`, `:133-143` | `possessionValue`, `possessionOf` | the value words by regex over Python's sentence; **returns null (no row at all) when the sentence does not match** | C, B | STAYS | A silent drop (AGENTS W46) |
| 41 | `caveats.ts:187-191` | `anglerOf` | "⚠ Closed to some anglers" block | F | **PY** if the table carries `notes` (Q-D7) | Same rule said twice if both stay |
| 42 | `caveats.ts:202-261` | `timedOf` (b) `:225-243` | **compares each fish's answer across the day's weekday/hour moments** and writes "Rainbow trout: 5 a day Sat–Sun; release Mon–Fri" | C, W, D | **PY** for trout fish (or exclude trout from (b)) | A per-fish answer comparison in TS (12.6 #5) |
| 43 | `caveats.ts:213-221` | `timedOf` (a) | picks the closure that closes every asked game fish, else the one closing the most | C, F | STAYS (whole-water) | |
| 44 | `describe.ts:25-70` | `bands`, `bandTxt`, `describe` | a rule as words (size bands, "cutWords") | W | STAYS (sheets, caveats) | Used by timed (a)/(c) lines |
| 45 | `decided.ts:71-200` | `rowSources`, `ladderOf` | "How this was decided": which rules are listed, their order, the role words | F, W | **PY** for trout (`src` order, sheet "Sources") | |
| 46 | `sheets.ts:105-160` | `shareSheet`, `multiSheet` | the headline's ⓘ and "Fishing more than one water today?" from `headline.info` and the tree root | F | **PY** (`elsewhere`) | |
| 47 | `sheets/fish.ts:20-38` | `fishSheet` | pictures + `FISH_TIPS` | — | LAYOUT (app content, by fish code) | "How to tell it" is the app's by design |
| 48 | `tap.ts:32-82`, `settle.ts:78-126` | `tapDay`, `segmentIndex`, `yearSegments`, `settleAt` | which segment (and moment) is the day's | F | LAYOUT (selection, ported from Python `encode.tap`) | The one TS day choice the contract keeps |
| 49 | `banner.ts:122-133` | `sayOnce` | drops banner sentences already said by the strip (case/punctuation-insensitive compare) | C, F | STAYS (closed day, no table) | |

### 12.3 ui and ui-native
Draw only, except the order/placement choices in `KeepTab.tsx` (salmon block first `:77-86`, where the species table
sits `:62-63`, timed lines under the bar `:65-67`, daylight hours `:69-71`) [read]. Tone → colour mappings
(`KeepBlocks.tsx:256,306,340`, `GroupCard.tsx:181`, `PlainRow.tsx:106`, `YearStrip.tsx:155`) and size/spacing by tier
(`GroupCard.tsx:111-127`) are LAYOUT. No regex, no rule logic found in these components [ran grep]. `useAnchors.ts:70-84`
reads tier numbers for the pin bar: **OFF** with Q2.

### 12.4 The web site (regs-web / SEO)
`app/tools/seo/model.ts:100-135` `sectionsOf` selects which presenter strings a crawler page carries (groups, plain
rows, other fish, possession) and their order: F, **must follow** the table (ticket T). `:175-195` reads the strip and
banner (stays). `verify.ts` proves the page against the presenter; it will prove whatever the trout VM says [read].

### 12.5 Hand-written types that mirror release payloads [read]
Generated (good): `core/src/answers.generated.ts` (146 exports from `pipeline.deliver.answers.model`: `RowsValue`,
`Row`, `TreeTier`, `TreeKind`, `RuleFacts`, `StatusFrame`, `Moment` …) and `data/src/pack.generated.ts` (from
`contract.py`).
Hand-written:
| file:line | Type | Mirrors |
|---|---|---|
| `core/src/regs/inputs.ts:24` | `CellIds` | the `cell` table's columns (the `trout` column would go here) |
| `inputs.ts:27` | `LadderFrameWire` | the ladder frame |
| `inputs.ts:65` | `AkeyRow` | the `akey` table |
| `inputs.ts:90` | `ExportRule` (`fields: Record<string, unknown>`) | the export rule; its `fields` are read with casts in `grouphead.ts:35,37,43,95`, `standing.ts:100-101`, `decided.ts:147,203`, `boats.ts:21-43`, `caveats.ts:183`, `settle.ts:106`, `story.ts:87` |
| `inputs.ts:92` | `Statics` | the `static` rows |
| `core/src/regs/time.ts:15` | `RuleWhen` | a rule's `when` |
| `core/src/regs/names.ts:10` | `SpeciesBook` | `export.species` |
| `data/src/regs/reads.ts:59-70` | `PartLine`, `RulesetRow`, `StoredRule`, `Frames` (+ item/part/picker rows) | water-file tables |
| `core/src/regs/versions.ts:9-16` | `SectionName`, `SECTION_NAMES`, `KNOWN_VERSIONS`, `REGS_FORMATS` | `answers.py` section versions (pinned by `test_answers_tidal.py` `app_reader_versions`, not generated) |
| `tree.ts:190` | `TreeKind & { size_note?… }` | a field the generated `TreeKind` does not declare |

**Proposal:** the trout payload types are generated only (pydantic → `answers.generated.ts`), `CellIds` and the
section list come from `contract.py` / `answers.py` through the existing emitters, and `trout.ts` reads no `f.*` field.

### 12.6 One word or fact, two sources (TS words table vs a Python string)
| # | Fact | TS source | Python source | Risk |
|---|---|---|---|---|
| 1 | The stop duty's water noun | `tree.ts:111` `dutyWords` + `words.ts:212` `THE_NAME` ("the Skeena River") | `pipeline/deliver/answers/card.py:385` "stop fishing this {water} after N a day" | The owner's design reads "this river"; TS overrides it today |
| 2 | A box's size sub-cap | `tree.ts:56` `subcapLead` ("Of your 2 trout and char, ") | the tree's "Of these, only 1 may be over …" (`answers.py:262`) | TS edits Python's sentence |
| 3 | Short / pin lines | `grouphead.ts:223` `shortOf` + `words.ts:174-194` `SHORT_*`, `PIN_*` | `TreeTier.short` (T481), stored for some tiers only | Half TS, half Python |
| 4 | A fish's one-word answer ("5 a day", "release") | `others.ts:63`, `caveats.ts:64`, `words.ts:53` `A_DAY` | the table's `fold`, band texts | Fold, other-fish list and timed line can word one answer three ways |
| 5 | Per-moment answers ("Sat–Sun: 5 a day") | `caveats.ts:225-243` | the table per segment | Same day, two statements |
| 6 | Release / closed dates for trout | `plainrow.ts` `YearWords` (`words.ts:140-153` `SUM_*`, `NO_KEEP_TAIL`, `THEN_*`), `strip.ts:128` `troutLine` (`words.ts:74-75`), `strip.ts:161` "Closed from …" | `DateSet` (card / box / band / row) and the not-kept sentences ("Release every lake trout, Oct 15–Jan 31.") | Up to three date statements for one fish on one screen during the build-behind; two after it if Q4 keeps the bar line |
| 7 | Possession | `standing.ts:97-143` (page line, TS pick + regex) | the row possession lines (T805 B, in today's group and in the table) | Page line and row line from two pickers |
| 8 | "What is trout" | `others.ts:24` `TROUTISH`, `strip.ts:122` `TROUT_LINE_FISH` | the table builder's row set; `catalogue.BOOK_FAMILIES` | A fish in both lists or neither |
| 9 | The hatchery link | `words.ts:180` `HOW_TELL` "How to tell hatchery ›"; shown by regex (`tree.ts` kindRow) | `hatchLink`; the spec writes "How to tell a hatchery fish ›" | Two wordings already |
| 10 | Card title / fish names | `grouphead.ts:32` `rowTitle`, `tree.ts:140` `fishNames` (`words.ts:181` `TROUT_TITLE` "Trout") | `Row.name` | |
| 11 | Scope words | `words.ts:177-179` `SCOPE_*`, `SEPARATE_FROM`; `words.ts:214` `EXCEPTION_REGIONAL` | the table's `tag` ("Lake's rule"), `strip` | |
| 12 | The steelhead line | `tail.ts:84-86` `STEEL_LINE` over Python's enum (`words.ts:217`) | `steelhead_line` enum | Not in the table shape: who says it? |

### 12.7 Old and new side by side during the build-behind
| # | Old path | New path | Where they meet |
|---|---|---|---|
| 1 | `keepOf` trout branch → `GroupCard` / `PlainRow` | `troutOf` → `TroutCard` | `KeepTab` behind the dev switch (O) |
| 2 | one fold per group, `groupGlance` (no value) | one fold, `fold` words | `KeepTab.tsx:43-48` |
| 3 | pin bar group lines | none, or the table's top line (Q2) | `RegsScreen` / `pinStore` |
| 4 | `troutLine`, "Closed from …" on the bar | `DateSet` | the same screen |
| 5 | timed (b) and angler caveats | table `notes` | the same screen (Q-D7) |
| 6 | `sp:`, `src:rN`, `share:rN`, `multi:rN` sheets | the fish sheet key (`tf:`) | `parseSheetKey`, start links (`sheet=` in a link must keep working for old keys or be refused, said) |
| 7 | `seo/model.ts` groups + plain rows | trout VM | the site build |
| 8 | page-oracle (prototype v36) + keep.props parity | the shared vectors | conformance |
| 9 | store gate: required sections | optional `trout` (M0) | `store.ts:116-122` |
| 10 | Python: the group tree (`tree.py`, `card.py`), checked by `card_check.py` | the table builder, checked by J | both write words for the same fish until L; owner reviews two catalogues |
| 11 | release gate: `app_gate` derives `minAppVersion` from **file formats only** (`pipeline/deliver/release/app_reads.json`: `regs-*` = `regs-region/1`; no answers-section versions; `app_gate.py` has no `versions`/`layout`) [read] | `rows` 34 at the switch | The switch's `minAppVersion` is a hand stamp unless the gate learns section versions; an older app that installs a rows-34 release fails in the regs store instead of being asked to update [inferred] |

### 12.8 Count by kind (the 49 entries of 12.1–12.2, an entry counted under each kind it has)
[ran: counted from the tables] C 24 · F 22 · W 19 · D 6 · N 5 · B 5 · T 2 (plus 10 hand-written payload types in
12.5). Fates (an entry can carry two, e.g. "STAYS for other fish; trout part PY"): **OFF 22**, **PY 18**, **STAYS 10**,
**LAYOUT 4**. Two-source facts: 12. Side-by-side paths: 11.

### 12.9 The five worst places
1. **Dates from three TS engines** (`plainrow.ts:62-169` YearWords, `strip.ts:128-174` troutLine + the 21-day
   "Closed from", `caveats.ts:225-243` per-moment answers) beside Python's `DateSet`. On one screen the table's
   "until …" and the bar's "Trout release only until …" / "Closed from …" can disagree.
2. **TS rewrites Python's sentences**: `dutyWords` (`tree.ts:111`, "this river" → "the <name>") and `subcapLead`
   (`tree.ts:56`). Any reuse of the old composer pieces for the table carries these rewrites with it.
3. **Possession picked twice**: the page line's TS pick and regex with a silent null (`standing.ts:97-143`) and
   Python's per-fish possession (T805 B, then the table). The owner kept the page line, so both stay.
4. **"What is trout" decided twice**: `TROUTISH` / `splitRows` / `looseRows` (`others.ts:24-53`) against the table
   builder's row set. A fish can land in the table and the Other fish list, or in neither.
5. **The version gates are hand-typed and blind to section versions**: `versions.ts` (`SECTION_NAMES`, M as written
   refuses every release without `trout`) and `app_gate` (file formats only), so the rows-34 switch depends on a hand
   `minAppVersion` stamp.
