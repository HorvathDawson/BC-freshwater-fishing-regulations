# T810 — Trout and char block as the new table, built from the Python pipeline to the screen (umbrella; the spec is written, the build waits for the owner's go)

Status: OPEN, NOT STARTED beyond its first two steps (T805 = step 1, T809 = step 2). Nothing here holds any other fix
or a release (owner 2026-11-15).

Source: owner 2026-11-16 15:18: "have another opus agent spec out what would be involved in transitioning the trout
portion of the app to the new table format. want it implemented clean from pythion up to front end. etc etc. use opus
agent". The spec: `handoff/PLAN/design/trout-table-build-spec-2026-11-16.md` (tickets A to L for the main chat, M to S
for the app chat; this ticket stands for all of them until each is filed with its own number when it starts).

## The design to build
The trout mockup page, round 11 as corrected by round 12 (owner 15:16 and 15:36): the round 8 look with no outline;
today's rule in the bigger text, the coming change smaller underneath; rows between rounds 8 and 9; a coming closure
says what closes without the reopening date, and the reopening date shows only while it is closed today; a coming
change that only loosens a rule reads "rule changes <date>" with no details; Adams River shows the stricter 80 cm on
the row. Design record: `handoff/PLAN/design/trout-adversarial-review-2026-11-16.md` (four attacks; the fourth says
"yes, with conditions").

## Owner, 2026-11-16 15:42 (dialog on the spec's one-page summary)
- How to start: "Start steps 1 and 2" (T805 data gaps; T809 the changed-answers tool).
- Possession: "Keep the page line (Recommended)" (the trout table speaks only where a trout's possession differs).
- Size, if the switch does not fit the first-tap limit: "Raise the limit, shrink later (Recommended)".
- Other fish (kokanee, whitefish and the rest) in the same look: NOT ANSWERED ("what? we want to migrate the new
  trout style into the current app not the other way around. "): asked again.
- Steps 3 to 7 (the date rule, the table written in Python, the permanent proof, the app's table and sheet, the one
  switch) wait for his go.

## Standing conditions (from the spec)
Rules resolved in Python, the app only lays out; one load path; no silent fallbacks; built behind the current block
and switched in ONE release that drops the old card data (the first tap has almost no room for both); the old path
deleted after; every changed answer shown to him first (T809).

## Owner, 2026-11-16 15:47 (the other-fish question, asked again)
"we should just have the trout table under the trout entry like it currently is with the better text like 2 or 4 with
expand arrow etc. " Reading (main chat's): the page keeps its present shape. The Trout and char entry stays where it
is, folded to its short line with the better words ("Up to 2", "Up to 4") and the expand arrow; the new table opens
under it. The other fish entries are NOT redrawn: trout only. Consequences for the spec: its ticket Q (other fish on
the same table) is dropped; its ticket S cannot delete the old card code outright (it stays for the other fish; only
its trout path goes); the folded line of ticket O is the entry's own row, not a new block. Round 12's design answers
(15:36) are also his: a coming closure without the reopening date; the reopening date only while closed today;
"rule changes <date>" for a change that only loosens.

## Owner, 2026-11-16 15:48 (wild and hatchery rows)
"i dont like the new wild and hatchewry option you sent we should jsut say hatchery only unless specifically wild has
a seperate quota to wild like 1 wild and 2 hatchery or something.... otherwise it is hatrchery only or unspecified (the
same for both no mentiuon needed)". Rule for the table: same rule for both origins = no mention; keepable only when
hatchery = one row per fish saying "hatchery only", no wild row under Release; wild shown apart only when wild fish
have a quota of their own. The spec's row model (one row per fish and origin) changes accordingly.

## The accepted design, as one list (owner, 2026-11-16, rounds 10 to 14; his words are in OWNER-TODO.md)
1. Look: the round 8 tinted keep box with white rows and white boxes for tighter limits; NO outline or frame line.
2. Text: the rule in force today is the bigger, bold text; the coming change is smaller and quieter on its own line
   underneath. Row spacing between rounds 8 and 9.
3. Beside a fish with no tighter limit: "up to the N" (the box's number).
4. Dates: a coming closure says what closes ("then closed to trout and char" / "to all fishing" / for one fish) and
   no reopening date; the reopening date shows only while it is closed today; a coming change that only loosens a
   rule reads "rule changes <date>" with no details; otherwise the next stricter change is dated. A box's top line
   carries a date only when every fish in it shares that date.
5. Adams River: the stricter 80 cm on the row, the one-day rule in the fish's sheet.
6. Wild and hatchery: same rule for both = no mention. Hatchery only = ONE row per fish with a filled "HATCHERY ONLY"
   label beside the name, and no separate wild row; when it applies to every fish of a keep box, the box's top line
   carries the same filled label once and the rows carry none. Wild is shown apart only where wild fish have a
   quota or size of their own (Kitimat, Lois, Khartoum, Ruby, Cultus). Lois / Khartoum steelhead: hatchery only.
7. Possession: the page keeps its own line; the table speaks only where a trout's possession differs.
8. Place: under the existing Trout and char entry (folded line with the better words and the expand arrow); other
   fish untouched.
9. Fish sheet: "Here, today", "How to tell it", "Sources"; swipe down to close; no sideways swipe; no year bar except
   an all-release table.
Still open in the design (from the fourth attack): a build rule for dated number / size / wild / possession changes
on the waters not hand-written in the mockup; Lois's "All together" line sitting outside both boxes.

## Owner, 2026-11-16 16:37: work on the transition
Typed (on the Nation Lakes possession question): "i feel like this possession stuff is just kind of broken but fixed in
the new trout format? so maybe we can just work on the transisiton to the new trout redisgn?" Read as his go to move on
to steps 3 to 7. First: the spec takes in his answers of 15:36, 15:42, 15:47, 15:48 (trout only, the folded entry with
the expand arrow, no "reopens" unless closed today, "rule changes <date>" for a loosening, hatchery only), and the app
chat's read-only check of the app half (tickets M to S against the real app code; its worker started ~16:30).

## Owner, 2026-11-16 16:50: robust, end to end, no fighting
"we want to make sure the plan is robust so that we change the data all the way in python and the front end and it all
just works withotu fighting it and makign sure everythign is streamlined etc". Plan rule: Python decides every word,
number and date; the app draws only (no rule logic in TS, payload types generated from `contract.py`); one trout path
after the switch. Gate before build code: a fresh Opus review of the whole spec (after the app half is merged) for
every place the two sides could fight; findings fixed in the spec first.

## Owner, about 16:55 (app chat's batch, labels picked verbatim)
Fold row: "All rows get arrow" (every folded row, other fish too, gets words plus the expand arrow). Pinned trout top
line: "Drop it (Recommended)". Lone other fish beside trout: "Stays as today (Recommended)". Year bar's "Trout: release
until …" line: "Drop it (Recommended)". The app chat's sizing file section 12 lists every place today's trout card
decides in TypeScript (49 in core, 10 hand-written payload types, 12 facts with two sources, 11 old/new paths): input
for the robustness review (owner 16:50).
Clarified about 17:15 (app chat re-asked): "Salmon + trout folds": the arrow replaces "Details" on the salmon and trout
folded entries; the Other fish list stays as it is.

## Owner, 2026-11-16 17:40: the two open plan questions
Ship early: "No, only at the switch" (the section appears only in the switch release; previews through side
releases). Angler-only notes: "Page's notes block (Recommended)" (Q-D7: the table carries none).

## 2026-11-16 18:12: step 1 landed locally (I0 + A0, merge 1b9cce5d2)
I0: `pipeline/tests/test_answers_versions.py` pins every answers section version + glossary against the app's
`KNOWN_VERSIONS` / `OPTIONAL_VERSIONS` (8 mutations fail it); `AWAITING_ONE_SIDE = {trout: 1}` must be deleted by
whichever of M0 or I lands second; `app/packages/core/src/regsCell.generated.ts` (CellIds, CELL_COLUMNS) emitted by
pack.py under --check. A0: 622 mixed trout+other rows (Python splits them at rows 34; contract amended), 0 angler-only
trout closures, 0 hours-window trout changes; cutover regs-5 largest, 22-51 kB over first-tap (raised at the switch).

## 2026-11-16 21:02: F built (lane/trout-f, merged locally; 5,950 tables over r-46e4355a7277, 0 refused)
Questions F leaves for G2 (the owner's sentence catalogue; NOT asked yet): (1) Echoes: box head "Keep 5 in all" with
"only 1 of these 6" under the top line "Up to 1 {day} // only 1 from both lakes together"; (2) the band form "only 3
from these 2 together" (Tagish); (3) some row size caps reuse the tree's sentences J cannot parse ("up to 1 cutthroat
trout over 50 cm (20 in)", Kamloops' "none over 50 cm (20 in) if you've already kept …"): word them through G; (4)
strip parts joined by "; " with "in your possession: …" lower-case (contract showed "In your possession: …"); (5)
plus-form fold says only "Up to 2" (Kaslo "Up to 2 {day}, plus 2 Dolly Varden/bull trout"); (6) "any size" on a row
unless the box has a size strip; (7) stop-duty noun from the stored duty ("this stream" vs "this river"); (8) Fraser
R3 Oct 15: "reopens Nov 1" per row, not under the top line (steelhead closed all year never reopen); (9) twin wild
rows "up to the 4" with the strip "only 2 wild, each 40 cm (16 in) or less" (Windsor, Dodd); (10) possession below
the day's number: band "only 2 in your possession" + quiet line (Cunningham). Checked: Bowron LAKE reads 2 a day
correctly (book p.43: Bowron Lake's own row says 2; the 1 is for the other park waters). Left for H: `elsewhere`, the
fish sheet's licence/stamp line, "today" wording in sheet lines. Left for I: wire models, FRAMES["trout"]; I must
compose before L drops `group` from covered rows.

## 2026-11-16 23:30: J built (lane/trout-j): the proof, inside the card gate
`answers/trout/check.py` reads every shipped table back with G's grammar (extended in `words.py` to every text the
table, its dates and sheets say, the card's reused size / also / kept-elsewhere / possession / weekday words included)
and proves it against today's answers and the keeping engine, every key and segment: NULL per Q-D3, `covers`, every
fish once, state and origin words, each fish alone at every length (engine), the joint limits through the engine's
basket proof (`check_caps`' method), possession vs F0's lead (`card_check.possession_problems(lead=…)`), T793 / T805
re-aimed, top line and fold numbers, every date against the following segments (and the sheet's), the sheets, the year
bar, rule refs, kept-elsewhere numbers. Runs in `card_check --all` whenever the answers hold the trout section; the
stamp gains `trout_check` and the store refuses a stamp that checked fewer tables than non-NULL trout cells.
Full flagged side build over the live bundle: 5,950 tables of 5,950 non-NULL cells. It first found ~630 keys with
faults in F's builder, fixed on the lane (each pinned): a kind's number drawn as each fish's own ("only 4" each in a
box of 5), a kind's size cap drawn per fish ("up to 1 over 50 cm" on each row), "uses the 1" on a steelhead of a
non-steelhead water, a twin wild row missing its own quota, "only 2 wild" in a strip that holds only some wild fish,
boxes-apart top lines saying the region's total instead of the box's most. LEFT (5 keys, the proof fails them):
(1) Kaslo River and Keen Creek from Jun 15: "Up to 2 {day}, plus 2 Dolly Varden/bull trout" with DV outside the
"2 of these 5" band; the engine keeps 2 in all (DV counts toward the 2 from streams) — F's question 5; (2) Kitimat
River (2 keys) and Humphrys / Iron Mine / Tetlock Creeks from Jul 1: the wild rainbow's "only 1 wild" is not joined
with brown and cutthroat's "only 1 of these 2"; the engine holds the three to 1 together.

## 2026-11-17 00:24: J's 5 keys fixed in the builder (lane/trout-vpy)
Full flagged side build over the live bundle again (answers `--trout-table` + `card_check --all`): 5,950 tables of
5,950 non-NULL cells, 0 problems (card gate 0). Six keys changed, nothing else: the five J listed plus a third Kitimat
River part the random baskets had missed. Both fixes pinned in `test_trout_check.py` FOUND (flag off: the live water
fails the proof).
- `A_SHARE_HOLDS_ITS_UNLIFTED_FISH` (Kaslo River, Keen Creek, from Jun 15): the stream share's band holds every fish
  its rule holds that no lift takes out (`group.subcaps[].of`). Words chosen: top line "Up to 2 {day}" (no plus form at
  all), fold "Up to 2", one band "only 2 of these 6" over the five trout and the Dolly Varden; the Dolly Varden's own 2
  is not said (it is not tighter). F's open question 5 (the plus-form fold) stays for G2: the plus form still exists
  where a fish IS lifted out of the share.
  FOR THE OWNER (data, not the table): Kaslo and Keen print "Bull trout daily quota = 2 (any size)", the same words as
  the Lardeau and the Duncan, but only the Lardeau's and the Duncan's rows carry the lift out of Region 4's "2 from
  streams" (T3a names them). So today the engine keeps 2 in all on Kaslo and Keen, while today's shipped card says
  "Not part of the 2 from streams" there (`tree.py` nests a water's own tier by its number only). If the owner reads
  Kaslo and Keen like the Lardeau, it is a corpus edit (an `exempts` on `kaslo_river.r1` / `keen_creek.r1`) and a
  rebuild; the table follows by itself (it reads the lift).
- `A_WILD_QUOTA_JOINS_ITS_COUNT` (Kitimat River, its tributaries Humphrys / Iron Mine / Tetlock and the rest, Jul 1 to
  Oct 31): Region 6's "1 trout from streams" holds brown, cutthroat and WILD rainbow trout together (Kitimat's hatchery
  rainbow quota lifts hatchery fish only). Words: one band "only 1 of these 3" over brown, cutthroat and the wild
  rainbow row; the wild row's "only 1 wild" is no longer said; the twin rainbow rows stay side by side (wild, then
  hatchery).

## 2026-11-17 01:51: V-py built (lane/trout-vpy): the shared fixture, Python half
- `python -m pipeline.tools.emit.trout_vectors` (registered in `pipeline.tools.emit --check` and fast-fail) writes
  `app/fixtures/trout/trout-vectors.generated.json`: 53 cases (the contract 2.8 list: Skeena x3, Lois, Khartoum,
  Windsor, Dodd, Horseshoe, Nanton, Ireland, Shamrock, Kilgard, Echoes, Bowron, Istsi, Nation Lakes, Cunningham, Tagish,
  Takla, Buckinghorse, Fraser R2 Mission, Fraser R3 Sep/Oct/Nov 15, Bish, Adams Jul 15/31, Kamloops Lake Jan/Jul, Elk
  Nov/Apr/Jul, Kootenay main, Glenn, Madden, Kaslo, Keen, Vedder, Ross, Kitimat Jul 15/Nov 1, Humphrys Jul 15/Nov 1,
  Ruby, Cultus, Bella Coola, Andi, Denetiah, Shuswap, Bruer, Mat, Burnt, Nitinat tidal), each anchored by stream key +
  start metre or by its lake, with `s`, the decoded frame (null only for Elk Apr 15 and tidal Nitinat) and `composed`.
  `drafts` is empty (the builder exists).
- The vectors decode from a TROUT FIXTURE of their own, `app/fixtures/trout/regs-release/` + `section-lines.json`
  (1.7 MB), cut by `regs_fixture --trout-cases` from side release r-6a81c322c649 (the live bundle, `--trout-table`):
  until the app's M0 the app's own regs fixture and goldens must stay the normal release's (contract 2.8 amended, "As
  built").
- `app/fixtures/trout/trout-hand.json`: 14 hand cases on one valid base frame; the model refuses 13 for their own
  reason, the dangling rule index is refused by the rule table (`rule_count`).
- `scripts/trout-refresh.sh` (via heavy.sh): side release -> fixture -> vectors -> golden sync (skipped and said until
  `versions.ts` lists `trout`) -> every `--check` + the cut again byte for byte. Run twice on r-6a81c322c649: no change.
  The side release is over the web first-tap size gate (4,273,307 > 4,200,000 B; low 3,433,932 > 3,400,000): the script
  passes those two gates only, said, for a side release.
- `deploy/staging/test-updates.mjs side <release dir>`: the app reads every release file relative to `<origin>/release/`
  on the test channel too, so the scenario names its files `../test/release/<id>/…`; the staging data worker resolves
  the dot segment itself (`deploy/staging/test-updates.test.mjs`). Nothing uploaded; not walked on a device.
- Not in the case list yet: an unnamed Region 6 stream and one of the 8 rule sets with no stored headline (both need a
  handle or a find step the cut does not have).

2026-11-17 08:50, owner on Kaslo/Keen (verbatim): "Dollys and bulls always have same quota per the book.  Why would they
not count towards the regions 2? That seems wrong". Kaslo and Keen: B (count toward the 2 from streams; the table as
built is right; the shipped card's "Not part of the 2 from streams" line there is wrong and goes at the cutover).
Follow-up asked: does T3a (Lardeau, Duncan lift) stand.

## 2026-11-17 ~15:41: G2 answered by the owner (OWNER-TODO 15:34 and 15:41, verbatim there)
Q1 what you keep here; Q2 "between/among them" (box number: "up to the N"); Q3a "only 1 over 50 cm"; Q3b once in the top line + Tagish lake trout "release 60-90 cm" form (exact book bounds); Q4 own line "In your possession"; Q5 gone (T834); Q6 "any size" once in the top line; Q7 the water's own kind, else "this water"; Q8 reopens once, steelhead "closed all year"; Q9 group the wild rows with clearer words than "only 2 of these 4", wild vs hatchery more obvious (FOLLOW-UP MOCKUP OWED), Dodd lake into the hard cases; Q10 box + one quiet line. Bugs to fix: Echoes double "both lakes together"; Fraser R3 closed table year bar. Pictures: scratchpad t810-g2/.

2026-11-17 ~15:46, owner: rows with no limit of their own say "any of the 5" (not "up to the 5"); applies to G2 Q2 and Q9 too. Verbatim in OWNER-TODO 15:46.

2026-11-17 ~15:50, owner Q9: option 1, badges + "only 2 wild in all" (hatchery rows first, then wild rows grouped; filled HATCHERY / outlined WILD badge, a look change in O). OWNER-TODO 15:50.
## 2026-11-17 14:10: M0 + M built (app chat, branch app/t810-m0-m)
- M0: `OPTIONAL_VERSIONS = Object.freeze({ trout: 1 } as const)` in `core/src/regs/versions.ts`, in I0's line shape,
  with the bump rule beside it. `openRegsStore` walks it. An absent section is fine. A section at another version is
  refused, and so is a manifest that disagrees with the water file. `meta.optional` records what the store holds. A
  region file missing a held section's `cell` column is refused as `damaged`, because the release must be all or
  nothing. `readsRelease` holds `regulations.versions` to `KNOWN_VERSIONS` and `OPTIONAL_VERSIONS` at install. A
  manifest with no versions passes, and so does a section the app does not know. `store.test.ts` no longer hand-types
  versions.
- M: core's `CellIds` is the emitted one. I0's `regsCell.generated.ts` already emits it, so the emitter is unchanged.
  `keyCells` and `cellOf` use `CELL_COLUMNS`, plus `trout` only when the store holds the section. `reads.trout(akey,
  id)` reads the four families through the generic `familyOf` / `blob`, typed with the emitted wire types. It returns
  `TroutValue`, which is the emitted name for Python's `TroutFrame`. `RegsInputs.trout` is today's cell: null for a
  NULL cell, absent when the release has no section or the tap has no cell. Nothing draws it yet.
- Pipeline file touched: `pipeline/tests/test_answers_versions.py`. M0 was the second side to list `trout`, so it
  emptied `AWAITING_ONE_SIDE`, as that file asks. The allowance's mutation pins now pass an explicit `_TROUT_WAITING`.
- Tests: `store.test.ts` and `manifest.readable.test.ts`. New `reads.trout.test.ts` checks the trout fixture (every
  cell decodes, NULL stays null, the SELECT is `CELL_COLUMNS` plus `trout`) and the normal regs fixture (no `trout`
  anywhere). `cellIds.test.ts` checks the keys. `inputs.release.test.ts` checks the G proof: on all 53 vectors,
  `inp.s` and `inp.trout` equal Python's. 10 mutations of the new guards each fail a test.
- Next for the main chat: `scripts/trout-refresh.sh` step 4 (the golden sync) now RUNS, because `versions.ts` lists
  `trout`. The conformance checks of the golden versions now accept an optional section at `OPTIONAL_VERSIONS`.

## 2026-11-17 21:05: A1 measured (lane/t810-a1): the cutover form fits every gate, no raise
`python -m pipeline.tools.audit.trout_size <side> --cutover --live <release>` (via heavy.sh) on side release
r-eaddd042e618 (live bundle, `--trout-table`) beside live r-46e4355a7277. Every region file is written again by the
release's own writer from the reassembled store: side = the shipped bytes (checked), flag off = the store less the
section (exact on the mini release), cutover = rows 34 (1,878 mixed rows split, covers equal to the shipped ones;
`group` / `elsewhere` cut from 1,418 covered rows; F0's `page_possession` on 4,330 of 4,340 frames; the release's own
dictionaries, not retrained). gz bytes:

| file | live | flag off | side | cutover | side − off | cutover − off | F0 slot alone |
|---|---|---|---|---|---|---|---|
| regs-1 | 300,920 | 302,923 | 312,424 | 300,066 | +9,501 | −2,857 | +25,788 |
| regs-2 | 264,805 | 260,787 | 275,442 | 236,705 | +14,655 | −24,082 | +22,282 |
| regs-3 | 351,239 | 368,361 | 400,801 | 289,843 | +32,440 | −78,518 | +28,724 |
| regs-4 | 341,816 | 345,041 | 364,618 | 332,810 | +19,577 | −12,231 | +33,605 |
| regs-5 | 387,155 | 395,834 | 443,715 | 324,613 | +47,881 | −71,221 | +21,376 |
| regs-6 | 413,612 | 413,008 | 442,945 | 360,496 | +29,937 | −52,512 | +31,791 |
| regs-7a | 315,434 | 289,915 | 317,272 | 223,960 | +27,357 | −65,955 | +14,380 |
| regs-7b | 144,334 | 159,637 | 173,957 | 149,628 | +14,320 | −10,009 | +23,162 |
| regs-8 | 149,423 | 149,274 | 158,967 | 138,020 | +9,693 | −11,254 | +11,673 |
| water-shared | 745,970 | 746,537 | 761,788 | 761,788 | +15,251 | +15,251 | |
| water | 2,570,279 | 2,571,033 | 2,586,362 | 2,586,362 | +15,329 | +15,329 | |

The four dictionaries alone (gz): trout_dates 2,159, trout_frame 5,829, trout_sheet 2,448, trout_table 2,814; the
water slices +75 to +100 B each. Largest region file: flag off regs-6, side regs-5, cutover regs-6.

| WEB_BUDGET at HEAD | gate | live | flag off | side | cutover | cutover room |
|---|---|---|---|---|---|---|
| first_tap | 4,400,000 | 4,198,757 | 4,293,775 | 4,339,833 | 4,256,614 | +143,386 |
| first_tap_low | 3,600,000 | 3,359,382 | 3,454,360 | 3,500,418 | 3,417,199 | +182,801 |
| first_tap_unnamed | 6,200,000 | 6,018,006 | 6,113,024 | 6,159,082 | 6,075,863 | +124,137 |
| cold_load | 3,350,000 | 3,042,537 | 3,042,252 | 3,072,959 | 2,989,740 | +360,260 |
| first_paint / first_search / conditions_open | | | | unchanged by the section | | |

Offline total: flag off 18,066,926, side 18,287,616, cutover 17,753,616 (gate 20,000,000). No file over its own gate.
VERDICT: the cutover is under every gate, so L raises no limit (first tap stays 4,400,000). The side form (decision 1)
would also have fit: +46,058 B on the first tap, 60,167 B under the gate. Soft target met in every region (cutover <
flag off). Levers if room is needed later: F0's slot is stored inline per rows frame (+11 to +34 kB gz per region);
interning it would recover most of that. Largest bodies: Lois / Khartoum 297 B stored (11 rows), Vaseux Creek,
Okanagan River, Pitt / Chilliwack / Cultus / Horseshoe / Ireland / Lillooet ~195 B; largest sheets: Vaseux Creek
rainbow 94 B, Tagish / Morley / Bennett / Teslin lake trout ~80 B (full list in the report). For L: `shards.frame_refs`
unpacks a rows frame as exactly five slots, so a rows-34 frame (six) breaks the shard cut until it learns the slot.
## 2026-11-17 21:33: owner signs off the picture pack (R), with one wording change
Verbatim in OWNER-TODO 21:33. (1) Rows with no limit of their own say "counts toward the 2" (replacing "any of the 2",
the 15:46 wording): Python's words (G2 builder, main chat); the app re-cuts the a11y golden and vectors after. (2) The
table is signed off for the cutover, "also make sure to do the source table and the species row expansion when you
click on one with the sources for that entry plus identification": P's fish sheet (`tf:<row key>`: "Here, today", "How
to tell it", the row's own Sources) and "Sources ›" (`table.src`) must be in the cutover build; the app chat shows him
the sheet pictures before switching. R built on app/t810-r bb403c97f (selectors match old card or table; scenario
`trout-table` written, run owed on the heavy lane with the side release on staging test/).

## 2026-11-17 23:29: owner on the cutover look (verbatim in OWNER-TODO 23:29)
The app's gates had cut the top line to 17 px and removed the fish tile (cutover a107d81cd); the owner keeps both as he
signed off: "Keep it big, 23 px (Recommended)", "Keep the tile (Recommended)" — applied as named gate allowances
(branch app/t810-cutover-keep-look 8ecf8d9ac: SIZE_ALLOW "trout-top"; census ALLOW "picture" for keep-tile and
keep-tile-more). The pin bar has nothing to pin after the switch: "Remove it (Recommended)" (S2 deletes useAnchors,
PinBar, the regs:rule CoachSpots). `elsewhere` on a table covering several keep rows: "Yes, name the fish
(Recommended)" — each line starts with its fish where a table has more than one such line; Python writes the words (L).

## 2026-11-18 00:35: owner on the simplify examples (verbatim in OWNER-TODO 00:35)
Pictures: 6 before/after (main chat's owner-examples.md; real web export, afters edited in the DOM). R4 cutthroat +
rainbow: one band "counts toward the 2 · only 1 over 50 cm (20 in) between them" (yes). Bowron park creeks: one box line
"In your possession: just 1, the same as the day" (yes). NOT approved: dropping the DV/LT "only 1, counting any …" lines
(he wants the inferred "only 1 between them" STATED once as a shared band, the reason kept). Owed: plainer band words
("only 2 of these 6" reads poorly) and a band that visibly spans all its rows (app drawing); hatchery + wild of one
species drawn as one species group with one combined count; HATCHERY and WILD badges in the same spot (app); "only 4
wild" goes (confirm on the next pictures). Next pictures from the app chat.

## 2026-11-18 00:52: owner on round 2 (verbatim in OWNER-TODO 00:52)
Approved: the shared two-row box "counts toward the 2 · only 1 between them" (Fraser); 3+ row box words A "K total from
these M" (was "only K of these M"); Group A for hatchery + wild of one fish (one row, one count). Owed, with pictures
from the app chat: groups drawn as sub-groups of the grey card (words in the grey, white rows inside; no empty white
band cells; no group splitting another; ungrouped rows first); the Vedder hatchery-rainbow 4 apart as the river's own
rule; "uses the 1 over 50 cm" dropped (the strip says it); a date shared by every row said once; lighter HATCHERY/WILD
tags.

## 2026-11-18 01:07: owner on round 3 (verbatim in OWNER-TODO)
Neither group look: keep the table's form (count on the right on every row, fish on the left) with a white box spanning
all of a group's rows (round 2's look), ungrouped rows first, inner group last inside its outer one; rainbow above
steelhead unless a water's own rainbow rule. Vedder's "River rule" apart doubted and the hatchery rainbow's "counts
toward the 4" reads oddly: options owed. Hatchery/wild tags: neither; options owed. Chilliwack brook trout: the main
chat's C2 change (owner) gives wild brook trout "50 cm or less" at the six lakes; wild lake trout release-only there.

## 2026-11-18 01:22: owner on round 4 (verbatim in OWNER-TODO)
APPROVED everywhere: the count-column rules (R1 "counts toward the N"; R2 "counts toward the N · only k"; R3 one white
box spanning its rows, "counts toward the N · only k between them" for 2 rows, "k total from these m" for 3+; R4 nested
"only 1 between them"; R5 "uses the 1 over 50 cm" and "only 1, counting any …" gone, the result stated once); "any size"
off the box head; ungrouped rows first, inner groups last; hatchery + wild as one row "50 cm (20 in) or less if wild"
(no tags). Vedder: lead with 2 ("only one is an exception"): top line form owed, picture next.

## 2026-11-18 01:28: owner on Vedder and three look questions (verbatim in OWNER-TODO)
Vedder: "Keep 2 trout and char today" + subline "or 4 if they're hatchery rainbow trout 50 cm (20 in) or less", the
rainbow row LAST (V2). Char HATCHERY ONLY: leave as is. "in your possession": keep. Madden's DV clause: he asks if it can
go (it is 2, as is the general limit); the main chat to answer whether the DV 2 sits outside the general 2.
