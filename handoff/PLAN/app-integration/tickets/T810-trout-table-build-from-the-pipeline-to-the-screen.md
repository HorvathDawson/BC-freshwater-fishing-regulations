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

2026-11-17 ~15:47, owner: rows with no limit of their own say "any of the 5" (not "up to the 5"); applies to G2 Q2 and Q9 too. Verbatim in OWNER-TODO 15:47.
