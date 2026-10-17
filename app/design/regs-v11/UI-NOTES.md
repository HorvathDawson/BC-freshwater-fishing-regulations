# regs v18 (round 16) — in place on regs-v11.html

- **5 Gear in labelled lines.** The gear sentence ("1 line (2 while alone in a boat) with one hook, lure or fly. Weight on
  the line up to 1 kg. Book p.8") is drawn from its own parts (gear `summary.parts`; snap.mjs now copies them): a fly-only
  mode first, on its own in the restricted colour ("Fly fishing only ⓘ"), then **Lines · Hooks · Weight on the line**,
  label in a fixed 7.4em column (the dated rows' column), value big. Hooks = the `attach` words then the `hook` words on a
  second line ("One hook, lure or fly / Single barbless hook ⓘ"). A part the sentence leaves out (no hook restriction:
  "any hook", Kootenay Lake Main Body) is not drawn; a value repeating its label drops it ("Up to 1 kg"), a leading "a"
  goes. Each part's ⓘ on its line; the book pages once, under the lines; "Different on other days" after them, carrying
  any ⓘ only a change needs. Check `gear-lines` (Vedder Oct 10 / May 15, Bella Coola May 1, Kootenay LWA, Main Body);
  `gear-sentence` / `gear-sentence-plain` now read the lines. Screens 104-106.
- **2 How it adds up closes its group.** After the buckets AND the not-kept lines (Ape Creek May 1 had it between the
  trout and the steelhead / char lines), opening on a 1.5 px ink rule so it reads as the group's sum, not a note on the
  line above (the round 15 persona trip it was moved up for). Its words ("This river's own limit. …") are the data's;
  making them specific is DATA's (round 16 item 2). Check `worked-last`; `no-bare-zero` no longer holds the old order.
  Screen 107.
- **1 The quota table is each group's display** (rows 15 `group.table` / `cap_kinds` / `caps`, page v36 @ b934dc8b,
  re-snapped, 134 states; the round 15 buckets stay at `#qt=0` for comparison and for the older checks).
  - **Table.** Rows = the table's fish (picture, name, the kind's notes / duties / steelhead presence small under it);
    columns = the union of every row's cut points; a row's band spans the columns it covers and, when it spans 2+, says
    its band small under the number (persona: a number centred over columns hid its band). Cell = the data's `n` big,
    "no limit" for null, else its `why` word (released / closed / too small / too big / protected size); "hatchery
    only" small on a cell whose `origin` says so (persona: with the mark left to the origin line, Vedder's cutthroat 2
    read as wild fish), `by_origin` as "hatchery N · wild M". On a steelhead water the rainbow row stops at
    `steelhead_cm` and the steelhead row starts there: the other span reads "over 50 cm is a steelhead" / "under 50 cm
    is a rainbow trout" (`as`), neighbouring `as` bands one span.
  - **Shared limits** under it: every cap that keeps some (take > 0) over two or more fish, once, the data's `says`,
    then whose rule (the rule's level: "This water’s rule", "Region 5 rule") · Book page. Then the water's own-limit
    sentence (`elsewhere.says`).
  - **Check my catch**: fish ▾, length, Wild | Hatchery (only where origin changes a fish someone may keep), "Where is
    this fish?" Here | Kept on another stream today | Kept on a lake today. "Can I keep it?" evaluates the data's caps
    as given: a basket {here, stream, lake: {kind: count}} is legal iff every cap's count ≤ take, an `if_kept_here` cap
    only once the basket keeps one of its here kinds; fish kept elsewhere first, then here; a rainbow at / over the
    steelhead size is the steelhead's kind. Kept: "✓ You can keep it", the table re-reads as "Still yours here" (each
    cell = the most more of its kinds that keeps the basket legal: a small search, bounded by each kind's smallest cap);
    "none left" where a cap is spent. Not keepable: not added; "Release it." + the failing cap's words (a take-0 cap
    first) + "You have N toward it already." + whose · Book page. A length exactly on a cut point is read as the longer
    band and says so (the data's bands carry no inclusive / exclusive end; a question for DATA).
  - **Proof** (`tools/oracle.mjs`, run after snap.mjs → `oracle.json`): seeded random baskets (150 per group) plus each
    cap at its take and one past it, for Bella Coola May 1 (both parts), Vedder Oct 10 and Bennett Lake (11 groups,
    1,906 baskets), judged by `pipeline.deliver.answers.caps.accepts`, and every cell's `most_here`. Checks
    `table-is-the-data` (every cell = the data's n / why, `as` spans; Bella Coola ×2, Vedder, Bennett, Ape Creek),
    `checker-is-the-caps` (the page agrees with accepts on every basket and with most_here on every cell),
    `check-my-catch` (Bella Coola: a 30 cm bull trout is 1; after a 30 cm cutthroat no rainbow, in the cap's words, but a
    30 cm brook trout; a 55 cm brown kept on another stream leaves no fish over 50 cm here; Vedder: a wild 35 cm cutthroat
    released, a hatchery one kept; each shared limit once with its Book page). The round 9-15 bucket checks read #qt=0.
  - Persona (haiku): "how many 30 cm bull trout on the Bella Coola May 1?" → 1 (DV row, 25–50 cm); "kept one 55 cm brown
    trout — what else?" → one more trout or char 25–50 cm, none over 50. Fixed from its trips: band words in spanning
    cells, per-cell "hatchery only", "is a steelhead" wording, place toggle labelled. Left for DATA: Vedder's "Up to 2
    hatchery trout and char a day from Region 2 streams" excludes rainbow under 50 cm but says "trout and char" (read as
    contradicting rainbow 4); "No wild steelhead may be kept in B.C.." (double stop); the shared CT/RB "1" still reads as
    two 1s in the table until the shared line is read.
  - Screens 108-116 (Bella Coola May 1 side channels / limits / after a cutthroat / a release reason / mainstem / after a
    55 cm brown; Vedder Oct 10; Ape Creek May 1; Bennett Lake).
- **3 Rule titles** (display 12 `rules[].title`, snap: `ruleInfo.title`): a sources sheet's rule is headed by its effect
  ("Keep up to 2 trout and char a day (none under 25 cm), Apr 1–May 31"), the book's words under it as the quote. Check
  `rule-titles`. Screen 117.
- **Q1 Caution** (`inherited[].caution`): under the inherited line, small. Glenn Creek May 1 snapshotted (pill). Check
  `inherited-caution`. Screen 118.
- **Q2 Restatement** (`rows[].restates` from another row): one ladder line, the winner's, with the data's note under it
  ("Zone B also prints this: …"); snap adds `of` to `ruleKey`. Check `restates-one-line` (Schooler Creek grayling).
  Screen 119.
- **4 Under 16** (licence 6): the data's book words render as given (`u16-by-residency`). Screen 120.

# regs v17 (round 15) — in place on regs-v11.html

- **Follow-up (page v36 @ 388b2b7b, 41 waters, 132 states).** Glenn Creek (a Trout Lake tributary: bull trout released
  by "Trout Lake’s rule for its tributaries") and Hamill Creek (a Duncan tributary: the Duncan River’s 2 bull trout),
  snapshotted on Oct 10 and Jun 15 (`w=glenn`, `w=hamill`), with Jun 15 quick pills. A closed card says each sentence
  once: the year strip's "Closed" / "Opens Jul 1" first; a banner headline phrase, banner sentence or link sentence
  saying the same words (case, punctuation, "again" aside) is not drawn (Vedder Jun 15, Cowichan mouth). Checks
  `closed-card-once`, `nearer-row-tribs-front`. Screens 100-103.

- **Phase 2 (rows 14, licence 5, display 11; page v36 @ b881e931, re-snapped, 128 states).** snap.mjs also copies
  `rows[].restates`, each answer's `profile_labels` label (`licLabel`), the licence frame's `under_16` (`u16`), and the
  `by` of every size group, weekday part rule and restatement into `ruleKey`; it reads each water back in 1 MB slices
  under a timeout (a 4 MB slice of the Fraser hung the DevTools socket) and names each water before it starts.
  **1 Size groups** (`buckets[].sizes`): the count group's box (`.qgrp.sgrp`): the kinds it names, its number big, its
  rule's Book page, a picture of each fish it binds; the kind rows stay in the bucket. Check `size-group-box`.
  **5 Shared cap** (`buckets[].between`): big under the own-rule bucket's title, like a share bucket ("Up to **2** a day
  between them"). Check `bucket-between-big`. **2 Under 16**: the open answer's label is the data's profile label
  ("B.C. resident, under 16"); on the Under 16 tab page v36's "Under 16, by where you live" list (one item per residency,
  label + words, the tab's own marked ✓) sits OPEN right under the answer; it and "Showing the answer for" come out of
  every other answer (dropdowns keep the detail). Check `u16-by-residency`. **3 Weekday sheet**: status box = the weekday,
  the data's `week.status` ("Weekends: keep 5 · Weekdays: release"), the sentence; "How this was decided" lists each
  `week.parts` as a rule line (days, words, "Today" tag, where it applies, Book page) then the restatement (combined quota
  with the Upper West Arm) once; page v36's own parts/notes under the sentence are not repeated. The Other fish line shows
  `week.status` under "Kokanee". Check `week-parts-decided`. 6/7/8 words (tributaries rule, no presence when closed,
  "hatchery, …", "the") are the data's and render as given. Screens 85, 86, 89-99 (all re-shot).
  Persona (haiku): a kind's number repeating its bucket's shared number (2+ kinds) is not drawn (Ape Creek May 1 "1 a
  day" over brown 1 + rainbow 1 read as 2), a kind keeping none draws no 0 (its "catch and release" in the release
  colour), and "How it adds up" now precedes the not-kept lines (Vedder May 15 "Keep 2 more here" read as about the
  released rainbows); held by `no-bare-zero`. `bucket-note-once` / `every-cap-once` count size groups as caps.

- **DO NOW (render-only, page v36 @ 545ac9fd snapshot).** **8 No bare 0**: a not-kept RELEASE line draws no number (its
  words, in the release colour, say it); only a closure keeps its ✕. A big 0 under the buckets read as a total above "How
  it adds up" ("Keep 2 more here", Vedder May 15). A kind's 0 inside a bucket's column of counts stays. Check
  `no-bare-zero` (`not-kept-said` now wants no mark on a release line). **8 Licence for whom**: the open answer has a
  label right above it, the page's own residency and age names ("B.C. resident · Under 16"). Check
  `licence-says-residency`. **3 Weekday sheet**: a row with `rows[].week` opens its sheet with ONE status block (the day,
  page v36's number or pill, the data's weekday sentence, and each day-part any `week` array gives, today's marked), then
  "How this was decided"; a rule line repeating the one above it in the same step is drawn once (every sheet). Check
  `week-sheet-one-story`. Screens 84-90.

# regs v16 (round 14) — in place on regs-v11.html

- **Phase 2 (rows 13, display 10; page v36 @ 545ac9fd, re-snapped, 128 states).** snap.mjs now also copies each row's
  `presence` and `week`, the frame's `steelhead_line` (`steelLine`), and every nested group's `by` into `ruleKey`.
  **3 Nested count groups** (`buckets[].groups`): first in their bucket, before the size notes and the other kinds, a
  box: the group's words with its number big ("Cutthroat trout or rainbow trout: **1** between them"), its rule's book
  page, then each of its kinds with its own size; a kind's number that only repeats the group's is not drawn (persona: two
  1s read as 2). Check `nested-count-group`. **4 Steelhead presence** in its own section, once: the kind row
  (`kinds[].presence`, also inside a group), the not-kept line (`not_kept[].presence`), or the row (page v36's own HTML;
  a row page v36 draws as a kind of another card, Kitimat's steelhead, puts it on that kind row). The card-level quiet
  line is only page v36's `known_no_rules` one. A closed day (Vedder Jun 15) draws no sections, so no presence line.
  Check `presence-in-section`. **5 Weekdays in words**: the kokanee row says "Keep up to 5 on Saturdays and Sundays;
  release Monday to Friday." (page v36's `.sum.week`, no date list); the day's one-word verdict beside it in "How this
  was decided" is dropped. Check `week-in-words` (Sat + Mon). **6** Not-kept lines in the data's order and words
  (Vedder May 15 one rainbow + cutthroat line; Ape Creek char with members). Check `not-kept-merged`. Screens 81-87.

- **2 Licence, Under 16.** The Under 16 tab shows the default angler's (B.C. resident) under-16 answer as the same open
  "You need" list as 16 or older: page v36 gives it as a box ("No licence needed to fish here" + its sentence), which is
  now an item of a list under the page's own "You need" label (read from the 16-or-older answer); a box both ages share
  (a classified water's Class I/II box) stays a box. Nothing about it is folded. Other residencies stay in their "Rules
  for other anglers · <residency>" dropdowns, the tab's own age first. Check `licence-u16-you-need` (Vedder + a classified
  water, both tabs). Screen 71 renamed 71-licence-vedder-u16-you-need.

# regs v15 (round 13, owner H L B) — in place on regs-v11.html

- **Phase 2 (rows 12, gear 9, display 9; page v36 @ c4eecc9e, 128 states).** D: the lift box is gone; a water's
  own-rule bucket says what it replaces (`buckets[].replaces.says`) right under its title. S: `group.not_kept` after the
  buckets, one kind-style row per deciding rule: the data's words, whose rule (capitalised) and its Book page, the fish
  pictured; a closure is ✕ in the closed colour, a release 0 in the release colour (snap.mjs adds each `by` to
  `ruleKey`). Z: a kind keeping none shows the data's "catch and release" and its 0 in the release colour. G/P: the gear
  sentence and the lake line are the data's words. Ape Creek (a Bella Coola tributary, `w=ape`) on Oct 10 and May 1 with
  quick pills. Checks `no-lift-box`, `bucket-replaces`, `not-kept-said`, `zero-kind-no-size`, `gear-no-any-hook`,
  `ape-creek-front` (`card-one-book-per-lift` retired). Persona (haiku): sizes and captions no longer break before
  "cm" / "›". Screens 76-80.
- **H Header, one shape.** No compact mode: the header is the same height and words at the top and scrolled (only the
  tab in view changes); "View on map ›" is always shown. The two section tabs are slim underline tabs (the app's TabBar
  look: the chosen one in ink on a 3 px ink bar sitting on the header's rule), 38 px instead of a 47 px boxed control.
  Check `header-one-shape` (retires `compact-header-place`).
- **L Licence.** Each tab (16 or older / Under 16) shows only the default angler's answer (B.C. resident, the page's
  default). Every other residency is a closed dropdown of its own, "Rules for other anglers · <residency>" (the page's
  fold name and residency names), carrying that residency's 16-or-older AND under-16 answers; the page's own "Rules for
  other anglers (N)" fold is replaced by these. snap.mjs now also reads `adult_non` and `adult_alien`. Each answer's
  "Where each one comes from" goes to the Sources sheet under its residency · age. Check `licence-others-in-dropdowns`.
- **B Bella Coola.** Snapshotted on Oct 10 and May 1 (inside the mainstem's Apr 1–May 31 exception), both parts the
  page offers (mouth → Talchako; the 4 side channels); page v36 offers no Bella Coola tributary water. `w=bellacoola`.
  Four quick pills in the side panel, named by the snapshot's own water and part labels. Check `bella-coola-front`.
  Screens 69-75.

# regs v14 (round 12, owner 1 3 4 + More) — in place on regs-v11.html

- **Phase 2 (answers rows 11, gear 8; page v36 @ 3be6310d, 38 waters, 124 states).** Buckets in the data's order (a
  water's own rule first), size words whole (`detail`: "any length, 50 cm or less", "60 cm or less, or 90 cm or longer"),
  every cap a bucket note (Bennett Lake 3, Atlin 3). ONE "How it adds up" after the buckets: the data's
  `elsewhere.says`, then each `group.worked` line ("Keep 3 rainbows here → …" for `at: here`, a question and its
  answer otherwise); the "Fished elsewhere today?" row and sheet are gone. Gear sentence as gear 8 says it. Checks
  `one-worked-list`, `buckets-own-first-sizes-whole`, `every-cap-once`, `gear-sentence-plain`. Persona (haiku): a lone
  ✕ column now marks each item; "Other days" -> "Different on other days". Screens 63-68 (Vedder, Wahleach, Bennett,
  Morley, Bella Coola).
- **1 + 4 Header.** No kind: the water's name, then the part's label alone as a full-width line of its own aligned with the
  name, then "View on map ›" under it (the whole block is one button); a whole water with a map: just "View on map ›"; a
  whole water with nowhere to go: no line. The water's name wraps instead of truncating. Compact (scrolled): the label
  stays whole, smaller and grey, the action folds into it (still a button); `.scroller` has `overflow-anchor:none` so the
  header changing height never moves the scroll. Checks `place-in-header` (no kind, bare label, own line), `compact-header-place`.
- **3 ⓘ sheets.** Built from the term's own fields (snap.mjs now copies `says`, `example`, `quote`, `pages` from page v36's
  glossary): the plain words, "In the book’s words ▸" folded (closed), then "Book p.4 ↗ p.8 ↗", each a link to that page
  of the PDF through `SNAP.pdfPages` — the one printed -> PDF map (`pipeline.common.synopsis_pages.printed_to_pdf`, read
  by snap.mjs; nothing typed). Check `sheet-book-words`.
- **3 Bait / Ways to fish.** Bait is two columns, ✓ Allowed | ✕ Not allowed (one column, flowed in two, when the other is
  empty; a note that only repeats the column, "banned", is dropped). Ways to fish inline: each allowed way one line, its
  conditions running on after its name; the ways not allowed one line. Both take no more height than v13 on the four
  states measured. Check `bait-ways-compact`.
- **3 Dated changes.** One row component everywhere in Gear & methods (the sentence's "Other days", Bait's bait-ban runs):
  ("Different on other days"), the date in a fixed 7.4em column, the change beside it in the data's words, "· in force today" when it is. Check
  `dated-rows`.
- **More: How to tell.** The "Hatchery or wild (adipose fin)" term sits inside the How to tell sheet, under the pictures,
  smaller (book's words folded, Book p.13 / p.80 linked); no separate ⓘ beside "How to tell". Check `howtell-has-hatchery`.
- **More: release-only groups.** A release row with no card of its own (Iron River's "Char") lists its members as a kind
  row: their names (page v36's species names, " · " between), then a whole picture of each with its caption. Check
  `release-group-pictures`.

# regs v13 (round 11, owner 2 3 4 6; answers rows 10, gear 7, display 8) — in place on regs-v11.html

- **4 Header.** The place is IN the header: water name; "<kind> · <the part's label>" (page v36's long label when its
  subtitle names the part by it, else the uncut short one; a whole water: just the kind); "View on map ›" (no map: "Other
  parts ›"); the date. No section count, no MAP button, no row under the header. v12's row was reported missing until the
  part changed — not reproduced headless, fixed structurally; `place-in-header` loads every state (117) fresh.
- **2 Card.** A bucket's shared number (`cap`, found in its title) is big; the title's scope after " — " a smaller
  line; the bucket's size note (rows 10 `notes`) once under it; one rail down the bucket so its kinds read as sharing
  it (persona). Kind row: name · detail · number on one line, then notes/duties, then pictures. Checks
  `bucket-number-big`, `bucket-note-once`, `kind-row-one-line`.
- **3 Gear.** Gear & methods = the data's ONE sentence (`summary.says`), one ⓘ for its terms, its book pages, the other
  days as dated chips (`summary.changes`); Bait and Ways to fish as before; no icons. Licence is the last section; the
  province-wide rules are a closed "Reference" fold after it, named by the data's tile ("Handling fish · 21 rules that
  apply everywhere"). Checks `gear-sentence`, `gear-no-icons`, `licence-last`, `always-as-reference`.
- **A3 Inherited.** A lift another water's row carries here (display `inherited`) is a line right under the strip, the
  data's words + the lifting rule's book page, under v36's own label ("By another water’s rule"). Iron River Jul 20.
  Check `inherited-line`.
- **6 Braids.** `water_map_export --braids` writes `WATERMAP.braids`: one map per braid section the bundle marks
  (`section_straddle` via `read.straddles`), each rule it was placed for with its cut (the rule's extent split nearest /
  crossed; a braid over both ends of a short stretch has no single cut), side, share and above/below. The side panel's
  "Braid review" lists all 38 placements (18 braids, 11 waters); each opens a map: the braid dashed, the rule's stretch
  shaded, the cut marked, and "This channel crosses <cut> — counted as below it: 69% of its length is below it. So this
  rule applies to the whole channel." No regulation changed. Check `braid-review`. Phone width hides the panel.

# regs v12 (round 10, owner items 1, 2, 3, 5) — in place on regs-v11.html so the published link updates

- **1 Where you can't fish** is a quiet note at the END of "What you can keep" (after the groups and Other fish): small
  grey text under a small caps label, no ✕, no red. Check `nofish-quiet-note`.
- **2 Pictures.** Every photo in the app's manifest is 2:1; each species is drawn whole in a 2:1 box (contain, no
  rounded clip), side by side under the kind's name, with the photo's own caption ("Bull trout ›") under it, and each
  is a button that opens its picture sheet. Why under the name, not a side column: on a 390 px phone a side column is
  ≤ 92 px, which forces overlap or 28 px fish; under the name there is ~300 px, room for 3–4 whole 80 px fish, none
  hidden behind "+2". Other fish rows: the thumbnail is its own button (picture sheet); the row opens the rule sheet,
  which also shows every member's picture. snap.mjs now names every fish the page names, not only FISHART's.
  Check `pictures-whole`.
- **3 Gear & licence** = 1 Gear & methods (one "what's allowed" grid: lines, hooks, points, barbs, lures, weight, then
  bait, ways to fish, boats; an icon per cell; the strip's dated fly-only / bait-ban runs sit on the Lures / Bait cell;
  a run all year is no change and is not listed), 2 Always, 3 Licence (its own section, last). Boat lines: only the data
  — the Lines row's own words where it has them (lakes: "2 while alone in a boat"; Kootenay Lake Main Body: "no limit
  while in a boat"). Gear v6: the Lines cell is the data's `says` ("1 line (rod), in a boat too" on streams) with book links from `cites` (snap: `st.lines`). Checks
  `gear-one-summary`, `boat-lines`, `licence-last`.
- **5** No part strip. Under the header: "📍 Where these rules apply · <part label> — View on map ›" (the data's short
  label unless it was cut with "…", then the full label). The map's title: "Where these rules apply" + water · part.
  Waters this prototype has no map for get "Other parts ›" (a parts sheet). Checks `no-part-strip`, `map-way-obvious`.
- Lifts cite every lifting rule's page once (answers rows v9 `each[].by`; snap records `ruleKey` = D.rid per `by`).
