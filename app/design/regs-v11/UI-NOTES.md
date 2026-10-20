# regs v32 (round 20h, owner: the region total vs the stream share, one surface, pictures without a tile, the pin line's end) — in place on regs-v11.html

Report: handoff/PLAN/r20h_report.md (the two head forms with their readers, before/after pairs, scores). UI only until the
data agent's round-20h rows land (roe note out of time, possession duplicate, the example from the water's own total).
- **1 The region total over the stream share — two forms, one pick.** On a card whose total is the REGION's and whose headline
  is a stream share (`relTier`: the top tier carrying `headline.n`; the Vedder's 4 over 2, Glenn's 5 over 2, Bella Coola's 5
  over the river's own 2), the group head is drawn one of two ways, `tot=a|b` in the hash:
  (a) the share leads — the data's headline is the big line ("**2** a day per angler from streams") and the region total is a
  quiet note under it: "of the " + the root's words ("of the 4 a day per angler across Region 2, lakes included"); the pin bar's
  group line follows: "Trout and char · 2 a day from streams (of 4 across Region 2)", two lines allowed, and the share tier's own
  "↳ 2 of them from streams" line is not repeated;
  (b) the total leads as before ("**4** a day per angler across Region 2, lakes included") with a second strong line under it,
  "↳ **2** of them from streams" (shortOf's own words); the pin bar as before.
  Two fresh opus readers (one per form, the same ten questions on the Vedder, Glenn, Bella Coola and Kootenay cards and
  screenfuls; scratchpad r20h/prompt-tot-a|b.md, reader-tot-a|b.md): both right everywhere; (a) answered "how many from this
  stream" from the top screenful at conf 4, (b) at conf 3; (b)'s reader named the head's big 4 next to the river's own 4 as the
  thing that most risked "4 from the Vedder", (a)'s reader named only the river's own-rule heading (present in both forms).
  **THE PICK is (a)**, the default; (b) stays behind `tot=b`. Page-side connecting words: "of the", "(of … )". A one-total card
  (Kootenay) is unchanged.
- **2 ONE surface.** The card is the only surface: the group head block and the pin bar paint the card colour (the head keeps
  its 1.5 px ink rule on top, the bar its rule below); the size section is no longer a tinted band but a section framed by two
  hairlines (`--line2`) full-bleed, with its chip still tinted (a chip, not a band). The old head band (`--wash`), the size band
  (`--tint`) and the bar were three surfaces that striped the card white → grey → white → tinted → white.
- **3 Pictures without a tile.** Every fish picture is an RGBA cut-out (`app/apps/mobile/assets/fish/*.webp`, transparent
  corners); the grey tile was CSS (`background: var(--wash)` on `.kp img`, `.th img`, the sheet's `.pic`, the gallery's `.im`).
  Gone everywhere, light and dark (the dark brightness filter stays).
- **4 (i) A pinned tier line ends sooner.** Besides the round-20f rule (less than a row of its section left), a tier's line goes
  as soon as the NEXT section's head is in view (its top above the bottom of the scroll area less a line): the Vedder's own
  rainbow row and Glenn's Duncan row never sit under "↳ 2 of them from streams". (ii) The possession sheet does not draw the
  term's line when it merely restates the plain line (containment, not only equality). (iii) The roe note and the example wait for
  the data.
- **Data: gear 11, rows 25, glossary 4** (commit 7131c6d4; handoff/PLAN/r20h_data.md; re-snapped, oracle regenerated). The
  roe row's own words name the ban it is excepted from ("the fish or fish parts ban excepts it", `excepted_from` on the days the
  province's fin-fish ban is in force); the page drops the banned row's trailing "roe is still OK" and adds nothing page-side (the
  UR2-7 phrase "still OK — not part of the ban" is gone; it only stands, naming the item, where the data gives the allowed row
  no words). The possession sheet shows the card's own `group.possession` examples as a list under "For example:" ("With 4 trout
  and char a day you may have 8: 2 days’ fish." + in ink "Possession doubles the 4 a day, not the 2 from streams."; the
  whitefish and bass lines after), the glossary's generic 5 → 10 only as a fallback; the term's line no longer repeats the rule.
- **Checks** (ONLY=20h; all six fail on regs v31 — scratchpad r20h/check-20h-before.txt): `r20h-one-surface` (inside a keep
  card and the pin bar nothing band-sized, > 200 × 40 px, paints a background but the card's; six page loads, light and dark),
  `r20h-picture-surface` (no fish picture or tile paints a background), `r20h-pin-drops` (the own row at 60 % of the viewport:
  no "from streams" line; the share's head just under the bar with the own head below the fold: the share still said),
  `r20h-total-relation` (a region total over a share shows both numbers in the head: form a's big headline number + the "of the"
  note, or form b's "↳ N of them" line), `r20h-roe-names-ban` (an allowance names its ban, the banned row drops "is still OK",
  the dated line carries nothing of it), `r20h-possession-own-example` (the card's examples + doubles, never the generic beside
  them, the term line not repeating the rule). Retargeted with a reason: `r20f-headers`, `size-tier-box`, `r20d-flat` (hairlines, no
  band), `r20d-compact-head` (+ the lead line), `r20d-pinned-stack` (a two-line group line; the share rides it), `r20e-region-vs-
  stream`, `r20f-region-words` (the group line says the share), `r20g-palette` (the tint lives on chips), `group-head-compact`,
  `ur1-tier-words` (notes stripped before comparing), `ur2-bait-allowed-note` (the roe row names its ban).
- **Readers**: the round-20g 36 questions + Q29 (from the Vedder vs anywhere in Region 2), F9 (the same from one screenful), Q30
  (possession: 8 or 4?) — BEFORE (v31) 39/39 (Q29 conf 4, F9 conf 4 "a reader could think 2 is the Vedder's maximum", Q30 conf 4);
  AFTER: see r20h_report.md.

# regs v31 (round 20g, owner: colours with one meaning each, one way to show a future change, the ways list, possession) — in place on regs-v11.html

Report: handoff/PLAN/r20g_report.md (before/after pairs, the colour census, the changes-line pick, the reader scores).
- **Data: rows 24, display 16, glossary 3** (commit b13f6110; handoff/PLAN/r20g_data.md; page v36 re-snapped, oracle
  regenerated). On a Region 2 stream card `group.origin` is null and the SHARE tier carries `origin` ("From streams, keep
  hatchery fish only — wild trout and char go back."): the page draws it under the share's line (`.torigin`, `data-tier-origin`)
  with "How to tell ›" and the root's Rule book p.21, which move there from the group head (no `.gsub` block then); where
  `group.origin` is still set (a water's own total) it stays at the head. The steelhead kind has no `size_note` (the band head
  says "— 2 if one is a hatchery steelhead" once); duties "Stop fishing here after 2 a day · Record each on your licence right
  away"; presence "A rainbow trout over 50 cm (20 in) here counts as a steelhead." The possession foot is two sentences
  ("Possession: 2 days’ quota. Fish at home don’t count."): the card row reads "Possession · 2 days’ quota ›" (the posrow regex
  takes the first sentence as the value); the sheet is the plain line, the term's short line, the example, the quote alone in the
  closed fold, the source line. Pins moved: ur2-kind-words (presence, duty), r20g-possession-fold (two sentences).
- **1 Colour tokens, each with ONE meaning** (the census tool, `tools/census.mjs`, counted 7 / 6 / 6 / 5 distinct text colours
  on Keep / Gear / Methods / Licence on the Vedder in light; now 6 / 5 / 5 / 4, and the same in dark):
  `--ink` text and EVERY number (the group total, the tier numbers, a row's "Up to 2", the pin bar's figures, the size box's
  numbers, the duty line, the gear mode line — they were in the open green, the accent or the restricted amber) ·
  `--sub` the one secondary grey: labels, kickers, "How it adds up", the year strip's months, a row's size chip, the changes
  line's words (the second grey `--faint` and the second tint `--wash` now resolve to `--sub` / `--tint`, light and dark) ·
  `--accent` links and actions only: "How to tell ›", "Rule book p.N ↗", the ⓘ buttons, "Source ›", a picture's caption, the
  pressed tab's rule · `--open` / `--closed` / `--restricted` ONLY status: the year strip and its legend, Open / Closed, the ✓
  / ✕ marks of the ways and bait lists, a closed or release row's title and number slot, the status words in the regulation
  lines · `--tint` the ONE grouping tint: the group head band, the size band, the pin bar, the Now / Next chip ·
  `--card` text only on a filled control (the pressed segment). `--line` / `--line2` are rules. No other text colour is drawn.
- **2 ONE pattern for a rule that differs on other dates**: a quiet "changes" line `.chg` right under the item's value —
  a small chip, **the dates in ink**, then the data's words — before the item's source line, which also carries the change's
  rule ("Province · Region 2 · This water’s ›" with its Rule book pages). No tinted box, no amber bar, no page link or ⓘ inside
  the line (the 20e note was a tinted bar with "ⓘ Rule book p.22 ↗" inline). The same line on Hooks ("May 1–31: fly fishing
  only"), on a fly-only day's Hooks ("Jun 1–Apr 30: one hook, lure or fly and weight on the line up to 1 kg (2.2 lb)") and
  on Bait, between the bait sentence and the list ("May 1–31: bait ban · in force today"). Two chips were drawn and read
  (scratchpad r20g/chg-a, chg-b; prompt-chg.md; reader-chg.md): (a) a ◷ glyph on every dated line, (b) a NOW / NEXT chip.
  THE PICK is (b), reader conf 4: the chip says which dated line is today's before the dates are read (May 15: NOW on the bait
  ban, NEXT on the way back); (a) made the same line read like part of today's rule for a beat, both on Hooks and on Bait.
  `chg=a` in the hash keeps the glyph. Page-side connecting words (structural, not data): "Now", "Next", "· in force today".
  Methods and the keep rows have no dated changes in the data (a way's fly-only condition is its own small line; a kind
  row's dates are its year strip), so the pattern has nowhere else to go yet; `r20g-changes-line` refuses any other dated note.
- **3 Ways to fish / Bait as ONE list**: the ✓ ways then the ✕ ways, stacked (no second column at 390 px: the "Not allowed"
  column wrapped "Water insects and crayfish" and left the ✓ column half empty), every row with its own mark on the left,
  the row's name in emphasis, its condition small under the name, a short header word over each group ("Allowed" / "Not
  allowed", sub grey, no mark of its own). A lone group (Bait on May 15: everything banned) is one column too.
- **4 The possession sheet**: one plain line — the possession row's own words ("Possession: twice the daily quota (fish at
  home don’t count).") — then the term's example ("For example: …"), then "In the rule book’s words ▸", a fold CLOSED by
  default holding the term's sentence and the exact quote, then "Source › Rule book p.21 ↗". 388 px tall (was 512, the full
  term body open).
- **Checks** (ONLY=20g; all three fail on regs v30 — scratchpad r20g/check-20g-before.txt): `r20g-palette` (a probe reads
  every token's computed colour; on each tab, light and dark, every text colour is a token, status colours only on status,
  every number in ink, one grouping tint, at most 6 / 5 / 5 / 4 distinct text colours on Keep / Gear / Methods / Licence),
  `r20g-changes-line` (the pattern above, nothing else dated anywhere, the bait line before the list), `r20g-possession-fold`
  (plain line, example, closed fold, source, in that order, under 260 px). Retargeted from 20e (both fail on v30 —
  r20g/check-20e-before.txt): `r20e-dated-on-its-line` wants the quiet line before the source line, `r20e-ways-columns` wants
  one stacked list with a mark on every row; `r20e-sources-per-item`, `gear-sentence`, `gear-lines` let the change's rule and page join its line's source line;
  `dated-rows` reads past the chip; `bait-ways-compact` measures slack (rows as tall as their text, no gap over 8 px) instead
  of the v28 two-column height; `ur2-bait-allowed-note` reads past a row's mark. The size note's bar is `--line2`.
- **Readers**: the r20f 34-question set + Q27 (bait and the hook rule on the Vedder, May 15) + Q28 (possession), fresh opus,
  PNGs only — BEFORE (v30) 36/36; AFTER: see r20g_report.md.

# regs v30 (round 20f, owner: thumbnails in one place, tier headers without the left bar, "from streams", small items) — in place on regs-v11.html

Report: handoff/PLAN/r20f_report.md (before/after pairs, the header alternatives with screens, the reader scores).
- **Data: rows 23** (page v36 re-snapped; oracle regenerated; ANSWERS-SPEC ROUND20f). A share that is the REGION'S (a zone or
  province rule — the Region 2 streams' combined 2) says "from streams" / "from lakes": the share line "Only 2 of the 4 may be
  from streams, not counting hatchery rainbow trout", the headline "2 a day per angler from streams"; a share a water's own row
  sets keeps "from this stream" (`tree.share_place`, `share_region_wide`). The pin bar follows: "↳ 2 of them from streams".
- **1 Thumbnails in one place.** Every kind row draws its picture strip right under the title and its size chip, BEFORE any
  note (the steelhead's size note, "10 a year · …", the presence line); the pictures keep their 80 px size. A caption may run
  wider than its picture (a tile is 80–132 px), so "Coastal cutthroat trout ›" is two lines, not three.
- **2 The tier headers without the left bar** (owner: "the left box outline with tab indent for headers" felt weird). Three
  styles were drawn and shot (scratchpad r20f/frames-a|b|c, the Bella Coola, Glenn and Vedder row frames): (a) a plain header on
  a hairline above it with the big number as the anchor; (b) the same with the number in a tinted chip; (c) no hairline, more
  space and a display-face heading. Headers, their kickers and the rows now share the card's text edge (no 3 px bar, no 13 px
  step); the size band keeps its tint; a water's own rule's number is in the accent. `hdr=a|b|c` in the hash shows the others.
  THE PICK and why: see r20f_report.md.
- **4 Small items**: the gear tab's row labels (Lines · Hooks · Weight on the line) in sentence case (`.gline .glab` outranked
  round 20c's rule); the group head's "Rule book p.N ↗" sits on the origin's own line after "How to tell ›"; Bennett's pinned
  own line reads "↳ 2 of them (lake trout) under this lake’s rule"; a pinned tier line now goes once less than about a row
  (110 px) of its section is left under the bar (the 20e reader saw a line outlive its section over the next section's rows).
- **Checks** (ONLY=20f; all three fail on regs v29 — scratchpad r20f/check-before.txt): `r20f-pictures-first` (the strip right
  after the title + chip, before every note; no caption over two lines; no picture under 80 px), `r20f-headers` (no left bar on
  a header or kicker, one text edge, the band tinted), `r20f-region-words` (the share line and the pin say "from streams").
  `ur1-tier-words`, `ur3-lifted-share`, `r20-share-inside-total`, `r20e-region-vs-stream` read the new words.
- **Readers**: the r20e 34-question set, fresh opus, PNGs only — BEFORE (v29) 34/34, AFTER: see r20f_report.md.

# regs v29 (round 20e, owner: Gear / Methods split, dated notes, sources per item, ways in columns, licence choices, region vs stream) — in place on regs-v11.html

UI only (no data change): handoff/PLAN/r20e_comprehension.md has the reader gate; handoff/PLAN/r20e_progress.md the handoff.
- **Four tabs** (owner 1): What you can keep · Gear · Methods · Licence, one row at 390 px (tab gap 16 px). Still ONE scroll;
  `#gear` holds the day's gear lines (Lines · Hooks · Weight on the line, a fly-only line first) and Bait; `#methods` holds Ways
  to fish, Boats and the Reference fold (handling fish, closed); `#licence` the licence answer. `sec=keep|gear|methods|licence`
  in the hash; each tab lands pressed. buildGear() returns the three parts; render() no longer cuts one HTML string. A closed
  day's "No fishing here until …" stands under Gear AND Methods, and the Licence tab now has its answer on a closed day too
  (it was a dead tab before).
- **A dated change is a note joined to the line it changes** (owner 2): no floating "Different on other days" label. Each
  `summary.changes[]` is a tinted note (`.gnote`, the restricted colour's bar, `--tint` ground) inside the line it changes, its
  last block: "**May 1–31:** fly fishing only ⓘ Rule book p.22 ↗" on Hooks. Placement (page logic, no words): on a day with a
  fly-only line, the change (the way back) goes on that line; else by the change's own words — fly / hook / lure → Hooks, weight
  / float / sinker → Weight, line / rod → Lines. The change's terms' ⓘ and its rule's Rule book page sit inside the note. Bait's
  bait-ban runs (the strip's bit 16) are the same note at the bottom of the Bait item ("**May 1–31:** bait ban").
- **A source line per item** (owner 3): every gear and methods item (each gear line, Bait, Ways to fish, Boats, the Reference
  fold) ends with ONE source line `.isrc` — who says so (the rules' levels in the page's chip words, "Province · Region 2 ›",
  opening the Sources sheet of exactly that item's rules: `SRCS['gl|<state>|hook']` etc.) then "Rule book p.N ↗" per page. A gear
  line's rules are its parts' `cites`; a line the data cites nothing for (a fly-only day's "No float, sinker or attracting
  device") has no source line. No section-end "Sources" line and no bunched "Rule book p.8 p.21" under the lines any more. The
  licence's source line takes the same style ("Province · This water ›", its Sources sheet).
- **Ways to fish in Bait's two columns** (owner 4, decided YES): ✓ Allowed | ✕ Not allowed side by side (the same `yesNo`
  component), each way's conditions small under its name. Why: the ✕ list was one run-on line ("Set lines · Spear or bow · Nets ·
  Snagging · / Chumming") whose ✕ read as a lone glyph; in columns each forbidden way is its own line under a header, the
  allowed ways' conditions sit under their own names, and it is the same component as Bait one tab over. It costs up to 59 px
  (Vedder May 15: the five ✕ ways stack) but the Methods tab is now well under one screenful.
- **Licence: two choices, no dropdowns** (owner 5): under page v36's own question "Who is fishing? ⓘ" (the under-16 term), two
  segmented radio groups — age (`#who-age` options: 16 or older · Under 16) and where you live (`#who-residency`: B.C. resident ·
  Canadian, other province · From outside Canada) — `role=radio`, `aria-checked`. The chosen pair's own answer (`st.licence[adult |
  adult_non | adult_alien | u16_res | u16_non | u16_alien]`) is the only one shown, its label (`licLabel`) above it; `age=` and `res=`
  in the hash. Gone: the "If you're not a B.C. resident · …" dropdowns and the "Under 16, by where you live" list (each pair is a
  choice now). The guiding fold ("If you are guiding other anglers") is drawn OPEN under the answer, its sentence only (page v36's
  repeated requirement names under it are hidden). The data's `guidance` (Guided / Not guided) and `status` axes have no answers in
  the snapshot, so they are not choices.
- **Region quota vs stream quota** (owner 6): where a group's root is a region's (or zone's) total and the row's scope tag is not
  that region's ("Region 2-wide"), the tag leaves the head band and heads the share it belongs to: the head reads "Trout and char /
  4 a day per angler across Region 2, lakes included", the share "ALL REGION 2 STREAMS / Only 2 of the 4 may be from this stream,
  …" (`.tkick` = `.scope`, a kicker on the share's bar). A "This water" tag over a region total with a water's own tier (Bella
  Coola May 1, Zone A lakes) is not drawn: the own tier's words say whose rule it is. The pin bar is unchanged and agrees ("Trout and
  char · 4 a day across Region 2" / "↳ 2 of them from this stream").
- **Not touched** (owner 7, the next UI agent's): the tree's left-bar headers, the species thumbnails, visual polish.
- **Checks** (ONLY=20e; all 7 fail on regs v28 — scratchpad r20e/check-before.txt): `r20e-four-tabs`, `r20e-dated-on-its-line`,
  `r20e-sources-per-item`, `r20e-ways-columns`, `r20e-licence-selectors` (all six pairs tapped, Vedder and Kootenay LWA),
  `r20e-region-vs-stream`. Retired: `r20b-three-tabs`, `licence-others-in-dropdowns`, `u16-by-residency`, `ur3-licence-not-resident`.
  Retargeted: `one-sources-line` (no section-end line in Gear / Methods, one in Licence), `gear-sentence` / `gear-lines` (pages from
  the lines' own source lines; the change inside its line), `gear-no-icons` (ways in #methods), `always-as-reference` (closes Methods),
  `bait-ways-compact` (Bait against its v28 height + 6 px per note; Methods within one screenful), `dated-rows` (the note's bold date),
  `licence-u16-you-need` (no dropdown order).
- **Tools**: tall.mjs `tab` mode takes sec=methods, + r20e-* shots; shoot.mjs 154–157; audit.mjs shoots the Methods tab.

# regs v28 (round 20d, owner: stacked pinned headers instead of nesting; clearer grouping) — in place on regs-v11.html

UI only (no data change): handoff/PLAN/r20d_nesting.md has the before/after screenfuls, the reader scores and the measurements.
- **FLAT instead of nested.** The tree still comes from the data (`group.tree`), but it is DRAWN as a flat list: every tier is
  a section HEADER (its full data words, a 3 px bar at the card's edge, accent for a water's own rule) and the rows after it
  sit on the header's text edge — no rails, no indent per level, no box in a box. A size section is a tinted BAND (its data
  words say "this box"): a surface, never a border inside a border; its chips take the card colour. A GROUP starts with a
  full-bleed tinted head band (name · scope, then the total on ONE line: the figure at --t2, the words at --t4, the ⓘ
  inline) and the origin line + the root's Rule book page in the block under it; groups end with air.
- **The pin bar.** Under the header, `#pins` (sticky, zero height, an overlay) carries the chain of sections the reader is
  inside — the group's line first, then one "↳ …" line per tier whose header has slid under the bar — each ONE line, built
  by `shortOf()` from the tier's own fields (n, its `says`, `whose`, `over_cm`, `extra`, the folded kind's fish names) and
  the page's connecting words, which are structural like the tab names and listed here: "↳", " · ", "a day across", "of
  them", "from this stream/lake" (the data's), "of those", "over … cm", "plus up to … under", "… of them … under". So:
  "Trout and char · 4 a day across Region 2" / "↳ 2 of them from this stream" / "↳ of those, 1 over 50 cm (2 if one is a
  hatchery steelhead)" / "↳ plus up to 4 hatchery rainbow trout under this river’s rule"; a water's own total "Trout and
  char · This lake’s rule: 2 a day"; a count tier "↳ of those, 1 cutthroat or rainbow trout in all". Every level that limits
  the count stays pinned while the reader is inside it (owner: never unpin "2 of the 4"); a line appears once its in-place
  header's top has slid under the bar and goes when its section ends; at How it adds up only the group line remains; in
  Other fish the bar is hidden. The stack is 26 px a line (≤ 90 px measured bound). Round 20c's sticky headers are retired.
- **Possession covers every fish** (owner): the "ⓘ Possession: twice the daily quota (…) Source" footnote under Other fish
  is a card-level row in the list's style after Other fish and before Where you can't fish — "Possession · twice the daily
  quota ›" — whose sheet holds the sentence, the term's words (`SNAP.gloss`) and the Source / Rule book links (data words).
- **Side items**: the fish names under the pictures `text-wrap: balance` (pictures unchanged).
- **Checks** (in the 20c block, ONLY=20c; all three fail on regs v27 — scratchpad r20d/check-before.txt): `r20d-pinned-stack`
  (six waters scrolled to a row: the bar under the header, the group line first with the root's number, "↳" lines for exactly
  the sections containing the pin line whose header passed, each one line and uncut, the stack ≤ 90 px, the row below; only
  the group line at How it adds up; no bar in Other fish), `r20d-flat` (no bordered section, no rail, one text edge for
  headers and rows, a size section a tinted band), `r20d-compact-head` (the head ≤ 66 px with the total on one line).
  `size-tier-box` now wants the band; `tree-folders` wants no indent; the root's Rule book page is found in the head blocks.
  `r20c-pinned-context` retired.
- **Readers**: screenful gate (one mid-scroll frame per question, 7 questions incl. "from this stream, 2 or 4?" on the
  steelhead/over-50 frame) 7/7 (before 6/6 on the six shared); the full-card 21-question set: see r20d_nesting.md.
- **Screens**: every screen and tall shot re-shot; before/after screenful pairs under screens/r20d/.

# regs v27 (round 20c, owner: a format audit, "the style feels off", context in every screenful) — in place on regs-v11.html

Audit and numbers: handoff/PLAN/r20c_style_audit.md (17 issues: 12 SMALL done, 4 BIG logged, 1 verified). No data change.
- **Context in every screenful** (owner). The group's name + total line is its own block (`.ghd.gpin`) and PINS under the header
  while the reader is inside the group; a top-level tier line (the share, or the river's own rule) pins under it; a size box's
  head pins under the tier it sits in. `position:sticky` with measured offsets (`pinTops()` → `--hdr-h`, `--pin1`, `--pin2`, after
  render, on resize and on fonts). The origin line is the block under the pinned one (`.ghd.gsub`) and scrolls away. Tall shots
  never scroll, so nothing pins in them; the frames (`tools/frames.mjs`) show it.
- **Tokens** (one rule per thing at the end of the CSS): weights `--w-body/--w-emph/--w-head/--w-num` (400/600/700/800; the 500s
  and the one 900 are gone), spacing `--s1..--s4` (4/8/12/16) on the card; every inline numeral shares one line box (26 px figures
  no longer make their line taller); ALL-CAPS only on the section kickers (`.lab`, `.scope`, `.bkh`, the no-fishing note's h3) —
  row and column labels on gear and licence read in sentence case, `--sub`; ONE link style (accent, 700, no underline; ↗ the
  book's PDF, › opens here); 44 px hit areas for the inline How to tell, the Source chip and every book link (a pseudo-element,
  no line moves); the size box 1.5 px with a 1 px head rule and its head words at meta size (two lines while pinned); size chips
  tinted blocks, not borders inside the box's border; the kinds' rail 28 px in (36 before); the header's total line and every
  tier line `text-wrap: balance`; the "Gear · for this water today" / "Licence · …" kickers are gone (the tab names the tab).
- **Checks** (ONLY=20c; both fail on regs v26 — scratchpad r20c/check-before.txt): `r20c-pinned-context` (scrolled deep into the
  Vedder's over-50 box, Chilliwack's, Bennett's top-level box and Glenn's own tier at phone size: the group block sits under the
  header, the tier line under it, the box head under that, the row below; a section too short to pin shows its line in view),
  `r20c-style-tokens` (Keep tab: ≤ 4 sizes beside the strip's axis, ≤ 4 weights, one link colour, no underline, caps only on
  kickers, every hit area ≥ 32 px). `group-head-compact` reads the two header blocks; `tree-folders` takes a tinted chip.
- **Readers**: the r20b 21-question set on the restyled tall screens 21/21; the new screenful gate (one mid-scroll frame per
  question) 0.5/4 → 4/4.
- **Tools**: `tools/audit.mjs` (every tab + the four sheets, light and dark, five waters), `tools/frames.mjs` (390×844
  screenfuls with 90 px overlap), `tools/census.mjs` (computed-style census: sizes, weights, families, colours, link marks,
  caps, borders, left edges, small targets). tall.mjs marks the tab shown in `tab` and `licence` modes.
- **Screens**: every screen and tall shot re-shot.

# regs v26 (round 20b, owner: fewer words, real visuals, split tabs) — in place on regs-v11.html

- **Data: rows 22** (page v36 re-snapped; oracle regenerated). Four cuts, all words or order (ANSWERS-SPEC ROUND20b):
  C1 a tier whose only child is one kind row at its number names the fish and folds the kind (`folded`): "This river’s
  rule: up to **4** hatchery rainbow trout a day", "The Duncan River’s rule: up to **5** rainbow trout a day", "Only **2** of
  the 5 may be brook, brown, cutthroat or lake trout from this stream", "This lake’s rule: up to **2** lake trout a day (part
  of the 5)" — the kind row keeps its size chip, notes, duties and pictures and draws no sentence (the number once); C2 an
  own tier's sub-line is why it applies here, never "Replaces …" (the panel read it against the share's "not counting
  hatchery rainbow trout" as two limits); C3 under the header's total the SHARE comes first ("4 … / Only 2 of the 4 …", the
  owner's 2 as the next line), the water's own tier after; C4 How it adds up keeps the two ends of the keep-N range ("If
  you keep 4 …", "If you keep 2 …").
- **Page.** A Rule book page is shown once per branch: a tier whose pages are all on a line above it (the header's root
  line or a tier it sits in) draws no link (Vedder: p.21 once in the header, p.22 on the river's own tier). The "How to
  tell ›" inside a kind's sentence is an inline link (its 44 px button height made the second line taller), and the kind's
  big number keeps a short line box, so every line of the sentence has one spacing.
- **Tabs: What you can keep · Gear · Licence** (owner). Still ONE scroll (round 13: the header keeps one shape): `#gear`
  holds Gear & methods and the Reference fold, `#licence` the licence answer alone; each tab button scrolls to its section
  and the tab in view is the last section whose top reached the header; `sec=keep|gear|licence` in the hash; the two
  sections' labels "Gear · for this water today" / "Licence · for this water today"; every `#gear …` style is scoped
  `:is(#gear,#licence) …`. Nothing else moved: the licence dropdowns, "Write each one on your licence" (a kind's duty on the
  card) and the closed-day gear note sit where they did, each now under its own tab.
- **Checks** (ONLY=20b; all 5 fail before on the committed page: scratchpad r20b/check-before-headpage.txt): `r20b-three-tabs`
  (three tabs on one row at 390 px, each its own section, each lands pressed, `sec=licence` lands, no words of the old tab, no
  dead `[data-sec]`/`[data-jump]`), `r20b-number-once` (a number never twice on a tier and its only kind; folded kinds draw
  chip + pictures, no sentence), `r20b-book-once`, `r20b-worked-ends` (Vedder 4 and 2), `r20b-howtell-inline` (one line
  spacing). `own-tier-why-here` no longer wants "Replaces"; `tree-folders` and `card-is-the-caps` read a folded kind's number
  off its tier line; `ur2-worked-plain` wants the two ends; `licence-last` reads the sections across the whole scroller and
  `always-as-reference` wants the Reference fold after the Gear tab's last section (it closes that tab). New tools:
  `tools/words.mjs` (rendered word counts per state) and tall.mjs modes `tab` (a whole tab, the tab shown marked) and the
  r20b-* shots (Glenn Creek Jun 15, Vedder Jun 15 keep, the Gear and Licence tabs on the Vedder and the Kootenay LWA).
- **Words on the cards** (`tools/words.mjs`, the ledger cards' rendered text; before → after): Vedder Oct 10 415 → 367,
  Glenn Creek Jun 15 289 → 217, Chilliwack part 2 331 → 323, Bella Coola May 1 mainstem 262 → 254, Bennett Lake 200 → 191;
  Bella Coola Oct 10 173, Kootenay LWA 105, Ape Creek 192 unchanged (no lifted tier, no middle keep-N line).
- **Comprehension gate**: handoff/PLAN/r20b_comprehension.md — 20 questions keyed from the data; a fresh opus reader on the
  BEFORE screens 20/20, on the AFTER screens (see the file).
- **Screens**: every screen and every tall shot re-shot.

# regs v25 (round 20, owner review of v24) — in place on regs-v11.html

- **Data: rows 21** (page v36 re-snapped; oracle regenerated). `group.header` IS the tree's root tier (`header.tier` names
  it, `header.says` is its line); the share tier "Only 2 of the 4 may be from this stream", lifted ", not counting hatchery
  rainbow trout"; a water's own total says "per angler"; a cap sentence ends in ONE full stop ("… in B.C." was "… in B.C..").
- **1 One header block.** The group's top and the first tier are one block: name · scope, then the ROOT TIER'S LINE where the
  headline stood — its number big inside its words, flowing as one sentence ("**4** a day per angler across Region 2, lakes
  included ⓘ Rule book p.21"; "This river’s rule: up to **1** a day per angler"), a walked total's "Its row covers its
  tributaries, so it applies here" under it (Ape Creek: that line was never drawn before) — then the origin line. No total
  tier is drawn; the root's children (own tier, share tier, or the kinds) are the top of the tree, flush left: one tier
  level less. The headline's "2 a day per angler from this stream" is no longer repeated above the tree; the share tier
  says it. Vedder Oct 10 header → tree, before: "2 a day per angler from this stream" / "4 a day per angler across Region 2,
  lakes included" / "This river’s rule: up to 4 a day" / "2 a day per angler from this stream — your hatchery rainbow trout
  count toward the 4, not these 2"; after: "4 a day per angler across Region 2, lakes included" / "This river’s rule: up to
  4 a day" / "Only 2 of the 4 may be from this stream, not counting hatchery rainbow trout".
- **2 Everything inside the one total.** The data's share words (above) — "Only 2 of the 4" cannot read as 4 + 2; the
  lifted fish are named as not counted in the 2 (they are in the 4).
- **3 Big numbers in the size box.** A size box's and a count tier's number are the same size as every other tier's
  (`--t1`), the box's second number ("— **2** if one is a hatchery steelhead") big too (`.tn2`), and the box's head words a
  step larger (`--t3`).
- **Checks** (ONLY=20): `r20-header-is-total`, `r20-share-inside-total`, `r20-big-numbers` all fail before (scratchpad
  r20/check-before.txt, 3 of 3). `tree-folders`, `size-tier-box`, `ur1-tier-words`, `group-head-compact`, `card-headline`
  and `card-cap-once` read the root from the header (`TOPS` helper); `ur3-lifted-share` reads the new words.
- **Screens**: every screen and every ur1-*/ur2-* tall shot re-shot; new tall shots r20-bella-coola-oct10-mainstem-cards
  (a water's own total as the header), r20-bennett-lake-cards, r20-ape-creek-may1-cards (the why line).

# regs v24 (user round 3, six-angler panel on v23) — in place on regs-v11.html

- **Data: rows 20 / display 15** (page v36 re-snapped; oracle regenerated). Rendered as the data's words:
  - the no-fishing buffers "within 23 m (75 ft) downstream …", "within a 100 m (330 ft) radius …" (2: page v36 prints the
    answers' `display.rules[].plain` for a Where-you-can't-fish rule);
  - a count tier inside a water's own tier "1 cutthroat or rainbow trout in all (part of the 2)" (3);
  - the Vedder share "2 a day per angler from this stream — your hatchery rainbow trout count toward the 4, not these 2" (4);
  - the Kootenay LWA kokanee note "Kokanee from the Lower and Upper West Arms together: no more than 5 a day." (6).
- **1: How to tell on the steelhead row.** A kind row whose data words say "hatchery steelhead only" carries page v36's
  "How to tell ›" right after them; it opens the existing How to tell sheet.
- **5: licence dropdowns.** Each other-residency dropdown reads "If you’re not a B.C. resident · Canadian, other province"
  / "· From outside Canada" (page v36's fold name "Rules for other anglers" read as guiding; the structure is unchanged).
- **Checks** (ONLY=ur3): `ur3-howtell-steelhead`, `ur3-buffer-feet`, `ur3-count-part-of`, `ur3-lifted-share`,
  `ur3-licence-not-resident`, `ur3-combined-plain` all fail before (scratchpad ur3/check-before.txt, 6 of 6).
  `ur1-tier-words` and `licence-others-in-dropdowns` read the new words.
- **Screens**: every screen and every ur1-*/ur2-* tall shot re-shot.

# regs v23 (user round 2, six-angler panel on v22) — in place on regs-v11.html

- **Data: rows 19** (page v36 @ b9306de1 re-snapped, 134 states; oracle regenerated). Rendered as the data's words:
  - headline "2 a day per angler from this stream" (a lake "4 a day per angler") (1);
  - "Only 1 of the 2 may be hatchery Dolly Varden/bull trout or lake trout" (3);
  - "Up to 2 may be hatchery steelhead only — wild steelhead go back"; the presence line "Steelhead are known to be in this
    water, so a rainbow trout over 50 cm (20 in) counts as a steelhead." (4);
  - "After you keep 2, stop fishing for the day" (5).
- **2: How it adds up.** A keep-here line is the data's two halves joined by a colon, with no arrow: "If you keep 3 hatchery
  rainbow trout: 1 more fish of any kind, from here or from a lake." (`one-worked-list` now looks for the colon.)
- **6: a closed day.** The gear note reads "No fishing here until Jul 1." The date is the first day after this one that
  the data's strip (`st.runs`) does not mark closed (`closedUntil`). The licence sentence is gone from that spot.
- **7: Bait.** An allowance carried by a banned item ("roe is still OK" under Fish or fish parts) moves to the allowed item
  it names, as "still OK — not part of the ban". It is kept to one line so `bait-ways-compact` still fits v13's height.
- **Checks** (ONLY=ur2): `ur2-headline-per-angler`, `ur2-worked-plain`, `ur2-kind-words`, `ur2-closed-until` and
  `ur2-bait-allowed-note` all fail before (scratchpad ur2/check-before.txt, 5 of 5). Full suite 71/71.
- **Screens**: every screen and every ur1-* tall shot re-shot. New tall modes are `keep` (the whole Keep tab) and `sheet`
  (header + the open sheet at full height). New shots: ur2-kootenay-lwa-oct12-cards (Monday),
  ur2-kootenay-licence-other-province-open, ur2-vedder-keep-full, ur2-kootenay-kokanee-sheet-full.

# regs v22 (user round 1, six-angler panel on v21) — in place on regs-v11.html

- **Data: rows 18 / gear 10 / display 14** (page v36 @ e17579ef re-snapped, 134 states; oracle regenerated). These render
  as the data's words, with no layout change:
  - totals "4 a day per angler across Region 2, lakes included";
  - the Vedder share "… (part of the 4) — your hatchery rainbow trout don’t use these 2";
  - the size box "Only 1 of the fish in this box may be over 50 cm (20 in) — 2 if one is a hatchery steelhead";
  - "1 cutthroat or rainbow trout in all";
  - "Steelhead: no fishing for them, not even catch and release — release any you hook by accident";
  - "Write each one on your licence right away";
  - Weight on the line "Up to 1 kg (2.2 lb)";
  - Kootenay Lake main body Lines "1 line from shore; any number from a boat".
- **C: every page link reads "Rule book p.N ↗"**, and the page labels before bare links read "Rule book p.13 p.80". The panel
  asked "Book p.21: what book?". This is one label change in `bookLinks` / `booksOnce`, plus `glpages` and `bks`.
- **E: How to tell.** The sheet always sat over the current water. Screen 59 had been shot over Iron River, Jul 20, so the
  panel saw "Iron River" while asking about the Vedder. It is now shot over the Vedder, Oct 10.
- **Checks** (ONLY=ur1):
  - `rule-book-label`, `ur1-tier-words`, `ur1-plain-words` fail before (scratchpad ur1/check-before-final.txt, 3 of 4 FAIL).
  - `howtell-current-water` passed before too: the render was right.
  - `card-one-book-per-lift`, `inherited-line` and the book count now read "Rule book p.N".
- **Screens**: every screen re-shot. New full-length shots (`tools/tall.mjs`, the page's `tall` mode, header on top):
  ur1-vedder-oct10-cards, ur1-chilliwack-part2-oct10-cards, ur1-bella-coola-may1-mainstem-cards,
  ur1-kootenay-lwa-oct10-cards, ur1-vedder-licence-16-plus, ur1-vedder-licence-under-16,
  ur1-vedder-licence-outside-canada-open.

# regs v21 (round 19, owner review of v20) — in place on regs-v11.html

- **Phase 2: rows 17 / display 13** (page v36 @ b073389b re-snapped, 134 states; oracle regenerated, 18 groups / 3,100
  baskets).
  - **A size tier is a BOX** (owner 3). It holds every kind it caps, and whole tiers when the data wraps them. It has a 2 px
    ink border, and its line is the box's head (wash, ink rule under it, "Only **1** of these may be over 50 cm (20 in) — or
    2, if one is a hatchery steelhead", Book page). Its kinds sit inside with no guide line. Other tiers keep the folder
    look.
  - **The steelhead sits inside the box**, and its `size_note` is a line on its own row in the accent's bar ("It counts
    toward the 1 over 50 cm (20 in), but once you keep one, a second fish over 50 cm (20 in) may be kept"). No cross-tier
    line is left on the Vedder / Chilliwack cards.
  - **Origin: no tag** (owner 1). header.tag is null because the fish names carry "hatchery", so the origin line is the
    data's words whole ("Keep hatchery fish only — wild trout and char go back.") with How to tell.
  - **Sizes**: the chips read "50 cm (20 in)" (owner 2).
  - **Checks**:
    - New: `size-tier-box` (8 states: a box on all four sides, marked with its tier id, every child inset inside it, its line
      inside it), `size-note-on-kind` (the note's words on the kind's row, inside the box of the tier it names), and
      `origin-no-tag`.
    - Updated: `tree-folders` now reads "cm (in)" chips and takes a size tier's box in place of its guide line;
      `group-head-compact` expects no tag when header.tag is null.
    - `card-is-the-caps` passed unchanged (a kind above its size tier's number: min(kind, tier) over the size).
    - Fail-before: scratchpad r19p2-check-before.txt (3 of 5 FAIL). Full suite 62/62.
  - **Persona (haiku)**, both right:
    - "Vedder today: keep a hatchery steelhead, still a 55 cm cutthroat?" → yes, if hatchery (2 over 50 cm, only 1
      non-steelhead).
    - "Chilliwack Tamihi→Slesse, bull trout?" → 1, hatchery, 60 cm (24 in) or longer, shared with lake trout.
    - Fixed: the box border was too light to show that the bull trout row is held by the head line (now 2 px with an ink
      rule under the head).
    - Left for WORDING: "these" in the size line read as possibly the whole Region 2 total; whether the steelhead counts
      toward "2 a day from this stream" is not said; "(part of the 4)" with "lakes included" made one reader ask whether
      this is a stream or a lake rule; "How it adds up"'s "at most 2 over 50 cm" took a re-read against "Only 1".
  - **Screens**: 151-153 added, every screen re-shot.
- **4 Header: the label gets two lines (checkpoint).** The header keeps ONE fixed size (117.5 px on every state, Chilliwack
  part 2 to Kootenay LWA; 103 before, the second line costs ~15 px) but the part's label is no longer cut to "…": it has TWO
  lines at a fixed height and the header's full width under the name. It is the FULL label (the map's title words, "From the
  Vedder Crossing Bridge up to the Tamihi Rapids Bridge") when that fits in two lines, else the data's short label ("Tamihi
  Rapids Bridge → 100 m below the Slesse Creek confluence", Chilliwack part 2), clamped only if longer still (`fitLabel`
  measures after layout, on font load and resize). A whole water keeps the empty slot so the size never changes. "View on
  map ›" left the label's line: a compact 44 px Map button (the app's Map tab icon over "Map"; "Parts" with no map) at the
  right end of the name's row, its aria-label "View on map: where these rules apply, <full label>". The date stacks its day
  over its date ("Today ▾ / Oct 10") to make room; a long name wraps to two lines inside the row (West Road (Blackwater)
  River). At 200% text the header is static, so the row and label grow and the button is its icon. Check
  `header-label-two-lines` (10 states: label = full if a clone measures ≤ 2 lines, else short; not overflowing; a 2-line
  box; one header height; the action ≥ 44 px on the name's row, off the label's line; the label ≥ header width − 72 px);
  `header-compact`, `place-in-header`, `header-one-shape` read the new places. Fail-before: scratchpad
  r19-4-check-before.txt. Screens 146-150. Full suite 59/59.

# regs v20 (round 18, owner review of v19) — in place on regs-v11.html

- **Phase 2 — the quota as FOLDERS (rows 16 `group.tree`; page v36 @ 728ea2eb re-snapped, 134 states; oracle 18 groups,
  3,100 baskets).** The card draws `group.tree` and none of the bucket fields (no buckets, count / size boxes, between
  lines or tags). Each tier is a line of its own on the left, in a wash with a 4 px bar (ink; a water's own rule in the
  accent; count / size tiers smaller), its number big inside the data's words ("**2** a day from this stream", "This
  river’s rule: up to **4** a day", "**1** over 50 cm / 20 in a day — a second may be a hatchery steelhead") and its Book
  page at the end; what it holds is indented beneath it on a 2 px guide line, in the data's order. An own tier says why it
  applies here and what it replaces under its line (Glenn Creek: "Its row covers its tributaries, so it applies here ·
  Replaces Region 4’s …"). A kind is the data's sentence ("Up to **2** may be brook, brown or cutthroat trout"), then its
  size as a boxed chip in the detail's own words carrying cm AND inches ("30 cm / 12 in or longer", "25–33 cm / 10–13 in",
  "over 50 cm / 20 in"), with the detail's other words beside it ("hatchery"). Each `also_counts` is a short line under
  the kind (persona: it carries the real over-50 limit and read as skippable grey, so it is ink, bold, on its own bar),
  then the duties, presence and one picture strip. A kind keeping none: its sentence in the release colour, no chip. The
  group's top now reads `group.header`: its first part with the number big; the total is the tree's root line, which
  carries the ⓘ, so the header doesn't repeat it. The tag is `header.tag` (drawn in capitals, rem-sized so it scales at
  200%). snap.mjs adds every tier's and `also_counts`' `by` to `ruleKey`.
- Checks (fail-before on the checkpoint page: scratchpad r18p2-check-before.txt): `tree-folders` (10 states: every tier
  its words / number / bar / indented guide line / children in order, every kind once with its sentence, number, a size
  chip of the detail's words holding every cm and inch of `sizes`, prominent, its also lines, one picture strip; no bucket
  field drawn, no tags), `own-tier-why-here`. `card-is-the-caps` (internal, never drawn) walks the tree. Every kind's
  number is the table's most, and under a size tier min(kind, tier) over its size. Every count / share / own / total
  tier's number is a cap's take over all the kept fish it holds, and every size tier's a cap's take over its fish at its
  size. `presence-in-section`, `card-picture-per-species`, `bull-trout-row` read the tree's kinds. `no-bare-zero` and
  `no-quota-table` accept the tree where the buckets stood. `card-cap-once` lets size chips repeat.
  `group-head-compact` reads `header`. Retired with the bucket display: bucket-number-big, bucket-note-once,
  kind-row-one-line, buckets-own-first-sizes-whole, every-cap-once, bucket-replaces, zero-kind-no-size,
  nested-count-group, bucket-between-big, kind-number-always, size-cap-line, within-shared-no-twice. Full suite 58/58.
- Persona (haiku, phone-height screens): "Vedder today, how many brook trout and what size?" → 2 a day, shared with
  brown and cutthroat, hatchery, 30 cm / 12 in or longer (right). "Bella Coola May 1, 30 cm bull trout?" → 1, both parts
  (right). Fixed: the cross-tier line (above). Left for WORDING (data words): "1 between them" names no antecedent;
  "Hatchery only — release wild trout and char." first read as "release the brook trout"; the root total and the share
  read as two competing numbers until the folders are followed; the 30 cm minimum in the own tier's "Replaces …" line
  read as possibly the brook trout's.
- Screens 135-145 (tree: Vedder Oct 10 / May 15, Bella Coola May 1 both parts, Glenn, Bennett, Ape, Kootenay LWA, dark,
  200%); every screen re-shot.

- **5 A real phone.** The frame is 390 x 844 INSIDE (an iPhone 14's points; the 1.5 px edge is now an outline, so it
  takes nothing), with a 47 px status bar, the app's own four tabs at the bottom (TabBar.tsx: Map · Search · Conditions ·
  Spots, Riffle's icons; Search marked, the water came from Search; each a toast) and a 34 px home indicator. The
  regulations now scroll inside 703 px (800 before: the frame had no tab bar and no home band, ~100 px more than a phone
  leaves an app). At phone width (≤ 520 px) the frame is the viewport and the home band is the OS's, so not drawn. Sheets
  and the scrim start under the 47 px bar; the toast sits above the tabs. Check `phone-real-height` (shot + desktop: inside
  390 x 844, the four tabs with one selected, the home band, ≤ 714 px of rules = 844 - 47 - 49 - 34, scrolls inside).
- **6 Header.** ‹ · water name · date on ONE line (the date pill lost its "Change date" caption; the aria-label keeps it);
  the part's label ONE line, the data's SHORT label when uncut ("Tamihi Rapids Bridge → 100 m below the Slesse Creek
  confluence"), else the long one, cut with "…" by the screen, with "View on map ›" / "Other parts ›" beside it on that
  line; the full label is the map's title and, for a water without a map, a small line under each part in the parts
  sheet (and the label's `title`). Slim tabs 34 px. Chilliwack part 2, Oct 10: **179 → 103 px**. Check `header-compact`
  (8 states: ≤ 120 px, date beside the name, one-line label = the data's short label, action on its line, the full label
  shown where the action goes, tabs ≤ 36 px); `place-in-header` now wants the action beside the label.
- **4 Group top, one block** (`.ghd`, today's fields): name · scope on one line; the big number with its unit on one line
  ("**2** a day from this stream"), the data's `also` under it; the origin as a tag, its words before " — " ("HATCHERY
  ONLY"), then the rest ("release wild trout and char.") and How to tell on the same line; the total line small. Vedder
  Oct 10: **218 → 142 px**. Check `group-head-compact` (6 states: every part the data's words, inside the block, number
  and unit on one line; Vedder ≤ 145 px, two thirds of before).
- Screens 131-134. Fail-before: scratchpad r18-check-before.txt (3 of 3 FAIL on the v19 page); full suite 68/68.

# regs v19 (round 17, owner: revert the table, refine the card) — in place on regs-v11.html

- **1 Revert.** The quota table and the catch checker are gone from the page: no `#qt` toggle, no table / form / evaluator
  code or CSS (`qtHtml`, `qAccepts`, `qMore`, basket hash keys removed; an old `#qt=1&bk=…` link draws the buckets). The
  round-15 bucket / group card is THE display. Checks `table-is-the-data`, `checker-is-the-caps`, `check-my-catch`,
  `size-group-box` retired; the round 9-15 card checks read the default page (no `#qt=0`). Screens 108-116 deleted.
- **2 Internal test only.** `group.table` / `cap_kinds` / `caps` stay in the snapshot and `oracle.json` (now also Vedder
  May 15, Ape Creek May 1, Hamill Jun 15: 18 groups, 3,100 baskets) but are never drawn. `card-is-the-caps`: on Bella
  Coola May 1 (both parts), Vedder Oct 10 / May 15, Bennett Lake, Ape Creek May 1, Hamill Jun 15 every number the card
  shows agrees with them: a kind row's number = the most of any of its fish's table cells (each = `caps.most_here`); a
  kind keeping none has only none-cells; a size-covered row's cells over the size = min(row, cap); a count group's number
  = a cap's take over exactly its fish; a size line's = a cap's take over fish at its size naming all it names; a
  bucket's = a cap's take over all its kept fish.
- **3 Card refined** (owner's Bella Coola May 1 complaint). Every kind row ALWAYS shows its own number (`kind-number-
  always`; the round 14/15 hiding of a number equal to a shared cap is reversed). Each row says what it shares in small
  tags under its name: a count group's "shares the 1 between them", a size cap's "shares the 1 over 50 cm" (the cap's own
  words split at its number), a shared bucket's "within the river’s 2" (the bucket's rule title; else its number + unit,
  "within the 2 a day"; not where the kind's note already says "counts toward the N"). The size cap is a compact boxed
  line, no pictures, re-ordered from the data's words ("Only **1** brown trout, Dolly Varden/bull trout, lake trout or
  brook trout over 50 cm · Book p.42"), standing right before the first row it covers (`size-cap-line`). The CT/RB count
  box and "This river’s rule · Up to 2 a day between them" stay. No species picture twice in a group
  (`within-shared-no-twice`). The owner's question off one row: "Dolly Varden/bull trout 25 cm or longer · 1 · within
  the river’s 2 · shares the 1 over 50 cm" (`bull-trout-row`). Screens 121-130.
- Persona (haiku): "30 cm bull trout, Bella Coola May 1" → 1 (read off the row; both parts). "Vedder today, how many
  trout" → hatchery only, 4 hatchery rainbows (≤50 cm), the others 2 between them. Trips left: the "shares the 1 over
  50 cm" tag under a "25 cm or longer" row took a second read; whether the steelhead line counts toward the 2 (its row
  carries "within the 2 a day") and how the "Kept … elsewhere" lines combine — data / wording, not changed here.

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
