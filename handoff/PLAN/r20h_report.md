# Round 20h — the region total vs the stream share, one surface, pictures without a tile, the pin line's end (2026-10-19)

Owner: "getting much better, so no big big changes". UI on regs-v11 (regs v32); the data agent's round-20h rows (roe note out of
time, possession duplicate, the example from the water's own total) land separately. Details: app/design/regs-v11/UI-NOTES.md
(v32). Scratchpad: /private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/266b6a4b-cf8d-417b-b326-25e034ead4e0/scratchpad/r20h/

## 1 The region total vs the stream share — two forms, readers, one pick
On a card whose total is the region's and whose headline is a stream share (Vedder 4 → 2, Glenn 5 → 2, Bella Coola 5 → the
river's own 2; Kootenay has one total and is unchanged), two forms were drawn from existing fields and shot at the same states
(`tot=a|b` in the hash):
- (a) the share leads: the data's headline big ("**2** a day per angler from streams"), the region total a quiet note under it
  ("of the 4 a day per angler across Region 2, lakes included"); the pin bar's group line "Trout and char · 2 a day from streams
  (of 4 across Region 2)"; the share tier's own "↳ 2 of them from streams" line not repeated.
  Shots: /private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/266b6a4b-cf8d-417b-b326-25e034ead4e0/scratchpad/r20h/tot-a/{vedder-oct10-card,vedder-oct10-f01,vedder-oct10-f02,glenn-jun15-card,glenn-jun15-f01,bella-coola-may1-card,bella-coola-may1-f01,kootenay-lwa-oct12-card}.png
- (b) the total leads as before ("**4** a day per angler across Region 2, lakes included") with "↳ **2** of them from streams"
  as a second strong line in the head; the pin bar as before. Shots: /private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/266b6a4b-cf8d-417b-b326-25e034ead4e0/scratchpad/r20h/tot-b/ (the same names).
Two fresh opus readers, one per form, the same ten questions (prompt-tot-a.md / prompt-tot-b.md → reader-tot-a.md /
reader-tot-b.md). Both right on every question (Vedder: 4 only if all hatchery rainbows ≤ 50 cm, else 2 from streams; region 4;
Glenn 5 rainbow / 2 other / region 5; Bella Coola 2 / 5; Kootenay 2, the lake's rule). Where they differed:
- the top screenful's "how many from this stream" — (a) conf 4 ("the big 2 is the stream share, the grey 'of the 4' the
  region's"); (b) conf 3 ("this screen only says 2 from streams … with an exception I can't size").
- the pinned bar on the second screenful — (a) "2 a day from streams (of 4 across Region 2)" conf 4; (b) "4 a day across
  Region 2 … a 4 sits in both places" conf 4.
- "the one thing that most risked making you think 4 from the Vedder": (b)'s reader named the head's big 4 and the bar's
  "4 a day across Region 2" read alongside the river's own "up to 4 hatchery rainbow trout" ("easy to merge them into 4 of
  anything from the Vedder"); (a)'s reader named only the river's own-rule heading, which both forms share.
- Bella Coola's screenful: (b) conf 5 ("part of the 5"), (a) conf 4 (two 2s, two book pages).
**Pick: (a), the default.** The owner's rule was "never let a reader think 4 from the stream": only (b)'s reader saw the head's
own number as that risk. (b) stays behind `tot=b`. Checks: r20h-total-relation (fails on v31).

## 2 One surface
Before: card white → head band grey → white → size band tinted → white (and the pin bar grey). After: the card is the one
surface; the head block and the pin bar paint the card colour (the ink rules stay); the size section is framed by two hairlines
full-bleed; the size chip keeps the tint (a chip). Pairs (390 × 844, light and dark): /private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/266b6a4b-cf8d-417b-b326-25e034ead4e0/scratchpad/r20h/rows-before/vedder-oct10-f01.png →
/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/266b6a4b-cf8d-417b-b326-25e034ead4e0/scratchpad/r20h/rows-after/vedder-oct10-f01.png, …-dark-f01.png, …-f02.png (the size section's end, the bar), and the audit sets
/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/266b6a4b-cf8d-417b-b326-25e034ead4e0/scratchpad/r20h/before/ → /private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/266b6a4b-cf8d-417b-b326-25e034ead4e0/scratchpad/r20h/after/ (every tab, five states, light + dark). Check: r20h-one-surface (fails on v31).

## 3 Fish pictures without a tile
The pictures are RGBA cut-outs; the tile was CSS. Gone on kind rows, the Other fish list, the sheets' big picture and the
gallery, light and dark. Pairs: the same f01 / f02 frames and /private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/266b6a4b-cf8d-417b-b326-25e034ead4e0/scratchpad/r20h/before/vedder-oct10-keep-dark.png →
/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/266b6a4b-cf8d-417b-b326-25e034ead4e0/scratchpad/r20h/after/vedder-oct10-keep-dark.png. Check: r20h-picture-surface (fails on v31).

## 4 Open issues
- (i) A pinned tier line now ends once the next section's head is in view. Verified: with the Vedder's own rainbow row (and
  Glenn's Duncan row) at 60 % of the viewport the bar shows no "from streams" line; with the share's head just under the bar and
  the own head below the fold the share is still said. Check: r20h-pin-drops (fails on v31 on both waters).
- (ii) The possession sheet skips the term's line when it only restates the plain line (containment); on rows 25 the term's
  line no longer repeats the rule. (iii) Data 7131c6d4 (gear 11, rows 25, glossary 4), re-snapped: the roe row reads "the fish or
  fish parts ban excepts it" (the data's words; the page no longer adds "still OK — not part of the ban", and the banned row's
  "roe is still OK" is dropped); the possession sheet lists the card's own examples — "With 4 trout and char a day you may have
  8: 2 days’ fish. **Possession doubles the 4 a day, not the 2 from streams.**", then whitefish 15 → 30, bass 20 → 40 — the
  generic 5 → 10 only as a fallback. Shots: r20h/look/vedder-oct10-sheet-possession.png, r20h/look/vedder-oct10-gear-tab.png.
  Checks: r20h-roe-names-ban, r20h-possession-own-example (both fail on v31 with the new snapshot: check-20h-before.txt).

## Readers (fresh opus, PNGs only; the round-20g 36 questions + Q29 Vedder-vs-region, F9 the same from one screenful, Q30 possession 8 or 4)
BEFORE (regs v31; r20h/gate-before + rows-before + gate-before/NEW): **39 / 39**; Q29 conf 4 ("how many from the Vedder has no
single number"), F9 conf 4 ("a reader could think 2 is the Vedder's maximum"), Q30 conf 4 (8, inferred from the region's 4).
reader-before.md.
AFTER (regs v32 on rows 25, every screen re-shot; r20h/gate-after + rows-after + gate-after/NEW): **39 / 39**, no new wrong
answer (reader-after.md). Moved: Q30 possession conf 4 → 5 ("the sheet says 'With 4 trout and char a day you may have 8 …
Possession doubles the 4 a day, not the 2 from streams'; the example spells it out"); F7 "2 or 4 from this stream" conf 5 ("the
pinned bar settles it: 2 a day from streams (of 4 across Region 2)"); Q1 conf 5 from the "of the 4 a day per angler across
Region 2, lakes included" note; F9 conf 4 — "the big 2 is the streams' number and the 4 is the region's" (the risk the owner
named — reading 4 as the stream's — did not occur); Q29 conf 4 (two right answers: 2 unless they are hatchery rainbows, then
up to 4; region 4). Still read twice, none new: F2 "of the 2" conf 3 (both rounds), the Chilliwack rainbow without the "not
counting" clause (data), "NEXT Jun 1–Apr 30" (the 20i data moves it to Jul 1), Bennett's three stacked lake-trout limits.

## Checks
check.mjs **103 / 103** on the final page + the rows-25 snapshot (check-full-5.txt; 97 before this round). New (ONLY=20h; all six
fail on regs v31 — check-20h-before.txt): r20h-one-surface, r20h-picture-surface, r20h-pin-drops, r20h-total-relation,
r20h-roe-names-ban, r20h-possession-own-example. Retargeted with a measured reason in the file: r20f-headers, size-tier-box,
r20d-flat (the size section is two hairlines, no band), r20d-compact-head (the lead line's height), r20d-pinned-stack (a two-line
group line; the share rides it; a line ends when the next head is in view), r20e-region-vs-stream and r20f-region-words (the
group line says the share), r20g-palette (the tint lives on chips), group-head-compact and ur1-tier-words (notes stripped before
comparing), ur2-bait-allowed-note (the roe row names its ban), r20g-possession-fold (the card's three examples: 273 px).
Colour census unchanged from round 20g (6 / 5 / 5 / 4 light and dark; census-after.json).

## Commits
- 7131c6d4 — ROUND20h data (gear 11, rows 25, glossary 4), the Opus data agent's.
- Regs v32 (this round's page commit): app/design/regs-v11.html, regs-v11/{tools/check.mjs, v36snap.js, oracle.json,
  UI-NOTES.md, screens/ (every screen re-shot; screens/r20h/ holds the before/after pairs and the two head forms)},
  handoff/PLAN/r20h_report.md. Not published. app/design/regs-diagram/ untouched.
Committed pairs (app/design/regs-v11/screens/r20h/): before-/after- vedder-oct10-f01 + dark, -f02 + dark, glenn-jun15-row-rb,
vedder-oct10-keep-dark, bennett-keep-light, vedder-oct10-sheet-possession, vedder-oct10-gear-tab; total-a-* / total-b-* (the
Vedder card and screenfuls, Glenn and Bella Coola screenfuls in each form).
