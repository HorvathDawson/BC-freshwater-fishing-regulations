# Round 20g — colours with one meaning each, one way to show a future change, the ways list, possession (2026-10-19)

Owner items 1–4 on regs-v11 (regs v31), UI only; the Opus data agent's round-20g rows (origin rule under a stream share,
shorter steelhead notes, possession) land separately and are re-snapped after. Details: app/design/regs-v11/UI-NOTES.md (v31).
Scratchpad: /private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/266b6a4b-cf8d-417b-b326-25e034ead4e0/scratchpad/r20g/

## 1 Colours — the census, before → after
`tools/census.mjs` counts the distinct computed text colours on each tab (five states, light and dark). Distinct text colours,
Keep / Gear / Methods / Licence:

| state | before (v30) | after (v31) |
|---|---|---|
| Vedder Oct 10, light | 7 / 6 / 6 / 5 | 6 / 5 / 5 / 4 |
| Vedder Oct 10, dark | 7 / 6 / 6 / 5 | 6 / 5 / 5 / 4 |
| Kootenay LWA Oct 12, light + dark | 7 / 5 / 6 / 5 | 6 / 5 / 5 / 4 |
| Bella Coola May 1, light | 7 / 5 / 6 / 5 | 6 / 4 / 5 / 4 |
| Glenn Jun 15, dark | 7 / 4 / 6 / 5 | 6 / 4 / 5 / 4 |
| Bennett Lake, light + dark | 6 / 5 / 6 / 5 | 5 / 5 / 5 / 4 |

(The same counts on the rows-24 data after the re-snap: census-after.json.)

What the Vedder's Keep tab used, light (count = text elements): before — ink ×33, accent ×17, a second grey `--faint` ×15, `--sub`
×14, open green ×8 (the group total "4", the tier numbers "2", "1", "2"), restricted amber ×3 (the duty line, the release words),
closed ×1. After — ink ×42, sub ×29, accent ×16, amber ×2, green ×1, red ×1: the three status colours now sit ONLY on status
(the year strip's Open, the release / closed words of the regulation lines), every number is ink, and one grey is left.

Tokens, each with one meaning (UI-NOTES v31 has the list): `--ink` text + every number · `--sub` the one secondary grey ·
`--accent` links and actions only · `--open` / `--closed` / `--restricted` status only (year strip and legend, Open / Closed,
the ✓ / ✕ marks, a closed or release row's title and number slot, the regulation lines' status words) · `--tint` the one grouping
tint (group head band, size band, pin bar, the Now / Next chip) · `--card` only as text on a filled control. `--faint` and
`--wash` resolve to `--sub` and `--tint` in light and in dark. Also: the size note's accent bar became a `--line2` rule (the
accent was decorating a note, not an action).

Before/after pairs (390 × 844 screenfuls, light and dark): before/ ↔ after/ (every tab of five states, `audit.mjs`), and
r20f/rows-after ↔ r20g/rows-after (the Vedder's seven frames light + dark, the seven row frames). Look at
before/vedder-oct10-keep-light.png ↔ after/vedder-oct10-keep-light.png: the "4", "2", "1" in green are ink now; the duty line
is no longer amber; the kicker and "How it adds up" share the one grey.

## 2 ONE pattern for a rule that differs on other dates
The 20e note was a tinted box with an amber bar, "ⓘ Rule book p.22 ↗" inline, after the line's source line. Now: ONE quiet line
`.chg` right under the item's value — a chip, the dates in ink, then the data's words — before the item's source line, which
carries the change's rule too ("Province · Region 2 · This water’s ›" + its pages). The same on Hooks (Oct 10: "May 1–31: fly
fishing only"), on a fly-only day's Hooks (May 15: "Jun 1–Apr 30: one hook, lure or fly and weight on the line up to 1 kg
(2.2 lb)") and on Bait, between the bait sentence and the list ("May 1–31: bait ban · in force today").

Two chips were drawn and shot at the same states (chg-a/, chg-b/: Vedder Gear Oct 10 and May 15) and read by a fresh opus
reader from the PNGs (prompt-chg.md → reader-chg.md): (a) a ◷ glyph on every dated line; (b) a NOW / NEXT chip.
**The pick is (b), confidence 4.** The reader: with (a) "the clock line sits directly under the hook rule in the same small type,
so for a beat it reads like part of today's rule… the clock-icon line looks the same whether it describes today (bait) or other
dates (hooks); only the words 'in force today' tell them apart"; with (b) "NOW on the bait-ban line and NEXT on the Jun 1–Apr 30
line make the difference clear before reading"; neither looks like a warning; (b)'s chips "add a little visual weight… neutral
labels, not alarms". `chg=a` in the hash keeps the glyph. Page-side words: "Now", "Next", "· in force today".
Methods and the keep rows carry no dated changes in the data (a way's fly-only condition is its own small line, a row's dates are
its year strip), so there is nowhere else to apply it yet; `r20g-changes-line` refuses any dated note outside the pattern.

## 3 Ways to fish / Bait: one list
At 390 px the two columns wrapped "Water insects and crayfish" and left the ✓ column half empty. Now one stacked list: the ✓ group,
then the ✕ group, a mark on every row, the name in emphasis, the condition small under its name, a short grey header over each
group. A lone group (Bait on May 15, everything banned) is one column. Pairs: before/kootenay-lwa-oct12-methods-light.png ↔
after/…; gate-before/vedder-may15-gear-tab.png ↔ gate-after/NEW/vedder-may15-gear-tab.png.

## 4 The possession sheet
One plain line (the possession row's words: "Possession: twice the daily quota (fish at home don’t count)."), the term's example,
"In the rule book’s words ▸" closed by default (the term's sentence + the exact quote inside), then "Source › Rule book p.21 ↗".
388 px tall, was 512. Pair: gate-before/vedder-oct10-sheet-possession.png ↔ gate-after/NEW/vedder-oct10-sheet-possession.png.

## 5 The round-20g data on the page (rows 24, display 16, glossary 3 @ b13f6110; re-snapped, oracle regenerated)
- The origin rule where its scope is: on a Region 2 stream card the group head has no origin line any more; under the share's
  line "Only 2 of the 4 may be from streams, not counting hatchery rainbow trout" stands "From streams, keep hatchery fish only
  — wild trout and char go back." with "How to tell ›" and "Rule book p.21 ↗" (moved from the head). A water's own total keeps
  its origin at the head. Screen: gate-after/ur1-vedder-oct10-cards.png.
- The steelhead row: no size note (the band head says "— 2 if one is a hatchery steelhead" once); "10 a year · Stop fishing here
  after 2 a day · Record each on your licence right away"; "A rainbow trout over 50 cm (20 in) here counts as a steelhead."
- Possession: the card row "Possession · 2 days’ quota ›" (the foot is two sentences now; the row takes the value, the sheet the
  rest); the sheet: "Possession: 2 days’ quota. Fish at home don’t count." / the term's line ("The fish you have with you at any
  time — in camp, a cooler or a vehicle: 2 days' quota. Fish at home don't count.") / "For example: With a daily quota of 5 trout
  you may have 10: 2 days' fish, not 3." / "In the rule book’s words ▸" (closed, the p.80 definition) / "Source › Rule book p.21 ↗".
  The data agent's logged confusion — which daily number is doubled — is a words matter; layout-wise the sheet opens from the row
  under the card whose total is on the pinned head.

## Readers (fresh opus, PNGs only; the r20f 34-question set + Q27 bait and hooks on the Vedder May 15 + Q28 possession)
BEFORE (regs v30; r20f/gate-after + rows-after + r20g/gate-before): **36 / 36**; Q27 conf 4 ("the Jun 1–Apr 30 band reads oddly
because the river is closed all of June"), Q28 conf 5; F2 conf 3 (two different "2"s near the bull trout row — the data agent's
origin-rule item). reader-before.md.
AFTER (regs v31; r20g/gate-after + rows-after + gate-after/NEW): **36 / 36**, no new wrong answer (reader-after.md). Q27 conf 4
(as before: "the NEXT line from Jun 1 doesn't say whether the barbless rule continues; also the river is closed Jun 1–30" — the
data's change words), Q28 conf 5 ("twice the daily quota… fish at home don't count", nothing read twice), Q26 conf 5 (the NEXT
line read as the May dates at once). What the reader read twice, none of it new this round: F2 "of the 2" conf 3 (both rounds;
the data agent's origin item), F4 conf 4 — the pin "↳ 2 of them from streams" is still up while the Duncan River's own rainbow
row (outside the share) is on screen (the share's size line is still 130 px under the bar, so the drop rule — less than a row
left — has not fired; a follow-up for the pin rule: drop a tier line once the next section's head band is in view), the
Chilliwack rainbow row outside the grey box (both rounds), and a data note: on Oct 10 the Vedder's bait list says "Roe — still OK
— not part of the ban" with no ban in force that day (the bait pane's words; for the data side).
AFTER, FINAL (regs v31 on rows 24, every screen re-shot; the same 36 questions, a third fresh reader): **36 / 36**, no new
wrong answer (reader-after-final.md). Q27 conf 4 (same note as before), Q28 conf 4 ("two days' quota, e.g. 8 at Region 2's 4
a day; fish at home don't count" — the example's 5 is not the water's 4: the term's words), Q26 conf 5 ("NEXT May 1–31"), F4 now
conf 5 ("5 rainbow trout a day, the Duncan River's rule", nothing read twice — the shorter card put the Duncan head band in the
frame). Read twice, none new: F2 conf 3 (both rounds, both data sets), Q17 conf 3 (Glenn's worked line "If you keep 2 rainbow
trout: none from other streams today" against the Duncan's 5 — the data's words, logged in round 20e), the 1-over-50 wording
differing card to card.

## Checks
check.mjs: **97 / 97** on the final page + rows-24 snapshot (check-full-5.txt; 94 before this round). New (ONLY=20g; all fail on v30, check-20g-before.txt): r20g-palette, r20g-changes-line, r20g-possession-fold.
Retargeted with a measured reason: r20e-dated-on-its-line (quiet line before the source line), r20e-ways-columns (one stacked
list, a mark per row) — both fail on v30 (check-20e-before.txt); r20e-sources-per-item, gear-sentence, gear-lines (the change's
rule and page join its line's source line); dated-rows (the chip is not a word); bait-ways-compact (one stacked list, so the v28 two-column
height no longer bounds Bait; compact now means no slack: every row as tall as its text, no gap over 8 px); ur2-bait-allowed-note (rows carry their own mark).

## Commits
- b13f6110 — ROUND20g data (rows 24, display 16, glossary 3), the Opus data agent's.
- Regs v31 (this round's page commit): app/design/regs-v11.html, regs-v11/{tools/check.mjs, tools/census.mjs, v36snap.js,
  oracle.json, UI-NOTES.md, screens/ (every screen re-shot; screens/r20g/ holds the before/after pairs and the two changes-line
  styles)}, handoff/PLAN/r20g_report.md. Not published. app/design/regs-diagram/ untouched (the other agent's).
Committed pairs (app/design/regs-v11/screens/r20g/): before-/after- vedder-oct10-keep light + dark, kootenay-lwa-oct12-methods,
vedder-oct10-gear, vedder-oct10-licence, vedder-may15-gear-tab, vedder-oct10-sheet-possession, vedder-oct10-row-dv;
changes-a-glyph-* / changes-b-chip-* (Vedder Gear Oct 10 and May 15).
