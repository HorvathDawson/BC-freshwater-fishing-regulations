# Round 20d — the nesting rework: stacked pinned headers, flat sections, clearer grouping (2026-10-19)

Owner: "we just need the nesting rework and improve the way groups are grouped visually" … "maybe instead of nesting it could
just be stickied headers that stack up" … "never unpin a share tier like 'Only 2 of the 4'" … "write the stack so it flows".
UI only (regs-v11.html); the data tree is unchanged. Details and the page-side connecting words: app/design/regs-v11/UI-NOTES.md (v28).

## What changed on the screen
- Nesting → a FLAT list: each tier of the data tree is a section header (its full data words, a bar at the card's edge) and the
  rows after it sit on the header's text edge; no rails, no box in a box. A size section is a tinted band. A group starts with a
  full-bleed tinted head band (name · scope / the total on one line) and ends with air; Possession is a card-level row.
- A pin bar under the header carries the chain of sections the reader is inside, one short flowing line each (built from the
  tiers' fields): "Trout and char · 4 a day across Region 2" / "↳ 2 of them from this stream" / "↳ of those, 1 over 50 cm (2 if
  one is a hatchery steelhead)" / "↳ plus up to 4 hatchery rainbow trout under this river’s rule". Every count-limiting level
  stays pinned while inside it; a line goes when its section ends.

## Before / after screenfuls (390×844; absolute paths)
Scratchpad: /private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/266b6a4b-cf8d-417b-b326-25e034ead4e0/scratchpad/r20d/
- Vedder, bull trout row mid-frame: frames-before/vedder-oct10-row-dv.png → frames-after/vedder-oct10-row-dv.png
  (before: three full headers pinned, 2 px box; after: a two-line bar "Trout and char · 4 a day across Region 2 / ↳ 2 of them
  from this stream", the band's head in place, rows flush)
- Bella Coola May 1, cutthroat row: frames-before/bella-coola-may1-row-ct.png → frames-after/bella-coola-may1-row-ct.png
  (after: "Trout and char · 5 a day across Region 5 / ↳ 2 of them under this river’s rule / ↳ of those, 1 over 50 cm", then the
  count header "1 cutthroat or rainbow trout in all (part of the 2)" in place)
- Bennett, bull trout row (the lake's box): frames-before/bennett-row-dv.png → frames-after/bennett-row-dv.png
- Group boundaries: frames-before/vedder-oct10-f03.png → frames-after/vedder-oct10-f03.png (the end of Trout and char — How it
  adds up under the pinned group line — into Other fish), frames-after/vedder-oct10-f04.png (Other fish → Possession row → Where
  you can't fish → Gear), frames-after/vedder-oct10-f01.png (the group head band + compact head).
The same four pairs are committed under app/design/regs-v11/screens/r20d/.

## Measured
- Pinned stack (measured, 390 px): Vedder bull trout row 2 lines 49 px; Chilliwack steelhead row 3 lines 71 px; Bella Coola
  cutthroat row 3 lines 71 px; Bennett bull trout row 2 lines 49 px; Glenn rainbow row 2 lines 49 px; Kootenay LWA 1 line 27 px
  (bound 90 px). The group head band 57 px (name line + ONE total line; 95 px before). Two of today's full header lines ≈ 90 px.
- Logged, small: the gear tab's row labels (LINES / HOOKS) still render in caps — a `.gline .glab` rule outranks round 20c's
  sentence-case rule; Bennett's pinned own line reads "2 of them lake trout under this lake’s rule" (a folded own tier that is
  not a lift) — a comma or dash around the fish would read better; page-side words, next round.
- Checks: r20d-pinned-stack, r20d-flat, r20d-compact-head — all three fail on regs v27 (r20d/check-before.txt), pass after.
  check.mjs full: see the end of this file.

## Readers (fresh opus, one PNG per question, keyed from the data)
| | F1 group + total | F2 "of the 2" = ? | F3 Bennett's 5 | F4 Glenn rainbow 5, whose | F5 Bella CT after a RB | F6 Bennett LT 2, 70 cm, inside | F7 Chilliwack: this stream 2 or 4? | total |
|---|---|---|---|---|---|---|---|---|
| BEFORE (v27, pinned full headers) | 1 | 1 (conf 3) | 1 | 1 | 1 | 1 | — | 6 / 6 |
| AFTER (v28, stacked flowing lines) | 1 | 1 (conf 3) | 1 (conf 3) | 1 (conf 3) | 1 | 1 | 1 (conf 5: "2, not 4") | 7 / 7 |
Remaining doubts, both data-word matters (logged for the owner): a row's bare "of the 2" / "of the 5" sits under a neighbour row
that says "Up to 2" / "Up to 5" for other fish — the reader wished the row said "of this stream's 2"; Glenn's own section
("The Duncan River’s rule: up to 5 rainbow trout a day") does not say in place that its rainbows sit outside the stream's 2
(the pinned line "plus up to 5 rainbow trout under the Duncan River’s rule" says it, but only once that header has passed).

## Full-card reader (the r20b 21-question set on the redrawn tall screens)
**21 / 21** (before 21 / 21), no new wrong answer. Its reads: "Only 2 of the 4 may be from this stream, not counting hatchery
rainbow trout" took a second read (data words); whether the second fish over 50 cm may be another steelhead (data words, as in
rounds 20b/20c); Bennett's slot size and the combined "no more than 3" (unchanged); Glenn's third water, Trout Lake (data).
Nothing read as 4 + 2: Q1 "4 … across all of Region 2", Q7 "up to 2 … and Only 2 of the 4 may be from this stream".
