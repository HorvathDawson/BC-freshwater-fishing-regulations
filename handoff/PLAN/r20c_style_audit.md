# Round 20c — format and style audit of regs-v11 (2026-10-19)

Yardstick (owner): good mobile design — a clear type hierarchy with few sizes/weights, one spacing rhythm, aligned left
edges, one link style, restrained colour, no box-in-box clutter, readable dark contrast, 44 px targets, clean wraps — and,
one SCREENFUL at a time, a reader must always know what they are looking at. Riffle glanced at for the palette/fonts only
(same tokens: Archivo + Bricolage Grotesque, --ink/--sub/--faint, --accent, --open/--closed).

Measured (tools/census.mjs on the rendered Keep tab, five waters, light + dark; tools/audit.mjs tall screens; tools/frames.mjs
390×844 screenfuls). Screens: scratchpad r20c/before/*.png, r20c/frames-before/*.png (BEFORE = regs v26 @ 5de14cce);
r20c/after/, r20c/frames-after/ (AFTER). Severity: H / M / L. SMALL = CSS/layout only, done; BIG = structure or words, logged.

| # | issue | where (screen) | sev | SMALL / BIG | done |
|---|---|---|---|---|---|
| 1 | **No context mid-scroll.** Inside the over-50 box the frame shows "Only 1 of the 2 may be … bull trout" with no fish group, no total, no "Only 2 of the 4", no box head in view; Bennett's "Only 3 of the 5" with no 5; Glenn's "any size" chip with its tier line gone; "HOW IT ADDS UP" with no group name. | frames-before/vedder-oct10-f02, bennett-f02, glenn-jun15-f02, chilliwack-p2-oct10-f03 | H | SMALL (CSS sticky) | yes: the group's name + total line pin under the header while inside the group; a top-level tier line (share / own) pins under it; the size box's head pins under that. Measured offsets, `position:sticky`, no new words. |
| 2 | **Box-in-box.** The size box (2 px ink border) sits inside the share tier's 4 px rule-bar + 2 px guide line, and holds bordered size chips: three nested border signals. | before/vedder-oct10-keep-light, bennett-keep-light | M | SMALL for the chips and the border weight; BIG for the nesting itself (the owner keeps the box) | partly: chips become tinted pills (no border), the box border 1.5 px, its head wash lighter; nesting logged |
| 3 | **Five type sizes, five weights** on the card (10 / 12.5 / 15 / 17 / 26; 400 500 600 700 800, + 900 on gear); the 10 px is the year strip's month axis (JetBrains Mono, fine as a chart axis); weight 500 is picture captions and the strip. | census-before.json keep.sizes / weights | M | SMALL | yes: four sizes (the strip axis kept), four weights (500 → 600, 900 → 800) as tokens --w-body/--w-emph/--w-head/--w-num |
| 4 | **Big numbers' line boxes.** 26 px numerals inside 15 px lines: tier lines grow to 30 px on the number's line while other lines are 20 px; three different rules (.tn margin, .ghn vertical-align −2, .kn line-height .7). | vedder-oct10-keep-light (every tier line) | M | SMALL | yes: one `.n-inline` rule for every inline number (line-height .7, vertical-align −1 px) |
| 5 | **Two greys for body text.** --sub (95,102,108) and --faint (110,117,123) both carry running text on the card (worked lines, presence, chip words); --faint is the weaker contrast in dark mode (≈4.5:1 at 12.5 px). | census keep.colours; vedder-oct10-keep-dark | M | SMALL | yes: --faint only for caps kickers and the strip axis; card body meta uses --sub |
| 6 | **Links in three colours and four markers**: accent ↗ with underline (Rule book), accent › (sheets), plain accent (How to tell, See every regulation line), ▸ (In the book's words), underline-only (Sources on gear), ink buttons that look like links (Source chip). | census keep.linkColours / linkMarks; sheet-howtell-light | M | SMALL | yes: one link style — accent, 700, no underline; ↗ only for the PDF, › for in-app; ▸ → ›; the Source chip reads as a button (tint, no underline) |
| 7 | **ALL-CAPS overused**: 3 on the card, 14 on gear, 14 on licence (row labels LINES / HOOKS / WEIGHT ON THE LINE, YOU NEED, B.C. RESIDENT · 16 OR OLDER, DONE …). | before/vedder-oct10-gear-light, -licence-light, sheet-date-light | M | SMALL | yes: caps kept for section kickers only (.lab, .scope, .bkh); row and column labels become sentence-case 700 --sub; DONE → Done |
| 8 | **Duplicated labels**: "GEAR · FOR THIS WATER TODAY" over "Gear & methods", "LICENCE · FOR THIS WATER TODAY" over "Licence" (the tab already names the tab; the header shows the date). | before/vedder-oct10-gear-light, -licence-light | L | SMALL (a page label, no data words) | yes: the two `.sech` labels removed (−6 words on each tab) |
| 9 | **Touch targets under 32 px**: "How to tell ›" inside a kind's sentence (20 px, a round-20b inline link) and the "Source" chip (28 px). | census keep.smallTargets | M | SMALL | yes: inline padding 12 px with negative margins (the line box unchanged), Source min-height 32 → 44 hit area |
| 10 | **Orphans / wraps**: the header line wraps "… across Region 2, lakes / included ⓘ Rule book p.21"; "… up to 4 hatchery rainbow / trout a day"; a picture caption "Coastal cutthroat trout ›" in three lines under an 80 px tile. | before/vedder-oct10-keep-light | L | SMALL | yes: `text-wrap: balance` on the header's root line and tier lines; captions `text-wrap: pretty` |
| 11 | **Spacing rhythm**: one-off vertical gaps 2/3/4/5/6/8/10/12/14 px across .tnode/.krow/.kmeta/.kac/.ksn/.worked. | vedder-oct10-keep-light | L | SMALL | yes: a 4-step scale --s1 4 / --s2 8 / --s3 12 / --s4 16 and the one-offs mapped onto it |
| 12 | **Indent rail**: kinds sit 36 px in from their tier line (margin 10 + padding 12 + border 2 + row); on 390 px that costs a line of text in every box. | vedder-oct10-keep-light | L | SMALL | yes: 28 px (margin 8 + padding 10 + 2 + 8), still one clear rail |
| 13 | **Dark mode**: fine overall; the size box's 2 px white border is the loudest element on the screen; --faint meta text is borderline. | before/vedder-oct10-keep-dark | L | SMALL | yes: via #2 (1.5 px, line2 for the chips) and #5 |
| 14 | **Tab bar**: the three underline tabs are fine; the pressed tab's 3 px bar and the header's 1.5 px rule meet correctly. | before/*-gear-* | — | — | no change |
| 15 | **Stray row** under Other fish: "Possession ⓘ : twice the daily quota … Source Rule book p.21 ↗" mixes an inline ⓘ, a chip button and a link in one grey line. | vedder-oct10-keep-dark (bottom) | L | BIG (words / structure) | logged |
| 16 | **Picture strips**: three or four 80 px tiles with captions under every kind row make a card long; the owner chose whole pictures (round 10). | every keep screen | L | BIG | logged (a single row of smaller thumbnails would halve the card) |
| 17 | **The over-50 box nested in the share, the share in the total**: the structure itself is what needs three pinned lines to read mid-scroll. | frames | M | BIG | logged (a flat list with "part of" words was rejected in rounds 16/17) |

## Screenful gate (frame-only reader, fresh opus, one PNG per question)
Questions keyed from the data: F1 Vedder f02 — which fish group, the most in all (trout and char; 4); F2 Vedder f02 — "Only 1 of the
2": 2 of what (the 2 a day from this stream, inside the 4); F3 Bennett f02 — "Only 3 of the 5": the 5 (5 trout and char a day per
angler across Region 6); F4 Glenn f02 — the rainbow row at the top: how many, whose rule (5; the Duncan River's rule).

| | F1 | F2 | F3 | F4 | total |
|---|---|---|---|---|---|
| BEFORE (frames-before/, regs v26) | 0 — "the group heading has scrolled off"; a guess | 0 — "whatever 2 it nests under has scrolled away" | ½ — worked out from the How-it-adds-up footer, conf 3 | 0 — "its number is above the frame"; "5 or 2?" | **0.5 / 4** |
| AFTER (frames-after/; the restyled card is shorter, so F2/F4 were re-pointed at the frames holding those rows, vedder-oct10-row-dv.png and glenn-jun15-row-rb.png, each with the row at 60 % of the viewport, as the check places it) | 1 — read off the pinned "Trout and char · 4 a day per angler across Region 2, lakes included" | 1 — "of the 4 … only 2 may come from this stream … of those 2, only 1 may be a bull trout" | 1 — off the pinned "5 a day per angler across Region 6" | 1 — "The Duncan River's rule: up to 5 rainbow trout a day … covers its tributaries" | **4 / 4** |

Its remaining doubts (AFTER): the row could still be tied to "2 if one is a hatchery steelhead" rather than the share's 2 (the box
head sits between them) — would need the row to say "from this stream" (words: logged); how Glenn's rainbow 5 relates to Region 4's
5 and to the share's 2 is not on one frame (the Q23b reading; words: logged); "Its row covers its tributaries" does not name the row.

## Results

- Done (SMALL): #1 pinned context, #2 chips + box weight, #3 four weights, #4 one line box for inline numerals, #6 one link style,
  #7 caps only on kickers, #8 duplicated tab kickers removed, #9 44 px hit areas, #10 balanced wraps, #11 spacing scale, #12 28 px
  rail, #13 via #2. Verified, no change: #5 — the second grey is `--faint` on kickers and the strip's axis only; body meta is `--sub`.
  Logged (BIG): #15 the stray Possession row, #16 picture strips, #17 the nesting itself; and the readers' two wording wishes above.
- Measured (census, Keep tab, Vedder, before → after): weights 5 → 4 (400 / 600 / 700 / 800); sizes 5 → 5 where the fifth is the year
  strip's 10 px month axis (the card's text uses 4); underlined links 5 → 0 on the card, 2 → 0 on gear, 1 → 0 on licence; ALL-CAPS
  elements on gear 14 → 4 (the kickers) and on licence 14 → 3; kind rows 52 → 47 px from the left edge; the size box 2 → 1.5 px.
- Full-page reader (the r20b 21-question set on the restyled tall screens): 21 / 21 (before 21 / 21).
- Screenful reader: 0.5 / 4 → 4 / 4.
- Checks: `r20c-pinned-context`, `r20c-style-tokens` (both fail on regs v26; both pass); `group-head-compact` reads the two header
  blocks; `tree-folders` takes a tinted chip as prominent. Tools: tools/audit.mjs (every tab and sheet, light + dark),
  tools/frames.mjs (390×844 screenfuls), tools/census.mjs (computed-style census).
- Most visible before/after pairs (absolute paths under /private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/266b6a4b-cf8d-417b-b326-25e034ead4e0/scratchpad/r20c/):
  frames-before/vedder-oct10-f02.png → frames-after/vedder-oct10-f02.png (pinned context inside the over-50 box);
  frames-before/bennett-f02.png → frames-after/bennett-f02.png (a top-level box pinned under the group);
  before/vedder-oct10-keep-light.png → after/vedder-oct10-keep-light.png (chips, box, links, numerals, rail);
  before/vedder-oct10-gear-light.png → after/vedder-oct10-gear-light.png (caps → sentence case, no kicker, one link style);
  before/vedder-oct10-keep-dark.png → after/vedder-oct10-keep-dark.png (dark: lighter box, tinted chips).
