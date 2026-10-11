# regs-v10: the UI practices applied, and where

- **Visual hierarchy.** Each screen has one primary answer: the verdict (✓ Open / ✕ Closed). The group numeral is second. The type scale has exactly four sizes: 26 (verdict and numerals), 17 (water name and section and group heads), 15 (body), 12.5 (meta and labels). They are set as `--t1`–`--t4` in regs-v10.html, and v36's fragments are forced onto them.
- **Progressive disclosure.** On the main view: the verdict, each group's total and sub-caps, and each kind's limit, size, origin and duties. On demand, in a sheet: how it was decided, rule wording, glossary, and the species' look-for notes. Every rule that changes what you may keep stays on the main view. Examples: kind rows (`.krow`), "Kept 2 on another stream?", steelhead duties.
- **Consistency: one component per job.**
  - One row for kinds, other fish, the date list and the parts (`.krow`, `.orow`).
  - One chip, for sources.
  - One sheet (Sheet.tsx look) for kind, rule, glossary, species, source and date.
  - One section header: a 1.5 px ink rule plus the heading.
- **Proximity by whitespace and rules, not boxes.** Groups are separated by a 1.5 px ink rule and their heading, with no nested cards (`.row.ledger`). The only bordered elements are controls: the segmented control, the date pill, and the map button.
- **Scannable text.** Each group's sentence is split into a bold lead plus a short dashed list (`.lead`, `.caps`). Kind rows front-load the name, then "Keep up to **2**", then the chips. There are no paragraphs on the main view. Long text (exact words, glossary) lives in sheets.
- **Touch targets of 44 px or more (all, including the ⓘ tips after the review), within thumb reach.** Covered: the segmented control and View on map (44 px), the date pill (44 px), the part segments (40 px tall and full width), kind and other-fish rows (≥ 44 px), tabs (48 px), sheet Done (44 px), and the map's part squares (44 px hit area). The ⓘ tips are 28 px, centred in their line, the one exception; it is noted below.
- **Contrast and no colour-only meaning.** Text tokens hold AA against card. Status always carries a glyph as well as colour (✓ Open / ✕ Closed). On the strip, every flag from the data has its own pattern: closed is hatched, fly-only is striped, bait ban is underlined in amber. The legend carries ✓, ✕ and a word for each. Release words are amber with the word itself ("release"), and closed reads "closed".
- **Predictable navigation.**
  - The water map is an overlay. Back (‹, Escape, or a swipe from the left edge) returns to the regulations at the same scroll position (screens 24 → 25 → 26).
  - The sheets close back to where you were.
  - The segmented control scrolls within one page, so state is never lost.
  - The header back (‹) goes to the app's Search.
  - The bottom tabs stay the water's tabs throughout.
  - There are no dead ends: closed days link to the reopening day, and days not in the prototype say so.
- **Platform conventions.** Bottom sheets with a grabber and Done. A segmented control for Keep / Gear & licence and for 16 or older / Under 16. A back chevron and edge-swipe back on the map overlay. A sticky header that compacts on scroll (the part strip hides, and the map button drops its label).
- **Empty, closed and unknown states.**
  - Closed day: a banner listing every closure, plus "No gear may go in the water…" on Gear & licence (screens 09, 10, 17).
  - Days not in the prototype: an explicit note in the date sheet.
  - Data still missing: a dashed "pending data" marker.
- **Dark mode and 200% text.** All colours are theme tokens (screens 21, 22). Every size is in rem. At 200% the sticky header turns static, so content is not crushed (screen 29).

## Review fixes applied (v10 review, all 15)
1. **Decision sheets.** One small heading per level ("Region 2 · region-wide"). Each row is the plain rule plus a grey effect line, with no badges, no quote marks and no green. Rule rows no longer repeat "Region 2 rule · Region 2". Sheets now cover the tab bar.
2. **Segmented control.** It follows the section under the header, and the end-of-page case is handled. Tapping a segment sets it at once. The duplicate "Gear & licence" heading is gone.
3. **Under 16.** The title and body are separate blocks.
4. **Vedder group.** The Q&A collapsed to one "Fished elsewhere today? ›" row that opens a sheet. The group is now about 13 lines before EACH KIND.
5. **Strip.** The slider handle became a 2 px ink tick with the date above it, and nothing about it says "drag".
6. **The words' ⓘ tips.** Now labelled links ("Daily quota", "Region 2", "Stream"), each 44 px tall. Inline ⓘ tips have 44 px hit areas.
7. **Header.** No pill border: the date is accent text with ▾. The map button keeps the word "Map" with an 18 px icon when the header compacts. The header is 107 px without the status bar.
8. **Gear summary.** Borderless, two columns, one rule under it.
9. **Prices.** Archivo with tabular figures, not monospace.
10. **Bait.** The glyph comes before each item: "✓ Worms ✓ Roe ✓ Insects ✕ Fish parts".
11. **Folds.** Each is a 48 px row with a chevron that rotates when it opens.
12. **Species row.** About 3.5 tiles show, which cues scrolling. Labels are one line with an ellipsis. No tile border, and dark-mode pictures are dimmed.
13. **Closed list.** One lead line. Each item is just the dates and scope, with Source right-aligned on the same line.
14. **Copy.** No hand edits to the data's wording; "1 line (rod)" is left to the pipeline.
15. **PARITY.md.** Corrected: both "All sources" buttons are hidden.

The Kootenay's "Only 1 … over 50 cm" line now carries the ✓ like the Vedder's.

## Self-critique pass
| Check | Result | Fix |
|---|---|---|
| Segmented labels wrapped to two lines in uppercase | Failed | Sentence case at 12.5, no tracking. They now fit on one line. |
| Kind rows ran "2" into "30 cm" | Failed | Spacing around the numeral and the chips. |
| Ways to fish and Always rendered as run-on text | Failed | Restyled v36's `.wrow`, `.nochip` and `.hcard` as rows and chips. |
| Release lines lost their amber | Failed | `.sl .rl` coloured. |
| "Pending data" box made Cowichan taller than v36 | Failed | Made it one inline legend item. |
| Folds captured open made Gear & licence taller | Failed | Folds now start closed. |
| ⓘ tips were 28 px, below 44 | Fixed | 44 px hit areas, and the group's tips are labelled links. |
| "All gear/licence sources" buttons hidden | Open gap | See PARITY.md. |

## Answers 2.6 pass
- **Pending markers removed.** The strip, the group lead and items, and the book links now come from the data.
- **Book links.** Each is a 44 px tap target labelled "Book p.N ↗". It opens the PDF in a new tab.
- **Rule sheet subtitle.** "Region 2 rule · Region 2" collapses to "Region 2".
- **Gear summary.** The tiles wrap their bait items and no longer overlap.
