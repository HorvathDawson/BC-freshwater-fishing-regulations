# Round 20e (owner items 1-6; item 7 left for the next UI agent): progress / handoff

Regs v29 changes the UI only, in app/design/regs-v11.html. It makes no data or pipeline change and needs no snapshot or oracle
regeneration. Details are in app/design/regs-v11/UI-NOTES.md (the v29 section at the top). The reader gate is in
handoff/PLAN/r20e_comprehension.md.

Scratchpad: /private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/266b6a4b-cf8d-417b-b326-25e034ead4e0/scratchpad/r20e/
- check-before.txt: ONLY=20e on regs v28 fails 7 of 7 new checks.
- check-final.txt: 91 / 91.
- before/: the v28 gear and licence tabs.
- frames-before/, frames-after/, rows-before/, rows-after/
- gate-before/, gate-after/
- ways/: the item-4 inline vs columns comparison.

## Done
- [x] 1. Four tabs: What you can keep · Gear · Methods · Licence.
  - `#gear` holds the gear lines and Bait.
  - `#methods` holds Ways to fish, Boats and the Reference fold.
  - `#licence` holds the licence answer.
  - `sec=methods` works in the hash. buildGear() now returns {gear, methods, licence}.
- [x] 2. A dated change is a tinted note (`.gnote`, `data-change`) inside the line it changes: "May 1–31: fly fishing only ⓘ Rule book
  p.22 ↗" on Hooks. Bait's bait-ban runs use the same note.
- [x] 3. Every gear and methods item ends with its own source line (`.isrc`, `data-keys`): "Province · Region 2 ›" opens that item's
  Sources sheet, then "Rule book p.N ↗". The licence's source line uses the same style.
- [x] 4. Ways to fish now uses Bait's two columns (`yesNo`). Why: the ✕ run-on line becomes a list, the conditions sit under each name,
  and the component is the same one Bait uses.
- [x] 5. The licence has two segmented radio groups (age, residency; page v36's own option words). The chosen pair's answer is the only
  one shown. There are no dropdowns. The guiding fold is drawn open.
- [x] 6. The row's streams tag ("All Region 2 streams") moved from the head band (the region total) to a kicker on the share. A "This
  water" tag over a region total is dropped.
- [x] Checks:
  - New: r20e-four-tabs, -dated-on-its-line, -sources-per-item, -ways-columns, -licence-selectors, -region-vs-stream.
  - Retired: r20b-three-tabs, licence-others-in-dropdowns, u16-by-residency, ur3-licence-not-resident.
  - Retargeted: one-sources-line, gear-sentence, gear-lines, gear-no-icons, always-as-reference, bait-ways-compact, dated-rows,
    licence-u16-you-need.
  - check.mjs ends at 91/91.
- [x] Tools: tall.mjs (sec=methods, r20e-* shots; the two "-open" licence shots now use `res=`), shoot.mjs 154–157, audit.mjs (Methods tab).
- [x] Every screen and tall shot re-shot. Before/after pairs are under app/design/regs-v11/screens/r20e/.
- [x] Reader gate: BEFORE 34/34, AFTER 34/34, no new wrong answer.

## For the next agent (Fable: item 7, the tree's left-bar headers, the species thumbnails, visual polish)
- These were not touched: `.tier .tl` (its 3 px bar), `.tnode[data-tnode="size"]` (the band), the `.kpics` thumbnails.
- The new share kicker `.tier .tkick` (also `.scope`) borrows the tier line's bar and top rule. If you restyle the tier header, restyle
  the kicker with it. It is drawn only on a share under a region's total (`data-scope-tag`; checked by r20e-region-vs-stream).

## Logged for the owner
- The share's data words say "from this stream", but the 2 is the Region 2 streams' combined share. With the "ALL REGION 2 STREAMS"
  kicker above it, the AFTER reader read it as both (score kept, confidence 5 → 4). Fixing this means a data change, for example
  "Only 2 of the 4 may be from streams". Not done this round.
- On a fly-only day the line "No float, sinker or attracting device" has no cite in `summary.parts`, so it has no source line.
- Some source lines read "This water's ›". That is the page v36 chip word, taken as is.
- The Methods tab costs up to 59 px more with columns (Vedder May 15: five ✕ ways stacked). It still fits within one screenful.
- On a closed day the Licence tab now shows the licence answer. Before, the tab was dead.
