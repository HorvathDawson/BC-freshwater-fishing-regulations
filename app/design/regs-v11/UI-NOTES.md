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
