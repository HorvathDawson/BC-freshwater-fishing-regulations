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
