# regs-v10 vs page v36 (regenerated): parity

**Source.** `regs-v10/v36snap.js` is a snapshot of the regenerated page v36. A script made it: it opens the page in headless Chrome and copies the page's own HTML fragments, row groups, rule, glossary and source sheets, and fish look-for notes. Fish pictures come from the app's manifest (`regs-v10/fish.js`). **No fact is hand-written.**

The only text v10 adds is UI copy and two labels:
- **UI copy:** section names, "Change date", "View on map", the "pending data" markers.
- **Two label clarifications the owner asked for:** "1 line" → "1 line (one rod)", and "Lines" → "Lines (rods)".

## Heights
Each comparison is in `screens/parity-*.png`. v36's Today's card is on the left. v10's "What you can keep" section is on the right, including v10's header.

| State | v36 card | v10 keep |
|---|---|---|
| Vedder, Oct 10 | 2101 px | 1877 px |
| Vedder, May 15 | 2178 px | 1948 px |
| Kootenay LWA, Sat | 1633 px | 1511 px |
| Kootenay LWA, Mon | 1633 px | 1532 px |
| Cowichan mouth part, Oct 10 | 605 px | 644 px* |

\* Cowichan is the one state where v10 is taller. The 39 px is v10's numbered part strip. In v36 the part choice is a separate "Which part?" select above the card (about 70 px), so it is not in v36's 605.

**Gear & licence.** v36 shows the six tiles plus one pane at a time. Opening every pane in turn totals 4067 px for the Vedder. v10 shows the same tiles with every topic inline on one page in 3052 px (Kootenay: 3865 vs 2899). The "where it comes from" folds start closed in v10.

## Facts v36 shows that v10 does not
| v36 | v10 |
|---|---|
| The "All gear sources" and "All licence sources" buttons | **Gap.** Hidden, because the snapshot did not capture those two sheets. Every gear and licence line still carries its own source chip (Province, Region…), and that chip opens the rule. |
| "What they look like · N fish" fold at the end of each group | Replaced by the species row under each group's header, one picture per species, as the owner asked. The look-for notes are v36's FISHART tips. |
| The axis selects inside the Licence pane (residency, age, guidance, status) | Replaced by a 16 or older / Under 16 switch. Under 16 shows each residency outcome stacked (B.C. resident; Canadian, other province; from outside Canada when it differs). Guidance and status are not offered. **Gap:** the guided and status variants are not in the snapshot. |
| Clicking the strip or a "See the rules from then" link to change day | The strip is display only (owner's point 3). "Opens again Nov 16. See the rules from then ›" shows a toast, because the snapshot holds only these days: Vedder Oct 10, May 15 and Jun 15; Kootenay Oct 10 and Oct 12; Cowichan Oct 10. |

Everything else on v36's card is in v10's "What you can keep": the ⓘ glossary tips, Source chips, every kind with its "How this was decided" (in the kind's sheet), other fish (each with its full row and decision in a sheet), Anywhere in B.C., and possession.

## Pending data (answers 2.6): shown as visible placeholders, never filled by hand
| Placeholder | Where |
|---|---|
| "Fly-only / bait-ban days: pending data" | On every open-days strip legend |
| "Book page: pending data. A link to this rule's page in the official Synopsis PDF arrives with answers 2.6." | In every rule sheet and source sheet |
| The clearer group sentence ("Up to 4 a day in all — no more than 2 from streams") | Until then v10 shows the data's current `group.says`, split at its semicolons into a lead line and a short list |
| "Mile 66" on the Cowichan part names | v10 uses the data's part titles as they are ("From the mouth up to the CNR Trestle") |
