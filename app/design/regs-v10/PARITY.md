# regs-v10 vs page v36 (regenerated with answers 2.6): parity

**Source.** `regs-v10/v36snap.js` is a snapshot of the regenerated page v36. A script made it: it opens the page in headless Chrome and copies the page's own HTML fragments, row groups, rule, glossary and source sheets, and fish look-for notes. Fish pictures come from the app's manifest (`regs-v10/fish.js`). **No fact is hand-written.**

The only text v10 adds is UI copy: section names, "Change date", "View on map", "Fished elsewhere today?", "What these words mean", and the "pending data" markers. No v36 wording is edited. "1 line (rod)" will come from the pipeline (answers 2.6).

## Heights
Each comparison is in `screens/parity-*.png`. v36's Today's card is on the left. v10's "What you can keep" section is on the right, including v10's header.

| State | v36 card | v10 keep |
|---|---|---|
| Vedder, Oct 10 | 2358 px | 2152 px |
| Vedder, May 15 | 2413 px | 2188 px |
| Kootenay LWA, Sat | 1714 px | 1710 px |
| Kootenay LWA, Mon | 1714 px | 1710 px |
| Cowichan mouth part, Oct 10 | 605 px | 605 px* |

\* Cowichan is the same height in both. That height is v10's numbered part strip. In v36 the part choice is a separate "Which part?" select above the card (about 70 px), so it is not counted in v36's 605.

**Gear & licence.** v36 shows the six tiles plus one pane at a time. Opening every pane in turn totals 4067 px for the Vedder. v10 shows the same tiles with every topic inline on one page in 2997 px (Kootenay: 3865 vs 2851). The "where it comes from" folds start closed in v10.

## Facts v36 shows that v10 does not
| v36 | v10 |
|---|---|
| The "All gear sources" and "All licence sources" buttons | **Gap.** Both are hidden, because the snapshot did not capture those two sheets. Every gear and licence line still carries its own source chip (Province, Region…), and that chip opens the rule. |
| "What they look like · N fish" fold at the end of each group | Replaced by the species row under each group's header, one picture per species, as the owner asked. The look-for notes are v36's FISHART tips. |
| The axis selects inside the Licence pane (residency, age, guidance, status) | Replaced by a 16 or older / Under 16 switch. Under 16 shows each residency outcome stacked (B.C. resident; Canadian, other province; from outside Canada when it differs). Guidance and status are not offered. **Gap:** the guided and status variants are not in the snapshot. |
| Clicking the strip or a "See the rules from then" link to change day | The strip is display only (owner's point 3). "Opens again Nov 16. See the rules from then ›" shows a toast, because the snapshot holds only these days: Vedder Oct 10, May 15 and Jun 15; Kootenay Oct 10 and Oct 12; Cowichan Oct 10. |

Everything else on v36's card is in v10's "What you can keep":
- **ⓘ glossary tips.** The group's ⓘ tips are labelled links under "What these words mean"; inline ⓘ tips have 44 px targets.
- **Source chips.**
- **The kept-elsewhere Q&A.** It sits behind one "Fished elsewhere today? ›" row, as the review asked.
- **Every kind with its "How this was decided"**, in the kind's sheet.
- **Other fish**, each with its full row and decision in a sheet.
- **Anywhere in B.C. and possession.**

## Answers 2.6: now from the data (no pending markers left)
| Was pending | Now |
|---|---|
| Fly-only and bait-ban days on the strip | Drawn from each day's strip flags: closed (bit 1), release (2), fly-only (4), flies-only (8), bait ban (16). Each flag has its own pattern and legend entry: closed is hatched, fly-only is vertically striped, bait ban has an amber underline. |
| The group sentence | The data's `group.says` is the lead, for example "Up to 4 trout and char a day in all — no more than 2 of them from streams (all streams combined)". Each item in `group.items` becomes a list line. |
| Book page links | Each rule in "How this was decided" and each rule sheet shows the data's `applies` (and `applies_trib`) plus "Book p.N ↗". The link opens the official Synopsis PDF at `#page=<PDF page>`, using the URL cited in `pipeline/regs/extraction/extract_synopsis.py` (`SYNOPSIS_URL`). |
| "Mile 66" on the Cowichan | Comes from the data's part titles: "CNR Mile 66 Trestle" and "CNR Bridge (mile 70.2)". |

Checks: `grep 'data-src="design"\|pending' regs-v10.html` finds nothing.

## Still missing in the data
Nothing. The line count now comes from the data: "1 line (rod)", "1 line (rod) (2 alone in a boat)". The "Missing from the answers file" note is gone.
