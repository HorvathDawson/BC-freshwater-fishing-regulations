# regs-v9 vs page v36 (regenerated, answers 2.5): parity

v9 now reads the regenerated page v36's data, including answers 2.5's `group` and `elsewhere`, the Vedder May rows (F13), and the Vedder as one part. Every fact was copied from the page on 2026-10-11 into `regs-v9/v36plus.js`.

Side-by-side screenshots are in `screens/parity-*.png`: v36's Today's card on the left, v9's Regulations tab on the right, both 390 px wide. Heights run from the water's name to the end of the card, and include v9's sticky header.

| State | v36 | v9 |
|---|---|---|
| Vedder, Oct 10 | 2101 px | 1630 px |
| Vedder, May 15 | 2178 px | 1729 px |
| Kootenay LWA, Sat Oct 10 | 1633 px | 1329 px |
| Kootenay LWA, Mon Oct 12 | 1633 px | 1329 px |
| Cowichan mouth part, Oct 10 | 605 px | 579 px |

v9 is shorter than v36 in every state.

## Corrections now supplied by the data, so removed from v9
- **The group total and its sentence.** The trout/char total ("4 a day"), its sub-caps, and the lifted kind ("hatchery rainbow trout 50 cm or less excepted") come from `group.says`. v9 shows the sentence compactly: "Up to 4 trout and char a day" in bold, then each sub-cap as a chip.
- **The kept-elsewhere line.** The line and its questions come from `elsewhere`. For example: "Keep 0 here, except hatchery rainbow trout 50 cm or less: up to 4 in all".
- **The Vedder May 15 rows.** Rainbow and cutthroat show as release until May 31, steelhead keep up to 2. The group sentence for that day comes from the data.
- **"Keep 1 of your 2"** (Dolly Varden, bull trout and lake trout). This is the data's wording again: the char count within the 2 from streams.
- **Under 16.** Both residency outcomes now use the data's words: `U16` for a B.C. resident, and the "other anglers" line for a non-resident.
- **The Vedder's second part.** It is gone from the data. The Vedder has one part, so it has no part strip.

## Corrections v9 still makes (`data-src="design"` + `data-cite`)
| Line | Cite | Why the data doesn't have it |
|---|---|---|
| Fly-only windows on the open-days strip, drawn with their own pattern: Vedder May 1–31 "fly fishing only, bait ban"; Cowichan upstream parts "fly fishing only" (Sep 1–Nov 15, or all year from CNR Bridge to Greendale) | p.22, p.15 | The data's strip has only open and closed days. The fly-only rule is in the gear answer for the day, but not in the year view. The owner asked to keep the look. |
| "(Mile 66)" after "CNR Trestle" in the Cowichan's split label | p.15 | The data names the stretch "CNR Trestle" only. The book and the anglers' questions call it the CNR Mile 66 Trestle. |

## Facts v36 shows that v9 does not
| v36 shows | v9 |
|---|---|
| ⓘ glossary tips: "daily quota", "Region 2", "stream", "How to tell" | Only "How to tell" (it opens the hatchery sheet). The glossary text is not in the mock's data. **Gap.** |
| "Source" chips (the possession line, and each Cowichan closure) | Not shown. On the Vedder they are reachable through "See every regulation line". **Gap.** |
| "What they look like · 6 fish" inside the group | "What they look like · 14 fish" as its own section, one picture per species (the owner's request). |
