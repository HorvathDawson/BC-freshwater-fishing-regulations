# regs-v9 vs page v36: parity

Each comparison is a side-by-side screenshot in `screens/parity-*.png`, with page v36's Today's card on the left and v9's Regulations tab on the right. Both are rendered at 390 px wide and measured from the water's name to the end of the card. v9's figure includes its sticky header and part strip.

| State | v36 height | v9 height | v9 vs v36 |
|---|---|---|---|
| Vedder mouth part, Oct 10 | 1995 px | 1454 px | −27% |
| Vedder mouth part, May 15 | 2132 px | 1610 px | −24% |
| Kootenay LWA, Sat Oct 10 | 1610 px | 1297 px | −19% |
| Kootenay LWA, Mon Oct 12 | 1610 px | 1297 px | −19% |
| Cowichan mouth part, Oct 10 | 605 px | 579 px | −4% |

**v9 is never taller than v36 for the same content.** The fold-out "What they look like" is collapsed in both pages. Opened, v9 lists every species on the card with its own picture (`parity-vedder-oct10-look.png`, 2956 px). v36's fold, opened, holds the same kind of list.

Gear & licence was not measured as a height. v36 opens the Licence pane under its tiles (1538 px for the Vedder). v9 shows the same tiles with nothing open, so the tab fits on one screen (624 px), and each tile's detail is one tap away as a sheet.

## Facts v36 shows that v9 does not

| v36 shows | v9 | Why |
|---|---|---|
| "2 a day in total, all kinds together" and the Q&A "Kept 2 on another stream? Keep 0 here / Kept some at a lake? …" (Vedder mouth part, Oct 10 and May 15) | Replaced by "Up to 4 trout and char a day, all kinds together. At most 2 from streams, except hatchery rainbow 50 cm or less." and the numeral 4 (`data-src="design" data-cite="p.21–22"`) | The answer key (Q1, Q3) shows v36 is wrong here: its template ignores the r9 lift. This removal is deliberate. Cowichan and Kootenay keep v36's Q&A unchanged. |
| "Keep 1 of your 2" (Vedder char) | "Keep 1 a day" (`data-src="design"`, p.21) | Same reason: the 2 is the wrong total. |
| ⓘ glossary tips on "Region 2", "stream" and "How to tell" | Not reproduced, except "How to tell", which opens the hatchery sheet | **Gap.** The glossary sheets are not in the mock's data. They should sit behind the same words in the app. |
| "Source" chip on the possession line, and per-closure "Source" chips (Cowichan) | Not shown | **Gap.** The source text is one tap away through "See every regulation line" on Vedder only. The app should keep v36's chips. |
| The part's full legal text in the header ("From the mouth up to the Vedder Crossing Bridge") | A short label in the part strip; the full text is in the part sheet (one tap) | Deliberate, to keep the header compact. |
| "What they look like · 6 fish" inside the trout group | "What they look like · 14 fish" as its own section after Other fish, one picture per species | Owner's request. It also covers the other fish. |

## Things v9 shows that v36 does not

- Fly-only windows on the open-days strip, drawn with their own pattern. For example, the Vedder strip marks May 1–31 as "fly fishing only, bait ban" (`data-src="design"`, p.22). v36's strip has no fly-only window.
- On the Cowichan, a labelled split between the two halves of the river: "This river is split at the CNR Mile 66 Trestle: below it closed, opens Nov 16 · above it open, fly fishing only".
- Under 16: both residency outcomes in the Licence sheet (`data-src="design"`, p.6).
- On fly-only days, the Line & hooks sheet drops the lure and line-weight rows. v36 still lists "Lures: any lure or fly".

## Round-3 items
- The next-open date counts fly-only days as open.
- No lure or weight rows on fly-only days.
- The split water hero is labelled.
