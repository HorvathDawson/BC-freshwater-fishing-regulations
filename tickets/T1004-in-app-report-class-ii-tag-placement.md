# T1004 — In-app report 2026-11-21: the Class II tag sits far right of the water's name, "kind of hidden"

Status: IN REVIEW — owner: see ## Owner (built, code d27c09e24, PR open; after-shots in T1004-shots/built-*).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1004 | In-app report: the Class II tag sits far right of the water's name, "kind of hidden" (Skeena part 11) | IN REVIEW | | owner 11-22: "right under the water body name"; built d27c09e24 (PR feat/T1004-class-ii-line); look: T1004-shots/built-*; T802 closes with it |

Source: `GET /reports` on the production feeds worker, read 2026-11-22 (report `c1f639871183168d`; T985 holds the
four before it, T979 the fifth). Android 0.8.4 (42), production, release `r-fec3fab4c5aa`; water · regulations,
Skeena River (`gnis:2936`) part 11 "From 1.5 km upstream of the Zymoetz River confluence up to the Chimdemash Creek
confluence". Verbatim: "Clas ii tag should be next to the name I think? On the right side it looks weird and it kind of
hidden".
Owner 2026-11-22 23:20 (round 1, asked A now / B beside the name), verbatim: "I don't like either could we think of a
better spot?" Round 2 offered A line under the place text / B badge on the Licence tab / C Licence head only / D strip
under the tabs (`T1004-shots/option-<X>-<name>-light|dark.png`, mock-ups). Owner 2026-11-22 ~23:55 (round 5), verbatim:
"What about right under the water body name in the white space there?" Reading: the line goes in the white space under
the name (the name row, left of the date and sun times, above the place description), on every sub-tab; words as option A.

## What
Cause: `WaterTitle` gave the name `flex: 1`, so the chip landed at the row's right edge beside the date pill.

Built (lane L17, d27c09e24, PR feat/T1004-class-ii-line): the chip is gone from the name row. `ClassLine` (`RegsHeader.tsx`,
the one place) draws core's `classed.line` under the name, in the white space left of the sun times (beside them when the
day's sun is drawn, alone on the desktop's side column and before the sun is read), above the place text, on every
sub-tab: "Class II water · Classified Waters Licence · Jul 1–Dec 31" (`LIC.CLASS_LINE`; with no dates: "Class II water ·
Classified Waters Licence"). Accent in period, secondary colour out of period; a press opens the classified-waters term.
Tests: RegsHeader.test (6 cases, mutation-proved), classBadge.test, RegsWide.test. T802's card half is this line; its map
half was declined (11-22 23:35). Press area: a full-width press 24 px high (`CLASS_LINE_H`), NO hitSlop (any slop would
overlap a neighbour); a recorded exception to 44 (`SHORT_PRESS` in a11yAudit), which fails a declared box that is not the
style's real min-height and fails the exception if it ever reaches 44. The same term is reachable from the Licence tab's ⓘ.
Pictures: `T1004-shots/built-before-light|dark.png` (live app), `built-after-light|dark.png` (a web export of this branch).

## Work (left)
- The owner's look (below); then merge the PR; T802 closes with it.

Depends: none. Size: S. Owner: picks the look.

## Owner

- [ ] 2026-11-22 — Look at the Class II line under the water's name (`docs/app-integration/T1004-shots/built-before-*` / `built-after-*`, light and dark, every sub-tab): A now / B beside the name (B recommended).
