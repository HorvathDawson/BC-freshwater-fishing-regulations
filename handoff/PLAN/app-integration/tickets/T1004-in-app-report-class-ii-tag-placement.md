# T1004 — In-app report 2026-11-21: the Class II tag sits far right of the water's name, "kind of hidden"

Status: IN REVIEW — owner: OWNER-TODO "Look (you): T1004 Class II line under the name" (built, code d27c09e24, PR open; after-shots in T1004-shots/built-*).

Source: `GET /reports` on the production feeds worker, read 2026-11-22 (report `c1f639871183168d`; T985 holds the
four before it, T979 the fifth). Android 0.8.4 (42), production, release `r-fec3fab4c5aa`; water · regulations,
Skeena River (`gnis:2936`) part 11 "From 1.5 km upstream of the Zymoetz River confluence up to the Chimdemash Creek
confluence". Verbatim: "Clas ii tag should be next to the name I think? On the right side it looks weird and it kind of
hidden".

## Work
1. Reproduce on that water and part (emulator, not the owner's phone).
2. Show the owner the tag beside the name vs where it is now (pictures in the same turn as the question); the owner
   picks. The same tag on every water that has a class, one place in the code.

Depends: none. Size: S. Owner: picks the look.

## Pictures (2026-11-22, lane L3, web 390 px, Skeena River, a Class II part)
Cause: `WaterTitle` gives the name `flex: 1`, so the chip (a child of the row) lands at the row's right edge beside the date
pill, a long way from a short name. One component (`ClassBadge` in `RegsHeader.tsx`), so one change moves it on every water.
- A (now): `T1004-shots/A-now-dark.png`, `A-now-light.png`.
- B (tag right after the name, the date pill alone at the right): `B-beside-dark.png`, `B-beside-light.png`. The diff that
  made B is `T1004-shots/option-B-patch.diff` (not applied; a long name shrinks first and the chip stays next to its end).
Recommendation: B. Question for the owner: A or B?

## Owner 2026-11-22 ~23:55 (round 5)
Verbatim: "What about right under the water body name in the white space there?"

## Built (2026-11-22, lane L17, d27c09e24)
The chip is gone from the name row. `ClassLine` (`RegsHeader.tsx`, the one place) draws core's `classed.line` under the name, in the white space left of the sun times
(beside them when the day's sun is drawn, alone on the desktop's side column and before the sun is read), above the place text, so it shows on every sub-tab:
"Class II water · Classified Waters Licence · Jul 1–Dec 31" (option A's words, `LIC.CLASS_LINE`; with no dates: "Class II water · Classified Waters Licence"). Accent in
period, secondary colour out of period; a press opens the classified-waters term. Pictures: `T1004-shots/built-before-light|dark.png` (live app) and `built-after-light|dark.png`
(a web export of this branch, staging data, Skeena Class II part). Tests: RegsHeader.test (6 cases, mutation-proved: the line moved back into the name row fails 5),
classBadge.test, RegsWide.test (order on phone/tablet/desktop). T802's card half is this line; its map half was declined (11-22 23:35).

Press area (review fix): the line is a full-width press 24 px high (`CLASS_LINE_H`, one number for the style's min-height and the declared box), NO hitSlop: above it are the date pill and sun times, below it the place text and the tab row, so any slop would overlap a neighbour (largest non-overlapping slop: 0). It is a recorded exception to 44 (`SHORT_PRESS` in a11yAudit): the audit fails a declared box that is not the style's real min-height (mutation: h 30 and h 44 both fail) and fails the exception if it ever reaches 44. The same term is reachable from the Licence tab's ⓘ.
