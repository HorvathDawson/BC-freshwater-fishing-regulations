# T1004 — In-app report 2026-11-21: the Class II tag sits far right of the water's name, "kind of hidden"

Status: BLOCKED (owner) — round 2 pictures made 2026-11-22 (lane L17, T1004-shots/option-*.png): the owner rejected both A and B; he picks one of four new places, nothing changed in the app.

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

## Owner 2026-11-22 23:20 (question round 1)
Asked A (now, far right) or B (beside the name); verbatim: "I don't like either could we think of a better spot?"

## Pictures, round 2 (2026-11-22, lane L17)
Live app (canifishthis.ca, web 390 px, Skeena River, the Class II part 1.5 km upstream of the Zymoetz confluence up to Chimdemash Creek), light + dark,
with the tag moved by editing the page: MOCK-UPS of the live app, not renders of a build. `T1004-shots/option-<X>-<name>-light|dark.png`:
- A `option-A-line-under-name`: a line under the part's place text, in the accent: "Class II water · Classified Waters Licence · Jul 1-Dec 31". Why: it is
  read with the name and the part, says what the class means in words, and takes no width from the name or the date pill. Costs one line of height.
- B `option-B-badge-on-licence-tab`: a small "CLASS II" badge on the Licence sub-tab label. Why: the class matters for the licence, and the Licence tab
  already has the dates ("Classified (Class II) Jul 1-Dec 31"). Weakness: hidden until the reader looks at the tabs; it is the "kind of hidden" complaint again.
- C `option-C-licence-head-only`: the chip beside the Licence section's own heading, nothing in the header. Why: one place, where the licence is explained.
  Weakness: a reader on "What you can keep" never sees the water is classified.
- D `option-D-strip-under-tabs`: a full-width tinted strip under the tabs (like the access banner): "Class II water · Classified Waters Licence needed
  Jul 1-Dec 31 ›" (the press would open the glossary term). Why: impossible to miss. Weakness: it looks like the access warning stacked on it, and takes a row.
Recommendation: A. Question for the owner: A, B, C or D (or A with D's weight)?

## Owner 2026-11-22 ~23:55 (round 5)
Verbatim: "What about right under the water body name in the white space there?" Reading: the line goes in the white space under the name (the name row, left of the date and sun times, above the place description), on every sub-tab; words as option A.
