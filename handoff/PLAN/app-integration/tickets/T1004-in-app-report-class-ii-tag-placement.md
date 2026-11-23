# T1004 — In-app report 2026-11-21: the Class II tag sits far right of the water's name, "kind of hidden"

Status: BLOCKED (owner) — pictures made 2026-11-22 (T1004-shots/); the owner picks A or B, nothing changed in the app.

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
