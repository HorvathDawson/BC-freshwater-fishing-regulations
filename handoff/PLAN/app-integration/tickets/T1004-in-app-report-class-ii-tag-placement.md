# T1004 — In-app report 2026-11-21: the Class II tag sits far right of the water's name, "kind of hidden"

Status: OPEN — next release batch (owner 2026-11-19: small finds go into the next release).

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
