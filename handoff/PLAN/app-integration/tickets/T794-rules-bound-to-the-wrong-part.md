# T794 — Rules bound to the wrong part: the Peace closure below W.A.C. Bennett Dam, Williston Lake's 30 cm rainbow minimum, "No powered boats" on Bella Coola side channels

Status: OPEN (data / rule binding; curated region files 7 and 5).

Source: general audit 2026-11-16, answers checker A3, A7, B5 (`handoff/PLAN/audit-2026-11-16/answers.md`).

1. **Peace River part 9 reads open (errs OPEN).** The Dinosaur Lake row (p.65): "No Fishing from W.A.C. Bennett Dam to
   100 m south of Gething Creek and between the anti-vortex dyke and Peace Canyon Dam". `dinosaur_lake.r1` is held on
   the lake only, as an undrawn part; the river part "From Dinosaur Lake up to the Williston Lake — Zone B outlet" is
   open with no caveat. That part 9 lies inside the closure is inferred from its ends: check on the map, then bind
   the closure to the river stretch (a cut 100 m south of Gething Creek if none exists).
2. **Williston Lake "No rainbow trout under 30 cm" (p.61)** binds the Zone A piece only (`williston_lake.r1` matches
   `wbk:-18`); Nation Arm and Davis Bay, parts of the same lake, read "any size" (errs OPEN). Owner question 32.
3. **Bella Coola part 0** (side channels joining at km 55 to 59.1) carries the Atnarko's "No powered boats … from Goat
   Creek to the confluence with Talchako River" (the Talchako joins at km 62.6). Errs closed; geometry not checked.

## Owner, 2026-11-16 (question dialog; verbatim in `handoff/PLAN/OWNER-TODO.md`, "Answers by batch")
Go-ahead card: "Go on all (Recommended)". Question 32 (Williston Lake's 30 cm minimum): "All three parts (Recommended)".

## Lane result (2026-11-16, branch `lane/book-rule-fixes-1116`)
1. **Peace below W.A.C. Bennett Dam** (16f92dfcc). Atlas, read: Peace part 9 is the 628 m stretch from the Williston
   Lake Zone B outlet (km 1706.74, the dam) to Dinosaur Lake's inlet (km 1706.11); Gething Creek's mouth (56.0039 N,
   122.2322 W) is at Dinosaur Lake's west tip, about 1.1 km south of part 9's lower end. So part 9 lies wholly inside
   "from W.A.C. Bennett Dam to 100 m south of Gething Creek", bounded by existing lake-edge cuts: NO new cut. New rule
   `dinosaur_lake.r1b` (between `peace_river__dinosaur_lake` (upper) and `peace_river__williston_lake_zone_b`, no
   tributary walk). Part 9 now reads closed all year. The lake part stays r1's undrawn note. The row's entry-level
   `extents: [whole]` was dropped (it clipped r1b to the lake: `empty_after_scope`).
2. **Williston 30 cm rainbow minimum** (56f4e4e91): `williston_lake.r1` binds wbk:-18, -16, -17; Nation Arm and Davis
   Bay now read "rainbow trout 30 cm or longer" every day. Entry-level `extents: [whole]` dropped for the same reason.
3. **Bella Coola part 0** (eae8241a3): r12 is now downstream of Goat Creek on the Atnarko alone (the Atnarko ends at
   the Talchako confluence). Bella Coola part 0 (7 side-channel pieces) and 43 tributary sections walked from them lose
   "No powered boats"; the Atnarko stretch keeps it.
- Proof: second reach run: williston r1 +2 lakes, dinosaur r1b bound (1 section, 359572348:1706109), r12 -50; no
  other rule moved besides this lane's own. Main chat: reach run + bundle + release.
