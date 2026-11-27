# T1244 — Three data tests fail on main's own bundle

Status: OPEN — found in #312 round 5; not caused by it. They must be green before the 0.10.0 re-cut

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1244 | Three data tests fail on main's own bundle | OPEN — must be green before the 0.10.0 re-cut | — | wrap `test_part_runs`' span in `with_joins`, then see what else fails; measure the 474 floor and the 17 marks and re-pin each with its reason |

Source: PR #312 round 5 (T1172). The clean clone's data tests, and the same three on main's own pre-#312 bundle. These
data pins need a built bundle and do not run in CI.

## Work
1. `pipeline/tests/test_part_names_unnamed_lake.py:68` `test_province_wide_no_part_ends_at_a_lake_or_a_count`:
   `assert waters > 475` reads 474.
   - It reads 474 on main's own data too, and on the #312 exports, which are byte-identical in their parts.
   - Known in part: T1146 (9f0856154, 2026-11-25) folded the side parts of Koksilah, Manson, Shelagyote and Omineca
     into their main parts, so each now has one part.
   - Measure the whole drop against the export the floor was set on (r-6258ffded676, 2026-11-20, 52f647546) and
     re-pin with its reason.
2. `pipeline/tests/test_part_geo.py:509`: `UNLABELLED_MARKS = 22`, but main's bundle has 17. The same 17 are on #312
   rounds 2 and 5. Measure mark by mark which 5 went, and why, then re-pin.
3. `pipeline/tests/test_part_runs.py:239` `test_runs_cover_exactly_the_part_s_sections` recomputes the runs from
   `section_span` without the T1140 joins (`export.read.with_joins`, `pipeline/deliver/export/read.py:959`).
   - The first mismatch is Bedwell River (gnis:11575) p0, section 577, with its measured join at 3,730 m. It may not be
     the only one.
   - Fix: wrap the test's span in `with_joins` (the bundle's `section_join`), then see what else fails.

## Acceptance
All three pass on a clean-clone bundle of current main. Each new pin says what moved it, as the existing pins do.
Depends: none. Size: S. Owner: no.
