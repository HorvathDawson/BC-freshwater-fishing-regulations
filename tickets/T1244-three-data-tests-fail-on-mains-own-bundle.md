# T1244 — Three data tests fail on main's own bundle

Status: PARTIAL — found in #312 round 5; not caused by it. They must be green before the 0.10.0 re-cut; left (Claude): re-pin items 1-2 on the re-cut's candidate

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1244 | Three data tests fail on main's own bundle | PARTIAL — item 3 fixed; items 1-2 measured; left (Claude): re-pin items 1-2 on the re-cut's candidate | b523cf97c | re-pin the multi-part floor and `UNLABELLED_MARKS` on the 0.10.0 re-cut's candidate (after T1199 / T1240 / T1242), each with the waters below plus whatever those change |

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

## Findings (lane H, 2026-11-27, measured on r-83588a004b03: the clean clone's side release of main, its bundle)
Item 3 (b523cf97c): the test lays `section_join` over the span with `export.read.with_joins`, as the export does. Without
the joins 15 parts on 15 waters differ (38 join rows): Bedwell p0, Mahood p1, Stirling p1, Old Kootenay River Channel
p1, Thelwood p0, Theodosia p1, Noons p4, Squamish p1, Robertson p1, Snehumption p1, Skeena p4, Halfway p2, MacMillan p2,
Seymour p3, Okanagan River Oxbows p1. With them `test_part_runs.py` passes whole (85); nothing else failed behind Bedwell.

Item 1, the floor (`waters > 475`, 474): measured on the shipped water files (`part` rows with a rule set, which give the
test's own count: 474 on r-83588a004b03, 482 on r-6258ffded676). 482 -> 474 is 8 waters LEAVING the several-parts set;
none entered:
- 482 -> 479, r-6258ffded676 -> r-fec3fab4c5aa: Harmer (gnis:15289), Shea (gnis:22080), Brownie (gnis:39245), T944
  d23bbc9b6 (a dead end flows where its water flows; the same three the marks pin names).
- 479 -> 478, r-0aec88ed3edb -> r-71d09ad82e81: Dove Creek (gnis:21542), T1117 19a8dc392 (the "ANDERSON" LAKE row now
  binds the unnamed Walbran lake, so Dove Creek above the Comox Anderson Lake loses the rules it wrongly carried: one part
  "through 1 lake").
- 478 -> 474, r-71d09ad82e81 -> r-c8dc7712a9c0: Koksilah (gnis:14076), Manson (gnis:7712), Shelagyote (gnis:8138),
  Omineca (gnis:8264), T1146 9f0856154 (A_SIDE_CHANNEL_GOES_WITH_ITS_WATER: the side part gains the walk's trib rules and
  joins the main part). 474 on every release since (r-c8dc7712a9c0 ... r-83588a004b03); #312 moved nothing here.

Item 2, `UNLABELLED_MARKS` (22 -> 17): measured on the bundles (`part_mark.label IS NULL`). The live bundle under
data/generated (r-71d09ad82e81, 478 several-part waters) has 22; the shipped marks (down, up, token) are identical from
r-fec3fab4c5aa through r-741815c6090d (the pin's release) to r-71d09ad82e81, so these are the pin's 22 (release-file proxy:
the release ships no label). The side bundle before #312 (session 5162 `t1172/pre`, 474) and r-83588a004b03 have 17, the
same 17 pairs. The 5 that went, all by T1146 9f0856154, each a side-channel part lacking the walk's trib rules that now
joins its main part:
- Forsyth Creek (gnis:11815) 0>1 (its p1 lacked elk_river_s_tributaries r1-r4; the water keeps its Connor Lake mark);
- Koksilah River (gnis:14076) 0>1 (lacked cowichan_river r2 trib);
- Old Kootenay River Channel (gnis:2123) its second pair 0>2 (p1 lacked kootenay_river_lower r4 trib; 3 parts -> 2; the
  water is not named in the T1146 commit, the mechanism is the same);
- Shelagyote River (gnis:8138) 0>1 (lacked babine_river r6/r7, skeena_river r1 trib);
- Omineca River (gnis:8264) 0>1 (lacked williston_lake r4 trib).
No mark entered. The 17: Burman, Bedwell, Mahood, Cottonwood (1>2), Stirling, Old Kootenay (0>1), Thelwood, Theodosia,
Rees, West Road (0>2, 3>4), Klinaklini (1>2), Comox Creek, MacMillan (1>2), Fraser (21>23), Campbell (6>7), Alsek.

Not re-pinned (planning chat 2026-11-27): the cut rule (T1199), the channel folds (T1240) and maybe loop removal (T1242)
change the parts before the re-cut; both pins move on the re-cut's candidate with this list as their reason. Scripts and
raw outputs: the lane H scratch (multipart_diff.py / marks_diff.py over the release water files).

## Acceptance
All three pass on a clean-clone bundle of current main. Each new pin says what moved it, as the existing pins do.
Depends: none. Size: S. Owner: no.
