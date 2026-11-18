# T798 — Readings for the owner from the answers audit: Coal Creek above the bridge, the Puntledge above Comox Lake, Skeena "Section 4", DFO "non-retention"

Status: OPEN (owner: OWNER-TODO "2026-11-16 morning list" questions 31, 33, 35, 36).

Source: general audit 2026-11-16, answers checker A4, F1, F3, F5 (`handoff/PLAN/audit-2026-11-16/answers.md`).

1. **Coal Creek above the old MF&M Railway bridge** asks for no Classified Waters Licence (errs OPEN on licence).
   p.36: "ALL tributaries (EXCEPT Coal Creek downstream of old MF&M Railway Bridge …) are Class II waters when open";
   the Coal Creek row (p.35): "Part described is NOT a Classified Water". Ruling Li5 was applied to a row that
   un-classifies only its own part. Recommended: Class II above the bridge, with an Li5 exception for such rows.
2. **Puntledge above Comox Lake.** p.18: "Fly fishing only upstream of the BC Hydro diversion dam …, including
   tributaries" and "Exempt from July 15-August 31 summer closure"; the repo ends both at the lake outlet, so parts
   8 and 9 are closed Jul 15 to Aug 31 and not fly only. Recommended: carry both above the lake, or a caveat.
3. **Skeena "Section 4"** (p.53) has no printed upper end; the app classifies the river to its source.
4. **DFO "non-retention" / "No retention of coho" reads Closed** (20 rows; ruling T349). Keep, or show release only?
   If kept, RULINGS.md should say it covers dated rows.
5. Noted, no question: Region 6 "1 trout from streams" read as excluding char (rulings C2 / C4), so brook and lake
   trout show keepable from streams in winter; the Cowichan below Mile 66 (book closed Sep 1-Nov 15, DFO opens salmon
   catch and release).

## Owner, 2026-11-16 (question dialog; verbatim in `handoff/PLAN/OWNER-TODO.md`, "Answers by batch")
Question 31: "Class II above the bridge (Recommended)". Question 33, typed: "If the river continues then yes it goes above" (carry fly-fishing-only and the summer-closure exemption onto the Puntledge above Comox Lake). Question 35: Section 4 runs all the way up, main river only (T801). Question 36: "Keep "Closed"" (add to `pipeline/docs/RULINGS.md` that the ruling covers dated rows).

## Lane result (2026-11-16, branch `lane/book-rule-fixes-1116`)
1. **Coal Creek** (20b902f56): `licensing.A_WALKED_PAST_EXCEPTION_IS_NO_STOP`: a water a designation excepts by part
   and walks past (`tributary_excludes` + `walk_past`) is no stop for that designation's walk under Li5. RULINGS Li5a;
   POLICY_VERSION 45. Reach run: the two `own_row_not_classified` rows for Coal Creek (190 sections each, the Elk's two
   records, both unit "Elk River") are gone, nothing else in the licensing diagnostics changed; Line and Grave creeks
   still drop. Coal Creek above the bridge now asks for a Classified Waters Licence (Class II, Elk River) when open.
   Tests: 3 new in `test_licensing_first_classified.py` (fix, narrowness, mutation).
2. **Puntledge above Comox Lake** (b79a4513d): r1 (summer-closure exemption) holds on the whole river (+6 sections,
   parts 8 and 9: open Jul 15 to Aug 31 now); r6 (fly fishing only, including tributaries) binds upstream of the dam:
   10 -> 1,069 sections. Decided: "upstream of the diversion dam ..., including tributaries" read as printed, so the
   streams feeding Comox, Willemar and Forbush lakes (Cumberland, Perseverance, Comox, Rees, Capes, Beech creeks, about
   1,000 unnamed sections) become fly fishing only; the lakes themselves are not bound (Comox Lake has its own row;
   tributaries are streams). Binding the river's inter-lake stretches instead gives the same 1,069 (the walk seeds at
   each lake-end cut), so "only streams joining the river" cannot be drawn today. OWNER CHECK owed before it ships.
4. **Non-retention** written into RULINGS (D7, 865cbac37).

## Owner check, item 2 (2026-11-18 09:52)
Verbatim: "the whole watershed (...) but are we sure puntledge river actually has tribs included?" So the as-printed
reading stands (1,069 sections, every stream above the dam). Confirmed in the book: printed p.17 (PDF p.19), the
Puntledge row's last line ends "..., including tributaries"; the row name has no tributaries star. Closed. Follow-up
(not owner): r5 "No Fishing tributaries" binds downstream of the Comox Lake cut, not the dam, so any creek joining in the
3.5 km between the lake and the dam reads closed although r6 makes it fly fishing only; check whether such creeks exist.

## Item 2 corrected (2026-11-18 ~09:54-09:57, owner)
Verbatim: "no fishing tributaries shoudl be on the entire river??"; "why woudl you think it doesnt close the wholel
river????"; "look at entry it is clearly close all tributaries" (picture of the printed row); "the fly fishing on is if
a tributary gets opened on its own entry". Done: `puntledge_river.r5` ("No Fishing tributaries", tributaries only)
rebound from `downstream_of puntledge_river__comox_lake` to `whole`, as Qualicum r4 / Little Qualicum r2 read. Reach
run (scratch punt-r5): exactly 1 of 3,404 rules changed, r5 +1,053 sections (1,389 now): every side stream of the river,
Comox Lake's feeders included, is closed; r6's fly fishing only stays on the river above the dam. Own entries on
those side streams: Browns River and Cruickshank River ("No Fishing") and Strathcona Park waters (boats only), so no
side stream is opened by its own entry today (RULINGS T4: an inherited closure still closes an own-row tributary; only
a lift opens it). `test_watershed_parts` now pins Cruickshank River INSIDE r5 (it pinned it outside). The follow-up
above (creeks between the lake and the dam) is moot.
