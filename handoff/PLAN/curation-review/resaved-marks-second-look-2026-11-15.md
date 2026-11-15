# Re-saved DFO marks: second look (2026-11-15)

Read-only. Nothing in marks, places, cuts or code was changed. This is an independent check of
`resaved-marks-audit-2026-11-15.md`, answering the owner's "please audit these and see if you see any issue".
These are candidates for the owner. What a wording means is his call.

**How each claim was checked**
- **run**: a script over the files, which I ran.
- **read**: read in code or data.
- **inferred**: my reasoning from what I read, not proven.

River km is the FWA route measure from `data/generated/atlas/full/splits.resolved.json` (live atlas, read). The
scripts are in the session scratchpad (`resaved.py`).

## 1. Recount (run, on marks.jsonl at 2,669 lines)

- DFO places with a mark: **228**. The earlier report counted 213 at line 2,652.
- Places saved 2 or more times: **33**. Places whose saves differ: **31**. This agrees with the earlier report.
- The 2 pure repeats are `5b:atnarko-river:whole` (2585, 2587) and `6:nakina-river:whole` (2513, 2514).
- **None of the 33 was saved again after line 2652.** Lines 2653–2669 are all first saves on 17 other places:
  - Bulkley ×2, Zymagotitz ×2, Sproat ×2, Kitimat ×4, Nanaimo ×2
  - Illiance, Khyex, Pinkut, Shegunia, Wilauks (all unsure)

  One of these needs a look. `1:sproat-river:sproat-river-from-sproat-lake-to-f` (2658) is a dormant older wording of the
  Sproat stretch. It names the same two cuts as the re-saved place below.
- Cuts in `cuts.json` made from review marks: **42, none orphaned** (run). This agrees with the earlier report.
- B1, B2 and B3 are as the earlier report describes (read). There are two corrections, under B1 and B2 below.

## 2. The 31 places

| # | place | saves (in order) | in force now | verdict | reason |
|---|---|---|---|---|---|
| 1 | 1:little-qualicum-river:all-open-portions-of-the-little-qu | 2471 whole → 2616 other_places | other_places | looks right | "all open portions" read as the union of the placed places, as the owner chose |
| 2 | 1:little-qualicum-river:from-the-falls-in-little-qualicum | 2440 unsure (hand point) → 2467 wrong (park cut) → 2617 verified | between the park's downstream boundary (km 12.57) and the hatchery fence (km 2.92) | cannot tell | DFO says "from the falls". We cut at the park line the owner asked for. The curated `upper_waterfall` cut sits at km 13.26. Where the falls DFO means sit relative to the park line is not confirmed |
| 3 | 1:qualicum-river:all-open-portions-of-the-qualicum | 2651 → 2652 (items dropped) | other_places | looks right | same reading as #1 |
| 4 | 1:quatse-river:100m-upstream-and-downstream-of-th | 2452 weir point → 2475 between ±100 m | km 1.335–1.535 around the weir (km 1.435) | looks right | the 200 m window around the owner's own weir point |
| 5 | 1:sproat-river:from-sproat-lake-to-approx-300m-be | 2453 unsure → 2468 wrong (move the cut +50 m) | Sproat Lake edge → "signs 300 m below Hwy 4" (km 1.92) | looks wrong (owner's own words) | the cut still has its 300 m offset. The +50 m to "the bottom of the pool" is not done. The same cut also serves the new dormant place from line 2658 |
| 6 | 1:stamp-river:except-in-those-areas-and-times-li | 2454 … 2466 (rest, whole, rest) | rest | looks right | "except in those areas listed below" is the rest. The dormant-sibling policy is open (earlier report F) |
| 7 | 1:stamp-river:from-approx-200-m-upstream-of-the | 2455 unsure → 2469 wrong (names, Deer Creek check) | between km 5.99 and km 6.69 (fishway point −500 m / +200 m) | cannot tell | the arithmetic is right. The Deer Creek cross-check the owner asked for is **not done**. Deer Creek's code puts its mouth at 10.79 % of the coded line, but that line's full length is not in repo data, so this is not confirmed |
| 8 | 2:ashlu-creek:whole | 1189 auto → 2510 | whole | looks right | |
| 9 | 2:cheakamus-river:whole | 1191 auto → 2536 | whole | looks right | |
| 10 | 2:chilliwack-vedder-river-including:…boun (dormant) | 2480 → 2501 (+Sumas piece) | Chilliwack/Vedder below Slesse +100 m; Sumas below the Barrowtown pump station (km 5.58) | looks right | |
| 11 | …boun~2 (dormant) | 2481 → 2502 | same as #10 | looks right | |
| 12 | …boun~3 (live) | 2479 → 2500 | same as #10 | looks right | "signs near the confluence with the Fraser" is read as the mouth (inferred, small) |
| 13 | …upstream-from-a-line-between-2-fis~2 (dormant) | 2482 → 2503 | same as #10, **downstream_of** | cannot tell | the words begin "**upstream** from a line …". This wording was on the page 2023-02 and 2024-09 (history, run). It is probably a DFO slip, but it is the owner's reading |
| 14 | 2:coquitlam-river:whole | 1192 → 2485 (item named) | whole | looks right | |
| 15 | 2:fraser-river:from-the-downstream-side-of-the-cp (dormant) | 2549/2550/2553/2554 → 2561 cleared → 2571 unsure (Mission→Hope) | **nothing**: extents [] | cannot tell | the last save waits on the Hope cut. That reading (CPR Mission → Hwy 1 Hope) is right by the words |
| 16 | …from-the-downstream-side-of-the-cp~2 (dormant) | 2551 → 2562 cleared → 2572 unsure (Mission→Alexandra) | nothing | cannot tell | waits on the Alexandra cut. Which bridge DFO means is the owner's call (dossier) |
| 17 | 2:fraser-river:mainstem-waters-downstream-of-the (**live**) | 2546 → 2555 → 2563 cleared → 2568 unsure (Mission→Hope, no tributaries) | **nothing**: own extents [], and its link points to #15, which is also empty | looks wrong for now | the live chum/coho place is undrawn until the Hope cut lands. It is a card caveat only, not paint (`salmon.py:576-578`). The last save reads the words right |
| 18 | …mainstem-waters-from-the-downstrea (dormant) | 2552 → 2564 cleared → 2574 unsure (Mission→Hope) | nothing | cannot tell | waits on the Hope cut. #17 links to this place |
| 19 | …mainstem-waters-upstream-of-the-cp (dormant) | 2548/2556 → 2565 cleared → 2575 verified | CPR Mission (km 76.5) → Spuzzum (km 197.0), no tributaries, `gnis:39325` only | looks right, with a gap | drops the Herrling, Seabird N and Jeperson channels and Maria Slough (fraser-channels §1). The Alexandra question is in issue 5 |
| 20 | …mainstem-waters-upstream-of-the-cp~2 (live) | 2547/2557/2559/2560 → 2566 cleared → 2569 verified | Mission → Spuzzum, not item-restricted; tributaries **null** | looks right, with a gap | drops the 3 named channels (20.2 km). Tributaries are null rather than `false`; that resolves to false through the water default, so the result is the same |
| 21 | 2:fraser-river:upstream-from-the-highway-1-bridge (dormant) | 2558 → 2567 cleared → 2576 unsure (Hope→Spuzzum) | nothing | cannot tell | waits on the Hope cut. Ending at Spuzzum (the region line) is the owner's reading |
| 22 | 2:khartoum-lake:whole | 1195 → 2487 (note only) | whole | looks right | |
| 23 | 3:bridge-river:whole | 1202 → 2527 | whole | looks right | |
| 24 | 3:fraser-river:mainstem-waters-of-the-fraser-rive (dormant) | 2577 verified → 2578 wrong | between `area:3` and Spuzzum = **km 197.0 → 452.7** | looks right to me (see B2) | Region 3's mainstem by its words. The "wrong" may come from a misleading label |
| 25 | 3:thompson-river:from-kamloops-lake-outlet-downstre (dormant) | 2520 link → 2521 wrong | rides ~2: Kamloops Lake → Goldpan signs (km 26.05) | looks wrong | the 3 no-fishing areas are not cut out (see B1) |
| 26 | 5b:atnarko-river:downstream-of-the-fishing-boundary | 2582 → 2584 (tributaries true) | nothing yet (waits); tributaries true | cannot tell | the sign is snapped from the lodge's OSM point: ~260 m off, at km 2.34. The hunt note says the lodge is "~1 km above the Talchako". The position is not confirmed. Tributaries true matches the water default, but the words are silent on them |
| 27 | 5b:wannock-river:whole | 1208 → 2528 | whole | looks right | |
| 28 | 6:braverman-river:whole | 1212 → 2529 → 2530 | whole | looks right | |
| 29 | 6:morice-river:from-fishing-boundary-signs-locate (dormant) | 2595 verified → 2596 wrong (species words) | between km 54.53 (Lamprey) and km 75.07 (Gosnell −100 m); tributaries true | looks right | history (run): the "chinook only" sentence was last on the page 2022-05. The row is retired, which the owner said is okay. The 100 m point is 80 m below the Thautil mouth, and Gosnell joins the Thautil 222 m up (cuts, read). Tributaries true is the water default; the words are silent |
| 30 | 6:rainy-creek:whole | 1218 → 2497 (note only) | whole | looks right | |
| 31 | 6:sheldens-creek:upstream-of-white-triangular-fishi | 2470 wrong (new cut) → 2516 verified → 2517 wrong | upstream of km 3.40 on a **3.69 km** line: **the top ~290 m only** | looks wrong | see B3 |

## 3. B1, B2, B3

### B1: Thompson (agrees with the earlier report, with one sharper point)

- **Same stretch? Yes, by DFO's words (history, run).**
  - The 2025-06 copy reads: "From Kamloops Lake outlet downstream to the fishing boundary signs just downstream of Gold
    Pan Provincial Park, except: Deadman … Juniper … Ashcroft …".
  - The 2026-11-09 copy has the same two ends, then "Please note that no fishing is allowed in the following areas,
    year-round:", followed by the same three areas, word for word.
- The link is right for the stretch. The real defect is on the **live** place ~2: its extent has no carve-outs, so the
  three year-round no-fishing areas carry ~2's rules. Today those rules are chinook "to be determined", pink and coho no
  fishing.
- If chinook opens, the map would show chinook open inside Deadman, Juniper and Ashcroft (inferred from the extent; not
  run through a release).
- The owner's "wrong" (2521) sits on the **dormant** place. ~2's own last mark is 2518 unsure. So nothing on the live
  place records the problem.

### B2: Fraser Region 3 (I partly disagree)

- In force: `area:3` resolves on the Fraser main line at **km 452.7**, where the atlas labels it "Region 3 – Region 5
  boundary" (`splits.resolved.json`, run). The other end is Spuzzum at km 197.0. That is exactly the Region 3 mainstem
  the suggester names (T668).
- The owner's checklist row said "ours: from Region 3 – Region 8 boundary". `area:3` has 190 crossings with 4 labels
  (129 R3–R5, **28 R3–R8**, 27 R3, 6 R2–R3; run). The checklist probably showed another water's label.
  - This is inferred. I did not trace the checklist's label code.
  - If so, the "wrong" may be about the label, not the extent.
- The upper end includes the both-regions stretch km 418.4–452.7, per the dossier.
- **"All channels, like Seabird Island"**: Seabird, Herrling and Jeperson are at km 113–137, which is **Region 2**, not
  Region 3 (fraser-channels §1). Above Spuzzum, every Fraser channel is already in `gnis:39325`: 183 sections, read
  from that doc and not re-run.
  - So the request applies to the Region 2 places (#19, #20).
  - What it needs there: add `blk:355992187`, `blk:355991777` and `blk:355994562` to the water record `2:fraser-river`.
  - Item-restricted saves also need to take those items in, or be re-marked without the restriction.
  - The owner decides Maria Slough first.
- **Can a "wrong" last save ship the older extent? Yes** (read).
  1. `marks.py:839-845`: `latest()` keeps the last save. A wrong save with no fix, split, link or tidal call proposes
     nothing (`proposals.py:655-658`, `954`, `1092`).
  2. So `apply --check` has nothing unlanded, and the release preflight `check_marks` passes (`release.py:732-741`).
  3. Marks are not a build input (`pipeline/build.py:51-53`, `132`).
  4. The reach run draws `binding.extents` as the file holds them (`pipeline/atlas/reach/dfo_salmon.py:100-111`).
  5. Dormant places stay in their chains (`pipeline/deliver/salmon.py:715-718`). The live feed paints them the day DFO
     lists them again, without a rebuild.

  No gate looks at a wrong verdict.

### B3: Sheldens (read)

- The stream itself is an **assumption**. `blk:360846083` is an unnamed FWA line into Sheldens Bay. The owner accepted
  the name on 2026-10-29 as "an ASSUMPTION … low confidence" (`found_name_calls.json`). The only evidence is the forest
  road names around it.
- The cut was found as follows: the FTEN road "SP 19(1) TO SHELDONS" (R06206 sec 331) crosses the line at
  53.12968, −131.78224, measure 3,402 m. Sources: the T164 ticket line 118-121 and `name-variants/mine.py:417-423`.
- The line is 3,692 m long, so "upstream of" binds only the top ~290 m (registry sections 0 / 3074 / 3401).
- `mine.py` also names a second road, "SP 19H TO SHELDONS", whose crossing was never located.
- Not confirmed: whether DFO's "Spur 19 Bridge" is the SP 19(1) crossing, the SP 19H crossing, or on this stream at all.

## 4. Issues, most important first

1. **Thompson ~2 (live): no carve-outs for the 3 year-round no-fishing areas.** The wrong mark sits on the dormant
   twin, so it is invisible on the live place. Evidence: B1.
2. **A "wrong" with no fix ships the old extent, and nothing flags it.** No preflight or gate reads wrong verdicts
   (B2 code chain). This affects B2, B3, Sproat (#5) and Stamp (#7).
3. **Sheldens: the cut covers the top 290 m of an assumed stream.** It needs a new look at the Spur 19 site, and
   possibly at the stream's identity. Evidence: B3.
4. **The live Fraser Mission→Hope place (#17) is undrawn** until the Hope cut is accepted and the atlas rebuilt. Its
   link target (#15) is empty too.
5. **Region 2 versus the Alexandra Bridge.** The Region 2 page named the Alexandra Bridge in 2023-09, 2025-08 and
   2026-03 (history, run). That is 4.3 km above Spuzzum, where every Region 2 extent stops and Region 3 begins.
   Whether Region 2's "upstream of Mission" runs to the Alexandra Bridge is the owner's call.
6. **Fraser Region 2 channels (Seabird and others) are missing from #19 and #20.** The fix is the water record
   (B2). The owner's note was saved on the Region 3 place, so it may be filed against the wrong place.
7. **The B2 label mismatch** may have produced a "wrong" on a correct extent (inferred).
8. Smaller points:
   - the Sproat +50 m move is not done, and the cut is now shared with 2658;
   - the Little Qualicum falls versus the park line;
   - the Stamp Deer Creek check is not done;
   - the Atnarko sign position;
   - the Chilliwack ~2 "upstream" wording.

## 5. Not checked

- No reach run or bundle. In-force means the curated files plus `splits.resolved.json`. The waiting marks have not
  landed.
- No outside map lookups. Every landmark position above is from repo data. Where the repo cannot confirm one, it says
  "not confirmed".
- I did not trace the review checklist's label source (B2).
