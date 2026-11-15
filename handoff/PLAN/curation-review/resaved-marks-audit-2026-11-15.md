# Re-saved DFO marks audit — 2026-11-15

Read-only. Nothing was changed. This answers the owner's question: "we really need to check a lot of the items for this same issue and make sure we remove duplicates or bad ones?"

**Short answer.** For every DFO place saved more than once, what is in force now is what the last save says. Only one place still carries something left over from an earlier save: the Thompson group link (B1). Two more places keep an extent that the owner's own last save calls wrong (B2, B3). That is by design, since a "wrong" save with no fix changes nothing, but it is worth knowing. The Atnarko place is clean: the later save, with tributaries included, is the one in force, and nothing remains from the first save.

How the rules work (from `pipeline/regs/review/marks.py` and `proposals.py`):
- A save replaces the earlier one on the same key (`scope`, `target`, `rule_id`). The last line wins (`latest`).
- A `cleared` line takes the place back to unmarked.
- The write-through only **sets** fields: the extent list, tributaries, `duplicate_of`, `tidal_water`. It never undoes an edit that has already landed. So something an older save wrote stays in the file when the newer save says nothing about that field. This happens when:
  - the newer save's tributaries are left empty ("as the place has them");
  - the newer save is a "wrong" save with no fix;
  - the newer save is a `cleared` line.

  The audit looked for exactly these cases.

## A. Counts

| | count |
|---|---|
| DFO places with at least one mark | 213 |
| places saved 2 or more times | 33 |
| … all saves identical (pure repeats) | 2 |
| exact-repeat lines in total, across all re-saved places | 9 |
| places whose saves differ | 31 |
| … differ only in note or checklist text | 2 (`2:khartoum-lake:whole`, `6:rainy-creek:whole`) |
| places with something left over from a superseded save | **1** (B1) |
| places where the last save says "wrong" but the earlier extent stays in force | 2 (B2, B3) |
| cuts in cuts.json made from review marks | 42, **none orphaned**: every one is used by a place, a synopsis entry or tidal_limits.json |
| DFO groups where only some members were later re-saved on their own | 0 (the Fraser groups were reset and every member re-saved) |
| verified places with no extent, no group link and no tidal call | 0 |
| one wording on one water in force under two place ids | 0 |

**`apply --check`, run 2026-11-15** (`PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.review.apply --check`):
- Result: exit 1, "21 NOT LANDED". Every one of the 21 is **waiting**, none refused or stale.
- 19 are "a cut changes: make the side rebuild first" or "waits on new split(s)". They are on these places:
  - Fraser (region 2): 6 places
  - Fraser (region 3): 1 place
  - South Thompson: 1 place
  - Quesnel: 2 places
  - Atnarko "downstream of the sign": 1 place
  - Cowichan: 1 place
- 2 tidal marks are on the allowed-to-wait list (T359).
- In short, every mark in force has either landed or is waiting on a side rebuild. Atnarko is one of the waiting ones: its last save (tributaries in) has not landed yet, and the place file still shows no extent with tributaries `true`.

## B. Leftovers found (proposals; nothing applied)

| # | place id | mark lines | what is left over | proposed fix |
|---|---|---|---|---|
| B1 | `3:thompson-river:from-kamloops-lake-outlet-downstre` | 2520 (verified, "same place as …~2"), then 2521 (wrong: rows 2 and 4–8; "ours: no extent", the exceptions are not cut out) | `duplicate_of = …downstre~2`, `duplicate_confirmed = true`, written by the superseded 2520 (commit 7b0a259cc). The last save (2521) neither keeps nor removes the link. The place has no extent of its own, so it still follows ~2's extent. | Owner decides. If the link still holds, nothing to do: the "wrong" is about ~2's extent and its exceptions. If it does not hold, save a "not the same place" mark on it. |
| B2 | `3:fraser-river:mainstem-waters-of-the-fraser-rive` | 2577 (verified: between `area:3` and Spuzzum), then 2578 (wrong: the "from" row; "make sure … it includes all channels too, like Seabird Island") | The 2577 extent is still in force. The last save says it is wrong and gives no fix. | A ticket for the Region 3 start point and the side channels, then re-save. Fits with the Fraser reset (T743). |
| B3 | `6:sheldens-creek:upstream-of-white-triangular-fishi` | 2470 (wrong + new cut "Spur 19 Bridge site", saved by Claude at the owner's request), 2516 (verified), 2517 (wrong: "I think Spur 19 is at completely the wrong spot?") | The cut `sheldens_creek__spur_19_bridge_site` (blk 360846083 @ 3402 m) and the `upstream_of` extent from 2470 are still in force. The last save disputes both. | Re-locate the Spur 19 bridge (owner or a split hunt), then move the cut (it is used by this one place) and re-save. |

Checked and **not** leftovers:
- **Fraser `…upstream-of-the-cp~2`.** Its `duplicate_of …upstream-of-the-cp` was there before any mark (naming pass 73c04dbc2). The T743 reset put it back on purpose (a9fbaa802). Line 2569 then gave the place its own extent as well. Both point the same way (CPR to Spuzzum). One small difference: the place's own tributaries are null, while the linked place has `false`.
- **Atnarko (2582, then 2584).** The only differences between the two saves are: `items` [gnis:17209] became [], and tributaries null became `true`. The second save is in force and nothing remains from the first. One small point: the shared cut record will be built from the *first* save's proposal of that cut (same anchor). Only its long research note differs; the position is the same.
- **Quatse.** The weir point from 2452 (`n2-2`) never became a cut. Only the ±100 m cuts from 2475 exist.
- **Little Qualicum falls.** The manual coordinate cut from 2440 and 2467 became an area-boundary cut (LITTLE QUALICUM FALLS PARK), as that note asked.
- **Ashlu, Cheakamus, Bridge, Wannock, Braverman.** The earlier saves were auto-approved "whole". The later saves are checklist "verified" with no fix. The file says whole in both cases.
- **Sproat and Stamp (unsure, then wrong).** The extents are the same in both saves. The "wrong" asks for the cut to move (a split-level ticket).

## C. Candidates for the owner to re-look (not verdicts)

| place | DFO wording | why |
|---|---|---|
| `6:morice-river:from-fishing-boundary-signs-locate` (last: 2596 wrong) | "…100 m downstream of Gosnell Creek to Lamprey Creek. This area is open for angling for chinook salmon only and is closed …" | The extent is unchanged and fine. The owner flagged that species words sit inside the location text, which is an issue unless the row is retired. |
| `5b:bella-coola-river-not-including-at:all-tributaries-to-the-bella-coola` (2583 verified) | "All tributaries to the Bella Coola River." | In force: the whole Bella Coola **with** tributaries, so the mainstem is included too. The wording means tributaries only. Flag for the tributaries-only lane. |
| `6:bulkley-river:all-tributaries-of-the-bulkley-riv` and `…~2` (unmarked) | "all tributaries of the Bulkley River other than the Suskwa River" / "… other than Morice River and tributaries, Suskwa …, Two Mile Creek" | The file has tributaries `false` (taken from the water's default). The proposal says `true`. Both mean tributaries only, with exclusions. |
| `2:stave-river:downstream-of-b-c-hydro-dam-to-the~2` (2524 unsure) | "downstream of B.C. Hydro Dam to the CPR Railway Bridge; except … east side …" | The wording mentions tributaries, but tributaries are not stated anywhere (place, water and proposal are all null). |
| `6:nass-river:mainstem-waters-downstream-of-the~2`, `6:nass-river:mainstem-waters-near-the-meziadin` (unmarked) | "mainstem waters …" | Tributaries are null, not `false`. "Mainstem" suggests they should be `false`. |
| `2:fraser-river:mainstem-waters-downstream-of-the` (2568 unsure) | "mainstem waters downstream of the CPR Bridge …" | It follows `…from-the-downstrea` through a link from before any mark, and that place's tributaries are null. "Mainstem" suggests `false`. |

## E. Tributary words versus tributaries in force (every active DFO place: 258)

How the columns are read:
- **In force** is the place's `binding.tributaries`. When that is null, the water's default is used. When the place has a `duplicate_of`, the linked place's value is used.
- **Proposal** is `suggestion.tributaries` in `dfo-suggestions.json`:
  - "as place" means null, so whatever the place has;
  - "–" means the place has no suggestion.
- States of the 258 places: 152 verified, 20 unsure, 3 wrong, 83 unmarked.
- Matches: 47 places mention tributaries; 7 use the weaker words (watershed, system, drainage); 16 say "mainstem" or "excluding tributaries".
- **Mismatches: 4 where the wording mentions tributaries**:
  - 3 tributaries-only wordings: Bella Coola "all tributaries", and the two Bulkley "all tributaries … other than";
  - Stave ~2, whose tributaries are not stated anywhere.
- **Plus 3 "mainstem" places with tributaries null** rather than `false` (see C).
- Every "including tributaries" place has tributaries `true` in force. Every "excluding tributaries" place has `false`.

### E1. Wording says tributaries are included

All of these are `true` in force, and the proposal is `true` or "–":

| place id | state | in force | proposal |
|---|---|---|---|
| 1:koksilah-river:including-tributaries | verified L2604 | yes | yes |
| 1:nanaimo-river:including-tributaries-except-in-th | unmarked | yes | yes |
| 1:stamp-river:from-the-confluence-of-the-ash-riv~2 ("…including tributaries") | verified L2459 | yes | – |
| 2:capilano-river:including-tributaries | verified L2597 | yes | yes |
| 2:chehalis-river:downstream-of-the-logging-bridge-2 ("…including tributaries to that part") | verified L2478 | yes | yes |
| 2:little-campbell-river:downstream-of-12th-avenue-includin | verified L2537 | yes | yes |
| 2:alouette-river-and-tributaries:upstream-of-the-216th-street-bridg | verified L2464 | yes | – |
| 2:alouette-river-and-tributaries:downstream-of-the-216th-street-bri | verified L2463 | yes | – |
| 2:alouette-river-and-tributaries:north-alouette-and-tributaries | verified L2506 | yes | – |
| 5b:long-lake:including-tributaries | verified L2598 | yes | yes |
| 6:cranberry-river:including-tributaries-note-the-sec | verified L2633 | yes | yes |
| 6:ishkheenickh-river:including-tributaries | verified L2610 | yes | yes |
| 6:kiteen-river:including-tributaries | verified L2635 | yes | yes |
| 6:meziadin-lake:including-tributaries | verified L2647 | yes | yes |
| 6:meziadin-river:including-tributaries | verified L2639 | yes | yes |
| 6:bish-creek / brim-river / dala-river / khutze / khutzeymateen-river / kiltuish-river :including-tributaries | verified L2607–2613 | yes | yes |
| 6:kloiya-river / quaal-river :including-tributaries | verified L2623, L2624 | yes | yes |
| 6:stikine-river:including-tributaries-see-the-tahl | verified L2627 | yes | yes |
| 6:swift-river:including-tributaries | verified L2648 | yes | yes |
| 6:tahltan-river / taku-river(…see-the-naki) / tatsatua-creek / wahoo-river | verified L2628–2631 | yes | yes |
| 6:babine-lake:including-tributaries | verified L2605 | yes | yes |
| 6:bear-river:including-tributaries | verified L2606 | yes | yes |
| 6:kitseguecla-river / kitwanga-river :including-tributaries | verified L2637, L2615 | yes | yes |
| 6:morice-lake:including-tributaries | verified L2493 | yes | – |
| 6:sustut-river / suskwa-river :including-tributaries | verified L2643, L2641 | yes | yes |
| 6:kitsumkalum-lake / redsand-lake / scotia-river :including-tributaries | verified L2614, L2625, L2626 | yes | yes |
| 6:tatshenshini-river:upstream-of-the-bc-yukon-border-al~3 ("Tatshenshini mainstem and all other tributaries") | unmarked | yes | – |
| 2:stave-river:downstream-of-b-c-hydro-dam-to-the~2 | unsure L2524 | **not stated (null)** | as place (null) |

### E2. Wording means tributaries ONLY (flag for the tributaries-only lane)

| place id | state | in force | proposal |
|---|---|---|---|
| 5b:bella-coola-river-not-including-at:all-tributaries-to-the-bella-coola | verified L2583 | yes, **and the mainstem too** | yes |
| 6:bulkley-river:all-tributaries-of-the-bulkley-riv | unmarked | **no** (water default false) | yes |
| 6:bulkley-river:all-tributaries-of-the-bulkley-riv~2 | unmarked | **no** (water default false) | yes |

### E3. Reverse check: the wording says mainstem only or excluding tributaries

| place id | state | in force | proposal |
|---|---|---|---|
| 6:meziadin-lake:excluding-tributaries | verified L2646 | no | no |
| 6:bulkley-river:downstream-of-the-morice-river-con ("excluding tributaries") | verified L2653 | no | no |
| 6:babine-lake:babine-lake-excluding-tributaries~3 | unmarked | no | no |
| 2:fraser-river:mainstem-waters-upstream-of-the-cp~2 | verified L2569 | no (via link) | as place |
| 2:fraser-river:mainstem-waters-downstream-of-the | unsure L2568 | **null** (via link) | no |
| 6:skeena-river:mainstem-waters-only (and …-only-between-cedar, …-only-upstream-of-c, …-only-upstream-of-c~2, …-downstream-of-cnr, …-near-the-kitwanga, …-near-the-kitsumkal) | unmarked | no | – |
| 6:nass-river:mainstem-waters-downstream-of-the~2 | unmarked | **null** | as place (null) |
| 6:nass-river:mainstem-waters-near-the-meziadin | unmarked | **null** | as place (null) |

None of these has tributaries `true` in force.

### E4. Weaker words (watershed, system, drainage)

All 7 are region-wide or section rows, and all are unmarked:
- `5b:region:section`, `6:c:section` (Nass watershed), `6:d:section`, `6:e:section`, `6:b(i):section`, `6:b(ii):section`: tributaries `false` (the section binds its waters by area).
- `6:f:closure` ("Fraser River Watershed"): `true`.

Nothing here contradicts the wording. These are bound by area, not by a single water.

## F. "Rest of" extents: do they adapt when other places are added or removed?

**Answer: yes, but only at the next reach build.** Both kinds are worked out from the places as they are in the curated files at build time; nothing about the siblings is frozen at save time. A new place, or one taken away, changes the rest without anyone re-marking it. Two exceptions:
- **Dormant places count.** A place that is no longer on the live page still takes its water away from the rest, as long as it has an extent.
- **One broken sibling blanks the whole rest.** If any one sibling cannot be drawn, the rest stops mapping entirely. It does not shrink: it goes blank.

### F1. The kinds and where they are worked out

| kind | meaning | stored as | worked out at |
|---|---|---|---|
| `rest` ("the rest of the water") | the water minus every other placed place on it | `{"op": "rest"}` with no sibling list (`rest_at_build`; an old explicit list is ignored for DFO) | `pipeline/regs/dfo_salmon/entries.py:984-992` picks the siblings; `pipeline/atlas/reach/build.py:639-706` (`_build_rest`) subtracts them |
| `other_places` ("all the other placed places on this water", T607) | the union of every other placed place on the water, the rest included; it has no stretch of its own | `{"op": "other_places"}` | `entries.py:968-971, 993-1000`; `build.py:592-627` (`_build_others`) |

When each piece runs:
- The reach build (`pipeline/atlas/reach/dfo_salmon.py:108`) passes in every place of the region file (`siblings=by_id`).
- The review app's preview does the same from the live files (`curation-review/backend/dfo.py:1068-1071`).
- The **bundle** holds whatever the last reach run produced, so a change reaches the app only after the reach step and the bundle are rebuilt. Per memory, a placement change needs only the reach step plus the bundle, not a full atlas rebuild.

### F2. Places using these kinds (all in Region 1)

| place | kind | wording | last mark | siblings it depends on now |
|---|---|---|---|---|
| `1:little-qualicum-river:all-waters-unless-indicated-below` | rest | "all waters unless indicated below" | L2472 verified | the two "between" places (falls to hatchery fence; hatchery fence to signs) |
| `1:little-qualicum-river:all-open-portions-of-the-little-qu` | other_places | "all open portions of the Little Qualicum River" | L2616 verified | the rest above plus the two "between" places |
| `1:qualicum-river:all-open-portions-of-the-qualicum` | other_places | "All open portions of the Qualicum River" | L2652 verified | 3 places: upstream of the Big Qualicum…, downstream of the upper weir, below the QFN boundary fence |
| `1:stamp-river:except-in-those-areas-and-times-li` | rest | "Except in those areas and times listed below" | L2466 verified | 5 active placed places (Ash confluence ~2 with tributaries, 200 m above the falls, inlet above Roberts…, powerline, below Girl Guide); 8 dormant places have no extent |
| `1:nanaimo-river:including-tributaries-except-in-th` | rest, tributaries `true` | "including tributaries, except in the area described below." | L2664 verified | 5 placed places, **2 of them dormant** (power line crossing; Cedar Road bridge upstream); the 2 tidal Subarea 17-14 places have no extent, so they are not subtracted |

None of the 21 waiting marks is on one of these waters.

### F3. Proof, run in memory (nothing written)

**Which siblings get picked**, from `to_reach_input` on in-memory copies of the real region-1 places:

| case | result |
|---|---|
| Qualicum other_places as it is | 3 siblings |
| … a new placed place added on the water | joins: 4 siblings |
| … the weir place's extent emptied | drops out: 2 siblings |
| … the weir place deleted | drops out: 2 siblings |
| Stamp rest as it is | 5 siblings |
| … the "below Girl Guide" place emptied | drops out: 4 siblings |
| … a **dormant** Ash place given an extent | **joins: 6 siblings**. Dormant places are not filtered out. |
| Nanaimo rest as it is | 5 siblings, 2 of them dormant |

**Which sections result**, from `build_reach` on the test suite's 4-piece toy river (`test_complement_rest`):

| case | result |
|---|---|
| rest with one placed stretch | m1, m4 |
| that stretch removed | m1–m4 (the rest grows back) |
| the stretch's water missing from the registry | rest **unresolved** (`complement_unknown`): one undrawable sibling blanks the whole rest |
| other_places over that broken stretch plus the now-unresolved rest | **unresolved, no sections** (the blank carries through) |

A sibling's extent changing (for example a cut moving) feeds through the same path, because each sibling is rebuilt from its current extent on every run. This is inferred from the code: the code proves it, but it was not run separately.

### F4. Edge cases

| case | what happens | loud or silent |
|---|---|---|
| sibling unplaced (no extent), or its mark is waiting on a new cut, so its binding has not landed | not a sibling, so the rest **covers its stretch** until it lands | silent (the review app lists it as "unplaced", `dfo.py:1091`) |
| sibling's extent names a cut that does not resolve, or it does not bind | rest → unresolved and the place goes **unmapped**; other_places → that sibling adds nothing | rest: shows as an unmapped place in the reach rows (a gate on it was not checked). other_places: silent, a diagnostic only (`build.py:619`) |
| sibling marked wrong or unsure | ignored: only the extent in the file counts | silent |
| sibling dormant or retired from the live page but still has an extent | **still subtracted** (Nanaimo now: 2). The rest may then leave water with no salmon rule where an old place used to be. | silent. Policy question for the owner. |
| two `rest` places on one water | each skips the other (`rest_siblings`), so both cover the same remainder. No such case today. | silent |
| rest plus other_places on one water | handled: other_places is never subtracted from a rest, and the rest is part of the union (Little Qualicum) | pinned by tests |
| rest with tributaries | the rest's water walks its own tributaries; each sibling is subtracted with its own walk. **Inferred from the code:** inside a rest, a sibling whose tributaries are null takes the *rest's* setting (`entries.py:992` passes the raw null), while on its own, or inside other_places, it takes the *water's* default (`entries.py:979-980`). Today they agree everywhere (Nanaimo: rest true, water true). | latent, silent |
| one placed sibling only (other_places) | the two cover the same sections; which one answers first is decided by id (T611) | the review app warns (`dfo.py:1094`) |

### F5. Tests

Already pinned:
- `pipeline/tests/test_other_places_extent.py`: union; each sibling's own walk; an unbound sibling adds nothing; nothing bound means unresolved; the Little Qualicum shape; an unplaced place is not in the union.
- `pipeline/tests/test_review_offset_cuts.py:71-104`: a place placed later drops out of the rest without a re-mark.
- `pipeline/tests/test_complement_rest.py`: the rest's arithmetic.

Missing (proposals):
1. A dormant sibling with an extent: decide the policy, then pin it.
2. A sibling with null tributaries inside a rest whose tributary setting differs from the water's.
3. Two rests on one water: refuse it or say so.
4. A DFO-level test that one undrawable sibling makes the rest unresolved, and that other_places then loses the rest.
5. A gate or report line that lists every rest or other_places place that came out unresolved after a reach run, so the blank-out is loud.

## D. What was not checked

- **The marks file grew while this audit ran.** Sections A and B were read at 2,652 lines; the owner kept saving (for example Nanaimo L2664, read in F). Saves after line 2652 are not in A or B.
- F was not run against the real registry and graph. It used the toy river from the tests plus the real place files, to avoid a heavy load.

- Marks outside the `dfo` scope (entry, rule, split, tidal, slough, name, closed_area) were not audited for re-saves.
- Whether "tributaries null" means in or out at build time was not checked against the built bundle. No build or bundle was read: the in-force column comes from the curated files plus the water's default.
- Whether each landed extent matches the DFO wording on the map. The 21 waiting marks have not landed, so their final state is unknown until the side rebuilds are done.
- The review app's live queue (the server on port 8787 was not touched) and the 2,652 lines' history before the `extent_before` field existed (some places have no recorded starting state).
- The text-pattern matching in E is a word scan. A wording that says tributaries in other words is missed.
- Exact repeats were judged with seconds, date, prints, `extent_before` and the new-split note, quote, confidence and name ignored.
