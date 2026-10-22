# A water's own gauge leads its answer — report (2026-10-22)

THE RULE AS BUILT
- What counts as an own gauge: a panel donor whose station is matched to the section's own water (gauge.item_id equals the section's water item, from build.py's `owner` map). A side channel takes its river reach's panel, lead included (option A).
- Who may lead. The pipeline decides this at build time, in panel.py `may_lead` and build_panels.build:
  - Not FAR: the along-water distance from the gauge to the nearer end of the tapped section is at most max(10 km, 0.2 × the river's Hack length at the larger catchment). Hack length is L = 1.27·A^0.6 km.
  - Not MUCH SMALLER: the section drains at most 10× what the gauge does. A gauge draining more than the section is not limited by this, because the owner named only the smaller case.
  - The station is real-time (the roster's `realtime` flag), because the feed carries nothing else.
  - Per quantity (the station's gauge_stats parameters), only the nearest 4 such gauges are candidates.
  - Candidates sit on top of the 4-member cap, so nothing a panel holds today is displaced. A first version put them inside the cap; that silenced 125 sections in Flow, so I changed it.
  - Each candidate gets `lead` = its rank by distance. This ships as `panel_member.lead` and in paint/2.
- What leading means (core `leaderOf`/`leadShares`, mirrored by panel.py `leader_of`/`lead_shares`):
  - The leader is the best-ranked candidate that is reporting the quantity asked for. This is how "never mix quantities" works: a level-only gauge has no discharge reading, so it is not a vote in Flow. In Level it leads.
  - The leader carries max(70%, its usual share). The other donors split the rest in their usual proportion.
  - A leading own gauge counts as a measurement of this water, so a disagreeing tributary cannot make the panel refuse.
  - If nobody may lead, or every candidate is quiet, the usual weighting applies exactly as before.

THRESHOLDS (named constants in panel.py)
- OWN_LEAD_SHARE = 0.7 (mirrored in trust.ts). It is a floor: a gauge already at 90% keeps 90%.
- OWN_SMALLER_RATIO = 10. This is where the measured error ladder leaves its flat best row ("close", ≤10×).
- OWN_FAR_FRACTION = 0.2 and OWN_FAR_MIN_KM = 10. Far scales with the river size. The limit is 10 km up to ~1,000 km², 16 km at 1,100 km², 42 km at 5,000 km², 97 km at 20,000 km², and 193 km on the Fraser below Hope.
- OWN_LEADS_PER_QUANTITY = 4. At 4, Hope (the 4th discharge candidate at Mission) still leads lower-Fraser Flow; at 2 or 3 it does not.
- Grid (sections where an own gauge would lead, Flow / Level, all at 10× smaller):

| far rule | Flow | Level |
|---|---|---|
| 25 km fixed | 8,201 | 4,862 |
| 50 km fixed | 9,018 | 5,413 |
| 100 km fixed | 9,986 | 6,175 |
| no far limit | 10,681 | 7,290 |
| 0.1 × Hack | 7,489 | 4,341 |
| 0.2 × Hack, 10 km floor (chosen) | 8,613 | 5,179 |
| 0.3 × Hack | 9,193 | 5,893 |

  Switching the size gate from 10× to 3× changes only 7 sections in Flow.
- Nearest own gauge, along the water: median 0 km, p75 24 km, p90 71 km, p95 119 km. Section-to-gauge catchment ratio: p95 1.47, p99 192.

REAL DATA (the changed build_panels.build run on the live graph via heavy.sh; feed 2026-10-21T18:00Z; estimate ported from trust.ts using the pipeline's own lead_shares/leader_of)
- Baseline check: the changed build without the water map reproduces option A on all 246,619 sections, 0 differ.
- 5,074 sections (16,639 km) hold an own gauge that may lead. 272 sections gain a member and none lose one.
- What stops own-gauge relations: 5,565 may lead; 10,336 are not real-time; 4,025 are far; 79 are far and much smaller; 1 is much smaller only.
- Dictionary size: 4,109 → 4,322 panels, 9,512 → 10,464 member rows. Members per section stay 1–4 everywhere except 272 sections: 5 (96), 6 (46), 7 (3), 8 (8), 9 (65), 10 (54). The 9s and 10s are the lower Fraser.

Province-wide, before → after (sections / km):

| effect | Flow | Level |
|---|---|---|
| unchanged | 98,891 / 218,956 | 106,048 / 230,565 |
| moves under 5 points | 921 / 3,288 | 696 / 2,487 |
| moves 5–10 points | 191 / 825 | 280 / 946 |
| moves 10–25 points | 15 / 79 | 137 / 485 |
| moves 25+ points | 0 | 103 / 435 |
| gains an answer | 2 / 10 | 32 / 88 |
| loses its answer | 0 | 0 |
| silent before and after | 146,599 / 206,836 | 139,323 / 194,988 |

Named examples, before → after:
- Pitt (sids 308350, 308351, 60043): Flow 17th (08MH006 100%) → unchanged; the own gauge does not touch Flow. Level 10th (08MH035 67%, 08MH006 33%) → 9th (08MH035 70%, 08MH006 30%).
- Chilliwack at Vedder Crossing (sid 60210): 5th, 08MH001/016/103 at 33% each → 5th, 08MH001 70% and 15/15.
- Chilliwack above Slesse (sid 1235783): 08MH016 35%/08MH103 35%/08MH001 30% → 70/16/14.
- Vedder (sid 313838): nothing leads. Its own gauge, 08MH047, is retired, and the Chilliwack gauges belong to another water item. Unchanged at 5th, a third each.
- Lower Fraser, Mission to the mouth (sids 59623, 59739, 59752, 59779, 59780). Hope is 37–106 km upstream.
  - Flow 50th (Hansard/Shelley/Hope a third each) → 44th (Hope 70%). This answers the side-channels report's defect 2.
  - Level 36th → 8th–9th, led by the tidal level gauges at Whonock, Mission and Port Mann.
- At Hope (sid 60087): Flow 50th → 44th; Level 36th → 18th.
- Upper Fraser:
  - Below Shelley (sid 59483): 56th → 54th, Shelley 70%. Red Pass, 493 km up and 13× smaller, does not lead there.
  - At Red Pass (sid 61130): 56th → 60th.
- Long river with its gauge near the top, the Similkameen: 08NL070 (272 km²) is 137 km above sid 316754 (3,193 km²). It is far and 11.7× smaller, so nothing leads and the answer is unchanged. Near the gauge (sid 316820) it leads Level, 37% → 70%. The Sikanni Chief has one donor only, so nothing changes.
- Creek whose own gauge sits on a tiny fork, Inkaneep Creek: 08NM012 is matched to a 1.2 km² piece. It does not lead the 77 km² reach; 08NM200 does (it was already 100%). Kicking Horse below Sherbrooke Creek (61.8 vs 1,102 km²) behaves the same way.
- Coquihalla: 08MF062 → 70% on its 116 km² reach; 08MF068 → 70% on the 481 km² reach.

TESTS
- Pipeline, pipeline/tests/test_own_gauge.py (35 tests):
  - Shares: own leads; auxiliaries keep their proportion; the floor; no lead; own gauge alone; a quiet rank 1 hands over to rank 2.
  - The Pitt: level-only own gauge does not lead Flow and does lead Level.
  - combine with a lead.
  - Gates: far scales with the river; much smaller; a bigger gauge still leads; missing areas.
  - Build: rank order; far → 0; smaller → 0; another water → 0; no water map; cap ride-on; real-time only; per-quantity limit; no record → 0; lead is part of the interning; member rows; side channel takes its river's lead.
  - Mutation pins on: the far gate, the smaller gate, the 70% floor, the per-quantity limit, the real-time condition, and riding on top of the cap.
- App:
  - core ownGauge.test.ts (9 tests); data ownGauge.test.ts (4 tests, the Pitt): Flow equal to the unled panel, Level 70/30, map equals sheet.
  - paint.test decodes the lead and refuses paint/1.
  - one-formula.test runs Python lead_shares/leader_of against the TS on 10 cases.
  - trust-ladder holds OWN_LEAD_SHARE equal.
  - A manual mutation of the TS floor (Math.max removed) turned ownGauge.test red; restored.

## Review fixes (93531158)

The far limit is measured at the PUBLISHED catchment, and ties go to the gauge the model agrees with. Some figures above predate this fix; the fix notes below give the corrected ones.

Both reviewer findings were confirmed on real data and are fixed in /Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations/.claude/worktrees/wf_3c35e1af-596-1. I wrote the code changes before the new tests. The new tests fail against HEAD without the fix (they cannot import the new names), and each has a mutation test that checks it catches the fault it targets.

(1) The far limit used modelled areas, which made it too short. Confirmed: the panel areas match the reviewer's figures (Hope 60,356 modelled vs 216,600 published; Hedley 2,084 vs 5,568). At sid 316754 the limit was 32.2 km, so Hedley at 47.2 km counted as far. Fix in panel.py: a new `area_scale(area_gauge, published_km2)` gives published / modelled. It falls back to 1.0 when the published area is missing or more than `OWN_AREA_TRUST` = 10x away from the model in either direction. Only 3 of 395 real-time gauges fall outside that: 08MH028 (163,265x), 08GC005 (70x) and 08HD034 (63x). `far_limit_km(area_target, area_gauge, scale=1.0)` applies Hack's law to max(areas) x scale. `may_lead(..., published_km2=None)` passes the scale through. The "much smaller" test is a ratio, so it is unchanged. `build_panels.build(published=...)` reads the stations roster's `area_km2`, and `pipeline/deliver/bundle/build.py` and `scripts/rebuild.py` now pass it. Results: Hedley's limit at 316754 is now 58.0 km and it leads there. Hedley leads 39 of the 40 sections below it, up from 5. Hope's limit is now 415 km (was 193); it led all 105 sections to the mouth (158 km) before and still does. Usk leads all 126 lower-Skeena sections, up from 71.

(2) The Chilliwack tie went to station order. Confirmed: 08MH016 and 08MH103 both sit at 643.989 km² and 0 km on sid 1235783, and 08MH016 got rank 1. The sort key in build() is now (distance, |log modelled/target|, |log area_scale|, station). So when distance and modelled size tie, the gauge whose published area matches the model at that section wins. 08MH103 now leads: 70% on 1235781 and 1235783, with 08MH016 at 16% and 08MH001 at 14%.

New tests in pipeline/tests/test_own_gauge.py:
- test_far_is_measured_at_the_published_catchment (Similkameen and Fraser numbers)
- test_a_published_catchment_below_the_model_shortens_far
- test_a_published_catchment_past_the_trust_bound_is_not_believed
- test_the_build_measures_far_at_the_published_catchment
- test_own_gauges_tied_on_distance_and_size_go_to_the_one_the_model_agrees_with (named so station order would pick the wrong gauge)
- the constants test now includes OWN_AREA_TRUST
- mutation tests for the scale, the trust bound and the tie-break

`scripts/gates.py` now uses the published scale, with a `--model-areas` flag for the old behaviour.

Bundle and release changes for the merger's `deliver all`: only the `panel_member.lead` values change. There is no schema change. Distinct panels go from 3,409 to 3,415. Release paint/2 carries the new lead values, with no contract change. No app code changed.

1. REPORT.md was not written. The tool blocked it ("subagents should return findings as text, not write report files"). It was never committed in 047f4c66 either, but panel.py still points to handoff/PLAN/app-data/own-gauge/REPORT.md. The orchestrator or user needs to write it. The figures below and the commit message hold the content. Named examples (option A before, then after):
- Pitt 308351 Level: 10th → 9th, 08MH035 at 70%. Flow is unchanged at 17th from 08MH006 alone.
- Chilliwack 1235783: 08MH103 70% / 08MH016 16% / 08MH001 14%; 60210: 08MH001 70%.
- Vedder: unchanged (none of its donors is on its own water).
- Mission 59779: Flow 50th → 44th (Hope 70%); Level 36th → 9th (08MH024 70%).
- Similkameen 316754 Level: 08NL038 70%.
- Lower Skeena 1235149 Level: 73rd → 88th (Usk 70%); this is the biggest move from the fix.
- Sikanni Chief: unchanged (single donor).
- Inkaneep 08NM012 and Kicking Horse 08NA053: much smaller, so they do not lead.
- Fraser 315444: 08KB001 leads; Red Pass is far and 13x smaller.

2. test_release.py ran but did not pass: 7 setup errors in this worktree. It needs a built bundle plus regs.sqlite, and errors with "missing bundle ... regs.sqlite" even with UI_EXPORT_BUNDLE pointing at the main checkout's bundle. The same 7 errors occur at HEAD without my changes. It needs the merger's `deliver all` to run.

3. Owner questions still open, with defaults in place:
- OWN_LEAD_SHARE 0.7
- OWN_SMALLER_RATIO 10
- OWN_FAR_FRACTION 0.2 of the Hack length at the real catchment
- OWN_FAR_MIN_KM 10
- OWN_AREA_TRUST 10 (new)

4. The tie-break assumes the target section shares the gauge's model bias. Two real-time own gauges have no published area (08HD024, 08ME030) and keep the modelled scale.
