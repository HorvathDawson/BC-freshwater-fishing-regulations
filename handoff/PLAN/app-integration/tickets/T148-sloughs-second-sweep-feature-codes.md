# T148 — Sloughs/canals second sweep: FWA feature codes, inside line codes, calibrated on the owner's marks

Status: DONE — 5b8066d7 (data); workflow wf_dc7c8506-0c6 (census → revise → adversarial verify, 5 verification fixes). Outputs in
handoff/PLAN/sloughs/v2/ (census.md, sweep2.md with its Verification section, candidates_v2.json.gz 2,424 records,
census.py, revise.py, local_entries.py, not_in_candidates_elong6.csv). Next: T149 (review page).

Source: owner 2026-10-23: "we should have also looked at lakes with feature codes in mind … go through the lakes again
with the ones I labelled in mind to do a second sweep plus revision". Part of T55.

## Work
Census every lakes / manmade / wetlands polygon by FEATURE_CODE (Lake definite/indefinite/intermittent; reservoirs;
sewage basins, tailing ponds, dumps; canals; flooded land; marsh/swamp) and the streams-layer line codes inside it
(Main Flow, Lake Arm, Secondary Flow, Main Connector, ditch, canal). Revise the 865 candidates with rules learned from the
owner's 98 marks (dammed widening → lake; piece of a named lake → that lake; lake-arm-only → lake; main flow through a
narrow polygon → stream), never moving an owner-marked piece off its verdict. New class NOT_FISHING_WATER (sewage,
tailings, dumps) with any the app draws/binds. Naming suggestions (Fuller Slough, Williamson Slough). Honest
leave-one-out agreement with the marks.

## Acceptance
sweep2.md with rules, agreement table, changes, new candidates; candidates_v2 ready for the review page (T149).

## Result (2026-10-23)
- 2,424 candidates (865 first sweep + 1,559 new): STREAM_BY_BOOK 96, LIKELY_STREAM 827, UNSURE 520, LAKE 706,
  NOT_FISHING_WATER 275. 58 first-sweep classes changed (R6 narrow named river 24, R5 secondary flow 17, owner marks 7,
  R4 soft lake on a river 5, R1 part of a named lake 3, R0 2).
- Feature codes are a WEAK stream/lake signal: 79/89 stream marks and all not_stream marks are Lake–Definite; named
  Indefinite/Intermittent lake items are real lakes (0 lake-arm, 0 on a named-regulated river). The codes are strong
  for EXCLUSION: 275 sewage basins / tailing ponds / dumps are all drawn as lakes (z8–14), 12 carry the Liard watershed
  rule, 4 tailing ponds sit on named creeks.
- Strongest stream signal: a Secondary Flow (1300) line inside (41/89 stream marks; 17/233 LAKE). Lake Arm (1475) is in
  only 88 lakes. Main flow (1200) alone is noise (79% of lakes).
- STRUCTURAL GAP: a river entry's reach rule almost never binds a lake polygon on its course (7 polygons province-wide):
  456 unnamed polygons on rivers regulated by a named entry fall back to zone rules; 180 were missed by the first sweep.
- Dams: no dam dataset in the repo, so the owner's "dammed area = lake" case (Quinsam, South Nanaimo, Bonaparte) cannot
  be detected; the provincial dams layer would be needed (follow-up).
- Honest agreement: rules 90/97 on marks, leave-one-out 89/97 (= first sweep); the v2 rules are untested on fresh marks.
- Intermittent/Indefinite polygons with a named stream through: 1,071 (A pool) → 506 LIKELY (elongation ≥ 3 or a slough
  name), 185 UNSURE, 379 LAKE (round ponds); only 7 sit where the river's own entry binds; 602 are in Region 7B.
