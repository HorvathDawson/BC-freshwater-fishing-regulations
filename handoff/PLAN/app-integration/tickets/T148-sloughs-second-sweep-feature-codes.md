# T148 — Sloughs/canals second sweep: FWA feature codes, inside line codes, calibrated on the owner's marks

Status: IN PROGRESS — workflow wf_dc7c8506-0c6 (census → revise → adversarial verify), outputs in the session scratchpad
t55v2/ (census.md, sweep2.md, candidates_v2.json.gz).

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
