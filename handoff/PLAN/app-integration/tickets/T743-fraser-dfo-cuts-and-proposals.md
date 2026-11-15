# T743 — The Fraser's DFO places: cuts checked, proposals fixed, a dossier to label from

Status: DONE on the data side, 319a447a0 (Fraser data lane, 2026-11-14); the re-marks and cut acceptances wait on the owner

Source: owner 2026-11-14: "also please try to fix up fraser and make sure all the cuts are good and fix proposals etc."


## Work
1. Every DFO place on the Fraser, all regions, with its dated wordings, suggestion and mark.
2. Every cut those wordings need: exists, where, right bridge; missing ones as proposals.
3. Suggestions corrected where the cause is mechanical (suggester fixes with tests); the diff shown before it is applied.
4. Fraser marks that look wrong, listed for the owner to re-mark one at a time.
Dossier: `handoff/PLAN/curation-review/fraser-dossier-2026-11-14.md`.

## Result
- Dossier written (12 places: 10 mainstem, 2 Nicomen Slough; with marks 10; written by a group click 4; suspect 5).
- Suggester (commit 319a447a0): a DFO page is held to its region (open ends stop at a region-line cut, a whole
  water across regions is not suggested whole, an end past the line is named); "A to B and from B to C" is A to C
  and never clustered with a half; a proposal on the winning cut is no rival. Tests:
  `pipeline/tests/test_dfo_suggest_fraser.py`. On today's marks 3 of 136 suggestions change, all Fraser.
- Cuts: CPR Bridge at Mission and Spuzzum right (Mission is the rail bridge, one point with the tidal limit);
  Hope Hwy 1 and Alexandra missing from cuts.json but already proposed, each under two proposal ids. Nothing added
  to cuts.json (accepting a cut needs an atlas build).

## Waits on the owner
- Re-mark the three "Mission to Hope" wordings Mission -> Hope (now Mission -> Spuzzum; two by one group click).
- Before the next release: as the places file stands, Hope -> Spuzzum (~38 km) would show chum/coho open where DFO
  says no fishing for salmon (the live Mission-to-Hope place wins a tie with "upstream of Mission").
- Alexandra: which bridge, and keep the 4.3 km past the Region 2/3 line or not; the Lillooet signs; Region 2
  "upstream of Mission" ending at Spuzzum.
- Regenerate dfo-suggestions.json only after the re-marks: `PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.dfo_salmon.suggest`.
