# T685 — Build the coast rule from the owner's 2026-11-12 answers

Status: OPEN

Source: owner 2026-11-12, T678 Tidal Coast Proposal answers (`handoff/PLAN/app-integration/T678-coast-proposal.md`, line ids there). RULINGS E21–E24.

## Work
1. **Single rule (E21).** No DFO written boundary on the water: (a) a water that reaches the sea is fresh to the shoreline, DFO's polygon never cuts; (b) a creek entering a TIDAL river part is cut at the river's edge (its line crossing into the river's drawn water area), only the piece inside tidal; a creek entering a FRESH part is untouched.
2. **Line lists, by reference to the write-up** (expected tidal km, today -> after):
   - G1 59 lines fresh: 21.2 -> 0.0 km. G2 17 cuts dropped: 1.9 -> 0.0. G6 E1–E10, 63 lines fresh to the shoreline: 36.1 -> 0.0.
   - G3 18 lines cut at the river's edge: 11.8 -> 8.2 km (write-up figure; re-measure at build).
   - G4 10 and G5 3 keep today's cuts (E22): 6.7 -> 6.7 and 0.5 -> 0.5 km.
   - G7: X1, X5, X6 cut at the river's edge; X2 Suter Brook Creek fresh to the shoreline; X3 stays fresh; X4 loses its cut; X7 cut at the river's edge, 168 m tidal / 53 m fresh (E23). The write-up's G7 "after" (0.6 km) predates these answers; re-measure.
3. **Five owner slough rows (E24).** Add Deas (S3), Woodward (S10), McDonald (S8), Tamboline (S9), London (S6) as `"limit": "whole"` owner rows in `data/curated/waters/tidal_limits.json` (where Gilmour, Tilbury, Williamson, Ewen live). The other sloughs (S1, S2, S4, S5, S7, S11–S20) follow rule (b): cut at the river's edge, slough fresh.
4. **Fix the automatic reading.** It called G4/G5 "all fresh" because it ignores the piece drawn across a tidal river's own water; make it count that piece.
5. **Confirm cut placement in code.** The owner asked whether the cut uses the river polygon or the FWA section/waterbody key. Intended: the river polygon edge. Read the code, confirm which it does, report in the ticket's closing note, and change it if it uses the key.
6. Diff the binding before and after; only the lines above change.

Depends: one atlas rebuild; bundle with T682, T683, T684. Size: M.
