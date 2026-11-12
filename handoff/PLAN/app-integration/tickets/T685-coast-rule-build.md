# T685 — Build the coast rule from the owner's 2026-11-12 answers

Status: DONE (434f16264)

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

## Built 2026-11-12 (434f16264; candidate `data/generated/atlas/full_next`, NOT promoted)
Per-group tidal km, live -> candidate: G1 21.1 -> 0.0; G2 1.65 -> 0.0; G3 11.82 -> 8.25; G4 6.65 -> 6.65 (owner rows,
E22); G5 0.49 -> 0.49 (owner rows); G6 36.07 -> 0.0; G7 0.53 -> 0.47 (X1 174.9 m, X2 130.3, X7 168.1; X3 X4 X5 X6
fresh); sloughs 8.6 -> 26.9 (five whole). Binding diff (reach on live vs candidate, same code/corpus): 199 rules change,
every changed section on a line this ticket or T684 names; unresolved 28 -> 28.
**Cut placement (owner's question):** the cut is the river POLYGON's edge. `coast.CoastGeometry.river_at` picks the FWA
river polygon(s) the creek's mouth sits in by location (only the creek's own polygon is excluded, by its key);
`coast.water_edge` walks up the line to where it leaves that polygon; FWA's edge-1450 vertex is used only when it is the
same place within 1 m. No FWA section or waterbody key chooses the receiving water.
**Did not come out as answered:** G4/G5: the polygon edge falls 0-283 m below today's cut on 10 of 13 lines (354101341:
7 m vs 290 m), so "keep today's cut" is kept as 13 owner rows in `shore_held.json`, each recording the rule's edge. X5,
X6: the Brunette is drawn single-line there (no polygon within 1 m of either mouth), so the river's edge is the mouth:
fresh. Turner/Noons (-2200000392): accepted 21.3 m tidal (E25) but not built: it is in the Noons water the owner's
railway-bridge split governs, and the cut left a piece the reach's unplaced gate refused.
Left (Claude): promote `full_next` (`python -m pipeline.atlas.promote data/generated/atlas/full_next
--accept-boundary-loss`: 18 lost cut points, all listed in `T685-progress.md`; the permission system refused it in this
lane); owner: the Turner/Noons conflict.

## Closed 2026-11-12
`full_next` promoted to `data/generated/atlas/full` with `--accept-boundary-loss` (owner-approved; old atlas at `full.prev`).
Turner/Noons: owner answer 2026-11-12, leave it fresh; the railway-bridge boundary governs (RULINGS E25 note). Ships in the next staging release.
