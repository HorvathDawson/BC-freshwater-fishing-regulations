# HANDOFF — braids, splits across side channels, canals (PAUSED 2026-10-16 by the owner)

The owner paused this whole topic ("all these complicated things") to be picked up later by a fresh agent.
Nothing below is implemented beyond what "Live today" says. Start here, then read the files in "Read next".

## The problem in one paragraph
Curated splits (book landmarks: bridges, signs, confluences, "5 km below X") cut a river's MAIN blue line.
Braids / side channels / multi-channel networks that leave the main line above a cut and rejoin below it are
not cut, so a whole channel has to be put on ONE side of the boundary. Wrong side = a closure that reads open
(or the reverse) over hundreds of metres to kilometres. Related: man-made or river-shaped polygons that carry a
river's own flow (Serpentine canal) belong to no water, so the river's rules stop at them.

## Live today (what the build does now)
- **D5 flow placement** (R9, `extent._flow_reach`, commit ce74eaef): off-line pieces are placed by following their
  flow path to the cut line. Keep this — it is not braid logic.
- **E15 majority side** (R10, RULINGS E15, commit 37292302): a braid straddling a cut goes wholly to the side holding
  most of its length (projected on the main line), marked straddling; bundle table `section_straddle`,
  `read.straddles`, reach output `straddle.jsonl`, POLICY_VERSION 19. Answers/UI do NOT read the mark.
- Unplaced after R10: only 3 municipal streams not joined to FWA (Noons ×2, Seymour) — `flows_to_the_sea_unjoined`.
- Known wrong today (R12 research): majority side never picks the WRONG side, but whole-channel placement leaves
  long stretches mis-ruled — high-risk: Peace Site C (~1.7 km + 970 m of the year-round dam zone read open), Peace
  Halfway closure (1.3 km open), Adams Sep–Oct closure (~600 m open), Chowade (+710 m open / 420 m closed), Skeena
  winter closure (486 m open), Morice Lamprey (444 m over-closed), South Atnarko. 2 Serpentine side lines at 168th St
  were placed by topology and never marked. 2 marks are spurious (Atnarko 360225339, Zymoetz 360245483: junction
  within 1 m of the cut).

## Owner decisions so far (binding)
1. Don't delete water (it would read as unregulated). (R10)
2. ONE uniform method, done in the BUILD step (atlas/sectionizer), so entry extents apply with no special cases.
   No mix of methods. (R13)
3. Leaning: SPLIT. The rule (R14): **"any piece that attaches to both sides of a split is cut along the median
   tangent"** — the tangent averaged over ±100 m of the main line around the cut point. The owner accepts any cut
   line produced this way on such a piece.
4. The Serpentine canal (man-made polygon 329292019) should be part of the Serpentine River, as the Vedder Canal is
   part of the Vedder.

## Research results (all research-only; nothing changed)
- R12 `r12_braids_research.md` (+ `r12_braids/`, PNG sketches dean_canyon / peace_site_c / chowade_network):
  majority vs tangent split agree on side for 38/38; ±100 m tangent cuts cleanly once in 19/21, window size barely
  matters (median 26 m shift); simplification options (snap cut to junction, fold narrow braids) rejected — they
  move book landmarks (median 363 m) or apply to 2/18.
- R13 `r13_braids_uniform.md` (+ `r13_braids/`): recommends FULL SPLIT in the build step = topology gate (a network
  is cut only if it joins the main river on BOTH sides) + ±100 m tangent cut + edge-consistency pass (FWA duplicate
  edges re-wired); simulation over 68 cases: 0 unplaced, 0 marks, fixes all 8 dangerous braids, net +16 sections,
  deletes all E15 machinery. The existing `anchors.resolve_split_defs` perpendicular sweep (gauges only, GNIS-scoped,
  no topology) is to be replaced — 4 of its current gauge braid cuts are artifacts (one 3.1 km from 08KG001).
- R14 `r14_braid_split_plan.md` (+ `r14_braids/`, review page `braids_review/index.html`, published
  https://claude.ai/artifact/Lei1Bt4cCpnvEwessMVyQK): the implementation plan with the rule made exact (cut line
  clipped at its first re-crossing of the host; a piece qualifies only if on a path between the two sides and owned
  by the host's water; one cut at the crossing nearest the cut point; junctions re-homed by end geometry), a wider
  census (any connected line within 3 km of every cut; no curated braid missed), classes cut / junction /
  one-sided (gated, 12 curated) / another water's (8 curated: Stamp/Ash, Alouette, Kootenay CPR, Mitchell, Babine…),
  tests (10 no-data fixtures + 2 mutation pins) and rollout. Effort ≈ 2 days code + ½ day builds; tiles are
  human-run.
- Serpentine (R14): the 168th St "loops" exist in FWA but are drainage ditches (edge code 2300 "manmade secondary
  flow", joined by 1450 connectors) touching the river on ONE side of 168th St → the rule does not cut them. The canal
  329292019 (≈2.36 km × 18 m, a straight cut across a meander) belongs to no item; the river carries a lake boundary
  onto it (`serpentine_river__lake_329292019`), so r2/r3 stop at both ends.
- Canal census (R14): 21,784 polygons are fed and drained by the same river; of 60 unnamed MAN-MADE ones only the
  Serpentine canal is channel-shaped (ratio 131; next 11.5) → "rule A" (unnamed man-made, length/width ≥ 20, one
  river) folds exactly that one. Reconciled with memory `lake-must-not-answer-to-its-river` (that is about NAMES; a
  named lake is never folded).

## Open questions for the owner (unanswered)
1. Drop 2300-only ditch lines from river items, or keep them as the river?
2. Apply the split rule at automatic length cuts too (~265 extra sections, no answer changes)?
3. Option B: fold 339 narrow unnamed lakes-layer polygons that look like river reaches (≤30 m wide, ≥20× longer than
   wide; 130 sample maps in the review page) into their rivers?
4. Cap the cut line length (~1 km)? Peace 5110 crosses at 982 m, Exchamsiks at 1.75 km.
5. (R13) Accept a cut line drawn across the floodplain (up to ~1 km on the Peace) — safe only behind the topology gate.

## Read next (in order)
1. This file. 2. `r14_braid_split_plan.md` (the plan + open questions). 3. The review page (published link above,
source `braids_review/index.html`). 4. `r13_braids_uniform.md`, `r12_braids_research.md`. 5. RULINGS E15, D5/T11 and
AGENTS 12/13 (no silent widening). 6. Memory notes: straddling-sections-mark-not-cut, end-cut-halves-the-water,
split-fix-changes-trib-walk, atlas-build-not-deterministic, tiles-bundle-same-vintage, lake-must-not-answer-to-its-river,
reach-only-rerun.

## Ground rules for whoever resumes
- Heavy jobs only via `handoff/tools/heavy.sh` (one at a time). Never run the LLM parser.
- An atlas change = side atlas build → parity diff minus old-vs-old noise → per-rule binding diff → tiles + bundle
  from the SAME atlas (vintage check) → card gate (`python -m pipeline.deliver.answers.card_check`, must stay 0) →
  fast + slow suites → page v36 regen + v11 re-snap.
- Test-first, concise no-data CI fixtures that fail before and pass after (owner requirement).
