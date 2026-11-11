# T623 — Rules as shown vs the sources: 21 findings from the 48-water audit

Status: OPEN

Source: T608 part A, `handoff/PLAN/app-integration/T608A-findings.md` (commit ef14f3b83): 48 waters, 34 clean, no wrong
value in any regional table; 21 findings F1–F21 (5 high, 7 medium, 9 low), each with the rule as shown, the source
text and printed page. Only 5 are certain; 16 need the owner's reading. 15 more waters are listed apart as DFO
placement/coverage, waiting on the owner's labelling: not part of this ticket.

## Work
1. **Certain ones first** (the findings file marks them): among them F3 (Campbell River salmon: DFO's chinook-only
   "to be determined" shown for all five species) and F13 (Squamish coho short line drops "hatchery marked only").
   F3 and F13 are in the DFO feed's modules, which the review-app chat owns: hand those two over with the evidence.
2. **Owner's reading** for the rest, as proposals in the review app or a short list he answers, never changed on
   Claude's reading alone. The high ones: F1 (Campbell River closed Jul 15–Aug 31 under the Region 1 summer closure
   though the book lists it in unit 1-10), F2 (Campbell parts 4–5 at Elk Falls), F4 (Shuswap and Quesnel bay/arm
   closures undrawn: keep limits shown lake-wide, the closure only a "not yet mapped" note; Salmon Arm Bay is closed
   today), F5 (Nechako part 5 and the Region 6 Fraser-watershed spring closure).
3. For each answered finding: fix at its cause (parse, placement, cascade, feed wording, display), with a test that
   pins the printed page; where one finding stands for a class (F4: part-of-lake closures; F15: "on parts" lost from
   vessel rule titles; F11: an in-season closure that is only a banner), fix the class and count how many waters move.
4. After the owner finishes DFO labelling, re-run the audit's DFO placement list.

## Acceptance
- Every F row is fixed with a pinned test, or answered "as shown is right" by the owner with his words recorded.

Depends: T553 (base rules pinned), T546 (reach run). Size: L. Owner: yes (16 readings).
