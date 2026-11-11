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

## Owner readings 2026-11-11 (12 of 16; F2, F9, F16, F21 wait for map shots)
- **F1 Campbell River:** the river straddles the unit boundary for a stretch. Only the stretch that straddles a unit in
  1-1 to 1-6 takes the Jul 15-Aug 31 summer closure; the rest (unit 1-10 only) does not. Show which parts straddle, on a map.
- **F4 part-of-lake closures (Shuswap, Quesnel, every lake like them):** a clear warning on the card for all such lakes now.
  Splitting the lakes properly is far future.
- **F5 Nechako inside Region 6:** closed Apr 1-Jun 30. The "Exempt from spring closure" on the Region 7 row lifts Region 7's only.
- **F7 Fraser steelhead notice (Region 3):** the banner stops at the Region 3 boundary.
- **F8 Fraser, Regions 5/7:** upstream of the Cottonwood River the exemption LIFTS the bait ban, Region 5's too ("typically I
  would say it holds but this is explicit"). Needs a cut at the Cottonwood confluence; below it Region 5's ban holds.
- **F10 Babine above the Fort Babine Bridge:** not fly only. As shown is right.
- **F11 in-season closures:** parse in-season notices the same way as normal regulations, so a closure changes the card.
  That needs a rebuild; between a new notice and the rebuild that carries it parsed, a good banner shows so it is not lost
  (an unparsed notice is the banner). Design ticket needed; the parser run is the owner's.
- **F12 Stellako:** release all rainbow. General rule: a rainbow rule with no size constraint applies to both steelhead and
  ordinary rainbow, since it did not specify.
- **F17 Lakelse Lake:** no bait ban. By the book's definitions page, tributaries are streams.
- **F18 Babine salmon:** sockeye only. The notice itself says "closed to recreational fishing for Sockeye Salmon". As shown is right.
- **F19 Stellako top stretch:** nothing to add. The river already carries "No Fishing Nov 15-May 14; Class II water when
  open"; the book's pointer is a note for people looking at the lake.
- **F20 Fraser tidal stretch:** a freshwater notice starts at Mission, not on the tidal stretch.

## Owner readings 2026-11-11, the four map ones (maps in handoff/PLAN/app-integration/T623-shots/)
- **F2 Campbell at Elk Falls:** the no-fishing water is Elk Falls down to the John Hart generating station (1.86 km below the
  falls). Fly only starts at the station and runs down to the power line near the Quinsam. Above the falls: the river's ordinary rules.
- **F9 Campbell power line:** the cut goes to the cleared power-line strip about 320 m above the Quinsam mouth (today it sits
  1,332 m up, beside the station).
- **F16 Little Qualicum:** Upper Waterfall, as today. The book says only "the falls in Little Qualicum Falls Provincial Park".
- **F21 Peace at Clayhurst reserve:** closed, as today (the reserve covers the north half of the channel).
All 16 readings are now answered. The cuts for F2/F9 (and F8's Cottonwood cut) are proposals for the owner's tick in the review app.
