# T806 — The 40 saves to re-check with the owner, in chat with a picture each: 5 he marked wrong, 35 written by Claude for him (11 live and open in Nov or Dec)

Status: OPEN (the owner's review, in chat; then saves through the review app with his words).

Source: owner 2026-11-16, question 18: "can we sort this out now i want to review it and make it correct for sure";
how: "In chat, with a picture each (Recommended)". T786 (ship DFO's words undrawn for a place marked wrong) stays
open behind this: he chose to review now rather than answer it.

## The list (a checker's run on release r-46e4355a7277; full data in the session scratch folder `wrong-saves/`)
- "Wrong" in his hand, live: Sproat (line 2468; the cut is now 350 m below Hwy 4: re-save as right); Stamp Falls
  (2469; labels done; the lower cut is about 165 m below Deer Creek, DFO says 225 m: he chose "Move it 60 m down").
  Dormant: Fraser Region 3 mainstem (2578; answered right), Thompson (2521; answered by the three closed areas),
  Morice (2596; retired, fine by his own note).
- Written by Claude, live and open in Nov or Dec (11): Stave 2672, Bella Coola 2731, Cowichan 2774 and 2775, Nanaimo
  2776, Tatshenshini Blanchard 2779, Skeena pink 2782, Stamp rest-of 2788, Little Qualicum 2789, Nitinat 2792,
  Somass 2787.
- Written by Claude, other live (17 places + 4 closed areas): Exchamsiks 2728, Bella Coola tributaries 2730, Bulkley
  2732 and 2733 (never marked by him), Illiance 2734, Wilauks 2735, Gitnadoix 2736, Morice 2738, 2739 and 2781,
  Nanaimo 2777, Tatshenshini creeks 2780, Sheldens 2783, Babine Lake 2784, Kitimat west bank 2786, Skeena sockeye
  2790, Dean 2793; closed areas Thompson Deadman 2748, Ashcroft 2749, Juniper 2785 (low confidence), Shuswap 2770.
- His own "unsure" saves all carry a fix (53); nine are live and open in Nov or Dec, Fraser Mission to Hope (2568)
  first.
- The audit tool's section B finds only 8 of the 40 (T776).

## How
Pictures per place (map, ends labelled, DFO's words, rule, side streams on / off) are drawn into the scratch folder
`recheck-pics/`; sent to him in batches, most urgent first; each answer is saved through the review app as a new
save carrying his words (never by editing `data/curated/review/marks.jsonl`).

## 2026-11-16 16:18: Deadman (mark 2748) closed
Why "495 m": the re-check drawing tool used the new cut's base measure (the bridge, 117,608) instead of the landed cut
(116,608, 1,000 m below) and snapped to the gauge 08LF033 section edge (117,094). The build binds the landed cut
(`closures._piece_location` via `landed_cut`). Redrawn: 999 m below the bridge, 3.6 km closed. Owner: "Right
(Recommended)". Nothing changes.
