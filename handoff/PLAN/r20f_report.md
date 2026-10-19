# Round 20f — thumbnails in one place, tier headers without the left bar, "from streams", small items (2026-10-19)

Owner items 1–4 on regs-v11 (regs v30), after the Opus agent's round 20e (regs v29 @ 4cd404f6). Details: app/design/regs-v11/UI-NOTES.md
(v30); ANSWERS-SPEC ROUND20f (rows 23). Scratchpad: /private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/266b6a4b-cf8d-417b-b326-25e034ead4e0/scratchpad/r20f/

## 1 Thumbnails in one place
Every kind row: title → size chip → the picture strip → notes (size note, duties, presence). The pictures keep their 80 px size; a
caption tile may run to 132 px, so "Coastal cutthroat trout ›" is two lines. Before/after (390×844 screenfuls):
- frames-before/vedder-oct10-row-dv.png → rows-after/vedder-oct10-row-dv.png (the steelhead's picture was under three notes;
  now right under "over 50 cm (20 in) · rainbow trout")
- frames-before/bella-coola-may1-row-ct.png → rows-after/bella-coola-may1-row-ct.png (the cutthroat caption on two lines)

## 2 The tier headers without the left bar — three styles, one pick
Drawn and shot at the same scroll positions (frames-a/, frames-b/, frames-c/: Bella Coola cutthroat row, Glenn rainbow row,
Vedder bull trout row); `hdr=a|b|c` in the hash shows each.
- a — a plain header on a hairline above it, the big number as the anchor (Archivo 700, 15 / 12.5 px)
- b — a, with the number in a tinted chip
- c — no hairline; more space and a display-face heading (Bricolage Grotesque 800, 17 / 15 px)
A fresh opus reader (PNGs only) answered three comprehension questions per style (all nine right, Q2's "5 or 2" doubt is the
data's Glenn matter) and ranked **c > a > b**: "c is the only style where a header outranks its rows in type, so you can see
which rows belong to it without reading"; a "keeps sentences whole and the line marks the start, but the small header type
inverts the hierarchy"; b "has a's problems plus broken text flow: chips that separate the number from its words, an orphaned
'—' and wide gaps". **c is the default** (regs v30); a and b stay behind `hdr=a|b`. In all three, headers, kickers and rows share
the card's text edge (no 3 px bar, no 13 px step), the size band keeps its tint, and a water's own rule's number is in the accent.
Noted by the reader, all styles: a half line of the in-place share header peeks out under the pin bar (the sticky overlay's edge).

## 3 "from streams" (data, rows 23)
A share that is the region's (a zone or province rule) says "Only 2 of the 4 may be from streams, not counting hatchery rainbow
trout"; the headline "2 a day per angler from streams"; the pin bar "↳ 2 of them from streams". A water's own share keeps
"from this stream". test_answers_r20f_streams.py (fails before: the helper did not exist); pins moved in 12 test files; model
--check 0; live build rc 0; gate 15,862 groups / 7,144 cards 0 problems; page v36 regenerated; re-snapped; oracle.

## 4 Small items
Gear tab row labels in sentence case; the group head's "Rule book p.N ↗" on the origin's line after "How to tell ›"; Bennett's pin
"↳ 2 of them (lake trout) under this lake’s rule"; a pinned tier line goes once less than about a row of its section remains
(the 20e reader saw "2 of them lake trout…" outlive its section over the bull trout rows).

## Checks
r20f-pictures-first, r20f-headers, r20f-region-words — all three fail on regs v29 (check-before.txt), pass after (check-after-2.txt
on the default c). ur1-tier-words, ur3-lifted-share, r20-share-inside-total, r20e-region-vs-stream read "from streams".
check.mjs full: see the end of this file.

## Comprehension gate (the r20e 34-question set, fresh opus readers, PNGs only)
BEFORE (regs v29, the r20e screens): **34 / 34**; Q25 and F8 ("the 2: stream or region?") at confidence 4 — "the ALL REGION 2 STREAMS
kicker over 'Only 2 … from this stream' could also be read as 2 across all Region 2 streams combined".
AFTER (regs v30, screens r20f/gate-after + rows-after): **34 / 34**, no new wrong answer. The screenful question F8 ("the 2: stream
or region?") went **4 → 5**: "a stream limit (all Region 2 streams), not the region limit; the region's limit is 4". The full-card
Q25 stayed at 4 ("the 2 is a limit for all Region 2 streams combined … the ALL REGION 2 STREAMS kicker is needed to tell 'streams'
from 'this stream'" — the kicker and the words now agree). Other moves: F4 (Glenn's rainbow, whose rule) 4 → 5; F3 3 → 4;
Q3 and Q7 at 4 (the rainbow-vs-stream-share relation, data words — the AFTER reader: the share says "not counting hatchery
rainbow trout" while How it adds up says "none from other streams" after 2 rainbows; both are the Q23b ruling).
Confusions left (all data words, logged): the bull trout row's bare "of the 2" under an "Up to 2 …" row; rows outside the band
(Chilliwack's rainbow, the Vedder's own rule) read as outside the stream share on the tall card (the pin bar says it on a
screenful); several rule owners on one card; the over-50 steelhead exception said in the band head and the note.
