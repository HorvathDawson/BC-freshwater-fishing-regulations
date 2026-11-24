# T905 — Shrink the live salmon feed back under its old size

Status: OPEN (main chat; later).

T870 (2026-11-19) added each place's printed DFO rows (`pr`) to the live salmon feed: index.json grew to ~133 kB
(15.7 kB gzipped) and its size check was raised from 112 kB to 135 kB. Owner 2026-11-19 ~06:40, on that question,
verbatim: "okay we will work to shrink it down later."

## Work
Measure where the bytes are (`pr` ~34 kB raw), then shrink without losing a row: intern repeated row text, or ship the
rows once per DFO page and point to them by index; bring the budget back down in the same change. Python and the
feeds worker twin together (AGENTS 40).

## 2026-11-24: the release's salmon head too (note)

The release file `salmon` (the head) went over its 100,000 B gz gate on the 0.9.0 build (101,480; live 97,723) and the
owner raised it to 110,000 (2026-11-24, "Raise to 110,000 (Recommended)", T796). Shrink it with the feed: measure what
grew since r-741815c6090d (T754 one-bank places and T812 Babine circles are the likely adds), then bring the gate back
down in the same change.
