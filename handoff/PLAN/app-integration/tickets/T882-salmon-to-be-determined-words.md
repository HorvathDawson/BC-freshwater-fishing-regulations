# T882 — salmon: DFO's "To be determined" says "To be determined", not "Check DFO"; green whenever any salmon is open

Status: IN PROGRESS (app chat, lane app/t882-to-be-determined).

Owner 2026-11-18 ~21:18, verbatim: "To be determined I think we could change to actually say to be determined /
However they should still show green if any salmon there are open only yellow if closed other then the to be
determined".

## Found (staging release r-fc137c4040fa)
- The only "check" verdicts in the salmon file are DFO's own "To be determined": "To be determined (Apr 1–Mar 31)" and
  "To be determined (as printed: To be determined)" — Thompson River chinook + sockeye, South Thompson sockeye
  (and Kamloops Lake sockeye, the same Region 3 rows).
- They read "Check DFO" on the card's row, "Check DFO today" in its headline, "See Salmon below" in the year bar's
  key, "check DFO" on the folded row, and "Check DFO (not mapped yet, or season not set)" in the map key.

## Fix
1. Words, composed in core (`salmon.ts`): a "check" verdict whose every row is DFO's "To be determined" reads
   "To be determined" (row), "To be determined by DFO" (headline), "to be determined" (folded row, bar day, strip),
   "To be determined" (year bar's key), "Closed today · to be determined in part" (an undrawn place). A row the app
   could not read (dates or wording) keeps "Check DFO"; a held row keeps its own words.
2. The map key (one entry covers both kinds, proposal): "To be determined by DFO, or a DFO row the app could not
   read" (long), "To be determined" (short key on the map); the Help key's sentence the same.
3. Colour: already the owner's rule (pipeline `deliver.salmon.map_status`, app `salmonFeed` `mapStatus` and the card's
   `headlineStatus`: open, then release, then check, then closed). Checked over every chain and day of the staging
   release: no chain-day paints yellow while any salmon is open. Pinned by tests.
