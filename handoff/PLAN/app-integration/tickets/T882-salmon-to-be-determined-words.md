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
   could not read (dates or wording) keeps "Check DFO"; a held row keeps its own words, and a HELD place whose carried
   answer was "To be determined" keeps "Check DFO" too (review fix: DFO has changed that row since). Left as is, the safe
   direction: a row whose words are not "To be determined" but whose date cell is ("… (as printed: To be determined)")
   reads "Check DFO".
2. The map key (one entry covers both kinds, proposal): "To be determined by DFO, or a DFO row the app could not
   read" (long: the Layers / Help key's name), "TBD/unread" (the short key on the map, review fix: it colours unread rows too,
   which the card calls "Check DFO"; the 390 dp row holds 366 dp, `legend-rows.test.ts`: "To be determined" is 28 dp
   over, "TBD / check DFO" 27, "TBD or unread" 8; "Check DFO" alone would also fit and be honest); the Help key's sentence says both. When T873's short Help
   lands, its `HELP_WORDS.salmon.check: null` wins the merge (the key's name carries the words).
3. Colour: already the owner's rule (pipeline `deliver.salmon.map_status`, app `salmonFeed` `mapStatus` and the card's
   `headlineStatus`: open, then release, then check, then closed). Checked over every chain and day of the staging
   release (400,770 chain-days): none paints the check colour while any salmon is open. Pinned by tests
   (`salmonTbd.test.ts`, `test_salmon_file.py::test_t882_green_whenever_any_salmon_is_open`).
   - Today (Nov 18): Thompson River closed everywhere (rule book, Oct 1–May 31, plus the DFO closed areas); South
     Thompson below Campbell Creek closed, above it check (sockeye TBD); Kamloops Lake check.
   - Sep 1: Thompson Gold Pan → Skihist green (chinook open Aug 28–Sep 22, sockeye TBD); Kamloops Lake outlet → Gold Pan
     check (chinook + sockeye TBD Jul 1–Sep 30, nothing open); South Thompson above Campbell Creek and Kamloops Lake
     green (chinook open Aug 16 / Aug 28 – Sep 22).
4. SEO pages and the share: the water pages prerender the app's own Shell (`tools/seo/pane.tsx`), so they take core's
   words; the share sends a link only (no salmon text).

## Pictures (390 dark, staging data, web build of this branch)
Before / after: `/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/699dcd1b-ff73-4ed1-a69c-69feb621d839/scratchpad/t882/t882-{before,after}-390-dark-south-thompson-nov18.png`, `…-thompson-goldpan-sep1.png`,
`…-thompson-kamloops-sep1.png`; the maps: `/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/699dcd1b-ff73-4ed1-a69c-69feb621d839/scratchpad/t882/maps/`.

Left (Claude): an independent review, then merge (lane app/t882-to-be-determined); the owner's yes on the key words.
