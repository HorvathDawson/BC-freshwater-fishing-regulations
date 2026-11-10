# T598 — "Days in 10" replaces the percentile wherever the reader sees it

Status: OPEN

Source: owner 2026-11-10, on keeping "Lower than 8 days in 10": "also easier for a user uneducated to understand";
asked whether measured gauges should say it the same way: "if it is user visible then yes probably better".

## Today
- T583 put every standing wording in `app/packages/core/src/standing.ts` (band, headline, short, long, reads,
  percentile, place; one `figure()`). Measured gauges print "Below normal · 20th percentile"; only the estimate
  sentence prints "Lower than 8 days in 10".

## Work
1. In core's standing words, the reader-facing figure becomes the days-in-10 form for measured gauges too, e.g.
   "Below normal · lower than 8 days in 10" (high side: "higher than 8 days in 10"). One rounding rule, stated in the
   file, with the ends handled (never "10 days in 10"; the place chip's "≤10th percentile" gets its plain form).
2. Every surface follows from core: Conditions card and rows, map dot label, chips, saved spots, the public gauge page
   and card, alert editor and alert text (an alert set "below the 20th percentile" reads in the same plain form).
3. "Percentile" may stay only where it is not reader-facing (code, feeds, tests) and in one Help line that explains the
   plain form. Extend the `one-standing-words` gate to refuse a reader-facing "percentile".
4. Shots of each surface, web; short forms must still fit the chips and the dot label (propose a short form if not,
   e.g. "8 in 10 days higher"; owner sees it in the shots).

## Acceptance
- No reader-facing "percentile" outside Help; web and phone print the same words from core; the gate is in.

Depends: T583. Size: S–M. Owner: look at the shots (short forms).
