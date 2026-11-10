# T598 — "Days in 10" replaces the percentile wherever the reader sees it

Status: IN REVIEW (built; web shots in ../T598-shots/; phone not run; owner: look at the short form)

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

## Built (T598)
- core `standing.ts`: one rounding rule. A percentile p at or under the middle becomes n = round((1-p) x 10), above it n = round(p x 10), half up, so a rank and its mirror say the same n (25th and 75th both 8).
  - Long: "lower than 8 days in 10" / "higher than 8 days in 10" (`daysInTen`); card "Below normal · lower than 8 days in 10 for the date"; row "Reads below normal · lower than 8 days in 10."
  - Short (PROPOSED, owner to judge in the shots): "8 in 10 days higher" / "8 in 10 days lower" (`daysShort`), the long form turned round. Used for the map dot label, a lead-up day's chip, the histogram tick ("about 8 in 10 days higher"), the like-this-visit line and alert preset chips.
  - Ends: n = 10 (p under 0.05 or from 0.95) never prints "10 days in 10": "lower than almost every day on record" / "higher than almost every day on record" (short "almost all days higher" / "almost all days lower"). A day ON the envelope edge: chip "9+ in 10 days higher" / "9+ in 10 days lower", sentence "lower than at least 9 days in 10 for the day". An estimate's interval: "Lower than 7 to 9 days in 10 for early November" (counts held to 9).
  - Alerts keep storing a whole-number rank 10-90; the words are "the level lower than 9 days in 10 for the day" ("the median" at 50); the custom box says "Custom level, 10 to 90 (50 is the median)". Nearby ranks (18, 22) say the same words.
- Help (gauge step) explains the plain form in one line; the walkthrough test still bars the old word.
- Gate: `tools/one-standing-words.test.ts` refuses any string or template with a space that holds "percentile" (identifiers, kind tags and test ids pass), proven on the spellings it replaces; "days in 10" stays core's (Help excepted).
- Public gauge page and share card only print the band headline (no rank), so nothing changed there; the staging-site build on disk predates T583, so no shot of it.
- Shots (web): Conditions card and rows, map dot label. Saved spot, lead-up chips and the alert editor are phone-only (Spots and Notifications are not on the web): their words are in `saved-spot-chips-alert-words.txt`, from the same functions.
