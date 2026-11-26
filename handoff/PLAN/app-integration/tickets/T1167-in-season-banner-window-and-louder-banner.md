# T1167 — In-season notices: a dated notice's banner shows from two weeks before to its last day; the banner is louder; the answer stays the book's

Status: IN REVIEW — owner: see ## Owner (built, code 3a58e77c0, not pushed; pictures in T1167-shots/: before / inside / after, light and dark)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1167 | In-season notices: a dated notice's banner shows from two weeks before to its last day; the banner is louder; the answer stays the book's | IN REVIEW | 3a58e77c0 | owner: look at T1167-shots/ (before / inside / after, light and dark); then the PR and an independent review; ships in the first patch after 0.10.0 |

Source: T1157's finding and the owner's answers, 2026-11-25 night. Seven provincial in-season closures of
July 30-Sept 15, 2026 (Salmon River 3-26, Bessette Creek 8-22, Granby River and its tributaries 8-15 from noon to
midnight, Okanagan River and Oxbows 8-1, Similkameen River 8-2) are in the app as banners only: during the window
the answer under the red banner still said "open"; and the banners still show in November, because a notice with
no parsed end date shows until gov.bc.ca's page drops it (his rule of 2026-11-11,
`app/packages/core/src/inseason-span.parity.json:24-28`). Owner, verbatim: "we could not show the banners unless it
is in the date range (or 2 weeks before?)". Dialog picks: the window **"2 weeks before to the end
(Recommended)"**; whether a dated closure also turns the answer to closed: **"Banner only, but louder"**.
Related: the memory note `inseason-is-bc-not-dfo`, T230, `data/curated/inseason/README.md`.

## Work
1. Read the dates a notice states in its own text ("July 30 to September 15", with the year); a notice with a
   stated range shows its banner from 14 days before the first day to the last day, then not at all. A notice
   with no dates in its text keeps today's rule (until the province drops it). A range that cannot be read is
   treated as undated and reported, never guessed.
2. Louder: inside the window the banner sits above the answer and the status line says to see the in-season
   notice; in the 14 days before it reads as upcoming with its dates. The answer and the map colour stay what the
   book says (his pick). Whole-year calendars mark the window.
3. Pictures (390 px, light and dark; before the window, inside it, after it) to the owner before it ships.

## Acceptance
Tests for the three states and for an unreadable range, each failing without the change; the seven 2026 notices
show nothing today. Severity: medium (an angler could fish a closed water under an "open" answer during a
window). Model: Sonnet (Opus for the date reading). Size: S–M. Owner: the look.

## Built (3a58e77c0)

- The window rule lives in ONE place per language, pinned by one case file: core `inSeasonDating` / `inSeasonSpan` /
  `inSeasonOn` (`app/packages/core/src/inseason.ts`) and `pipeline.regs.inseason.matches.dating` / `span` / `on`, both
  read `app/packages/core/src/inseason-span.parity.json`; the 14 days are `INSEASON_LEAD_DAYS`
  (`pipeline/common/feed_formats.py`, emitted to `feedFormats.generated.ts`).
- Dates: the feeds worker's one printed range (`parsed.window`, month and day; a time of day after it does not break it,
  pinned in the worker's `events.test.ts`) with its year from the notice's effective day: read only when the range opens
  on that day; it ends on the next last day (over the new year: the year after). A rescind's quoted range is undated.
  No start, a day the month lacks, no effective day, or a range not opening on it ("August 15 to December 31" posted
  Sept 16) is undated and reported once per set (`INSEASON_FAILURES.unreadRanges`); a curated `effective` dates one.
- Words: before "Upcoming in-season change (Jul 30–Sep 15)", inside "In-season change (Jul 30–Sep 15)" with the Keep
  strip's line "See the in-season notice above", after: nothing. Year bars (game fish, salmon, trout) carry a band over
  the range and the legend "In-season notice, Jul 30–Sep 15". Upcoming cards wear the quiet bar.
- Pictures: `app/tools/inseason-shots.test.ts` (`T1167_SHOTS=1`), the real RegsScreen over the trout fixture's Vedder River
  page with the real Okanagan notice row, on Jul 20 / Aug 15 / Sep 20, 2026.

## Owner

- [x] 2026-11-25 — When a dated notice's banner shows → "2 weeks before to the end (Recommended)"; whether it turns the answer to closed → "Banner only, but louder" (2026-11-25)
- [ ] 2026-11-25 — Look at the three pictures of the louder banner (before, inside, after the window) when the lane has them.
