# T1167 — In-season notices: a dated notice's banner shows from two weeks before to its last day; the banner is louder; the answer stays the book's

Status: IN REVIEW — owner: see ## Owner (built, code 3a58e77c0 3dc579337 01eefbe2a; pictures in T1167-shots/: before / inside / after, light and dark)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1167 | In-season notices: a dated notice's banner shows from two weeks before to its last day; the banner is louder; the answer stays the book's | IN REVIEW | 3a58e77c0 3dc579337 01eefbe2a | owner: look at T1167-shots/ (before / inside / after, light and dark); then the PR and an independent review; ships in the first patch after 0.10.0 |

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

## Built (3a58e77c0, review fixes 3dc579337, the warning look 01eefbe2a)

- The rule lives in ONE place per language, pinned by one case file: core `app/packages/core/src/inseasonWindow.ts`
  (`inSeasonDating` / `inSeasonSpan` / `inSeasonOn`; the feeds worker reads it too) and
  `pipeline.regs.inseason.matches.dating` / `span` / `on`, both reading `inseason-span.parity.json` (30 cases); the 14
  days are `INSEASON_LEAD_DAYS` (`pipeline/common/feed_formats.py`, emitted).
- DATED: the text's one range (`parsed.window`) with its year from the effective day, read only when the range opens
  on that day (over the new year it ends the year after; Feb 29 reads in the leap year); or an effective cell printing
  both ends. Text and cell ends: the later. A rescind's quoted range: undated. The worker never takes a time of day
  or a day with its month after it as a range's end. Unreadable (no whole start and end, a same-month range ending
  before it starts, a day the month lacks, no effective day, a range not opening on it) or dates read as no one
  range: shown as undated, told to the owner ONCE per set by the feeds worker (`undatableEvent`); the app reports none.
- Look (owner 2026-11-26): in force, the banner, its line and the pointer wear the page's warning amber as the
  no-access block does (`palette.warning` bar, `palette.noAccessBand` ground, words ink: 15.5:1 light, 11.5:1 dark; the
  bar 3.0:1 / 12.4:1 on the card, pinned in `theme.test.ts`) and the caveat titles' caution mark "⚠", hidden from a
  screen reader; upcoming stays grey.
- Words: before "Upcoming in-season change (Jul 30–Sep 15)" (inbox: "Upcoming", its range, never opening the bell by
  itself), inside "In-season change (Jul 30–Sep 15)" with "See the in-season notice above" on the game-fish strip's
  status line, or at the top of the Keep tab on a page with no strip (core `inSeasonKeepOf`); after: nothing. Year
  bars (game fish, salmon, trout) carry a band and "In-season notice, Jul 30–Sep 15".
- Pictures: `app/tools/inseason-shots.test.ts` (`T1167_SHOTS=1`), the real RegsScreen over the trout fixture's Vedder
  River page with the real Okanagan notice row, on Jul 20 / Aug 15 / Sep 20, 2026.

## Owner

- [x] 2026-11-25 — When a dated notice's banner shows → "2 weeks before to the end (Recommended)"; whether it turns the answer to closed → "Banner only, but louder" (2026-11-25)
- [x] 2026-11-26 — How loud the banner is inside the window (A amber with a mark / B red / C grey as pictured) → "A: warning amber with a mark (Recommended)" (2026-11-26)
- [x] 2026-11-26 — How the banner looks in the 14 days before (A grey, as pictured / B amber too) → "A: grey, as pictured (Recommended)" (2026-11-26)
- [x] 2026-11-26 — A range that starts later than its effective day (A date it when it starts within N days / B never date these) → "B: never date these" (2026-11-26)
- [ ] 2026-11-25 — Look at the three pictures of the louder banner (before, inside, after the window) when the lane has them.
