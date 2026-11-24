# T787 — Regions 7 and 5A: the page header "no fishing for salmon until further notice" overrides the dated in-season openings in the table

Status: DONE — 178d52320, ba106ce58. Rows of a DFO table DFO hides in an HTML comment are kept for provenance, marked hidden, and answer nothing (Python and feeds worker); merged 02aea5be3.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T787 | Regions 7 and 5A: the page header "no fishing for salmon until further notice" overrides the dated in-season openings in the table | DONE | 178d52320, ba106ce58 | — |

Closed by the 2026-11-20 audit: the owner's question 15 is answered in the ticket ("we should keep reading them for provenance but mark them hidden..."). Python (178d52320) and worker twin (ba106ce58) are merged via 02aea5be3; the vectors were regenerated in the same lane.

Source: general audit 2026-11-16, labels checker finding 2 (`handoff/PLAN/audit-2026-11-16/labels.md`).

## What happens
`location_rules` replaces a place's rows with the header closure when the page header carries one
(`quota.header_closure`, owner 2026-10-31: "the header closure wins"). So the Nechako (sockeye Aug 22-Sep 21, pink;
FN0851), the Quesnel River (sockeye Aug 22-Sep 15, pink to Sep 30; FN0846) and Quesnel Lake Horsefly Bay read closed
every day. DFO's page carries both: the header, and above the table "There is no fishing for salmon in Region 7
except for the opportunities listed in the table below"; the page cached Aug 29 shows the header and the open rows
together, inside the opening's dates. No effect until next Aug 22.

## What to build (after the owner's answer)
Recommended: a dated table row that carries a fishery notice answers on its own dates; the header closure answers
every other day and fish. Python and the worker's reading change together (parity vectors).

## Owner, 2026-11-16 (question dialog; verbatim in `handoff/PLAN/OWNER-TODO.md`, "Answers by batch")
Question 15, typed: "can we look at when the rules got added to see if they are stale? what came first header or rules?" Found: DFO keeps both in the page and switches each season (off-season the table of openings is inside an HTML comment; FN0851 and FN0846 are from 2025-08-20); our parser reads the hidden table (Regions 7, 5A, one row in Region 4). Then, typed: "we should keep reading them for provenance but mark them hidden somehow so they are ignoired". BUILD: `pipeline/regs/dfo_salmon/parse.py` and the worker's `app/workers/feeds/src/dfo/parse.ts` keep a row from a commented-out table with a `hidden` mark; a hidden row answers nothing (deliver, feed, vectors); his "header wins" ruling stays as the net.

## Lane result (lane/salmon-rules-1116, 2026-11-16)
- Parse: `parse.comment_spans` / `table_hidden`: a table that STARTS inside a comment (balanced, as today, or
  unterminated) is hidden; every row of it (and a banner row made from it) carries `RegRow.hidden`, the parsed region
  `table_hidden`. Rows commented out inside a VISIBLE table stay out of the grid as before (their cells lean on a
  visible row's rowspan) and the visible rows stay live: hidden is per table, never per page. The module docstring no
  longer calls the comment a defect.
- The mark rides: untangle `Rule.hidden` -> scraped `RuleRecord.hidden` (written only when true) -> typed rule
  `hidden: true` (`feed.type_rows`) and the locator's `hidden` (indices into `rows`); a hidden row's words never go to
  the card (`words`). `typed.FORMAT` 3 -> 4 (the release refuses older typed files); the scraped rules file carries
  `scrape_format` 2 and `feed.emit` refuses an older one (typing old scrapes would lose the mark). Main checkout before
  the next build: `python -m pipeline.regs.dfo_salmon.locations && python -m pipeline.regs.dfo_salmon.feed` (cached
  pages, no network, no credits).
- A hidden row answers nothing: `deliver.salmon.in_force`, read by `location_rules` (release), `live.join` and
  `live.location_calendar` (feed). The header's closure no longer stands in for a hidden-only place; the region's own
  place (the header closure on the page default) answers.
- Review: rows unchanged (so prints and marks are untouched; places keep their fingerprints and `active` status);
  the queue row has `n_hidden`, the detail's rule labels `hidden: true`.
- Effect on r-46e4355a7277's inputs (salmon step rebuilt): no section's answer changes (the header closed them
  already). Nechako, Quesnel River and the drawn Horsefly Bay place stay in their chains with no rule today; Region 4's
  place keeps answering through its page sentence. The undrawn "that portion of Quesnel Lake known as Horsefly Bay"
  now ships `d: 1` (no rule today) in `unmapped`, so the card leaves it off until DFO shows its row again.
- Tests: `pipeline/tests/test_t787_hidden_rows.py` (11; mutations: `table_hidden` off fails 6, `in_force` keeping
  every rule fails 1, `feed.emit` without the scrape check fails 1).
- Vectors regenerated: `typed_format` 3 -> 4; pages 4, 5a, 7: every locator rule gains `hidden: true` and its calendar
  is empty (no verdicts, no gears, every run -1); `live.locs` of the Nechako and both Quesnel places: every species -1.
- App chat (worker twin), exact changes: `dfo/parse.ts` — the comment spans of the raw page (from a `<!--` to the next
  `-->`, an unterminated one to the end); if the table's start lies in one, every row (and a banner row synthesised
  from the table) gets `hidden: true`. `untangle.ts` / `locations.ts` — carry `hidden` onto the rule and the scraped
  rule record (key present only when true). `feed.ts` `build` — each typed rule of a hidden row gets `hidden: true`
  (the unread rule too when every row of the locator is hidden); the locator gets `hidden: [row indices]`; `words`
  skip hidden rows. `live.ts` — `join` and `locationCalendar` read only rules without `hidden` (`in_force`).
  `dfo.vectors.test.ts` then reproduces the moved vectors above.
