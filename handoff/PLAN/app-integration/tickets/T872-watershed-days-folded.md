# T872 — Conditions: the watershed "by day" card is folded unless someone opens it

Status: OPEN (proposal; lands only on the owner's yes). Lane `app/t872-watershed-fold`, from `app/integrate-081-web`.

Owner 2026-11-18 ~20:30, verbatim, looking at Mamquam River → Conditions, the "Squamish watershed · Flow vs. past years,
by day" card: "These section should be collapsed in conditions I think unless someone expands"

Today the card lists Now plus 10 forecast days, one row each (Normal / Much above normal / No gauge reading), and fills a
phone screen (Android, dark).

## Proposal (built on the lane, not landed)

- `ui-native/BasinDays.tsx`, `layout="rows"` (a water's Conditions view only; the browse map's bottom-bar strip is one
  line already and is unchanged): FOLDED BY DEFAULT. The card shows its title, one summary line and a chevron; a tap
  opens today's full list (the day rows, the scale line and the dash key) and the chevron turns.
- The summary is composed in core (`basinDays.ts` `basinDaysSummary`, `BASIN_DAYS_WORDS.summary`, AGENTS 33): the first
  day, then only the days the standing CHANGES — "Now: Normal · Much above normal from Wed 25 · No gauge reading from
  Sat 28". One standing all the way: "Now: Normal · the same through Sat 28". Past 3 changes the rest are counted
  ("· 2 more changes"). A day no gauge reads is a change like any other, never skipped. The line does not move with the
  day the map shows. Words are a proposal; the owner picks.
- Accessible: the fold is one button (on the web a real `<button>`, so Enter / Space work), named
  "Squamish River watershed, by day: <summary>", with `aria-expanded` saying open or shut.
- One load path: the fold is shut on every way in; it stays open only while the same watershed group is shown (a
  refreshed feed, another day picked) and opens shut for another group.

Other long lists on the same page, checked: the gauge picker (`GaugeTitle`) is already behind a chevron; the donor gauge
list already folds its not-reporting gauges (`GaugeGroups`, T168); "Sources and notes" is already a closed fold; the
Change block is one line per model. Only the watershed card had the long-list shape.

Pictures (Mamquam River → Conditions, 390 wide, dark, staging web export): scratchpad `t872-before.png`,
`t872-after-collapsed.png`, `t872-after-expanded.png`.

Tests: `core/basinDays.test.ts` (the summary for runs of change points, pinned as whole strings),
`ui-native/BasinDays.test.tsx` (shut by default, the toggle and `aria-expanded`, a `<button>` in the tab order, another
group opens shut).
