# T1077 — "All sources" lists the bait rules' source, but the bait list on the page draws no Source line

Status: IN PROGRESS — owner approved 2026-11-26; what's left: the PR merge. Code 392a36531 (hardening lane); ships with the next app build, no data release.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1077 | "All sources" lists the bait rules' source, but the bait list on the page draws no Source line | IN PROGRESS | 392a36531 | the PR merge (owner approved 2026-11-26) |

Source: the 0.9.1 chat, 2026-11-24, PR #126 (T1036): group 1 of the sheet counts the bait Source line's rules (`srcs:bait`) in `app/packages/core/src/regs/sheets/allsources.ts`, but BaitList draws no Source line, so the sheet lists a source the page gives no way to open.

## Work
1. BaitList draws its Source line like the other gear rows (the sheet and the screen then agree, which is T1036's rule), or the sheet stops counting it; the first is the default.
2. A test that every source the sheet lists has a drawn Source line on the page.

## Acceptance
the test fails without the change; before/after picture.

Severity: low. Release: 0.9.2 (0.9.1 only if PR #126's lane takes it in the same change). Model: Sonnet. Size: S. Owner: no.

## Done (hardening lane, 2026-11-26, 392a36531)
- Work item 1 took the SECOND way, not the default: the bait list keeps no Source line of its own. T900 (owner-approved
  2026-11-19) gives the Gear section ONE Source line (`srcs:gear`, `mergedSource`), and that line already carries the
  bait rules and pages; the defect was only that the every-source sheet's walks still read the bait list's folded line
  (`srcs:bait`) as drawn. Drawing it again would undo T900.
- One test names the folded lines, `foldedSource` (`core/src/regs/sheetKey.ts`: `srcs:gl|…` and `srcs:bait`), read by
  `drawnCitations` and `reachableSheets` (`sheets/allsources.ts`) and the Source row's page walk (`tabSources.ts`).
- What the angler sees: the sheet lists the same rules; the bait rules now come after the gear lines' rules, in the
  Gear section's Source line order (Skeena River part 11, Jul 15: "Bait ban" moves from 15th to 19th row).
- Tests (each fails without the fix): `allsources.test.ts` (a rule only the bait's own line names is not listed, nor is
  `srcs:bait` one tap away; the order follows the drawn line), `tabSources.test.ts` (the bait's own pages are not
  cited), and work item 2, `WaterScreen.sources.test.tsx` / `WaterScreen.salmonOnly.test.tsx`: every sheet core lists
  is one a press on the drawn screen opens, with no `srcs:` exception (failed on gnis:2936 part 11 Jul 15 with
  `srcs:bait`).

## Owner

- [x] 2026-11-26 — Look at the before/after of the every-source sheet's order (pictures in chat). Owner: "Yes, bait after gear is fine".
