# T1197 — Report a problem on a saved-spot page; a spot visited more than once (layout and flow)

Status: OPEN — feature report; design first.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1197 | Report a problem on a saved-spot page; a spot visited more than once (layout and flow) | OPEN — design first | — | decide where "Report a problem" lives on the spot page; then the multiple-visits layout and flow |

Source: in-app report `edb80e33ad6e7951` (marked [feature]; production worker, 0.9.0), first read by `pipeline reports
--since 2026-11-25` (planning chat, 2026-11-26; found by the repo clean chat with no ticket). Feedback log F54. Related:
T1115 (the saved-spot redesign), T1095 (fish measurements per location), T714 (sharing spots).

## Work
1. **No way to report a problem from a spot page.** Water pages have the report link; the saved-spot page does not.
   Add it where the report form already takes its page context (T1004 holds the tag placement), and carry the spot's
   id (never its coordinates) in the report so a defect can be found.
2. **Multiple visits.** The report asks about the layout and flow when a spot is visited more than once (several trips
   to one spot). Read the report's own words from the worker (`GET /reports`, read-only) before designing: what is
   shown now, what the reporter expected.
3. Put both into T1115's redesign pictures (options first), not as a separate patch to the old page.

## Acceptance
A spot page offers "Report a problem"; the multiple-visits layout has pictured options and the owner's pick recorded
here; the report is cited in the log row.

Severity: low (a feature ask). Model: Opus (design), Sonnet (the link). Size: M. Owner: yes (the layout pick).

## Owner

- [ ] 2026-11-26 — Pick the multiple-visits layout from the options T1115's redesign brings. Default: one list of trips under the spot, newest first.
