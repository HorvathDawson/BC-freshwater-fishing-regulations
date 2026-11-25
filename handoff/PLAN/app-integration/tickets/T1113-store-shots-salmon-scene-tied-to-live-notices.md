# T1113 — Store pictures: the salmon scene depends on one water's live DFO notices and fails out of season

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1113 | Store pictures: the salmon scene depends on one water's live DFO notices and fails out of season | OPEN — with T1072, or 0.9.2 | | store content chat 2026-11-24 |

Source: store content chat 2026-11-24 23:50: store-shots scene `19-regs-salmon` fails today because the Vedder page
has no "Salmon (DFO)" block (live notices change through the season). The scene is not in the current set, so
nothing is blocked.

## Work
1. The scene runs against a pinned state (a frozen feed fixture and a set date, as the store video ticket T715
   asks for the video), not the live feed, so it renders the same in any month.
2. Check every other scene for the same tie to live data or today's date; list them in the PR.
3. The reshoot fails with the scene's name and the missing block, as it does now (keep that).

## Acceptance
The salmon scene renders on any date from a fresh checkout; no scene reads a live feed.

Depends: T1076. Severity: low. Release: with the store content work (T1072), or 0.9.2. Model: Sonnet. Size: S.
Owner: no.
