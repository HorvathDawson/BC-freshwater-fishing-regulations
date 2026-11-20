# T946 — Tier 2 tributary lines: 20 parts whose "rule for its tributaries" line is not what their rail sets

Status: OPEN.

Source: lane/t936-t931 (`scripts/test-full.sh`, step "release: table tributary lines = their sheets (T892)"),
2026-11-20. T892 itself is DONE, so this is a new ticket.

## What
`app/tools/release-tributary-lines.test.ts` (RELEASE_DIR = r-6258ffded676, TIER2=1) finds 20 parts where the table row's
tributary line says "X's rule for its tributaries" but the rail's setting steps are not all tributary ones:
- 18 tributaries of Lake Revelstoke, trout row DV ("Lake Revelstoke's rule for its tributaries"): Kelly, Belcher,
  Fuller, Craw, Standard, Pelkey, Stitt, Murder, Argonaut, Graham, Granite, Hiram, Pass, Sweeper Bill, Holden, Holway,
  McCulloch and Old Camp creeks (part 0 each);
- Iverson Creek (gnis:11245) part 0, Whitefish and Arctic grayling ("Rancheria River's rule for its tributaries").

## Work
Find whether the line or the rail is wrong (the DV row after the C2/T854 char changes is a first suspect, or a zone rule
now also setting the row); fix the side that is wrong; the Tier 2 test stays as is.
Depends: none. Size: S-M.
