# T946 — Tier 2 tributary lines: 20 parts whose "rule for its tributaries" line is not what their rail sets

Status: IN PROGRESS — fixed on lane/lc-reach-leftovers; left (Claude): main chat review + merge.

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

## Done (lane LC, 2026-11-20)
Neither side was wrong: the sweep's RAIL READER was. T919 a (30e052328, owner 2026-11-18) names a row whose own name already
says tributaries once — the kicker is "Lake Revelstoke's Tributaries", no longer "… Tributaries · its tributaries"
(`DECIDE.K_DOWN`). `conformance/src/regs/tributaryEvidence.ts` read a tributary step only by the suffix " · its
tributaries", so every such row's setting step read as "not a tributary one" and the row's (correct) line failed. The
lines, the rails and the release data were right: the rows' `down` is "Lake Revelstoke's Tributaries" and Python's line
"Lake Revelstoke’s rule for its tributaries" is `downLine` of it. The 20 listed were only the test's first 20.
- `downWater` reads `K_DOWN` back in both forms (a kicker with "tributar" and no " · " is the bare form; every other
  kicker is "<name> · <what>" or a fixed phrase); `tools/tributary-evidence.test.ts` round-trips `K_DOWN` (both forms) and
  refuses every other kicker, so the next wording change fails there, not in a Tier 2 sweep.
- The Tier 2 test is unchanged. On r-6258ffded676 (TIER2=1): green — 20,959 parts; lines on 708 Other fish rows, 171
  trout folds, 3,312 trout rows; proved partial 84 / 1,777 / 60.
