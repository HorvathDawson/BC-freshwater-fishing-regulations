# T879 — "What you can keep": two headers, one table (salmon in the fish list, each row says whose rule)

Status: OPEN — proposal; lands only on the owner's yes. Pictures taken 2026-11-18 ~21:05 MST; lane
`app/t879-two-headers-one-table` (from `app/integrate-081-web`), not merged, not pushed.

Owner 2026-11-18 ~20:53, with a shot of Babine Lake — Main Body
(/Users/dawson.horvath/.claude/uploads/b8ddfca9-44eb-400e-a9bf-bd89b4b0f359/a6d620fa-image.png), verbatim: "I feel like
we could order this better to allow salmon to be in the table but still have the two timeline calendars and big words
easy to read and make sense/know it is different to between them and easily know which is which".
In the main chat's dialog he picked "Two headers, one table (Recommended)": both status blocks first (SALMON · DFO: the big
"✕ Closed · Opens Aug 1" and its own year line; then GAME FISH · B.C.: the big "✓ Open" and its own year line), each
clearly labelled with whose rules it is; then ONE fish list (Salmon, Trout and char, Kokanee, Whitefish…), each row tagged
DFO / B.C. Declined: the two big words side by side in two columns; one list split in two parts each with its own header.

## Today (before)
SALMON (DFO) block (word, year bar, legend) with its own Salmon row under it, a heavy rule, then GAME FISH (B.C. RULES)
(word, year bar, timed lines) with the trout fold, the quota groups and the other fish under it, no list heading.

## Plan (built in this lane)
1. Order: the leading blocks (closure caveats, banner) as today → **SALMON [DFO]** block (big word, when, "Opens …", year
   bar, legend; no rows) → a thin line2 rule → **GAME FISH [B.C.]** block (big word, year bar, daylight hours, timed
   lines; no rows) → the heavy ink rule → **"Species and limits"**, ONE list: Salmon first, then Trout and char, the
   quota groups, the other fish, the status key; then Possession, Where you can't fish, as today.
2. Salmon rows: the Salmon fold is the list's first row (folded, as today: value word, the issue "!" mark, the T796
   rule-book lines under a plain closed row). Opening it shows the whole salmon card in place, unchanged.
3. The tag: `SourceTag` — the scope kicker's type ("REGION 2-WIDE"), `palette.sub` text in a 1 px `palette.line2` square
   outline, no status colour. The block name wears it ("SALMON [DFO]", "GAME FISH [B.C.]") and so does every row, with the
   SAME word as its block: the Salmon row wears "DFO + RULE BOOK" / "RULE BOOK" where the salmon bar is named so
   (`SalmonSourceBy`), the B.C. rows "B.C.". Words are core's (`SOURCE_WORDS`, core/src/glance.ts).
4. The salmon card's extras — the daylight rule (`hours`), the gear lead ("B.C. rules, also for salmon …"), the closed
   group and eulachon closure line, the T870 Source (DFO) fold — stay inside the Salmon row's fold, where they are
   today, so the two header blocks stay just the big words and the bars, and T870's fold work lands unchanged.
5. A page with no DFO block draws no tags and keeps "Game fish (B.C. rules)". In practice every water has a salmon card
   since T820 (an unlisted water draws the full closed bar), so the "no salmon" case is a salmon-closed-all-year water
   (Charlie Lake, Peace region).

No contract change: `GlanceRowVM.source` (T531) already says whose each row is. Core gains only `SOURCE_WORDS` and
`SalmonStripVM.by` (the card's `year.by`, already in the salmon VM). The T810 trout contract and the DFO feed are untouched.

## Pictures (390 dark, the whole tab; scratchpad `t879/`)
before/after: Babine Lake — Main Body, Coquitlam River, Mamquam River, Charlie Lake; after only: Mamquam with the Salmon
row open, Babine at 1280.

## Left (Claude), on the owner's yes
- Tests: KeepTab (T531 block placement), SalmonCard, a11y, palette/style-token and walkthrough (CoachSpot "regs:keep",
  "regs:salmon") pins move with the order; `speciesBlocks` becomes unused (knip) — delete it with its test.
- `SALMON_WORDS.barSalmon` / `barGeneral` stay for the untagged case only; check the wording tests.
- Native check on the emulator (the tag's outline, a long name wrapping its tag under it at 200 % text).
- Gear tab's "For salmon (DFO)" heads could wear the same tag (not in this proposal).
