# T82 — Regs leftovers from the T41 walk: the reader-gate run, the timed caveat box (O3), sheet height (O5)

Status: DONE — 2864cdc2 (O3 + O5), 7852275d (T138, the reader-gate finding). The reader gate PASSED 2026-10-23 (re-run
after T137, app be8b4693): light 390 px 39/39, dark 390 px 39/39, no confidence drop >= 2, every answer matches the
prototype reader's (reader/2026-10-23-app-rerun.md; first run reader/2026-10-23-app.md 38/39, fixed by T137). 1280 px:
not applicable (tests.md §4 fixes the gate at 390x844; no prototype 1280 reader record). Finding → T138 (DONE, 7852275d).
No deliver needed (app code and tools only; no data changes). T41 can close as DONE.

Source: T41-walk/REPORT.md — O3: x15p2 "At certain times" has no grey box; O5: sheets open taller on a phone than in
the prototype. `handoff/PLAN/app-integration/reader/` does not exist: the comprehension reader gate (tests.md §4) has
never run. Owner 2026-10-22 delegated the look calls ("the prototype is a reference, not the end-all … Claude decides").

## Work
- O3: draw the timed caveat in the same caveat box the other top caveats use (T15b).
- O5: measure sheet heights vs v38 (`app/tools/regs-heights.mjs`); size sheets to content up to the prototype's cap, or
  record why not.
- Record both calls in decisions.md.
- Reader gate (DONE 2026-10-23, see Status): the human launches compare-regs `--reader` (an agent run) at 390 px and 1280 px, light and dark; file
  the dated result in `reader/`; each finding becomes a fix or a ticket.

## Acceptance
O3/O5: before/after shots, a rhythm/height test pinning each value; census + style gates green. Reader report committed,
every finding has a commit or a ticket id. Then T41 → DONE.

Depends: T56 (fixtures re-cut). Size: S + a human run. Owner: launches the reader run. Default: after T56.

## As built (2026-10-23, lane-T82)

**O3 — the timed caveat look (2864cdc2).** No code change to the caveat: the app already drew every top caveat (timed,
side, angler, check) in the one `CaveatBlock` box (3 px restricted rule on the left, sub-grey label, items as rule
buttons, the where line in sub grey). The prototype's grey box around x15p2's hours is not a design: it is v38's
location-bar rule `.where{display:flex;min-height:56px;padding:8px 16px;background:var(--wash);border-bottom:…}`
(regs-v11.html l.408; no v38 page draws that bar) leaking into the caveat's `<span class="where">`, whose own rule
`.caveats .where` sets only font-size and colour. Measured in the staged prototype (computed style): x15p2's timed item
and x14p2's "On one side of the channel only" item both get the 56 px wash box; angler items have no where line.
Recorded in decisions.md ("CLAUDE 2026-10-23 (T82, Q12 carried out)").
- Pinned: `app/packages/ui-native/src/regs/Edges.test.tsx` "O3" — (1) the whole style (border, padding, margin,
  background, colour, size, line) of block, kicker, item and where line is equal across timed / side / angler /
  uncertain; (2) the where line is sub grey with the block's (no) fill, no padding, no min height. Mutations run: a
  `kind === "timed"` wash + padding on the where line fails both.
- Shots: `T82-shots/o3-after-x15p2-keep-light.jpg`, `o3-after-x14p2-keep-light.jpg` (before: T41-walk
  `shots/x15p2_1010-keep-light.jpg`; the app column is unchanged).

**O5 — sheet heights vs v38 (2864cdc2).** `node app/tools/regs-heights.mjs --sheets --url … --states …` now opens every
golden sheet of a state in both (prototype `#sheet.on`, app `sheet-panel`) and prints sheet / chrome / body / capped per
sheet, then `sheetSummary` (`app/tools/regs-web/sheets.mjs`, tested in `sheets.test.ts`).
- BEFORE (vedder_1010, 65 sheets, 60 comparable): median app − prototype sheet height **0 px**, 26 taller; chrome
  +25 px median, bodies −26 px; tallest app sheet 726 px vs the prototype's cap 743 (88 % of 844).
- Cause of "taller": not the cap (the app's body cap, 0.68 of the window, keeps it ≤ ≈ 666 px with plain chrome, under
  v38's 743) but the chrome — the sheet title was 19 px on TYPE.screen's 27 px line (4 px of air per line; most rule
  titles wrap), plus the 28 px foot under the body (kept: the stand-in for the home-indicator area the Modal does not
  inset).
- Change: `SHEET_TITLE` = screen face 19 / 23 in both Sheet branches (the app's other 19 px heading is 19 / 23);
  `SHEET_BODY_CAP` = 0.68 named, with why it stays under the prototype's cap (0.82 covered the water's header, T37).
- AFTER (vedder_1010 + klwa_1010 + bc1_501, 192 sheets, 177 comparable): median app sheet **6.9 px shorter** than
  v38, 69 taller (rule sheets median +15 px: a 19 px title wraps where v38's 17 px does not, and the sub line), chrome
  +21 px, bodies −26 px; both cap the same 19 sheets; tallest app sheet 722 px. Every Vedder sheet moved 4, 8 or 12 px
  shorter. On screen the app's sheet still reaches higher up the REGS page than v38's at the same height, because the
  app's window also holds the water frame above the regs header (G1, deliberate).
- Pinned: `app/packages/ui-native/src/SheetSize.test.tsx` — the title 19 / 23 as drawn (mutation: spreading
  TYPE.screen alone, 27 px, fails); body cap 0.68 and cap × 844 + the tallest measured chrome (158 px,
  rule:zp:conduct::conduct.r6) < 88 % of 844 (mutation: 0.82 fails).
- Shots: `T82-shots/o5-before-vedder-rule-r2-light.jpg`, `o5-after-vedder-rule-r2-light.jpg`.

**Checks.** Strict app check (`REGS_STORE_REQUIRED=1 pnpm -C app check` via heavy.sh): 336 files / 8,031 tests passed,
23 skipped, exit 0. Targeted: ui-native regs + Sheet + tools/regs-web 720/720. The style gates (check-style, check-colours,
r20c style tokens, palette census render tests) are inside that check, green.

## Left / follow-ups
- `app/tools/regs-census.mjs` (the browser palette census) fails before measuring anything: "no Search tab to tap" — it
  drives the old bottom tab bar. Its rules run green in the render census (`palette.render.test.tsx`); the browser
  tool needs its navigation updated (new ticket).
- Rule sheets stay ~15 px taller than v38 at the median: the one Sheet's 19 px title and its sub line under the rule.
  Changing those is a Sheet-wide look (map sheets too; T41 Q4 "one Sheet style"), not done here.
- Licence person sheets (`licsrc:*`, 15 of 192) have no prototype sheet to compare (v38 opens none by those keys).
