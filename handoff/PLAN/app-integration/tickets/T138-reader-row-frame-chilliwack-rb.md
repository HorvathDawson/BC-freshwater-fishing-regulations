# T138 — Reader shooter: the chilliwack-p2-row-rb frame does not land on the rainbow row

Status: DONE — d8997fbd. No deliver needed (a test tool + two testIDs). No reader re-run needed (the gate passed).

## Today
`app/tools/compare-regs.mjs` `frames()` row mode is meant to give "the one screenful with that fish's row just under
the pin bar". For `chilliwack-p2-row-rb` (state chw2_1010, row RB) the PNG starts at the box head "Of these, only 1 may
be over 50 cm (20 in) — 2 if one is a hatchery steelhead" and ends inside the steelhead row. The "Up to 2 may be
hatchery rainbow trout · keep 30–50 cm" row is below the fold. The light and dark PNGs both show this, and the light one
is byte-identical to the first run's. The prototype frame (r20h rows-after/chilliwack-p2-row-rb.png) shows the
steelhead row and then the rainbow row just under the pin bar. Prompt-gate39 F7 ("2 or 4?") is written for that screen.
The light reader wrote "This screen does not actually show a hatchery rainbow row; it ends at the steelhead row"
(conf 5 -> 4; dark 4). The answer was still right, so the gate passed.

Likely cause: the scroll target is the top of the size box, not the RB row. Either `querySelector('[data-testid="pic:RB"]')`
matches the steelhead row's picture first, or the 3-parent climb toward `keep-group` stops at the nested box wrapper.

## Work
- Row mode scrolls to the row whose picture strip carries the named fish as its own kind (on Chilliwack p2, the hatchery
  rainbow row, not the steelhead row), and puts that row's block just under the pin bar, as frames.mjs did.
- Check the other row frames (vedder-oct10-row-dv, bennett-row-dv, bennett-row-lt, glenn-jun15-row-rb,
  bella-coola-may1-row-ct) against the prototype's rows-after/ frames for the same rule.
- Re-shoot FRAMES (light + dark).

## Acceptance
- FRAMES/chilliwack-p2-row-rb.png shows "Up to 2 may be hatchery rainbow trout" with its 30–50 cm chip on screen, under
  the pin bar. The other row frames show their named row.
- A test on the row-target rule: given the page's blocks, it picks the RB row and not the steelhead row.
- No reader re-run needed for this alone (the gate passed). The next gate run uses the fixed frames.

Depends: none (T41's tool). Size: S. Owner: Claude.

## As built (2026-10-23, lane-T82, d8997fbd)

- Cause: the 3-parent climb. On Chilliwack p2 the RB picture's chain is strip → kind row → the group's body (the RB row
  sits BESIDE the size box, not in it), so the climb stopped on the body, and the frame opened on the box head. The
  steelhead row's climb landed on the size box itself. `querySelector('[data-testid="pic:RB"]')` was right (one RB
  picture on the page).
- Rule (one function, `app/tools/regs-web/rows.mjs`): `rowOf(root, code)` = the first `krow` (a kind / release /
  not-kept row) whose own pictures include `pic:<code>`; `blockOf(row)` = the tier head (`khead`) when it directly
  precedes the row (the row opens its tier), else the row. `rowTopExpr` runs the same two functions in the page.
  GroupCard gains `testID="krow"` on Kind / Release / NotKept rows and `testID="khead"` on the Tier head.
- `compare-regs.mjs` frames() row mode: scrolls so that block sits 8 px under the pin bar's foot, re-measured in up to
  4 steps (the bar's height follows the scroll; converges in 2).
- The head matters for Glenn: its rainbow row opens the Duncan River's tier, whose head says "Its row covers its
  tributaries, so it applies here" (reader F4). Row-only (a first cut) put that line under the pin bar; with `blockOf`
  it is on screen, as in the gate's frame.
- Test: `app/packages/ui-native/src/regs/rowFrame.test.tsx` over the frozen Vedder (Chilliwack p2's group: size box
  with DV and ST rows, the rainbow row under its own head beside the box) and Glenn Jun 15 pages. Mutations run: the
  old climb fails "RB" and "ST"; dropping `krow` fails the three row cases; dropping `khead` fails the two head cases;
  `blockOf` returning any previous sibling fails "ST".
- Re-shot (light + dark, `--reader`, own Expo web :39311 against tiles :39217): every row frame shows its named row
  under the pin bar — chilliwack-p2-row-rb ("Of your 2 trout and char a day in streams, up to 2 may be hatchery rainbow
  trout", chip "keep 30–50 cm (12–20 in); release smaller ones"), vedder-oct10-row-dv, bennett-row-dv (with "No more
  than 3 counting any lake trout you keep"), bennett-row-lt (opened by "This lake's rule: up to 2 lake trout a day"),
  glenn-jun15-row-rb (the Duncan River head + "Its row covers its tributaries"), bella-coola-may1-row-ct.
  Shots: `T82-shots/t138-before-chilliwack-p2-row-rb-light.jpg`, `t138-after-chilliwack-p2-row-rb-{light,dark}.jpg`,
  `t138-after-row-frames-{light,dark}.jpg` (all six).
- Checks: strict app check green (with T82, 8,031 passed).
