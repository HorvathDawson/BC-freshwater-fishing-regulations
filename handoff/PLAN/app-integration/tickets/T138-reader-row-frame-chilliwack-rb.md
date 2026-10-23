# T138 — Reader shooter: the chilliwack-p2-row-rb frame does not land on the rainbow row

Status: OPEN — source: reader gate re-run 2026-10-23 (handoff/PLAN/app-integration/reader/2026-10-23-app-rerun.md), F7.

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
