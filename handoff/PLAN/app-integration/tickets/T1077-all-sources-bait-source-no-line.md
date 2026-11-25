# T1077 — "All sources" lists the bait rules' source, but the bait list on the page draws no Source line

Status: OPEN.

Source: the 0.9.1 chat, 2026-11-24, PR #126 (T1036): group 1 of the sheet counts the bait Source line's rules (`srcs:bait`) in `app/packages/core/src/regs/sheets/allsources.ts`, but BaitList draws no Source line, so the sheet lists a source the page gives no way to open.

## Work
1. BaitList draws its Source line like the other gear rows (the sheet and the screen then agree, which is T1036's rule), or the sheet stops counting it; the first is the default.
2. A test that every source the sheet lists has a drawn Source line on the page.

## Acceptance
the test fails without the change; before/after picture.

Severity: low. Release: 0.9.2 (0.9.1 only if PR #126's lane takes it in the same change). Model: Sonnet. Size: S. Owner: no.
