# T988 — The Sources menu's Done button doesn't close the menu; check every menu/sheet like it

Status: OPEN — joins the 0.8.5 batch (owner 2026-11-21).

Source: owner 2026-11-21: "the done button in sources menu doesnt close menu check for other menus like this".

## Work
- Reproduce: Sources menu, tap Done: the menu stays open. Fix at the cause.
- Audit every menu, sheet and dialog with a Done / Close / OK / Cancel button (and the scrim / back gesture): each one
  closes. Same on Android, iPhone and web (AGENTS 34a: a platform fix goes in that platform's file).
- Pin with one test per closing control (or one table-driven test over all of them).

## Acceptance
Every Done/Close button closes its menu on every platform; the test fails if one stops closing.
