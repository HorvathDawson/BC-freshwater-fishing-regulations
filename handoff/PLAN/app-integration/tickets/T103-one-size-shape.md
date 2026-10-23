# T103 — One size shape: the plain words contradict the bands and the title

Status: OPEN.

Source: owner 2026-10-22 ("look for other spots where this same trap of multiple things that should be checking the
same thing failing to have a unified function … simplify by unifying the code checks and logic where possible");
unification audit 2026-10-22, finding 1 (CONFIRMED on `data/generated/regs/ui-rules-answers.json`).

## Decision being unified
"What size rule does this quota carry: a minimum, a floor, a ceiling, or a slot?"

## Where it is decided today
- `pipeline/deliver/answers/display.py:275-277` `_plain`: picks `mn`/`floor`/`ceil` with no open-end test and no slot.
- `pipeline/deliver/answers/display.py:391-394` `_title`: open-end tests plus `slot`.
- `pipeline/deliver/answers/display.py:129` `bands()`: the cut points both should derive from.

How they differ: 24 of the 52 "X or longer, none over Y" plain strings have Y < X, e.g. "Keep up to 1 lake trout a day,
90 cm (35 in) or longer, none over 60 cm" beside the title "At most 1 lake trout over 90 cm"; pike 100/70; grayling
44/36. A reader gets a size rule that cannot be met.

## Work
- `display.size_shape(lengths) -> {min, floor, ceil, slot}`, derived from `bands()`; `_plain` and `_title` both render
  from it and keep no size logic of their own.
- Gate: every bound named in `plain` and `title` is one of `bands()`' cut points, and no "or longer, none over" has the
  ceiling below the floor.
- Pin (one-* style): a grep/AST test that no other function in `pipeline/deliver/answers/` reads `lengths` bounds
  directly (only `size_shape` and `bands`).

## Acceptance
Proof is a reviewed behaviour change: the answers diff changes exactly the 24 `plain` strings and no `title` or
`bands`. The gate fails on a mutation that brings back the `_plain` pick; the pin fails on a re-inlined bounds read.

Rebuild: deliver. Depends: none. Size: S. Owner: no.
