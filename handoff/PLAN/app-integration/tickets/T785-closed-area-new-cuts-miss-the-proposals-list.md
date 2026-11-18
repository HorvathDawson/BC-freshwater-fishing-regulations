# T785 — Review app: a confirmed closed area's new cuts do not reach the proposals list until a later save or a restart

Status: DONE db33d2980 (lane/bugs-080 2026-11-17: marks_delta counts a closed area's piece.new_splits; test + mutation)

Source: general audit 2026-11-16, code checker finding 3 (`handoff/PLAN/audit-2026-11-16/code.md`); two lanes 30 minutes apart (3920760a4 against fe11e0af7).

## What is wrong
`marks_delta` looks at a mark's top-level `new_splits`; a closed-area mark carries its new cuts in `piece.new_splits`.
Reproduced on a temp copy of the marks store: proposal groups go 1215 to 1216 and the stamp does not move. All five
closed-area marks have this shape. The owner confirms a closed area and its cuts are not offered for the rebuild.

## What to build
`marks_delta` and the stamp read `piece.new_splits` too; a test with a closed-area mark that adds a cut.
