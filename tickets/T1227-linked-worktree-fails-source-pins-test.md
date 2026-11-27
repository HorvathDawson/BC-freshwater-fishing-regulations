# T1227 — A source-pins test fails in any worktree whose `data/generated/regs` is a link to the main checkout

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1227 | A source-pins test fails in a worktree that links `data/generated/regs` to the main checkout | OPEN | — | compare the configured path, not the resolved one; name the setup command when a link is undocumented |

Source: the data and release chat, 2026-11-26. A lane's pre-PR check failed for a reason that was not its branch.

## What was found
- `pipeline/tests/test_source_pins.py::test_a_generated_key_lives_under_data_generated_and_round_trips` (line 12)
  calls `P.key_of(...)` on `data/generated/regs/extraction/row_boxes.json`.
- `key_of` resolves the path (`pipeline/tools/source_pins.py:54`) and checks it against the resolved
  `GENERATED_ROOT` (`:55`). With `data/generated/regs` a link to the main checkout, the resolved file lands outside
  the worktree's `data/generated`, so it raises "under neither ..." (`:58`).

## Work
Lean (the default, owner: no): 
1. Fold the link into T1112's one worktree-setup command, so a lane never makes it by hand.
2. `key_of` compares the configured path (made absolute, not resolved) against the configured roots; a link inside
   `data/generated` is then fine.
3. A link that no setup command made fails with a message naming that command (T1112), not "under neither".

## Acceptance
In a worktree with `data/generated/regs` linked, the test passes; with an unrelated path it still raises. A test
for the message.

Depends: T1112 (the setup command). Size: S. Owner: no. Default: the lean above.
