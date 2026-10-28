# T103 — One size shape: the plain words contradict the bands and the title

Status: DONE — e7dfba0a. Answers diff: exactly 24 `plain` strings (Bennett 6, Morley/Tagish/Teslin 18),
no `title`, no `bands`; card gate 0 problems. Rebuild: deliver. Left: owner question below.

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

## Owner questions

- **A no-keep slot reads "none over 60 cm (24 in), under 90 cm (35 in)"** (`band_words`, RULINGS P37) where the book
  prints "none between 60 cm and 90 cm"; the comma can read as two clauses ("none over 60", "under 90"). The title has
  the same words. Default: keep P37's words (no change); say "between" only on the owner's call.
- **"Keep only lake trout, none over …"** (take-less size rule, 18 Morley/Tagish/Teslin plains) follows the existing
  house style ("Keep only trout and char, none over 50 cm", 40+ plains) but can read as "only lake trout may be kept".
  Default: unchanged.

## Owner answers (2026-10-28, Q62 + Q63)
- Q62: a slot uses the book's "between", in the must-be / release style. `common.band_words` says a band holding neither
  end "between 60 cm (24 in) and 90 cm (35 in)" (table bands, caps); `_plain` "Release any lake trout between 60 cm (24 in)
  and 90 cm (35 in)." / "Only 1 lake trout over 90 cm (35 in) a day; release any between …"; title "Release lake trout
  between …"; the tree's `size_rule` chip for a pure slot "release any between …". RULINGS P37 updated.
- Q63: a size rule with no count says the fish first — "Lake trout must be 50 cm (20 in) or less; release any longer.",
  "Trout must be 25 cm (10 in) or longer; release any shorter.", "Wild rainbow trout must be …" — never "Keep only …".
  The kept band's words hold their ends as the book does ("No cutthroat trout 40 cm or more" → "must be under 40 cm").
  ("or less", not the owner's example "or shorter": the one P37 word, the same as the tree's chip.)
  Gate: `size_problems` also refuses a "must be X–Y cm" with Y ≤ X. Tests: `test_one_size_shape.py`,
  `test_answers_r19_tree.py`, `test_answers_r20i.py::test_a_slot_says_the_books_between`.
