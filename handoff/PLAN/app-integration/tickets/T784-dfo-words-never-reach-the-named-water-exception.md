# T784 — "Unless DFO's words name it" cannot fire: the builder never sees the DFO place's words (a lake place's through-river, a joining water)

Status: DONE 0639e2309 (lane/bugs-080 2026-11-17: to_reach_input passes specific_area as extent_text; binding diff empty (9 diagnostics re-asked))

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T784 | "Unless DFO's words name it" cannot fire: the builder never sees the DFO place's words (a lake place's through-river, a joining water) | DONE | 0639e2309 | general audit 2026-11-16, code checker finding 2 (`handoff/PLAN/audit-2026-11-16/code.md`); commit eafa7b9f5 |

Source: general audit 2026-11-16, code checker finding 2 (`handoff/PLAN/audit-2026-11-16/code.md`); commit eafa7b9f5.

## What is wrong
The reach builder keeps a lake place's through-river (and a joining water) out "unless the words name it", reading
`verbatim` / `extent_text`. `to_reach_input` passes neither for a DFO place, so the exception can never apply. The
test for it hand-builds a rule with `verbatim=`, which production never builds. Today no place's words name a kept-out
water (2 through-river and 7 confluence diagnostics checked), so no binding is wrong.

## What to build
- `to_reach_input` passes the place's `specific_area` as `extent_text`.
- The test drives the exception through `to_reach_input`, not a hand-built rule.
- Binding diff: expected empty today.
