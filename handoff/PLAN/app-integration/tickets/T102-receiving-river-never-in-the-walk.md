# T102 — The receiving river is never walked into (parent-code check in the walk, not only in seeding)

Status: PARTIAL — part A done (95e78dd5: `THE_WALK_NEVER_ENTERS_A_PARENT` + one `_refusal` for walk and seeds; seed-only check removed; POLICY_VERSION 23) — reach rerun binding diff vs reaches/full EMPTY (0 rules, 0 unresolved, 0 licensing); REVIEWED (b44461d5: code_runs now takes the parent rule too; rerun digest identical, diff EMPTY); open: owner call on order guard for equal codes; part B open. Raised 2026-10-22 from the owner's question on T90 ("I thought the tributary logic handled the Harrison,
using the WSC").

## Why
- The tributary walk (pipeline/atlas/graph/tributaries.py) keeps the receiving river out by STREAM ORDER
  (`_breaks_strahler`: "a bigger river cannot be a tributary"); the WSC there only OVERRIDES that in favour of a genuine
  tributary (line ~729). Nothing in the walk says "a water whose code is a strict ancestor of the reach's code is the
  river the reach drains into — never enter it".
- The receiving river's SIDE CHANNELS carry its code at a small order, so they pass the order test. T90 (ca9aa451)
  closed the one way they got in (mouth seeding) with `THE_RECEIVING_RIVER_IS_NEVER_A_SEED`, but "only seeding asks
  this; the walk itself is untouched".
- A braid that rejoins the receiving river can still let the walk climb into it the same way.

## Owner decision (2026-10-22)
"Shouldn't we only use the wsc check parent/child to handle it during the walk? no walking into a parent? to keep it
consistent and then remove your added check?" — Yes:
- ONE rule, in the walk: never step into a water whose watershed code is a strict ancestor of the reach's code. A seed is
  the walk's first step, so the same check covers seeding.
- REMOVE `THE_RECEIVING_RIVER_IS_NEVER_A_SEED` (T90's seeding-only check) — superseded.
- WSC decides parent/child wherever both codes are known; the Strahler/order guard stays only as the fallback where a
  code is missing (out of B.C., added streams with no code).
- KEEP `MOUTH_SEEDS_ONLY_AT_THE_CUT`: it fixes a different bug (at a natural mouth the old seeding matched SIBLING
  waters joining the receiving river at the same measure; a parent check does not stop siblings).
- Respect `code_runs` (Dewar Lake ruling: follow the code).

## Work
- Implement the rule above test-first; fixtures: a braid into the receiving river's side channel (blocked), a genuine
  tributary whose order looks too big (allowed by code), a no-code water (order fallback), the Chehalis/Harrison case
  (still blocked with the seed check removed). Mutation pins: drop the parent check → the side channel binds; put the
  order guard first again → the code-wins case fails.
- Reach rerun into a side dir + per-rule binding diff; list every rule that moves, with names and why, before promoting.

## Part B — one entry check, used everywhere (owner 2026-10-22)
"Then we could make the check logic (with the edge type check etc) a single function used in multiple spots."
- After part A lands: gather every acceptance rule the walk and the seeding apply — parent code (WSC), edge type,
  the order fallback, the entry's own water, barriers, lakes, code_runs, within-area — into ONE function
  (e.g. `may_enter(src, dst, reach) -> reason | None`) in tributaries.py, and call it at every step of the walk, at
  every seed, and from any other place that re-implements a subset (find them: the T57 sweep tool's checks, panels'
  tributary-ish walks, reach extent code). Each refusal returns a named reason, so diagnostics read one vocabulary.
- Pure refactor: the reach binding diff against part A's output must be EMPTY (proof of no behaviour change). A test
  asserts no other module re-implements the rules (a grep pin, like the one-pathway checks).

## Acceptance
Binding diff reviewed; 0 sections of a receiving river (by code) bound by any tributary walk; tests pinned.
Rebuild: reach + deliver, no atlas.
