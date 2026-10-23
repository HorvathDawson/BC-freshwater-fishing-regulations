# T102 — The receiving river is never walked into (parent-code check in the walk, not only in seeding)

Status: OPEN — raised 2026-10-22 from the owner's question on T90 ("I thought the tributary logic handled the Harrison,
using the WSC").

## Why
- The tributary walk (pipeline/atlas/graph/tributaries.py) keeps the receiving river out by STREAM ORDER
  (`_breaks_strahler`: "a bigger river cannot be a tributary"); the WSC there only OVERRIDES that in favour of a genuine
  tributary (line ~729). Nothing in the walk says "a water whose code is a strict ancestor of the reach's code is the
  river the reach drains into — never enter it".
- The receiving river's SIDE CHANNELS carry its code at a small order, so they pass the order test. T90 (04d8fb85)
  closed the one way they got in (mouth seeding) with `THE_RECEIVING_RIVER_IS_NEVER_A_SEED`, but "only seeding asks
  this; the walk itself is untouched".
- A braid that rejoins the receiving river can still let the walk climb into it the same way.

## Work
- Apply the same strict-ancestor test (`_receives`) to every step of the walk, not only to seeds; respect the FWA
  code-run exceptions (`code_runs`, the Dewar Lake ruling: follow the code).
- Test-first: a fixture where a reach's walk reaches a side channel of its receiving river through a braid; mutation
  pin (switch off → the channel is bound).
- Reach rerun into a side dir + per-rule binding diff: expect no change today (prove it), or list every rule the
  check now stops, with names, for review before promoting.

## Acceptance
Binding diff reviewed; 0 sections of a receiving river (by code) bound by any tributary walk; tests pinned.
Rebuild: reach + deliver, no atlas.
