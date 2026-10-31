# T327 — Lock names: owner name decisions out of code into a curated file

Status: OPEN (owner 2026-10-30; T324 design approved, plan in handoff/PLAN/data-flow.md §4–5).

Move `names.OWNER` (pipeline/regs/review/names.py) into `waters/mined_names.json` (or its own curated file);
write Sheldens Creek (accepted, never written). Code reads the file only. Depends: T325. Size: S.
