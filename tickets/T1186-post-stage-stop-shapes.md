# T1186 — A recorder miss stops the build after the stage ran, and throws the run away

Status: OPEN — a #286 follow-up; scoped 2026-11-26: real on main

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1186 | A recorder miss stops the build after the stage ran, and throws the run away | OPEN — a #286 follow-up; scoped 2026-11-26: real on main | — | a lane: keep the finished stage's record (key None, the reason) as the moved-while-running case does; the release refuses an unkeyed stage; a test per shape |

Source: PR #286's re-check note: "Follow-up, LOW (not fixed): a stage can still stop the build AFTER it ran, before the manifest is saved, in the remaining
shapes: config.yaml read with no value recorded, or an empty package / config set (a recorder miss). Each is a real recorder bug, so it stays loud, but every
build repeats it until fixed." The third review's M1 (an unowned module) was the first such shape; #286 fixed that one.

## The shapes (on main, 1e2dc8a3b)
`run_stage` (`pipeline/build.py:813-815`) runs the stage, then `_traced_key`. Before the record is written (L836-839) these stop the build:
1. `Untraceable`: config.yaml read with no value recorded (`stepcache.py:336-338`) -> `SystemExit` at `build.py:851-852`.
2. `Untraceable`: a run of repo code that recorded no package or no config value (`stepcache.py:340-341`) -> the same `SystemExit`.
3. No trace file at all (`stepcache.Trace`, `stepcache.py:283-285`, `FileNotFoundError`): not caught, a traceback after the stage finished.
In each, `run["stages"]` gets no row and the manifest keeps the old key, so the next build reruns the whole stage (reach 12 min, tiles 15-18, deliver 15)
and stops again. Not shapes: a stage edited while it ran (L853-854 keeps the run with key None and goes on); the atlas (`code_stamp` records `unrecorded`, the
release refuses); the tiles preflight (runs before the stage).

## Proof
In `test_build_key_holes.py`'s sandbox: a stage command that opens config.yaml with a bare `open` (shape 1), or a recorder that writes no P lines (shape 2),
or `PIPELINE_TRACE` unset in the child (shape 3); rc 0, then the build stops and the manifest holds no record of the stage.

## Fix (one sentence)
Treat all three as the moved-while-running case: write the stage's record with key None and `unkeyed: <why>`, print it loudly, let the build finish, and have
`pipeline release` refuse a manifest with an unkeyed stage (a shown failure, never a silent one).

## Acceptance
One test per shape: the stage's record is kept with the reason, the next plan says why it reruns, the release refuses. Severity: low-medium (a recorder bug costs
every build its slowest stage until fixed). Model: Opus. Size: S.
