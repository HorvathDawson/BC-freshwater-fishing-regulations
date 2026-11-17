# T835 — the step cache reuses stale outputs in a worktree under /tmp

Status: OPEN (main chat; pipeline tooling).

Source: the T834 worker, 2026-11-17. `pipeline/_trace.py` `install()` skips every path under `/private/tmp` and `/tmp`
(the system-path skip list). A lane worktree in the session scratchpad lives there, so a deliver step records no
modules or reads, and the step cache then reuses old outputs whatever the code says: the worker's second side build
silently reused a stale bundle. Workaround today: `--no-step-cache` in scratch worktrees.

Fix: skip system paths by what they are (interpreter, site-packages, OS dirs), not by a /tmp prefix, so a repo checkout
anywhere is traced; or have the step cache refuse to reuse when the trace recorded no repo modules. Pin with a test that
runs a step from a checkout under a tmp dir, changes a module, and sees the step rerun. No silent fallback.
