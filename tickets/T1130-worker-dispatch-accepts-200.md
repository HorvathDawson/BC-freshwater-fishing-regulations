# T1130 — The feeds worker counts only HTTP 204 as "workflow started"

Status: OPEN — 0.10.1

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1130 | The feeds worker counts only HTTP 204 as "workflow started" | OPEN — 0.10.1 | | repo-move chat 2026-11-25 |

Source: the repo-move chat's read of the worker's token path, 2026-11-25. Not checked by the planning chat. Related:
T1041 (the move re-issues the token this call uses), T1066.

`app/workers/feeds/src/dispatch.ts` (about line 95) treats only a 204 answer from GitHub's workflow-dispatch call as
a start. GitHub's docs now also describe a 200 answer (with run details, when `return_run_details` is asked for;
changelog 2026-02-19); the default is still 204. If the default ever changes, every start (HYDAT, run timing) would
read as failed and be reported as a defect while the workflows in fact run.

Work: accept 200 as well as 204 (any other status stays a reported failure, `no-silent-fallbacks`); a test per
status. Optional, say in the PR whether it is worth it: ask for the run details and log the run id.

Severity: low (nothing is wrong today). Release: 0.10.1. Model: Sonnet. Size: S. Owner: no.
