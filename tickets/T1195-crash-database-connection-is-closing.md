# T1195 — Crash 3457ab5cac8b9cc7: InvalidStateError "The database connection is closing" (0.9.0)

Status: OPEN — scope to confirm (read the stack first).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1195 | Crash 3457ab5cac8b9cc7: InvalidStateError "The database connection is closing" (0.9.0) | OPEN — scope to confirm | — | read the kind's stack and platform from the worker; then fix or class it |

Source: production feeds worker, crash kind `3457ab5cac8b9cc7`, app 0.9.0, first read by `pipeline reports --since
2026-11-25` (planning chat, 2026-11-26). The repo clean chat found it with no ticket (feedback log F53). Same family as
T1092 (`Database deleted by request of the user`, an IndexedDB error that arrived as a crash) and the old staging kind
`database connection is closing` that T1057 item 1 still has to class.

## Work
1. Read the kind's stack and platform from the worker (`GET /crashes`, read-only): which store was open (settings,
   saved spots, the release cache), and whether it is the web app or a phone.
2. If a store closes under a pending read or write (the browser or the system closed the connection, a tab or a
   version change), handle it the way T1092 does: reopen once, or show the failure; no silent fallback.
3. If it is browser noise with nothing the app can do, class it `expected` in the worker's kinds and say why here.
4. Cite the id in T1057's table.

## Acceptance
The kind has a stack read, and either a test that fails without the fix or a class with the reason written here.
`pipeline reports` no longer lists it as NEW.

Severity: low-medium (one report; the app's stored data may fail to save). Model: Sonnet. Size: S. Owner: no.
