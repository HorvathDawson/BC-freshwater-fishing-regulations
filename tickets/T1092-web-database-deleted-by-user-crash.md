# T1092 — Web: "Database deleted by request of the user" arrives as a crash report

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1092 | Web: "Database deleted by request of the user" arrives as a crash report (kind `790ae7cc172f9424`) | OPEN — 0.10.1 | | planning chat 2026-11-24 |

Source: production feeds worker, crash kind `790ae7cc172f9424`, 1 report, 2026-11-25 01:24Z, app 0.8.5 (web, by the
message: it is the browser's IndexedDB error when the site's stored data is cleared while a tab is open). Read by
`pipeline reports`, planning chat 2026-11-24 22:20 MST.

## Work
1. Read the kind's stack from the worker (`GET /crashes`, read-only): which store was open (settings, saved spots,
   the data cache) and what the page did next.
2. The page must survive it: the open store is reopened or the page says plainly that its stored data was cleared
   and reloads once. Nothing is lost silently: saved spots that were cleared are gone by the user's own act, and the
   page must not show stale ones from memory as if saved.
3. Class the kind `expected` in the worker's crash classes (the user's own act), with the reason, so it stops
   counting as a defect; a shown failure still reports (`no-silent-fallbacks`).

## Acceptance
A test deletes the database under an open page: no unhandled rejection, the page states what happened, and the next
write works. The kind is cited here and classed. Fails without the change.

Severity: low (one report; the user cleared the data). Release: 0.9.2. Model: Sonnet. Size: S. Owner: no.
