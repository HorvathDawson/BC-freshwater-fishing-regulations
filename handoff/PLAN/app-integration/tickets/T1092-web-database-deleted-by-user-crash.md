# T1092 — Web: "Database deleted by request of the user" arrives as a crash report

Status: IN REVIEW — fix built; proof: crash kind `790ae7cc172f9424` no longer arriving from web builds that carry it (the old kind stays classed `expected`).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1092 | Web: "Database deleted by request of the user" arrives as a crash report (kind `790ae7cc172f9424`) | IN REVIEW — kind 790ae7cc172f9424 stops arriving | (this branch) | proof is the kind not arriving; no real-browser clear was run |

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

## Built (hardening lane, 2026-11-26)
- Stack not read (no worker access from the lane). By code reading the open connections are the two IndexedDB stores
  (`gaugeHistory` write in `store.web.ts`, which reported it via `reportOnce`, and the photos in `photoStore.web.ts`).
- `spots/idbCleared.ts`: the browser's words become the environment's plain error ("Your browser cleared this site's saved
  data while the page was open. Try that again."): shown by the photo caller, never reported; both stores drop the dead
  connection so the next request reopens and works. The gauge week is a cache: nothing visible is lost.
- Worker: `EXPECTED` in `workers/feeds/src/crashes.ts` classes the old builds' kind.
- Left: a real-browser check (clear site data under an open tab); the stack of the kind was not read.
