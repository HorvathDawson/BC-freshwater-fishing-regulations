# T1092 — Web: "Database deleted by request of the user" arrives as a crash report

Status: PARTIAL — proof: kind `790ae7cc172f9424` stops arriving from web builds that carry the fix; left (Claude): a real-browser clear of site data under an open tab.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1092 | Web: "Database deleted by request of the user" arrives as a crash report (kind `790ae7cc172f9424`) | PARTIAL — fix built, not yet merged or seen in the field | (this branch) | proof still to come: kind 790ae7cc172f9424 not arriving from builds with the fix; no real-browser clear was run |

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
- `spots/idbCleared.ts` `keptConnection`: one kept connection per store that listens for the browser's `close` event (the
  idle case: a later `transaction()` throws InvalidStateError, retried once on a fresh connection), drops the dead
  connection, revokes the photo pictures drawn from it, and says ONCE, as the environment's (shown, never reported):
  "Your browser cleared this site's saved data: the spots and photos kept in this browser are gone, and the app's settings
  are back to their defaults." (what clearing site data removes: localStorage = spots, notes, settings; IndexedDB = photos,
  the gauges' kept week; Cache Storage = the downloaded data copy, fetched again). `photoFailed` says that line whole.
- The line is seen where the next photo or gauge request meets it; the Spots list itself shows no line (left: a page-level
  notice, which needs a place for it: the owner's call).
- Worker: `EXPECTED` in `workers/feeds/src/crashes.ts` classes the old builds' kind (with a negative case).
- Left: a real-browser check (clear site data under an open tab); the stack of the kind was not read.
