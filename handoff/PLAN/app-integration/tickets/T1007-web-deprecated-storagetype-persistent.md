# T1007 — Website: Chrome warns "`StorageType.persistent` is deprecated" (first party)

Status: OPEN — next release batch.

Source: owner 2026-11-22 (Chrome DevTools / Lighthouse on https://canifishthis.ca), verbatim: "`StorageType.persistent`
is deprecated. Please use standardized `navigator.storage` instead." Source column: "canifishthis.ca 1st party".

## Found so far (2026-11-22)
- None of our code names the old API (`git grep` over `app/packages`, `app/apps` for `webkitPersistentStorage`,
  `webkitStorageInfo`, `StorageType`, `requestQuota`: no hit).
- Neither of the two served bundles names it either (`/_expo/static/js/web/index-76f27ada….js`, 3.86 MB, and
  `later-82c4390b….js`, 347 KB, searched as fetched).
- So it comes from code loaded another way: a worker (MapLibre's, the SQLite / wasm loader), a dependency that probes
  `navigator` / `window.PERSISTENT`, or a browser extension that Chrome attributes to the page (the same report listed
  React DevTools' scripts). Check in a clean Chrome profile with no extensions first.

## Work
1. Reproduce in a clean profile; take the warning's stack (DevTools → Issues → the affected resource) to the file.
2. Ours → move it to `navigator.storage` (`persist()` / `estimate()`); a dependency's → update it or report upstream,
   with the version that fixes it named here. An extension's → close as not ours, saying how that was shown.
3. A test that fails if the old API comes back in our code.

Depends: none. Size: S. Owner: no.
