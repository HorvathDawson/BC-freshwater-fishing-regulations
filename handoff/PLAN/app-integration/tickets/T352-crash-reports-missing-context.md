# T352 — Crash and problem reports lack the facts that would find the cause

Status: OPEN (from T346, 2026-11-02).

What the 7 crash kinds and 1 problem report held vs what was needed:
1. No water/item id, section, or open pane on a crash (only `route` = water/map/other). The Coquitlam crash shows as
   route "water" with a minified stack; add the open item id and the screen/pane state.
2. "Requiring unknown module '…'": the module id is cut to an ellipsis, and the minified `index-<hash>.js` stack cannot be
   mapped back. Keep the module id; send a release id that maps to uploaded source maps (or keep maps with the release).
3. Two hashes (b0eaeeb61c3d3a6c, f1a058176d7c1a37) are the same error, split because the stack format differs by browser.
   Normalise the stack before hashing (or hash name + message + route only).
4. No install/session id, so "owner's vs other testers'" cannot be told and distinct-user counts are impossible. Add a
   random per-install id (no personal data), or an `owner` flag set by the staging build.
5. Native exits carry no stack or tombstone; background OS kills (SIGNALED 9, LOW_MEMORY) are sent like crashes. Mark
   them as `background` kills and keep them out of the crash counts.
6. Problem report context holds only screen ("more"), mode, version, tiles; add the active layer, forecast offset,
   zoom/centre and the open water id (the colour report below needed them).
7. Dev-build crashes arrive too (env "local", `useRegsPrefetch is not defined`, 1 report); the store should drop env
   "local" (it is not a tester).
Size: S–M.
