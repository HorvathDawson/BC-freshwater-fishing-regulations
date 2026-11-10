# T542 — Emulator suites: wait on conditions, and a static check that every id and label they use exists

Status: OPEN.

Source: workflow review 2026-11-09 (verified against the code). Rank 10.

## Today
- Fixed sleeps dominate the release suites: `app/tools/testbed/scenarios/offline.mjs` 16 (`:57,74,94` 4 s; `:152` 15 s),
  `water-nav.mjs` 12 (`:121` 8 s), against few condition waits (install 0, gestures 0, water-nav 1, offline 6).
  `water-nav.mjs:66` and `gestures.mjs:69,131` tap computed coordinates.
- Nothing reads the scenarios outside an emulator run (the only test is `lib/channel.test.mjs`), so an id or label a
  step depends on can change with no failure until a heavy run. Chat 9 fixed three stale steps after the fact; the bell
  step predating T263 is the kind a static check catches (the © chip overlap and the Retry race need a run).
- `feeds.mjs:25`'s 65 s sleep is deliberate (must pass the 60 s foreground timer): keep it, say why in place.

## Work
1. Static check (vitest, seconds; in fast-fail when `app/` or `testbed/` changes): every testID and literal screen text a
   scenario taps or waits for exists in the app source.
2. Replace fixed sleeps in install, offline, feeds, big-update, gestures, water-nav with condition waits with a time
   limit, except documented timer waits.
3. A failed step's report names the commit that last touched the step and the screen file it reads.

## Acceptance
- Renaming a testID used by a scenario fails fast-fail. Suite wall time before/after recorded here; all six green.

Depends: T534 (same test-bed files). Size: M. Owner: no.
