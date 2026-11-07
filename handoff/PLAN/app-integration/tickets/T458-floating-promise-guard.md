# T458 — A guard against un-awaited promises (the T446 class of bug)

Status: DONE 2026-11-06 — 935497ef9

T446's cause (7fc594427): the Expo SDK 58 upgrade turned expo-file-system's `File.write` / `move` / `copy` into
promises. Our calls were not awaited, so a rename moved an EMPTY file into place. Fresh installs got 0-byte release
files, and the spots save (write, then rename) could have emptied a reader's spots file. TypeScript accepts a dropped
promise, and nothing in `pnpm check` looks for one. (The app has no ESLint.)

## Work
1. A gate in `pnpm check` that fails on a floating promise in app source (not tests): typescript-eslint's
   `no-floating-promises` (+ `no-misused-promises`) with type information, OR a small TS-compiler-API check in
   `app/tools/` if adding ESLint costs too much (rule 30: a line in `app/deps.md` either way). Explicitly discarded
   promises use `void p` with a reason in a comment.
2. Run it over the tree, fix what it finds (each a real bug or a `void` with its reason), commit the fixes with the gate.
3. Scan done in chat 5 (by hand, expo-file-system only): no un-awaited `write`/`move`/`copy` left after T446.
   `delete`/`create`/`rename` are synchronous in SDK 58.
Size: S–M.

## Done (2026-11-06, 935497ef9)
`tools/check-promises.mjs` (`pnpm promises`, in `pnpm check` after the type checks) uses the compiler API that
`typescript` already gives, so no ESLint (deps.md says why). Over the 6 projects `tsc -b` builds, tests excluded, a
statement whose value is or may be a thenable fails unless it is awaited, returned, assigned, `void`ed, or its chain
handles the rejection (`.catch(h)` / `.then(a, b)`, past a trailing `.finally`). Those are typescript-eslint's
`no-floating-promises` defaults.
**Deviation from Work 1 (said, not silent):** a `void` needs no reason comment. Asking for one flagged 74 sites, and the
busiest were in files the walkthrough lane is editing. The rule's own default allows `void`. The first strict run found
135 sites; with those defaults, 10.
**Fixed (the 10):**
- 5 were false positives of an early version: a handled chain ending in `.finally()`. The gate now looks past it.
- 3 are MapLibre `once("load", fn)`: typed `this | Promise`, it returns the map when given a listener. These are `void`ed
  with that comment.
- 2 were real: chains with no rejection handler. FishSpinner's reduce-motion query and `sync.ts`'s `Promise.all` now
  report a failure (`reportFailure`, rule 46); sync also stops "checking".
**Pinned:** `tools/check-promises.test.ts` plants T446's shape (an un-awaited `write` before a rename) and is caught. A
maybe-promise and an unhandled `.then` are caught. Awaited, returned, assigned, `void` and handled chains pass.
