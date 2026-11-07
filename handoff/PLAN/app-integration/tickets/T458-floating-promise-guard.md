# T458 — A guard against un-awaited promises (the T446 class of bug)

Status: OPEN (chat 5, 2026-11-06, after T446).

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
