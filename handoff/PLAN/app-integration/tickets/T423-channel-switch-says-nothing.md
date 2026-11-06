# T423 — The update-channel (and environment) switch gives no feedback in an installed build

Status: IN REVIEW — 4e3d4ce59; proof pending: phone check (installed build, tap Test / another source: "applies when you reopen the app" shows at once; force a failed write if possible). Web shots in T423-shots/ (failed-write state at 390 and 1440; pending needs the native build, web reloads at once).

In a non-`__DEV__` build `restart()` does nothing, so tapping Test (Updates log) or another environment (About) writes
the choice and leaves the button on the running one: it looks dead until the app is closed and reopened (that worked).
Also `envStore` / `channelStore` `write` swallow errors (`catch { /* a preference */ }`): a failed write is silent
(no-silent-fallbacks rule).

## Work
1. The picked value shows at once as "Test — applies when you reopen the app" (both switches), reading the stored
   choice, not only the running one.
2. A failed write is shown under the switch and reported (`reportFailure`).
3. Tests for both (the native store, the page's state).
Size: S. Owner: phone check.
