# T423 — The update-channel (and environment) switch gives no feedback in an installed build

Status: OPEN (owner 2026-11-05, 0.6.2 from Play internal: "won't let me click button to change update log … button in
about won't change it").

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
