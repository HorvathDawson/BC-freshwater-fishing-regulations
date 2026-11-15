# T746 — While new data is applied the app shows a loading mark and a line of text, not a still screen

Status: IN PROGRESS (iOS chat; on a branch from `ios/t722-install`, merges with T722).

Source: owner 2026-11-14 (evening), relayed by the Android chat, verbatim: "also when changes apply to app we want to make
it so that it has a loading icon with text like splash so it doesnt looking like it is frozen. (applying changes as in
new data regs)".

## What is asked
While a new data release is being applied (after its download: checking, swapping the release in, reopening the
databases, the map reloading its style and tiles), the app shows a splash-like state: a loading mark and one line of
text that says what is happening, in the app's plain words. On Android, iPhone and the web.

## Work
1. Find every moment the screen can sit still during an apply, on each platform: at a cold start with a staged
   release, on return to the app, while the app is in use (if an apply can happen then), and on the web when the
   site's release changes under an open tab. For each: what the reader sees today, and how long it lasts (measured on
   the emulator, the simulator and the web).
2. Build one state for all of them (one load path per screen): the loading mark, the line of text, and nothing
   tappable underneath while the swap is in flight. Platform differences only through `.native` / `.web` file pairs.
3. A failed apply says so on the screen and is reported (`reportFailure`); the app keeps the release it had. No silent
   fallback.
4. The words live in `packages/core` with whole-sentence tests. Proposals for the wording go to the owner with a
   picture of each platform.
5. Benches: Android `regs-open`, `tap-stalls`, `update-freeze`, `big-update`; the iOS twins that exist.

## Acceptance
- No still screen longer than about 300 ms during an apply without the loading state up, on the three platforms; the
  measured times before and after are in this ticket.
- A failed apply is shown and reported; the old release still opens.
- The owner has picked the wording from pictures.

Depends: T722 (install branch). Size: M. Owner: wording pick.

## Results
(filled as it lands)
