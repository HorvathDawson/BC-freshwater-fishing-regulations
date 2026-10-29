# T272 — First install: a second progress bar after the download, and make that step fast

Status: OPEN (owner 2026-10-29 on Android: "it downloaded everything then at 100% it took a very long time to open. it needs
a second loading bar so the user doesnt get confused").

## Work
1. Measure on the APK (testbed emulator + timings in the Updates log) what runs between download 100% and the map: unzip /
   verify / SQLite import / index build / first queries. Suspect T264's serialized SQLite queue and the T262 streaming path.
2. Loading screen gets a second stage with its own bar and words (e.g. "Downloading the regulations" → "Setting up on this
   device"), real progress (bytes/rows/steps), never a stuck 100%. Same component/look as the first bar (FishSpinner rules).
3. Cut the time where cheap (batch inserts in one transaction, defer non-map work until after first paint).

## Acceptance
No moment where a bar sits at 100% with nothing moving; before/after timings for the post-download step on the emulator;
screenshots of both stages.

Depends: T262, T264. Size: S–M. Owner: no.
