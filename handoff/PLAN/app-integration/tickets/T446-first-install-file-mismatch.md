# T446 — First install: "release/r-4230bc0c1dd5/geo-1a94cc221b73.sqlite does not match the manifest"

Status: DONE (7fc594427): the cause was SDK 58's async `write`/`move`; emulator fresh install clean.

Source: staging worker `GET /crashes` (read 2026-11-06), kind 708eb429638d84ff: 0.6.3 (build 12), Android 35 (the
emulator test bed, walk-063b, 2026-11-06 16:31Z), route other, release none, non-fatal, thrown in `install`. The same
run's download then completed and the map opened, so a retry recovered — but a file failing its check on a clean
first install means the bytes on disk were not the file: a resumed or interrupted download kept a partial / stale
piece ("What has downloaded is kept"), or the test channel's switch (T442) changed the file under a running download.

## Work
1. Check the download's resume path: a kept partial file is verified by size+sha before it is reused, and a mismatch
   deletes it and fetches again (said in the Updates log, not swallowed).
2. Reproduce on the emulator: interrupt the first-run download (airplane mode / kill) mid-file, resume; and a manifest
   switch mid-download. Pin it in the download tests.
Size: S.

## Read (2026-11-06, chat 4) — not reproduced yet
The throw is the install's re-verify (`install.ts` "every file of the new set, re-verified on disk"), not the download:
`dl` (fetchVerified) already refuses bytes that are not the manifest's, and `place` writes `.part` then renames. Within
one install nothing else writes `geo-*.sqlite` (small files are fetched one at a time; the assets start after the
re-verify). So the bytes changed between a verified write and the re-read, which needs a SECOND writer: a second
`install` into the same folder (a boot retried while the first install still ran — `useReleaseBoot`'s cleanup only sets
`live = false`, it does not stop a running `bootRelease`), or a file read that came back short. The report's time
(16:31Z) is inside T442's window, when `test/` named a release whose files had been cleared, so the run was already
abnormal. Next: the emulator repro in Work 2 (retry during a first install; a kill mid-file) once the slot is free.

## Next report names itself (cddfdbe10, 2026-11-06)
The re-verify's error now says "(N % of its size)" or "(the same size, other bytes)" — a cut write vs a second writer
or another release's file — in words the crash scrubber keeps. Emulator repro not run: no path to a second writer was
found in code, and the one report sits in T442's broken window. Reopen with the next report's words.

## Cause and fix (7fc594427, 2026-11-06 chat 5)
The next reports named it: "0 % of its size" for core, regs, lines, salmon, flow-area (0.6.5–0.6.7, Play walks and the
test bed). Cause: the Expo SDK 57→58 upgrade (T250, 320785434) made expo-file-system's `File.write` and `File.move`
PROMISES (the work runs natively after the call returns). `install.native.ts` called both unawaited, so `place`
(write `.part`, rename) moved the EMPTY file `create()` made onto the release path while the bytes landed in a new
`.part` behind it; the re-verify then read 0 bytes, the boot failed and the auto-retry recovered (fetchInto re-downloads
a file whose sha is wrong) — sometimes failing on the next file. A race, so some files won. Not the downloader, not a
Range/resume or R2 answer, not a second install.
Fix: awaited in `install.native.ts` (write, rename, the resumed tail's move) and `synopsisPdf.native.ts` (move, the
highlight append); the sync stores use `writeSync`/`moveSync` — `spots/store.native.ts` (the spots save is
write-then-rename: the same defect could empty the spots file), `runTimingStore.native.ts`, `envStore.native.ts`.
Pinned by driver tests over a fake whose write/move land after the call (`install.native.test.ts`,
`store.native.test.ts`, `synopsisPdf.native.test.ts`); each fails with the awaits/sync calls reverted.
Before/after (test bed `install`, fresh app): before 2 boots per install (one failed start on a 0-byte file, runs
2026-11-06T16-04-57 and T16-46-03; Play walks on 0.6.6 reported 2 mismatches per install); after (run
2026-11-07T02-35-30) 1 boot, 0 mismatches, no crash report, up in 92 s.
