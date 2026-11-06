# T446 — First install: "release/r-4230bc0c1dd5/geo-1a94cc221b73.sqlite does not match the manifest"

Status: PARTIAL (cddfdbe10): diagnosis in the next report; not reproduced.

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
