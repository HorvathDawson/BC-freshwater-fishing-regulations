# T446 — First install: "release/r-4230bc0c1dd5/geo-1a94cc221b73.sqlite does not match the manifest"

Status: OPEN.

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
