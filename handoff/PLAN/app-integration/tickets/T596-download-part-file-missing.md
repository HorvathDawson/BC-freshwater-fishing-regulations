# T596 — A release download fails with "NoSuchFileException … .sqlite.part"

Status: OPEN

Source: worker `GET /crashes` 2026-11-10, kind 4219b587: once, the owner's phone (install c909e605), 0.7.0 build 22,
2026-11-09 23:07 UTC, while taking release r-d14bdb180f55: `java.nio.file.NoSuchFileException:
/data/user/0/ca.canifishthis.app/files/release/r-d14bdb180f55/water-a8aca53ad7f4.sqlite.part`.

## Today
- The `.part` file was gone when the download went to finish it (move or verify). Likely two downloads of the same
  file at once (a resumed update beside a fresh one), or a cleanup that removes `.part` files while a download runs.
  T446 fixed the unawaited write/move; this is a different path. Not reproduced.

## Work
1. Read the release download path for every place a `.part` is created, moved or deleted; find what can delete or move
   one that another task still holds.
2. Reproduce in the test bed (start an update, background and reopen, or tap Sync twice).
3. One download per file at a time; cleanup never touches a file in flight. Test for the race.

## Acceptance
- The reproduction no longer files the error; the update finishes.

Depends: —. Size: S. Owner: no.
