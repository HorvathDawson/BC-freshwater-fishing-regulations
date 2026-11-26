# T596 — A release download fails with "NoSuchFileException … .sqlite.part"

Status: IN REVIEW — 845734782, ae308c372 (release install and prune run one at a time; the queue has no swallowing catch). The acceptance read is `GET /crashes`, which answers 403 without the RUN_TOKEN (checked 2026-11-20 on staging and production; the lane may not read the token). owner: see ## Owner: he (or a session he gives the token) runs the two curls and records that kind 4219b587 has not come back on builds from 0.7.3.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T596 | A release download fails with "NoSuchFileException … .sqlite.part" (owner phone, 0.7.0) | IN REVIEW — owner: see ## Owner | 845734782 | /crashes needs the run token: the owner's two curls (kind 4219b587) |

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

## L3 2026-11-20 read attempt
`GET /crashes` answers HTTP 403 without the bearer RUN_TOKEN on staging (`bc-fishing-feeds-staging.horvath-dawson.workers.dev`) and production (`feeds.canifishthis.ca`); the token is a secret the lane does not read, so no crash read was made. The owner (or a session he gives the token) runs, per worker: `curl -H "Authorization: Bearer $RUN_TOKEN" "<worker>/crashes?since=2026-11-09"` and records: kind 4219b587 absent since 0.7.3.

## Owner

- [ ] 2026-11-19 — In the same `/crashes` read as T595, check that kind 4219b587 (`.sqlite.part`) has not come back on builds from 0.7.3; tell Claude.
