# T799 — Ten `test_stepcache` tests fail when the repo lives under the system temp folder (the recorder sees "0 modules, 0 reads"); a release rehearsed from a scratch worktree would hit it

Status: DONE d95792c17 (lane/bugs-080 2026-11-17: a skipped root that holds the repo skips only paths outside it; test_stepcache passes under the temp dir)

Source: the app chat, 2026-11-16 10:40. In a worktree under `/private/tmp` (two different heads, one of them untouched
code) the same ten tests of `pipeline/tests/test_stepcache.py` fail; all 20 pass in the main checkout. The recorder
reports "0 modules, 0 reads" when the repo's path is under the system temp folder (most likely it treats temp paths as
not ours). GitHub's runner is not under a temp folder, so CI is green.

## What to build
- Find where the recorder filters paths (temp folder prefix) and make it filter by "outside the repo root", not by
  "under the temp folder"; a test that runs the recorder from a repo copied under the temp folder.
