# T1062 — Release candidates: tag `vX.Y.Z-rc.N` and promote, instead of moving the version tag after late fixes

Status: OPEN.

Source: owner idea during the 0.9.0 release, relayed by the 0.9.0 chat 2026-11-24: 0.9.0's tag had to move after late fixes (T1027, T1034, T1049 cherry-picks).

## Work
1. Plan first (docs): how rc tags fit `pipeline release --cut`, release-please / the version files (T998), the staging release, Play Internal build numbers and the app_reads row (T1048).
2. Then build: the cut makes `-rc.N`; the final tag is put on the rc that passed, never moved.

## Acceptance
a written plan the owner agreed to; then one release done with rc tags and no moved tag.

Severity: process. Release: with chat 3 (repo move + merge queue), or before 0.10. Model: Sonnet for the plan, Opus for the cut change. Size: M. Owner: yes (agree the plan).
