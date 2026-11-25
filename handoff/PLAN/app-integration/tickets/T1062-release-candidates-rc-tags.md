# T1062 — Release candidates: tag `vX.Y.Z-rc.N` and promote, instead of moving the version tag after late fixes

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1062 | Release candidates: tag `vX.Y.Z-rc.N` and promote, instead of moving the version tag after late fixes | OPEN — plan agreed 2026-11-25; the build is not scheduled | | step 2 (the build), a later chat |

Source: owner idea during the 0.9.0 release, relayed by the 0.9.0 chat 2026-11-24: 0.9.0's tag had to move after late fixes (T1027, T1034, T1049 cherry-picks).

## Work
1. Plan first (docs): how rc tags fit `pipeline release --cut`, release-please / the version files (T998), the staging release, Play Internal build numbers and the app_reads row (T1048).
2. Then build: the cut makes `-rc.N`; the final tag is put on the rc that passed, never moved.

## Acceptance
a written plan the owner agreed to; then one release done with rc tags and no moved tag.

Severity: process. Release: with chat 3 (repo move + merge queue), or before 0.10. Model: Sonnet for the plan, Opus for the cut change. Size: M. Owner: yes (agree the plan).

Owner 2026-11-24 (planning chat question, picked "With the repo-move chat"): the plan is written in chat 3; nothing is
built before the owner agrees it.

Owner 2026-11-25 ~13:58 (repo-move chat, dialog). The plan is `handoff/PLAN/repo-move/RC-TAGS-PLAN.md`; his picks,
verbatim labels: visible version in an rc build: "Plain 0.10.1 (Recommended)"; where the rc is tagged: "At the
staging ship (Recommended)"; who runs `release --final`: "Claude, on your word (Recommended)"; the tag ruleset:
"After the first rc release (Recommended)"; data-only releases get rcs: "No (Recommended)"; the plan as written:
"Agreed". Step 1 is done; step 2 (the build, about 150 lines with tests and five doc pages) waits for a chat.
