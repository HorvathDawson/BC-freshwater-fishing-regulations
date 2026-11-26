# T1147 — Release candidates: two review finds left after the build (the rc check by branch name; a misleading refusal)

Status: OPEN — the hardening release (tooling)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1147 | Release candidates: two review finds left after the build (the rc check by branch name; a misleading refusal) | OPEN — the hardening release (tooling) | | repo-move chat 2026-11-25, from the independent review of PR #257 |

Source: the independent review of T1062's build (PR #257, on main as 7139d1ae2), 2026-11-25; not fixed there.
1. The phone builds' rc check decides by BRANCH NAME (`deploy/lib/build-links.sh:160`): on a detached HEAD or a
   branch that is not a release branch it says "not asked", and no reason is required. Wanted: a build that will
   be uploaded to a store track always answers the rc check, or names its reason in the build record.
2. Production's tag check says "does not hold the data" when the local clone simply lacks the rc's commit. It
   still refuses (safe), but the words send the reader the wrong way. Wanted: "this clone does not have commit
   <id>: fetch tags" as its own case.
Also for T1062's notes: 0.9.0 passed production's tag check only because production ran (12:00) before its tag
was re-made (14:22); a production dry run of 0.9.0 would refuse today.

Acceptance: a real-git test for each case, failing without the change. Severity: low. Model: Sonnet. Size: S.
Owner: no.
