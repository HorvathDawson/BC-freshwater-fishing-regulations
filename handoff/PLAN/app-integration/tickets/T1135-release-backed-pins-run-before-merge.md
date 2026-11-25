# T1135 — A lane that changes answers can run the release-backed tests on a side release before it merges

Status: OPEN — 0.10.1, first Opus lane with T1062's build (it stopped the 0.10.0 release six times)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1135 | A lane that changes answers can run the release-backed tests on a side release before it merges | OPEN — 0.10.1, first Opus lane with T1062's build (it stopped the 0.10.0 release six times) | | 0.10.0 chat 2026-11-25 |

Source: the 0.10.0 chat, 2026-11-25 14:50. Not checked by the planning chat. Related: T1062, T1048, T1053, T666.

Six test pins that read the live release stopped the 0.10.0 release one after another, about 45 minutes each: the
10-character version slice (fixed, PR #212), a parity literal (PR #218), the `reads.release` statics, the
`sourceLine.fixture` table, the `WaterScreen.unnamed` page, and a sentence in `test_answers_26`. No lane could
see them before merge, because they only run against a real release and the lanes changed answers (T1053, T1117,
T1029, T1050 to T1052) without one.

Work: (1) one command a lane runs before its PR: build a side release from its branch (reach step and deliver, no
atlas rebuild unless the change needs one; through `scripts/heavy.sh`), run the follow tests against it, print
every pin that would stop the release with the file and the new value; (2) `before_pr.sh` says when a diff
touches answers and this was not run; (3) the pins that are literals copied from a release are read from the
release, or re-cut by one command, so a correct change needs one accept, not six stops; (4) the rehearsal lists ALL
would-stops in one pass (today it found three, the real run found three more).

Acceptance: replaying the 0.10.0 changes on a branch, the command lists all six pins before merge; the release then
passes its follows on the first run. Severity: medium (cost about four hours on 0.10.0). Model: Opus. Size: M.
Owner: no.
