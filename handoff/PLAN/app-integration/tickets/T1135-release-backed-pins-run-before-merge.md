# T1135 — A lane that changes answers can run the release-backed tests on a side release before it merges

Status: OPEN — the speed work (was 0.10.1): the follows from 48 to about 21 minutes; the pins in one file; resume

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1135 | A lane that changes answers can run the release-backed tests on a side release before it merges | OPEN — the speed work (was 0.10.1): the follows from 48 to about 21 minutes; the pins in one file; resume | | 0.10.0 chat 2026-11-25 |

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

## Deep look 2026-11-25 night: the checks after a build ("follows"), measured; this widens the ticket
Notes: `tickets/archive/night-notes-2026-11-25.tar.gz` `follows-deep-look.md` (evidence: the release chats' session logs, the 0.10.0 run logs, the
ledger). The replacement text for this ticket is in the notes; in short:
- "25 minutes clean" is out of date (measured on 0.8.5, before the pipeline's live-data tests joined the follows
  on 2026-11-21): a clean run today is about 48 min on a quiet Mac (0.10.0 run 1, 13:52 to 14:40); the data tests
  alone take 36 min, one chunk at a time.
- The follows already list every failure in one pass: each release stopped ONCE in its tests, with 4 failures
  together. The hours went on: every check rerunning after a one-line fix (about 48 min); the fresh-copy run
  before each rerun (12-17 min), which repeats PR CI; the rehearsal giving up when the fixture re-cut failed
  (`release.py:1974-1975`), which is why it missed 4 of 0.10.0's 6 pins; the data tier reporting only "rc=1"
  with no test name.
- None of the 12 failures in 0.9.0 and 0.10.0 was a real data defect: 9 literals copied from an older release, 1
  version-length assumption, 1 stray untracked file, 0 flaky. Earlier releases show these checks catching real
  bugs (T931, T980, T1140): make them cheaper, do not cut them.
- The changes: the data tier in parallel; resume (a step that passed on this exact data + code is not run again);
  the fresh-copy run dropped in favour of green CI on the shipped commit; the release literals in one pins file
  re-cut by one command, the rest turned into properties; the rehearsal never gives up early; the failing test
  named. Estimates: clean about 21 min (was 48); one real failure about 35-40 min (was about 2 h).

## Owner

- [ ] 2026-11-25 — Picks (defaults first): the data tier in parallel / only add resume / split part of it to weekly; the fresh-copy run: drop it and require green CI on the shipped commit / run it once / keep; release literals: one pins file plus properties / properties only / as today; the side release before merge: app and pipeline tests first / all / not before merge; the 4 minutes of CI repeats inside the follows: keep / drop.
