# T1142 — A release day eats 30 GB of disk; the next full build would be refused

Status: OPEN — before the next full data build (the hardening release at the latest)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1142 | A release day eats 30 GB of disk; the next full build would be refused | OPEN — before the next full data build (the hardening release at the latest) | | 0.10.0 chat 2026-11-25: 68 GB free at noon, 37 GB at 15:28 |

Source: the 0.10.0 chat, 2026-11-25 15:28: free disk on the Mac fell from 68 GB at noon to 37 GB (the build, the
candidate release and the baselines); `scripts/heavy.sh` refuses under 20 GB. A rerun of the staging step needs no
new build, but a second full build would be tight, and T1120's fresh clone builds everything a second time.
Related: the memory `tmpdir-fills-with-test-leftovers` (151 GB of release temp folders on 2026-11-15, 82 GB of merged
worktrees on 2026-11-20), T1063, T1120, T1133 (builds off the Mac).

Work: (1) say what a release leaves behind and how big each is (the candidate, old releases beyond live and
previous, baselines, the rehearsal's copies, lane worktrees, `$TMPDIR` leftovers, Gradle and Xcode caches);
(2) one command that lists them with sizes and removes what the rules already allow (releases pruned to live +
previous + the candidate; merged worktrees; temp folders of finished runs), dry run first, never a pattern-kill and
never the owner's review files; (3) the release's preflight prints free space and the space the run needs, and
stops with that number, before hours of work, not at 20 GB mid-run; (4) the runbook's last step runs the clean-up.

Acceptance: after a release and the clean-up, free space is back within 5 GB of where the day started; the
preflight refuses a run that cannot finish. Severity: low today (medium the day a build is refused mid-release).
Model: Sonnet. Size: S. Owner: no (nothing in R2 or of his is deleted).
