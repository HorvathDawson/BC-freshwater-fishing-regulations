# T1161 — A release run keeps the Mac awake, records the start and end of every stage, and other chats stay off the Mac on release day

Status: OPEN — the first patch after 0.10.0 (tooling)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1161 | A release run keeps the Mac awake, records the start and end of every stage, and other chats stay off the Mac on release day | OPEN — the first patch after 0.10.0 (tooling) | — | a build lane: `caffeinate -i` inside the release command and heavy.sh, refuse an unattended run on battery, stage times in the release ledger and the phone build scripts, the AGENTS release-day line; S |

Source: the release-timeline pass of 2026-11-25 night (notes: `tickets/archive/night-notes-2026-11-25.tar.gz` `release-timeline.md`, with the PR and
run data it measured from). Avoidable stops it found: the Mac was asleep for 8.3 h during 0.10.0 and nothing in the
release tooling keeps it awake; six release-backed test pins stopped 0.10.0 one at a time, about 45 min each
(T1135); other chats' jobs held up the 0.9.0 production builds for 70 min (T1063); the repo move in the middle of
release day stopped the staging run. The timeline itself had to be inferred: the start of the follows, the
staging-site time and both phone builds are not recorded anywhere.

## Work
1. `pipeline release` and `scripts/heavy.sh` hold the Mac awake for the length of a run (`caffeinate -i`), and an
   unattended run refuses to start on battery.
2. The release ledger records the start and end of every stage (preflight, build, checks, follows, promote,
   upload, staging site, smoke); the phone build scripts record theirs.
3. An AGENTS.md line: while a release holds the main checkout, other chats run no heavy jobs, emulators or hook
   test runs on the Mac and make no repo or infrastructure changes (with T1063's priority lane).
4. Phone builds with `--no-upload` on the rc overnight, so the morning needs only the uploads.

## Acceptance
A release run survives a closed lid on power (tested with a short stage); the next release's timeline is read
from the ledger, not inferred. Severity: tooling. Model: Sonnet. Size: S. Owner: no.
