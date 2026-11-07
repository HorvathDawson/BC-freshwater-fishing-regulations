# T463 — Deep code review of the branch before production (`/code-review ultra`)

Status: OPEN (owner 2026-11-06, chat 5).

## What
The owner runs `/code-review ultra` on `redesign/stream-sections`. It is a multi-agent cloud review of the local
branch and is billed. Only the owner can start it; Claude cannot. Claude then:
1. reads the findings,
2. checks each one against HEAD,
3. files the real ones as tickets (or fixes the small ones under a ticket),
4. and writes the outcome here.

## When
After 0.6.8 merges, when no lane is mid-merge. The review takes a snapshot of the branch, so code that is still moving
gives stale findings. Run it again before T404 (production).

Related: T462 (regs presentation audit), T240 (launch checklist), T404. Size: S for Claude (triage). Owner: starts the
review.
