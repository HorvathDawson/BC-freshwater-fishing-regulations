# T1063 — `scripts/heavy.sh`: a release build should not queue behind other chats' check tiers

Status: OPEN.

Source: the 0.9.0 chat, 2026-11-24: the 0.9.1 lanes' `before_pr` tiers made the production fast-fail take 30 minutes and queued the production build behind them.

## Work
1. A priority (or a reserved lane) for release builds in `heavy.sh`: a waiting release job takes the next free slot.
2. Say in AGENTS how a lane asks for it, and that only a release run may.

## Acceptance
a test of the queue order; the runbook names the flag.

Severity: low. Release: 0.9.2. Model: Sonnet. Size: S. Owner: no.
