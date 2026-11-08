# After chat 7: start here

Written 2026-11-07. It replaces `NEXT-SESSION-after-chat6.md`.

## Paste this to start the chat
> Read `handoff/PLAN/START-HERE.md`, the restart page it names, and the STATUS.md top entries. Do the list in order.
> Start ticket work without asking; ask me only for decisions. Keep replies short and STATUS.md current.

## The goal
**T495** (owner 2026-11-07): Play closed testing ON PRODUCTION, good enough for first users. Four gates: no wrong regs,
no crashes/ANRs, smooth map, clean first run. The website moves v1 → new before closed testing. Owner: salmon extent
review (T164) + every prod step. Claude: everything else. Work T495's "Order" section top-down.

## Where chat 7 left things
- **Data r-f56b90ad2540** is LATEST and live on staging (T482 salmon fold; same bindings as r-1b4f); staging site
  redeployed on it, smoke 17 pass / 0 fail. Prod: `CONFIRM_PROD=r-f56b90ad2540 python -m pipeline release --prod r-f56b90ad2540` (owner).
- Done: T490 (/changes = rule changes), T486 (feed heartbeats; worker deployed), T491. IN REVIEW: T483 fast-fail
  (`scripts/fast-fail.sh`, run first by release + APK), T484.
- New: T493 (creek gap at hidden lake, decisions in the ticket), T494 (web map console TypeError), T495, T496.
- Owner phone push on questions/permission prompts: `inputNeededNotifEnabled` in ~/.claude/settings.json.

## Next, in order
1. T495 step 1: batch the IN REVIEW looks into one owner phone check (shots/staging link, not paths).
2. T478, then T494, T351/T360 (gate 2), T493 + T489 (one tiles rebuild, built beside).
3. T404/T240 prod cut-over prep (dry-run against staging).
