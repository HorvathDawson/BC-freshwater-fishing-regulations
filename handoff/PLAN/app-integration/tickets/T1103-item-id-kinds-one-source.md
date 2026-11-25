# T1103 — Item id kinds (`gnis`, `wbk`, `wsc`, …) are hand-kept in four places; one source, generated

Status: OPEN.

Source: 0.9.1 chat 2026-11-24 ~23:40, finds of the T1083 lane (PR #164: the app refused `wsc:` ids because one list
lacked the kind; 557 `wsc:` waters).

## What was found
- `app/tools/check-test-anchors.mjs` line 128, `WATER_ID`, lists gnis|wbk|fwa|blk|area|zp but not wsc: a raw part
  pinned on a `wsc:` id in a test is not flagged.
- The kinds are hand-kept on both sides: Python `pipeline/tools/audit/check_curated.py` `ITEM_PREFIXES` and
  `pipeline/regs/registry/build.py` `item_id`, and the new TS `ITEM_ID_KINDS`. A release-backed test pins the TS
  list, but it is not generated (AGENTS rule 40).
- The old committed fixture release `app/packages/data/dev/fixture-release` holds 208 `name:…` ids no reader accepts.

## Work
1. One list of id kinds, written by the pipeline (the registry build) into the contract the app reads; the TS list
   and `check-test-anchors` read it or are generated from it; a check fails when a release holds a kind the app's
   list lacks (that is the T1083 bug as a guard).
2. The fixture release: rebuild it from a current release or delete the 208 dead ids; say which.

## Acceptance
Adding a kind in the pipeline without the app list fails a check; `check-test-anchors` flags a raw `wsc:` part.

Depends: T1083 (#164). Severity: low (the guard against T1083's class). Release: 0.9.2. Model: Opus (pipeline +
app contract). Size: S–M. Owner: no.
