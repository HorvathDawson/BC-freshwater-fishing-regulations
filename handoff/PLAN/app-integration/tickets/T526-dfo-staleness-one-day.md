# T526 — DFO salmon is stale when it has not been CHECKED for a day

Status: IN REVIEW (build 22 ship-blocker) — worker unchanged, not deployed

Owner 2026-11-09: "should be if it is older then a day. and older just means it wasnt checked because it doesnt change
that frequently but we need toi check it often."

## What
- `STALE_AFTER.dfo` (app/packages/core/src/staleness.ts) was `age 120 d` judged on the RELEASE's DFO date. Now `age 24 h`
  judged on when the feeds worker last CHECKED DFO: the DFO feed's `at`. The worker already records it
  (`dfo/live.ts`: `index.at` is the run's time, rewritten at least every 20 h by `INDEX_HEARTBEAT_MS` even when DFO's
  content did not change) — so no worker change, nothing to deploy.
- DFO content changing rarely is NOT staleness: content last changed 30 days ago but checked 1 h ago = Up to date.
- The Data line reads "Nh ago" (the feed line's wording) from `SourcesInput.dfoCheckedAt`; no checked time = "Not checked
  yet", badge Unknown. Never the changed time, never the release's date (no silent fallback).
- `useDfoCheckedAt` (app/packages/ui/src/dfoNotice.ts) reads the stamp from the same `SalmonFeedSource` as the card;
  no feed read, or a stamp that is not a time, throws inside `useAsync` so it is reported (`reportFailure`).
- Wired Shell.tsx -> BrowseHost `dfoCheckedAt` -> `dataFacts`.

## Tests
- menu.test.ts: table says dfo 24 h; 23 h fresh, 24 h fresh, 25 h stale; changed 30 d ago + checked 1 h ago fresh;
  missing/unreadable checked time unknown; row wording.
- ui/dfoCheckedAt.render.test.tsx: the stamp is returned unreported; a missing feed / bad stamp is null and reported.
