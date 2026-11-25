# T1060 — App robustness: no React error boundary, and three one-render stale states

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1060 | App robustness: no React error boundary, and three one-render stale states | OPEN —  | | planning chat 2026-11-24 |

Source: review lows from PRs #107 and #115, handed over by the 0.9.0 and 0.9.1 chats 2026-11-24.

## Work
1. No React error boundary: any render throw is a whole-app crash. Add one per screen that shows the failure and reports it (`reportFailure`, no silent fallback).
2. A stale FAILED nav answer can flash "unavailable" for one render (`app/packages/ui/src/frameView.ts`).
3. `useRegsTarget`'s `found` is stale for one render on a frame with no part.
4. T1049: a clamped start jump can still be taken for the reader's own scroll when the page grows before the scroll event; late fonts or images, and page growth after a still page with nothing coming, are not covered by the start-link follow (T1049 ticket, "not covered").

## Acceptance
a thrown render in a test shows the boundary and files a report; each stale state has a test that fails before the fix.

Severity: medium for item 1 (crash containment), low for 2–4. Release: 0.9.2. Model: Opus. Size: M. Owner: no (the boundary's words: default to the existing failure card).
