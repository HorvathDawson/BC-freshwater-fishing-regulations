# UI chat (T367): start here

For the owner's separate UI clean-up chat. Batch 2 (emulator, memory, PDF) carries on in the main chat; this chat
touches only the screens' text and layout.

## Paste this to start the chat

> Read `handoff/PLAN/NEXT-SESSION-UI.md`, then the ticket it names. We're doing T367 as a back-and-forth: I say what
> bothers me on a screen, you write a short plan, I say yes, you change it on the dev web app, I check, repeat. Keep
> replies short.

## The loop (ticket: `handoff/PLAN/app-integration/tickets/T367-ui-text-cleanup-loop.md`)
1. The owner names a screen + what's wrong (often a screenshot).
2. Claude: a 2–5 line plan (what changes, which file, whether the text comes from the data).
3. Owner yes/adjust → Claude changes it → owner reloads the web app and checks.
4. Log the round in the ticket's "Rounds" (note → fix → verdict, commit). Next note.
Into the 0.5.0 APK only when the owner calls the round done; no APK per round.

## Rules for this chat
- Areas: the regs pane, the conditions map pane, More. Other screens only if the owner asks.
- Text that comes from the DATA (rule wording, place names) is shown as sourced: formatting it is a display change;
  rewording or dropping content needs the owner's yes (memory ask-before-filtering-content).
- Words live in `app/packages/core/src/**/words.ts` (and `regs/words.ts`); screens in `app/packages/ui-native/src`.
  Web and native are ONE component tree (react-native-web): a change shows in both.
- After each change: the touched component's tests (`cd app && npx vitest run <paths>`), `npx tsc -b`; commit with
  `-- <paths>` (another chat works in the same tree: never `git add -A`). Snapshot fixtures may need
  `UPDATE_REGS_FIXTURES=1` — say so in the round.
- Owner rules in force: no silent fallbacks (T368), one load path per screen (T369), better package over minimal.
- Don't touch the water-map pane's logic, the map package, or `regs/synopsisPdf*` / `PdfViewer*` (batch 2 is there);
  their TEXT is fine.

## The dev web app
- http://localhost:39218 — `cd app && pnpm dev:web` (Expo, Fast Refresh: a save shows on reload).
- Data: the local release `data/release/LATEST` (r-4df5bccc9360) served by `cd app && pnpm tiles` on :39217.
- If either is down, start it in the background and kill only its own PID. Check with
  `curl -s -o /dev/null -w "%{http_code}" http://localhost:39218/` and `…:39217/release/LATEST`.
- The walkthrough opens on a fresh browser profile: "Skip".
- Crash reports from the dev app get 400 from staging (the new crash/2 format waits for the worker deploy, batch 7):
  expected, ignore.

## Done already (round 0)
- Regs loading spinner: in the middle of the pane, large size (0db30c2).
