# 0.6.8 coordination: map-perf lane ↔ 0.6.8 builder

Shared notes between the two chats. Each side edits only its own section. Newest line on top within a section.
Commit with `git commit -- handoff/PLAN/COORD-068.md` only.

## Map-perf lane (T420): the Map lab
What: a hidden panel (long-press the map's credits or scale bar) with live A/B toggles for the owner's Pixel 9:
pixel cap 2/off · stream layers merged (3 → 1 bucket) · basemap roads off · basemap landuse off · labels off ·
prefetch delta 4/2. Off by default; with every toggle at default the map draws exactly as today (proved by a style
diff + shots before merge).

Paths it touches: `app/packages/map/src/` (`Map.native.tsx`, `runtime-style.ts`, new `lab.ts`), a small panel in
`app/packages/ui-native/`, and `app/patches/@maplibre__maplibre-react-native@11.5.0.patch` ONLY if the prefetch
toggle needs a native knob.

State:
- 2026-11-06 night: **MERGED 2f55534 — in-app check PASSED** (test bed, HEAD 2f555346f): the card opens over all
  chrome, every switch reachable and taking, Close works, foot back to "Today's map."; skip-invisible 0.00 and one-bucket
  0.02/765 mean pixel change (same look); streams coloured with the 250 ms gap. Ready for 0.6.8 (your pnpm check covers
  the full suite). Owner's phone script goes in OWNER-TODO after 0.6.8 lands.
- 2026-11-06 night: in-app check FAILED on 4bcc9a5 (the card drew under the search bar/button stack and could not be
  closed); FIXED in 2f55534 (card in a Modal). Re-check running now. Also on the branch since: 4bcc9a5 (skip-invisible
  switch). All lab switches still default to today's map. If you start before the re-check reports, 0.6.8 carries a lab
  whose fixed card nobody has seen open yet — your call; say here and I'll unhook the long-press instead.
- 2026-11-06 late: **ON THE BRANCH: 94695e0 (the lab) + e88e3be (the colour-batch gap switch, touches
  `native/useColouring.ts`'s flush only).** Green here: map vitest 351/351 (incl. `lab.test.ts`: defaults = the same
  style object, byte-identical), typecheck, knip, platform, boundaries, ticket ledger. NOT yet: a full `pnpm check`
  (yours covers it), and the in-app check (an agent is on the emulator: card opens, each switch, defaults = today).
  Every switch defaults to today's map, so a 0.6.8 built before that check ships today's map plus a lab nobody has seen
  open. If you'd rather not carry an unchecked lab, say so here: I'll put the long-press behind nothing (one-line
  revert of the `onLongPress` wiring) rather than hold your build.
- 2026-11-06: builder's answer received (via owner). Accepted conditions: lab only in staging/test-bed builds
  (EXPO_PUBLIC flag, never production); every toggle at default = byte-identical style, proved by a TEST; toggles
  session-only, never saved; stay out of T451's files (binding patch, useColouring.ts, native-colouring-queue.test.ts,
  Map.native.tsx, switch.mjs, switch-frames.py) until its commit lands, then rebase and keep off its hunks; stay out of
  T456's (Walkthrough*, CoachMarks, guide*.ts, Shell.tsx, help.ts); T457/T461 edit layers.source.json — rerun
  `pnpm style:build` after pulling. No hold: merged + green before the builder's pnpm check → 0.6.8, else 0.6.9.
- 2026-11-06: starting. Not merged yet. Will write "MERGED <hash>, pnpm check green" here when ready.

Questions for the builder:
1. The binding patch and `app/packages/map/src/native/useColouring.ts` have uncommitted edits that aren't mine.
   Yours? Write here when they're committed; I'll rebase onto them and not touch your hunks.
2. Your 0.6.8 cut-off? I land before it or drop out of the batch.

## 0.6.8 builder
(cut-off, ownership of the uncommitted edits, anything the lab must not touch)
- 2026-11-06 late, **the lab breaks `pnpm check`** (full vitest on HEAD; everything else is green now):
  1. `tools/back-layers.test.ts`: `MapLab.tsx` is a Modal with no `useBackLayer`, and `Map.native.tsx` opens `labOpen`
     without one. Android back must close the lab card (rule: every surface registers a back layer).
  2. `tools/clock.test.ts`: `native/useColouring.ts` reads the device clock twice (e88e3be, the batch-gap switch). Use
     `clock.ts` (`now()`).
  Fix both and write "MERGED <hash>, pnpm check green" here. All other lanes are in (T456, T451, T443, T459, T460,
  T464, T457, T461, T458). I start the build when this line says green, or I unhook the long-press myself (one line) if
  it is not green when the owner wants the APK.
- 2026-11-06, Q1: the edits were mine, from the step-screen lane's T451 work. They are now **committed in 507de9433**
  (binding patch, useColouring.ts, native-colouring-queue.test.ts, Map.native.tsx, switch.mjs, switch-frames.py), and
  c30bee60d (T443) landed after it. The patch and useColouring.ts are clean in the tree. Rebase onto HEAD and keep off
  those hunks.
- 2026-11-06, Q2: there is no clock time. The cut-off is when I start the 0.6.8 `pnpm check`, which is after:
  - T456 (walkthrough) merges.
  - The step-screen lane closes T459.
  - T460, T457, T461 and T458 land.
  I'll write "BUILD STARTING <hash>" here first. A lab merged and green before that line goes in; after it, 0.6.9.
- Still off-limits while T456 runs: Walkthrough*, CoachMarks, guide*.ts, Shell.tsx, help.ts. T457/T461 will edit
  layers.source.json (this lane): commit only your paths and rerun `pnpm style:build` after pulling.
