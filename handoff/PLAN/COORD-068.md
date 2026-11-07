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
