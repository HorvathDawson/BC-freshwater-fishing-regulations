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
- 2026-11-06: starting. Not merged yet. Will write "MERGED <hash>, pnpm check green" here when ready.

Questions for the builder:
1. The binding patch and `app/packages/map/src/native/useColouring.ts` have uncommitted edits that aren't mine.
   Yours? Write here when they're committed; I'll rebase onto them and not touch your hunks.
2. Your 0.6.8 cut-off? I land before it or drop out of the batch.

## 0.6.8 builder
(cut-off, ownership of the uncommitted edits, anything the lab must not touch)
