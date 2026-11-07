# T472 — In-season change: one collapsed line above the species table, not a wall of text at the top

Status: DONE 2026-11-07 (owner: "this is a big wall of text to get when you open a regulation page.. (because of the inseason notification) … maybe a line saying there are inseason changes click here to see").

Was: the full notice card(s) (T230) opened expanded at the top of the Keep tab, above Open/closed and the year bar.
Now: `InSeasonBanners` is a card LINE (attention bar + tint, 44 px) — "In-season change (since May 16, 2025) · see ›" for one,
"3 in-season changes · see ›" for several (`inSeasonSummary`, core) — drawn by `KeepTab` after the status, year bar and leading
notes, just above the species table. A press opens the full notices under it, a second closes them (the Access strip's
pattern, `aria-expanded`). Never hidden, no fallback; nothing in effect, nothing drawn.

Tests: `InSeason.test.tsx` (collapsed, expanded, several, placement in KeepTab), `core/inseason.test.ts` (summary).
Shots (Fraser River, 390 and 1440): scratchpad `t472-{before,after}-{390,1440}.png`.
Size: S.
