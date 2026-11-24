# T909 — Web app: no bell and no reload in the search bar

Status: DONE — shipped in 0.8.3 (merged 96d9e4aab; staging + production 2026-11-19).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T909 | Web app: no bell and no reload in the search bar (owner) | DONE — shipped in 0.8.3 (staging + production 2026-11-19) | 96d9e4aab | owner 2026-11-19 ~07:20 |

Owner 2026-11-19 ~07:20 (main chat), verbatim: "also staging still has the bell and reload but bell does nothing on
webapp?" Asked (dialog: remove both on web / bell only / make the bell work on web); owner picked "Remove both on web
(Recommended)".

## Work
The web build's search bar shows neither the bell nor the reload button; phones keep both. Web and native split by
FILE (AGENTS 34a), never a Platform.OS branch in shared code. What's new stays in More → About on the web (T888). Check
nothing else on the web points to the bell (help/tour steps T293/T824 that highlight it, the coach marks, surfaces
tests T60) and that the search bar's width/layout is right without them (390 and 1440). Render tests both platforms.

## Built (2026-11-19, lane app/t909-web-bar)
File pair `BarControls.web.tsx` / `BarControls.native.tsx`, typed by `BarControls.d.ts` (same surface: `BarControls`,
`BAR_HAS_CONTROLS`). Native draws the bell and the refresh button as before; web draws nothing. `TopBar.tsx` renders
`<BarControls>`; the shared pieces (TOP_BAR, SyncButton, SyncGlyph, BellGlyph) moved to `barParts.tsx` (re-exported by
TopBar) so the platform file does not import the bar. No `Platform.OS` added. Walkthrough: no step targets "bell" or
"sync" (T842's six steps), asserted for web and app. Help: `helpVM("web")` drops the bell section (the refresh section stays:
the More tab keeps its refresh button). Tests: `BarControls.test.tsx` (web: neither drawn, no tour step), 
`BarControls.native.test.tsx` (phone: both drawn); SyncButton/InSeason bar tests now mock the native half.
`pnpm check:fast` passes (platform, boundaries, knip, tsc). Pictures: scratchpad t909-shots (the fixture served no feeds,
so "before" also shows no controls: it proves layout only, not the removal; the render tests prove the removal).
