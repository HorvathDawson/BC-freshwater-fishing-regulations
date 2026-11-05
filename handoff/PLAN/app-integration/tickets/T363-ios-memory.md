# T363 — iOS memory footprint

Status: DROPPED — merged into T88 (2026-11-05 audit; the work continues there) — was: PARTIAL 2026-11-05 — 089c8a00f (memory scenario), bf8ca3820; simulator measured (ios-t313g); the real iPhone is T364's run.

Run T360's per-screen measurement on the iOS Simulator (boot, pans, water sheet, Conditions, Salmon, rule-book
PDF), then on a real iPhone once T88 gives a signed build. Simulator showed RSS 420 MB, 665 MB peak panning (T313).
Apply T360's fixes and check they help on iOS too. Size: S–M.

## Simulator, 0.5.0 (ios-t313g, 2026-11-05; `run-ios.mjs memory`, MB)
`footprint` of the simulator process from the Mac: the FOOTPRINT is what iOS's memory limit (jetsam) reads; malloc is the
native heap (Hermes external bytes, MapLibre, SQLite); RSS counts shared pages too. GPU memory is the Mac's here (footprint
lists no IOSurface/IOAccelerator: graphics 0), so a phone adds its own GPU memory on top. JS from the [heap] probe.
| step | footprint | malloc | RSS | JS heap | JS external |
|---|---|---|---|---|---|
| boot | 147 | 58 | 497 | 40 | 47 |
| 8 pans | 170 | 61 | 541 | 44 | 57 |
| satellite on, 4 pans | 178 | 62 | 561 | 52 | 60 |
| water page (Cultus) | 232 | 60 | 670 | 108 | 30 |
| rule book open / closed | 290 / 241 | 122 / 65 | 884 / 866 | 108 | 30 |
| rule book 2nd open / closed | 238 / 241 | 70 / 65 | 896 / 877 | 108 | 30 |
| Conditions | 265 | 71 | 914 | 108 | 43 |
| Map tab after Conditions | 270 | 70 | 923 | 116 | 45 |
| Salmon | 273 | 69 | 928 | 116 | 50 |
| after a memory warning | 273 | 69 | 928 | 116 | 50 |
(First run, t313g before the fix, agreed within ~10 %: boot 153, water page 229, rule book open 286, Conditions 234.)
Readings:
- The footprint stays ~150–290 MB: under Android's PSS (257–412, T360 round 6), and far under an iPhone's limit
  (roughly half the RAM on 6 GB phones). RSS (450–930) is not the number iOS judges.
- The rule book (PDFKit, T354) costs ~50 MB while open and gives it back on close; a second open + close holds the same
  (241 → 241): no leak — the Android pdfium leak does not exist on iOS.
- The water page is the biggest step (+54 footprint, JS heap 52 → 108: the decoded regs), as on Android; it is shared JS.
- Conditions adds only ~25 MB on iOS (Android +100 native): the Android cost is MapLibre-Android's, not the app's JS.
- **A memory warning frees nothing** (273 → 273): Android's trim handlers (T360: MemoryTrim, SQLite shrink, decoded caches)
  have no iOS listener. Small today (nothing near the limit); a follow-up only if the real iPhone (T364) is near its limit.
Left: the same on a real iPhone (T364 / T88 build), where GPU memory counts.
