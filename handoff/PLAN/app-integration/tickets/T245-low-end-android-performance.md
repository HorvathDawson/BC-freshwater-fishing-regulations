# T245 — low-end Android performance

Status: DROPPED — merged into T360 (2026-11-05 audit; the work continues there) — was: PARTIAL 2026-11-05 — 49e303e96, d91f300f6 (z8/z12 measured; pan fps fails on the emulator; a real low-end phone left)

Source: owner 2026-10-28 — approved proposal P11 in handoff/PLAN/proposed-tickets-2026-10-28.md ("approved ALL 16").

## Work
Profile on an older phone / emulator profile (2–3 GB RAM, Android 10–11): cold start to first paint, memory peak,
map FPS while panning at z8/z12, regs page open time. Set budgets (default: cold start < 4 s, memory < 400 MB, pan
≥ 45 fps) and fix the top offenders (style swap cost, feature-state writes, JS bundle size, sql.js memory).
## Acceptance
A measured table before/after on the profile; budgets recorded and checked by a repeatable script.
Depends: T50/T74 (native engine). Size: M. Owner: no (an old phone helps).

## Measured (2026-10-29)
Check: `node app/tools/testbed/run.mjs perf regs-open` in a test-bed session (`scripts/heavy.sh … session.sh -- …`).
`perf` FAILS a step when a number is over its budget, and writes `T245-perf.json` beside the report.
Budgets (P11 defaults): **cold start < 4 s, memory (TOTAL PSS) < 400 MB, pan ≥ 45 fps**. Regs open is T286's scenario
(its own ≤150 ms target).
Profile: `n1_pixel` emulator, 2 GB RAM (2.47 GB reported), API 35. No Android 10–11 image is installed. Release test-bed
APK, staging r-164ca35636ea (the T280 basemap). Two runs.

| measure | run 1 | run 2 | budget |
|---|---|---|---|
| cold start, launch → tab bar on screen (±1 s: a screen read) | 3.77 / 3.27 / 3.58 s | 3.96 / 3.84 / 3.39 s | < 4 s ✅ (tight) |
| activity drawn (`am start -W`) | 286 / 260 / 262 ms | 292 / 239 / 248 ms | — |
| PSS after start | 235–267 MB | 259–307 MB | — |
| PSS peak (after panning) | 345 MB | **390 MB** | < 400 MB ✅ (tight) |
| pan fps, start view ~z10 (map surface while moving) | 35.5 (satellite, left on by an earlier run) | 31.5 / 25.4 (drawn map, two passes) | ≥ 45 ❌ |
| regs open, map tap → first frame (cold / warm) | small 281 / 70 ms, lake 131 / 92 ms | — | T286 |

Read:
- **Cold start and memory pass**, both close to the line. Memory climbs ~130 MB while panning (start ~260 MB → 390 MB):
  tile and glyph caches. It is the number to watch on a real 2 GB phone.
- **Pan fps fails on the emulator**, but the emulator's own frame floor is the limit. p50 is 33 ms (30 fps) here, against
  16.7 ms in T280's runs earlier the same day: the host was running 5 lanes. The JS thread is not involved (T273). So
  this is not a measure of the app on a phone. Nothing points at an app-side offender to fix (style swap, feature-state
  writes, sql.js: none show), so no app code was changed.

Left (follow-up):
1. Pan at z8 and z12. adb has no pinch, a scripted double-tap lands as a tap (it opens a water), and a search preview's
   framing is undone when the search closes. Needs a camera deep link or a test-bed-only zoom hook.
2. The same `perf` run on a real low-end phone (or an Android 10/11, 2 GB AVD) on an idle host. Pan fps can only be
   judged there.

## Pan at z8 and z12 (2026-11-05, `perf`, run t312-a; camera link d91f300f6)
| where (Fraser Valley, 8 travelling swipes) | fps moving | p50 / p95 / p99 | frames > 50 ms | PSS peak |
|---|---|---|---|---|
| start view (z 9.9) | 44.6 | 16.9 / 35.4 / 65.2 ms | 15 | 294 MB |
| **z8** | **29.4** | 33.3 / 51.4 / 117 ms | 50 | 326 MB |
| z12 | 44.8 | 16.8 / 35.1 / 51 ms | 13 | 318 MB |
Cold start 3.9 s (budget 4 s), memory peak 326 MB (budget 400). z9.9 and z12 sit at the budget line (45 fps); **z8 is the
slow zoom** — many basemap/atlas features per tile (T273: the basemap's green-space polygons were the hitch). Lowering it
is a visual change (e.g. no wood/forest fill below z9, or the atlas's small streams from z9): the owner's choice, and
only worth deciding after the same `perf` run on a real low-end phone (emulator fps is not a phone's).

## Owner 2026-11-05: no lighter map at z8
The z8 pan (29 fps on the emulator) is NOT to be fixed by drawing less at z8 ("No to this"). Any z8 work must keep the
map as it looks; a real low-end phone run still decides whether it matters.
