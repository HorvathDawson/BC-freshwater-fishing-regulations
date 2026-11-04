# Batch 2 chat: start here (memory, crashes; then batch 3)

Runs beside the UI chat (`NEXT-SESSION-UI.md`, T367). Read `handoff/PLAN/STATUS.md` (top entry: chat 4) and
`handoff/PLAN/RELEASE-PLAN-0.5.0.md`.

## Paste this to start the chat

> Read `handoff/PLAN/NEXT-SESSION-BATCH2.md`, the STATUS.md top entry and RELEASE-PLAN-0.5.0.md, then carry on with
> batch 2 (memory fixes, T351) and on into batch 3. A separate UI chat runs at the same time: stay out of its files as
> the handoff says. Start ticket work without asking; ask me only for decisions. Keep updates short and STATUS.md current.

## The two chats never collide
| | this chat (batch 2 →) | UI chat (T367) |
|---|---|---|
| files | `app/packages/map`, `app/packages/data`, `app/packages/ui/src/{waterMap,route,startLink,place,crashReport}.ts`, `ui-native/src/regs/{synopsisPdf*,PdfViewer*,pdfView,appendPdf}`, `ui-native/src/water/*` logic, `ui-native/src/{Shell,part}.tsx`, `apps/mobile/*`, `pipeline/`, `app/tools/testbed/` | `app/packages/core/src/**/words.ts`, regs / Conditions / More **presentational** components in `ui-native` (layout, text, spacing) |
| heavy jobs, emulator, builds, releases | YES (scripts/heavy.sh, one at a time) | NEVER |
| dev web app :39218 / data server :39217 | may restart only if down | uses them; restarts only if down |
- Both commit with `git commit -- <paths>` (never `git add -A`). A file the other chat needs: ask the owner first.
- If a UI change needs a logic change in this chat's files, the UI chat writes it as a note in T367; this chat does it.

## Where batch 2 stands (2026-11-03, chat 4)
- **Rule book (T354):** opens in the app at the page, highlight drawn (appearance stream; source lines carry boxes),
  header under the window insets, back closes it. In-app timing: book 3 ms, copy 6–39 ms, drawn ~400–700 ms. DONE
  except a real-phone look.
- **Coquitlam (T345):** sliver / past-its-bounds / web highlight / reload colouring FIXED (8a5b5bd). LEFT: the 0.4.0
  map-tap → reopen crash, not reproduced; confirm on the owner's phone with 0.5.0 (crash/2 carries water + pane).
- **Reports (T352):** crash/2 + report/2 in code. **The staging worker is older and REJECTS crash/2 (400)**: no
  crash/2 is stored until it is deployed. The deploy waits for 0.5.0 (SALMON_NEEDS) unless the owner says otherwise —
  ask once. A kept fatal crash is resent each start until accepted (intended; do not drop it).
- **Place links (T369):** code done, web-only `lines` file (b6588a3). NEEDS the rebuild `scripts/heavy.sh deliver 30
  -- python -m pipeline.deliver all` (reason: T369 `section_measure` → `lines`), planned WITH batch 3's release (one
  rebuild). Until then the bundle-backed `test_release` tests fail with "no section_measure" — expected.
- **Memory (T360)**, emulator numbers in the ticket. NEXT, in order:
  1. Conditions adds ~130–150 MB native over a water page (not a second map: one map is shared). Split it: toggle the
     flow colouring / gauge layers / chart one at a time in the `memory` scenario and measure.
  2. The PDF viewer keeps ~35–40 MB after closing (trim frees nothing): free it on close.
  3. Nothing answers `onTrimMemory`: on a trim, drop decoded release caches (data/src/release/loader.ts) and close idle
     SQLite handles.
  4. OpenGL vs Vulkan is 5–25 MB on the emulator (host GPU): the SIGSEGV question needs a real phone (owner).
  Then **T351** (the 406/435 MB native crashes) with what the fixes show.
- Then batch 3 (DFO salmon clean-up, T356/T358/T357/T338/T343/T366) and its release (with the T369 rebuild).

## Test bed (app/tools/testbed)
- `scripts/heavy.sh testbed 30 -- bash app/tools/testbed/session.sh --build --local [--opengl] --max-min 30 -- node
  app/tools/testbed/run.mjs memory|water-nav --out app/tools/testbed/out/runs/<name>`
- `--local` reads this machine's release over 10.0.2.2:39217 (no update channel); `--opengl` builds the other map
  variant (app.json restored after). Scenarios: `memory` (per-step meminfo + JS heap from the `[heap]` probe, rule-book
  open/close/timings), `water-nav` (Coquitlam; `NAV_VIA=open|tap`; the map-tap target still misses).
- Maestro adds seconds per tap: time things from the app's own log lines (`[synopsis] …`, `[heap] …`), not the step.

## Owner rules in force (memory)
No silent fallbacks, every shown failure reported (T368, batch 6c); one load path per screen (T369); better package
over minimal; ask before filtering data content; parser runs are human-only; prod is human-only.
