# T245 — low-end Android performance

Status: OPEN — not started.

Source: owner 2026-10-28 — approved proposal P11 in handoff/PLAN/proposed-tickets-2026-10-28.md ("approved ALL 16").

## Work
Profile on an older phone / emulator profile (2–3 GB RAM, Android 10–11): cold start to first paint, memory peak,
map FPS while panning at z8/z12, regs page open time. Set budgets (default: cold start < 4 s, memory < 400 MB, pan
≥ 45 fps) and fix the top offenders (style swap cost, feature-state writes, JS bundle size, sql.js memory).
## Acceptance
A measured table before/after on the profile; budgets recorded and checked by a repeatable script.
Depends: T50/T74 (native engine). Size: M. Owner: no (an old phone helps).
