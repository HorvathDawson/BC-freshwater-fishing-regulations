# T1125 — One-bank closures (T1029): the leftovers

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1125 | One-bank closures (T1029): a bank place that cannot be drawn, creeks with no side read, tests, the device walk | OPEN — 0.10.1 | | owner answers item 1 (Nakina) from pictures; item 2 answered 2026-11-27 (T1029) |

Source: the 0.9.1 chat's T1029 lane (PR #112, look A, merged 2026-11-25), sent to the planning chat 02:12. Not
checked by the planning chat. Related: T1029, T1074 (rule 28 and runtime map layers).

1. The Nakina bank place is dormant (no chain of its own) and draws no cue; nothing on the map says a one-bank rule
   exists there. Decide what the map and the page say for a bank place that cannot be drawn.
2. A creek whose side could not be read is kept and printed with a `!!` line. Whether such a creek is shown closed
   or open is a proposal. ANSWERED 2026-11-27 (T1029): keep it closed, and the build warns when one first appears.
3. The CLI print and the `report.json` lines have no test.
4. Not verified: MapLibre Native's `line-gap-width` on a device, and the device walk on Android and iPhone (look A
   was checked as built = shot on the web renderer).
5. A previous lane had left 28 `data/` symlinks pointing into the main checkout's data (removed, nothing written
   there). The lane brief says: no symlink into the main checkout's `data/`; T1112's setup command is the only way
   in.

Acceptance: 1 and 2 answered by the owner from pictures and built; 3 tested; 4 a device picture per platform.
Severity: medium (2 can show a closed bank as open or the reverse; count unknown). Release: 0.9.2. Model: Opus for
1–2, Sonnet for 3–4. Size: M. Owner: yes (item 1; item 2 answered 2026-11-27).

## Owner

- [ ] 2026-11-25 — Pick, from pictures, what the map says for a bank place that cannot be drawn (Nakina).
- [x] 2026-11-25 — Creeks whose side could not be read → "keep it closed (for trib rules i assume) but one thing to note we should add a thing that warns us if this is ever a thing" (2026-11-27, T1029)
