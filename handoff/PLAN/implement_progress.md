# Implementation progress (IMPLEMENT-HANDOFF.md)

Restart here. Update this file after EVERY step: what is done, what is running, and the exact next action.
Logs from timing runs live in the session scratchpad, which a reboot wipes. The numbers that matter are copied here.

## Owner answers (2026-10-20)
- Heavy jobs: allowed now for the day-1 baseline (via heavy.sh, one at a time, ≤4 workers).
- Order: **P00 first**, then P05 + P03 (then P06, N1 scaffold).

## §7 First day
| step | state |
|---|---|
| 1 read docs | AGENTS.md + IMPLEMENT-HANDOFF read; APP-PREP §0–§1, MINIMAL-PACKAGING, integration README still to read before P05/P03 |
| 2 log / pause / go-ahead | HEAD 7a326133; UR7 committed; no `heavy-job.pause`; go-ahead given |
| 3 reader files | ALREADY DONE in a4b37eff (`app/design/regs-v11/port/reader/`) |
| 4 baseline | IN PROGRESS: pytest fast suite timed (`--durations=0`); then `pnpm -C app check`, vitest per-file, check.mjs, card gate |
| 5 golden re-sync | NOT NEEDED: golden.json meta ref 7fb6809f, v38, misses unchanged (4) |
| 6 first tickets | P00 → P05 + P03 → P06 → N1 |

## P00: test tiers
- [ ] measure: pytest fast (durations), pytest slow (collect count only), vitest per file, `pnpm check` per step, check.mjs total + per round, card gate
- [ ] pytest `smoke` marker (registered in pytest.ini) on ~1 test per gate/invariant, no live-bundle scans
- [ ] vitest smoke filter/project
- [ ] check.mjs `ONLY=smoke` (NOTE: check.mjs is in the prototype lane, rule 12; P00 asks for it explicitly, so the edit is additive only)
- [ ] `scripts/test-smoke.sh`, `scripts/test-full.sh`; one line in AGENTS.md
- [ ] commit with a before/after time table

### Measurements
(fill in)

## Baseline counts
(fill in)

## Not verified
(fill in)
