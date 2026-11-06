# T439 — Retire golden.json and the tests that only compare against it

Status: OPEN (owner 2026-11-06: "the golden as before is bad and we should retire it and the tests with it").

## Why
`app/fixtures/golden/golden.json` (with `pins.json`, `tokens.json`, `inventory.json`) was extracted ONCE from the
regulations prototype (`app/design/regs-v11.html`, v38), deleted in T87. `sync.mjs` calls it FROZEN, but it was
hand-edited for wording rulings up to 2026-10-28 and carries 19 `until:` / 14 "never" skips (audit 2026-11-05, tests
finding 11). It is neither the prototype's truth nor the pipeline's, and re-cutting it from today's answers would
only prove "today equals today". What stays as the oracle: the release-backed `taps.json` / `hydrate.json` (re-cut by
`sync.mjs` per release), the pipeline's card gate (`pipeline.deliver card_check`) and the Python answers tests.

## Work
1. List every reader of golden.json / pins / tokens / inventory (~56 files under `app/conformance/src/regs`, plus
   `fixtures.ts`, `golden.ts`, `goldenSlices*`, `oracles.ts`). For each test: golden-only (delete) or it also checks
   something real (keep; move that check onto taps.json / hydrate.json or a unit fixture).
2. Anything a deleted test checked that nothing else does: listed in this ticket, and moved or recorded "no" with why.
3. Delete the frozen files and their sync/notes; `sync.mjs` keeps only the release-backed re-cut.
4. `pnpm -C app check` (or the conformance + knip steps) green; knip finds no orphan.

Size: M. Owner: no.
