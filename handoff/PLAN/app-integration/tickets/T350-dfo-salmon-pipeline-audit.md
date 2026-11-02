# T350 — Quick audit: DFO salmon pipeline unified with the freshwater (trout) model, no fallbacks

Status: OPEN, starts after T349 lands (owner 2026-11-02: "a quick audit of the salmon DFO pipeline to make sure we don't
have bad datatypes or tech debt deviating from other datatypes like the trout freshwater ones … unified … solid … no
weird fallbacks").

**Scope:** pipeline/regs/dfo_salmon/*, pipeline/deliver/salmon.py, app/workers/feeds/src/dfo/*, app/packages/core
salmon*.ts, ui-native SalmonCard / RegionRules, and the review app's DFO / cascade backend.

**Look for:**
- **Data types that differ from the freshwater ones for the same idea:** quota / size / season (`when`) / gear /
  species codes / extents / water binding / tributaries / notes / provenance.
- **Two code paths for one answer:** Python vs TS beyond the tested parity, duplicate parsers, a second cascade.
- **Silent fallbacks:** defaults that hide missing data, broad excepts, "if missing use X", a stale cache read
  (e.g. the `item_ids` cache, T337), env overrides.
- **Dead fields and code, leftover versions,** tests that can't fail.

**Output:** a ranked findings list (file:line, why, fix), each checked by a second read. Small, safe fixes are applied
with targeted tests. Bigger ones become tickets.

Size: S–M, kept lean.

## Principle (owner 2026-11-02)
"All cascade logic should be resolved in Python, then distributed to the front end in a way that is cheap to store,
easy to implement and quick to decode."
- Check that no cascade, default or quota resolution lives in TypeScript. That covers the app (core salmon*.ts) and
  the feeds worker's live path (dfo/typed, quota, salmon, live .ts).
- The worker exists to pick up DFO changes between releases without Python. Find the leanest split that keeps the
  rule there: e.g. the release ships the resolved chain/scope per section plus a fingerprint → rule-slot table, the
  worker only swaps rule TEXT/numbers per fingerprint, and the app picks the slot.
- Propose the format (sizes, decode cost) and the migration.

## Two passes (owner 2026-11-02)
- **Now (this cycle), LEAN.** Run right after T349 lands: one agent, read-mostly. Only the top findings (data types
  that differ from freshwater, cascade logic still in TS, silent fallbacks). Fix only the small, safe ones; ticket the
  rest. Targeted tests only, no rebuild.
- **Next credit cycle, IN DEPTH.** The full pass, with the T333 Fable audit, including the worker split proposal for
  "cascade resolved in Python".

## Input from T349 (2026-11-02): cascade logic the feeds worker still runs in TypeScript (left as is)
T349 put page defaults, non-retention = closed and shared quotas in Python only. The worker was NOT changed, and its byte
parity suites (`test/dfo.parity.test.ts`, `test/dfo.corpus.test.ts`) are `describe.skip` until this ticket decides the
split. What `app/workers/feeds/src/dfo` resolves itself today:
- `typed.ts` / `feed.ts`: limits cell → typed rules (take, may_target, sub-limits), per fingerprint. Does NOT emit the
  page-default locators (`cascade.statement_locators`).
- `entries.ts`: reconcile (fingerprint → location, drift / new / dormant). Reports T349 page defaults as "dormant"
  (no row on the page).
- `live.ts` `join`: rules by location. Does NOT mark shared quotas (`quota.mark_shared`), and puts no `page:` locator.
- `salmon.ts` `locationCalendar` / verdict: per species and day the narrowest location's rule; `open` / `release` /
  `closed` / `check`. **Still answers `release` for plain non-retention** (Python: closed unless the row says catch and
  release).
- `quota.ts`: `speciesTree` (no aggregate tier for R5B), `bucket` (no `shared` Squamish tier), `headerClosure` (the
  header's closure wins over the table).
- `header.ts`: header notes.
The app core composes a section's live answer from the release chain's location order plus the feed's per-location
calendars (`withSalmonFeed`). So until the worker is redeployed on a Python-resolved format, a live feed can show
`release` where the release says `closed`.
