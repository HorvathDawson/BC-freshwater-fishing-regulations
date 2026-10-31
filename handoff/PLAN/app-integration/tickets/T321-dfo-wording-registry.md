# T321 — DFO limits wording as a closed registry: unknown wording is held, shown in DFO's words, and alerts urgently

Status: DONE (b52942134, 86476b6e2) — rides the next release + a feeds-worker deploy (owner 2026-10-31: "It should be urgent yes. Because it could mean a quota changed but with a new wording.
Also display a specific waters unparsed items if they exist (wording change happened). Yes make this a ticket.")

## Why
T320's guard is "every NUMBER in a limits cell is read". A restriction in WORDS passes as fully read:
- "one hour before sunrise to one hour after sunset" (14 rows): the hours are dropped.
- "N coho per day in the aggregate" (6): read as a plain daily limit.
- "(see exception below)" (8): the pointer is dropped.
A notice that changes a quota in new wording could pass the same way.

Baseline: 75 saved DFO region web pages (12 test fixtures, 10 cached today, 53 archived 2017–2026) → 267 distinct
cells, 85 clause templates (numbers blanked). The list is in `T321-wording-baseline.txt`; the generator is
now `pipeline/regs/dfo_salmon/wording.py build` (the registry builder).

## Work
1. **Registry** (`data/curated/dfo_salmon/wording.json`, one file both readers load: pipeline `typed`/`quota` and the
   feeds worker TS port): each clause template (numbers → N, FN → FN#, normalised spaces/case/dashes) → its meaning:
   a structure field (daily, hatchery, over, wild, monthly, max_cm, hours, gap, gear…), `note` (shown in DFO's
   words on the card), or `provenance` (FN numbers). Seed with the 85 baseline templates; each is reviewed, not
   bulk-accepted (everything-added-is-a-proposal).
2. **Close the gaps:** hours as words → structure (sunrise/sunset); "in the aggregate" and "see exception below" →
   notes on the card.
3. **Unknown clause = unparsed row:** any clause not in the registry → the row is flagged (`review_reason` "new
   wording: '…'"), its verdict `q.u`, the app shows DFO's words for that row, never a number.
4. **Water page shows its unparsed items:** a water whose DFO rows include an unparsed one shows a visible block on
   its card: "DFO changed the wording here — read their text" + the row's species, dates and DFO's words.
5. **Urgent alert (feeds worker):** a new unknown clause on a live run → URGENT owner email: the wording, the place,
   the region, the FN, the full cell, and what to do (add a registry entry + test line). Once per new template, not
   per run.
6. **Tests:** every saved page reads with zero unknown clauses; a mutation test (an unseen clause in a known cell →
   flagged + alert); TS-vs-Python parity on the registry; the three gap wordings.

Depends: T320. Size: M. Owner: review the seeded registry entries (proposals).

## Done
- **Registry** `data/curated/dfo_salmon/wording.json` (`dfo-wording/1`, one entry a line): **85 templates, all
  `status: proposed`** — 67 structure (`reads` names what reads it), 15 note, 3 provenance. Builder / checker:
  `python -m pipeline.regs.dfo_salmon.wording build|check` (rebuild keeps every entry's kind/reads/status, refreshes
  counts, adds unseen templates as proposals). A `rejected` entry reads as unknown. Python `wording.py`, Worker
  `src/dfo/wording.ts` (imports the same JSON; `resolveJsonModule` in the worker tsconfig).
- **Unknown clause = unparsed row:** `typed.to_rules` (+ `typed.ts`): review_reason `new wording: '…'` on the limit rule
  (or the gear rules when there is none) → verdict `q.u`, no number.
- **Gaps:** hours in words → `when.hours` solar clocks (`_RE_SOLAR_TIMES`, residue + `read_limits`/`render` too);
  "in the aggregate" and "(see exception below)" are `note` entries → the row is shown in DFO's words on the card.
- **Rows in DFO's words:** typed feed locators carry `words` (`{k: new|unread|note, sp, d, t}`); live feed
  `locs[lid].x` (Python + Worker, carried through holds); release `salmon` payload `x` (only when non-empty; skipped
  under a header closure). Core `salmonCard().dfoWords` → `SalmonCard` draws a boxed "DFO changed the wording here —
  read their text" block (species · dates, DFO's words) for new/unread rows, and a "DFO's words for this water" list for
  note rows. Today's pages: 4 note rows (5B ×2 "see exception below", R6 "unless otherwise stated", R6 F. Fraser), 0 new.
- **URGENT alert:** live `wording_notices` / `wordingNotices`: key `dfo-wording:<template>` (once per template, ever),
  text + email data (clause, place, region, FN, species, dates, full cell); `email.ts` renders it with the steps
  (read DFO's words, add the registry entry + a test line, redeploy). `DFO_REV` bumped so cached regions re-type.
- **Tests:** `pipeline/tests/test_dfo_wording.py` (13: zero unknown on every saved page, registry shape, builder keeps
  meanings, rejected = unknown, the 3 gaps, the mutation incl. a live run told once); worker `test/dfo.wording.test.ts`
  (TS == Python over every fixture cell + mutants, flag, live run → email, told once); core `salmon.test.ts`,
  `salmonFeed.test.ts`, `SalmonCard.test.tsx` additions. `dfo.worker.test.ts`'s bad-row mutant now spells "5mm" as
  DFO does (it was also a new wording).
- **T320's 2 failures** in `test_dfo_salmon.py` (`test_suggestions_are_recorded_but_never_applied`,
  `test_a_locked_water_is_never_overwritten_by_a_later_match_run`) are NOT T320's: they fail before cf5301f59 too and
  start at a97d9e8dc (Sheldens Creek bound — the last unbound DFO water carrying suggestions). The tests need a water
  that is unbound; none is left in the curated files. Left as is (data-driven tests; fix = build a synthetic unbound
  water in the test).
- **Runtime input (10-30):** the registry is no longer redeploy-bound. `deploy/staging/sync-inputs.sh` uploads
  `data/curated/dfo_salmon/wording.json` to `private/feeds/dfo/wording.json`; `runDfo` reads it once per run (priv
  `wording.json`, `readRegistry`) and passes it through `typePage` -> `build` -> `toRules` / `rowWords`. Missing or
  invalid -> the compiled-in import (kept as the fallback) + a warning in the log and `DfoResult.warnings`. A changed
  copy (hash in `inputs.json`) re-types every region, so an accepted entry clears cached verdicts. Alert logic
  (`wordingNotices`, once per template) untouched. Tests: R2 copy wins, missing/invalid falls back, new entry stops the
  alert. Owner: run `deploy/staging/sync-inputs.sh` after each registry edit; the worker needs one deploy to pick this up.

## Owner
- Review the 85 proposed registry entries (kind / reads) — especially the 15 notes.
- Ship: `python -m pipeline.regs.dfo_salmon.feed`, release build (scripts/heavy.sh), and the feeds worker deploy.
