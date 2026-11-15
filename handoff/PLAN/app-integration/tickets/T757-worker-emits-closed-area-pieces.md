# T757 — Feeds worker and app: closed areas inside a DFO place (T573's pieces) are live locations, never read as open

Status: OPEN (iOS chat; waits for the Android chat's spec `T573-files/worker-handoff.md`, which lands with branch `data/t573-closed-area-build`).

Source: Android chat 2026-11-15 (its pipeline lane's T573 build step), agreed between the two chats the same day.

## Why
T573's build step makes each confirmed closed area inside a DFO place (the Thompson's Deadman / Juniper / Ashcroft
year-round no-fishing areas, the Babine circles, the middle Shuswap stretch) its own DFO location
`<water_id>#closed:<key>` in the reach run, the bundle and the Python live feed. It wins on its sections by the
existing narrowest-first order. The worker's live feed does not emit those locations yet, and the app skips a chain
location the feed lacks. So a release with bound pieces beside a worker that does not emit them would show a closed
area as OPEN, without a word. Nothing binds today (the owner has confirmed no piece), so nothing is wrong now.

## Work
1. **Worker (`app/workers/feeds`):** the live DFO feed emits `locs[<piece id>]` for the pieces, a port of the
   pipeline's `derive/piece_key`. Tested against the spec's JSON fixtures from the Python live feed (confirmed piece
   on today's row; unconfirmed; confirmed but dropped by DFO today) and its id grammar, with an escaping test.
2. **Worker `/status.json`:** a field that says the feed shape it emits (so a release step can check it). Name and
   value from the spec.
3. **App:** a release-bound closed-area piece that the feed lacks reads "not known, check the notice" and is reported
   (`reportFailure`); it never falls through to open (W46). The app knows a location is a closed-area piece from the
   bundle's marker (the pipeline lane adds it and names it in the spec), not by parsing the id. Words in
   `packages/core` with whole-sentence tests.
4. **Not this ticket (the Android chat's pipeline lane):** the shared release step refuses a release with any bound
   piece when that environment's worker status lacks the field or cannot be read; the bundle marker; the fixtures and
   their regenerate command.

## Acceptance
- The worker's `locs` for the fixtures equal the Python live feed's, piece for piece.
- With a bound piece missing from the feed, the app shows "not known" and reports; no path shows open.
- Deployed to staging only together with the Android chat's word, on top of `ios/staging-worker` (push + crash limit
  are what is deployed there now). No release with bound pieces before that.

Depends: the spec; T573's branch. Size: M. Production stays the owner's.

## Results (worker lane)
Branch `feeds/t757-worker-pieces` (not merged). Work items 1 and 2 done; item 3 (the app) is not this lane's.
- **Port:** `app/workers/feeds/src/dfo/closures.ts`, a line-for-line twin of `closures.py`'s read side (`derive`,
  `says_closed`, `areas`, `inline_items`, `closure_entry`, `piece_key`, `_inline_key`, `piece_id` (throws outside the
  grammar), `piece_rule`, `OWNER_READINGS`, `live_piece_rules`); sha1 is the worker's own `hash.ts`; Python's
  `f"{r:g}"` is `py.ts formatG`. Wired after the join in `live.ts` (`rules.set` per piece, so the header closure and the
  calendar are the places' own path). `DFO_REV` bumped.
- **`/status.json`:** top-level `"dfo": {"pieces": 1}` (`status.ts DFO_SHAPE`, from `closures.ts PIECES_SHAPE`), on the
  503 too, as `inputs`; `pieces_gate.py` reads `dfo.pieces` (int >= 1).
- **Tests:** `test/dfo.closures.test.ts` (the three oracle cases piece for piece; the id grammar against the Python's
  answers; the two typing slips and `:g` with Python-computed values; first entry wins; runDfo over the saved pages:
  17 pieces, region 6 = 150; `/status.json`); `dfo.vectors.test.ts` covers each page's `pieces`, which
  `python -m pipeline.tools.emit.dfo_vectors` now writes (region 3: 3, region 6: 13, region 8: 1). Each new test was
  mutation-checked (it fails with its piece of code removed or broken).
- **Not covered by a dedicated test:** a piece on a page whose region header closes it (no saved page has both); it is
  the same line every place takes.
- **Follow-up (fixture refresh):** `test/fixtures/dfo/entries/region-*.json` are now the committed curated places of
  `redesign/stream-sections` (they were T225 copies). The vectors carry `live`: every location id the Python's `live.run`
  publishes over the saved pages (274), and the worker's run must equal it except ONE NAMED GAP: the worker types no
  page-default sentence (T349 `cascade.statement_locators`), so the 7 `page_default` places are in the Python's feed only.
