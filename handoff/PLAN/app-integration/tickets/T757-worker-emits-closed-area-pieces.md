# T757 — Feeds worker and app: closed areas inside a DFO place (T573's pieces) are live locations, never read as open

Status: IN PROGRESS (iOS chat; started 2026-11-15 14:44 on the spec `T573-files/worker-handoff.md`; branches `feeds/t757-worker-pieces` and `app/t757-piece-not-known`, nothing merged).

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
