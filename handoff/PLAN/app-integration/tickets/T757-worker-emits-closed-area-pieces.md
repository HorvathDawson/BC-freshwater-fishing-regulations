# T757 — Feeds worker and app: closed areas inside a DFO place (T573's pieces) are live locations, never read as open

Status: IN PROGRESS (iOS chat; worker half live on STAGING since 2026-11-15, branch `feeds/t757-worker-pieces`, merges with the iOS merges after build 33; APP HALF HELD: the owner's T759 decision of 2026-11-15 evening (no "not known" status) contradicts it, it is reworked inside T759 once that ticket is rewritten).

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

## State 2026-11-15 14:57 (iOS chat; each lane's own Results section is on its branch)
- **Worker, `feeds/t757-worker-pieces` 2523097a9:** `src/dfo/closures.ts` is the port of the read side of `closures.py`;
  wired after the fingerprint join in `live.ts`; `/status.json` serves `"dfo": {"pieces": 1}` on the 200 and the 503.
  Tested against the oracle's three cases (locs, verdicts, gears, rules), the id grammar, the two typing slips (values
  from the Python), first-entry-wins, and the real pages through the regenerated vectors (17 pieces: region 3: 3,
  region 6: 13, region 8: 1). Worker suite 31 files / 479 tests pass (re-run by the iOS chat); `check:fast` rc 0. The
  lane broke each covered part by hand and saw its test fail. It changed `pipeline/tools/emit/dfo_vectors.py` (a
  `pieces` entry per page) and bumped `DFO_REV`.
- **App, `app/t757-piece-not-known` a5af25920:** in `core/salmonFeed.ts` `composeFeedPayload`, a chain naming a location
  whose `p` entry has `piece: 1` and `bound: 1` and which `feed.locs` lacks reads "check" every species and day with
  `SALMON_WORDS.pieceNotKnown` ("Not known: today's DFO check does not include this closed area. Check the DFO notice
  before you fish."), `reportFailure` once per location per feed read. All readers go through that one function. 7 new
  tests; core 121 files / 1568 pass. No fixture with the marker exists under app/, so the tests build the payload by hand.
- **Open points:** (1) the worker's saved pages give 265 locs (248 places + 17), the spec's pin says 271 (254 + 17) from
  the pipeline's own page fixtures; region 6 matches (137 -> 150). Not pinned by a worker test; asked the Android chat.
  (2) No saved page has a piece under a region-header closure; that path is the same line as any place, untested.
  (3) When DFO drops an item, a bound piece reads "not known" and reports every feed read until the release changes:
  by the agreed rule. (4) The two branches conflict only in this ticket file (both append a Results section).
  (5) No device or emulator look yet.

## Held, 2026-11-15 evening: the app half no longer matches the owner's rule
Owner, T759, rounds 2–6 in OWNER-TODO.md: no "not known" status. A place (or closed area) DFO removed: no status
change, the remaining rule answers. A new wording that is not matched on the river: the colour stays, the stream line
gets an "issue" look and every stretch of the river shows a banner quoting the unmatched rule until it is re-matched.
A closed-area piece's id is a hash of DFO's words, so a reworded closed area is exactly that second case: the bound
id vanishes and a new, unbound `#closed:` id appears in the feed. So `app/t757-piece-not-known` (a5af25920: "Not
known: today's DFO check does not include this closed area…") is NOT merged; work item 3 above and the second
acceptance line are superseded by T759. Asked of the Android chat for its rewrite: the feed's per-river `issues` must
carry an unbound closed-area wording with its text and water. Work items 1 and 2 (worker) stand.
