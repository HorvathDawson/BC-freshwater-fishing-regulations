# T757 — Feeds worker and app: closed areas inside a DFO place (T573's pieces) are live locations, never read as open

Status: DONE — c7a09348a, 5a4f5e245, fa0c7a1c5, 8967ca419, 731447000. Worker emits closed-area pieces and states its feed shape; the app's minimum feed shape and the deploy refusals are merged (item 3 superseded by T759 / item 6); the production worker was deployed 2026-11-19.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T757 | Feeds worker and app: closed areas inside a DFO place (T573 pieces `<water_id>#closed:<key>`) are live locations: the worker emits `locs` for them and says its feed shape in `/status.json`; a bound piece the feed lacks reads "not known" and is reported, never open. | DONE | c7a09348a, 5a4f5e245, fa0c7a1c5, 8967ca419, 731447000 | — |

Closed by the 2026-11-20 audit: the worker half merged (c7a09348a); item 5 (5a4f5e245) and item 6 (fa0c7a1c5, via 7875490fe) merged with merge/t759; item 7's deploy refusals merged (8967ca419, fix 731447000). The ticket says item 3 was superseded by T759 / item 6. The production feeds worker with closed-area pieces was deployed 2026-11-19 (OWNER-TODO '0.8.3 to production', step 1).

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

## Ruling 2026-11-15 evening (Android chat, after the owner's T759 decision): what replaces the app half
The upload gate checks the worker only at upload time; a worker can go backwards later (a rollback, a staging release
that puts an older worker back). Then every bound piece would fall through and closed areas would read open, silently.
That is a technical failure, not DFO removing words, and it gets a guard that needs no new status:
5. **Feed shape in the feed itself.** `feeds/dfo/index.json` carries `"dfo": {"pieces": 1}` at top level (the value
   `/status.json` serves; the Python writes it too, on the Android chat's T759 branch). Worker: write it; the vectors
   hold worker == Python.
6. **App (replaces item 3 and branch `app/t757-piece-not-known`'s guard):** a release that binds at least one piece
   (`p[..].piece === 1 && bound === 1`), read with a feed whose index does not carry `dfo.pieces >= 1`, does not use
   the live feed for salmon at all: it shows the release's own calendar (the offline reading, which has the closed
   areas closed), says so the way a stale feed is said today, and reports once (`reportFailure`). Written as a named
   MINIMUM FEED SHAPE the release requires, so the next feed-shape change reuses it. A single piece missing from a
   feed that does carry the shape falls through, as the owner ruled (DFO removed the words); a reworded closed area is
   T759's `closed_area_unbound` issue.
7. **Deploy refusals:** the staging worker deploy (`deploy/staging/feeds.sh`, and whatever the site deploy uses to
   put a worker back) refuses a worker whose code does not serve `dfo.pieces >= 1` while the live release binds a
   piece; the same check in the production deploy script, as a refusal with words (edited by an agent, never run by
   one; on the owner's code-review list).
Merge timing unchanged: after build 33.

## Results, items 6 and 7 (2026-11-15 evening; both pushed, neither merged)
- **Item 6, app, `app/t757-min-feed-shape` fa0c7a1c5** (off shared head b0f5c10b5; replaces `app/t757-piece-not-known`,
  which is abandoned): `requiredFeedShape(payload)` (`{pieces: 1}` when a chain names a location with `piece` and
  `bound`) and `feedMeets(feed, need)` in `core/salmonFeed.ts`; `parseSalmonFeed` reads the feed's top-level `dfo`
  (absent = an older feed; malformed = the feed is refused like any bad field). In `pickSalmon`, a feed that does not
  meet the shape is not used: the release's own file for every chain, one `reportFailure` per release head and feed
  read, and the card's feed line reads: "The latest DFO check could not be used with this app's data, so the DFO rules
  this data was built with are shown. Check DFO's page before you fish." (new words; for the owner's eye with the
  next batch). A piece missing from a feed that HAS the shape falls through with no words and no report (pinned).
  6 new tests, core 1567 pass, `check:fast` passes; seven mutations, each caught.
- **Item 7, deploy, `deploy/t757-worker-shape-refusal` 0382f0f5d:** `deploy/lib/worker-shape.mjs` bundles the worker's
  `status.ts` and prints `DFO_SHAPE` (today's shared head: `null`; the T757 worker branch: `{"pieces":1}`);
  `pieces_gate.py` gains `worker_problems`, `read_live` and a `--worker-deploy` mode (additive; the Android chat's
  file, for its review); `deploy/staging/feeds.sh` and `deploy/prod/feeds.sh` run it before `wrangler deploy` (the
  bucket's `release/manifest.json` by rclone, the shape, the gate; anything unreadable refuses). 21 new pytest tests;
  eight mutations, each caught. Nothing under `deploy/prod` was run. Known limits: the gate reads the live release
  from the local `data/release` (a deploy from a worktree without it is refused), and it refuses an unreadable worker
  shape even when no piece is bound.
- **Item 5 (worker writes the shape into index.json):** waits for the Python on the Android chat's T759 branch.
- **Item 7, follow-up (Android chat's ruling on the two limits), 05383894d on the same branch:** both feeds scripts
  pass `--releases "${RELEASES_DIR:-data/release}"`; a folder that does not exist, or that lacks the live release, is
  refused with words that name the path and say a deploy from another checkout sets
  `RELEASES_DIR=/path/to/main/checkout/data/release`; never a fall back to the default. An unreadable worker shape
  stays refused even with no piece bound, and its words name `app/workers/feeds/src/status.ts`. `read_live` now also
  returns the refusal text (`pieces_gate.py`, for the Android chat's review). 22 tests in the file; seven mutations caught.

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

## Merged, 2026-11-16 (app chat; owner's answer to question 19, 10:29: "Yes, one at a time (Recommended)")
The worker half (items 1 and 2) is on `redesign/stream-sections`, merged from `feeds/t758-page-defaults` (which holds
this branch and T758's). Checked on the merged head before landing: worker suite 31 files / 487 tests; `python -m
pipeline.tools.emit.dfo_vectors --check` and `pipeline.tools.emit --check` current (the saved-page vectors did not
move under the 181 commits the branch was behind); `pnpm run check:fast` rc 0; the mini release check; the pipeline CI tier 6,479
passed, 10 failed, all ten in `test_stepcache.py` and not from this merge: the same ten fail in an untouched worktree
under the system temp folder and all 20 pass in the main checkout (the recorder sees no reads there). No file the app bundles changed (the worker is outside the app's workspace), so the Android benches of
build 33 stand. Staging keeps running `ios/staging-worker` (this + push + the crash limit) until push merges.
Production: the owner's `deploy/prod/feeds.sh`, BEFORE or WITH the first release that binds a closed area.

## Item 7 changed before landing, 2026-11-16 (app chat; rule agreed by the main chat, whose file the gate is)
As written, the worker-deploy gate read the environment's live release first and refused when it could not. Tried by
hand: a worker that serves `{"pieces":1}` beside an empty live manifest was refused. Production has no release yet, so
the owner's first production worker deploy would have been refused, while the upload gate refuses the release until
that worker is up: each waited on the other.
- **Rule now:** a worker whose code serves `dfo.pieces >= PIECES_SHAPE` is safe beside any release and passes without
  the live release being read (`serves_pieces`; `main` does not call `read_live` for it). The live release is read,
  and unreadable = refused, only for a worker that lacks the shape. An unreadable worker shape is refused always.
  `true` is not a shape number.
- **Tests:** `test_worker_deploy_pieces_gate.py` 30 pass (was 22): the pinned case turned round, the four unreadable
  cases refused for three shapes without pieces, and one that fails if the release is read at all for a worker that
  serves pieces. Four mutations, each caught (the old order: 6 tests fail; `main` always reading: 1; `true` as a
  number: 1; a worker without the shape passing an unreadable release: 7).

## Pipeline CI red after landing, fixed (2026-11-16 15:55, app chat)
Landed at 1809e6a54; pipeline-ci failed 3 cases of `test_worker_deploy_pieces_gate.py`: the worker-shape reader bundles
with the APP's esbuild, and pipeline CI installs no app packages ("Cannot find module 'esbuild'"). My pre-landing tier ran
locally, where app/node_modules exists, so it could not see this. Fix: the six reader cases skip, said by name, when
`app/node_modules/esbuild` is absent (test_dfo_suggest.py's pattern); with it they run (30 pass), without it 24 pass and
6 skip. The deploy itself still refuses when the bundler is missing.

Second red (cefa78170): `test_no_silent_skips.py` refuses a skip outside its allowlist. The esbuild skip is an optional
tool, the same kind as `test_dfo_suggest.py`, so it is now listed there (count 1). The guard plus the deploy tests: 50 pass
with esbuild; with it moved aside, 44 pass and 6 skip. The rest of that CI run passed (6521).

## Items 5 and 6 merged, 2026-11-16 20:55 (with T759, branch `merge/t759`, not landed)
Item 5 (the shape in index.json, `dfo.pieces`) came with T759's feeds half (5a4f5e245); item 6 (the app's minimum feed
shape, fa0c7a1c5) with T759's app half (7875490fe). Both land when `merge/t759` lands.
