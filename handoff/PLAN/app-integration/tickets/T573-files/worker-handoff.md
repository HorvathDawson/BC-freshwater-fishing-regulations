# T573 → T757: closed areas inside a DFO place, the feeds worker and the app

Pipeline side: branch `data/t573-closed-area-build`. The Python is the reference throughout; the cascade stays in Python.

## What the pipeline now does
- Each LIVE closed area inside a DFO place (the Thompson's Deadman, Juniper and Ashcroft areas, the middle Shuswap
  stretch, Babine's mouths and the Gullwing line) is a DFO LOCATION of its own: `<water_id>#closed:<key>`
  (`pipeline/regs/dfo_salmon/closures.py` `build_pieces`). It has its place's tier, its own rule (all salmon, take 0;
  "No fishing, year-round — <DFO's words for the area>") and fewer sections. The existing narrowest-first chain order
  therefore puts it first on its sections; there is no new precedence rule.
- The release binds a piece only when the owner confirmed it: a stretch between its cuts, or a circle on the sections
  wholly inside it. Every other live piece is carried on its water (the chain's `u`). An item today's rows do not name
  builds nothing.
- The Python live feed (`live.run`) adds `closures.live_piece_rules`: one `locs[<piece id>]` per item TODAY's row
  names, whether confirmed or not. The feed decides liveness only; binding is the release's job.

## THE PIECE ID (exact grammar, pinned by `test_the_piece_id_grammar`)
```
^(?P<water>[0-9a-z]+:[a-z0-9-]+)#closed:(?P<key>[0-9a-f]{12})$          closures.PIECE_ID_RE
```
- `water` is the place's `water_id`: `<region key>:<slug>` (`entries.make_water_id`). Every curated water id fits;
  `test_every_curated_water_id_fits_the_piece_grammar` checks this.
- `key` is `piece_key(item)`: `sha1(re.sub(r"\s+", " ", item.strip().lower()))`, the first 12 hex digits. The item
  is DFO's words for the area. For a circle the item is `f"{radius:g} m radius: {name}"`. For an inline
  "except closed from A to B" the key comes from `_inline_key`, which first sets aside two typing slips:
  `"( "` → `"("` and `"200m"` → `"200 m"`.
- Nothing needs escaping: no spaces, no non-ASCII, and `#` / `:` appear only where the grammar puts them.
  `piece_id` raises on anything else.

## 1. THE WORKER (T757)
After the fingerprint join (`live.ts`, the `join` twin), for each scraped row joined to a curated place that has a
`water_id`, apply the same rules as `closures.live_piece_rules`:
1. Build `{location_id, water_id, water, region, status: "active", source_text: {specific_area, excludes}}` from
   the row's OWN `specific_area` and `excludes` (today's page, not the curated copy).
2. Run the TS port of `closures.derive` on those rows, which needs `says_closed`, `areas`, `inline_items`,
   `closure_entry`, `piece_key` and `OWNER_READINGS`. Keep only entries whose status is active, in location-id order.
3. For each area of each entry:
   - `lid = <water_id>#closed:<key>`; the first entry to name a key wins.
   - `rule = piece_rule(entry, area, lid)`: entry B's rule, with `verbatim` = `"<rule words> — <words>"`, where
     `<words>` is the area's `words` for a circle and its `item` otherwise.
   - `locs[lid] = {r: slug, ...location_calendar([rule])}`. When the region header closes the page, use the
     header closure instead, exactly as for any place.
4. Emit nothing for an item today's row no longer names. The release's piece then has no feed answer.

**`/status.json` must say it:** a new top-level object `"dfo": {"pieces": 1}`. `1` means this shape: one loc per
live piece, ids per the grammar above. `pipeline/deliver/release/pieces_gate.py` (`PIECES_SHAPE = 1`) refuses to
upload a release with at least one BOUND piece to any bucket whose worker does not serve `dfo.pieces >= 1`. The
same refusal applies when the worker is unreachable or no `FEEDS_URL` is set. Both `deploy/staging/upload.sh` and
`deploy/prod/upload.sh` call it before the first copy.

**Pins:**
- On the fixture pages, the loc count rises from 254 to 271 (+17: Thompson 3, middle Shuswap 1, Babine 12 circles +
  the Gullwing line). Region 6 rises from 137 to 150. Source: `pipeline/tests/test_dfo_live.py`.
- The oracle: `app/workers/feeds/test/fixtures/dfo/closure-oracle.generated.json` (beside `python-vectors.generated.json`), regenerated with `python -m pipeline.tools.emit.closure_oracle` (`--check` runs in `python -m pipeline.tools.emit --check`);
  `test_the_worker_oracle_is_what_the_python_feed_emits` keeps it current. Each case gives `entry_file` (as
  `loadEntryFile` reads it), today's `rows`, the owner's `confirmed` placements (the feed ignores them), the per-piece
  `rules`, and `expect: {locs, verdicts, gears}` (verdicts[0] is NOT_LISTED, as in `live.run`):
  - (a) `a_confirmed_on_todays_row`: Deadman is confirmed and named today, so it is emitted, closed every day.
  - (b) `b_unconfirmed_on_todays_row`: Juniper is unconfirmed, and is still emitted (the release carries it).
  - (c) `c_confirmed_dropped_today`: DFO dropped Deadman today, so it is not emitted; Juniper still is.
- Suggested: add the piece ids and calendars to `dfo_vectors` (`python -m pipeline.tools.emit.dfo_vectors`) so
  `dfo.vectors.test.ts` covers the real pages too. This was not done on the pipeline branch, because the vectors
  file lives under app/.

## 2. THE APP (T757)
**The marker:** the salmon payload's `p[<location id>]` carries `piece: 1` for a closed-area piece and `bound: 1`
when it covers a section (`deliver/salmon.py` `build`; `test_a_bound_piece_is_marked_in_the_payload_and_a_place_is_not`).
The app can read these without parsing the id. A carried piece is in `p` only through its own unmapped chain, and
has no `bound`.

**The rule:** a chain location that the release binds (in `l`) and whose `p` entry has `piece: 1`, but which the
live feed's `locs` lacks, must NOT fall through to the next location (the place's opening). For each species and day
it reads "not known, check the DFO notice", plus `reportFailure` (the W46 pattern). Without a feed (offline), the
release's own calendar for the chain answers, and it already closes those sections. A dormant ordinary place keeps
today's behaviour, saying nothing.

## The remaining owner and build steps (not the worker's)
- The owner confirms each piece in the review app.
- A confirmed stretch's NEW cuts now become cut proposals (`marks.cut_proposals_of` → `proposals.build`). An existing
  cut within 25 m on the same line is reused, and no cut is made on a line's end. Once they land in the cuts file and
  the atlas is rebuilt, the reach run reads each new end as the cut it landed as (`proposals.landed_cut`), and the
  stretch binds.
