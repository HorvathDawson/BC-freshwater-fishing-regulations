# T373 — Run timing always matches the release's map

Status: DONE (1cca5a8ff) 2026-11-04 (was: OPEN — before 0.5.0) (owner 2026-11-03: "we need the run timing to always be synced").

Found by the UI chat 2026-11-03: the run-timing feed was bound on 10-29 to handles 10cbcb72…, the local release
carries 822db448…, so Run timing paints nothing (the app refuses a paint index whose handles are not the tiles').
Cause: the feed is its own stream (T199) and its BASE is an owner step "once per atlas" (`runtiming.feed.base`) — a
new atlas without it leaves the feed on the old one.

Work:
1. `pipeline.deliver all` / the release build builds the run-timing base and binds the paint index for ITS OWN
   handles from the cached timing data (`feed.build`, no HTTP), so every release has a matching paint.
2. The release refuses to package (like a size gate) when no run-timing paint matches its handles; staging/prod upload
   sends the matching feed with the release (the scheduled workflow keeps refreshing the runs against that base).
3. A test: a release whose handles differ from the run-timing index's is refused.
Size: M. Heavy (the base streams ~3 GB of tile layers): through heavy.sh.

## Done 2026-11-04
- Local fix now: the feed served (`runtiming/feed/out`) was the 10-29 build (10cbcb72); rebound for 822db448 + the tiles'
  own cut (old outputs kept as `out.1029-10cbcb72`, `out.new`, `out.1031`).
- `pipeline/runtiming/feed/release.py` (`bind`): the base of the tiles' handles (built when missing), the parsed timings
  (`SOURCE/runtiming/timing.json`, `GENERATED.runtiming/cu_polygons.geojson` — `runtiming.parse`'s defaults; missing →
  refused with the commands), `build.build(..., tile_version=<the tiles'>)`, `build.check`.
- `deliver all`: a `runtiming` step after `release` (`pipeline.deliver runtiming --release-root …`), promoted whole to
  `GENERATED.runtiming/feed/out` with the release.
- `vintage.report_all`: `Artifacts.runtiming` (in_dir + canonical) — refused when its handles are not the bundle's or its
  tile_version not the tiles' (strict: refused when missing).
- `deploy/staging/upload.sh`: refuses a run-timing feed of other tiles, puts the base in `private/runtiming/base/` when
  missing, publishes the feed (`runtiming.feed.publish --via wrangler --yes`).
- Tests: test_vintage (two refusals + missing), test_deliver_atomic / test_card_gate_in_chain (the step, the promotion),
  test_runtiming_feed (missing inputs refused).
