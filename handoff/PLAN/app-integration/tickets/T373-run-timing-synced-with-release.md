# T373 — Run timing always matches the release's map

Status: OPEN — before 0.5.0 (owner 2026-11-03: "we need the run timing to always be synced").

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
