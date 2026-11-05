# T392 — A feeds worker release reruns the feeds

Status: DONE 2026-11-05 (6f0a04a1c; owner 2026-11-05: "the feeds in staging never updated after release to get the new forecast
detail; when a feeds worker release happens it should rerun feeds automatically").

**Cause:** the gauge job reuses the day's forecast output (and each station's) while its key `GAUGE_REV` matches, and the
DFO job carries regions on `DFO_REV`. Both were hand-bumped constants; the forecast-detail release did not bump
`GAUGE_REV`, so the old output stood until the next day's new issuance.

**Fix:**
- `src/io.ts` `rev(base)`: the base + the deployed version (Cloudflare `[version_metadata]`, set per isolate by the
  fetch/scheduled entry points). Every reuse key (gauge forecast state + station cache, DFO inputs) uses it, so the first
  run after any deploy recomputes everything. In-season needed nothing (its key hashes the parsed rows; writes are
  content-hashed). Test: `test/rev.test.ts`.
- `deploy/staging/feeds.sh`: deploy, then POST `/run/gauges`, `/run/dfo`, `/run/inseason`; exit 1 on a failed job. Use it
  instead of the bare `wrangler deploy`.

**Verified:** deployed (version ba2b9176); gauges recomputed all stations (0 unchanged, 18.8 MB written), dfo and
in-season ok.
