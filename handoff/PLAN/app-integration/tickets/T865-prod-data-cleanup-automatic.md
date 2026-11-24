# T865 — Production data clean-up is automatic (stay under R2's free 10 GB)

Status: DONE — 4f4b5c827, 8b279ce64. production prunes old releases after each promote (upload.sh, allowlist); v1 dropped by drop-v1.sh at the switch.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T865 | Production data clean-up is automatic: old releases and the v1 layout (owner) | DONE | 4f4b5c827, 8b279ce64 | — |

Closed by the 2026-11-20 audit: both commits merged on the dev branch; the switch happened 2026-11-18 ~23:05-23:21 (OWNER-TODO): bc-fishing-cron schedules removed, main's three update workflows gone, deploy/prod/drop-v1.sh freed 20,019 objects / 8.57 GB, smoke --production 8/0/0.
switch, on the owner's word, turn off `bc-fishing-cron` + `main`'s three update workflows and run
`deploy/prod/drop-v1.sh` (PRODUCTION.md step 11).

Owner 2026-11-18, verbatim:
- ~18:20: "We will also what to delete the old data for old app version once everything is settled to avoid being over 10gb"
- ~18:23: "That clean up should be automatic"
- ~18:25: "when we flip the switch i will get you to clean that stuff up too"

Today `deploy/prod/upload.sh` clears nothing (old releases are pruned by hand, PRODUCTION.md "Not yet"), and the v1
layout (the old Vite site's data) is deleted by hand at PRODUCTION.md step 11.

Work:
1. After the pointer moves, `deploy/prod/upload.sh` deletes every release-scoped object that neither
   `release/manifest.json` nor `release/manifest.previous.json` names (rollback keeps working). Delete by an allowlist
   of release-scoped prefixes, never "everything else"; refuse (and say so) when a manifest cannot be read or parsed, or
   a file a kept manifest names is missing. Prints what it deleted and the bytes freed; runs under `--dry-run` and
   against a `--bucket` rehearsal.
2. The v1 layout goes by its own command, `deploy/prod/drop-v1.sh`, which Claude runs at the switch on the owner's
   word (review 2026-11-18: dropped automatically, a site rollback to the Vite build would load with no data). It
   fails closed on a marker the new web export has and the old Vite site lacks.
3. Tests with no network; one proven to fail when the guard is broken.
4. PRODUCTION.md step 11: CLAUDE's step at the switch, on the owner's word (owner ~18:25), not before: turn off the
   old `bc-fishing-cron` worker and `main`'s `update-hydro.yml`, `update-in-season.yml`, `update-stocking.yml`, then
   `deploy/prod/drop-v1.sh`.

## Built (2026-11-18, 4f4b5c8)

- `pipeline/deliver/release/prod_prune.py` (`releases`, `v1`): `releases` run by `deploy/prod/upload.sh` after the
  pointer; `v1` only by `deploy/prod/drop-v1.sh` (see the review fixes below).
- Layout map (module docstring): PRUNED = `release/r-*/`, `release/tiles/*/`, `release/basemap/*/` not named by
  `release/manifest.json` / `release/manifest.previous.json` (a kept manifest's folders kept whole). NEVER = the two
  pointers, `map-assets/v1/`, `*.wasm`, `runtiming/` (own publisher, own pointer), `feeds/`, `private/`,
  `app/android/play.json`, `feedback/` (the v1 worker's stored user reports), anything else.
- Refused, nothing deleted, exit 1: a pointer unreadable / unparseable / without `data_release`, a named file missing,
  both pointers naming one release. No previous pointer (first promote): nothing pruned, exit 0.
- `upload.sh`: promoting the live release again no longer copies it over `manifest.previous.json` (the prune would
  then have deleted the real previous release).
- v1 list from `main` (`V1_FILES`, `V1_DIRS`): `cron/**`, `shards/**`, `mobile/**`, `row_images/**`, `bathymetry/**`,
  root `tier0.json`, `admin_visibility.json`, `match_table.json`, `poly_reaches.json`, `data_version.json`,
  `layer_manifest.json`, `in_season.json`, `stocking.json`, `waterbody_data.json`, `regulation_index.json`,
  `regulations.json` and the ten `*.pmtiles` the webapp named. On `main` the hydro keys were always under `cron/hydro/`
  (`KEY_PREFIX`); a root `stations.json` was never written (PRODUCTION.md step 3's curl now names `cron/hydro/`).
- Marker: index.html loads `/_expo/static/js/web/index-<hash>.js` and lacks `<!-- build: webapp UI fixes`. Checked
  2026-11-18 18:2x: staging.canifishthis.ca has it, canifishthis.ca has the old comment.
- Tests: `pipeline/tests/test_prod_prune.py` (22, no network, a folder bucket). Mutations each fail it: missing-file
  guard off, previous not kept, the same-release guard off, either site marker ignored, the allowlist widened to
  "everything else" (listing + unit together). Rehearsed through real rclone on a local remote
  (`RCLONE_CONFIG_R2_TYPE=local`, `r2:<dir>`): keep/delete, dry run, refusals, v1 kept (canifishthis.ca) / deleted
  (staging site as the marker).

## Review fixes (2026-11-18, 8b279ce; independent review: 4 defects)

1. v1 no longer goes from `upload.sh` / `site.sh`: `CONFIRM_PROD=canifishthis.ca deploy/prod/drop-v1.sh [--dry-run]`,
   Claude at the switch on the owner's word; no marker = nothing deleted, exit 1.
2. `--bucket` is compared bare in every `deploy/prod` script that takes it (`deploy/lib/bucket.sh`: remote prefix and
   slashes dropped, lower-cased, a path refused): `bc-fishing-regulations/` no longer skips `CONFIRM_PROD`.
3. Each key is printed once rclone reports it deleted (`-v --use-json-log`, streamed); a delete that stops part way
   says how many objects and bytes went, never "nothing deleted".
4. A release-scoped folder whose newest object went up under 24 h ago (`lsjson --use-server-modtime`) is kept and
   said: a web tab open across two promotes the same day keeps reading its region files.
Tests: 63 in `test_prod_prune.py` (stub-tree runs of the four scripts for the bucket guard); each fix's guard broken
fails them.
