# T865 — Production data clean-up is automatic (stay under R2's free 10 GB)

Status: PARTIAL — built 4f4b5c8 (lane app/t865-prod-prune, not merged); left (Claude): at the switch, on the owner's
word, turn off `bc-fishing-cron` and `main`'s three update workflows (PRODUCTION.md step 12).

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
2. The v1 layout goes automatically once canifishthis.ca serves the new site (a marker the new web export has and the
   old Vite site lacks); without it, v1 is kept and the output says why. Run by `upload.sh` and `deploy/prod/site.sh`.
3. Tests with no network; one proven to fail when the guard is broken.
4. PRODUCTION.md: step 11's delete becomes automatic. Turning off the old `bc-fishing-cron` worker and `main`'s
   `update-hydro.yml`, `update-in-season.yml`, `update-stocking.yml` is CLAUDE's step at the switch, on the owner's
   word (owner ~18:25), not before.

## Built (2026-11-18, 4f4b5c8)

- `pipeline/deliver/release/prod_prune.py` (`releases`, `v1`), run by `deploy/prod/upload.sh` after the pointer and
  (`v1`) by `deploy/prod/site.sh` after a real deploy (its `--dry-run` reads no production bucket).
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
