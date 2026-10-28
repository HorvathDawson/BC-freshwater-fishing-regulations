# T75 — The basemap in the release manifest (audit N18, P18b)

Status: PARTIAL — manifest side DONE: the live LATEST (r-c706f5eaa2a3) names `basemap-147bd82d051f` (274 MB), built/released by T202 d4589cb5 + 6f25c3c9 (verified 2026-10-27). Left: an offline native launch drawing ground (needs the phone, T77).

Source: the live `r-98ded5f443e1/manifest.json` has `basemap: null`; `release/build.py` takes a `basemap` argument but
deliver never passes one; `release/manifest.py` allows `{id,url,bytes,sha256} | null`. Native cannot install the ground
map (~918 MB, D1), and native is "ALWAYS OFFLINE" (Q5).

## Work
The tile step names the basemap in the manifest; native install/update fetch it with a sha check; tile tests and the
vintage check cover it.

## Acceptance
Manifest basemap non-null; P11 install/update/rollback tests include it; an offline native launch draws ground.

Depends: T64 + T85 (the basemap size cut lands first so it ships once). Size: M. Owner: no.

## Outcome
Code done 2026-10-24; takes effect when the basemap step and the release step run in the tiles window.
- `python -m pipeline.deliver.tiles.basemap` now builds AND releases: `data/release/basemap/<id>/<id>.pmtiles`,
  `id = basemap-<sha12>` (content-addressed, immutable; same bytes = same id) and the sidecar
  `data/generated/tiles/basemap.meta.json` `{basemap_version, bytes, sha256}`. `--release-only` releases the archive
  already there (no rebuild); `--no-release` is a scratch build; `--out` builds elsewhere.
- `release.manifest.basemap_block` names it (`basemap: {id, url, bytes, sha256}`, re-hashed from the released copy;
  a sidecar naming missing/other bytes is refused); `pipeline.deliver.release` passes it; the id moves the release id.
  `publish.plan` uploads it after the tiles and before any data file (immutable).
- Vintage (`report_all`): a manifest's basemap must be on disk at its size, and the current release must name the
  basemap released beside the tiles.
- App: install/update/prune already handled `manifest.basemap` (sha-checked stream, fetched only when the id moves);
  now pinned in `loader.test.ts` (first install, data-only update, new id, prune of the old, a bad sha keeps the old
  set). `releaseTiles` draws the held basemap when the manifest names one.
- Tests: `test_release_basemap.py`, `test_vintage.py` (+1), `test_lake_facts.py` (stubs the block).
- Not verified here: an offline native launch drawing ground (needs the phone, T77).
