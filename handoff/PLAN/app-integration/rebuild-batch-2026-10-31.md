# Batched full rebuild — prep (2026-10-31)

PREPARED, NOT RUN. One `--atlas` rebuild carries everything below. Run the commands from the repo root, one heavy job at a time.

## Items
| item | state | what the rebuild does with it |
|---|---|---|
| T301 HYDAT | ready, nothing to do | The build already reads `data/source/hydat.sqlite3`. Its VERSION table says **2026-10-19 07:27:03**, and `hydat.release` says `20261019`. The untracked `hydat.20260717.sqlite3` is the OLD archive, kept for the T301 diff (VERSION 2026-07-17). It is now gitignored (2e4a7e797); delete it when happy. `clim` is **up to date**, and the live release r-038e6818e80f already says `hydat 2026-10-19`, so the flows and normals have been refreshed already. This rebuild changes nothing for HYDAT. `feeds-hydat.yml` stays `workflow_dispatch` only. |
| T296 / T292 | ready, nothing to do | Both are already in the LIVE outputs from the 10-30 build: `section_down.lake` (22,519 rows) and `panel_member.lake`, and `not_fishing_water` tiles carry `kind` (214 sewage, 59 tailings, 2 dump). The rebuild carries them along. What's left is outside the build: T292 shots, and the APK for T296. |
| T320 / T321 DFO | done | `pipeline.regs.dfo_salmon.feed` reads no network and no model. It ran twice into scratch and both outputs were byte-identical; then it ran live (247 locators, 517 rules). The new feed adds the T321 `words` notes (regions 5b and 6). `wording check`: 0 unknown clauses. The release takes `wording.json` through the typed feed: the deliver key now reads "inputs changed: …, sources" (`dfo_feed_digest`). |
| 08MH044 | committed (1e72e203e) | The `matches.json` point moved to Albion. It stays on the Fraser mainstem, blk 356364114. This is an atlas-cut input, so it needs `--atlas`. |
| T55 sloughs | BLOCKED: no fold tool | `decided/sloughs.json` holds 811 owner marks, all `call: stream`, `P: y`. No build code reads them. See T55 "Fold: what is needed". |

## Stages (`pipeline build --dry-run`, 2026-10-31)
`atlas` (only with `--atlas`; needed for 08MH044) → `clim` up to date → `reach` RUN (code) ‖ `tiles` RUN (code) → `deliver` RUN
(code, curated, sources). After the atlas is promoted, clim, reach, tiles and deliver all rerun on the new atlas.

## Commands
```sh
# 1. SIDE run: side atlas + every stage into DIR; live outputs and LATEST untouched (~50 min)
scripts/heavy.sh batch-side 70 -- .venv/bin/python -m pipeline build --side data/generated/side/batch-1031 --atlas --jobs 3 \
  > data/generated/side/batch-1031.log 2>&1
.venv/bin/python -m pipeline build --side data/generated/side/batch-1031 --status

# 2. DIFF against live. Old-vs-old noise baseline, measured 2026-10-31: atlas/full.prev vs side/build-pass-1031 full_next is IDENTICAL,
#    and build-pass-1031 vs live atlas/full differs by Sheldens Creek alone (+1 item, +2 sections). So any change in the new atlas
#    other than 08MH044's Fraser cut (blk 356364114) needs an explanation.
.venv/bin/python -m pipeline.tools.audit.build_parity data/generated/atlas/full data/generated/side/batch-1031/atlas/full_next
NEW=$(ls -d data/generated/side/batch-1031/deliver/release/r-* | grep -v tiles | head -1)
diff -rq data/release/r-038e6818e80f "$NEW"      # expect: salmon* (T321 words), manifest, tile set; anything else gets explained
#    Release noise: manifest published/minAppVersion (stamps only). T305 made tile_version deterministic, so a moved tile_version
#    now means the tiles really changed.

# 3. PROMOTE the side atlas, then run the live build (reach, tiles, deliver; writes release + LATEST)
.venv/bin/python -m pipeline.atlas.promote data/generated/side/batch-1031/atlas/full_next
scripts/heavy.sh batch-live 70 -- .venv/bin/python -m pipeline build --jobs 3 > data/generated/side/batch-1031-live.log 2>&1
.venv/bin/python -m pipeline build --status; cat data/release/LATEST

# 4. STAGING upload (staging bucket only; paint/5 needs app >= 0.3.0)
MIN_APP_VERSION=0.3.0 deploy/staging/upload.sh

# 5. RUN-TIMING base (after every atlas promote; staging private/runtiming/base/)
scripts/heavy.sh rt-base 50 -- .venv/bin/python -m pipeline.runtiming.feed.base --upload --yes

# 6. SMOKE against staging
node app/tools/smoke-staging.mjs
```
Prod upload stays human (`pipeline.deliver.release.publish --release data/release/<new> --min-app-version 0.3.0 --yes`).
