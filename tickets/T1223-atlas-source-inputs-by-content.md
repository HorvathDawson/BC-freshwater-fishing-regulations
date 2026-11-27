# T1223 — The atlas record keeps its two big source files by size and mtime

Status: IN PROGRESS — lane `fix/T1223-atlas-source-inputs-by-content` (stacked on T1220): PR open for an independent review; merge before the 0.10.0 re-cut's atlas build

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1223 | The atlas record keeps its two big source files by size and mtime | IN PROGRESS — lane fix/T1223-atlas-source-inputs-by-content | — | review + merge after T1220 and BEFORE the 0.10.0 re-cut's atlas build (after it, the switch costs another atlas rebuild or a second value shape) |

Source: T1220 "Found, not done" and the independent review of the T1220 PR, 2026-11-26.

## The defect (on main)
`pipeline/atlas/inputs.py` `digest()` recorded `source:fwa_gpkg` and `source:fwa_coast` as `stamp:<size>:<mtime>`. A copy or re-download of the
same bytes therefore read "changed since the atlas was built", and `pipeline release` refused the atlas. A same-size edit within one second
was not seen.

## Work (done on the branch)
1. `stamp` and its cache moved to `pipeline/common/source_hash.py`. `pipeline.build.stamp` is a thin wrapper over it, and `pipeline.atlas`
   never imports `pipeline.build`.
2. `digest()` gives both `source:` keys `sha256:<64 hex>` through the SAME cache, READ-ONLY: `check`, `behind` and the atlas build write
   nothing there. They read the live cache (`source_hash.live_cache()`); a miss is hashed in memory.
3. A per-process memo (same hit fields) means a plan that keys the gpkg and then prints the atlas warning reads it once. The gpkg is in the
   cache after any `pipeline build`, so the warning on every plan rehashes at most the coast file (0.6 GB).
4. An atlas built before T1223 records `stamp:`: `behind` names it ("recorded by size and mtime"), and it is rebuilt. There is no second
   value shape and no backfill: the live atlas is already behind ("records no code") and is rebuilt at the re-cut.
5. `mini_release._drop_stamps` normalises the two `source:` keys by NAME (the scratch gpkg's bytes move every run); every curated `sha256:`
   stays compared.
6. `backfill`'s proof for a missing `source:` key (an mtime before the build) is unchanged and records the sha256.

## Acceptance
- `pipeline/tests/test_atlas_source_content.py`: the digest is the bytes' sha256 wherever the file is; the check writes no cache and a warm
  cache costs no read; one process hashes a file once; an old `stamp:` record is named; backfill still proves a missing source key; the
  atlas does not import the build.
- `test_atlas_inputs.py` and `test_mini_release.py` adapted.
- The mini release matches its baseline.

Depends: T1220. Size: S. Owner: no.
