# T1220 — The build's source stamps hold an absolute path and an mtime

Status: IN PROGRESS — lane `fix/T1220-build-stamp-no-path`: PR open for an independent review; merge before the 0.10.0 re-cut build

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1220 | The build's source stamps hold an absolute path and an mtime | IN PROGRESS — lane fix/T1220-build-stamp-no-path | — | review + merge before the 0.10.0 re-cut build (clim, tiles and deliver rerun once on it); the atlas record's mtime (below) needs its own ticket |

Source: the planning chat's build lane, 2026-11-26, measured on the main checkout: `pipeline.build.stamp` of the gpkg reproduces the manifest's
recorded tiles input (`63daf2ac646760c0`); the same file reached through another root gives `7e60d0ba0ca3ec78`. Neighbours, not this defect: T1187
(the atlas record keeps an unowned module's absolute path), T1183–T1186, the #286 design (T1171).

## The defect (on main, 334f493d5)
- `pipeline/build.py` `stamp()` returned `[str(path), st_size, int(st_mtime)]`, the path absolute (`sources()`: `get_config().fwa_data_gpkg`, `SOURCE`).
- It feeds `tiles_key` (gpkg, obstacles), `clim_key` (hydat, stations) and `deliver_sources` (gpkg, stations, places, hydat).
- So a moved checkout, a fresh clone, a copy or a re-download of identical bytes reran clim, tiles and deliver. A same-size edit within one second did not.

## Work (done on the branch)
1. `stamp()` is `sha256:<hex>` of the file's bytes: no path, no time.
2. Hashed once per (repo-relative path, size, mtime_ns), cached in `source-hashes.json` beside the manifest (`MANIFEST` is
   `GENERATED.bundle.parent`; a `--side` run and a test keep their own). A touch costs one rehash, then matches. The gpkg (10.2 GB) hashes in
   4.2–7.3 s on the build Mac (warm page cache; a cold read was not measured).
3. No fallback: a missing source file raises (FileNotFoundError, naming it); an unreadable cache raises, naming the file.
4. Tests: `pipeline/tests/test_source_content_keys.py`. Two roots with identical files key alike; a copy with new times, a touch and a moved
   checkout key alike (the touch costs one rehash); a one-byte change in each file moves exactly the keys that read it; no key input names the
   root. 11 of the 12 fail on main; the missing-file test passes on both.

## Found, not done: the atlas record still uses mtime
`pipeline/atlas/inputs.py` records `source:fwa_gpkg` and `source:fwa_coast` as `stamp:<size>:<mtime>` (no path). A copy or re-download of the
same bytes therefore still reads "changed since the atlas was built", and the release refuses the atlas. It was left as it is (docstring corrected
only) because switching `digest()` to sha256 would:
- add a new reason line to every recorded atlas (the live one included), claiming the gpkg and the coast file changed when they did not;
- need a backfill decision to avoid that: either a second value shape (`stamp:` compared the old way beside `sha256:`), or an owner call to
  re-prove old records. `backfill()` fills only MISSING keys, so it cannot convert them.

The live atlas is already "behind: records no code" and is rebuilt at the re-cut. After that rebuild, a switch would mark it behind once more
unless it lands first. Recommendation: a follow-up ticket (the planning chat numbers it) switches the two `source:` digests to the T1220 pin
BEFORE the re-cut's atlas build, so the new atlas records sha256 and no backfill is needed.

## Acceptance
- The tests above, plus `test_build_command.py`, `test_build_key_holes.py` and `test_tiles.py`, pass.
- The mini release matches its baseline (`mini_release --compare`, `--check`).
- After merging, clim, tiles and deliver rerun once; that rides the 0.10.0 re-cut build.

Depends: none. Size: S. Owner: no.
