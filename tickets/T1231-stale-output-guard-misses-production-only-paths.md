# T1231 — The stale-output guard does not see production-only code paths

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1231 | The stale-output guard does not see production-only code paths | OPEN | — | the hand-run module scan as a data-tier test; the reviewer's gaps; the sha256 cost in _trace.py |

Source: repo clean chat, 2026-11-27. Evidence: T1229 / PR #359 and its review (merged). `pipeline/tests/test_stage_content_keys.py` checks every curated file a
stage records reading in the sample-release chain, and a static scan of the modules those runs import. The scan that
found the reach's own read of `cuts.json` was run BY HAND over the live build manifest's module lists
(`stages[st].code_files` in `data/generated/build-manifest.json`).

## Work
1. Make that scan a data-tier test (it needs the built manifest: a `needs_*` marker); cover the atlas's recorded
   modules when its record holds them.
2. The reviewer's gaps: the name matcher misses `C.CURATED.x` and `load()` aliases; "covered" means the key function
   opened the file, not that it hashed every field the run reads; `generated.*` reads between stages are covered only
   by stage dependencies.
3. Cost: `pipeline/_trace.py` computes a sha256 on every recorded config read (about 1.6 us per access).

## Acceptance
The test fails when a production module reads a curated file its stage key leaves out (a pin: remove one, see red).

Depends: none (#359 merged). Size: S-M. Owner: no.
