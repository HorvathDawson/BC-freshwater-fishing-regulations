# T1224 — A stage key reads an unowned module's bytes at the path its record names

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1224 | A stage key reads an unowned module's bytes at the path its record names | OPEN | — | a lane: key an unowned module in `code_key` by `stepcache.unowned_identity_now` (found by name, as T1187's atlas check does); build-side tests with two venvs |

Source: the independent review of the T1187 PR, 2026-11-26. T1187 fixed the atlas record only; the stage keys are this ticket.

## The defect (on main after T1187)
- The build manifest's `code_packages` (each traced stage's record, `pipeline/build.py` `stage_code`) keeps `module:<top>=<absolute path>`
  for a module no distribution owns (`stepcache.distributions_of`).
- `stepcache.code_key` drops the path from the key's NAME, but `package_versions` reads the bytes AT that recorded path (`file_identity`).
- So:
  - After a venv or checkout move, the path is gone: the key reads "missing" and the stage reruns once.
  - If the OLD venv is still on disk, the key reads the old venv's bytes, not the active one's. An edit to the module the stage really
    imports then reruns nothing.
- Latent today: no unowned module is imported on the build Mac (#286's check). It bites with a uv / virtualenv venv (`_virtualenv.py`),
  an editable install with no `top_level.txt`, a conda package with no RECORD, or an entry script outside the repo.

## Work
1. In `code_key`, value an `UNOWNED` entry with `stepcache.unowned_identity_now(top, where)` (find_spec by name; an entry script by its
   recorded place), not `package_versions` at the recorded path.
2. Have `distributions_of` record the portable place (`stepcache.portable_where`), so the manifest holds no absolute path either.
3. One rerun of every traced stage that records an unowned module (none on the build Mac today).

## Acceptance
Build-side tests (`test_build_key_holes.py`'s sandbox):
- The same unowned module's bytes in venv A, then venv B with A deleted: equal keys.
- With A still on disk and B active, an edit in B moves the key; an edit in A does not.
- The written manifest holds no absolute path for it.

Depends: T1187. Size: S. Owner: no.
