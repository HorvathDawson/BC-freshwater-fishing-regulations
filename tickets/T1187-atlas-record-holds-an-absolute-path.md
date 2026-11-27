# T1187 — The atlas record keeps an unowned module's absolute path

Status: OPEN — a #286 follow-up; scoped 2026-11-26: real on main, latent (no unowned module on the Mac today)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1187 | The atlas record keeps an unowned module's absolute path | OPEN — a #286 follow-up; scoped 2026-11-26: real on main, latent | — | a lane: record an unowned module by name (and a path relative to the repo or `sys.prefix`); the check finds its file now by name; a test with two install paths |

Source: the planning chat's follow-ups after #286, 2026-11-26 (day notes); the third review's M1 fix keys a module no distribution owns by its file's bytes.

## The defect (on main, 1e2dc8a3b)
- The recorder writes a P line with the ABSOLUTE file a package was loaded from (`_trace._where`, `pipeline/_trace.py:236-242`); `distributions_of` turns an
  unowned one into `module:<top>=<that path>` (`stepcache.py:171-184`).
- A stage key drops the path on purpose (`code_key`, `stepcache.py:357-358`: "by its name, not its path"). The atlas record does not: `code_stamp` stores
  `package_versions(code["packages"])` keyed by the full entry (`pipeline/atlas/inputs.py:276`), and `code_behind` re-reads the bytes at that stored path
  (`inputs.py:325-331`).
- So the same atlas read from another checkout or machine (or after the venv is rebuilt elsewhere) finds the path gone, `file_identity` says "missing", and
  `python -m pipeline.atlas.inputs` says "behind" (rc 1) for a module whose bytes did not change; the release refuses the atlas.
- Latent today: #286's read-only check found no unowned module in any stage's real imports on this Mac. It bites with a uv / virtualenv venv
  (`_virtualenv.py`), an editable install with no `top_level.txt`, or a conda package with no RECORD.

## Proof
A unit test: an atlas meta whose packages hold `module:x=/old/venv/x.py`, the same bytes now at `/new/venv/x.py` (importable as `x`): `code_behind` names it
today; after the fix it does not, and a changed byte still does.

## Fix (one sentence)
Record the entry as `module:<top>` with its bytes (as `code_key` does) plus a path relative to the repo or `sys.prefix` for the message, and have the check find
the file now by name (`importlib.util.find_spec`), so only a byte change makes the atlas behind.

## Acceptance
The test above; the record holds no absolute path (a test greps the written meta). Severity: low. Model: Sonnet. Size: S.
