# T1187 — The atlas record keeps an unowned module's absolute path

Status: IN PROGRESS — lane `fix/T1187-atlas-record-unowned-module-by-name`: PR open for an independent review (a #286 follow-up; latent, no unowned module on the Mac today)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1187 | The atlas record keeps an unowned module's absolute path | IN PROGRESS — lane fix/T1187-atlas-record-unowned-module-by-name | — | review + merge; the stage keys' manifest: T1224 |

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

## Done on the branch (2026-11-26)
- `code_stamp` records `module:<top>=<place>`, the place `<repo>/…`, `<prefix>/…`, `<base_prefix>/…` or `<elsewhere>/<name>`
  (`stepcache.portable_where`): no absolute path.
- `code_behind` finds the file NOW by name (`stepcache.unowned_identity_now`: `importlib.util.find_spec`, the place `_trace._where`
  takes); an entry script (`__main__`, `__mp_main__`) by its recorded place. Not found: "missing", so the atlas reads behind, naming it.
- Tests: `pipeline/tests/test_atlas_record_unowned.py` (an old absolute record found by name in another venv; two install paths
  under `sys.prefix`; the written meta holds no absolute path; an entry script in a copied checkout). All 5 fail on main.
- Review fixes: a spec with no file reads "no file" (as recorded), never "missing"; `<prefix>` is tried before `<repo>`, so a venv
  inside the repo and a worktree with a linked `.venv` record one place. Two more tests (7 in all).
- Not changed: the build manifest's `code_packages` (stage keys) still records the absolute path and `code_key` reads bytes there: T1224.
