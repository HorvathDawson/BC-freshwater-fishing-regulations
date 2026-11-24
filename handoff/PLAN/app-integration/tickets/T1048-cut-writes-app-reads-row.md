# T1048 — `release --cut` writes the new version's app_reads row in its version commit

Status: OPEN — 0.9.1 (owner 2026-11-24: in the 0.9.1 bundle)

The 0.9.0 cut (`pipeline/release.py` `cut`, about line 2475) made the version commit without a row for 0.9.0 in
`pipeline/deliver/release/app_reads.json`. The release lane had to add it by hand (6ea5b344a, "Same formats as 0.8.5:
nothing the 0.9.0 app reads moved"). The release gates read that file (`app_gate.py`, `contract.py`), so a forgotten row
stops the release later instead of at the cut.

## Work
1. The cut's version commit adds the new version's row (`pipeline/tools/emit/app_reads.py` makes the row; same formats as
   the previous row unless the app's reads moved, and then the cut stops and says which).
2. `--dry-run` prints the row it would add.
3. A test in `pipeline/tests/test_release_command.py` that fails on main first (the cut leaves the row out), proved by
   mutation.

Depends: none. Size: S. Owner: no.

Source: the 0.9.0 cut, 2026-11-24 (lane L34).
