# T1048 — `release --cut` writes the new version's app_reads row in its version commit

Status: DONE — ec4480b66 (0.9.1, owner 2026-11-24: in the 0.9.1 bundle)

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

## Done (2026-11-24, lane L4)
`pipeline/release.py` `app_reads_row` (reads git only: rev-parse / cat-file / show) gives the version commit its
`app_reads.json` with the new row; `pipeline/tools/emit/app_reads.py` `reads_in` (what a commit's app reads: its
pack.generated.ts + ALSO_READS) and `for_cut` (the same formats as the newest recorded version, else `CutRefused`
naming each file old -> new). `--cut --dry-run` prints the row; `--reads-moved "<why>"` records a meant move. Tests:
`test_the_cuts_version_commit_adds_the_new_versions_app_reads_row`, `test_the_cut_stops_when_what_the_app_reads_moved_and_its_dry_run_prints_the_row`
(the first failed on main; five mutations killed). A dry run against origin/main moved no ref.
