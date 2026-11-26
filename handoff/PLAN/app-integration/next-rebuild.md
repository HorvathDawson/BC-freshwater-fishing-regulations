# Side atlas rebuild: how to try a curated change on a copy before promoting it

The 2026-11-11 rebuild (T664 oxbows, T665 tidal polygon rule, nine DFO cuts) ran this way and is promoted; its
step-by-step run and numbers are in git history (`next-rebuild.md`, `next-rebuild-results.md`). The tests that pin what
it built are `pipeline/tests/test_next_rebuild.py` and `test_shore_cuts.py`. What it left is ticket T677.
`T665-main-rivers-map.html` (beside this page) is the held-lines map the T665 lane drew; T677 and `waters/shore_held.json`
name it. The parser (`run_parse.sh`) is human-only: never run it for this.

## Steps
Heavy steps go through `scripts/heavy.sh` (`scripts/heavy.sh --status` first), one at a time.

1. Copy the curated tree and point every command at the copy (live data is never touched until step 6):
   ```
   SIDE=$PWD/data/generated/side/<name>
   .venv/bin/python -c "from pathlib import Path; from pipeline.common.curated import make_root; make_root(Path('$SIDE/curated'))"
   export CURATED_ROOT=$SIDE/curated
   ```
   Apply the change under test to the copy. Delete a reused `$SIDE/curated` first.
2. Build the side atlas (`python -m pipeline build --side DIR [--atlas]` runs every stage under DIR; the atlas alone is
   `python -m pipeline.atlas.build --full --out $SIDE/atlas/full_next`). It stops by itself on a failed gate and names
   the file to read.
3. Parity and the id dry run, no write: `python -m pipeline.atlas.promote $SIDE/atlas/full_next --dry-run`, and
   `python -m pipeline.atlas.inputs --build $SIDE/atlas/full_next`. Every removed or added item, section and boundary
   must be explained; a lost boundary of family `curated`, `length`, `area`, `border` or `lake` is a stop. Promote
   refuses while a curated id is gone or absorbed (`--accept-dead-ids`, `--accept-boundary-loss`: never pass either unread).
4. If item ids moved, resync the entries (see GOTCHAS, "registry change -> entry resync"):
   `python -m pipeline.regs.parsing.backfill_matched --registry $SIDE/atlas/full_next/registry.json --dry-run`, then the
   matcher (`python -m pipeline.regs.matching.matcher --registry ...`).
5. Reach run on the side atlas and the per-rule diff:
   `python -m pipeline.atlas.reach.cli --build $SIDE/atlas/full_next --out $SIDE/reach`, then
   `python -m pipeline.atlas.reach.binding_diff data/generated/reaches/full $SIDE/reach --json $SIDE/binding_diff.json`
   (exit 1 when anything differs). Read the diff by place, not only by id: an id diff cannot see a rule that kept its
   section id while the section grew (T677 item 3).
6. Stop and show the owner. Only on his go: `unset CURATED_ROOT`, back up the synopsis files (AGENTS A2), apply the same
   change to the live tree, `promote --dry-run` again, then `promote` (it renames the side atlas to
   `data/generated/atlas/full` and the old one to `full.prev`). If a curated atlas input changed since the copy, rebuild
   from step 1.
7. Full build on the promoted atlas, then `handoff/PLAN/runbooks/release.md`.
