# Next rebuild: one side atlas for T664 + T665 + the nine DFO cuts (runbook)

Written 2026-11-11 by the T664 lane. Run it top to bottom, and read each step's check before going on. Heavy steps go
through `scripts/heavy.sh` on the DEFAULT lane (`scripts/heavy.sh --status` first), one at a time. Claude may run
every step here except 9b (the parser: human only). The release itself is `handoff/PLAN/runbooks/release.md`.
`$SC` below is a scratch dir.

## 0. Before anything
```
rtk git status                                   # T664 code committed; T665 code committed (its lane)
mkdir -p $SC/backup && cp data/curated/regulations/synopsis/region-*.json $SC/backup/      # AGENTS A2
```
Check: `node app/tools/check-tickets.mjs` passes; the T664 commit contains `pipeline/atlas/registry/one_water.py` and
the `one_water` flag in `name_variants.json`.

## 1. Inputs (curated edits; all read by the atlas build)
a. **T664, Oxbows as one item.** Already in the tree: the `one_water: true` flag on the T637 name record in
   `data/curated/waters/name_variants.json`, and the registry pass `registry.one_water.merge_one_waters` (run in
   `atlas/build.py` after the MUs, and in `atlas/sidecars.py`). No curated edit is needed before the build.
b. **The nine DFO cuts.**
   `git apply handoff/PLAN/app-integration/T664-pending/nine-cuts.cuts.json.patch` (9 cuts; table and ambiguities in
   `T664-pending/README.md`). Only if the owner has not changed a mark since 2026-11-11: re-make it with
   `python -m pipeline.regs.review.proposals side <the 9 mark-dfo-… ids> --out $SC/side9` and diff against the patch.
c. **T665, tidal polygon exit cut + inside removal.**
   > **GAP: the T665 lane fills this in** (its curated inputs, e.g. tidal limits or `unplaced_piece_calls`; its code
   > in `pipeline/atlas/splits/shore.py` and the shore stage; its own checks). Leave this box until that lane writes it.

Check: `.venv/bin/python -m pipeline.tools.audit.check_curated --waters` → 0 errors (2026-11-11, on a root with 1a+1b:
0 errors, 0 warnings).

## 2. Side atlas
```
scripts/heavy.sh atlas 50 -- .venv/bin/python -m pipeline build --atlas      # -> data/generated/atlas/full_next + parity
```
Read:
- The build log line `one water (T664) Okanagan River Oxbows: 44 item(s) into wsc:300-432687-459615, 80 section(s);
  kept apart (still water): 8`, and `full_next/one_water_report.json`. A different count means the T665 or cut inputs
  moved an oxbow piece. Stop and read it before going on.
- `full_next/registry.json`: the 9 new boundaries on Nicomekl, Serpentine, Nakina (2), Thompson (2), Harrison and
  Nicomen (2), each at the measure in the README table (±5 m; the build snaps).
- Parity (`pipeline/tools/audit/build_parity.py`, printed by `--atlas`): items −44 (oxbows) plus whatever T665 states;
  sections: +9 to +18 from the cuts, plus T665's; boundaries +9, plus T665's. No boundary loss (promote refuses it).
- `pipeline.atlas.inputs` on `full_next`: every input hash current.

## 3. Id resync, entries (registry change; entry ids are row ids, so nothing is orphaned)
```
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.parsing.backfill_matched --registry data/generated/atlas/full_next/registry.json --dry-run
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.parsing.backfill_matched --registry data/generated/atlas/full_next/registry.json --restamp r8:okanagan_river_oxbows@8-1
git diff data/curated/regulations/synopsis/region-8.json
```
Check: the dry run lists only the oxbow entry, and any T665 entries that lane names. The region-8 diff is the
`matched` lines only, 53 → 9, equal to the region-8 hunk of `T664-pending/t664-references.patch`.
`prune_remapped` no longer exists: entry ids are `<region>:<row slug>@<MU>` and carry no item id. Then:
```
.venv/bin/python -m pipeline.regs.matching.matcher --registry data/generated/atlas/full_next/registry.json
```
It must pass. The oxbow override's 31 codes and 40 wbks resolve to `wsc:300-432687-459615` plus the 8 still-water items.

## 4. Id resync, the other curated references
Apply the notice hunk: `git apply --include='data/curated/inseason/*' handoff/PLAN/app-integration/T664-pending/t664-references.patch`.
It sets in-season match `is-bb5426c61ff3` to 9 extents and adds a `set` history row. Its waters and stamp are
re-resolved by the release upload (`pipeline.regs.inseason.resolve --release …`). Check: `flowing.absorbed_refs` over
the corpus is empty; the reach run refuses otherwise. A grep of `data/curated` for the 44 absorbed ids must find
nothing (list in `T664-pending/README.md`).

## 5. Reach run on the side atlas + per-rule binding diff (AGENTS 63)
```
scripts/heavy.sh reach 50 -- env PYTHONPATH="$PWD" .venv/bin/python -m pipeline.atlas.reach.cli \
    --build data/generated/atlas/full_next --out $SC/reach_next
.venv/bin/python -m pipeline.atlas.reach.binding_diff data/generated/reaches/full $SC/reach_next --json $SC/binding_diff.json
```
Expected, and every other difference must be explained before going on:
- `okanagan_river_oxbows.r1`: 89 → 89, the same sections (80 survivor + 8 still-water polygons + `lake:329461947`
  through river widening). No other rule moves for T664.
- The nine DFO places do not move yet (their extents land in step 7). The rules on Nicomekl, Serpentine, Nakina,
  Thompson, Harrison and Nicomen keep their sections, with new ids where a cut split a section. Each lost id must
  reappear as two ids.
- T665: whatever its lane predicts (tidal sections removed or added). **GAP for the T665 lane.**
- Standing: unplaced pieces are only the accepted ones (`unplaced_piece_calls.json`); no dead override.

## 6. Promote
```
.venv/bin/python -m pipeline.atlas.promote data/generated/atlas/full_next --dry-run     # parity = steps 2 + 5
.venv/bin/python -m pipeline.atlas.promote data/generated/atlas/full_next
```
Check: refused for a moved boundary or a boundary loss? Read the list; never pass `--accept-*` unread.

## 7. Land the nine DFO marks (they leave NOT_LANDED_OK)
```
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.review.apply dfo --dry-run      # = T664-pending/nine-cuts.dfo-places.patch
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.review.apply dfo --side-checked <id> …   # one commit per proposal
```
Then remove the nine `dfo|…` keys from `NOT_LANDED_OK` in `pipeline/regs/review/apply.py`, and run
`python -m pipeline.regs.review.apply --check`: exit 0, with the two T359 tidal keys still allowed. The `--dry-run`
diff must equal the dfo-places patch. If it differs, a mark changed: the marks are the authority.

## 8. Full build on the promoted atlas, then the release
```
scripts/heavy.sh build 50 -- .venv/bin/python -m pipeline build      # reach + tiles + deliver (labels: one oxbow name)
```
Check: the binding diff against the live release (the release command prints it) holds the step-5 changes plus the
nine DFO places, now bound. Read it per place: Nicomekl and Serpentine 23 m each; Nakina 1 km (whole width; the mark
cannot say "centre line to northern bank"); Thompson Kamloops Lake→Goldpan and Goldpan→Skihist; Harrison lake→bridge
and bridge→Fraser; Nicomen bridge→Fraser and Siddle→Fraser. Then in the app: searching "Okanagan River Oxbows" gives 1
stream hit plus the 8 still-water polygons, one card, one map name.
Then `handoff/PLAN/runbooks/release.md`.

## 9. Hand-offs
a. Owner: the two T664 calls in `T664-pending/README.md` (fold the 8 still-water oxbows in?) and the Nakina half-width.
b. **Human only (credits): `bash pipeline/regs/parsing/run_parse.sh parse-missing`.** Expect nothing to parse, since
   no entry id moved. Give the owner the command; never run it.
