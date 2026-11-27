# T1256 — Name candidates: a short list, a fixed miner, a re-mine

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1256 | Name candidates: a short list, a fixed miner, a re-mine | OPEN | — | Names tab short list; fix the miner (path errors, normaliser); re-mine (heavy job); review the OSM data preparation; after 0.10.0 |

Source: the owner, 2026-11-27, on the name-variants candidates file, verbatim: "keep and fix. IF it looks like the
names could be valuable? could you go through sources etc and see how many are actually potentially new and not in the main name variants file etc etc"
and, after the measurement: "short list plus remine also 1we could look at the osm data preparation at same time? this i think will be an after 0.10 item?"
Related: T1233 (review data pathway).

## Measured 2026-11-27 (read-only)
Script and CSVs: `~/.claude/projects/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/night-notes-2026-11-27-planning/name-candidates/`.
Of 8,649 undecided candidates:
- about 536 clean new names on streams with no name in the app (386 with high or medium evidence; 174 of those carry a
  water-specific regulation);
- about 110 other names for already-named waters (plus 10 English / Indigenous pairs);
- 7,401 a river's name sitting on its own side channel or braid;
- the rest duplicates, misspellings, likely mis-matches and generic names.

## Known problems with the miner
- It reads only OSM `name` (falling back to `name:en`); `alt_name`, `old_name` and `name:xx` were never mined.
- Three path-index errors after the docs folder move (checked on origin/main): `docs/curation-review/name-variants/fwa_index.py:22`
  and `osm_extract.py:20` use `parents[4]` where `parents[3]` is the repo root; `mine.py:32` uses `HERE.parents[3]` where
  `parents[2]` is right. `tables.py:7` has the same `parents[4]` (a fourth, found by this lane).
- It has its own name normaliser that differs from `pipeline.common.names.key`.
- A re-mine needs the OSM extract re-run (a heavy job) and renames candidate ids.

## Work (proposal)
1. The Names tab shows a short list: the clean new names, strongest first, the ones with their own regulation on top;
   the other-names list becomes search aliases; the junk groups are hidden, not deleted.
2. Fix the miner (paths, one normaliser, the extra tags).
3. Re-mine (heavy job, one at a time).
4. Look at the OSM data preparation at the same time: which tags are mined, the normaliser.

Target: after 0.10.0. Depends: none (related T1233). Size: L. Owner: no.
