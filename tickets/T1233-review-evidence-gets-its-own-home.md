# T1233 — Review evidence gets its own home, apart from throwaway generated files

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1233 | Review evidence gets its own home, apart from throwaway generated files | OPEN | — | after T1232's loud refusals: `git mv` the evidence to `data/review/<domain>/`; a re-make writes a dated copy, adopted after a printed diff |

Source: the owner, 2026-11-27, verbatim: "maybe we shoudl clean up pathway of ddata for reviews"; his pick on the options:
"A: a "review evidence" folder (Recommended)" (loud refusals first). Full research, per-domain table and options:
`~/.claude/projects/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/night-notes-2026-11-26/review-data-pathway.md`
(section D; baseline at origin/main f9927e4ee).

## The problem
Files the owner's marks depend on sit in `data/generated/` beside throwaway output, so "generated = safe to delete" is
false there: the Q154 sample, the tidal pass-2 proposals (5 tidal marks sit on rows that exist only there), the
hand-made files (dfo-split-hunt, dfo_fix_proposals, t89-awaiting-water, half of dfo_closed_area_geometry).

## The domains (one row each; inputs / in-between / his decisions / applied to / state)
| domain | inputs the tool shows | in-between | decisions | applied to | state |
|---|---|---|---|---|---|
| tidal | tidal-candidates, -edits, -web, -osm-landmarks, -preverify, pass2/proposals | pass2/entries | 1,168 marks + 3 hand files | waters/tidal_limits.json | open |
| DFO salmon | dfo-suggestions, -preverify, -split-hunt, dfo_fix_proposals | dfo-*.md | 309 marks + owner_carries | dfo_salmon/places + cuts.json | open |
| DFO closed areas | dfo_closure_entries, dfo_closed_area_geometry | none | 16 marks | dfo_salmon/closed_areas.json | open |
| entries / splits | entry-split-suggestions, t89-awaiting-water | none | 37 marks | synopsis region files + cuts.json | open |
| names | name-variants/candidates | none | 0 marks, 7 calls | waters/found_names.json | open |
| sloughs | candidates_v2.json.gz, slough-automark | none | 811 marks | waters/slough_calls.json | open |
| ponds (Q154) | q154/sample | none | 80 marks | nothing | closed |
| proposals | proposals/index | none | 7 retired marks | nothing | closed |

## Work (option A)
1. After T1232: every file a mark's check reads, plus the hand-made review files, moves unchanged by `git mv` to a
   tracked `data/review/<domain>/` (one README table: writer, vintage, sha256); marks hash content, not paths.
2. A re-make writes a new dated copy there, adopted only after a printed before/after of the marks' held / stale /
   orphan counts.
3. What is left under `data/generated/review` is then truly throwaway and leaves git.

## Acceptance
The marks health counts per scope are identical before and after the move. Baseline (notes, `health.py`, no `applied`
keys): closed_area 16 held; q154 80 held; slough 811 held; tidal 1,168 held; dfo 107 held / 202 stale (names, rule, split:
none). Re-measure the entry scope (37 marks) before the move.

Depends: T1232 first. Size: M. Owner: no (decided: option A).
