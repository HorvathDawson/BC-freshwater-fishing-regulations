# T111 — Every cut reaches the walk: only the first extent's window seeds tributaries

Status: OPEN.

Source: owner 2026-10-22 (unify checks that decide the same thing); unification audit 2026-10-22, finding reach 5
(CONFIRMED in code; the live list is from the reach agent).

## Decision being unified
"Where does this rule's reach cut the water, and which cuts bound its tributary walk?"

## Where it is decided today
- `pipeline/regs/reach/build.py:434` `window = window or got["window"]`: only the first extent's window survives.
- `pipeline/atlas/graph/tributaries.py:584-592`: once a window is given, the node `lower_bound` fallback is skipped.
- `pipeline/regs/reach/build.py:793` `_cut_places`: re-derives the cuts and ignores `cut_pick` (the ambiguous-cut
  ruling: the cut nearest the rule's other end).

How they differ: in `alouette_river.r2`, `bull_river.r1` and `atnarko_bella_coola_rivers.r2`, creeks at the second
reach's lower cut are never seeded, so their tributary rules are missing.

## Work
- `resolve_extent` returns the `cuts` it actually used (after `cut_pick`). A list of windows goes to the seeding, and
  `confluence_excludes` / `_cut_places` read the same cuts instead of re-deriving them.
- Pin: a test that `_cut_places` (or its successor) has no geometry of its own, and fixtures for a two-extent rule
  (both lower cuts seed) and an ambiguous cut (the picked one is used everywhere).

## Acceptance
Proof is a reviewed per-rule binding diff from a side-dir reach run: the 3 rules gain sections (additions only), plus
every rule with ambiguous or confluence cuts, each move named.

Rebuild: reach + deliver (no atlas). Depends: run in the same reach rerun as T102 if convenient, but diff separately.
Size: M. Owner: no.
