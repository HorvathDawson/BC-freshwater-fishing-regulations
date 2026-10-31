# T322 — Fold the owner-confirmed slough/canal polygons into their streams (own side build + binding diff)

Status: OPEN (owner 2026-10-30: "If no stream flows through then it can be ignored, it is not a stream then … do it in
its own build and ticket"). Split out of T55 (measurements in T55 "Fold: what is needed", 675265492).

## Rule
- Input: `data/curated/decided/sloughs.json` — 811 owner-confirmed calls (`stream`, `P: y`); pre-marks never folded.
- A confirmed polygon with a stream flowing in AND out (graph) becomes part of THAT stream (its sections take the
  stream's identity and rules). Several streams through one polygon: follow the existing braid/merge rules.
- NO stream through it → IGNORED (owner: then it is not a stream). Never create a new unnamed stream item. List these.
- 33 are registry items (29 lake, 3 wetland, 1 already stream); 778 are unnamed polygons outside the registry.

## Work
1. An apply step that fits T266 (decided → curated input the registry/atlas reads); no `decided/` in stage keys.
2. Tests: confirmed + through-stream folds; confirmed + no stream ignored; unconfirmed / `not stream` untouched.
3. Its OWN side build (`--atlas`), after the 10-31 batch: atlas parity minus the old-vs-old baseline, and a BINDING
   diff (every water whose rules change, lake → stream zone rules, named stream rules extended) → owner review
   before promote.

Depends: T55, T266. Size: M–L. Owner: review the binding diff.
