# `data/curated/` — everything a human's time is embedded in

**Nothing here is regenerable.** Losing a file loses hours; losing anything under `data/generated/` loses CPU. Only
the owner's decisions and hand-made data live here; machine output (proposal files, the gauge review queue) lives in
`data/generated/` even when the review app reads it (T324, owner 2026-11-05).

**Names (T324):** `*_calls.json` = the owner's per-item decisions · `*_waters.json` = a list of waters ·
`*_stations.json` = a list of gauges. Every dict-shaped JSON carries an `_about` header (what it holds, who writes it,
who reads it); every folder has a README.

| folder | holds |
|---|---|
| [`waters/`](waters/README.md) | the shape of the atlas (cuts, names, areas, added waters, tidal limits, sloughs) and lists of waters |
| [`regulations/`](regulations/README.md) | the synopsis corpus (`synopsis/`), the book's text (`reference/`), book-name links |
| [`dfo_salmon/`](dfo_salmon/README.md) | DFO's places (`places/`) and the limits wording registry |
| [`gauges/`](gauges/README.md) | gauge matches, the owner's match calls, tidal / backwater stations |
| [`inseason/`](inseason/README.md) | in-season notices bound to waters |
| [`runtiming/`](runtiming/README.md) | the owner's run-timing calls |
| [`review/`](review/README.md) | the review app's state: the append-only marks, closed domains |

The category splits again, and the split is the one people get wrong:

| | example | who made it |
|---|---|---|
| **authored** | `waters/cuts.json`, `waters/added_lakes.geojson` | a person typed it |
| **promoted** | `gauges/matches.json`, `waters/found_names.json`, `waters/added_streams.json` | a machine made it and a person **approved** it |
| **parsed, then curated** | `regulations/synopsis/region-*.json` | the parser wrote it (a person runs it — it spends credits); people correct it in the review app |

**Promoted data looks generated**, which is the danger: someone re-runs the generator "just to check" and destroys the
review. `gauges/match_calls.json` carries the decisions a person made against a map, and `waters/added_streams.json`'s
negative `blk` ids are a live ABI — reminting them strands every cut bound to the old ones.

## Reach these through config, never as a literal

```python
from pipeline.common.curated import CURATED
CURATED.waters.cuts              # pydantic-validated at first access (config.yaml `curated:`)
CURATED.gauges.matches
```

A wrong path then fails at import naming the key, instead of returning an empty list four builds later — and a missing
curated file RAISES wherever it is read (P2: no missing-file fallbacks).

## Checks

```
python -m pipeline.tools.audit.check_curated            # is a promoted file stale against its roster? (prints, never runs, the fix)
python -m pipeline.tools.audit.check_curated --waters   # the hand-edited waters files: shape + references, seconds
```

`--waters` is also the build's first step and a CI step: a bad hand edit fails in seconds, not after the atlas.

## Writing here

The review app's saves land as marks (`review/marks.jsonl`, append-only) and write through to the final file of their
domain (`pipeline.regs.review.apply`). By hand: back up first (AGENTS rule 2) and write region files through
`pipeline.regs.parsing.io.write_entryfile`, which validates the whole file.
