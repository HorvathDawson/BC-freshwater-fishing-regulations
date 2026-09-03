# `data/curated/` — everything a human's time is embedded in

**Nothing here is regenerable.** Losing a file loses hours; losing anything under `output/`
or `data/generated/` loses CPU. That is the whole distinction, and it is why these have
their own tree.

The category splits again, and the split is the one people get wrong:

| | example | who made it |
|---|---|---|
| **authored** | `waters/splits.json`, `waters/added_lakes.geojson` | a person typed it |
| **promoted** | `gauges/matches.json`, `waters/added_streams.json` | a machine made it and a person **approved** it |

**Promoted data looks generated**, which is the danger: someone re-runs the generator "just
to check" and destroys the review. `gauges/matches.json` carries 206 decisions a person made
against a map, and `waters/added_streams.json`'s negative `blk` ids are a live ABI — reminting
them strands every split bound to the old ones.

## Reach these through config, never as a literal

```python
from pipeline.curated import CURATED
CURATED.waters.splits            # pydantic-validated at first access
CURATED.gauges.matches
```

A wrong path then fails at import naming the key, instead of returning an empty list four
builds later. A `--splits` flag with no default once dropped all 376 curated cuts from three
full builds and nothing looked wrong.

## Regenerating a promoted file

```
python -m pipeline.tools.check_curated       # is anything actually stale?
```

It prints the command and never runs it. If it says `ok`, there is nothing to do — BC
commissions a handful of gauges a year, and a needless regenerate costs a review.

`review.json` beside each `matches.json` is an **input** to matching and is never written by
a program. Two files rather than one is what makes a regenerate physically unable to harm the
review.
