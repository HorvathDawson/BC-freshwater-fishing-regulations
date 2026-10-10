# `data/curated/` — everything a human's time is embedded in

**Nothing here is regenerable.** Losing a file loses hours; losing anything under `data/generated/`
loses CPU. That is the whole distinction, and it is why these have
their own tree.

The category splits again, and the split is the one people get wrong:

| | example | who made it |
|---|---|---|
| **authored** | `waters/splits.json`, `waters/added_lakes.geojson` | a person typed it |
| **promoted** | `gauges/matches.json`, `gauges/candidates.json`, `waters/added_streams.json` | a machine made it and a person **approved** it (or queued it for review) |
| **parsed, then curated** | `regulations/entries/catalogue/region-*.json` | the parser wrote it (a person runs it — it spends credits); people correct it in the curation-review app |

**Promoted data looks generated**, which is the danger: someone re-runs the generator "just
to check" and destroys the review. `gauges/review.json` carries 231 decisions a person made
against a map, and `waters/added_streams.json`'s negative `blk` ids are a live ABI — reminting
them strands every split bound to the old ones.

## Reach these through config, never as a literal

```python
from pipeline.common.curated import CURATED
CURATED.waters.splits            # pydantic-validated at first access
CURATED.gauges.matches
```

A wrong path then fails at import naming the key, instead of returning an empty list four
builds later — and a missing curated file RAISES wherever it is read (P2: no missing-file
fallbacks). A `--splits` flag with no default once dropped all 376 curated cuts from three
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

`gauges/candidates.json` is the review QUEUE `promote` writes beside the matches: generated matches
an independent check disagreed with. It is committed so CI sees it; only ACTIVE stations need a
decision (`check_curated`).

## Writing here

Every write from the curation-review app backs the target up first and is atomic
(`curation-review/backend/writes.py`). By hand: back up first (AGENTS rule 2) and write region files
through `pipeline.regs.parsing.io.write_entryfile`, which validates the whole file.
