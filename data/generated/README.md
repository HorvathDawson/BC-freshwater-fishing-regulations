# `data/generated/` — everything a program writes

Re-runnable by definition. Losing anything here costs CPU; losing anything under
`../curated/` costs human hours. That is the whole distinction, and it is why these are
sibling directories rather than one tree with a convention.

**There is no `output/`.** It was a fourth tree that meant the same thing as this one, and
it drifted without saying so: six of its declared directories
(`output/pipeline/{graph,atlas,matching,anglerinfo,hydro,deploy}`) had never existed on
disk, and `default_registry_path()` handed seven parser tools a `registry.json` inside one
of them. A missing output directory is created on demand, so that class of bug is silent by
construction.

## The tree

| path | written by | holds |
|---|---|---|
| `atlas/<name>/` | `pipeline.atlas.build --out` | `graph.pkl`, `registry.json`, `blk_chains.pkl`, `geometries.pkl`, `item_points.json`, `splits.resolved.json` |
| `reaches/<name>/` | `pipeline.atlas.reach.cli --out` | rule → sections tables |
| `bundle/` | `pipeline.deliver.bundle` | `bundle.sqlite` (what ships) |
| `tiles/` | `pipeline.deliver.tiles` | `atlas.pmtiles` + per-layer geojsonseq |
| `regs/extraction/` | `pipeline.regs.extraction` | `synopsis_raw_data.json`, `row_images/` |
| `regs/parsing/` | the LLM parse ingest | `synopsis_parsed.json`, `session_state.json` |
| `regs/parse/` | `run_parse.sh` | transient work dir: `batches/ responses/ reviews/` |
| `regs/dfo_salmon/` | `pipeline.regs.dfo_salmon.{locations,parse,untangle}` | the scrape |
| `regs/entries_backup/` | `pipeline.tools.reparse_candidates` | EntryFile backups |
| `gauges/` | `pipeline.gauges.generate` / `.feed` | `candidates.json`, `feeds/<station>.json` |
| `stocking/`, `bathymetry/` | the domain audits | identifier audits awaiting a matcher |
| `municipal/` | `added_streams` cleaning | cleaned source layers |
| `added_streams/` | `pipeline.atlas.waters.added_streams` | mapcheck / verify html, demo gpkg |
| `scratch/` | `pipeline/hack/*` | one-shot candidate dumps; nothing reads these |

## Addressing it

Through `pipeline.common.curated.GENERATED`, never as a literal:

```python
from pipeline.common.curated import GENERATED

GENERATED.build()                  # data/generated/atlas/full — for a WRITER
GENERATED.require_build()          # …and it must exist — for a READER
GENERATED.registry()               # …/registry.json, same guarantee
GENERATED.tiles                    # data/generated/tiles
GENERATED.regs.parse               # data/generated/regs/parse
```

**The rule is asymmetric, and it is the point.** A writer may create its directory; a reader
may not. `require_build()` refuses a directory that is not there and prints the command that
makes one — so the situation that used to produce a wrong answer three builds later is now
one legible line. `pipeline/tests/test_curated_paths.py` fails on any literal `output/` path
and on any generated path that lands inside `../curated/`.

## Git

All of this is ignored (`data/.gitignore`), with two temporary exceptions documented there:
`bathymetry/audit.json` and `stocking/identifier_audit.json` predate their matchers and
cannot currently be regenerated.
