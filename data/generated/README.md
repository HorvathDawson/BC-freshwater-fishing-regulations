# `data/generated/` — everything a program writes

Re-runnable by definition. Losing anything here costs CPU; losing anything under
`../curated/` costs human hours. That is the whole distinction, and it is why these are
sibling directories rather than one tree with a convention.

**There is no `output/`.** It was a fourth tree that meant the same thing as this one and drifted without saying so:
seven parser tools were handed a `registry.json` in a directory that never existed, silently.

## The tree

| path | written by | holds |
|---|---|---|
| `atlas/<name>/` | `pipeline.atlas.build --out` | `graph.pkl`, `registry.json`, `geometries.pkl`, `item_points.json`, `splits.resolved.json` |
| `reaches/<name>/` | `pipeline.atlas.reach.cli --out` | rule → sections tables |
| `bundle/` | `pipeline.deliver.bundle` | `bundle.sqlite` (what ships) |
| `tiles/` | `pipeline.deliver.tiles` | `atlas.pmtiles` + per-layer geojsonseq |
| `regs/extraction/` | `pipeline.regs.extraction` | `synopsis_raw_data.json`, `row_images/` |
| `regs/parse/` | `run_parse.sh` | transient work dir: `batches/ responses/ reviews/` |
| `regs/dfo_salmon/` | `pipeline.regs.dfo_salmon.{locations,parse,untangle}` | the scrape |
| `regs/entries_backup/` | `pipeline.tools.audit.reparse_candidates --apply` | copies of the catalogue region files, taken before it deletes entries to queue them for re-parse |
| `gauges/` | `pipeline.gauges.generate` / `.feed` / `pipeline.tools.audit.datum_remarks` | `candidates.json`, `feeds/<station>.json`, `datum_proposals.json` (T1047: HYDAT's datum / relocation remarks parsed, proposals; committed) |
| `stocking/`, `bathymetry/` | the domain audits | nothing committed (the two frozen identifier audits were removed from git 2026-11-27; in history at 9337319f7) |
| `municipal/` | `added_streams` cleaning | cleaned source layers |
| `added_streams/` | `pipeline.atlas.waters.added_streams` | mapcheck / verify html, demo gpkg |
| `sloughs/` | the T148 finder (`sloughs/v2/revise.py`) / automark (`sloughs/v2/automark/automark.py`; both removed from the tree by T1081, in git history at 03be2862e) | `candidates_v2.json.gz`, `slough-automark.json`: the Sloughs tab's candidates and pre-marks (committed; read by `pipeline.regs.review.slough`) |
| `review/dfo-preverify.json` | `.venv/bin/python -m pipeline.regs.dfo_salmon.preverify` | the DFO suggestions' checks (network, atlas); not committed |
| `review/dfo_closure_entries.json` | `-m pipeline.regs.dfo_salmon.closures` | the closure entries proposed to the review app; not committed |
| `review/entry-split-suggestions.json` | `-m pipeline.regs.review.split_suggest` | missing-split proposals (atlas); not committed |
| `review/tidal/tidal-preverify.json` | `-m pipeline.tools.audit.tidal_preverify` | the tidal candidates' checks (atlas, DEM); not committed |
| `review/tidal/pass2/proposals.json` | `-m pipeline.tools.audit.tidal_pass2 extract`, then `propose` | DFO's words on the tidal waters (DFO pages, atlas); not committed |
| `scratch/` | one-off audits (`pipeline/tools`) | one-shot candidate dumps; nothing reads these |

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

Ignored (`data/.gitignore`) except `gauges/candidates.json`, `gauges/datum_proposals.json`, `sloughs/` and the review
tool's hand-made inputs under `review/`. The five `review/` files with a command above are ignored too (T1193): a fresh clone has none.
