# pipeline/regs/dfo_salmon: DFO salmon limits, openings and closures (Regions 1-8)

The federal salmon rules for BC's eight freshwater regions, from `pac.dfo-mpo.gc.ca`: scraped, typed, bound to
waters and published as a live feed the app lays over the release's salmon chains. Salmon in non-tidal water is
DFO's; everything else is the provincial Synopsis. The two overlay and never merge.

## The design in one measurement

Re-parsing archived DFO pages (`python -m pipeline.regs.dfo_salmon.churn --regions 1 2 6 --detail`): Region 6's
waters were 77 of 77 present over 2.3 years, while the rules turned over by about half. The geography never
moves, the rules do. So the expensive half (which registry item a name is, where the cuts sit, whether
tributaries are in) is curated once; the cheap half is re-scraped every run and thrown away.

```
page -> fetch -> parse -> untangle -> locations --+--> typed -> feed      (scraped, disposable)
                                                  | fingerprint
                                                  v
                          entries seed / reconcile -> curated locators    (human, durable)
                                                  | join at read time
                                                  v
                  atlas/reach/dfo_salmon.py -> deliver/salmon.py -> release `salmon`
```

A locator is a place a rule attaches to (a named water's reach, or a cascade default); a rule record is
species x dates x limits keyed only by its locator's fingerprint (sha256[:16] of section, water, specific area).

## Stages

Each is a module run from the repo root as `.venv/bin/python -m pipeline.regs.dfo_salmon.<module>`.

| module | job | writes |
|---|---|---|
| `fetch` | snapshot each page, validate, hash; never overwrite good with bad | `cache/dfo_salmon/` |
| `parse` | faithful transcription: every `<tr>` a row, nothing read from the cells | `data/generated/regs/dfo_salmon/regionN.json` |
| `untangle` | regroup into waters, reaches, rules; `verify()` proves no row is lost | `--print` / `--out` |
| `locations` | split each place into a locator and rule records | `.../{locations,rules}/` |
| `typed` + `feed` | the whole decode of limits and dates, once | `.../typed/` |
| `quota`, `wording`, `header` | the salmon bucket, the closed limits-wording registry (`data/curated/dfo_salmon/wording.json`), region header rules | read by `typed` |
| `entries` | the curated locators: `seed` adds new places, `reconcile` classifies drift and never mutates | `data/curated/dfo_salmon/places/` |
| `closures`, `closure_mouths`, `side_streams` | a closed area inside a place is its own location (AGENTS 68); side streams from DFO's own words | read by the reach run |
| `live` | fetch, type, join, write the one feed file the app reads; uncurated places are held | `.../feed/index.json` (`dfo-feed/3`) |
| `refresh` | what a fetch would change against the typed rows in use | nothing |

Curation helpers suggest and never apply (the owner marks, AGENTS A8): `match` (exact registry hits only),
`suggest`, `preverify`, `coverage`, `splitwork`, `dossier`, `edge_gaps`, `cascade`. Measuring: `stability`,
`churn`, `validate`.

```bash
.venv/bin/python -m pipeline.regs.dfo_salmon.fetch
.venv/bin/python -m pipeline.regs.dfo_salmon.parse
.venv/bin/python -m pipeline.regs.dfo_salmon.entries reconcile
.venv/bin/python -m pipeline.regs.dfo_salmon.live run --regions 2 --dry-run   # writes nothing, prints the changes
.venv/bin/python -m pipeline.regs.dfo_salmon.live queue --regions 6           # held places into DFO mode
```

The release side: `atlas/reach/dfo_salmon.py` places each location that has an extent, and
`deliver/salmon.py` precomputes each chain's answers per day into the release's `salmon` file. The feeds
worker (`app/workers/feeds/src/dfo/`) is the TypeScript twin of `live`; `app/workers/feeds/parity/` holds them
to the same output. Nothing needs credits or secrets; never a GitHub schedule (the worker's cron starts it).

## Identity and reconcile

- `fingerprint` is the join key; `location_id` is assigned once and derived from nothing; `section` is an
  attribute, never part of the id; `fingerprints` is a list so wording drift costs nothing.
- A locator is `status: dormant` when its fishery closes, never deleted: these pages list openings.
- A human supplies per locator what nothing can re-derive: `WaterBinding.item_ids` (an exact registry hit or a
  curator, never heuristic), `Binding.extents`, `Binding.tributaries` (three-valued, AGENTS 9), notes.
- `reconcile` outcomes by severity: `ok`, `dormant`, `revived` 0; `drift` 1 (confirm the rebind); `new` 2;
  `section_moved` 3; `structural` 4 (banner set changed, locator count moved over 20%, or over 25% unbound:
  hold the region). A held locator keeps its last binding and only that water is withheld.

## Region 6 is a cascade

Every other region is a flat list. Region 6 is nested boxes where the smallest one containing the water wins:
A (all), B Skeena (B(i) above the CNR Railway Bridge at Terrace, B(ii) below), C Nass, D Haida Gwaii, F (Fraser,
banner only), and E, everything else (a set difference), with tidal-Area children. `cascade.build_scopes()`
derives the tree and `chain_for(scope)` gives the lookup order, narrowest first. Precedence (3 named water, 2
area, 1 section, 0 region default) belongs to the locator, so the feed carries none and the join may never
drop it (`test_the_skeena_cascade_resolves_through_the_join`). The tidal-Area rows belong to E alone. Section F
has no rows; its closure is synthesised with `source: "section_banner"`. Waters key on `(section, name)`.

## Rules a newcomer must know

- The transcription reads nothing out of the Limits or Dates cells; `typed.py` owns every reading.
- Never hand the whole page to a DOM parser: Regions 4, 5a and 7 carry an unterminated `<!--`, so the table is
  sliced out of the raw HTML by regex (`test_dfo_salmon.py` pins it). Region 5 is three pages (`5a`, `5b`); pages
  key by slug.
- Rules are a feed, locators are curated: `feed.py` takes no curated input; they meet at read time.
- `take=0` is not closed: `no_fishing` and `non_retention` differ; a bare "0 per day" is flagged. A size
  sub-limit is its own rule with `within`; a gear rule scopes by `when_targeting`. An unreadable season goes
  to review; an empty dates cell means all year.
- Early exit on the page hash, never `dateModified`. Fishery Notice rows (`FN0679`) change without it moving.
- Reach `kind` (`whole_water`, `upstream_of`, `between`, `sign_bounded_zone`, ...) says how a scope becomes
  geometry; anchors keep the boundary phrase verbatim (`atlas/splits/anchors.py`, the curated cuts).

## Tests

`pipeline/tests/test_dfo_*.py`, `test_salmon_*.py`, `test_t668_salmon_*.py`, `test_t671_salmon_*.py`,
`test_review_dfo*.py`; the core is `test_dfo_salmon.py`. The worker's twin: `app/workers/feeds/test/`.
