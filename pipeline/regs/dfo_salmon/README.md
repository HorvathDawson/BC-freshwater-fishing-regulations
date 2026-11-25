# pipeline/regs/dfo_salmon: DFO salmon limits, openings and closures (Regions 1-8)

The federal salmon rules for BC's eight freshwater regions, from `pac.dfo-mpo.gc.ca`: scraped, typed, bound to waters
and published as a live feed the app lays over the release's salmon chains. Design and the reasons: [`DESIGN.md`](DESIGN.md).

**A different authority from the Synopsis.** Salmon in non-tidal BC water is DFO's; everything else is the provincial
Synopsis. The two rule sets overlay, they never merge: a reach can carry a provincial trout rule and a federal salmon
rule at once, and neither overrides the other. Every DFO page says so ("This table refers to fishing for salmon only").

## The stages (each a module with a `__main__`; run from the repo root with `.venv/bin/python -m pipeline.regs.dfo_salmon.<module>`)

| module | job | writes |
|---|---|---|
| `fetch` | snapshot each page, validate it, hash it, never overwrite good with bad | `cache/dfo_salmon/` (`cache.dfo_salmon` in `config.yaml`; `manifest.json`, `history/`) |
| `parse` | faithful transcription: every `<tr>` is a row, nothing read from the cells | `data/generated/regs/dfo_salmon/regionN.json` |
| `untangle` | regroup into waters, reaches, rules and the defaults; `verify()` proves no row is lost | `--print` / `--out` |
| `locations` | split each place into a LOCATOR (curated half) and RULE RECORDS (scraped half) | `data/generated/regs/dfo_salmon/{locations,rules}/` |
| `typed` + `feed` | the whole decode of limits and dates, once: typed rules keyed by locator fingerprint | `data/generated/regs/dfo_salmon/typed/` |
| `quota`, `wording`, `header` | the salmon bucket (per-species numbers, the region total, a header closure that wins), DFO's limits wording as a closed registry (`data/curated/dfo_salmon/wording.json`), the region header rules | read by `typed` / the release |
| `side_streams` | a named-water place's side streams, derived from DFO's own words (an exact heading, else river only) | read by `entries.to_reach_input` |
| `entries` | the CURATED locators: `seed` adds places new to the page, `reconcile` classifies drift and never mutates | `data/curated/dfo_salmon/places/` (`CURATED.dfo_salmon.places`) |
| `closures`, `closure_mouths` | a closed area inside a place is a location of its own (AGENTS 68) | read by the reach run |
| `live` | THE LIVE FEED: fetch, type, join, write the file the app reads; places nobody curated are held and the owner told | `data/generated/regs/dfo_salmon/feed/index.json` (`dfo-feed/3`) |
| `refresh` | what a fetch would change against the typed rows in use (`live run --dry-run` prints it) | nothing |

Curation helpers (suggest only, never apply; the owner marks, AGENTS A8): `match` (exact registry hits only), `suggest`,
`preverify`, `coverage`, `splitwork`, `dossier`, `edge_gaps`, `cascade`. Measuring tools: `stability` (`python -m pipeline.regs.dfo_salmon.stability --burst`: fetch stability),
`churn` (how the pages changed across archived versions), `validate`.

```bash
.venv/bin/python -m pipeline.regs.dfo_salmon.fetch                       # snapshot -> cache/dfo_salmon/
.venv/bin/python -m pipeline.regs.dfo_salmon.parse                       # faithful rows
.venv/bin/python -m pipeline.regs.dfo_salmon.untangle --regions 6 --print
.venv/bin/python -m pipeline.regs.dfo_salmon.entries reconcile           # what changed, and how bad
.venv/bin/python -m pipeline.regs.dfo_salmon.live run --regions 2 --dry-run   # fetch + parse, write nothing, print the changes
.venv/bin/python -m pipeline.regs.dfo_salmon.live queue --regions 6      # put held new / reworded places in DFO mode
```

The release side: `atlas/reach/dfo_salmon.py` places every location that has an extent (`dfo_salmon.jsonl.gz` in the reach
run) and `deliver/salmon.py` precomputes each chain's answers per day into the release's `salmon` file. The feeds worker
(`app/workers/feeds/src/dfo/`) is the TypeScript twin of `live`; `app/workers/feeds/parity/` holds them to the same output.
The review app marks DFO places (`pipeline/regs/review/`, `curation-review/README.md`; AGENTS 65-68).

## Rules a newcomer must know

- **The transcription reads nothing out of the Limits/Gear or Dates cells** (`parse.py`, `locations.py`); `typed.py` owns
  every reading, so a wording is fixed in one place. Only `fishery_notices` (links) comes from the HTML.
- **Never hand the whole page to a DOM parser.** Regions 4, 5a and 7 carry an unterminated `<!--` above the table, so
  `find("table")` returns `None` silently; the table is sliced out of the raw HTML by regex. `test_dfo_salmon.py` pins it.
- **Rules are a feed, locators are curated** (`feed.py` takes no curated input; a test asserts it). They meet at read time
  by the locator FINGERPRINT, never by a text key a reword would break.
- **Binding is exact or human.** A name becomes a water only by an exact registry hit or a curator; a near spelling is a
  suggestion (`match.py`). Nothing here widens an extent or guesses a month (`dates.py`: an explicit variants table, never fuzzy).
- **`take=0` does not mean closed:** `no_fishing` and `non_retention` are separate; a bare "0 per day" is flagged for review.
- **Region 6 is a cascade** of eight lettered scope bands, not a list; read flat it says the opposite of what it means
  (see DESIGN.md "Region 6").
- Fishery Notice rows (`FN0679`, ...) are live variation orders that supersede the printed season and change without the
  page's `dateModified` moving: that is the volatile part.
- Region 5 is three pages (`5` is a 2016 stub; the rules are on `5a` and `5b`); pages are keyed by slug, not region number.

## Tests

`pipeline/tests/test_dfo_*.py`, `test_salmon_*.py`, `test_t668_salmon_*.py`, `test_t671_salmon_*.py`, `test_review_dfo*.py`
(the core is `test_dfo_salmon.py`: parser fixtures including the 2017 and 2018 archived pages, the unbalanced-comment pin,
the cascade resolving through the join). The feeds worker's twin is tested under `app/workers/feeds/test/`.
