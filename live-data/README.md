# Live Data

Recurring, non-static data feeds — sources that change over time and need
periodic re-fetching, as opposed to `data/` (mostly-static geospatial base
layers, re-fetched occasionally when the underlying government dataset
updates) or `pipeline/` (regulation extraction/parsing/matching, run against
a point-in-time synopsis).

Each subdirectory here is its own proof-of-concept: a standalone fetch
script + local SQLite database, meant to prove out a data source before it
graduates into the production pipeline/app (mirroring how `hydro/` started —
see its README for the "POC → production" framing).

## Current feeds

- **`hydro/`** — hydrometric gauge data (water level / discharge) from ECCC
  Wateroffice, plus BCRFC forecast models (CLEVER/COFFEE/ELF). Moved here
  from the repo-root `hydro-poc/` — same code, same `python hydro_poc.py
  bootstrap|update` workflow, just relocated.
- **`waterbody_db/`** — stocking (FIDQ) + gofishbc markers + bathymetry (WSA
  surveys) + WDIC 1:50K reference data, all fetched into one shared
  `waterbody_db.db`. This absorbed the original standalone `stocking/`,
  `bathymetry/`, and `common/` POCs (their standalone fetch/match scripts and
  shared matching cascade) once its own matching chain reached 100% resolution
  against both source populations — see `waterbody_db/README.md`'s own history
  for that lineage. `fetch_wdic.py` is a self-contained fetch of the *entire*
  WDIC layer — no bulk download exists
  for it, see that module's docstring. `match.py` matches every
  stocking/bathymetry/gofishbc-marker
  identifier directly against WDIC (deliberately no FWA join, name fallback,
  or overrides yet — see that module's own docstring) from one local
  connection, no cross-directory DB paths: `python fetch_all.py` then
  `python match.py`, writing `match_wbid`. A second pass, `match_fwa_gazette.py`,
  reads *only* `match_wbid` and independently corroborates each row against
  *current* FWA by curated override then gazetteer name search,
  disambiguating any name collision by picking whichever candidate sits
  closest to the row's own WDIC-derived coordinate. A third pass,
  `match_fwa_identifier.py`, reads *only* whatever that left unmatched and
  tries the row's identifier one more time — this time joined directly
  against FWA's own group-code column instead of WDIC's. A fourth pass,
  `match_fwa_override.py`, applies a small hand-curated `wbid_overrides.json`
  (keyed by identifier, kept separate from the production
  `pipeline/matching/overrides.json`) to whatever's left, each entry backed
  by a real geographic signal (point-in-polygon lookup, a manually-confirmed
  GNIS name, etc.) — or records a row as confirmed to have no current FWA
  polygon at all, rather than leaving it silently unexplained. Confirmed
  live: 5,217/5,222 (99.9%) resolved with a real `waterbody_key` across that
  four-stage chain; the 5 confirmed no-polygon rows are explained, not
  unexplained — 5,222/5,222 (100%) accounted for. A fifth, parallel pass,
  `match_wbid_gazette.py`, handles the opposite population — the 6
  `match_wbid` rows never matched to WDIC at all, so with no WDIC anchor to
  disambiguate by: gofishbc markers use their **own** lat/lng instead (the
  one exception to every other stage's "read only the prior stage"
  discipline), falling back to point-in-polygon when there's no gazetteer
  name to search by at all; stocking/bathymetry rows try the FWA identifier
  join directly, then gazetteer search narrowed by watershed-group-code.
  Confirmed live: 6/6 (100%) matched. A sixth and final pass, `match_final.py`
  — the one deliberate exception to "read only the prior stage," since its
  whole job is aggregation — reads every table above and writes one row per
  (source, source_id) with the single answer the pipeline landed on and
  which stage produced it. Confirmed live: 5,223/5,228 (99.9%) matched, 5
  `no_polygon`, 0 left `unmatched` — 100% of every row from every source
  accounted for in one place. All six matching tables share the `match_*`
  naming convention. See `waterbody_db/README.md`.

## Fetching everything at once

`fetch_all.py` runs each feed's own fetch step in order (currently just
`waterbody_db/`'s own self-contained `fetch_all.py`):

```bash
cd live-data
python fetch_all.py
```

Matching (`waterbody_db/match.py` and its downstream `match_fwa_*`/`match_final.py`
passes) stays a separate, deliberate step — not bundled into this script.

## Recurring data *not* here (and why)

`pipeline/recurring/in_season_scraper.py` + `in_season_resolver.py` also
fetch recurring data (in-season regulation change notices from the BC Gov
fishing-regulations page) and are conceptually the same kind of thing as
`hydro/`/`waterbody_db/` — but they were deliberately **not** moved into
`live-data/`, because they aren't a POC waiting to graduate: they're already
in production. Every 6 hours, `.github/workflows/update-in-season.yml` runs
`scripts/update-in-season.sh --upload`, which scrapes, resolves, and pushes
`in_season.json` straight to Cloudflare R2 (staging/production); the same
scrape+resolve also runs inline from `pipeline/enrichment/builder.py`'s main
build step. `live-data/` is for things still proving themselves out —
`in_season_scraper`/`in_season_resolver` graduated already.

They *did* move out of `pipeline/matching/` into their own
`pipeline/recurring/` package, though — matching's actual job is
synopsis→FWA linking, and lumping a scheduled live-site scraper in there
blurred what was static-build-time logic vs. what runs on a cron. The
resolver still imports `BaseEntry`/`MatchTable`/`OverrideEntry` from
`pipeline/matching/match_table.py` (that part's a normal cross-package
import, same as several other pipeline modules already do — nothing
special about the directory boundary here).
